"""Resumable public HH candidate collection using a local Scrapling browser.

SQLite is the checkpoint; CSV/JSONL are streaming, replaceable exports.
No Firecrawl key, HH login, stealth fetcher or CAPTCHA solving is used.
"""

import argparse
import asyncio
import csv
import gzip
import hashlib
import json
import os
import sqlite3
import time
import urllib.parse
from pathlib import Path
from lxml import etree, html as lxml_html

from .collect import csv_safe, utc_now
from .hh_pipeline import build_searches, load_config
from .hh_web import classify_record, parse_detail, parse_search, public_url, search_url

from .paths import CONFIG_DIR, PROJECT_ROOT

ROOT = PROJECT_ROOT


class AccessCheckRequired(ValueError):
    """HH explicitly redirected to a CAPTCHA or authentication page."""


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def connect(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS tasks (
            url TEXT PRIMARY KEY, kind TEXT NOT NULL, params TEXT NOT NULL,
            route TEXT NOT NULL, page INTEGER NOT NULL DEFAULT 0,
            state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT, snapshot TEXT, snapshot_sha256 TEXT,
            observed_at TEXT, found INTEGER, listed INTEGER);
        CREATE INDEX IF NOT EXISTS task_queue ON tasks(state, kind);
        CREATE INDEX IF NOT EXISTS search_queue ON tasks(state, kind, page);
        CREATE TABLE IF NOT EXISTS vacancies (
            id TEXT PRIMARY KEY, index_json TEXT NOT NULL,
            record_json TEXT, state TEXT NOT NULL DEFAULT 'pending');
        CREATE INDEX IF NOT EXISTS vacancy_state ON vacancies(state);
        CREATE TABLE IF NOT EXISTS routes (
            vacancy_id TEXT NOT NULL, route TEXT NOT NULL,
            PRIMARY KEY(vacancy_id, route));
    """)
    return db


def canonical(url):
    public_url(url)
    parts = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path,
                                   urllib.parse.urlencode(sorted(urllib.parse.parse_qsl(parts.query))), ""))


def enqueue(db, url, kind, route, params=None):
    url = canonical(url)
    db.execute("INSERT OR IGNORE INTO tasks(url,kind,params,route,page) VALUES (?,?,?,?,?)",
               (url, kind, encoded(params or {}), route, (params or {}).get("page", 0)))


def seed(db, config, searches, *, extend=False, previous_config=None):
    scope = {"config": config, "searches": searches}
    fingerprint = hashlib.sha256(encoded(scope).encode()).hexdigest()
    previous = db.execute("SELECT value FROM meta WHERE key='scope_hash'").fetchone()
    if previous and previous[0] != fingerprint:
        if not extend:
            raise ValueError("Search scope/config differs. Use --extend-searches for additive expansion, or another --output folder.")
        stored = db.execute("SELECT value FROM meta WHERE key='scope_json'").fetchone()
        if stored:
            old_scope = json.loads(stored[0])
        elif previous_config:
            old_scope = {"config": previous_config, "searches": [
                {"route": row["route"], "params": json.loads(row["params"])}
                for row in db.execute("SELECT route,params FROM tasks WHERE kind='search' AND page=0 ORDER BY rowid")]}
        else:
            raise ValueError("Original config is required to extend this older checkpoint")
        if hashlib.sha256(encoded(old_scope).encode()).hexdigest() != previous[0]:
            raise ValueError("Original scope fingerprint does not match; checkpoint left unchanged")
        validate_extension(old_scope, scope)
        with db:
            # Preserve completed requests and every detail. A larger list page
            # supersedes only pending old list requests; IDs remain deduplicated.
            if old_scope["config"]["per_page"] != config["per_page"]:
                db.execute("UPDATE tasks SET state='superseded' WHERE kind='search' AND state='pending'")
            for row in db.execute("SELECT id,record_json FROM vacancies WHERE record_json IS NOT NULL ORDER BY id"):
                record = json.loads(row["record_json"])
                updated = classify_record(record, config, record["discovery_route"])
                db.execute("UPDATE vacancies SET record_json=? WHERE id=?", (encoded(updated), row["id"]))
            db.execute("INSERT OR REPLACE INTO meta VALUES ('previous_scope',?)", (encoded(old_scope),))
            db.execute("INSERT OR REPLACE INTO meta VALUES ('scope_hash',?)", (fingerprint,))
            db.execute("INSERT OR REPLACE INTO meta VALUES ('scope_json',?)", (encoded(scope),))
            for search in searches:
                enqueue(db, search_url(search["params"]), "search", search["route"], search["params"])
        print(f"Expanded queue to {len(searches)} searches; saved cards retained and reclassified.")
        return
    with db:
        db.execute("INSERT OR IGNORE INTO meta VALUES ('scope_hash',?)", (fingerprint,))
        db.execute("INSERT OR IGNORE INTO meta VALUES ('scope_json',?)", (encoded(scope),))
        db.execute("INSERT OR IGNORE INTO meta VALUES ('started_at',?)", (utc_now(),))
        for search in searches:
            enqueue(db, search_url(search["params"]), "search", search["route"], search["params"])


def validate_extension(old_scope, scope):
    old, new = old_scope["config"], scope["config"]
    additive = {"search_queries", "employers", "strong_signal_groups", "weak_signal_groups", "exclude_title_patterns",
                "additional_searches", "candidate_employers"}
    for key in set(old) | set(new):
        if key in additive:
            if not {encoded(v) for v in old.get(key, [])} <= {encoded(v) for v in new.get(key, [])}:
                raise ValueError(f"--extend-searches cannot remove/change existing {key}")
        elif key == "per_page":
            if new[key] < old[key]:
                raise ValueError("--extend-searches cannot decrease per_page")
        elif old.get(key) != new.get(key):
            raise ValueError(f"--extend-searches cannot change {key}; use another output")
    def identity(search):
        return encoded({"route": search["route"], "params": {k: v for k, v in search["params"].items() if k != "per_page"}})
    if not {identity(s) for s in old_scope["searches"]} <= {identity(s) for s in scope["searches"]}:
        raise ValueError("--extend-searches cannot remove existing search routes")


def remember_index(db, record, route):
    vid = str(record["source_vacancy_id"])
    existing = db.execute("SELECT state,index_json FROM vacancies WHERE id=?", (vid,)).fetchone()
    if existing and existing["state"] == "restricted":
        previous = json.loads(existing["index_json"])
        record = dict(record, **{key: previous[key] for key in
                               ("detail_status", "source_visibility", "filter_decision")})
    db.execute("INSERT INTO vacancies(id,index_json) VALUES (?,?) ON CONFLICT(id) DO UPDATE SET index_json=excluded.index_json",
               (vid, encoded(record)))
    db.execute("INSERT OR IGNORE INTO routes VALUES (?,?)", (vid, route))
    enqueue(db, record["detail_url"], "detail", route)


def process(db, task, content, observed, config, pages, snapshot=None, digest=None):
    """Parse first; commit queue advance and all records in ONE transaction."""
    if task["kind"] == "search":
        rows, report = parse_search(content, task["url"], observed)
        state = "capped" if report["has_next"] and task["page"] + 1 >= pages else "done"
        # A repeated/empty page with next enabled is not treated as full coverage.
        if not rows and report["has_next"]:
            raise ValueError("Empty search page still reports a next page; pagination needs inspection")
        with db:
            for record in rows:
                remember_index(db, record, task["route"])
                # A newly discovered company-name route must also be recorded
                # for an already completed card, without downloading it again.
                if task["route"].startswith(("gur_name:", "gur_focus:")):
                    previous = db.execute("SELECT record_json FROM vacancies WHERE id=?", (record["source_vacancy_id"],)).fetchone()
                    if previous and previous[0]:
                        saved = json.loads(previous[0])
                        if saved.get("filter_decision") == "excluded_no_signal":
                            saved = classify_record(saved, config, task["route"])
                            db.execute("UPDATE vacancies SET record_json=? WHERE id=?", (encoded(saved), record["source_vacancy_id"]))
            if not report["has_next"] and isinstance(report["found"], int):
                discovered = db.execute("SELECT count(*) FROM routes WHERE route=?", (task["route"],)).fetchone()[0]
                if discovered < report["found"]:
                    # Some search endpoints stop pagination before the reported
                    # total. Flag the gap instead of silently claiming coverage.
                    state = "incomplete"
            if report["has_next"] and state == "done":
                params = json.loads(task["params"])
                params["page"] = task["page"] + 1
                enqueue(db, search_url(params), "search", task["route"], params)
            db.execute("UPDATE tasks SET state=?,found=?,listed=? WHERE url=?",
                       (state, report["found"], len(rows), task["url"]))
            finish_task(db, task["url"], observed, snapshot, digest)
    else:
        record = classify_record(parse_detail(content, task["url"], observed), config, task["route"])
        if record["filter_decision"] == "excluded_no_signal":
            company_route = db.execute("SELECT route FROM routes WHERE vacancy_id=? AND (route LIKE 'gur_name:%' OR route LIKE 'gur_focus:%') ORDER BY route LIMIT 1",
                                       (record["source_vacancy_id"],)).fetchone()
            if company_route:
                record = classify_record(record, config, company_route[0])
        with db:
            vid = str(record["source_vacancy_id"])
            db.execute("INSERT INTO vacancies(id,index_json,record_json,state) VALUES (?,?,?,'done') "
                       "ON CONFLICT(id) DO UPDATE SET record_json=excluded.record_json,state='done'",
                       (vid, encoded(record), encoded(record)))
            db.execute("INSERT OR IGNORE INTO routes VALUES (?,?)", (vid, task["route"]))
            db.execute("UPDATE tasks SET state='done' WHERE url=?", (task["url"],))
            finish_task(db, task["url"], observed, snapshot, digest)


def finish_task(db, url, observed, snapshot, digest):
    db.execute("UPDATE tasks SET attempts=attempts+1,last_error=NULL,snapshot=?,snapshot_sha256=?,observed_at=? WHERE url=?",
               (snapshot, digest, observed, url))


def failure(db, task, error):
    # The task stays pending. A later explicit --run resumes exactly here.
    with db:
        db.execute("UPDATE tasks SET attempts=attempts+1,last_error=? WHERE url=?", (str(error)[:2000], task["url"]))


def stats(db):
    result = {"unique_listed": db.execute("SELECT count(*) FROM vacancies").fetchone()[0],
              "full_cards": db.execute("SELECT count(*) FROM vacancies WHERE state='done'").fetchone()[0]}
    for row in db.execute("SELECT state,count(*) AS n FROM tasks GROUP BY state"):
        result["tasks_" + row["state"]] = row["n"]
    return result


def save_snapshot(output, url, content, metadata):
    raw = content.encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    name = hashlib.sha256(url.encode()).hexdigest()[:20] + "_" + digest[:16]
    folder = output / "raw"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / (name + ".html.gz")
    temp = path.with_suffix(".part")
    temp.write_bytes(gzip.compress(raw, compresslevel=6, mtime=0))
    temp.replace(path)
    path.with_suffix(".metadata.json").write_text(encoded(dict(metadata, sourceURL=url, sha256=digest)), encoding="utf-8")
    return str(path.relative_to(output)), digest


class Browser:
    def __init__(self, output, timeout, workers=2, transport="http", headless=True):
        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(ROOT / ".browser-cache"))
        from scrapling.fetchers import AsyncDynamicSession
        self.timeout, self.transport = timeout, transport
        self.state_path = output / "browser-profile" / "session-state.json"
        self.session = AsyncDynamicSession(headless=headless, google_search=False, retries=1,
                                      disable_resources=True, network_idle=False, max_pages=workers,
                                      timeout=int(timeout * 1000),
                                      user_data_dir=str((output / "browser-profile").resolve()))

    async def __aenter__(self):
        await self.session.start()
        if self.state_path.exists():
            try:
                cookies = hh_cookies(json.loads(self.state_path.read_text(encoding="utf-8")))
                if cookies:
                    await self.session.context.add_cookies(cookies)
            except Exception:
                await self.session.close()
                raise
        return self

    async def __aexit__(self, *args):
        try:
            await self.save_session()
        except Exception:
            # A closed browser or failed state write must not mask the actual
            # collection error. Records and queue remain in SQLite.
            print("Browser session could not be saved; the collection checkpoint is intact.", flush=True)
        finally:
            await self.session.close()

    async def save_session(self):
        state = await self.session.context.storage_state()
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_suffix(".json.part")
        temporary.write_text(encoded({"cookies": hh_cookies(state), "origins": []}), encoding="utf-8")
        temporary.replace(self.state_path)

    async def fetch(self, url):
        public_url(url)
        if self.transport == "browser":
            response = await self.session.fetch(url)
            return response.body.decode("utf-8-sig"), {"statusCode": response.status, "url": response.url,
                    "observed_at": utc_now(), "transport": "scrapling_browser"}
        current, visited, redirects = url, {url}, []
        for hop in range(5):
            response = await self.session.context.request.get(current, timeout=int(self.timeout * 1000), max_redirects=0)
            try:
                content = (await response.body()).decode("utf-8-sig")
                metadata = {"statusCode": response.status, "url": response.url,
                            "observed_at": utc_now(), "transport": "browser_session_http", "redirects": redirects.copy()}
                print(f"Fetched ({response.status}) <GET {current}> [HTTP HTML]", flush=True)
                if response.status not in (301, 302, 303, 307, 308):
                    return content, metadata
                location = response.headers.get("location")
                if not location:
                    metadata["redirect_error"] = "Redirect has no Location header"
                    return content, metadata
                target = urllib.parse.urljoin(current, location)
                decision, reason = redirect_decision(url, target)
                metadata.update(redirect_url=target, redirect_reason=reason)
                if decision != "follow":
                    metadata["redirect_skipped"] = decision == "skip"
                    if decision == "stop":
                        metadata["redirect_error"] = reason
                    return content, metadata
                if target in visited or hop == 4:
                    metadata["redirect_error"] = "Redirect loop or redirect limit reached"
                    return content, metadata
                redirects.append({"url": current, "status": response.status, "location": target})
                visited.add(target)
                current = target
            finally:
                await response.dispose()


def hh_cookies(state):
    """Keep only HH cookies; never export session data alongside vacancy files."""
    result = []
    for cookie in state.get("cookies", []):
        domain = str(cookie.get("domain", "")).lstrip(".").casefold()
        if domain == "hh.ru" or domain.endswith(".hh.ru"):
            result.append(cookie)
    return result


async def manual_check(db, args, input_function=input):
    blocked = db.execute("SELECT url FROM tasks WHERE state='pending' AND last_error IS NOT NULL ORDER BY rowid LIMIT 1").fetchone()
    task = blocked or db.execute("SELECT url FROM tasks WHERE state='pending' ORDER BY rowid LIMIT 1").fetchone()
    explicit_url = getattr(args, "check_url", None)
    if task is None and not explicit_url:
        print("No pending request to check.")
        return "no_pending_requests"
    url = explicit_url or task["url"]
    public_url(url)
    async with Browser(args.output, args.timeout, workers=1, transport="browser", headless=False) as browser:
        page = await browser.session.context.new_page()
        response = await page.goto(url, wait_until="domcontentloaded", timeout=int(args.timeout * 1000))
        if "captcha" in urllib.parse.urlsplit(page.url).path.casefold():
            print("HH показав капчу у відкритому браузері.", flush=True)
        else:
            print("Сторінка HH відкрита. Перевірте її у браузері.", flush=True)
        print("Відкрито браузер HH. Пройди капчу вручну, якщо вона показана.", flush=True)
        print("Дочекайся відкриття вакансії/пошуку. Повернись у термінал і натисни Enter. Залиш браузер відкритим до цього.", flush=True)
        await asyncio.to_thread(input_function, "Enter після завершення перевірки: ")
        try:
            public_url(page.url)
        except ValueError as exc:
            raise AccessCheckRequired("Перевірка ще не завершена: у браузері не відкрита публічна сторінка вакансії/пошуку") from exc
        content = await page.content()
        if urllib.parse.urlsplit(url).path.startswith("/vacancy/"):
            parse_detail(content, url, utc_now())
        else:
            parse_search(content, url, utc_now())
        await browser.save_session()
        await page.close()
    print("Перевірку пройдено. Сесію збережено локально; черга й картки не змінені.", flush=True)
    return "manual_check_complete"


def redirect_decision(requested, target):
    """Follow only public HH URLs preserving the vacancy ID or search path."""
    original, destination = urllib.parse.urlsplit(requested), urllib.parse.urlsplit(target)
    marker = ((destination.hostname or "") + destination.path).casefold()
    if any(term in marker for term in ("captcha", "/account", "/login", "/oauth", "/auth", "sso.")):
        return "stop", "Redirect requires authentication or CAPTCHA"
    try:
        public_url(target)
    except ValueError:
        return ("skip", "Vacancy redirects to a page outside public HH vacancy endpoints") if \
            original.path.startswith("/vacancy/") else ("stop", "Search redirects outside public HH search endpoints")
    if original.path.startswith("/vacancy/"):
        if destination.path != original.path:
            return "skip", "Vacancy redirects to a different page/ID"
    elif destination.path != original.path:
        return "stop", "Search redirects to a different endpoint"
    else:
        required = {"area", "employer_id", "text", "page", "items_on_page", "search_field"}
        first = {k: v for k, v in urllib.parse.parse_qs(original.query).items() if k in required}
        second = {k: v for k, v in urllib.parse.parse_qs(destination.query).items() if k in required}
        if first != second:
            return "stop", "Search redirect changes configured filters"
    return "follow", "Public HH redirect preserving page identity"


def run_queue(db, config, args, fetcher, output):
    # Compatibility helper for synchronous/offline fetchers; live mode awaits
    # the scheduler within the browser's own event loop.
    async def fetch(url):
        return fetcher(url)
    return asyncio.run(run_queue_async(db, config, args, fetch, output))


def next_task(db, inflight, prefer_search):
    reserved = {task["url"] for task in inflight.values()}
    kinds = ("search", "detail") if prefer_search else ("detail", "search")
    for kind in kinds:
        priority = "CASE WHEN route LIKE 'focus:%' THEN 0 WHEN route LIKE 'gur_focus:%' THEN 1 WHEN route LIKE 'candidate_employer:%' THEN 2 WHEN route LIKE 'region:%' THEN 3 ELSE 4 END"
        order = priority + (",page,rowid" if kind == "search" else ",rowid")
        # Equality predicates use queue indices, then skip the few in-flight
        # URLs. No persisted 'running' state can strand tasks after a crash.
        for task in db.execute(f"SELECT * FROM tasks WHERE state='pending' AND kind=? ORDER BY {order} LIMIT ?",
                               (kind, len(inflight) + 1)):
            if task["url"] not in reserved:
                return task
    return None


def restricted_vacancy_page(content):
    """Recognize HH's explicit per-vacancy denial, not a general HTTP block."""
    try:
        document = lxml_html.fromstring(content)
        visible = " ".join(document.xpath(
            "//body//text()[not(ancestor::script) and not(ancestor::style) and not(ancestor::template)]"))
    except (ValueError, etree.ParserError):
        return False
    visible = " ".join(visible.casefold().split())
    return ("вам недоступна эта вакансия." in visible and
            "войдите как пользователь, у которого есть доступ на просмотр" in visible)


def accept_result(db, task, content, metadata, config, pages, output):
    snapshot, digest = save_snapshot(output, task["url"], content, metadata)
    status = metadata["statusCode"]
    if status == 403 and task["kind"] == "detail" and restricted_vacancy_page(content):
        public_url(metadata["url"])
        vid = urllib.parse.urlsplit(task["url"]).path.rsplit("/", 1)[-1]
        with db:
            db.execute("UPDATE tasks SET state='restricted' WHERE url=?", (task["url"],))
            row = db.execute("SELECT index_json FROM vacancies WHERE id=?", (vid,)).fetchone()
            if row:
                record = json.loads(row[0])
                record.update(detail_status="restricted", source_visibility="access_restricted_by_source",
                              filter_decision="not_evaluated_restricted")
                db.execute("UPDATE vacancies SET state='restricted',index_json=? WHERE id=?", (encoded(record), vid))
            finish_task(db, task["url"], metadata["observed_at"], snapshot, digest)
        print(f"Skipped restricted vacancy {vid}: HH explicitly denies public viewing.", flush=True)
        return False
    skipped = metadata.get("redirect_skipped", False)
    reason = metadata.get("redirect_reason", "")
    if status == 200 and metadata["url"] != task["url"]:
        decision, reason = redirect_decision(task["url"], metadata["url"])
        skipped = decision == "skip"
        if decision == "stop":
            error_type = AccessCheckRequired if "authentication or CAPTCHA" in reason else ValueError
            raise error_type(f"{reason}; inspect {snapshot}")
    if skipped and task["kind"] == "detail":
        with db:
            db.execute("UPDATE tasks SET state='redirected' WHERE url=?", (task["url"],))
            vid = urllib.parse.urlsplit(task["url"]).path.rsplit("/", 1)[-1]
            row = db.execute("SELECT index_json FROM vacancies WHERE id=?", (vid,)).fetchone()
            if row:
                index = json.loads(row[0])
                index.update(detail_status="redirected_non_vacancy", filter_decision="not_evaluated_redirect",
                             source_visibility="no_public_vacancy_card_at_url")
                db.execute("UPDATE vacancies SET state='redirected',index_json=? WHERE id=?", (encoded(index), vid))
            finish_task(db, task["url"], metadata["observed_at"], snapshot, digest)
        print(f"Skipped redirected vacancy {vid}: {reason}", flush=True)
        return False
    if status in (404, 410) and task["kind"] == "detail":
        with db:
            db.execute("UPDATE tasks SET state='unavailable' WHERE url=?", (task["url"],))
            db.execute("UPDATE vacancies SET state='unavailable' WHERE id=?", (task["url"].rsplit("/", 1)[-1],))
            finish_task(db, task["url"], metadata["observed_at"], snapshot, digest)
        return False
    if status != 200:
        if "authentication or CAPTCHA" in metadata.get("redirect_error", ""):
            raise AccessCheckRequired(f"HH requires a manual access check; inspect {snapshot}. Run hh_bulk.py --manual-check.")
        raise ValueError(f"HTTP {status}. {metadata.get('redirect_error', 'Collection stopped')}; inspect {snapshot}")
    public_url(metadata["url"])
    process(db, task, content, metadata["observed_at"], config, pages, snapshot, digest)
    return task["kind"] == "detail"


async def run_queue_async(db, config, args, fetcher, output):
    requests = completed = 0
    initial_cards = full_cards = stats(db)["full_cards"]
    inflight = {}
    started, next_start = time.monotonic(), 0
    workers = getattr(args, "workers", 1)
    discovery_every = getattr(args, "discovery_every", 5)
    deadline = getattr(args, "deadline", None)
    checkpoint_every = getattr(args, "checkpoint_every", 0)
    status = "error"
    try:
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                status = "time_budget_reached"
                return status
            waiting_for_rate = False
            while (len(inflight) < workers and requests < args.max_requests
                   and full_cards + sum(t["kind"] == "detail" for t in inflight.values()) < args.limit):
                if time.monotonic() < next_start:
                    waiting_for_rate = True
                    break
                priority = getattr(args, "priority_url", None) if requests == 0 else None
                task = db.execute("SELECT * FROM tasks WHERE state='pending' AND url=?", (priority,)).fetchone() if priority else None
                if task is None:
                    task = next_task(db, inflight, prefer_search=requests % discovery_every == 0)
                if task is None:
                    break
                future = asyncio.create_task(fetcher(task["url"]))
                inflight[future] = task
                requests += 1
                # ONE global start interval, shared across workers. Do not add
                # another delay after completion of each page.
                next_start = time.monotonic() + args.delay
            if not inflight:
                if full_cards >= args.limit:
                    status = "limit_reached"
                    return status
                if requests >= args.max_requests:
                    status = "request_budget_reached"
                    return status
                if next_task(db, {}, prefer_search=True) is None:
                    status = "queue_exhausted"
                    return status
                sleep_seconds = max(0, next_start - time.monotonic())
                if deadline is not None:
                    sleep_seconds = min(sleep_seconds, max(0, deadline - time.monotonic()))
                await asyncio.sleep(sleep_seconds)
                continue
            timeout = max(0, next_start - time.monotonic()) if waiting_for_rate else None
            if deadline is not None:
                remaining_seconds = max(0, deadline - time.monotonic())
                timeout = remaining_seconds if timeout is None else min(timeout, remaining_seconds)
            done, _ = await asyncio.wait(inflight, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
            for future in done:
                task = inflight.pop(future)
                try:
                    content, metadata = future.result()
                    if accept_result(db, task, content, metadata, config, args.pages, output):
                        full_cards += 1
                except Exception as exc:
                    failure(db, task, exc)
                    raise
                completed += 1
                if checkpoint_every and completed % checkpoint_every == 0:
                    try:
                        export(db, output, config, "running")
                    except PermissionError:
                        print("CSV export is locked by another app; SQLite progress is saved.", flush=True)
                if completed == 1 or completed % 20 == 0:
                    elapsed = max(0.001, time.monotonic() - started)
                    print(encoded(dict(stats(db), requests_this_run=requests,
                                       requests_completed_this_run=completed, in_flight=len(inflight),
                                       new_cards_this_run=full_cards - initial_cards,
                                       cards_per_minute=round(60 * (full_cards - initial_cards) / elapsed, 1))), flush=True)
    except AccessCheckRequired:
        status = "access_check_required"
        raise
    except asyncio.CancelledError:
        status = "interrupted"
        raise
    finally:
        # Stop all other downloads on denial, error or Ctrl+C. Their tasks stay
        # pending; reserved work never counts as a completed card.
        for future in inflight:
            future.cancel()
        await asyncio.gather(*inflight, return_exceptions=True)
        elapsed = round(time.monotonic() - started, 3)
        metrics = {"requests_started": requests, "requests_completed": completed,
                   "new_full_cards": full_cards - initial_cards, "elapsed_seconds": elapsed,
                   "cards_per_minute": round(60 * (full_cards - initial_cards) / max(elapsed, .001), 1),
                   "workers": workers, "start_interval_seconds": args.delay, "status": status}
        with db:
            db.execute("INSERT OR REPLACE INTO meta VALUES ('last_run_metrics',?)", (encoded(metrics),))


async def run_live(db, config, args):
    async with Browser(args.output, args.timeout, args.workers, args.transport) as browser:
        return await run_queue_async(db, config, args, browser.fetch, args.output)


def challenge_task(db):
    return db.execute("SELECT url FROM tasks WHERE state='pending' AND last_error IS NOT NULL "
                      "AND (lower(last_error) LIKE '%captcha%' OR lower(last_error) LIKE '%manual access%') "
                      "ORDER BY rowid LIMIT 1").fetchone()


def night_event(output, event, **fields):
    entry = dict(observed_at=utc_now(), event=event, **fields)
    with (output / "night_events.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(encoded(entry) + "\n")
    print("Night: " + encoded(entry), flush=True)


async def run_overnight(db, config, args, runner=None, sleeper=None, monotonic=None):
    """Bounded unattended run. Wait on access checks; never solve or skip them."""
    runner = runner or run_live
    sleeper = sleeper or asyncio.sleep
    monotonic = monotonic or time.monotonic
    args.workers, args.delay = 1, max(3, args.delay)
    args.checkpoint_every = 100
    args.deadline = monotonic() + args.hours * 3600
    total_budget = args.max_requests
    total_requests = failed_checks = 0
    probe = challenge_task(db)
    waiting = probe is not None
    night_event(args.output, "started", hours=args.hours, workers=1, start_interval_seconds=args.delay,
                request_budget=total_budget, existing_full_cards=stats(db)["full_cards"])
    try:
        while monotonic() < args.deadline:
            if stats(db)["full_cards"] >= args.limit:
                return "limit_reached"
            if total_requests >= total_budget:
                return "request_budget_reached"
            if waiting:
                seconds = min(args.cooldown_minutes * 60, max(0, args.deadline - monotonic()))
                try:
                    export(db, args.output, config, "cooldown")
                except PermissionError:
                    print("CSV export is locked by another app; SQLite progress is saved.", flush=True)
                night_event(args.output, "access_cooldown", wait_seconds=seconds,
                            failed_checks=failed_checks, requests_started=total_requests)
                await sleeper(seconds)
                if monotonic() >= args.deadline:
                    return "time_budget_reached"
            probe = challenge_task(db) if waiting else None
            args.priority_url = probe["url"] if probe else None
            # Probe exactly the challenged URL once, without other requests.
            args.max_requests = 1 if probe else total_budget - total_requests
            with db:
                db.execute("DELETE FROM meta WHERE key='last_run_metrics'")
            challenged = False
            try:
                status = await runner(db, config, args)
            except AccessCheckRequired:
                challenged = True
                status = "access_check_required"
            metrics_row = db.execute("SELECT value FROM meta WHERE key='last_run_metrics'").fetchone()
            metrics = json.loads(metrics_row[0]) if metrics_row else {}
            total_requests += metrics.get("requests_started", 0)
            night_event(args.output, "batch_finished", status=status, requests_started=total_requests,
                        full_cards=stats(db)["full_cards"])
            if challenged:
                if probe:
                    failed_checks += 1
                if failed_checks >= args.max_access_checks:
                    night_event(args.output, "manual_check_still_required", failed_checks=failed_checks)
                    return "access_check_required"
                waiting = True
                continue
            waiting, failed_checks = False, 0
            args.priority_url = None
            if probe and status == "request_budget_reached":
                # Valid public response was parsed and committed. Resume the
                # remaining budget in the SAME persisted HH session.
                continue
            return status
        return "time_budget_reached"
    finally:
        args.max_requests = total_budget
        args.priority_url = None
        with db:
            db.execute("INSERT OR REPLACE INTO meta VALUES ('night_run',?)", (encoded({
                "requests_started": total_requests, "hours": args.hours,
                "start_interval_seconds": args.delay, "failed_access_checks": failed_checks,
                "finished_at": utc_now()}),))


def import_snapshots(db, folder, output, config, pages):
    """Replay existing pilot HTML with provenance, without downloading again."""
    count = 0
    # Listing pages first, so existing discovery routes are preserved on details.
    sources = []
    for path in sorted(folder.glob("*.html")):
        meta = path.with_suffix(".metadata.json")
        if not meta.exists():
            continue
        metadata = json.loads(meta.read_text(encoding="utf-8-sig"))
        url = canonical(metadata.get("sourceURL") or metadata.get("url"))
        if metadata.get("statusCode", 200) != 200:
            continue
        sources.append(("/search/vacancy" not in url, path, url, metadata))
    for _, path, url, metadata in sorted(sources, key=lambda item: (item[0], str(item[1]))):
        content = path.read_text(encoding="utf-8-sig")
        is_search = "/search/vacancy" in url
        if is_search:
            _, report = parse_search(content, url, "import")
            # Imported pages must match the configured, bounded search scope.
            configured = db.execute("SELECT * FROM tasks WHERE url=? AND kind='search'", (url,)).fetchone()
            if configured is None:
                raise ValueError(f"Snapshot search URL is outside this queue: {url}")
            route = configured["route"]
        else:
            configured = db.execute("SELECT * FROM tasks WHERE url=? AND kind='detail'", (url,)).fetchone()
            if configured is None:
                raise ValueError(f"Snapshot detail was not discovered by an in-scope search: {url}")
            route = configured["route"]
        observed = metadata.get("observed_at")
        if not observed:
            raise ValueError(f"Snapshot lacks observed_at: {path}")
        if configured["state"] == "done" and configured["observed_at"] and configured["observed_at"] >= observed:
            continue
        snapshot, digest = save_snapshot(output, url, content, metadata)
        process(db, configured, content, observed, config, pages, snapshot, digest)
        count += 1
    return count


def publish_export(part, target, warnings):
    try:
        part.replace(target)
        return target.name
    except PermissionError:
        alternate = target.with_name(f"{target.stem}_export_{time.time_ns()}{target.suffix}")
        part.replace(alternate)
        warnings.append({"requested_file": target.name, "saved_file": alternate.name,
                         "reason": "Windows denied replacement; the original file may be open in another app"})
        print(f"Cannot replace {target.name}; updated export saved to {alternate.name}. "
              "Close the original file and run --export-only to refresh its usual name.", flush=True)
        return alternate.name


def export(db, output, config, status):
    """Stream exports from SQLite; keep RAM independent of total row count."""
    fields = config["output_fields"]
    counts, warnings, files = {}, [], {}
    for group, condition in (
            ("full_cards", "state='done'"),
            ("vacancies", "state='done' AND json_extract(record_json,'$.filter_decision') LIKE 'candidate_%'"),
            ("review", "state='done' AND json_extract(record_json,'$.filter_decision') LIKE 'review_%'"),
            ("excluded", "state='done' AND json_extract(record_json,'$.filter_decision') LIKE 'excluded_%'"),
            ("index", "1=1")):
        target_json = output / (group + ".jsonl")
        target_csv = output / (group + ".csv")
        count = 0
        with target_json.with_suffix(".jsonl.part").open("w", encoding="utf-8") as jf, \
                target_csv.with_suffix(".csv.part").open("w", encoding="utf-8-sig", newline="") as cf:
            writer = csv.DictWriter(cf, fieldnames=fields)
            writer.writeheader()
            column = "index_json" if group == "index" else "record_json"
            for row in db.execute(f"SELECT id,{column} AS record FROM vacancies WHERE {condition} ORDER BY id"):
                record = json.loads(row["record"])
                jf.write(encoded(record) + "\n")
                writer.writerow({key: csv_safe(record.get(key, "")) for key in fields})
                count += 1
        files[group] = {
            "jsonl": publish_export(target_json.with_suffix(".jsonl.part"), target_json, warnings),
            "csv": publish_export(target_csv.with_suffix(".csv.part"), target_csv, warnings)}
        counts[group] = count
    with (output / "discovery_routes.jsonl.part").open("w", encoding="utf-8") as stream:
        for row in db.execute("SELECT * FROM routes ORDER BY vacancy_id,route"):
            stream.write(encoded(dict(row)) + "\n")
    publish_export(output / "discovery_routes.jsonl.part", output / "discovery_routes.jsonl", warnings)
    with (output / "search_report.jsonl.part").open("w", encoding="utf-8") as stream:
        for row in db.execute("SELECT url,route,page,state,found,listed,last_error FROM tasks WHERE kind='search'"):
            stream.write(encoded(dict(row)) + "\n")
    publish_export(output / "search_report.jsonl.part", output / "search_report.jsonl", warnings)
    with (output / "errors.jsonl.part").open("w", encoding="utf-8") as stream:
        for row in db.execute("SELECT url,kind,state,attempts,last_error FROM tasks WHERE last_error IS NOT NULL"):
            stream.write(encoded(dict(row)) + "\n")
    publish_export(output / "errors.jsonl.part", output / "errors.jsonl", warnings)
    transport = db.execute("SELECT value FROM meta WHERE key='transport'").fetchone()
    metrics = db.execute("SELECT value FROM meta WHERE key='last_run_metrics'").fetchone()
    night = db.execute("SELECT value FROM meta WHERE key='night_run'").fetchone()
    manifest = dict(stats(db), source="hh_html", scope="configured defence candidates only", status=status,
                    exported_at=utc_now(), counts=counts, export_files=files, export_warnings=warnings,
                    api_credits_used_this_run=0,
                    transport=transport[0] if transport else "scrapling_browser",
                    last_run_metrics=json.loads(metrics[0]) if metrics else None,
                    night_run=json.loads(night[0]) if night else None,
                    tasks_with_errors=db.execute("SELECT count(*) FROM tasks WHERE last_error IS NOT NULL").fetchone()[0],
                    coverage="Bounded search; capped/incomplete tasks, errors and pending tasks prevent a completeness claim",
                    classification="Candidates, weak signals and exclusions are recorded; VPK affiliation is unverified")
    (output / "manifest.json.part").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    publish_export(output / "manifest.json.part", output / "manifest.json", warnings)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--run", action="store_true")
    modes.add_argument("--export-only", action="store_true")
    modes.add_argument("--import-snapshots", type=Path)
    modes.add_argument("--manual-check", action="store_true", help="Open a visible HH browser for a human CAPTCHA/access check")
    modes.add_argument("--overnight", action="store_true", help="Unattended run with slower requests, checkpoints and access cooldowns")
    parser.add_argument("--config", type=Path, default=CONFIG_DIR / "hh_config.json")
    parser.add_argument("--output", type=Path, default=ROOT / "data" / "hh_bulk")
    parser.add_argument("--query")
    parser.add_argument("--employer-id")
    parser.add_argument("--check-url", help="Public HH URL to open with --manual-check, including after a queue reset")
    parser.add_argument("--extend-searches", action="store_true", help="Add searches/rules to this checkpoint while preserving cards")
    parser.add_argument("--limit", type=int, default=1000, help="Total unique full cards in database, including classified exclusions")
    parser.add_argument("--max-requests", type=int, default=1000, help="Top-level page downloads per invocation (browser also requests resources)")
    parser.add_argument("--pages", type=int, default=100, help="Maximum pages per configured search; capped searches are reported")
    parser.add_argument("--delay", type=float, default=1, help="Global seconds between request starts, shared across workers")
    parser.add_argument("--workers", type=int, default=2, help="Concurrent downloads (1-4)")
    parser.add_argument("--discovery-every", type=int, default=5, help="Prefer a search page every N request starts")
    parser.add_argument("--transport", choices=("http", "browser"), default="http", help="HTTP server HTML (fast) or rendered browser HTML")
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--hours", type=float, default=8, help="Time budget for --overnight")
    parser.add_argument("--cooldown-minutes", type=float, default=30, help="Wait before one access recheck in --overnight")
    parser.add_argument("--max-access-checks", type=int, default=3, help="Stop after this many unsuccessful cooldown rechecks")
    args = parser.parse_args()
    if (min(args.limit, args.max_requests, args.pages, args.discovery_every) < 1 or args.delay < .5
            or args.timeout <= 0 or not 1 <= args.workers <= 4
            or args.hours <= 0 or args.cooldown_minutes < 5 or args.max_access_checks < 1):
        parser.error("Positive limits/timeout, delay >= 0.5 seconds and workers between 1 and 4 required")
    args.run = args.run or args.overnight
    if args.check_url:
        if not args.manual_check:
            parser.error("--check-url requires --manual-check")
        public_url(args.check_url)
    config = load_config(args.config)
    if config.get("date_from") or config.get("date_to"):
        parser.error("Date filters are not supported by this HTML collector")
    searches = build_searches(config, query=args.query, employer_id=args.employer_id)
    if not (args.run or args.export_only or args.import_snapshots or args.manual_check):
        print(encoded({"mode": "dry_run", "searches": searches, "output": str(args.output),
                       "limit": args.limit, "max_requests": args.max_requests, "pages": args.pages,
                       "transport": args.transport, "workers": args.workers, "start_interval_seconds": args.delay,
                       "discovery_every": args.discovery_every, "network_requests": 0}))
        return 0
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    db = connect(args.output / "queue.sqlite3")
    status, code, ready = "export_only", 0, False
    try:
        old_pages = db.execute("SELECT value FROM meta WHERE key='pages'").fetchone()
        if old_pages and int(old_pages[0]) != args.pages:
            raise ValueError("--pages differs from this checkpoint. Use a new --output folder.")
        previous_config_path = args.output / "config.json"
        previous_config = json.loads(previous_config_path.read_text(encoding="utf-8-sig")) if previous_config_path.exists() else None
        seed(db, config, searches, extend=args.extend_searches, previous_config=previous_config)
        # The paging ceiling affects the persisted queue and must not silently change.
        with db:
            db.execute("INSERT OR IGNORE INTO meta VALUES ('pages',?)", (str(args.pages),))
            db.execute("INSERT OR REPLACE INTO meta VALUES ('transport',?)", ("browser_session_http" if args.transport == "http" else "scrapling_browser",))
        ready = True
        (args.output / "config.json").write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
        if args.manual_check:
            status = asyncio.run(manual_check(db, args))
        elif args.import_snapshots:
            count = import_snapshots(db, args.import_snapshots, args.output, config, args.pages)
            print(f"Imported {count} snapshots without network requests.")
            status = "snapshot_import"
        elif args.run:
            if stats(db)["full_cards"] >= args.limit:
                status = "limit_reached"
            elif not db.execute("SELECT 1 FROM tasks WHERE state='pending' LIMIT 1").fetchone():
                status = "queue_exhausted"
            else:
                status = asyncio.run(run_overnight(db, config, args) if args.overnight else run_live(db, config, args))
                if status == "access_check_required":
                    code = 1
    except KeyboardInterrupt:
        status, code = "interrupted", 130
        print("Interrupted. Committed progress is saved; repeat the same command to resume.")
    except AccessCheckRequired as exc:
        print(f"HH requires a manual access check: {exc}")
        print("Run: .\\.venv\\Scripts\\python.exe hh_bulk.py --manual-check")
        status, code = "access_check_required", 1
    except Exception as exc:
        # Scope mismatch must not re-export existing records under a different config.
        print(f"Collection stopped: {exc}")
        if not ready:
            db.close()
            return 1
        status, code = "error", 1
    try:
        manifest = export(db, args.output, config, status)
        print(encoded(manifest))
        print(f"Checkpoint and exports: {args.output}")
    except OSError as exc:
        print(f"Export could not finish: {exc}. SQLite progress is saved. "
              "Check file access/free space and run hh_bulk.py --export-only.")
        if not code:
            code = 2
    finally:
        db.close()
    return code


if __name__ == "__main__":
    raise SystemExit(main())

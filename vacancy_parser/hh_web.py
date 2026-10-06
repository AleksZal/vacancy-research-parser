"""Collect public HH HTML pages, or parse saved pages without network access."""

import argparse
import getpass
import html
import json
import os
import re
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from scrapling import Selector

from .collect import USER_AGENT, fetch, hh_record, text, utc_now, write_csv, write_outputs
from .hh_pipeline import build_searches, classify, export_record, load_config, write_extra_outputs

from .paths import CONFIG_DIR, PROJECT_ROOT

ROOT = PROJECT_ROOT


def public_url(url):
    parts = urllib.parse.urlsplit(url)
    hostname = parts.hostname or ""
    if (parts.scheme != "https" or not (hostname == "hh.ru" or hostname.endswith(".hh.ru"))
            or parts.username or parts.password or parts.port not in (None, 443)):
        raise ValueError("Only public HTTPS HH pages are supported")
    if parts.path != "/search/vacancy" and not re.fullmatch(r"/vacancy/\d+", parts.path):
        raise ValueError("Only vacancy search and numeric vacancy URLs are supported")
    return url


def search_url(params):
    # Keep documented UI filters; never reuse internal search/control flags.
    fields = {k: v for k, v in params.items() if k in {"area", "employer_id", "text", "page", "search_field"}}
    fields["items_on_page"] = params.get("per_page", 20)
    return "https://hh.ru/search/vacancy?" + urllib.parse.urlencode(fields, doseq=True)


def page_state(content):
    # HH's anonymous page state is rendered alongside the HTML. Firecrawl can
    # remove its enclosing script tag, so parse the JSON itself, without eval.
    for match in re.finditer(r'\{\s*"redirectConfig"\s*:', content):
        try:
            state, _ = json.JSONDecoder().raw_decode(content[match.start():])
        except ValueError:
            continue
        if isinstance(state, dict):
            return state
    # Unexecuted server HTML stores the same public state in an escaped
    # template. lxml decodes its HTML entities once; no JavaScript is executed.
    for node in Selector(content).css('template#HH-Lux-InitialState'):
        try:
            state = json.loads("".join(node.xpath("text()").getall()))
        except ValueError:
            continue
        if isinstance(state, dict) and isinstance(state.get("redirectConfig"), dict):
            return state
    return {}


def plain(value):
    return re.sub(r"\s+", " ", text(html.unescape(value or ""))).strip()


def dom_text(selector, css):
    nodes = selector.css(css)
    return plain(" ".join(" ".join(n.xpath(".//text()").getall()) for n in nodes))


def section_texts(description):
    """Extract only explicitly labelled sections; keep the full text as well."""
    selector = Selector("<div>" + description + "</div>")
    result = {"responsibilities": [], "requirements": [], "conditions": []}
    labels = {"обязанности": "responsibilities", "требования": "requirements",
              "условия": "conditions", "преимущества": "conditions"}
    active = None
    for node in selector.xpath("//body/div/*"):
        value = plain(node.get())
        label = value.casefold().strip(" :.;\u00a0")
        if label in labels:
            active = labels[label]
        elif active and value:
            result[active].append(value)
    return {k: "\n".join(v) for k, v in result.items()}


def normalize_item(item, *, detail):
    company = item.get("company") or {}
    compensation = item.get("compensation") or {}
    area = item.get("area") or {}
    address = item.get("address") or {}
    contacts = item.get("contactInfo") or {}
    # Hidden contacts and manager/account metadata are never used.
    if contacts.get("contactsHidden") or contacts.get("@contactsHidden"):
        contacts = {}
    keys = item.get("keySkills") or []
    return {
        "id": item.get("vacancyId"), "name": item.get("name"),
        "description": html.unescape(item.get("description") or "") if detail else "",
        "employer": {"id": company.get("id"), "name": company.get("visibleName") or company.get("name")},
        "area": {"name": area.get("name")},
        "address": {"raw": address.get("displayName") or ""},
        "salary": {"from": compensation.get("from"), "to": compensation.get("to"),
                   "currency": "RUB" if compensation.get("currencyCode") == "RUR" else compensation.get("currencyCode"),
                   "gross": compensation.get("gross")},
        "published_at": item.get("publicationTimeIso") or (item.get("publicationTime") or {}).get("$"),
        "key_skills": [{"name": v if isinstance(v, str) else v.get("name", "")} for v in keys if isinstance(v, (str, dict))],
        "contacts": {"name": contacts.get("fullName") or contacts.get("name"), "email": contacts.get("email")},
    }


def parse_search(content, url, observed_at):
    public_url(url)
    result = page_state(content).get("vacancySearchResult")
    if not isinstance(result, dict) or not isinstance(result.get("vacancies"), list):
        raise ValueError("HH search HTML has no vacancy list; possible challenge or markup change")
    params = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
    route = "employer:" + params["employer_id"][0] if params.get("employer_id") else "query:" + params.get("text", [""])[0]
    records = []
    for item in result["vacancies"]:
        if not str(item.get("vacancyId", "")).isdigit():
            raise ValueError("HH search item has no numeric vacancy ID")
        record = hh_record(normalize_item(item, detail=False), observed_at)
        record.update({"source": "hh_html", "source_url": "https://hh.ru/vacancy/" + record["source_vacancy_id"],
                       "detail_url": "https://hh.ru/vacancy/" + record["source_vacancy_id"],
                       "detail_status": "listed_only", "discovery_route": route,
                       "filter_decision": "pending_details", "vpk_status": "unverified"})
        records.append(record)
    paging = result.get("paging") or {}
    return records, {"url": url, "route": route, "found": result.get("totalResults"),
                     "listed_on_page": len(records),
                     "has_next": not (paging.get("next") or {}).get("disabled", True)}


def parse_detail(content, url, observed_at):
    public_url(url)
    expected = urllib.parse.urlsplit(url).path.rsplit("/", 1)[-1]
    selector = Selector(content)
    item = ((page_state(content).get("vacancyView") or {}).get("vacancyFull") or {}).get("vacancy")
    if not isinstance(item, dict) or not item.get("description"):
        raise ValueError("HH detail HTML has no public full description; possible challenge or markup change")
    if str(item.get("vacancyId")) != expected:
        raise ValueError("HH HTML vacancy ID differs from requested ID")
    status = item.get("status") or {}
    if status.get("disabled") or status.get("hiddenInArchive"):
        raise ValueError("HH vacancy is no longer publicly available")
    normalized = normalize_item(item, detail=True)
    record = hh_record(normalized, observed_at)
    if not record["title"] or not record["description"]:
        raise ValueError("HH detail has an empty title or description")
    salary = item.get("compensation") or {}
    record.update({"source": "hh_html", "source_url": url, "detail_url": url,
                   "salary_period": salary.get("mode") or "unspecified_by_source",
                   "experience": dom_text(selector, '[data-qa="vacancy-experience"]') or item.get("workExperience") or "",
                   "schedule": dom_text(selector, '[data-qa="work-schedule-by-days-text"]') or "; ".join(item.get("workScheduleByDays") or []),
                   "source_visibility": "archived_public_page" if status.get("archived") else "public_at_collection_time"})
    record.update(section_texts(normalized["description"]))
    return record


def classify_record(record, config, route):
    decision, matches = classify(record, config)
    if decision == "excluded_no_signal" and route.startswith(("gur_name:", "gur_focus:")):
        decision, matches = "review_gur_employer_search", ["Found by GUR company-name search; legal-entity identity unverified", route]
    record.update({"discovery_route": route, "filter_decision": decision,
                   "filter_matches": "; ".join(matches), "vpk_status": "unverified",
                   "employer_resolution_status": "hh_profile_identified" if record["employer_source_id"] else "unresolved",
                   "vpk_evidence": "Candidate filter: " + decision + "; " + "; ".join(matches)})
    return export_record(record, config)


def download(url, path, transport, timeout):
    public_url(url)
    if transport == "direct":
        content = fetch(url, path, timeout=timeout, retry_transient=False).decode("utf-8-sig")
        metadata = {"sourceURL": url, "url": url, "statusCode": 200, "transport": "direct"}
    else:
        key = (os.environ.get("FIRECRAWL_API_KEY") or "").strip()
        if not key:
            raise ValueError("Set FIRECRAWL_API_KEY locally, or collect via the connected Firecrawl plugin and use --snapshots")
        payload = {"url": url, "formats": ["rawHtml"], "proxy": "basic", "maxAge": 0,
                   "timeout": int(timeout * 1000)}
        request = urllib.request.Request("https://api.firecrawl.dev/v2/scrape", data=json.dumps(payload).encode(),
                                         headers={"Content-Type": "application/json", "User-Agent": USER_AGENT})
        request.add_unredirected_header("Authorization", "Bearer " + key)
        try:
            with urllib.request.urlopen(request, timeout=timeout + 15) as response:
                envelope = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            # Keep secrets out of stderr and snapshots. No retries or proxy escalation.
            raise ValueError(f"Firecrawl returned HTTP {error.code}; request stopped") from None
        if envelope.get("success") is not True:
            raise ValueError("Firecrawl did not return a successful scrape; request stopped")
        data = envelope.get("data") or {}
        metadata = dict(data.get("metadata") or {}, transport="firecrawl")
        if metadata.get("statusCode") != 200 or not data.get("rawHtml"):
            raise ValueError("Firecrawl returned no successful HTML page; request stopped")
        public_url(metadata.get("url") or url)
        content = data["rawHtml"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    metadata["observed_at"] = utc_now()
    path.with_suffix(".metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return content, metadata


def snapshots(folder, output, config, manifest, index, details):
    pending = []
    for path in sorted(folder.glob("*.html")):
        meta_path = path.with_suffix(".metadata.json")
        if not meta_path.exists():
            continue
        meta = json.loads(meta_path.read_text(encoding="utf-8-sig"))
        url = meta.get("sourceURL") or meta.get("url")
        public_url(url)
        if meta.get("statusCode") != 200:
            raise ValueError("Saved HH page has a non-success HTTP status")
        content = path.read_text(encoding="utf-8-sig")
        observed = meta.get("observed_at") or manifest["started_at"]
        for source in (path, meta_path):
            shutil.copy2(source, output / "raw" / source.name)
        if urllib.parse.urlsplit(url).path == "/search/vacancy":
            records, report = parse_search(content, url, observed)
            index.extend(records)
            manifest["searches"].append(report)
        else:
            pending.append(parse_detail(content, url, observed))
        manifest["source_pages"] += 1
        manifest["source_firecrawl_credits_reported"] += meta.get("creditsUsed") or 0
    routes = {r["source_vacancy_id"]: r["discovery_route"] for r in index}
    for record in pending:
        details.append(classify_record(record, config, routes.get(record["source_vacancy_id"], "saved_detail")))
    manifest["stop_reason"] = "saved_pages_processed"


def live(args, config, searches, output, manifest, index, details):
    seen = set()
    for i, search in enumerate(searches):
        for page in range(args.pages):
            time.sleep(args.delay)
            url = search_url(dict(search["params"], page=page))
            content, meta = download(url, output / "raw" / f"search_{i}_page_{page}.html", args.transport, args.timeout)
            manifest["source_pages"] += 1
            manifest["firecrawl_credits_reported"] += meta.get("creditsUsed") or 0
            rows, report = parse_search(content, url, meta["observed_at"])
            manifest["searches"].append(report)
            index.extend(rows)
            for row in rows:
                vid = row["source_vacancy_id"]
                if vid in seen:
                    continue
                if len(seen) >= args.limit:
                    manifest["stop_reason"] = "detail_limit_reached"
                    return
                seen.add(vid)
                time.sleep(args.delay)
                content, meta = download(row["source_url"], output / "raw" / f"detail_{vid}.html", args.transport, args.timeout)
                manifest["source_pages"] += 1
                manifest["firecrawl_credits_reported"] += meta.get("creditsUsed") or 0
                record = parse_detail(content, row["source_url"], meta["observed_at"])
                details.append(classify_record(record, config, row["discovery_route"]))
                print(f"[{len(details)}] {details[-1]['filter_decision']}: {record['title']}", flush=True)
            if not report["has_next"]:
                break
    manifest["stop_reason"] = "configured_pages_processed"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--run", action="store_true", help="Start live public-HTML collection")
    mode.add_argument("--snapshots", type=Path, help="Parse saved HTML and adjacent metadata files; no network")
    parser.add_argument("--transport", choices=["firecrawl", "direct"], default="firecrawl")
    parser.add_argument("--ask-firecrawl-key", action="store_true", help="Prompt privately for a Firecrawl API key in this process only")
    parser.add_argument("--config", type=Path, default=CONFIG_DIR / "hh_config.json")
    parser.add_argument("--query")
    parser.add_argument("--employer-id")
    parser.add_argument("--limit", type=int, default=10, help="Live: maximum unique full cards, including review/excluded")
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--delay", type=float, default=2)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.limit < 1 or args.pages < 1 or args.delay < 1 or not 1 <= args.timeout <= 120:
        parser.error("limit/pages >= 1, delay >= 1, timeout between 1 and 120 are required")
    try:
        config = load_config(args.config)
        if args.run and (config.get("date_from") or config.get("date_to")):
            raise ValueError("HTML collector does not support date_from/date_to yet; use a separate config with these fields null")
        searches = build_searches(config, query=args.query, employer_id=args.employer_id)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    if not args.run and args.snapshots is None:
        print(json.dumps({"mode": "dry_run", "network_requests": 0, "transport": args.transport,
                          "max_details": args.limit, "max_pages_per_search": args.pages,
                          "search_urls": [search_url(s["params"]) for s in searches],
                          "live_requires": "--run; firecrawl transport also needs FIRECRAWL_API_KEY",
                          "offline_example": "--snapshots data/hh_web_pilot/raw"}, ensure_ascii=False, indent=2))
        return 0
    if args.ask_firecrawl_key:
        if not args.run or args.transport != "firecrawl":
            parser.error("--ask-firecrawl-key requires --run with firecrawl transport")
        os.environ["FIRECRAWL_API_KEY"] = getpass.getpass("Firecrawl API key (hidden): ").strip()
    if args.run and args.transport == "firecrawl" and not (os.environ.get("FIRECRAWL_API_KEY") or "").strip():
        parser.error("Live Firecrawl collection needs your FIRECRAWL_API_KEY; HH client keys are not needed")
    if args.snapshots is not None and not args.snapshots.is_dir():
        parser.error("Snapshot directory does not exist")
    output = args.output or ROOT / "data" / ("hh_html_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    try:
        output.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        parser.error("Choose a new output directory; existing snapshots are preserved")
    (output / "raw").mkdir()
    manifest = {"source": "hh_html", "started_at": utc_now(), "errors": [], "searches": [],
                "transport": "saved_html" if args.snapshots else args.transport, "source_pages": 0,
                "firecrawl_credits_reported": 0, "source_firecrawl_credits_reported": 0,
                "mode": "snapshot_parse" if args.snapshots else "live",
                "coverage": "Bounded public vacancy pages; not all HH or verified defence companies",
                "parameters": {"limit": args.limit, "pages": args.pages, "query": args.query, "employer_id": args.employer_id}}
    index, details = [], []
    try:
        if args.snapshots:
            snapshots(args.snapshots, output, config, manifest, index, details)
        else:
            live(args, config, searches, output, manifest, index, details)
    except (OSError, ValueError, TypeError, KeyError) as error:
        message = str(error)
        for key in ("HH_ACCESS_TOKEN", "FIRECRAWL_API_KEY"):
            secret = (os.environ.get(key) or "").strip()
            if secret:
                message = message.replace(secret, "[REDACTED]")
        manifest["errors"].append({"error": message})
        manifest["stop_reason"] = "error"
        print(f"HTML collection stopped: {message}", file=sys.stderr)
    index = list({r["source_vacancy_id"]: export_record(r, config) for r in index}.values())
    details = list({r["source_vacancy_id"]: r for r in details}.values())
    candidates = [r for r in details if r["filter_decision"].startswith("candidate_")]
    review = [r for r in details if r["filter_decision"].startswith("review_")]
    excluded = [r for r in details if r["filter_decision"].startswith("excluded_")]
    (output / "index.jsonl").write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in index), encoding="utf-8")
    write_csv(output / "index.csv", index)
    write_extra_outputs(output, review, excluded)
    manifest.update({"index_postings": len(index), "review_postings": len(review), "excluded_postings": len(excluded),
                     "full_cards_collected": len(details)})
    write_outputs(output, candidates, manifest)
    print(f"Saved {len(candidates)} candidates, {len(review)} review, {len(excluded)} excluded, {len(index)} index rows to {output.resolve()}")
    return 1 if manifest["errors"] or not details else 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    raise SystemExit(main())

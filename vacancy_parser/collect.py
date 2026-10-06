"""Small, repeatable public-vacancy collectors. Run with --help."""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from .paths import CONFIG_DIR, PROJECT_ROOT

from scrapling import Selector

FKC_BASE = "https://www.fkc-opk.ru"
FKC_PAGE = FKC_BASE + "/trudoustrojstvo-v-opk/vacancy"
FKC_LIST = FKC_BASE + "/PublicResumeUser/AjAllGetVacancysList"
FKC_DETAIL = FKC_BASE + "/PublicResumeUser/AJGetVacancyMoreDetailed"
USER_AGENT = "VacancyResearchPilot/0.1 (public job-posting research)"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def text(value):
    """Turn source HTML into plain text, retaining text from nested elements."""
    if value is None:
        return ""
    value = str(value)
    if re.search(r"</?[a-zA-Z][^>]*>", value):
        value = " ".join(Selector(value).xpath("//text()").getall())
    return re.sub(r"[\u200b-\u200d\ufeff]", "", value).strip()


def number(value):
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(" ", "").replace(",", "."))
    except ValueError:
        return None


def redact_hh_token(value):
    token = (os.environ.get("HH_ACCESS_TOKEN") or "").strip()
    return value.replace(token, "[REDACTED]") if token else value


def error_info(error):
    info = {"error": redact_hh_token(str(error))}
    info.update(getattr(error, "diagnostics", {}))
    return info


def save_http_error(error, url, raw_path):
    """Keep a bounded server reply, without request headers or credentials."""
    content = error.read(65537)
    body = redact_hh_token(content[:65536].decode("utf-8", errors="replace"))
    try:
        parsed = json.loads(body)
    except ValueError:
        parsed = None
    server_errors = []
    if isinstance(parsed, dict) and isinstance(parsed.get("errors"), list):
        for item in parsed["errors"]:
            if isinstance(item, dict):
                server_errors.append({key: str(item[key])[:300]
                                      for key in ("type", "value", "reason") if key in item})
    values = {item.get("value") for item in server_errors}
    types = {item.get("type") for item in server_errors}
    if "captcha_required" in values:
        hint = "HH requests a CAPTCHA. Collection stopped; no automatic retry."
    elif "oauth" in types:
        hint = "HH returned an authorization error; inspect server_errors."
    elif "bad_user_agent" in types:
        hint = "HH rejected the application User-Agent; inspect server_errors."
    elif "forbidden" in types and urllib.parse.urlsplit(url).hostname == "api.hh.ru":
        if (os.environ.get("HH_ACCESS_TOKEN") or "").strip():
            hint = "HH denied an authenticated request. Check application access with HH; exact cause is not specified."
        else:
            hint = "HH denied an anonymous request. Next step: test with a valid HH_ACCESS_TOKEN from a registered application; exact cause is not specified."
    elif error.code == 429:
        hint = "Request rate limit reached. Collection stopped."
    else:
        hint = "Exact cause is not established; inspect the saved server response."
    error_path = raw_path.with_name(raw_path.name + ".error.json")
    error_path.parent.mkdir(parents=True, exist_ok=True)
    error.diagnostics = {"http_status": error.code, "request_url": url,
                         "server_errors": server_errors, "hint": hint,
                         "diagnostic_file": str(error_path.resolve())}
    if isinstance(parsed, dict) and parsed.get("request_id"):
        error.diagnostics["request_id"] = str(parsed["request_id"])[:300]
    saved = dict(error.diagnostics, response_body=body, truncated=len(content) > 65536)
    error_path.write_text(json.dumps(saved, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch(url, raw_path, *, payload=None, timeout=25, save_success=True, retry_transient=True):
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json, text/html"}
    target = urllib.parse.urlsplit(url)
    is_hh = target.scheme == "https" and target.hostname == "api.hh.ru"
    if is_hh:
        headers["User-Agent"] = os.environ.get("HH_USER_AGENT") or USER_AGENT
    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers.update({"Content-Type": "application/json", "Referer": FKC_PAGE})
    request = urllib.request.Request(url, data=body, headers=headers)
    token = (os.environ.get("HH_ACCESS_TOKEN") or "").strip() if is_hh else ""
    if token:
        if not token.isascii() or any(char.isspace() for char in token):
            raise ValueError("HH_ACCESS_TOKEN must contain only the token, without Bearer or whitespace")
        # urllib omits unredirected headers when creating a redirected request.
        request.add_unredirected_header("Authorization", "Bearer " + token)
    # One retry on transient failures; do not retry access denials or rate limits.
    attempts = 2 if retry_transient else 1
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                content = response.read()
            if save_success:
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_bytes(content)
            return content
        except urllib.error.HTTPError as error:
            if error.code < 500 or attempt + 1 == attempts:
                save_http_error(error, url, raw_path)
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt + 1 == attempts:
                raise
        time.sleep(2)


def json_fetch(url, raw_path, **kwargs):
    return json.loads(fetch(url, raw_path, **kwargs).decode("utf-8-sig"))


def fkc_record(row, detail, observed_at):
    if not isinstance(row, list) or len(row) < 11:
        raise ValueError("FKC list schema changed: expected at least 11 columns")
    item = detail.get("data", [{}])[0] if detail else {}
    fields = item.get("VacancySend", {})
    if fields and str(fields.get("id")) != str(row[7]):
        raise ValueError("FKC detail ID differs from list ID")
    if str(fields.get("Hidden", 0)) != "0":
        raise ValueError("Vacancy is no longer publicly visible")
    employer_id = fields.get("OrgId")
    return {
        "source": "fkc_opk",
        "source_vacancy_id": str(row[7]),
        "source_url": FKC_PAGE,
        "detail_url": FKC_DETAIL + "?" + urllib.parse.urlencode({"id": row[7]}),
        "observed_at": observed_at,
        "published_at_source": text(row[6]),
        "publication_date_raw": fields.get("PublicationDate"),
        "source_visibility": "listed_at_collection_time",
        "detail_status": "collected" if fields else "not_collected",
        "title": text(fields.get("Name") or row[0]),
        "employer_name": text(row[3]),
        "employer_name_detail": text(fields.get("Company")),
        "employer_source_id": str(employer_id) if employer_id is not None else "",
        "employer_id_namespace": "fkc_opk:OrgId" if employer_id is not None else "",
        "region": text(item.get("RegionTitle") or row[8]),
        "industry_label_source": text(row[9]),
        "address": text(fields.get("Address") or row[4]),
        "address_type": "unspecified_by_source",
        "salary_from": number(fields.get("FromSalary", row[1])),
        "salary_to": number(fields.get("ToSalary", row[2])),
        "salary_currency": "RUB",
        "salary_period": "unspecified_by_source",
        "salary_gross": None,
        "experience": text(fields.get("Experience") or row[5]),
        "responsibilities": text(fields.get("Responsibilities")),
        "requirements": text(fields.get("Requirements")),
        # The source uses Cyrillic capital C in this property name.
        "conditions": text(fields.get("Working\u0421onditions") or fields.get("WorkingConditions")),
        "education": text(fields.get("Education")),
        "skills": text(fields.get("Skills")),
        "schedule": text(fields.get("Schedule")),
        "public_business_contact_name": text(fields.get("ContactPerson")),
        "public_business_phone": text(fields.get("Phone")),
        "vpk_status": "unverified",
        "vpk_evidence": "Listed on the FKC OPK public vacancy portal; independent verification pending",
    }


def collect_fkc(args, output, manifest, records):
    page = fetch(FKC_PAGE, output / "raw" / "page.html", timeout=args.timeout)
    selector = Selector(page.decode("utf-8"))
    manifest["page_title"] = selector.css("title::text").get()
    listing = json_fetch(FKC_LIST, output / "raw" / "list.json", payload={}, timeout=args.timeout)
    if listing.get("success") is False or not isinstance(listing.get("data"), list):
        raise ValueError("FKC response does not contain a successful vacancy list")
    rows = listing["data"]
    manifest["source_list_count"] = len(rows)
    selected = rows[: args.limit] if args.limit else rows
    seen = set()
    for row in selected:
        # Validate list structure before constructing any detail request.
        base = fkc_record(row, {}, manifest["started_at"])
        vid = base["source_vacancy_id"]
        if vid in seen:
            continue
        seen.add(vid)
        if args.details:
            time.sleep(args.delay)
            url = base["detail_url"]
            try:
                detail = json_fetch(url, output / "raw" / f"detail_{vid}.json", timeout=args.timeout)
                if not detail.get("data"):
                    raise ValueError("Detail has no data")
                base = fkc_record(row, detail, utc_now())
            except (urllib.error.URLError, TimeoutError, ValueError, IndexError, TypeError) as error:
                manifest["errors"].append(dict(error_info(error), id=vid))
                base["detail_status"] = "failed"
                if isinstance(error, urllib.error.HTTPError) and error.code in (401, 403, 429):
                    records.append(base)
                    print(f"Stopping after HTTP {error.code}; see manifest.json", flush=True)
                    break
        records.append(base)
        if args.details or len(selected) <= 20 or len(records) % 50 == 0 or len(records) == len(selected):
            print(f"[{len(records)}/{len(selected)}] {base['title']}", flush=True)
    return records


def hh_record(item, observed_at):
    employer = item.get("employer") or {}
    salary = item.get("salary") or {}
    address = item.get("address") or {}
    contacts = item.get("contacts") or {}
    phones = []
    for phone in contacts.get("phones") or []:
        phones.append(phone.get("formatted") or " ".join(str(phone[k]) for k in ("country", "city", "number") if phone.get(k)))
    return {
        "source": "hh", "source_vacancy_id": str(item["id"]),
        "source_url": item.get("alternate_url", ""),
        "detail_url": item.get("url", ""), "observed_at": observed_at,
        "published_at_source": item.get("published_at", ""),
        "source_visibility": "listed_at_collection_time", "detail_status": "collected",
        "title": item.get("name", ""), "employer_name": employer.get("name", ""),
        "employer_name_detail": employer.get("name", ""),
        "employer_source_id": str(employer["id"]) if employer.get("id") else "",
        "employer_id_namespace": "hh:employer_id" if employer.get("id") else "",
        "employer_profile_url": employer.get("alternate_url", ""),
        "region": (item.get("area") or {}).get("name", ""),
        "industry_label_source": "", "address": address.get("raw", ""),
        "address_type": "unspecified_by_source",
        "salary_from": salary.get("from"), "salary_to": salary.get("to"),
        "salary_currency": salary.get("currency", ""), "salary_gross": salary.get("gross"),
        "salary_period": "see_original_source",
        "experience": (item.get("experience") or {}).get("name", ""),
        "responsibilities": "", "requirements": "", "conditions": "", "education": "",
        "description": text(item.get("description")),
        "skills": ", ".join(s.get("name", "") for s in item.get("key_skills", [])),
        "schedule": (item.get("schedule") or {}).get("name", ""),
        "public_business_contact_name": contacts.get("name") or "",
        "public_business_phone": "; ".join(phones),
        "public_business_email": contacts.get("email") or "",
        "vpk_status": "unverified", "vpk_evidence": "Keyword search is not evidence of VPK affiliation",
    }


def csv_safe(value):
    # Excel must display source text instead of interpreting it as a formula.
    if isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@")):
        return "'" + value
    return value


def write_csv(path, rows):
    if not rows:
        return
    columns = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows({key: csv_safe(value) for key, value in row.items()} for row in rows)


def write_outputs(output, records, manifest):
    with (output / "vacancies.jsonl").open("w", encoding="utf-8") as stream:
        for record in records:
            stream.write(json.dumps(record, ensure_ascii=False) + "\n")
    write_csv(output / "vacancies.csv", records)
    counts = Counter((r["source"], r["employer_name"]) for r in records)
    write_csv(output / "employers.csv", [
        {"source": source, "employer_name": name, "collected_postings": count,
         "identity_status": "source_name_only_not_resolved_legal_entity", "vpk_status": "unverified"}
        for (source, name), count in counts.most_common()
    ])
    manifest.update({"finished_at": utc_now(), "collected_postings": len(records),
                     "distinct_employer_names": len(counts),
                     "details_collected": sum(r["detail_status"] == "collected" for r in records),
                     "raw_sha256": {str(p.relative_to(output)): hashlib.sha256(p.read_bytes()).hexdigest()
                                    for p in sorted((output / "raw").glob("*")) if p.is_file()}})
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def check_hh_access(args, searches, output, manifest):
    from .hh_pipeline import search_url

    params = dict(searches[0]["params"], per_page=1, page=0)
    url = search_url(params)
    manifest.update({"mode": "access_check", "request_url": url,
                     "vacancies_saved": 0, "request_attempts": 1})
    try:
        # Test the same search route that collection starts with. Never fetch details.
        result = json_fetch(url, output / "raw" / "access_check.json", timeout=args.timeout,
                            save_success=False, retry_transient=False)
        if not isinstance(result, dict) or not isinstance(result.get("items"), list):
            raise ValueError("Response is not an HH vacancy search result")
        manifest["access_ok"] = True
        print("HH access check OK. No vacancies saved; collection was not started.")
    except (urllib.error.URLError, TimeoutError, ValueError, TypeError) as error:
        manifest["access_ok"] = False
        manifest["errors"].append(error_info(error))
        print(f"HH access check failed: {redact_hh_token(str(error))}", file=sys.stderr)
        if getattr(error, "diagnostics", None):
            print(json.dumps(error.diagnostics, ensure_ascii=False, indent=2), file=sys.stderr)
    manifest["finished_at"] = utc_now()
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Diagnostic report: {output.resolve() / 'manifest.json'}")
    return 0 if manifest["access_ok"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", choices=["fkc", "hh"])
    parser.add_argument("--limit", type=int, help="FKC postings or HH detail-attempt limit; 0 = no count cap")
    parser.add_argument("--details", action="store_true", help="FKC: fetch each selected posting's full description")
    parser.add_argument("--query", help="HH: override configured keyword searches")
    parser.add_argument("--employer-id", help="HH employer ID; combined with --query")
    parser.add_argument("--pages", type=int, help="HH: maximum pages per configured search")
    parser.add_argument("--delay", type=float, help="Seconds between requests; minimum 0.5")
    parser.add_argument("--config", type=Path, default=CONFIG_DIR / "hh_config.json", help="HH configuration file")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="HH: show plan without any network requests (default)")
    mode.add_argument("--run", action="store_true", help="HH: explicitly start network collection")
    mode.add_argument("--check-access", action="store_true", help="HH: one diagnostic search request; no vacancy export or detail requests")
    parser.add_argument("--timeout", type=float, default=25)
    parser.add_argument("--output", type=Path, help="New output directory (existing directories are rejected)")
    args = parser.parse_args()
    config = None
    searches = []
    if args.source == "hh":
        from .hh_pipeline import build_searches, load_config, plan
        try:
            config = load_config(args.config)
            searches = build_searches(config, query=args.query, employer_id=args.employer_id)
        except (OSError, ValueError, TypeError, KeyError, re.error) as error:
            parser.error(str(error))
        args.limit = config["max_details"] if args.limit is None else args.limit
        args.pages = config["max_pages_per_search"] if args.pages is None else args.pages
        args.delay = config["request_delay_seconds"] if args.delay is None else args.delay
    else:
        if args.dry_run or args.run or args.check_access:
            parser.error("--dry-run, --run and --check-access apply only to HH")
        args.limit = 20 if args.limit is None else args.limit
        args.pages = 1 if args.pages is None else args.pages
        args.delay = 1.0 if args.delay is None else args.delay
    if args.limit < 0 or args.pages < 1 or args.delay < 0.5 or args.timeout <= 0:
        parser.error("limit >= 0, pages >= 1, delay >= 0.5, timeout > 0 are required")
    if args.source == "hh" and not (args.run or args.check_access):
        planned = plan(config, searches, args.limit, args.pages)
        planned["delay_seconds"] = args.delay
        print(json.dumps(planned, ensure_ascii=False, indent=2))
        return 0
    prefix = "hh_check" if args.check_access else args.source
    output = args.output or PROJECT_ROOT / "data" / (prefix + "_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    try:
        output.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        parser.error("Output directory already exists; choose a new directory to preserve snapshots")
    manifest = {"source": args.source, "started_at": utc_now(), "errors": [], "parameters": {
        "limit": args.limit, "details": args.details, "query": args.query,
        "employer_id": args.employer_id, "pages": args.pages, "delay": args.delay,
    }}
    records = []
    review, excluded = [], []
    if config is not None:
        manifest["hh_config"] = config
        manifest["hh_access_token_configured"] = bool((os.environ.get("HH_ACCESS_TOKEN") or "").strip())
        manifest["coverage"] = "Bounded initial candidate search; not an exhaustive HH or VPK company database"
        (output / "hh_config.json").write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.check_access:
        return check_hh_access(args, searches, output, manifest)
    try:
        if args.source == "fkc":
            collect_fkc(args, output, manifest, records)
        else:
            from .hh_pipeline import run
            run(config, searches, args, output, manifest, records, review, excluded)
    except (urllib.error.URLError, TimeoutError, ValueError, KeyError, TypeError, IndexError) as error:
        manifest["errors"].append(error_info(error))
        print(f"Collection failed: {redact_hh_token(str(error))}", file=sys.stderr)
        if getattr(error, "diagnostics", None):
            print(json.dumps(error.diagnostics, ensure_ascii=False, indent=2), file=sys.stderr)
        if args.source == "hh":
            manifest["stop_reason"] = "error"
    if args.source == "hh":
        from .hh_pipeline import write_extra_outputs
        manifest.update({"review_postings": len(review), "excluded_postings": len(excluded)})
        write_extra_outputs(output, review, excluded)
    write_outputs(output, records, manifest)
    print(f"Saved {len(records)} postings to {output.resolve()}")
    return 1 if manifest["errors"] or not (records or review or excluded) else 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    raise SystemExit(main())

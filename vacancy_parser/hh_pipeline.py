"""Offline HH search planning and evidence-based candidate filtering."""

import json
import re
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

from .paths import resolve_config_path

ACRONYMS = {"опк", "впк", "гоз", "бпла", "фз", "рэб", "крэт", "каб", "умпк", "умпб", "fpv", "бпак"}
SUPPORTED_FIELDS = {
    "source", "source_vacancy_id", "source_url", "detail_url", "observed_at",
    "published_at_source", "detail_status", "source_visibility", "title",
    "employer_name", "employer_name_detail", "employer_source_id", "employer_id_namespace",
    "employer_profile_url", "employer_resolution_status", "region", "address", "address_type",
    "salary_from", "salary_to", "salary_currency", "salary_period", "salary_gross",
    "experience", "description", "responsibilities", "requirements", "conditions",
    "education", "skills", "schedule", "public_business_contact_name",
    "public_business_phone", "public_business_email", "filter_decision", "filter_matches",
    "discovery_route", "vpk_status", "vpk_evidence",
}
REQUIRED_FIELDS = {
    "source", "source_vacancy_id", "source_url", "title", "employer_name",
    "detail_status", "filter_decision", "filter_matches", "vpk_status",
}


def normalized(value):
    return re.sub(r"\s+", " ", (value or "").casefold().replace("ё", "е")).strip()


def load_config(path):
    config = json.loads(resolve_config_path(path).read_text(encoding="utf-8-sig"))
    for key in ("per_page", "max_pages_per_search", "max_details"):
        if type(config.get(key)) is not int or config[key] < 1:
            raise ValueError(f"{key} must be a positive integer")
    if config["per_page"] > 100:
        raise ValueError("per_page cannot exceed 100")
    if not isinstance(config.get("request_delay_seconds"), (int, float)) or config["request_delay_seconds"] < 0.5:
        raise ValueError("request_delay_seconds must be at least 0.5")
    if not config.get("area_ids") or any(not str(x).isdigit() for x in config["area_ids"]):
        raise ValueError("area_ids must contain numeric source area IDs")
    if any(not isinstance(x, str) or not x.strip() for x in config.get("search_queries", [])):
        raise ValueError("search_queries must contain nonempty strings")
    employers = config.get("employers", [])
    if any(not str(e.get("id", "")).isdigit() for e in employers):
        raise ValueError("employers must contain numeric HH employer IDs")
    if len({str(e["id"]) for e in employers}) != len(employers):
        raise ValueError("Duplicate employer IDs in config")
    candidates = config.get("candidate_employers", [])
    if not isinstance(candidates, list) or any(not isinstance(e, dict) or not str(e.get("id", "")).isdigit() for e in candidates):
        raise ValueError("candidate_employers must contain numeric HH IDs")
    if len({str(e["id"]) for e in candidates}) != len(candidates):
        raise ValueError("Duplicate candidate employer IDs")
    extras = config.get("additional_searches", [])
    if not isinstance(extras, list):
        raise ValueError("additional_searches must be a list")
    routes = set()
    for search in extras:
        if not isinstance(search, dict) or not isinstance(search.get("route"), str) or not search["route"].strip():
            raise ValueError("Each additional search needs a route")
        if search["route"] in routes:
            raise ValueError("Duplicate additional search route")
        routes.add(search["route"])
        params = search.get("params")
        if not isinstance(params, dict) or set(params) - {"area", "employer_id", "text", "search_field"}:
            raise ValueError("Additional searches accept only public area/employer/text/search_field filters")
        if "area" in params and (not isinstance(params["area"], list) or not params["area"] or any(not str(a).isdigit() for a in params["area"])):
            raise ValueError("Additional search area must contain numeric IDs")
        if "employer_id" in params and not str(params["employer_id"]).isdigit():
            raise ValueError("Additional employer ID must be numeric")
        if "text" in params and (not isinstance(params["text"], str) or not params["text"].strip()):
            raise ValueError("Additional text must be nonempty")
        if not params.get("text") and not params.get("employer_id"):
            raise ValueError("Additional searches must retain a company or text restriction")
        if "search_field" in params and (not isinstance(params["search_field"], list) or not params["search_field"] or set(params["search_field"]) - {"name", "company_name", "description"}):
            raise ValueError("Unsupported public search_field")
    if not config.get("search_queries") and not employers:
        raise ValueError("Configure at least one search phrase or employer")
    for key in ("strong_signal_groups", "weak_signal_groups"):
        groups = config.get(key)
        if not isinstance(groups, list) or any(not isinstance(g, list) or not g or any(not isinstance(t, str) or not t.strip() for t in g) for g in groups):
            raise ValueError(f"{key} must contain nonempty groups of nonempty strings")
    for pattern in config.get("exclude_title_patterns", []):
        re.compile(pattern)
    fields = config.get("output_fields", [])
    if not isinstance(fields, list) or any(not isinstance(f, str) for f in fields):
        raise ValueError("output_fields must be a list of field names")
    if len(set(fields)) != len(fields) or set(fields) - SUPPORTED_FIELDS:
        raise ValueError("Duplicate or unsupported output fields")
    if not REQUIRED_FIELDS <= set(fields):
        raise ValueError("Keep required identity, provenance and filter fields in output_fields")
    for key in ("date_from", "date_to"):
        if config.get(key):
            datetime.fromisoformat(config[key])
    if config.get("date_from") and config.get("date_to"):
        if datetime.fromisoformat(config["date_from"]) > datetime.fromisoformat(config["date_to"]):
            raise ValueError("date_from cannot be later than date_to")
    return config


def build_searches(config, *, query=None, employer_id=None):
    common = {"area": config["area_ids"], "per_page": config["per_page"], "page": 0}
    for key in ("date_from", "date_to"):
        if config.get(key):
            common[key] = config[key]
    searches = []
    if employer_id:
        if not str(employer_id).isdigit():
            raise ValueError("employer-id must be numeric")
        params = dict(common, employer_id=str(employer_id))
        if query:
            params["text"] = query
        searches.append({"route": "employer:" + str(employer_id), "params": params})
    elif query is not None:
        if not query.strip():
            raise ValueError("Empty query requires --employer-id")
        searches.append({"route": "query:" + query, "params": dict(common, text=query)})
    else:
        # No text filter on employer searches: retain all types of jobs.
        for employer in config["employers"]:
            searches.append({"route": "employer:" + str(employer["id"]),
                             "params": dict(common, employer_id=str(employer["id"]))})
        for phrase in dict.fromkeys(config["search_queries"]):
            searches.append({"route": "query:" + phrase, "params": dict(common, text=phrase)})
        for search in config.get("additional_searches", []):
            searches.append({"route": search["route"], "params": dict(common, **search["params"])})
        if len({s["route"] for s in searches}) != len(searches):
            raise ValueError("Additional route collides with an existing search")
    return searches


def search_url(params):
    return "https://api.hh.ru/vacancies?" + urllib.parse.urlencode(params, doseq=True)


def matching_groups(body, groups):
    matches = []
    for group in groups:
        def present(term):
            term = normalized(term)
            if term in ACRONYMS or term.isdigit():
                return re.search(r"(?<!\w)" + re.escape(term) + r"(?!\w)", body) is not None
            return term in body
        if all(present(term) for term in group):
            matches.append(" + ".join(group))
    return matches


def classify(record, config):
    title = normalized(record.get("title"))
    for pattern in config["exclude_title_patterns"]:
        if re.search(pattern, title):
            return "excluded_military_service", [pattern]
    employer_id = str(record.get("employer_source_id", ""))
    if employer_id in {str(e["id"]) for e in config["employers"]}:
        return "candidate_seed_employer", ["HH employer ID=" + employer_id]
    body = normalized(" ".join(str(record.get(key) or "") for key in (
        "title", "description", "responsibilities", "requirements", "conditions", "employer_name"
    )))
    strong = matching_groups(body, config["strong_signal_groups"])
    if strong:
        return "candidate_strong_signal", strong
    if employer_id in {str(e["id"]) for e in config.get("candidate_employers", [])}:
        return "review_employer_identity", ["Unverified GUR name match; HH employer ID=" + employer_id]
    weak = matching_groups(body, config["weak_signal_groups"])
    if weak:
        return "review_weak_signal", weak
    return "excluded_no_signal", []


def export_record(record, config):
    return {field: record.get(field, "") for field in config["output_fields"]}


def plan(config, searches, limit, pages):
    return {
        "mode": "dry_run", "network_requests": 0,
        "collection_requires": "explicit --run",
        "searches": [{"route": s["route"], "first_page_url": search_url(s["params"])} for s in searches],
        "max_pages_per_search": pages, "max_detail_attempts": limit or "unlimited (explicit --limit 0)",
        "delay_seconds": config["request_delay_seconds"],
        "output_fields": config["output_fields"],
        "candidate_rules": "Configured employer IDs or strong text signals; VPK status remains unverified",
        "weak_signals": "Saved separately for review",
        "excluded": "Military-service titles and postings without a configured signal",
        "coverage": "Initial two employer profiles plus configured searches; not the whole HH database",
    }


def run(config, searches, args, output, manifest, candidates, review, excluded):
    from .collect import hh_record, json_fetch, utc_now

    seen = set()
    manifest["searches"] = []
    manifest["detail_attempts"] = 0
    manifest["stop_reason"] = "configured_searches_processed"
    for search_index, search in enumerate(searches):
        report = {"route": search["route"], "pages_fetched": 0}
        manifest["searches"].append(report)
        for page in range(args.pages):
            time.sleep(args.delay)
            params = dict(search["params"], page=page)
            listing = json_fetch(search_url(params), output / "raw" / f"search_{search_index}_page_{page}.json", timeout=args.timeout)
            if not isinstance(listing.get("items"), list):
                raise ValueError("HH search response has no items list")
            report.update({"found": listing.get("found"), "reported_pages": listing.get("pages"), "pages_fetched": page + 1})
            for item in listing["items"]:
                if not str(item.get("id", "")).isdigit():
                    raise ValueError("HH result has no numeric vacancy ID")
                vid = str(item["id"])
                if vid in seen:
                    continue
                if args.limit and manifest["detail_attempts"] >= args.limit:
                    manifest["stop_reason"] = "detail_limit_reached"
                    return
                seen.add(vid)
                manifest["detail_attempts"] += 1
                time.sleep(args.delay)
                detail = json_fetch("https://api.hh.ru/vacancies/" + vid, output / "raw" / f"detail_{vid}.json", timeout=args.timeout)
                if str(detail.get("id")) != vid:
                    raise ValueError("HH detail ID differs from search result")
                record = hh_record(detail, utc_now())
                decision, matches = classify(record, config)
                record.update({"filter_decision": decision, "filter_matches": "; ".join(matches),
                               "discovery_route": search["route"],
                               "employer_resolution_status": "hh_profile_identified" if record["employer_source_id"] else "unresolved",
                               "vpk_status": "unverified",
                               "vpk_evidence": "Candidate filter: " + decision + "; " + "; ".join(matches)})
                target = candidates if decision.startswith("candidate_") else review if decision.startswith("review_") else excluded
                target.append(export_record(record, config))
                print(f"[{manifest['detail_attempts']}] {decision}: {record['title']}", flush=True)
            if page + 1 >= listing.get("pages", 0):
                break
        report["page_limit_reached"] = report["pages_fetched"] < (report.get("reported_pages") or 0)


def write_extra_outputs(output, review, excluded):
    from .collect import write_csv

    for name, records in (("review", review), ("excluded", excluded)):
        with (output / f"{name}.jsonl").open("w", encoding="utf-8") as stream:
            for record in records:
                stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        write_csv(output / f"{name}.csv", records)

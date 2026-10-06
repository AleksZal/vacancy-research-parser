"""Extract linked job cards from a saved Rostvertol HTML snapshot."""

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from .paths import PROJECT_ROOT
from urllib.parse import urlsplit

from scrapling import Selector

from .collect import text, write_outputs

SOURCE_URL = "https://rabota-rostvertol.ru/"


def parse_snapshot(html, observed_at):
    page = Selector(html)
    popups = {p.attrib["data-tooltip-hook"]: p for p in page.css(".t-popup[data-tooltip-hook]")}
    records, errors, seen = [], [], set()
    for link in page.css('a[href*="#popup:"]'):
        fragment = urlsplit(link.attrib["href"]).fragment
        hook = "#" + fragment
        if hook in seen:
            continue
        seen.add(hook)
        title = text(" ".join(link.xpath(".//text()").getall()))
        popup = popups.get(hook)
        description = ""
        if popup is not None:
            descr = popup.css(".t390__descr")
            if descr:
                description = "\n".join(text(t) for t in descr[0].xpath(".//text()").getall() if text(t))
        if not title or not description:
            errors.append({"id": fragment, "error": "Linked card has no title or description"})
        records.append({
            "source": "rostvertol_careers", "source_vacancy_id": fragment,
            "source_url": SOURCE_URL + hook, "detail_url": SOURCE_URL + hook,
            "observed_at": observed_at, "published_at_source": "",
            "source_visibility": "linked_from_job_list_at_collection_time",
            "detail_status": "collected" if description else "missing",
            "title": title, "employer_name": 'АО «Роствертол»',
            "employer_name_detail": 'АО «Роствертол»',
            "employer_source_id": "rabota-rostvertol.ru", "employer_id_namespace": "website_hostname",
            "region": "", "industry_label_source": "", "address": "",
            "address_type": "unspecified_by_source",
            "salary_from": None, "salary_to": None, "salary_currency": "",
            "salary_period": "unspecified_by_source", "salary_gross": None,
            "description": description,
            "responsibilities": "", "requirements": "", "conditions": "",
            "education": "", "experience": "", "skills": "", "schedule": "",
            "vpk_status": "unverified",
            "vpk_evidence": "Employer career website; affiliation verification handled separately",
        })
    return records, errors, len(popups)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--html", type=Path, default=PROJECT_ROOT / "data/rostvertol_firecrawl/raw/page.html")
    parser.add_argument("--metadata", type=Path, default=PROJECT_ROOT / "data/rostvertol_firecrawl/raw/firecrawl_metadata.json")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    metadata = json.loads(args.metadata.read_text(encoding="utf-8"))
    if metadata.get("metadata", {}).get("statusCode") != 200:
        parser.error("Snapshot must have a successful source HTTP response")
    records, errors, total_popups = parse_snapshot(args.html.read_text(encoding="utf-8"), metadata["retrieved_at"])
    output = args.output or PROJECT_ROOT / "data" / ("rostvertol_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    try:
        output.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        parser.error("Output directory already exists; choose a new snapshot directory")
    (output / "raw").mkdir()
    shutil.copyfile(args.html, output / "raw" / "page.html")
    shutil.copyfile(args.metadata, output / "raw" / "firecrawl_metadata.json")
    manifest = {
        "source": "rostvertol_careers", "started_at": metadata["retrieved_at"],
        "retrieval": "Firecrawl MCP scrape, maxAge=0, basic proxy",
        "source_url": SOURCE_URL, "source_list_count": len(records),
        "html_popup_blocks": total_popups,
        "coverage": "Only popup cards linked by the visible job-list links; not all HTML popups",
        "parameters": {"parser": "Scrapling Selector"}, "errors": errors,
    }
    write_outputs(output, records, manifest)
    print(f"Saved {len(records)} linked job cards; {total_popups} popup blocks in HTML; {len(errors)} errors")
    print(output.resolve())
    return 1 if errors or not records else 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    raise SystemExit(main())

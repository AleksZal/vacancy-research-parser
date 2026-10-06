"""Prepare an additive HH scope offline, without changing a live checkpoint.

Inputs are saved public GUR lists, HH area metadata and read-only checkpoints.
This entry point never downloads vacancies, starts a browser or writes SQLite.
"""
import argparse
import contextlib
import copy
import hashlib
import json
import re
import sqlite3
import urllib.parse
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from .hh_pipeline import build_searches, load_config, normalized
from .hh_web import search_url

from .paths import CONFIG_DIR, PROJECT_ROOT, resolve_config_path

ROOT = PROJECT_ROOT
LEGAL = re.compile(r"\b(?:государственная корпорация|публичное акционерное общество|открытое акционерное общество|закрытое акционерное общество|акционерное общество|общество с ограниченной ответственностью|федеральное казенное предприятие|федеральное государственное унитарное предприятие|автономная некоммерческая организация|пао|оао|ао|зао|ооо|фкп|фгуп|ано)\b", re.I)

FOCUS_QUERIES = {
    "drones": [
        "реактивные БПЛА", "реактивный беспилотник", "реактивные дроны",
        "турбореактивные БПЛА", "турбореактивный двигатель БПЛА",
        "малогабаритные турбореактивные двигатели", "двигатели для БПЛА",
        "беспилотники самолетного типа", "беспилотные воздушные суда", "БПАК",
        "ударные БПЛА", "ударные беспилотники", "баражирующие боеприпасы",
        "производство FPV", "разработка FPV", "FPV дроны", "дроны камикадзе",
        "автопилот БПЛА", "полетные контроллеры", "бортовое оборудование БПЛА",
        "системы управления БПЛА", "навигация БПЛА", "композитные корпуса БПЛА",
    ],
    "ballistics": [
        "баллистические ракеты", "баллистическая ракета", "ракетное вооружение",
        "ракетные двигатели", "твердотопливные ракетные двигатели",
        "ракетные системы управления", "головки самонаведения",
        "инерциальные навигационные системы", "бортовая аппаратура ракет",
        "ракетно-космическая техника", "ракетостроение", "ракетные топлива",
    ],
    "guided_bombs": [
        "корректируемые авиационные бомбы", "управляемые авиационные бомбы",
        "планирующие авиационные бомбы", "авиационные боеприпасы",
        "модуль планирования и коррекции", "УМПК", "УМПБ", "КАБ",
        "КАБ-500", "КАБ-1500", "системы наведения боеприпасов",
        "производство авиационных бомб", "головки наведения",
    ],
}
STRONG_GROUPS = [
    ["баллистич", "ракет"], ["авиационн", "бомб"], ["авиационн", "боеприпас"],
    ["баражирующ", "боеприпас"], ["ударн", "беспилотн"], ["ударн", "бпла"],
    ["дрон", "камикадзе"], ["ракетн", "вооружен"],
]
WEAK_GROUPS = [
    ["каб"], ["умпк"], ["умпб"], ["fpv"], ["бпак"],
    ["турбореактивн"], ["ракетн", "двигател"], ["ракетостро"],
    ["ракетн", "топлив"], ["головк", "самонаведен"], ["головк", "наведен"],
    ["инерциальн", "навигац"], ["полетн", "контроллер"],
]


def company_query(name):
    name = LEGAL.sub(" ", name)
    return re.sub(r"\s+", " ", re.sub(r'[^\w\s\-]', " ", name)).strip(" -")


def company_key(name):
    return re.sub(r"[^а-яa-z0-9]", "", company_query(normalized(name)))


def merge_companies(rostec, focus):
    entities = {}
    for row in [*rostec, *focus]:
        gid = str(row["gur_id"]) if row.get("gur_id") else None
        key = gid or row["source_entity_key"]
        entity = entities.setdefault(key, dict(entity_key=key, gur_id=gid, aliases=[], sources=[], focus_categories=[], lifecycle_note=""))
        for name in (row["name"], row.get("short_name", "")):
            if name and name not in entity["aliases"]:
                entity["aliases"].append(name)
        for url in [row["source_url"], *row.get("source_list_urls", [])]:
            if url not in entity["sources"]:
                entity["sources"].append(url)
        for category in row.get("focus_categories", []):
            if category not in entity["focus_categories"]:
                entity["focus_categories"].append(category)
        if row.get("source_lifecycle_note"):
            entity["lifecycle_note"] = row["source_lifecycle_note"]
    return list(entities.values())


def checkpoint_inputs(paths):
    employers = defaultdict(lambda: {"names": set(), "samples": []})
    broad = {}
    for path in paths:
        with contextlib.closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")  # Consistent read snapshot while a collector commits.
            for (serialized,) in db.execute("SELECT coalesce(record_json,index_json) FROM vacancies"):
                record = json.loads(serialized)
                eid = str(record.get("employer_source_id") or "")
                if not eid.isdigit():
                    continue
                item = employers[eid]
                item["names"].update(n for n in (record.get("employer_name"), record.get("employer_name_detail")) if n)
                if len(item["samples"]) < 3:
                    item["samples"].append(record["source_vacancy_id"])
            for params, found in db.execute("SELECT params,found FROM tasks WHERE kind='search' AND found>2000"):
                params = json.loads(params)
                # The existing generic special-equipment query is retained,
                # but not multiplied across regions because it is mostly dual use.
                if params.get("text") == "специальная техника":
                    continue
                identity = json.dumps({k: params[k] for k in ("text", "employer_id") if k in params}, sort_keys=True)
                broad[identity] = {k: params[k] for k in ("text", "employer_id") if k in params}
    return employers, list(broad.values())


def expand(config, companies, employers, broad, areas):
    config = copy.deepcopy(config)
    extras = config.setdefault("additional_searches", [])
    candidates = config.setdefault("candidate_employers", [])
    existing_ids = {str(e["id"]) for e in config["employers"] + candidates}
    by_name = defaultdict(list)
    for entity in companies:
        if entity["lifecycle_note"]:
            continue
        for alias in entity["aliases"]:
            key = company_key(alias)
            if len(key) >= 3:
                by_name[key].append(entity)
    for eid, employer in sorted(employers.items(), key=lambda pair: int(pair[0])):
        if eid in existing_ids:
            continue
        matched = {e["entity_key"]: e for name in employer["names"] for e in by_name.get(company_key(name), [])}
        if not matched:
            continue
        candidates.append({"id": eid, "name": sorted(employer["names"])[0],
                           "gur_ids": sorted({e["gur_id"] for e in matched.values() if e["gur_id"]}),
                           "source_entity_keys": sorted(matched), "source_urls": sorted({u for e in matched.values() for u in e["sources"]}),
                           "sample_vacancy_ids": employer["samples"],
                           "identity_status": "name_match_only_requires_verification"})
    for key, groups in (("strong_signal_groups", STRONG_GROUPS), ("weak_signal_groups", WEAK_GROUPS)):
        for group in groups:
            if group not in config[key]:
                config[key].append(group)
    def url_key(params):
        return tuple(sorted(urllib.parse.parse_qsl(urllib.parse.urlsplit(search_url(params)).query)))
    current_urls = {url_key(s["params"]) for s in build_searches(config)}
    current_routes = {s["route"] for s in build_searches(config)}

    def add(route, params, **metadata):
        merged = dict(area=config["area_ids"], per_page=config["per_page"], page=0)
        merged.update(params)
        url = url_key(merged)
        if url in current_urls:
            return
        if route in current_routes:
            raise ValueError("A route was reused for different public filters")
        extras.append(dict(route=route, params=params, **metadata))
        current_urls.add(url)
        current_routes.add(route)

    for topic, queries in FOCUS_QUERIES.items():
        for query in queries:
            add(f"focus:{topic}:{query}", {"text": query}, focus_category=topic)
    # Keep all public roles for possible employers, while retaining review status.
    for employer in candidates:
        add("candidate_employer:" + employer["id"], {"employer_id": employer["id"]}, identity_status=employer["identity_status"])
    for entity in sorted(companies, key=lambda e: (not bool(e["focus_categories"]), e["entity_key"])):
        if entity["lifecycle_note"]:
            continue
        for name in entity["aliases"]:
            query = normalized(company_query(name))
            if len(company_key(query)) < 3:
                continue
            prefix = "gur_focus" if entity["focus_categories"] else "gur_name"
            digest = hashlib.sha256(query.encode()).hexdigest()[:12]
            add(f"{prefix}:{entity['entity_key']}:{digest}", {"text": query, "search_field": ["company_name"]},
                gur_id=entity["gur_id"], source_entity_key=entity["entity_key"], source_urls=entity["sources"], focus_categories=entity["focus_categories"], identity_status="search_name_only_requires_verification")
    for params in broad:
        query_id = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()[:12]
        for area in areas:
            add(f"region:{area['id']}:{query_id}", dict(params, area=[area["id"]]), area_name=area["name"], purpose="Partition a public search whose reported total exceeded 2000")
    return config


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, default=CONFIG_DIR / "hh_config.json")
    parser.add_argument("--output", type=Path, default=CONFIG_DIR / "hh_expanded_config.json")
    args = parser.parse_args(argv)
    args.base = resolve_config_path(args.base)
    if args.output.resolve() == args.base.resolve():
        parser.error("Keep the expansion separate from the currently running config")
    base = load_config(args.base)
    folder = ROOT / "sources/hh_expansion"
    companies = merge_companies(json.loads((ROOT / "sources/gur_rostec/companies.json").read_text(encoding="utf-8")),
                               json.loads((folder / "focus_companies.json").read_text(encoding="utf-8")))
    checkpoints = [ROOT / "data/hh_fast/queue.sqlite3", ROOT / "data/hh_bulk/queue.sqlite3"]
    employers, broad = checkpoint_inputs(checkpoints)
    regions = next(a for a in json.loads((folder / "areas.json").read_text(encoding="utf-8")) if a["id"] == "113")["areas"]
    config = expand(base, companies, employers, broad, regions)
    args.output.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    config = load_config(args.output)
    from . import hh_bulk
    hh_bulk.validate_extension({"config": base, "searches": build_searches(base)}, {"config": config, "searches": build_searches(config)})
    kinds = defaultdict(int)
    for search in config["additional_searches"]:
        kinds[search["route"].split(":")[0]] += 1
    manifest = dict(prepared_at=datetime.now(timezone.utc).isoformat(), config=str(args.output.resolve()),
                    network_requests=0, live_checkpoint_writes=0, existing_searches=len(build_searches(base)),
                    expanded_searches=len(build_searches(config)), additional_searches=dict(kinds),
                    original_seed_employers=len(config["employers"]), candidate_employers=len(config["candidate_employers"]),
                    source_entity_entries=len(companies), source_region_segments=len(regions), broad_queries_partitioned=len(broad),
                    coverage="Additive candidate discovery; no promised vacancy count or exhaustive coverage")
    (folder / "plan.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

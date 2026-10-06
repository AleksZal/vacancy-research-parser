"""Import saved HH cards into the existing shared schema; no schema changes."""
import argparse
import contextlib
from collections import Counter, defaultdict
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import tarfile
import urllib.parse

from psycopg import sql
from psycopg.types.json import Jsonb

from .paths import PROJECT_ROOT, CONFIG_DIR
from .shared_db import connect_shared, inspect_schema, safe_error

LEVEL_MAP = {
    'candidate_seed_employer': 'likely',
    'candidate_strong_signal': 'likely',
    'review_weak_signal': 'review',
    'review_employer_identity': 'review',
    'review_gur_employer_search': 'review',
    'excluded_no_signal': 'no',
    'excluded_military_service': 'no',
}
WRITABLE = {'employer_profile', 'vacancy', 'vacancy_hh', 'vacancy_discovery',
            'vacancy_snapshot', 'classifier_run', 'vacancy_classification',
            'classification_evidence'}


def nullable(value):
    return None if value is None or value == '' else value


def timestamp(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError('Source timestamps must include a timezone')
    return result


def sha_file(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def prepare(output, verify_html=True, checkpoint_only=False):
    cards_path = PROJECT_ROOT / 'data/hh_fast/full_cards.jsonl'
    cards = []
    profiles = {}
    labels = Counter()
    ids = set()
    with cards_path.open(encoding='utf-8') as stream:
        for line in stream:
            row = json.loads(line)
            external_id = row['source_vacancy_id']
            if row['source'] != 'hh_html' or external_id in ids:
                raise ValueError('Unexpected source or duplicate vacancy ID')
            if row.get('detail_url') and row['detail_url'] != row['source_url']:
                raise ValueError('Different detail_url needs a schema-owner decision')
            if row['filter_decision'] not in LEVEL_MAP:
                raise ValueError('Unmapped source classification label')
            for required in ('source_url', 'title', 'observed_at'):
                if not row.get(required):
                    raise ValueError('Required vacancy field is empty')
            row['_seen'] = timestamp(row['observed_at'])
            if row.get('published_at_source'):
                timestamp(row['published_at_source'])
            ids.add(external_id)
            cards.append(row)
            labels[row['filter_decision']] += 1
            employer_id, namespace = row.get('employer_source_id'), row.get('employer_id_namespace')
            if employer_id and namespace:
                key = (namespace, employer_id)
                name = row.get('employer_name_detail') or row.get('employer_name')
                if not name:
                    raise ValueError('Employer profile has no name')
                seen = row['_seen']
                current = profiles.get(key)
                if current is None:
                    profiles[key] = dict(name=name, url=nullable(row.get('employer_profile_url')),
                                         first=seen, last=seen)
                else:
                    current['first'] = min(current['first'], seen)
                    if seen >= current['last']:
                        current.update(name=name, last=seen)
                        if row.get('employer_profile_url'):
                            current['url'] = row['employer_profile_url']

    routes = defaultdict(set)
    checkpoint = PROJECT_ROOT / 'data/hh_fast/queue.sqlite3'
    with contextlib.closing(sqlite3.connect(checkpoint.as_uri() + '?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        completed = {r[0] for r in db.execute("SELECT id FROM vacancies WHERE state='done'")}
        if completed != ids:
            raise ValueError('Export and checkpoint full-card IDs differ')
        for external_id, route in db.execute('SELECT vacancy_id,route FROM routes'):
            if external_id in ids and route:
                routes[external_id].add(route)
        checkpoint_snapshots = db.execute('''SELECT url,snapshot,snapshot_sha256,observed_at
            FROM tasks WHERE kind='detail' AND state='done' AND snapshot IS NOT NULL''').fetchall() if checkpoint_only else []
    for row in cards:
        if row.get('discovery_route'):
            routes[row['source_vacancy_id']].add(row['discovery_route'])

    snapshots = defaultdict(list)
    archive_paths = set()
    snapshot_keys = {}
    ignored = Counter()
    read_count = 0
    if checkpoint_only:
        for url,snapshot,sha,observed_at in checkpoint_snapshots:
            match = re.fullmatch(r'/vacancy/(\d+)',urllib.parse.urlsplit(url).path)
            if not match or match[1] not in ids:
                continue
            external_id=match[1]
            path=Path(snapshot)
            if not path.is_absolute():
                path=PROJECT_ROOT/'data/hh_fast'/path
            relative=path.relative_to(PROJECT_ROOT/'data').as_posix()
            observed=timestamp(observed_at)
            key=(external_id,observed)
            if key not in snapshot_keys:
                entry=dict(fetched_at=observed,format='html',path=relative,
                           locator=None,http_status=None,sha256=sha)
                snapshots[external_id].append(entry)
                snapshot_keys[key]=entry
    for folder in (() if checkpoint_only else ('hh_bulk', 'hh_fast')):
        raw = PROJECT_ROOT / 'data' / folder / 'raw'
        with os.scandir(raw) as entries:
            metadata_paths = sorted(Path(e.path) for e in entries if e.name.endswith('.metadata.json'))
        for metadata_path in metadata_paths:
            metadata = json.loads(metadata_path.read_text(encoding='utf-8'))
            parsed = urllib.parse.urlsplit(metadata.get('sourceURL', ''))
            match = re.fullmatch(r'/vacancy/(\d+)', parsed.path)
            if not match or match[1] not in ids:
                ignored['not_an_imported_vacancy'] += 1
                continue
            if parsed.hostname != 'hh.ru' and not (parsed.hostname or '').endswith('.hh.ru'):
                raise ValueError('Unexpected snapshot hostname')
            html_path = Path(str(metadata_path).removesuffix('.metadata.json') + '.gz')
            if not html_path.is_file():
                raise ValueError('Snapshot HTML file is missing')
            observed = timestamp(metadata['observed_at'])
            sha = metadata['sha256']
            if not re.fullmatch('[0-9a-f]{64}', sha):
                raise ValueError('Invalid snapshot checksum')
            if verify_html:
                digest = hashlib.sha256()
                with gzip.open(html_path, 'rb') as stream:
                    for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                        digest.update(chunk)
                if digest.hexdigest() != sha:
                    raise ValueError('Snapshot checksum mismatch')
            path = html_path.relative_to(PROJECT_ROOT / 'data').as_posix()
            external_id = match[1]
            key = (external_id, observed)
            previous = snapshot_keys.get(key)
            if previous:
                if previous['sha256'] != sha:
                    raise ValueError('Different snapshots have the same vacancy/fetched_at key')
                ignored['duplicate_snapshot_key'] += 1
            else:
                snapshot = dict(fetched_at=observed, format='html', path=path,
                                locator=None, http_status=metadata.get('statusCode'), sha256=sha)
                snapshots[external_id].append(snapshot)
                snapshot_keys[key] = snapshot
            archive_paths.add(html_path)
            archive_paths.add(metadata_path)
            read_count += 1
            if read_count % 1000 == 0:
                print(json.dumps({'phase': 'validate_html', 'checked': read_count}), flush=True)
    uncovered = ids - snapshots.keys()
    if uncovered:
        raise ValueError('Some full cards lack a saved vacancy HTML snapshot')

    source_sha = sha_file(cards_path)
    rules_sha = sha_file(CONFIG_DIR / 'hh_expanded_config.json')
    semantic_digest = hashlib.sha256()
    for external_id in sorted(ids):
        semantic_digest.update(json.dumps([
            external_id, sorted(routes[external_id]),
            sorted((s['path'], s['fetched_at'].isoformat(), s['sha256']) for s in snapshots[external_id])
        ], ensure_ascii=False, separators=(',', ':')).encode())
    import_id = hashlib.sha256(json.dumps([
        'hh_shared_import_v1', source_sha, rules_sha, semantic_digest.hexdigest(), LEVEL_MAP
    ], sort_keys=True).encode()).hexdigest()
    plan = dict(import_id=import_id, source='hh', source_jsonl_sha256=source_sha,
                rules_sha256=rules_sha, full_cards=len(cards), employer_profiles=len(profiles),
                cards_without_profile=sum(not (r.get('employer_source_id') and r.get('employer_id_namespace')) for r in cards),
                discovery_rows=sum(map(len, routes.values())), snapshot_rows=len(snapshot_keys),
                source_labels=dict(labels), level_counts=dict(Counter(LEVEL_MAP[r['filter_decision']] for r in cards)),
                html_sha256='decompressed UTF-8 HTML bytes', html_checksums_verified=verify_html,
                snapshot_metadata_source='SQLite checkpoint' if checkpoint_only else 'raw sidecar metadata',
                html_files_read=not checkpoint_only,
                ignored_metadata=dict(ignored), archive_files=len(archive_paths),
                raw_storage_relative_root='data/',
                archive_bytes=sum(p.stat().st_size for p in archive_paths),
                input_files={'cards': 'data/hh_fast/full_cards.jsonl', 'checkpoint': 'data/hh_fast/queue.sqlite3'},
                excluded_contact_columns=['public_business_contact_name','public_business_phone','public_business_email'])
    output.mkdir(parents=True, exist_ok=True)
    (output / 'plan.json').write_text(json.dumps(plan, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    return cards, profiles, routes, snapshots, archive_paths, plan


def insert_rows(conn, table, columns, rows, conflict='', returning=(), page_size=500):
    if table not in WRITABLE:
        raise ValueError('Table is not authorized for this HH importer')
    result = []
    for start in range(0, len(rows), page_size):
        page = rows[start:start + page_size]
        if not page:
            continue
        placeholders = sql.SQL(',').join(sql.Placeholder() for _ in columns)
        values_sql = sql.SQL(',').join(sql.SQL('({})').format(placeholders) for _ in page)
        statement = sql.SQL('INSERT INTO public.{} ({}) VALUES {} {}').format(
            sql.Identifier(table), sql.SQL(',').join(map(sql.Identifier, columns)),
            values_sql, sql.SQL(conflict))
        if returning:
            statement += sql.SQL(' RETURNING {}').format(sql.SQL(',').join(map(sql.Identifier, returning)))
        cursor = conn.execute(statement, [value for row in page for value in row])
        if returning:
            result.extend(cursor.fetchall())
    return result


def validate_target_schema(conn):
    schema = inspect_schema(conn)
    tables = {r['table_name'] for r in schema['columns'] if r['table_schema'] == 'public'}
    if not WRITABLE <= tables or 'vacancy_source' not in tables:
        raise ValueError('Required shared tables are missing')
    source = conn.execute("SELECT code,raw_storage FROM public.vacancy_source WHERE code='hh'").fetchone()
    if source is None:
        raise ValueError('HH source is not registered; owner must configure it')
    return {'raw_storage_configured': source['raw_storage'] is not None}


def get_run(conn, plan):
    params = dict(import_id=plan['import_id'], source='hh', level_map=LEVEL_MAP,
                  source_jsonl_sha256=plan['source_jsonl_sha256'],
                  import_mode='saved_labels_no_reclassification',
                  evidence_origin='export: original field, no inferred text snippet',
                  source_vpk_status_is_not_a_category=True,
                  original_filter_execution_timestamp_available=False,
                  expected_full_cards=plan['full_cards'])
    # Rerunning the same saved export resumes its own run; older runs are untouched.
    run = conn.execute('''SELECT run_id FROM public.classifier_run
        WHERE classifier='hh_filter' AND params->>'import_id'=%s
        ORDER BY run_id LIMIT 1''', (plan['import_id'],)).fetchone()
    if run:
        return run['run_id']
    with conn.transaction():
        return conn.execute('''INSERT INTO public.classifier_run
            (classifier,version,rules_sha256,params,started_at,note)
            VALUES (%s,%s,%s,%s,%s,%s) RETURNING run_id''',
            ('hh_filter', 'saved-labels-' + plan['rules_sha256'][:16], plan['rules_sha256'],
             Jsonb(params), datetime.now(timezone.utc),
             'Import of saved HH filter labels. started_at is import time; original filter '
             'run times are unavailable. All candidates remain unverified. '
             'Excluded labels map to no as specified in the HH loading instructions.')).fetchone()['run_id']


VACANCY_COLUMNS = (
    'source','external_id','url','employer_profile_id','employer_name','title',
    'locality','address','salary_from','salary_to','salary_currency','salary_gross',
    'salary_period','experience','education','schedule','description',
    'responsibilities','requirements','conditions','skills_raw','published_at',
    'first_seen_at','last_seen_at','is_active',
)


def vacancy_values(row, profile_ids):
    key = (row.get('employer_id_namespace'), row.get('employer_source_id'))
    mapped = dict(source='hh', external_id=row['source_vacancy_id'], url=row['source_url'],
                  employer_profile_id=profile_ids.get(key), employer_name=nullable(row.get('employer_name')),
                  title=row['title'], locality=nullable(row.get('region')), address=nullable(row.get('address')),
                  skills_raw=nullable(row.get('skills')),
                  published_at=timestamp(row['published_at_source']) if row.get('published_at_source') else None,
                  first_seen_at=row['_seen'], last_seen_at=row['_seen'], is_active=True)
    for field in ('salary_from','salary_to','salary_currency','salary_gross','salary_period',
                  'experience','education','schedule','description','responsibilities','requirements','conditions'):
        mapped[field] = nullable(row.get(field))
    return tuple(mapped[c] for c in VACANCY_COLUMNS)


def upload(conn, cards, profiles, routes, snapshots, plan, output, batch_size):
    acquired = conn.execute("SELECT pg_try_advisory_lock(hashtext('hh_research_import_v1')) AS locked").fetchone()['locked']
    if not acquired:
        raise RuntimeError('Another HH importer holds the database import lock')
    try:
        target = validate_target_schema(conn)
        profile_ids = {}
        items = sorted(profiles.items())
        for start in range(0, len(items), batch_size):
            with conn.transaction():
                rows = [('hh', key[0], key[1], value['name'], value['url'], None, None, None,
                         'unknown', value['first'], value['last']) for key,value in items[start:start+batch_size]]
                returned = insert_rows(conn, 'employer_profile',
                    ('source','id_namespace','external_id','name','url','inn','ogrn','kpp','publisher_type','first_seen_at','last_seen_at'),
                    rows, '''ON CONFLICT (source,id_namespace,external_id) DO UPDATE SET
                        name=EXCLUDED.name,url=COALESCE(EXCLUDED.url,employer_profile.url),
                        inn=COALESCE(EXCLUDED.inn,employer_profile.inn),
                        ogrn=COALESCE(EXCLUDED.ogrn,employer_profile.ogrn),
                        kpp=COALESCE(EXCLUDED.kpp,employer_profile.kpp),
                        first_seen_at=LEAST(employer_profile.first_seen_at,EXCLUDED.first_seen_at),
                        last_seen_at=GREATEST(employer_profile.last_seen_at,EXCLUDED.last_seen_at)''',
                    returning=('employer_profile_id','id_namespace','external_id'))
                profile_ids.update({(r['id_namespace'],r['external_id']):r['employer_profile_id'] for r in returned})
        run_id = get_run(conn, plan)
        imported = 0
        for start in range(0, len(cards), batch_size):
            batch = cards[start:start+batch_size]
            with conn.transaction():
                updates = sql.SQL(',').join(sql.SQL('{}=EXCLUDED.{}').format(sql.Identifier(c),sql.Identifier(c))
                    for c in VACANCY_COLUMNS if c not in ('source','external_id','first_seen_at','last_seen_at'))
                conflict = updates.as_string(conn) + ',first_seen_at=LEAST(vacancy.first_seen_at,EXCLUDED.first_seen_at),last_seen_at=GREATEST(vacancy.last_seen_at,EXCLUDED.last_seen_at)'
                returned = insert_rows(conn,'vacancy',VACANCY_COLUMNS,
                    [vacancy_values(r,profile_ids) for r in batch],
                    'ON CONFLICT (source,external_id) DO UPDATE SET ' + conflict,
                    returning=('vacancy_id','external_id'))
                vacancy_ids = {r['external_id']:r['vacancy_id'] for r in returned}
                hh_rows=[]; discovery_rows=[]; snapshot_rows=[]; class_rows=[]; evidence_rows=[]
                existing_classes = {r['vacancy_id'] for r in conn.execute('''
                    SELECT vacancy_id FROM public.vacancy_classification
                    WHERE run_id=%s AND vacancy_id=ANY(%s)
                ''',(run_id,list(vacancy_ids.values()))).fetchall()}
                for row in batch:
                    external_id=row['source_vacancy_id'];vid=vacancy_ids[external_id]
                    hh_rows.append((vid,*(nullable(row.get(f)) for f in ('employer_name_detail',
                        'employer_resolution_status','address_type','detail_status','source_visibility'))))
                    discovery_rows.extend((vid,route,None,None) for route in sorted(routes[external_id]))
                    snapshot_rows.extend((vid,s['fetched_at'],s['format'],s['path'],s['locator'],
                        s['http_status'],s['sha256']) for s in snapshots[external_id])
                    if vid not in existing_classes:
                        class_rows.append((run_id,vid,LEVEL_MAP[row['filter_decision']],row['filter_decision'],None,None))
                        for signal in ('filter_matches','vpk_evidence','vpk_status'):
                            if row.get(signal):
                                evidence_rows.append((run_id,vid,signal,'export',None,row[signal]))
                insert_rows(conn,'vacancy_hh',('vacancy_id','employer_name_detail','employer_resolution_status',
                    'address_type','detail_status','source_visibility'),hh_rows,
                    '''ON CONFLICT (vacancy_id) DO UPDATE SET employer_name_detail=EXCLUDED.employer_name_detail,
                    employer_resolution_status=EXCLUDED.employer_resolution_status,address_type=EXCLUDED.address_type,
                    detail_status=EXCLUDED.detail_status,source_visibility=EXCLUDED.source_visibility''')
                insert_rows(conn,'vacancy_discovery',('vacancy_id','route','first_seen_at','last_seen_at'),
                    discovery_rows,'ON CONFLICT (vacancy_id,route) DO NOTHING')
                insert_rows(conn,'vacancy_snapshot',('vacancy_id','fetched_at','format','path','locator','http_status','sha256'),
                    snapshot_rows,'ON CONFLICT (vacancy_id,fetched_at) DO NOTHING')
                insert_rows(conn,'vacancy_classification',('run_id','vacancy_id','level','raw_label','score','category'),class_rows)
                insert_rows(conn,'classification_evidence',('run_id','vacancy_id','signal','origin','weight','snippet'),evidence_rows)
            imported += len(batch)
            (output/'progress.json').write_text(json.dumps(dict(import_id=plan['import_id'],run_id=run_id,
                committed_cards=imported,total_cards=len(cards)),indent=2)+'\n',encoding='utf-8')
            print(json.dumps({'phase':'database_upload','committed_cards':imported,'total_cards':len(cards),'run_id':run_id}),flush=True)
        return run_id,target
    finally:
        conn.execute("SELECT pg_advisory_unlock(hashtext('hh_research_import_v1'))")


def verify(conn, plan, run_id):
    with conn.transaction():
        conn.execute('SET TRANSACTION READ ONLY')
        report = dict(
            source_counts=conn.execute('''SELECT source,count(*) AS total,count(*) FILTER (WHERE is_active) AS active,
                min(first_seen_at) AS first_seen_at,max(last_seen_at) AS last_seen_at
                FROM public.vacancy GROUP BY source''').fetchall(),
            hh_profiles=conn.execute("SELECT count(*) AS n FROM public.employer_profile WHERE source='hh'").fetchone()['n'],
            hh_without_profile=conn.execute("SELECT count(*) AS n FROM public.vacancy WHERE source='hh' AND employer_profile_id IS NULL").fetchone()['n'],
            hh_detail_rows=conn.execute("SELECT count(*) AS n FROM public.vacancy v JOIN public.vacancy_hh h USING(vacancy_id) WHERE v.source='hh'").fetchone()['n'],
            hh_discovery_rows=conn.execute("SELECT count(*) AS n FROM public.vacancy v JOIN public.vacancy_discovery d USING(vacancy_id) WHERE v.source='hh'").fetchone()['n'],
            hh_snapshot_rows=conn.execute("SELECT count(*) AS n FROM public.vacancy v JOIN public.vacancy_snapshot s USING(vacancy_id) WHERE v.source='hh'").fetchone()['n'],
            classification=conn.execute('''SELECT raw_label,level,count(*) AS n FROM public.vacancy_classification
                WHERE run_id=%s GROUP BY raw_label,level ORDER BY raw_label''',(run_id,)).fetchall(),
            evidence_rows=conn.execute("SELECT count(*) AS n FROM public.classification_evidence WHERE run_id=%s",(run_id,)).fetchone()['n'],
            foreign_source_in_run=conn.execute('''SELECT count(*) AS n FROM public.vacancy_classification c
                JOIN public.vacancy v USING(vacancy_id) WHERE c.run_id=%s AND v.source<>'hh' ''',(run_id,)).fetchone()['n'],
            run_id=run_id,import_id=plan['import_id'])
    if sum(r['n'] for r in report['classification']) != plan['full_cards']:
        raise ValueError('Classification count differs from the imported export')
    if report['foreign_source_in_run']:
        raise ValueError('Unexpected foreign-source classification')
    actual_labels={r['raw_label']:r['n'] for r in report['classification']}
    if actual_labels != plan['source_labels']:
        raise ValueError('Source classification labels did not round-trip')
    for key,expected in (('hh_profiles',plan['employer_profiles']),('hh_without_profile',plan['cards_without_profile']),
                         ('hh_detail_rows',plan['full_cards']),('hh_discovery_rows',plan['discovery_rows']),
                         ('hh_snapshot_rows',plan['snapshot_rows'])):
        if report[key] != expected:
            raise ValueError('Post-upload row count differs from the prepared plan: '+key)
    return report


def make_archive(paths, output):
    destination=output/'hh_raw_html.tar'
    temporary=destination.with_suffix('.tar.part')
    with tarfile.open(temporary,'w') as archive:
        for index,path in enumerate(sorted(paths)):
            archive.add(path,arcname=path.relative_to(PROJECT_ROOT/'data').as_posix(),recursive=False)
            if (index+1)%5000==0:
                print(json.dumps({'phase':'html_archive','files_written':index+1,'total_files':len(paths)}),flush=True)
    temporary.replace(destination)
    result=dict(file=destination.name,sha256=sha_file(destination),bytes=destination.stat().st_size,
                extract_root='Set vacancy_source.raw_storage to the extracted archive root; owner action only.')
    (output/'archive.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    return result


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',action='store_true',help='Write HH rows to the existing shared database')
    parser.add_argument('--batch-size',type=int,default=5000)
    parser.add_argument('--output',type=Path,default=PROJECT_ROOT/'outputs/hh_shared_import')
    args=parser.parse_args(argv)
    if not 1<=args.batch_size<=10000: parser.error('batch-size must be 1..10000')
    args.output.mkdir(parents=True,exist_ok=True)
    try:
        cards,profiles,routes,snapshots,paths,plan=prepare(args.output,verify_html=False,checkpoint_only=True)
        print(json.dumps({'phase':'prepared',**plan}),flush=True)
        with connect_shared() as conn:
            target=validate_target_schema(conn)
            if not args.run:
                print(json.dumps({'mode':'dry_run','database_writes':0,**target}),flush=True)
                return 0
            run_id,target=upload(conn,cards,profiles,routes,snapshots,plan,args.output,args.batch_size)
            verification=verify(conn,plan,run_id)
        archive=None
        report=dict(status='complete',uploaded_at=datetime.now(timezone.utc).isoformat(),
                    verification=verification,archive=archive,**target,
                    source_storage_owner_action_required=not target['raw_storage_configured'],
                    schema_changed=False,other_sources_written=False,
                    employer_classification_rows=0,company_matches_written=0,
                    note='No employer-level labels or NLP matches were provided; none invented.')
        (args.output/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8')
        print(json.dumps({'phase':'verified','report':str(args.output/'report.json'),
                         'cards':plan['full_cards'],'run_id':run_id},ensure_ascii=False),flush=True)
        return 0
    except Exception as exc:
        failure=safe_error(exc)
        (args.output/'error.json').write_text(json.dumps(failure,indent=2)+'\n',encoding='utf-8')
        print(json.dumps(failure),flush=True)
        return 1


if __name__=='__main__':
    raise SystemExit(main())

"""Read-only comparison of all imported HH card fields with the source JSONL."""
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from vacancy_parser.shared_db import connect_shared,safe_error
from vacancy_parser.shared_import import nullable,timestamp

FIELDS=('title','source_url','employer_name','region','address','salary_from','salary_to',
        'salary_currency','salary_gross','salary_period','experience','education','schedule',
        'description','responsibilities','requirements','conditions','skills',
        'employer_name_detail','employer_resolution_status','address_type','detail_status','source_visibility')

try:
    expected={}
    with (ROOT/'data/hh_fast/full_cards.jsonl').open(encoding='utf-8') as stream:
        for line in stream:
            row=json.loads(line)
            payload=[nullable(row.get(field)) for field in FIELDS]
            expected[row['source_vacancy_id']]=(hashlib.md5(json.dumps(payload,ensure_ascii=False).encode()).hexdigest(),row)
    with connect_shared() as conn:
        with conn.transaction():
            conn.execute('SET TRANSACTION READ ONLY')
            actual=conn.execute('''
                SELECT v.external_id,v.first_seen_at,v.last_seen_at,v.published_at,
                       p.external_id AS employer_external_id,p.id_namespace AS employer_namespace,
                       md5(jsonb_build_array(v.title,v.url,v.employer_name,v.locality,v.address,
                           trim_scale(v.salary_from),trim_scale(v.salary_to),v.salary_currency,v.salary_gross,v.salary_period,
                           v.experience,v.education,v.schedule,v.description,v.responsibilities,
                           v.requirements,v.conditions,v.skills_raw,h.employer_name_detail,
                           h.employer_resolution_status,h.address_type,h.detail_status,h.source_visibility)::text) AS digest
                FROM public.vacancy v JOIN public.vacancy_hh h USING(vacancy_id)
                LEFT JOIN public.employer_profile p USING(employer_profile_id)
                WHERE v.source='hh'
            ''').fetchall()
    if {r['external_id'] for r in actual} != expected.keys():
        raise ValueError('Imported vacancy IDs differ from the full-card export')
    mismatch=0
    for record in actual:
        digest,source=expected[record['external_id']]
        mismatch+=int(record['digest']!=digest)
        if source.get('employer_source_id') and source.get('employer_id_namespace'):
            if (record['employer_external_id'],record['employer_namespace'])!=(source['employer_source_id'],source['employer_id_namespace']):
                raise ValueError('Employer source identity changed during import')
        elif record['employer_external_id'] is not None:
            raise ValueError('Missing source employer identity was guessed')
        seen=timestamp(source['observed_at'])
        if not (record['first_seen_at']<=seen<=record['last_seen_at']):
            raise ValueError('Observed timestamp was not preserved')
        published=timestamp(source['published_at_source']) if source.get('published_at_source') else None
        if record['published_at']!=published:
            raise ValueError('Publication timestamp changed')
    if mismatch:
        print(json.dumps({'field_hash_mismatch_count':mismatch}))
        raise ValueError('Imported card field checksum differs')
    result=dict(status='passed',cards_compared=len(actual),mapped_fields_compared=len(FIELDS),
                mismatched_cards=mismatch,employer_ids_preserved=True,publication_times_preserved=True,
                zero_salaries_and_false_gross_preserved=True,database_writes=0)
    (ROOT/'outputs/hh_shared_import/field_verification.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result))
except Exception as exc:
    error=safe_error(exc)
    if isinstance(exc,ValueError) and str(exc) in (
        'Imported vacancy IDs differ from the full-card export',
        'Employer source identity changed during import',
        'Missing source employer identity was guessed',
        'Observed timestamp was not preserved',
        'Publication timestamp changed','Imported card field checksum differs'):
        error['reason']=str(exc)
    print(json.dumps(error));raise SystemExit(1)

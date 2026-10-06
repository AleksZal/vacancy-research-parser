"""Parallel full-card HH collection in a separate checkpoint.

Default execution is a read-only plan. --run uses data/hh_fast and never
writes the baseline data/hh_bulk. Complete vacancy IDs are reused locally.
"""
import argparse
import contextlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import asyncio

from . import hh_bulk as bulk
from .hh_pipeline import build_searches, load_config
from .hh_web import search_url

from .paths import CONFIG_DIR, PROJECT_ROOT

ROOT = PROJECT_ROOT


def readonly(path):
    db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    return db


def other_collectors():
    """Fail closed if active collectors cannot be checked; ignore our launcher."""
    if os.name != 'nt':
        raise RuntimeError('This entry point currently supports Windows process checks only')
    command = ('Get-CimInstance Win32_Process -ErrorAction Stop | '
               'Where-Object { $_.Name -match "^python(w)?\\.exe$" } | '
               'Select-Object ProcessId,ParentProcessId,CommandLine | ConvertTo-Json -Compress')
    result = subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',command],
                            capture_output=True,text=True,timeout=20)
    if result.returncode:
        raise RuntimeError('Cannot check running Python collectors; no collection started')
    rows = json.loads(result.stdout.strip() or '[]')
    if isinstance(rows, dict):
        rows = [rows]
    parents = {r['ProcessId']:r['ParentProcessId'] for r in rows}
    ancestors = {os.getpid()}
    current = os.getpid()
    while current in parents and parents[current] not in ancestors:
        current = parents[current]
        ancestors.add(current)
    return [r['ProcessId'] for r in rows if r['ProcessId'] not in ancestors
            and re.search(r'(?:\bhh_(?:bulk|fast)\.py\b|\bvacancy_parser\.hh_(?:bulk|fast)\b)', r.get('CommandLine') or '', re.I)]


@contextlib.contextmanager
def output_lock(output):
    """Prevent two fast processes from opening the same checkpoint."""
    import msvcrt
    output.mkdir(parents=True,exist_ok=True)
    stream = (output / '.fast.lock').open('a+b')
    stream.seek(0,2)
    if not stream.tell():
        stream.write(b'0')
        stream.flush()
    stream.seek(0)
    try:
        msvcrt.locking(stream.fileno(),msvcrt.LK_NBLCK,1)
    except OSError:
        stream.close()
        raise RuntimeError('Another fast collector holds this output folder')
    try:
        yield
    finally:
        stream.seek(0)
        msvcrt.locking(stream.fileno(),msvcrt.LK_UNLCK,1)
        stream.close()


def clone_baseline(baseline, output):
    source_path, target_path = baseline / 'queue.sqlite3', output / 'queue.sqlite3'
    if target_path.exists():
        return
    temporary = output / 'queue.sqlite3.part'
    with contextlib.closing(readonly(source_path)) as source, \
         contextlib.closing(sqlite3.connect(temporary)) as target:
        source.backup(target)
        # Source snapshots stay in the original folder. No HTML duplication.
        for row in target.execute('SELECT url,snapshot FROM tasks WHERE snapshot IS NOT NULL').fetchall():
            snapshot = Path(row[1])
            if not snapshot.is_absolute():
                target.execute('UPDATE tasks SET snapshot=? WHERE url=?',
                               (str((baseline/snapshot).resolve()),row[0]))
        target.execute("DELETE FROM meta WHERE key IN ('last_run_metrics','night_run')")
        target.commit()
    temporary.replace(target_path)
    state = baseline / 'browser-profile/session-state.json'
    if state.exists():
        destination = output / 'browser-profile/session-state.json'
        destination.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(state,destination)


def sync_completed(db, baseline):
    """Refresh deduplication at every start if the original advanced meanwhile."""
    count = 0
    with contextlib.closing(readonly(baseline/'queue.sqlite3')) as source:
        with db:
            for row in source.execute("SELECT * FROM vacancies WHERE state IN ('done','restricted','redirected','unavailable')"):
                current = db.execute('SELECT state FROM vacancies WHERE id=?',(row['id'],)).fetchone()
                if current and current['state'] != 'pending':
                    continue
                db.execute('INSERT INTO vacancies(id,index_json,record_json,state) VALUES (?,?,?,?) '
                           'ON CONFLICT(id) DO UPDATE SET index_json=excluded.index_json,record_json=excluded.record_json,state=excluded.state',
                           (row['id'],row['index_json'],row['record_json'],row['state']))
                url = 'https://hh.ru/vacancy/'+row['id']
                task = source.execute('SELECT * FROM tasks WHERE url=?',(url,)).fetchone()
                if task:
                    values = dict(task)
                    if values['snapshot'] and not Path(values['snapshot']).is_absolute():
                        values['snapshot'] = str((baseline/values['snapshot']).resolve())
                    columns = ','.join(values)
                    db.execute(f'INSERT OR REPLACE INTO tasks({columns}) VALUES ({",".join("?" for _ in values)})',list(values.values()))
                for route in source.execute('SELECT route FROM routes WHERE vacancy_id=?',(row['id'],)):
                    db.execute('INSERT OR IGNORE INTO routes VALUES (?,?)',(row['id'],route[0]))
                count += 1
    return count


def refresh_searches(db, searches):
    # Recheck listings only. Completed/restricted detail tasks are never reset.
    with db:
        db.execute("UPDATE tasks SET state='pending',last_error=NULL WHERE kind='search' AND state IN ('done','incomplete','capped')")
        for search in searches:
            bulk.enqueue(db,search_url(search['params']),'search',search['route'],search['params'])


async def run_live(db, config, args):
    async with bulk.Browser(args.output,args.timeout,args.workers,args.transport) as browser:
        async def fetch(url):
            try:
                return await browser.fetch(url)
            except Exception as exc:
                # Preserve an actionable error category without Playwright's
                # request headers/cookies, both in console and checkpoint.
                reason = next((s for s in ('ECONNRESET','ETIMEDOUT','timeout','closed')
                               if s.casefold() in str(exc).casefold()),type(exc).__name__)
                raise RuntimeError('HH transport failure: '+reason) from None
        return await bulk.run_queue_async(db,config,args,fetch,args.output)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--run',action='store_true')
    modes.add_argument('--manual-check',action='store_true')
    modes.add_argument('--export-only',action='store_true')
    parser.add_argument('--baseline',type=Path,default=ROOT/'data/hh_bulk')
    parser.add_argument('--output',type=Path,default=ROOT/'data/hh_fast')
    parser.add_argument('--config',type=Path,default=CONFIG_DIR/'hh_config.json')
    parser.add_argument('--extend-searches',action='store_true')
    parser.add_argument('--refresh-searches',action='store_true')
    parser.add_argument('--workers',type=int,default=4)
    parser.add_argument('--delay',type=float,default=.5)
    parser.add_argument('--limit',type=int,default=20000)
    parser.add_argument('--max-requests',type=int,default=10000)
    parser.add_argument('--pages',type=int,default=100)
    parser.add_argument('--timeout',type=float,default=30)
    args = parser.parse_args(argv)
    if not 1 <= args.workers <= 4 or args.delay < .5 or min(args.limit,args.max_requests,args.pages,args.timeout) <= 0:
        parser.error('workers: 1..4; delay >= 0.5; positive limits/timeouts required')
    args.baseline,args.output = args.baseline.resolve(),args.output.resolve()
    if (args.output==args.baseline or args.baseline in args.output.parents or args.output in args.baseline.parents):
        parser.error('Use an output folder separate from the baseline folder')
    config = load_config(args.config)
    if config.get('date_from') or config.get('date_to'):
        parser.error('HTML date filters are not supported')
    searches = build_searches(config)
    if not (args.run or args.manual_check or args.export_only):
        with contextlib.closing(readonly(args.baseline/'queue.sqlite3')) as source:
            state = bulk.stats(source)
        print(bulk.encoded(dict(mode='dry_run',network_requests=0,baseline=str(args.baseline),output=str(args.output),
            existing=state,searches=len(searches),workers=args.workers,start_interval_seconds=args.delay,
            theoretical_request_starts_per_second=round(1/args.delay,2),
            theoretical_full_cards_per_minute=round(60/args.delay,1),
            speed_is_unmeasured_on_hh=True,full_descriptions=True)))
        return 0
    args.transport,args.discovery_every,args.checkpoint_every = 'http',5,0
    db,status,code,ready = None,'error',0,False
    try:
        active = other_collectors()
        if active:
            raise RuntimeError(f'Another HH collector is running (PID {active}). Wait for it to finish; it was not interrupted.')
        with output_lock(args.output):
            clone_baseline(args.baseline,args.output)
            db = bulk.connect(args.output/'queue.sqlite3')
            try:
                pages = db.execute("SELECT value FROM meta WHERE key='pages'").fetchone()
                if pages and int(pages[0]) != args.pages:
                    raise ValueError('Keep the same --pages as the copied baseline checkpoint')
                bulk.seed(db,config,searches,extend=args.extend_searches)
                ready = True
                reused = sync_completed(db,args.baseline)
                print(f'Reused {reused} additional completed IDs from baseline; duplicate full cards will not be downloaded.',flush=True)
                (args.output/'config.json').write_text(json.dumps(config,ensure_ascii=False,indent=2),encoding='utf-8')
                if args.refresh_searches:
                    refresh_searches(db,searches)
                status = 'export_only'
                if args.manual_check:
                    status = asyncio.run(bulk.manual_check(db,args))
                elif args.run:
                    if bulk.stats(db)['full_cards'] >= args.limit:
                        status = 'limit_reached'
                    elif not db.execute("SELECT 1 FROM tasks WHERE state='pending' LIMIT 1").fetchone():
                        status = 'queue_exhausted'
                        print('No pending work. --refresh-searches rechecks listings for new IDs; it does not expand company coverage.',flush=True)
                    else:
                        status = asyncio.run(run_live(db,config,args))
            except KeyboardInterrupt:
                status,code = 'interrupted',130
            except bulk.AccessCheckRequired:
                status,code = 'access_check_required',1
                print('HH requests a manual check. Run hh_fast.py --manual-check.',flush=True)
            except Exception as exc:
                if not ready:
                    raise
                status,code = 'error',1
                # Playwright errors can contain cookies. Do not print call logs.
                print(f'Fast collection stopped ({type(exc).__name__}); progress is saved.',flush=True)
            if ready:
                manifest = bulk.export(db,args.output,config,status)
                print(bulk.encoded(manifest),flush=True)
                print('Fast checkpoint and exports:',args.output,flush=True)
            return code
    except (RuntimeError,OSError,ValueError,subprocess.SubprocessError) as exc:
        print(str(exc),file=sys.stderr)
        return 1
    finally:
        if db is not None:
            db.close()


if __name__ == '__main__':
    raise SystemExit(main())

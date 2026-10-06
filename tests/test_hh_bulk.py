"""Checkpoint, resumption, access failure and export checks without network."""

import contextlib
import asyncio
import copy
import csv
import io
import json
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch, AsyncMock

import hh_bulk
from hh_pipeline import build_searches, load_config

ROOT = Path(__file__).resolve().parents[1]
OBSERVED = "2026-10-04T20:00:00+00:00"


def listing(ids, more=False):
    return json.dumps({"redirectConfig": {}, "vacancySearchResult": {
        "vacancies": [{"vacancyId": int(v), "name": "Engineer", "company": {"id": 219911, "name": "Fixture"}} for v in ids],
        "totalResults": len(ids), "paging": {"next": {"disabled": not more}}}})


def detail(vid):
    return json.dumps({"redirectConfig": {}, "vacancyView": {"vacancyFull": {"vacancy": {
        "vacancyId": int(vid), "name": "Engineer", "company": {"id": 219911, "name": "Fixture"},
        "description": "<p>Engineering</p>"}}}})


class BulkTests(unittest.TestCase):
    def setUp(self):
        self.config = load_config(ROOT / "configs" / "hh_config.json")
        self.searches = build_searches(self.config, employer_id="219911")
        self.db = hh_bulk.connect(":memory:")
        self.addCleanup(self.db.close)
        hh_bulk.seed(self.db, self.config, self.searches)
        self.task = self.db.execute("SELECT * FROM tasks").fetchone()
        self.args = SimpleNamespace(limit=10, max_requests=10, pages=100, delay=0)

    def process_listing(self, ids, more=False, pages=100):
        hh_bulk.process(self.db, self.task, listing(ids, more), OBSERVED, self.config, pages)

    def test_dry_run_has_no_side_effects(self):
        with patch("sys.argv", ["hh_bulk.py"]), patch("hh_bulk.connect", side_effect=AssertionError("DB forbidden")), \
                patch.object(Path, "mkdir", side_effect=AssertionError("Write forbidden")), \
                patch("hh_bulk.Browser", side_effect=AssertionError("Network forbidden")), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(hh_bulk.main(), 0)

    def test_pagination_dedup_and_multiple_routes(self):
        self.process_listing(["123", "124"], more=True)
        page1 = self.db.execute("SELECT * FROM tasks WHERE kind='search' AND page=1").fetchone()
        self.assertIsNotNone(page1)
        hh_bulk.process(self.db, page1, listing(["124", "125"]), OBSERVED, self.config, 100)
        with self.db:
            record = json.loads(self.db.execute("SELECT index_json FROM vacancies WHERE id='124'").fetchone()[0])
            hh_bulk.remember_index(self.db, record, "query:ГОЗ")
        self.assertEqual(hh_bulk.stats(self.db)["unique_listed"], 3)
        self.assertEqual(self.db.execute("SELECT count(*) FROM tasks WHERE kind='detail'").fetchone()[0], 3)
        self.assertEqual(self.db.execute("SELECT count(*) FROM routes WHERE vacancy_id='124'").fetchone()[0], 2)

    def test_page_ceiling_does_not_claim_complete_coverage(self):
        self.process_listing(["123"], more=True, pages=1)
        self.assertEqual(hh_bulk.stats(self.db)["tasks_capped"], 1)
        self.assertEqual(self.db.execute("SELECT count(*) FROM tasks WHERE page=1").fetchone()[0], 0)

    def test_disabled_pagination_before_total_is_reported_as_incomplete(self):
        content=json.loads(listing(['123']))
        content['vacancySearchResult']['totalResults']=1000
        hh_bulk.process(self.db,self.task,json.dumps(content),OBSERVED,self.config,100)
        self.assertEqual(hh_bulk.stats(self.db)['tasks_incomplete'],1)

    def test_malformed_page_does_not_advance_checkpoint(self):
        with self.assertRaises(ValueError):
            hh_bulk.process(self.db, self.task, "Captcha", OBSERVED, self.config, 100)
        self.assertEqual(self.db.execute("SELECT state FROM tasks").fetchone()[0], "pending")
        self.assertEqual(hh_bulk.stats(self.db)["unique_listed"], 0)

    def test_write_failure_rolls_back_whole_page(self):
        original = hh_bulk.remember_index
        def interrupted_write(db, record, route):
            original(db, record, route)
            if record["source_vacancy_id"] == "124":
                raise RuntimeError("Simulated interrupted page transaction")
        with patch("hh_bulk.remember_index", side_effect=interrupted_write), self.assertRaises(RuntimeError):
            self.process_listing(["123", "124"], more=True)
        self.assertEqual(hh_bulk.stats(self.db)["unique_listed"], 0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM routes").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM tasks").fetchone()[0], 1)
        self.assertEqual(self.db.execute("SELECT state FROM tasks").fetchone()[0], "pending")

    def test_denied_request_stops_once_and_keeps_task_for_resume(self):
        def denied(url):
            return "Forbidden", {"statusCode": 403, "url": url, "observed_at": OBSERVED}
        with patch("hh_bulk.save_snapshot", return_value=("raw/error.html.gz", "hash")), \
                patch("hh_bulk.time.sleep"), patch("hh_bulk.Browser", side_effect=AssertionError("No real browser")), \
                patch("hh_bulk.run_queue", wraps=hh_bulk.run_queue):
            with self.assertRaisesRegex(ValueError, "HTTP 403"):
                hh_bulk.run_queue(self.db, self.config, self.args, denied, ROOT / "tmp")
        task = self.db.execute("SELECT * FROM tasks").fetchone()
        self.assertEqual(task["attempts"], 1)
        self.assertEqual(task["state"], "pending")
        self.assertIn("HTTP 403", task["last_error"])
        self.assertEqual(hh_bulk.stats(self.db)["full_cards"], 0)

    def test_explicit_restricted_card_is_skipped_and_collection_continues(self):
        self.process_listing(['123', '124'])
        denied = ('<html><body><p>Вам недоступна эта вакансия.</p>'
                  '<p>Войдите как пользователь, у которого есть доступ на просмотр, '
                  'либо как работодатель, создавший эту вакансию.</p></body></html>')
        async def fetch(url):
            restricted = url.endswith('/123')
            return (denied if restricted else detail('124')), {
                'statusCode': 403 if restricted else 200, 'url': url, 'observed_at': OBSERVED}
        with patch('hh_bulk.save_snapshot', return_value=('raw/test.html.gz', 'hash')):
            self.assertEqual(asyncio.run(hh_bulk.run_queue_async(
                self.db, self.config, self.args, fetch, ROOT/'tmp')), 'queue_exhausted')
        self.assertEqual(hh_bulk.stats(self.db)['full_cards'], 1)
        self.assertEqual(hh_bulk.stats(self.db)['tasks_restricted'], 1)
        row = self.db.execute("SELECT index_json FROM vacancies WHERE id='123'").fetchone()
        record = json.loads(row[0])
        self.assertEqual(record['source_visibility'], 'access_restricted_by_source')
        self.assertIsNone(self.db.execute("SELECT last_error FROM tasks WHERE url LIKE '%/123'").fetchone()[0])
        # Another search may list the same ID; it must retain the denied status.
        listed = json.loads(listing(['123']))
        rows, _ = hh_bulk.parse_search(json.dumps(listed), self.task['url'], OBSERVED)
        with self.db:
            hh_bulk.remember_index(self.db, rows[0], 'query:ГОЗ')
        record = json.loads(self.db.execute("SELECT index_json FROM vacancies WHERE id='123'").fetchone()[0])
        self.assertEqual(record['detail_status'], 'restricted')

    def test_restricted_detection_ignores_scripts_and_generic_blocks(self):
        denial = ('Вам недоступна эта вакансия. '
                  'Войдите как пользователь, у которого есть доступ на просмотр')
        for content in ('Forbidden', '<html><body>Access denied CAPTCHA</body></html>',
                        '<html><body><script>' + denial + '</script></body></html>'):
            self.assertFalse(hh_bulk.restricted_vacancy_page(content))

    def test_locked_csv_has_complete_alternate_and_other_exports_finish(self):
        self.process_listing(['123'])
        task = self.db.execute("SELECT * FROM tasks WHERE kind='detail'").fetchone()
        hh_bulk.process(self.db, task, detail('123'), OBSERVED, self.config, 100)
        folder = ROOT/'tmp'/('bulk_test_' + uuid.uuid4().hex)
        folder.mkdir(parents=True)
        locked = folder/'vacancies.csv'
        locked.write_text('old export', encoding='utf-8')
        replace = Path.replace
        def deny_locked(path, target):
            if Path(target) == locked:
                raise PermissionError('simulated Windows CSV lock')
            return replace(path, target)
        with patch.object(Path, 'replace', deny_locked):
            manifest = hh_bulk.export(self.db, folder, self.config, 'test')
        self.assertEqual(locked.read_text(encoding='utf-8'), 'old export')
        alternate = folder/manifest['export_files']['vacancies']['csv']
        self.assertNotEqual(alternate, locked)
        with alternate.open(encoding='utf-8-sig', newline='') as stream:
            self.assertEqual(len(list(csv.DictReader(stream))), 1)
        self.assertEqual(manifest['counts']['vacancies'], 1)
        self.assertEqual(manifest['export_warnings'][0]['requested_file'], 'vacancies.csv')
        self.assertTrue((folder/'index.csv').exists())
        self.assertTrue((folder/'errors.jsonl').exists())
        self.assertEqual(json.loads((folder/'manifest.json').read_text(encoding='utf-8'))['counts']['full_cards'], 1)

    def test_unrecoverable_export_error_exits_cleanly(self):
        folder = ROOT/'tmp'/('bulk_test_' + uuid.uuid4().hex)
        db = hh_bulk.connect(':memory:')
        out = io.StringIO()
        with patch('sys.argv', ['hh_bulk.py', '--export-only', '--output', str(folder)]), \
                patch('hh_bulk.connect', return_value=db), \
                patch('hh_bulk.export', side_effect=PermissionError('simulated disk permission')), \
                contextlib.redirect_stdout(out):
            self.assertEqual(hh_bulk.main(), 2)
        self.assertIn('SQLite progress is saved', out.getvalue())

    def test_durable_restart_skips_completed_card(self):
        folder = ROOT / "tmp" / ("bulk_test_" + uuid.uuid4().hex)
        folder.mkdir(parents=True)
        path = folder / "queue.sqlite3"
        db = hh_bulk.connect(path)
        try:
            hh_bulk.seed(db, self.config, self.searches)
            task = db.execute("SELECT * FROM tasks").fetchone()
            hh_bulk.process(db, task, listing(["123", "124"]), OBSERVED, self.config, 100)
            task = db.execute("SELECT * FROM tasks WHERE url='https://hh.ru/vacancy/123'").fetchone()
            hh_bulk.process(db, task, detail("123"), OBSERVED, self.config, 100)
        finally:
            db.close()
        db = hh_bulk.connect(path)
        try:
            hh_bulk.seed(db, self.config, self.searches)
            calls = []
            def fetch(url):
                calls.append(url)
                return detail("124"), {"statusCode": 200, "url": url, "observed_at": OBSERVED}
            with patch("hh_bulk.save_snapshot", return_value=("raw/test.html.gz", "hash")), patch("hh_bulk.time.sleep"):
                self.assertEqual(hh_bulk.run_queue(db, self.config, self.args, fetch, folder), "queue_exhausted")
            self.assertEqual(calls, ["https://hh.ru/vacancy/124"])
            manifest = hh_bulk.export(db, folder, self.config, "test")
            self.assertEqual(manifest["full_cards"], 2)
            self.assertEqual(len((folder / "vacancies.jsonl").read_text(encoding="utf-8").splitlines()), 2)
        finally:
            db.close()

    def test_scope_changes_require_another_database(self):
        with self.assertRaisesRegex(ValueError, "differs"):
            hh_bulk.seed(self.db, self.config, build_searches(self.config, query="ГОЗ"))
        self.assertEqual(self.db.execute("SELECT count(*) FROM tasks").fetchone()[0], 1)

    def test_additive_expansion_preserves_records_and_reclassifies(self):
        self.process_listing(["123"])
        task = self.db.execute("SELECT * FROM tasks WHERE kind='detail'").fetchone()
        hh_bulk.process(self.db, task, detail("123"), OBSERVED, self.config, 100)
        expanded = copy.deepcopy(self.config)
        expanded['search_queries'].append('New defence phrase')
        searches = self.searches + build_searches(expanded, query='New defence phrase')
        hh_bulk.seed(self.db, expanded, searches, extend=True)
        self.assertEqual(hh_bulk.stats(self.db)['full_cards'], 1)
        self.assertEqual(self.db.execute("SELECT attempts FROM tasks WHERE kind='detail'").fetchone()[0], 1)
        self.assertEqual(self.db.execute("SELECT count(*) FROM tasks WHERE state='pending' AND kind='search'").fetchone()[0], 1)
        # The same expansion is idempotent; no duplicate tasks or refetches.
        hh_bulk.seed(self.db, expanded, searches, extend=True)
        self.assertEqual(self.db.execute("SELECT count(*) FROM tasks").fetchone()[0], 3)

    def test_extension_rejects_removals_without_partial_changes(self):
        changed = copy.deepcopy(self.config)
        changed['weak_signal_groups'].pop()
        before = list(self.db.execute('SELECT * FROM tasks'))
        with self.assertRaises(ValueError):
            hh_bulk.seed(self.db, changed, self.searches, extend=True)
        self.assertEqual([tuple(row) for row in before], [tuple(row) for row in self.db.execute('SELECT * FROM tasks')])

    def test_larger_search_pages_keep_cards_and_supersede_old_pending_searches(self):
        small = copy.deepcopy(self.config)
        small['per_page'] = 20
        db = hh_bulk.connect(':memory:')
        try:
            searches = build_searches(small, employer_id='219911')
            hh_bulk.seed(db, small, searches)
            task = db.execute('SELECT * FROM tasks').fetchone()
            hh_bulk.process(db, task, listing(['123'], True), OBSERVED, small, 100)
            hh_bulk.seed(db, self.config, self.searches, extend=True)
            self.assertEqual(hh_bulk.stats(db)['unique_listed'], 1)
            self.assertEqual(db.execute("SELECT count(*) FROM tasks WHERE kind='detail' AND state='pending'").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT count(*) FROM tasks WHERE kind='search' AND state='superseded'").fetchone()[0], 1)
            new_task = db.execute("SELECT * FROM tasks WHERE kind='search' AND state='pending'").fetchone()
            self.assertEqual(json.loads(new_task['params'])['per_page'], 100)
        finally:
            db.close()

    def test_parallel_downloads_respect_card_and_request_limits(self):
        self.process_listing(['123','124','125'])
        self.args.workers, self.args.limit, self.args.max_requests = 2, 2, 2
        active = maximum = 0
        calls = []
        async def fetch(url):
            nonlocal active, maximum
            calls.append(url)
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(.01)
            active -= 1
            return detail(url.rsplit('/',1)[-1]), {'statusCode':200,'url':url,'observed_at':OBSERVED}
        with patch('hh_bulk.save_snapshot',return_value=('raw/test.html.gz','hash')):
            status = asyncio.run(hh_bulk.run_queue_async(self.db,self.config,self.args,fetch,ROOT/'tmp'))
        self.assertEqual(status,'limit_reached')
        self.assertEqual(maximum,2)
        self.assertEqual(len(set(calls)),2)
        self.assertEqual(hh_bulk.stats(self.db)['full_cards'],2)
        self.assertEqual(hh_bulk.stats(self.db)['tasks_pending'],1)

    def test_start_interval_is_shared_by_parallel_workers(self):
        self.process_listing(['123','124','125'])
        self.args.workers, self.args.delay = 2, .03
        starts=[]
        async def fetch(url):
            starts.append(time.monotonic())
            await asyncio.sleep(.04)
            return detail(url.rsplit('/',1)[-1]), {'statusCode':200,'url':url,'observed_at':OBSERVED}
        with patch('hh_bulk.save_snapshot',return_value=('raw/test.html.gz','hash')):
            asyncio.run(hh_bulk.run_queue_async(self.db,self.config,self.args,fetch,ROOT/'tmp'))
        self.assertEqual(len(starts),3)
        self.assertTrue(all(b-a >= .025 for a,b in zip(starts,starts[1:])))

    def test_access_denial_cancels_other_worker_and_leaves_tasks_pending(self):
        self.process_listing(['123','124','125'])
        self.args.workers = 2
        cancelled = []
        calls = []
        async def fetch(url):
            calls.append(url)
            if url.endswith('123'):
                await asyncio.sleep(.01)
                return 'Forbidden', {'statusCode':403,'url':url,'observed_at':OBSERVED}
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled.append(url)
                raise
        with patch('hh_bulk.save_snapshot',return_value=('raw/error.html.gz','hash')), self.assertRaisesRegex(ValueError,'HTTP 403'):
            asyncio.run(hh_bulk.run_queue_async(self.db,self.config,self.args,fetch,ROOT/'tmp'))
        self.assertEqual(len(calls),2)
        self.assertEqual(cancelled,['https://hh.ru/vacancy/124'])
        self.assertEqual(hh_bulk.stats(self.db)['full_cards'],0)
        self.assertEqual(hh_bulk.stats(self.db)['tasks_pending'],3)

    def test_discovery_does_not_wait_for_all_details(self):
        self.process_listing(['123','124','125'])
        with self.db:
            params = dict(self.searches[0]['params'], text='new', employer_id=None)
            params.pop('employer_id')
            hh_bulk.enqueue(self.db,hh_bulk.search_url(params),'search','query:new',params)
        task = hh_bulk.next_task(self.db,{},prefer_search=True)
        self.assertEqual(task['kind'],'search')
        task = hh_bulk.next_task(self.db,{},prefer_search=False)
        self.assertEqual(task['kind'],'detail')

    def test_expired_card_does_not_stop_remaining_queue(self):
        self.process_listing(["123", "124"])
        def fetch(url):
            return ("Gone", {"statusCode": 410, "url": url, "observed_at": OBSERVED}) if url.endswith("123") else \
                   (detail("124"), {"statusCode": 200, "url": url, "observed_at": OBSERVED})
        with patch("hh_bulk.save_snapshot", return_value=("raw/test.html.gz", "hash")), patch("hh_bulk.time.sleep"):
            self.assertEqual(hh_bulk.run_queue(self.db, self.config, self.args, fetch, ROOT / "tmp"), "queue_exhausted")
        self.assertEqual(hh_bulk.stats(self.db)["full_cards"], 1)
        self.assertEqual(self.db.execute("SELECT state FROM vacancies WHERE id='123'").fetchone()[0], "unavailable")

    def http_browser(self, responses):
        browser = hh_bulk.Browser.__new__(hh_bulk.Browser)
        browser.transport, browser.timeout = 'http', 30
        request = SimpleNamespace(get=AsyncMock(side_effect=responses))
        browser.session = SimpleNamespace(context=SimpleNamespace(request=request))
        return browser, request

    def response(self, status, url, content='', location=None):
        return SimpleNamespace(status=status,url=url,headers={'location':location} if location else {},
                               body=AsyncMock(return_value=content.encode()),dispose=AsyncMock())

    def test_regular_redirect_keeps_vacancy_identity(self):
        url='https://hh.ru/vacancy/123'
        destination='https://spb.hh.ru/vacancy/123'
        first=self.response(301,url,location=destination)
        final=self.response(200,destination,detail('123'))
        browser,request=self.http_browser([first,final])
        content,metadata=asyncio.run(browser.fetch(url))
        self.assertEqual(metadata['statusCode'],200)
        self.assertEqual(metadata['url'],destination)
        self.assertEqual(len(metadata['redirects']),1)
        self.assertEqual(request.get.await_count,2)
        self.assertEqual(content,detail('123'))
        first.dispose.assert_awaited_once()
        final.dispose.assert_awaited_once()

    def test_promoted_vacancy_redirect_is_skipped_without_fetching_article(self):
        self.process_listing(['123','124'])
        url='https://hh.ru/vacancy/123'
        response=self.response(301,url,location='/article/30525?utm_source=test')
        browser,request=self.http_browser([response])
        content,metadata=asyncio.run(browser.fetch(url))
        self.assertTrue(metadata['redirect_skipped'])
        request.get.assert_awaited_once()
        task=self.db.execute('SELECT * FROM tasks WHERE url=?',(url,)).fetchone()
        with patch('hh_bulk.save_snapshot',return_value=('raw/redirect.html.gz','hash')):
            self.assertFalse(hh_bulk.accept_result(self.db,task,content,metadata,self.config,100,ROOT/'tmp'))
        self.assertEqual(self.db.execute('SELECT state FROM tasks WHERE url=?',(url,)).fetchone()[0],'redirected')
        self.assertEqual(hh_bulk.stats(self.db)['full_cards'],0)
        self.assertEqual(hh_bulk.stats(self.db)['tasks_pending'],1)

    def test_auth_redirect_and_loop_still_stop(self):
        url='https://hh.ru/vacancy/123'
        for destination in ('https://hh.ru/account/login',url):
            with self.subTest(destination=destination):
                browser,request=self.http_browser([self.response(302,url,location=destination)])
                _,metadata=asyncio.run(browser.fetch(url))
                self.assertTrue(metadata['redirect_error'])
                self.assertFalse(metadata.get('redirect_skipped',False))
                request.get.assert_awaited_once()

    def test_captcha_is_reported_as_manual_check_and_queue_is_preserved(self):
        self.process_listing(['123','124'])
        async def fetch(url):
            return '', {'statusCode':302,'url':url,'observed_at':OBSERVED,
                        'redirect_error':'Redirect requires authentication or CAPTCHA',
                        'redirect_url':'https://hh.ru/account/captcha'}
        with patch('hh_bulk.save_snapshot',return_value=('raw/captcha.html.gz','hash')), \
                self.assertRaises(hh_bulk.AccessCheckRequired):
            asyncio.run(hh_bulk.run_queue_async(self.db,self.config,self.args,fetch,ROOT/'tmp'))
        metrics=json.loads(self.db.execute("SELECT value FROM meta WHERE key='last_run_metrics'").fetchone()[0])
        self.assertEqual(metrics['status'],'access_check_required')
        self.assertEqual(hh_bulk.stats(self.db)['full_cards'],0)
        self.assertEqual(hh_bulk.stats(self.db)['tasks_pending'],2)

    def test_saved_session_contains_only_hh_cookies(self):
        cookies=[{'name':'one','domain':'.hh.ru'}, {'name':'two','domain':'spb.hh.ru'},
                 {'name':'three','domain':'tracker.example'}, {'name':'four','domain':'hh.ru.evil.example'}]
        self.assertEqual(hh_bulk.hh_cookies({'cookies':cookies}),cookies[:2])

    def test_manual_check_uses_existing_task_and_does_not_collect(self):
        self.process_listing(['123'])
        page=SimpleNamespace(url='https://hh.ru/vacancy/123',goto=AsyncMock(),
                             content=AsyncMock(return_value=detail('123')),close=AsyncMock())
        browser=SimpleNamespace(session=SimpleNamespace(context=SimpleNamespace(new_page=AsyncMock(return_value=page))),
                                save_session=AsyncMock())
        context=AsyncMock()
        context.__aenter__.return_value=browser
        args=SimpleNamespace(output=ROOT/'tmp',timeout=30)
        with patch('hh_bulk.Browser',return_value=context) as factory:
            status=asyncio.run(hh_bulk.manual_check(self.db,args,input_function=lambda prompt:''))
        self.assertEqual(status,'manual_check_complete')
        self.assertFalse(factory.call_args.kwargs['headless'])
        page.goto.assert_awaited_once()
        browser.save_session.assert_awaited_once()
        self.assertEqual(hh_bulk.stats(self.db)['full_cards'],0)
        self.assertEqual(hh_bulk.stats(self.db)['tasks_pending'],1)

    def test_manual_check_explicit_url_works_after_queue_reset(self):
        self.db.execute("DELETE FROM tasks")
        page = SimpleNamespace(url='https://hh.ru/vacancy/123', goto=AsyncMock(),
                               content=AsyncMock(return_value=detail('123')), close=AsyncMock())
        browser = SimpleNamespace(session=SimpleNamespace(context=SimpleNamespace(new_page=AsyncMock(return_value=page))),
                                  save_session=AsyncMock())
        context = AsyncMock()
        context.__aenter__.return_value = browser
        args = SimpleNamespace(output=ROOT/'tmp', timeout=30, check_url=page.url)
        with patch('hh_bulk.Browser', return_value=context):
            status = asyncio.run(hh_bulk.manual_check(self.db, args, input_function=lambda prompt: ''))
        self.assertEqual(status, 'manual_check_complete')
        self.assertEqual(page.goto.call_args.args[0], args.check_url)
        browser.save_session.assert_awaited_once()
        self.assertEqual(hh_bulk.stats(self.db)['full_cards'], 0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0], 0)

    def test_request_budget_and_limit_leave_work_pending(self):
        self.args.max_requests = 1
        def fetch(url):
            return listing(["123", "124"]), {"statusCode": 200, "url": url, "observed_at": OBSERVED}
        with patch("hh_bulk.save_snapshot", return_value=("raw/test.html.gz", "hash")), patch("hh_bulk.time.sleep"):
            self.assertEqual(hh_bulk.run_queue(self.db, self.config, self.args, fetch, ROOT / "tmp"), "request_budget_reached")
        self.assertEqual(hh_bulk.stats(self.db)["tasks_pending"], 2)
        self.args.max_requests, self.args.limit = 10, 1
        calls = []
        def details(url):
            calls.append(url)
            return detail("123"), {"statusCode": 200, "url": url, "observed_at": OBSERVED}
        with patch("hh_bulk.save_snapshot", return_value=("raw/test.html.gz", "hash")), patch("hh_bulk.time.sleep"):
            self.assertEqual(hh_bulk.run_queue(self.db, self.config, self.args, details, ROOT / "tmp"), "limit_reached")
        self.assertEqual(len(calls), 1)
        self.assertEqual(hh_bulk.stats(self.db)["tasks_pending"], 1)


    def night_args(self, hours=8, budget=100):
        return SimpleNamespace(output=ROOT/'tmp',hours=hours,max_requests=budget,limit=1000,
                               workers=2,delay=.5,cooldown_minutes=30,max_access_checks=3)

    def test_night_waits_before_blocked_probe_then_resumes_remaining_budget(self):
        hh_bulk.failure(self.db,self.task,'HH requires CAPTCHA')
        clock=[0]
        waits=[]
        calls=[]
        args=self.night_args(budget=3)
        async def sleep(seconds):
            waits.append(seconds)
            clock[0]+=seconds
        async def runner(db,config,options):
            calls.append((options.priority_url,options.max_requests,options.workers,options.delay))
            with db:
                db.execute("INSERT OR REPLACE INTO meta VALUES ('last_run_metrics',?)",
                           (json.dumps({'requests_started':options.max_requests}),))
                db.execute("UPDATE tasks SET state='done',last_error=NULL")
            return 'request_budget_reached' if options.priority_url else 'queue_exhausted'
        with patch('hh_bulk.export'),patch('hh_bulk.night_event'),patch('hh_bulk.manual_check',side_effect=AssertionError('No human prompts')):
            status=asyncio.run(hh_bulk.run_overnight(self.db,self.config,args,runner,sleep,lambda:clock[0]))
        self.assertEqual(status,'queue_exhausted')
        self.assertEqual(waits,[1800])
        self.assertEqual(calls,[(self.task['url'],1,1,3),(None,2,1,3)])
        summary=json.loads(self.db.execute("SELECT value FROM meta WHERE key='night_run'").fetchone()[0])
        self.assertEqual(summary['requests_started'],3)

    def test_night_caps_access_rechecks_and_preserves_challenged_task(self):
        hh_bulk.failure(self.db,self.task,'HH requires CAPTCHA')
        clock=[0]
        waits=[]
        calls=[]
        async def sleep(seconds):
            waits.append(seconds);clock[0]+=seconds
        async def runner(db,config,options):
            calls.append(options.priority_url)
            with db:
                db.execute("INSERT OR REPLACE INTO meta VALUES ('last_run_metrics',?)",(json.dumps({'requests_started':1}),))
            raise hh_bulk.AccessCheckRequired('CAPTCHA still required')
        with patch('hh_bulk.export'),patch('hh_bulk.night_event'):
            status=asyncio.run(hh_bulk.run_overnight(self.db,self.config,self.night_args(),runner,sleep,lambda:clock[0]))
        self.assertEqual(status,'access_check_required')
        self.assertEqual(waits,[1800,1800,1800])
        self.assertEqual(calls,[self.task['url']]*3)
        self.assertEqual(hh_bulk.stats(self.db)['tasks_pending'],1)

    def test_night_deadline_during_cooldown_does_not_request(self):
        hh_bulk.failure(self.db,self.task,'HH requires CAPTCHA')
        clock=[0]
        async def sleep(seconds):clock[0]+=seconds
        runner=AsyncMock(side_effect=AssertionError('No request after deadline'))
        with patch('hh_bulk.export'),patch('hh_bulk.night_event'):
            status=asyncio.run(hh_bulk.run_overnight(self.db,self.config,self.night_args(hours=.25),runner,sleep,lambda:clock[0]))
        self.assertEqual(status,'time_budget_reached')
        self.assertEqual(clock[0],900)
        runner.assert_not_called()

    def test_scheduler_time_budget_and_periodic_exports(self):
        self.args.deadline=time.monotonic()-1
        fetch=AsyncMock(side_effect=AssertionError('Deadline already reached'))
        status=asyncio.run(hh_bulk.run_queue_async(self.db,self.config,self.args,fetch,ROOT/'tmp'))
        self.assertEqual(status,'time_budget_reached')
        fetch.assert_not_called()
        self.args.deadline=None
        self.args.checkpoint_every=1
        self.process_listing(['123','124'])
        async def fetch(url):return detail(url.rsplit('/',1)[-1]),{'statusCode':200,'url':url,'observed_at':OBSERVED}
        with patch('hh_bulk.save_snapshot',return_value=('raw/test.html.gz','hash')),patch('hh_bulk.export') as export:
            asyncio.run(hh_bulk.run_queue_async(self.db,self.config,self.args,fetch,ROOT/'tmp'))
        self.assertEqual(export.call_count,2)


if __name__ == "__main__":
    unittest.main()

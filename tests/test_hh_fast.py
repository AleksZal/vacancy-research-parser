"""Offline checks for isolation, reuse, and parallel full-card collection."""
import asyncio
import contextlib
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import hh_bulk
import hh_fast
from hh_pipeline import build_searches,load_config
from test_hh_bulk import listing,detail,OBSERVED


class FastTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory(dir=hh_fast.ROOT/'tmp')
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        self.baseline,self.output = self.root/'baseline',self.root/'fast'
        self.baseline.mkdir()
        self.output.mkdir()
        self.config = load_config(hh_fast.CONFIG_DIR/'hh_config.json')
        self.searches = build_searches(self.config,employer_id='219911')
        self.db = hh_bulk.connect(self.baseline/'queue.sqlite3')
        self.addCleanup(self.db.close)
        hh_bulk.seed(self.db,self.config,self.searches)
        self.task = self.db.execute("SELECT * FROM tasks WHERE kind='search'").fetchone()
        hh_bulk.process(self.db,self.task,listing(['123','124','125']),OBSERVED,self.config,100)
        card = self.db.execute("SELECT * FROM tasks WHERE url='https://hh.ru/vacancy/123'").fetchone()
        hh_bulk.process(self.db,card,detail('123'),OBSERVED,self.config,100,'raw/old.html.gz','fixture-digest')

    def clone(self):
        hh_fast.clone_baseline(self.baseline,self.output)
        db = hh_bulk.connect(self.output/'queue.sqlite3')
        self.addCleanup(db.close)
        return db

    def test_clone_preserves_source_and_completed_ids(self):
        before = self.db.execute('SELECT * FROM tasks ORDER BY url').fetchall()
        fast = self.clone()
        self.assertEqual(hh_bulk.stats(fast)['full_cards'],1)
        self.assertEqual(fast.execute("SELECT state FROM tasks WHERE url='https://hh.ru/vacancy/123'").fetchone()[0],'done')
        self.assertEqual(fast.execute("SELECT snapshot FROM tasks WHERE url='https://hh.ru/vacancy/123'").fetchone()[0],str(self.baseline/'raw/old.html.gz'))
        self.assertEqual([tuple(r) for r in before],[tuple(r) for r in self.db.execute('SELECT * FROM tasks ORDER BY url')])
        with self.assertRaises(sqlite3.OperationalError):
            with contextlib.closing(hh_fast.readonly(self.baseline/'queue.sqlite3')) as source:
                source.execute('DELETE FROM vacancies')

    def test_refresh_keeps_completed_detail_and_deduplicates_listed_ids(self):
        fast = self.clone()
        hh_fast.refresh_searches(fast,self.searches)
        task = fast.execute("SELECT * FROM tasks WHERE kind='search' AND state='pending'").fetchone()
        hh_bulk.process(fast,task,listing(['123','124','124','126']),OBSERVED,self.config,100)
        self.assertEqual(fast.execute("SELECT state FROM tasks WHERE url='https://hh.ru/vacancy/123'").fetchone()[0],'done')
        self.assertEqual(fast.execute("SELECT count(*) FROM tasks WHERE url='https://hh.ru/vacancy/124'").fetchone()[0],1)
        self.assertEqual(fast.execute('SELECT count(*) FROM vacancies').fetchone()[0],4)

    def test_sync_reuses_source_progress_since_clone(self):
        fast = self.clone()
        task = self.db.execute("SELECT * FROM tasks WHERE url='https://hh.ru/vacancy/124'").fetchone()
        hh_bulk.process(self.db,task,detail('124'),OBSERVED,self.config,100)
        self.assertEqual(hh_fast.sync_completed(fast,self.baseline),1)
        self.assertEqual(hh_fast.sync_completed(fast,self.baseline),0)
        self.assertEqual(hh_bulk.stats(fast)['full_cards'],2)
        self.assertEqual(fast.execute("SELECT state FROM tasks WHERE url='https://hh.ru/vacancy/124'").fetchone()[0],'done')

    def test_parallel_fetch_never_requests_completed_id(self):
        fast = self.clone()
        calls,active,maximum = [],0,0
        async def fetch(url):
            nonlocal active,maximum
            calls.append(url)
            active += 1
            maximum = max(maximum,active)
            await asyncio.sleep(.03)
            active -= 1
            return detail(url.rsplit('/',1)[-1]),dict(statusCode=200,url=url,observed_at=OBSERVED)
        args = SimpleNamespace(workers=4,delay=0,discovery_every=5,max_requests=10,limit=10,pages=100)
        with contextlib.redirect_stdout(io.StringIO()):
            status = asyncio.run(hh_bulk.run_queue_async(fast,self.config,args,fetch,self.output))
        self.assertEqual(status,'queue_exhausted')
        self.assertEqual(maximum,2)
        self.assertEqual(len(calls),len(set(calls)))
        self.assertNotIn('https://hh.ru/vacancy/123',calls)
        self.assertEqual(hh_bulk.stats(fast)['full_cards'],3)

    def test_active_collector_refused_before_output_or_clone(self):
        with patch('hh_fast.other_collectors',return_value=[42]), \
             patch('hh_fast.clone_baseline',side_effect=AssertionError('Clone forbidden')) as clone, \
             contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(hh_fast.main(['--run','--baseline',str(self.baseline),'--output',str(self.output)]),1)
            clone.assert_not_called()

    def test_dry_run_and_same_output_guard_have_no_writes_or_network(self):
        with patch('hh_fast.other_collectors',side_effect=AssertionError('Not required')), \
             patch('hh_fast.clone_baseline',side_effect=AssertionError('Write forbidden')), \
             patch('hh_bulk.Browser',side_effect=AssertionError('Network forbidden')), \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(hh_fast.main(['--baseline',str(self.baseline),'--output',str(self.output)]),0)
        self.assertFalse((self.output/'queue.sqlite3').exists())
        with self.assertRaises(SystemExit),contextlib.redirect_stderr(io.StringIO()):
            hh_fast.main(['--run','--baseline',str(self.baseline),'--output',str(self.baseline)])


if __name__=='__main__':
    unittest.main()

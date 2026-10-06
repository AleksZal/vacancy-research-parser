"""Offline scope expansion, identity uncertainty and completed-ID reuse."""
import copy
import json
import tempfile
import unittest
import urllib.parse
from pathlib import Path
from unittest.mock import patch

import hh_bulk as bulk
import hh_expand_scope as expansion
from hh_pipeline import build_searches, classify, load_config
from hh_web import classify_record, search_url
from tests.test_hh_bulk import listing, detail, OBSERVED

ROOT = Path(__file__).resolve().parents[1]


class ExpansionTests(unittest.TestCase):
    def setUp(self):
        self.config = load_config(ROOT / 'configs' / 'hh_config.json')

    def expanded(self):
        companies = expansion.merge_companies([
            dict(gur_id='42', name='АО "Завод Пример"', short_name='АО "ЗП"', source_url='https://example.org/42')], [])
        employers = {'999': {'names': {'Завод Пример'}, 'samples': ['777']}}
        return expansion.expand(self.config, companies, employers, [{'text': 'БПЛА'}], [{'id': '1', 'name': 'Москва'}])

    def test_extension_preserves_old_routes_and_uncertain_identity(self):
        config = self.expanded()
        searches = build_searches(config)
        bulk.validate_extension({'config': self.config, 'searches': build_searches(self.config)}, {'config': config, 'searches': searches})
        self.assertEqual(config['employers'], self.config['employers'])
        self.assertEqual(config['candidate_employers'][0]['id'], '999')
        self.assertNotIn('999', {e['id'] for e in config['employers']})
        company = next(s for s in searches if s['route'].startswith('gur_name:'))
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(search_url(company['params'])).query)
        self.assertEqual(query['search_field'], ['company_name'])
        regional = next(s for s in searches if s['route'].startswith('region:'))
        self.assertEqual(regional['params']['area'], ['1'])
        self.assertEqual(regional['params']['text'], 'БПЛА')

    def test_focus_queries_and_signal_strength(self):
        config = self.expanded()
        self.assertEqual(classify({'title':'Бухгалтер','employer_source_id':'999'},config)[0], 'review_employer_identity')
        for description in ('Проектирование баллистических ракет', 'Корректируемые авиационные бомбы', 'Баражирующие боеприпасы'):
            self.assertEqual(classify({'title':'Инженер','description':description},config)[0], 'candidate_strong_signal')
        for description in ('БПЛА для сельского хозяйства', 'Малогабаритные турбореактивные двигатели', 'УМПК', 'КАБ'):
            self.assertEqual(classify({'title':'Инженер','description':description},config)[0], 'review_weak_signal')
        self.assertEqual(classify({'title':'Укладка кабелей и кабельных линий'},config)[0], 'excluded_no_signal')

    def test_company_search_does_not_override_service_exclusion(self):
        record = {'title':'Военнослужащий по контракту','employer_source_id':'999'}
        result = classify_record(record,self.expanded(),'gur_focus:42:fixture')
        self.assertEqual(result['filter_decision'],'excluded_military_service')
        self.assertEqual(result['vpk_status'],'unverified')

    def test_region_and_company_routes_are_deduplicated_by_filters(self):
        config = self.expanded()
        companies = expansion.merge_companies([dict(gur_id='42',name='АО "Завод Пример"',short_name='Завод Пример',source_url='https://example.org/42')],[])
        again = expansion.expand(config,companies,{},[{'text':'БПЛА'}],[{'id':'1','name':'Москва'}])
        self.assertEqual(len(build_searches(config)),len(build_searches(again)))
        keys = [tuple(sorted(urllib.parse.parse_qsl(urllib.parse.urlsplit(search_url(s['params'])).query))) for s in build_searches(again)]
        self.assertEqual(len(keys),len(set(keys)))

    def test_completed_card_is_reviewed_without_reset_or_redownload(self):
        db = bulk.connect(':memory:')
        self.addCleanup(db.close)
        searches = build_searches(self.config,query='ГОЗ')
        bulk.seed(db,self.config,searches)
        task = db.execute('SELECT * FROM tasks').fetchone()
        content = json.loads(listing(['777']))
        content['vacancySearchResult']['vacancies'][0]['company']['id']=888
        bulk.process(db,task,json.dumps(content),OBSERVED,self.config,100)
        task = db.execute("SELECT * FROM tasks WHERE kind='detail'").fetchone()
        card = json.loads(detail('777'))
        card['vacancyView']['vacancyFull']['vacancy']['company']['id']=888
        bulk.process(db,task,json.dumps(card),OBSERVED,self.config,100)
        old = json.loads(db.execute("SELECT record_json FROM vacancies WHERE id='777'").fetchone()[0])
        self.assertEqual(old['filter_decision'],'excluded_no_signal')
        params = {'text':'Завод Пример','search_field':['company_name'],'page':0,'per_page':100,'area':['113']}
        with db:
            bulk.enqueue(db,search_url(params),'search','gur_focus:42:fixture',params)
        company = db.execute("SELECT * FROM tasks WHERE route='gur_focus:42:fixture'").fetchone()
        bulk.process(db,company,json.dumps(content),OBSERVED,self.config,100)
        saved = json.loads(db.execute("SELECT record_json FROM vacancies WHERE id='777'").fetchone()[0])
        self.assertEqual(saved['filter_decision'],'review_gur_employer_search')
        self.assertEqual(saved['description'],old['description'])
        self.assertEqual(db.execute("SELECT state FROM tasks WHERE kind='detail'").fetchone()[0],'done')
        self.assertEqual(bulk.stats(db)['full_cards'],1)

    def test_pending_card_uses_new_company_discovery_evidence(self):
        db = bulk.connect(':memory:')
        self.addCleanup(db.close)
        params = {'text':'ГОЗ','area':['113'],'page':0,'per_page':100}
        with db:
            bulk.enqueue(db,search_url(params),'search','query:ГОЗ',params)
        task = db.execute('SELECT * FROM tasks').fetchone()
        content = json.loads(listing(['778']))
        content['vacancySearchResult']['vacancies'][0]['company']['id']=888
        bulk.process(db,task,json.dumps(content),OBSERVED,self.config,100)
        with db:
            db.execute("INSERT INTO routes VALUES ('778','gur_focus:42:fixture')")
        task=db.execute("SELECT * FROM tasks WHERE kind='detail'").fetchone()
        card=json.loads(detail('778')); card['vacancyView']['vacancyFull']['vacancy']['company']['id']=888
        bulk.process(db,task,json.dumps(card),OBSERVED,self.config,100)
        saved=json.loads(db.execute("SELECT record_json FROM vacancies WHERE id='778'").fetchone()[0])
        self.assertEqual(saved['filter_decision'],'review_gur_employer_search')

    def test_focus_work_is_selected_before_general_work(self):
        db=bulk.connect(':memory:'); self.addCleanup(db.close)
        with db:
            bulk.enqueue(db,search_url({'text':'ГОЗ'}),'search','query:ГОЗ')
            bulk.enqueue(db,search_url({'text':'реактивные БПЛА'}),'search','focus:drones:fixture')
        self.assertEqual(bulk.next_task(db,{},True)['route'],'focus:drones:fixture')

    def test_company_filter_cannot_be_dropped_by_redirect(self):
        original=search_url({'text':'Завод Пример','search_field':['company_name']})
        changed=search_url({'text':'Завод Пример'})
        self.assertEqual(bulk.redirect_decision(original,changed)[0],'stop')

    def test_public_filters_only_and_no_unrestricted_expansion(self):
        for params in ({}, {'text':'БПЛА','internal_flag':'secret'}, {'text':'БПЛА','area':['bad']}, {'text':'БПЛА','search_field':['unknown']}):
            config=copy.deepcopy(self.config)
            config['additional_searches']=[{'route':'fixture','params':params}]
            with tempfile.TemporaryDirectory(dir=ROOT/'tmp') as folder:
                path=Path(folder)/'config.json';path.write_text(json.dumps(config),encoding='utf-8')
                with self.assertRaises(ValueError):load_config(path)


if __name__=='__main__':
    unittest.main()

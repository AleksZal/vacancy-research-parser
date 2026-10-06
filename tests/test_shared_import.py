"""No network: field mapping, raw HTML integrity and bounded write scope."""
import contextlib
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from vacancy_parser import shared_import as importer
from vacancy_parser.paths import PROJECT_ROOT


class SharedImportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=PROJECT_ROOT/'tmp')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for folder in ('configs','data/hh_fast/raw','data/hh_bulk/raw'):
            (self.root/folder).mkdir(parents=True)
        (self.root/'configs/hh_expanded_config.json').write_text('{}',encoding='utf-8')
        self.row = dict(source='hh_html',source_vacancy_id='123',source_url='https://hh.ru/vacancy/123',
            detail_url='https://hh.ru/vacancy/123',title='Engineer',description='Body',
            employer_source_id='99',employer_id_namespace='hh:employer_id',employer_name='Publisher',
            employer_name_detail='Publisher detail',observed_at='2026-10-05T12:00:00+03:00',
            published_at_source='2026-10-04T12:00:00+03:00',filter_decision='candidate_strong_signal',
            salary_from=0,salary_to=None,salary_gross=False,region='Moscow',skills='One, complex skill',
            discovery_route='query:fixture',vpk_status='unverified',filter_matches='fixture',vpk_evidence='original')
        self.write_cards([self.row])
        with contextlib.closing(sqlite3.connect(self.root/'data/hh_fast/queue.sqlite3')) as db:
            db.executescript('CREATE TABLE vacancies(id TEXT,state TEXT); CREATE TABLE routes(vacancy_id TEXT,route TEXT);')
            db.execute('INSERT INTO vacancies VALUES (?,?)',('123','done'))
            db.execute('INSERT INTO routes VALUES (?,?)',('123','employer:99'))
            db.commit()
        body=b'<html>Fixture</html>'
        self.html=self.root/'data/hh_bulk/raw/example.html.gz'
        self.html.write_bytes(gzip.compress(body,mtime=0))
        self.metadata=self.html.with_suffix('.metadata.json')
        self.metadata.write_text(json.dumps(dict(sourceURL=self.row['source_url'],
            observed_at=self.row['observed_at'],sha256=hashlib.sha256(body).hexdigest(),statusCode=200)),encoding='utf-8')

    def write_cards(self,rows):
        (self.root/'data/hh_fast/full_cards.jsonl').write_text(
            ''.join(json.dumps(r)+'\n' for r in rows),encoding='utf-8')

    def prepare(self):
        with patch.object(importer,'PROJECT_ROOT',self.root),patch.object(importer,'CONFIG_DIR',self.root/'configs'):
            return importer.prepare(self.root/'output')

    def test_inherited_html_routes_and_original_values_are_preserved(self):
        cards,profiles,routes,snapshots,paths,plan=self.prepare()
        self.assertEqual(routes['123'],{'query:fixture','employer:99'})
        self.assertEqual(snapshots['123'][0]['path'],'hh_bulk/raw/example.html.gz')
        self.assertEqual(plan['cards_without_profile'],0)
        values=dict(zip(importer.VACANCY_COLUMNS,importer.vacancy_values(cards[0],{('hh:employer_id','99'):42})))
        self.assertEqual(values['source'],'hh')
        self.assertEqual(values['employer_profile_id'],42)
        self.assertEqual(values['salary_from'],0)
        self.assertIs(values['salary_gross'],False)
        self.assertEqual(values['skills_raw'],'One, complex skill')
        self.assertNotIn('public_business_phone',values)
        self.assertEqual(values['locality'],'Moscow')

    def test_corrupt_html_fails_before_any_database_writes(self):
        self.html.write_bytes(gzip.compress(b'changed'))
        with self.assertRaisesRegex(ValueError,'checksum mismatch'):
            self.prepare()

    def test_duplicate_id_is_rejected(self):
        self.write_cards([self.row,self.row])
        with self.assertRaisesRegex(ValueError,'duplicate vacancy'):
            self.prepare()

    def test_unknown_label_is_not_silently_classified(self):
        self.row['filter_decision']='new_unknown_label';self.write_cards([self.row])
        with self.assertRaisesRegex(ValueError,'Unmapped'):
            self.prepare()

    def test_missing_employer_id_does_not_create_a_guessed_profile(self):
        self.row['employer_source_id']='';self.write_cards([self.row])
        cards,profiles,*_=self.prepare()
        self.assertEqual(profiles,{})
        values=dict(zip(importer.VACANCY_COLUMNS,importer.vacancy_values(cards[0],{})))
        self.assertIsNone(values['employer_profile_id'])

    def test_read_only_tables_cannot_be_written_through_bulk_helper(self):
        for name in ('company','vacancy_source','region','vacancy_trudvsem'):
            with self.subTest(name=name),self.assertRaisesRegex(ValueError,'not authorized'):
                importer.insert_rows(None,name,('x',),[(1,)])

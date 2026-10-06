"""Offline checks for the public HTML adapter and collection boundaries."""

import contextlib
import copy
import html
import io
import json
import os
import unittest
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import hh_web
from hh_pipeline import load_config

ROOT = Path(__file__).resolve().parents[1]
OBSERVED = "2026-10-04T19:00:00+00:00"


def html_state(**parts):
    return "<html><body><!--noindex-->" + json.dumps(dict(redirectConfig={}, **parts), ensure_ascii=False) + "</body></html>"


def listing(ids, has_next=False):
    return html_state(vacancySearchResult={"vacancies": [{"vacancyId": int(v), "name": "Fixture", "company": {"id": 219911, "name": "Fixture employer"}} for v in ids],
                                          "totalResults": len(ids), "paging": {"next": {"disabled": not has_next}}})


def detail(vid):
    item = {"vacancyId": int(vid), "name": "Fixture engineer", "company": {"id": 219911, "name": "Fixture employer"},
            "description": "&lt;p&gt;&lt;strong&gt;Обязанности:&lt;/strong&gt;&lt;/p&gt;&lt;ul&gt;&lt;li&gt;Проектирование.&lt;/li&gt;&lt;/ul&gt; &lt;p&gt;Требования:&lt;/p&gt;&lt;ul&gt;&lt;li&gt;Опыт с ГОЗ.&lt;/li&gt;&lt;/ul&gt; &lt;p&gt;Условия:&lt;/p&gt;&lt;ul&gt;&lt;li&gt;Трудовой договор.&lt;/li&gt;&lt;/ul&gt;",
            "area": {"name": "Fixture city"}, "address": {"displayName": "Fixture address"},
            "compensation": {"from": 100000, "to": 120000, "currencyCode": "RUR", "gross": False, "mode": "MONTH"},
            "publicationTimeIso": "2026-10-01T12:00:00+03:00", "status": {"archived": False},
            "keySkills": ["Fixture skill"], "contactInfo": {"contactsHidden": True, "email": "hidden@example.test"}}
    return html_state(vacancyView={"vacancyFull": {"vacancy": item}})


class HHWebTests(unittest.TestCase):
    def setUp(self):
        self.config = load_config(ROOT / "configs" / "hh_config.json")

    def test_dry_run_never_downloads_or_creates_output(self):
        with patch("sys.argv", ["hh_web.py"]), patch("hh_web.download", side_effect=AssertionError("Network forbidden")) as download, \
                patch("urllib.request.urlopen", side_effect=AssertionError("Network forbidden")) as http, \
                patch.object(Path, "mkdir", side_effect=AssertionError("Output forbidden")), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(hh_web.main(), 0)
        download.assert_not_called()
        http.assert_not_called()

    def test_detail_extracts_sections_salary_and_omits_hidden_contacts(self):
        record = hh_web.parse_detail(detail("123"), "https://hh.ru/vacancy/123", OBSERVED)
        self.assertEqual(record["salary_from"], 100000)
        self.assertFalse(record["salary_gross"])
        self.assertEqual(record["salary_currency"], "RUB")
        self.assertEqual(record["responsibilities"], "Проектирование.")
        self.assertEqual(record["requirements"], "Опыт с ГОЗ.")
        self.assertEqual(record["conditions"], "Трудовой договор.")
        self.assertEqual(record["skills"], "Fixture skill")
        self.assertEqual(record["public_business_email"], "")
        self.assertEqual(record["employer_source_id"], "219911")
        self.assertEqual(record["published_at_source"], "2026-10-01T12:00:00+03:00")

    def test_server_html_escaped_template_matches_rendered_state(self):
        rendered = detail("123")
        state = hh_web.page_state(rendered)
        raw = '<html><body><template id="HH-Lux-InitialState">' + html.escape(json.dumps(state)) + '</template></body></html>'
        self.assertEqual(hh_web.page_state(raw), state)
        parsed = hh_web.parse_detail(raw, "https://hh.ru/vacancy/123", OBSERVED)
        expected = hh_web.parse_detail(rendered, "https://hh.ru/vacancy/123", OBSERVED)
        for field in ("description", "responsibilities", "requirements", "employer_source_id", "salary_from", "skills"):
            self.assertEqual(parsed[field], expected[field], field)

    def test_challenge_and_wrong_id_are_not_exported_as_empty_jobs(self):
        for content, url in (("<html>Captcha required</html>", "https://hh.ru/vacancy/123"),
                             (detail("124"), "https://hh.ru/vacancy/123")):
            with self.subTest(url=url), self.assertRaises(ValueError):
                hh_web.parse_detail(content, url, OBSERVED)
        with self.assertRaises(ValueError):
            hh_web.parse_search("<html>Access denied</html>", "https://hh.ru/search/vacancy", OBSERVED)

    def test_search_keeps_index_separate_and_reports_pagination(self):
        rows, report = hh_web.parse_search(listing(["123", "124"], True), "https://hh.ru/search/vacancy?text=ГОЗ&area=113", OBSERVED)
        self.assertEqual(len(rows), 2)
        self.assertTrue(report["has_next"])
        self.assertTrue(all(r["detail_status"] == "listed_only" and r["filter_decision"] == "pending_details" for r in rows))
        url = hh_web.search_url({"employer_id": "219911", "area": ["113"], "per_page": 20, "page": 0, "control_flag": "private"})
        self.assertIn("items_on_page=20", url)
        self.assertNotIn("control_flag", url)
        self.assertNotIn("text=", url)

    def test_only_public_hh_job_pages_are_supported(self):
        for url in ("https://api.hh.ru/vacancies", "https://hh.ru/account/login", "https://example.org/vacancy/123",
                    "http://hh.ru/vacancy/123", "https://user:secret@hh.ru/vacancy/123"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                hh_web.public_url(url)

    def test_live_flow_deduplicates_and_stops_at_budget(self):
        searches = [{"params": {"text": "first"}}, {"params": {"text": "second"}}]
        def fixture_download(url, path, transport, timeout):
            if "/search/" in url:
                content = listing(["1", "2"] if "first" in url else ["1", "3"])
            else:
                content = detail(url.rsplit("/", 1)[-1])
            return content, {"observed_at": OBSERVED, "creditsUsed": 1}
        args = SimpleNamespace(pages=1, delay=2, transport="firecrawl", timeout=30, limit=2)
        manifest = {"searches": [], "source_pages": 0, "firecrawl_credits_reported": 0}
        index, details = [], []
        with patch("hh_web.download", side_effect=fixture_download) as download, patch("hh_web.time.sleep"), \
                patch("urllib.request.urlopen", side_effect=AssertionError("Network forbidden")), contextlib.redirect_stdout(io.StringIO()):
            hh_web.live(args, self.config, searches, ROOT / "tmp" / "not_created", manifest, index, details)
        self.assertEqual(download.call_count, 4)
        self.assertEqual({r["source_vacancy_id"] for r in details}, {"1", "2"})
        self.assertEqual(manifest["stop_reason"], "detail_limit_reached")
        self.assertEqual(manifest["firecrawl_credits_reported"], 4)
        self.assertTrue(all(r["vpk_status"] == "unverified" for r in details))

    def test_firecrawl_failure_stops_without_proxy_escalation_or_key_logging(self):
        error = urllib.error.HTTPError("https://api.firecrawl.dev/v2/scrape", 403, "Forbidden", {}, io.BytesIO(b"Forbidden"))
        with patch.dict(os.environ, {"FIRECRAWL_API_KEY": "fixture-secret"}), patch("urllib.request.urlopen", side_effect=error) as http:
            with self.assertRaises(ValueError) as caught:
                hh_web.download("https://hh.ru/vacancy/123", ROOT / "tmp" / "not_saved.html", "firecrawl", 30)
        self.assertEqual(http.call_count, 1)
        self.assertNotIn("fixture-secret", str(caught.exception))
        request = http.call_args.args[0]
        redirected = hh_web.urllib.request.HTTPRedirectHandler().redirect_request(request, None, 302, "Moved", {}, "https://example.org/")
        self.assertIsNone(redirected.get_header("Authorization"))


if __name__ == "__main__":
    unittest.main()

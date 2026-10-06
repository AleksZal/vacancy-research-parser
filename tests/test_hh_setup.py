"""Offline checks: no site access, only synthetic job records."""

import contextlib
import copy
import io
import json
import os
import unittest
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import collect
from hh_pipeline import build_searches, classify, load_config, run

ROOT = Path(__file__).resolve().parents[1]


class HHSetupTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ, {"HH_ACCESS_TOKEN": "", "HH_USER_AGENT": ""})
        environment.start()
        self.addCleanup(environment.stop)
        self.config = load_config(ROOT / "configs" / "hh_config.json")

    def test_token_is_scoped_to_https_hh_and_not_forwarded_on_redirect(self):
        with patch.dict(os.environ, {"HH_ACCESS_TOKEN": "fixture-token"}), \
                patch("urllib.request.urlopen") as http:
            http.return_value.__enter__.return_value.read.return_value = b'{}'
            for url, authorized in (("https://api.hh.ru/vacancies", True),
                                    ("https://www.fkc-opk.ru/", False),
                                    ("http://api.hh.ru/vacancies", False)):
                collect.fetch(url, ROOT / "tmp" / "not_saved.json", save_success=False)
                request = http.call_args.args[0]
                self.assertEqual(request.get_header("Authorization"), "Bearer fixture-token" if authorized else None)
                if authorized:
                    redirected = collect.urllib.request.HTTPRedirectHandler().redirect_request(
                        request, None, 302, "Moved", {}, "https://example.org/")
                    self.assertIsNone(redirected.get_header("Authorization"))

    def test_reflected_token_is_removed_from_error_report(self):
        token = "fixture-secret-not-for-storage"
        url = "https://api.hh.ru/vacancies"
        body = json.dumps({"errors": [{"type": "forbidden", "reason": token}],
                           "request_id": "fixture-request-id"}).encode()
        error = urllib.error.HTTPError(url, 403, "Forbidden", {}, io.BytesIO(body))
        with patch.dict(os.environ, {"HH_ACCESS_TOKEN": token}), patch.object(Path, "mkdir"), \
                patch.object(Path, "write_text") as write:
            collect.save_http_error(error, url, ROOT / "tmp" / "not_saved.json")
            report = json.dumps(collect.error_info(error))
        self.assertNotIn(token, write.call_args.args[0])
        self.assertNotIn(token, report)
        self.assertIn("[REDACTED]", report)
        self.assertEqual(error.diagnostics["request_id"], "fixture-request-id")

    def test_anonymous_forbidden_suggests_token_without_claiming_exact_cause(self):
        url = "https://api.hh.ru/vacancies"
        error = urllib.error.HTTPError(url, 403, "Forbidden", {}, io.BytesIO(b'{"errors":[{"type":"forbidden"}]}'))
        with patch.object(Path, "mkdir"), patch.object(Path, "write_text"):
            collect.save_http_error(error, url, ROOT / "tmp" / "not_saved.json")
        self.assertIn("anonymous", error.diagnostics["hint"])
        self.assertIn("HH_ACCESS_TOKEN", error.diagnostics["hint"])
        self.assertIn("exact cause is not specified", error.diagnostics["hint"])

    def test_default_and_explicit_dry_run_make_no_requests_or_output_directory(self):
        for extra in ([], ["--dry-run"]):
            with self.subTest(extra=extra):
                output = ROOT / "tmp" / "hh_dry_run_must_not_be_created"
                self.assertFalse(output.exists())
                with patch("sys.argv", ["collect.py", "hh", "--output", str(output), *extra]), \
                        patch("urllib.request.urlopen", side_effect=AssertionError("Network forbidden")) as http, \
                        patch("collect.json_fetch", side_effect=AssertionError("Collection forbidden")) as fetch, \
                        contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(collect.main(), 0)
                    http.assert_not_called()
                    fetch.assert_not_called()
                    self.assertFalse(output.exists())

    def test_access_denial_saves_server_reason_without_retry(self):
        for status in (401, 403, 429):
            with self.subTest(status=status):
                url = "https://api.hh.ru/vacancies?employer_id=219911"
                body = b'{"errors":[{"type":"access","value":"captcha_required"}]}'
                error = urllib.error.HTTPError(url, status, "Forbidden", {}, io.BytesIO(body))
                path = ROOT / "tmp" / "mock_response.json"
                with patch("urllib.request.urlopen", side_effect=error) as http, \
                        patch.object(Path, "mkdir"), patch.object(Path, "write_text") as write, \
                        patch("collect.time.sleep") as sleep:
                    with self.assertRaises(urllib.error.HTTPError) as caught:
                        collect.fetch(url, path)
                self.assertIs(caught.exception, error)
                self.assertEqual(http.call_count, 1)
                sleep.assert_not_called()
                saved = json.loads(write.call_args.args[0])
                self.assertEqual(saved["response_body"], body.decode())
                self.assertEqual(saved["server_errors"][0]["value"], "captcha_required")
                self.assertEqual(saved["http_status"], status)
                self.assertIn("CAPTCHA", collect.error_info(error)["hint"])

    def test_html_denial_does_not_invent_a_captcha_cause(self):
        url = "https://api.hh.ru/vacancies"
        error = urllib.error.HTTPError(url, 403, "Forbidden", {}, io.BytesIO(b"<html>Forbidden</html>"))
        with patch.object(Path, "mkdir"), patch.object(Path, "write_text"):
            collect.save_http_error(error, url, ROOT / "tmp" / "mock_response.json")
        self.assertEqual(error.diagnostics["server_errors"], [])
        self.assertIn("not established", error.diagnostics["hint"])

    def test_access_check_makes_one_search_and_never_runs_collection(self):
        output = MagicMock()
        manifest = {"errors": []}
        args = SimpleNamespace(timeout=25)
        with patch("collect.json_fetch", return_value={"items": [{"id": "123"}], "found": 1}) as fetch, \
                patch("hh_pipeline.run", side_effect=AssertionError("Collection forbidden")) as run_mock, \
                contextlib.redirect_stdout(io.StringIO()):
            result = collect.check_hh_access(args, build_searches(self.config), output, manifest)
        self.assertEqual(result, 0)
        self.assertEqual(fetch.call_count, 1)
        self.assertIn("per_page=1", fetch.call_args.args[0])
        self.assertIn("employer_id=219911", fetch.call_args.args[0])
        self.assertFalse(fetch.call_args.kwargs["save_success"])
        self.assertFalse(fetch.call_args.kwargs["retry_transient"])
        self.assertEqual(manifest["vacancies_saved"], 0)
        run_mock.assert_not_called()

    def test_company_search_does_not_drop_generic_professions(self):
        searches = build_searches(self.config)
        employer_searches = [s for s in searches if s["route"].startswith("employer:")]
        self.assertEqual(len(employer_searches), len(self.config["employers"]))
        self.assertTrue(all("text" not in s["params"] for s in employer_searches))
        decision, matches = classify({"title": "Бухгалтер", "employer_source_id": "219911"}, self.config)
        self.assertEqual(decision, "candidate_seed_employer")
        self.assertTrue(matches)

    def test_service_recruitment_is_excluded_even_for_seed_employer(self):
        decision, _ = classify({"title": "Военнослужащий по контракту", "employer_source_id": "219911"}, self.config)
        self.assertEqual(decision, "excluded_military_service")
        decision, _ = classify({"title": "Врач для обслуживания военнослужащих", "employer_source_id": "219911"}, self.config)
        self.assertEqual(decision, "candidate_seed_employer")

    def test_weak_drone_signal_is_review_not_confirmed_defence(self):
        decision, _ = classify({"title": "Разработчик", "description": "Беспилотные аппараты для сельского хозяйства"}, self.config)
        self.assertEqual(decision, "review_weak_signal")
        decision, _ = classify({"title": "Рабочий", "description": "Копка траншей"}, self.config)
        self.assertEqual(decision, "excluded_no_signal")

    def test_expanded_signals_keep_dual_use_roles_in_review(self):
        cases = [
            ("Проектирование комплексов радиоэлектронной борьбы", "candidate_strong_signal"),
            ("Работа по 275-ФЗ, раздельный учет", "candidate_strong_signal"),
            ("Изготовление продукции военного назначения", "candidate_strong_signal"),
            ("Разработка радиолокационных станций для гражданской авиации", "review_weak_signal"),
            ("Производство дронов для съемки полей", "review_weak_signal"),
            ("Выплаты 275000 рублей, оформление по ФЗ", "excluded_no_signal"),
        ]
        for description, expected in cases:
            with self.subTest(description=description):
                decision, _ = classify({"title":"Инженер", "description":description}, self.config)
                self.assertEqual(decision,expected)
        decision, _ = classify({"title":"Оператор БПЛА (СВО)", "description":"РЭБ и БПЛА"},self.config)
        self.assertEqual(decision,"excluded_military_service")

    def test_spelling_variants_and_generic_secret_work(self):
        decision, _ = classify({"title": "Инженер", "description": "Взаимодействие с военной приёмкой"}, self.config)
        self.assertEqual(decision, "candidate_strong_signal")
        decision, _ = classify({"title": "Инженер", "description": "Конфиденциальные проекты и режимное производство"}, self.config)
        self.assertEqual(decision, "excluded_no_signal")

    def test_duplicate_queries_and_detail_budget_on_synthetic_records(self):
        config = copy.deepcopy(self.config)
        config["employers"] = []
        config["search_queries"] = ["ОПК", "ВПК"]
        searches = build_searches(config)
        details = {
            "1": {"id": "1", "name": "Инженер", "description": "Гособоронзаказ", "employer": {"id": "999", "name": "Fixture company"}},
            "2": {"id": "2", "name": "Разработчик", "description": "БПЛА для аграрного сектора", "employer": {}},
            "3": {"id": "3", "name": "Военнослужащий", "description": "Служба", "employer": {}},
            "4": {"id": "4", "name": "Повар", "description": "Кафе", "employer": {}},
        }
        def fixture_fetch(url, path, **kwargs):
            if "vacancies?" in url:
                return {"items": [{"id": key} for key in details], "found": 4, "pages": 1}
            return details[url.rsplit("/", 1)[-1]]
        for limit, expected in ((100, (1, 1, 2, 4)), (2, (1, 1, 0, 2))):
            with self.subTest(limit=limit):
                manifest = {}
                candidates, review, excluded = [], [], []
                args = SimpleNamespace(limit=limit, pages=1, delay=1.0, timeout=25)
                with patch("collect.json_fetch", side_effect=fixture_fetch), \
                        patch("urllib.request.urlopen", side_effect=AssertionError("Network forbidden")), \
                        patch("hh_pipeline.time.sleep"), contextlib.redirect_stdout(io.StringIO()):
                    run(config, searches, args, ROOT / "tmp" / "hh_mock_run", manifest, candidates, review, excluded)
                self.assertEqual((len(candidates), len(review), len(excluded), manifest["detail_attempts"]), expected)
                self.assertTrue(all(r["vpk_status"] == "unverified" for r in candidates + review))
                if limit == 2:
                    self.assertEqual(manifest["stop_reason"], "detail_limit_reached")


if __name__ == "__main__":
    unittest.main()

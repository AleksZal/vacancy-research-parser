"""Relocation must preserve checkpoint paths, legacy imports and config scope."""
import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from vacancy_parser.paths import CONFIG_DIR, PROJECT_ROOT, resolve_config_path
from vacancy_parser.hh_pipeline import load_config


class ProjectStructureTests(unittest.TestCase):
    def test_legacy_imports_share_the_implementation(self):
        for name in ('collect', 'hh_pipeline', 'hh_web', 'hh_bulk', 'hh_fast',
                     'hh_expand_scope', 'parse_rostvertol'):
            with self.subTest(name=name):
                self.assertIs(importlib.import_module(name),
                              importlib.import_module('vacancy_parser.' + name))

    def test_checkpoint_and_browser_root_remain_unchanged(self):
        for name in ('hh_fast', 'hh_bulk', 'hh_web', 'hh_expand_scope'):
            self.assertEqual(importlib.import_module('vacancy_parser.' + name).ROOT,
                             PROJECT_ROOT)

    def test_old_config_arguments_retain_exact_scope(self):
        for name in ('hh_config.json', 'hh_expanded_config.json'):
            with self.subTest(name=name):
                self.assertEqual(load_config(PROJECT_ROOT / name),
                                 load_config(CONFIG_DIR / name))

    def test_missing_custom_config_is_not_silently_replaced(self):
        with tempfile.TemporaryDirectory(dir=PROJECT_ROOT / 'tmp') as folder:
            path = Path(folder) / 'hh_config.json'
            self.assertEqual(resolve_config_path(path), path)
            with self.assertRaises(FileNotFoundError):
                load_config(path)

    def test_custom_existing_config_takes_precedence(self):
        with tempfile.TemporaryDirectory(dir=PROJECT_ROOT / 'tmp') as folder:
            path = Path(folder) / 'hh_config.json'
            path.write_text('{}', encoding='utf-8')
            self.assertEqual(resolve_config_path(path), path)
            with self.assertRaises(ValueError):
                load_config(path)

    @unittest.skipUnless(os.name == 'nt', 'Windows process checks')
    def test_process_guard_detects_legacy_and_module_launches(self):
        from vacancy_parser import hh_fast
        rows = [dict(ProcessId=100001, ParentProcessId=0, CommandLine='python hh_bulk.py --run'),
                dict(ProcessId=100002, ParentProcessId=0, CommandLine='python -m vacancy_parser.hh_fast --run'),
                dict(ProcessId=100003, ParentProcessId=0, CommandLine='python unrelated.py')]
        result = SimpleNamespace(returncode=0, stdout=json.dumps(rows))
        with patch.object(hh_fast.subprocess, 'run', return_value=result):
            self.assertEqual(hh_fast.other_collectors(), [100001, 100002])

"""Compatibility launcher; implementation lives in vacancy_parser."""
import importlib
import sys

_implementation = importlib.import_module("vacancy_parser.hh_pipeline")

sys.modules[__name__] = _implementation

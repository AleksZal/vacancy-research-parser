"""Compatibility launcher; implementation lives in vacancy_parser."""
import importlib
import sys

_implementation = importlib.import_module("vacancy_parser.hh_expand_scope")

if __name__ == "__main__":
    raise SystemExit(_implementation.main())

sys.modules[__name__] = _implementation

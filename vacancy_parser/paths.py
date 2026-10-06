"""Stable project paths and narrowly scoped legacy config compatibility."""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / "configs"


def resolve_config_path(path):
    """Accept old project-root config arguments without shadowing other paths."""
    path = Path(path)
    if (not path.exists() and path.name in {"hh_config.json", "hh_expanded_config.json"}
            and path.resolve().parent == PROJECT_ROOT):
        return CONFIG_DIR / path.name
    return path

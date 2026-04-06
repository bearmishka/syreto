from __future__ import annotations

import pkgutil
from pathlib import Path
import sys

from .registry import ScriptSpec
from .registry import available_scripts
from .registry import get_script_spec
from .registry import has_script
from .registry import iter_script_specs


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = PACKAGE_ROOT.parent
ANALYSIS_DIR = PROJECT_ROOT / "03_analysis"


def analysis_dir() -> Path:
    return ANALYSIS_DIR


def _ensure_analysis_on_sys_path() -> None:
    entry = str(ANALYSIS_DIR)
    if ANALYSIS_DIR.exists() and entry not in sys.path:
        sys.path.insert(0, entry)


def _extend_submodule_search_path() -> None:
    global __path__

    __path__ = pkgutil.extend_path(__path__, __name__)
    entry = str(ANALYSIS_DIR)
    if ANALYSIS_DIR.exists() and entry not in __path__:
        __path__.append(entry)


def iter_modules() -> list[str]:
    if not ANALYSIS_DIR.exists():
        return []
    return sorted(path.stem for path in ANALYSIS_DIR.glob("*.py") if path.is_file())


_ensure_analysis_on_sys_path()
_extend_submodule_search_path()


__all__ = [
    "ANALYSIS_DIR",
    "ScriptSpec",
    "analysis_dir",
    "available_scripts",
    "get_script_spec",
    "has_script",
    "iter_modules",
    "iter_script_specs",
]
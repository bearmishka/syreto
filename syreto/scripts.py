from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path
from types import ModuleType

from .analysis import analysis_dir as _analysis_dir

ANALYSIS_DIR = _analysis_dir()


def analysis_dir() -> Path:
    return ANALYSIS_DIR


def iter_scripts() -> list[str]:
    if not ANALYSIS_DIR.exists():
        return []
    return sorted(path.stem for path in ANALYSIS_DIR.glob("*.py") if path.is_file())


AVAILABLE_SCRIPTS = tuple(iter_scripts())


def _normalize_script_name(script: str) -> str:
    normalized = str(script).strip()
    if normalized.endswith(".py"):
        normalized = normalized[:-3]
    if not normalized:
        raise ValueError("Script name cannot be empty.")
    return normalized


def script_path(script: str) -> Path:
    normalized = _normalize_script_name(script)
    path = ANALYSIS_DIR / f"{normalized}.py"
    if path.exists():
        return path

    available = ", ".join(iter_scripts())
    raise FileNotFoundError(
        f"Script `{normalized}` not found in `{ANALYSIS_DIR}`. Available scripts: {available}"
    )


def load_script_module(script: str) -> ModuleType:
    normalized = _normalize_script_name(script)
    script_path(normalized)
    module_name = f"syreto.analysis.{normalized}"
    return importlib.import_module(module_name)


def run_script(
    script: str,
    *args: str,
    check: bool = True,
    capture_output: bool = False,
    text: bool = True,
    cwd: str | Path | None = None,
) -> subprocess.CompletedProcess[str]:
    path = script_path(script)
    cmd = [sys.executable, str(path), *[str(argument) for argument in args]]
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd is not None else str(ANALYSIS_DIR),
        check=check,
        capture_output=capture_output,
        text=text,
    )

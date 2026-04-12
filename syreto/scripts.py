from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from types import ModuleType

from .analysis import analysis_dir as _analysis_dir
from .analysis.registry import available_scripts, get_script_spec

ENTRYPOINT_DIR = _analysis_dir()


def analysis_dir() -> Path:
    return ENTRYPOINT_DIR


def iter_scripts() -> list[str]:
    return list(available_scripts())


AVAILABLE_SCRIPTS = tuple(iter_scripts())


def _normalize_script_name(script: str) -> str:
    normalized = str(script).strip()
    if normalized.endswith(".py"):
        normalized = normalized[:-3]
    if not normalized:
        raise ValueError("Script name cannot be empty.")
    return normalized


def script_path(script: str) -> Path:
    return get_script_spec(script).path


def load_script_module(script: str) -> ModuleType:
    return get_script_spec(script).load()


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
        cwd=str(cwd) if cwd is not None else str(ENTRYPOINT_DIR),
        check=check,
        capture_output=capture_output,
        text=text,
    )

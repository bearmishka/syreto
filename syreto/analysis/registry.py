from __future__ import annotations

import importlib
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = PACKAGE_ROOT.parent
ANALYSIS_DIR = PROJECT_ROOT / "03_analysis"


def _normalize_script_name(name: str) -> str:
    normalized = str(name).strip()
    if normalized.endswith(".py"):
        normalized = normalized[:-3]
    if not normalized:
        raise ValueError("Script name cannot be empty.")
    return normalized


@dataclass(frozen=True)
class ScriptSpec:
    name: str
    module_name: str
    path: Path

    def load(self) -> ModuleType:
        return importlib.import_module(self.module_name)


def iter_script_specs() -> list[ScriptSpec]:
    if not ANALYSIS_DIR.exists():
        return []

    specs: list[ScriptSpec] = []
    for path in sorted(ANALYSIS_DIR.glob("*.py")):
        if not path.is_file():
            continue
        name = path.stem
        specs.append(
            ScriptSpec(
                name=name,
                module_name=f"syreto.analysis.{name}",
                path=path,
            )
        )
    return specs


def available_scripts() -> tuple[str, ...]:
    return tuple(spec.name for spec in iter_script_specs())


def has_script(name: str) -> bool:
    target = _normalize_script_name(name)
    return any(spec.name == target for spec in iter_script_specs())


def get_script_spec(name: str) -> ScriptSpec:
    target = _normalize_script_name(name)
    for spec in iter_script_specs():
        if spec.name == target:
            return spec

    available = ", ".join(available_scripts())
    raise FileNotFoundError(
        f"Script `{target}` not found in `{ANALYSIS_DIR}`. Available scripts: {available}"
    )

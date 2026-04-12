from __future__ import annotations

import importlib
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = PACKAGE_ROOT.parent
CANONICAL_SCRIPT_DIR = PACKAGE_ROOT
LEGACY_ENTRYPOINT_DIR = PROJECT_ROOT / "03_analysis"
EXCLUDED_CANONICAL_MODULES = {
    "__init__",
    "artifact_catalog",
    "cli",
    "review_config",
    "scripts",
}


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


def _canonical_script_names() -> list[str]:
    if not CANONICAL_SCRIPT_DIR.exists():
        return []

    names: list[str] = []
    for path in sorted(CANONICAL_SCRIPT_DIR.glob("*.py")):
        if not path.is_file():
            continue
        if path.stem in EXCLUDED_CANONICAL_MODULES:
            continue
        names.append(path.stem)
    return names


def _legacy_only_script_names() -> list[str]:
    if not LEGACY_ENTRYPOINT_DIR.exists():
        return []

    canonical = set(_canonical_script_names())
    names: list[str] = []
    for path in sorted(LEGACY_ENTRYPOINT_DIR.glob("*.py")):
        if not path.is_file():
            continue
        if path.stem in canonical:
            continue
        names.append(path.stem)
    return names


def iter_script_specs() -> list[ScriptSpec]:
    specs: list[ScriptSpec] = []
    for name in _canonical_script_names():
        legacy_entrypoint = LEGACY_ENTRYPOINT_DIR / f"{name}.py"
        execution_path = (
            legacy_entrypoint if legacy_entrypoint.exists() else CANONICAL_SCRIPT_DIR / f"{name}.py"
        )
        specs.append(
            ScriptSpec(
                name=name,
                module_name=f"syreto.{name}",
                path=execution_path,
            )
        )

    for name in _legacy_only_script_names():
        specs.append(
            ScriptSpec(
                name=name,
                module_name=f"syreto.analysis.{name}",
                path=LEGACY_ENTRYPOINT_DIR / f"{name}.py",
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
        f"Script `{target}` not found in `{CANONICAL_SCRIPT_DIR}` or `{LEGACY_ENTRYPOINT_DIR}`. Available scripts: {available}"
    )

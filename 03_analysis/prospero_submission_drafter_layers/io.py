from __future__ import annotations

import json
from pathlib import Path
from typing import Callable


def resolve_cli_paths(
    protocol_arg: str,
    structured_output_arg: str,
    xml_output_arg: str,
    summary_output_arg: str,
) -> tuple[Path, Path, Path, Path]:
    return (
        Path(protocol_arg),
        Path(structured_output_arg),
        Path(xml_output_arg),
        Path(summary_output_arg),
    )


def resolve_preset_manuscript_path(
    *,
    preset: str,
    manuscript_arg: str,
    default_path_for_preset: Callable[[str], str],
) -> Path:
    if manuscript_arg:
        return Path(manuscript_arg)
    return Path(default_path_for_preset(preset))


def read_protocol_text(protocol_path: Path) -> str:
    if not protocol_path.exists():
        raise SystemExit(f"Protocol file not found: {protocol_path}")
    return protocol_path.read_text(encoding="utf-8")


def write_text_artifact(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def load_autofill_profile(profile_path: Path) -> dict[str, str]:
    if not profile_path.exists():
        raise SystemExit(f"Autofill profile not found: {profile_path}")

    try:
        payload = json.loads(profile_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid autofill profile JSON: {profile_path} ({exc})") from exc

    if not isinstance(payload, dict):
        raise SystemExit("Autofill profile must be a JSON object.")

    fields = payload.get("fields", payload)
    if not isinstance(fields, dict):
        raise SystemExit("Autofill profile `fields` must be a JSON object (field_id -> value).")

    normalized: dict[str, str] = {}
    for key, value in fields.items():
        normalized[str(key).strip()] = str(value).strip()

    if "fields" in payload:
        for key, value in payload.items():
            if key == "fields":
                continue
            normalized[str(key).strip()] = str(value).strip()

    return normalized

"""Thin operational CLI for SyReTo.

The CLI is a user-facing shell over existing scripts, artifacts, and review
configuration. It should select, invoke, and summarize system behavior without
becoming the place where epistemic or methodological logic lives.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from importlib.util import find_spec
from pathlib import Path

from .artifact_catalog import (
    artifact_catalog_entries,
    artifact_groups,
    provenance_tracked_artifacts,
    required_artifact_entries,
    sync_artifact_catalog_surfaces,
)
from .csv_schema import schema_contract_paths, validate_csv_schema_contract
from .review_config import ReviewConfig, ReviewConfigError, load_review_config
from .scripts import AVAILABLE_SCRIPTS, run_script, script_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_CATALOG_DOC_PATH = PROJECT_ROOT / "docs" / "artifact-catalog.md"
ARTIFACT_CATALOG_JSON_PATH = PROJECT_ROOT / "artifacts" / "catalog.json"

FAILURE_SEMANTICS = {
    "config error": {
        "severity": "hard-fail",
        "recovery": "manual intervention",
    },
    "missing artifact": {
        "severity": "warning",
        "recovery": "manual intervention",
    },
    "schema violation": {
        "severity": "hard-fail",
        "recovery": "manual intervention",
    },
    "environment problem": {
        "severity": "warning",
        "recovery": "manual intervention",
    },
    "integrity guard failure": {
        "severity": "hard-fail",
        "recovery": "manual intervention",
    },
    "partial run or stale outputs": {
        "severity": "hard-fail",
        "recovery": "clean rerun or manual investigation",
    },
    "rollback state": {
        "severity": "hard-fail",
        "recovery": "manual investigation",
    },
}

ARTIFACT_GROUPS = artifact_groups()
PROVENANCE_TRACKED_ARTIFACTS = provenance_tracked_artifacts()

DOCTOR_REQUIRED_PATHS = (
    ("project root", PROJECT_ROOT),
    ("analysis dir", PROJECT_ROOT / "03_analysis"),
    ("data dir", PROJECT_ROOT / "02_data"),
    ("daily run script", PROJECT_ROOT / "03_analysis/daily_run.sh"),
    ("search log", PROJECT_ROOT / "02_data/processed/search_log.csv"),
    ("master records", PROJECT_ROOT / "02_data/processed/master_records.csv"),
    ("extraction template", PROJECT_ROOT / "02_data/codebook/extraction_template.csv"),
    ("audit log", PROJECT_ROOT / "02_data/processed/audit_log.csv"),
    ("record id map", PROJECT_ROOT / "02_data/processed/record_id_map.csv"),
)

DOCTOR_OPTIONAL_PATHS = (
    ("status summary", PROJECT_ROOT / "outputs/status_summary.json"),
    ("status report", PROJECT_ROOT / "outputs/status_report.md"),
    ("todo action plan", PROJECT_ROOT / "outputs/todo_action_plan.md"),
    ("manuscript dir", PROJECT_ROOT / "04_manuscript"),
)

DAILY_RUN_SCRIPT = PROJECT_ROOT / "03_analysis" / "daily_run.sh"
CURRENT_SPINE_DATA_ROOT = (PROJECT_ROOT / "02_data").resolve()
CURRENT_SPINE_PROTOCOL_ROOT = (PROJECT_ROOT / "01_protocol").resolve()
CURRENT_SPINE_OUTPUTS_ROOT = (PROJECT_ROOT / "03_analysis" / "outputs").resolve()
CURRENT_SPINE_MANUSCRIPT_ROOT = (PROJECT_ROOT / "04_manuscript").resolve()


def parser() -> argparse.ArgumentParser:
    cli_parser = argparse.ArgumentParser(
        prog="syreto",
        description="Run or inspect packaged SYRETO analysis scripts.",
    )
    subparsers = cli_parser.add_subparsers(dest="command")

    subparsers.add_parser(
        "list",
        help="List available analysis scripts.",
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Run an analysis script by name.",
    )
    run_parser.add_argument("script", help="Script name (with or without .py)")
    run_parser.add_argument(
        "script_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed through to the script; use `--` before script flags.",
    )

    path_parser = subparsers.add_parser(
        "path",
        help="Print resolved filesystem path for a script.",
    )
    path_parser.add_argument("script", help="Script name (with or without .py)")

    status_parser = subparsers.add_parser(
        "status",
        help="Inspect the current review status through the packaged status surface.",
    )
    status_parser.add_argument(
        "status_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed through to status_cli; use `--` before status flags.",
    )
    status_parser.add_argument(
        "--config",
        help="Path to review.toml for review-instance-aware status checks.",
    )

    artifacts_parser = subparsers.add_parser(
        "artifacts",
        help="Inspect artifact contracts and trust-bearing operational outputs.",
    )
    artifacts_parser.add_argument(
        "--kind",
        choices=["all", "input", "operational", "manuscript"],
        default="all",
        help="Which artifact group to show.",
    )
    artifacts_parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Show only missing artifacts.",
    )
    artifacts_parser.add_argument(
        "--provenance-missing-only",
        action="store_true",
        help="Show only tracked generated artifacts whose provenance sidecar is missing.",
    )
    artifacts_parser.add_argument(
        "--provenance-invalid-only",
        action="store_true",
        help="Show only tracked generated artifacts whose provenance sidecar exists but fails minimal validation.",
    )
    artifacts_parser.add_argument(
        "--json",
        action="store_true",
        help="Render artifact rows as machine-readable JSON.",
    )
    artifacts_parser.add_argument(
        "--sync-catalog",
        action="store_true",
        help="Sync docs/artifact-catalog.md and artifacts/catalog.json from the canonical Python registry before listing.",
    )

    validate_parser = subparsers.add_parser(
        "validate",
        help="Run packaged validation checks.",
    )
    validate_parser.add_argument(
        "target",
        nargs="?",
        choices=["csv", "extraction", "all"],
        default="all",
        help="Which validation target to run.",
    )
    validate_parser.add_argument(
        "validate_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed through to the validator; use `--` before validator flags.",
    )

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Run the preflight diagnostic for environment, inputs, and trust-bearing outputs.",
    )
    doctor_parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat warnings as failures.",
    )
    doctor_parser.add_argument(
        "--config",
        help="Path to review.toml for review-instance-aware diagnostics.",
    )

    observability_parser = subparsers.add_parser(
        "observability",
        help="Summarize run event observability artifacts.",
    )
    observability_parser.add_argument(
        "--input",
        help="Path to run_events.jsonl. Defaults to the current repository or selected review instance.",
    )
    observability_parser.add_argument(
        "--config",
        help="Path to review.toml for review-instance-aware observability lookup.",
    )
    observability_parser.add_argument(
        "--last",
        type=int,
        default=5,
        help="Number of most recent steps to show.",
    )

    analytics_parser = subparsers.add_parser(
        "analytics",
        help="Run review analytics builders.",
    )
    analytics_subparsers = analytics_parser.add_subparsers(dest="analytics_command")

    analytics_descriptives_parser = analytics_subparsers.add_parser(
        "descriptives",
        help="Build review-state descriptive analytics artifacts.",
    )
    analytics_descriptives_parser.add_argument(
        "analytics_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed through to review_descriptives_builder; use `--` before flags.",
    )

    review_parser = subparsers.add_parser(
        "review",
        help="Run review-level orchestration commands.",
    )
    review_subparsers = review_parser.add_subparsers(dest="review_command")

    review_run_parser = review_subparsers.add_parser(
        "run",
        help="Run the full review pipeline via daily_run.sh.",
    )
    review_run_parser.add_argument(
        "--config",
        help="Path to review.toml for config-aware review execution.",
    )
    review_run_parser.add_argument(
        "review_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed after `--` to the review runner.",
    )

    return cli_parser


def _normalize_passthrough_args(values: list[str]) -> list[str]:
    if values and values[0] == "--":
        return values[1:]
    return values


def _list_scripts() -> int:
    for script in AVAILABLE_SCRIPTS:
        print(script)
    return 0


def _script_path(name: str) -> int:
    try:
        resolved = script_path(name)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    print(str(resolved))
    return 0


def _run_script(name: str, script_args: list[str]) -> int:
    try:
        result = run_script(
            name,
            *_normalize_passthrough_args(script_args),
            check=False,
            capture_output=False,
            text=True,
        )
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    return int(result.returncode)


def _has_passthrough_option(args: list[str], option: str) -> bool:
    return any(value == option or value.startswith(f"{option}=") for value in args)


def _artifact_groups_for_kind(kind: str) -> list[tuple[str, tuple[str, ...]]]:
    if kind == "all":
        return [
            (
                "input",
                tuple(
                    entry.path
                    for entry in artifact_catalog_entries(include_inputs=True)
                    if entry.kind == "input"
                ),
            ),
            ("operational", ARTIFACT_GROUPS["operational"]),
            ("manuscript", ARTIFACT_GROUPS["manuscript"]),
        ]
    if kind == "input":
        return [
            (
                "input",
                tuple(
                    entry.path
                    for entry in artifact_catalog_entries(include_inputs=True)
                    if entry.kind == "input"
                ),
            )
        ]
    return [(kind, ARTIFACT_GROUPS[kind])]


def _provenance_sidecar_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.provenance.json")


def _load_json_object(path: Path) -> dict[str, object]:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid JSON at {path.as_posix()}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"expected JSON object at {path.as_posix()}")
    return parsed


def _validate_provenance_payload(payload: dict[str, object], *, artifact_path: Path) -> None:
    required_string_fields = (
        "artifact_path",
        "generated_at_utc",
        "generated_by",
        "review_mode",
    )
    for field_name in required_string_fields:
        value = payload.get(field_name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"missing or invalid `{field_name}`")

    upstream_inputs = payload.get("upstream_inputs")
    if not isinstance(upstream_inputs, list) or any(
        not isinstance(item, str) or not item.strip() for item in upstream_inputs
    ):
        raise ValueError("missing or invalid `upstream_inputs`")

    recorded_artifact = Path(str(payload["artifact_path"]).strip())
    try:
        expected_artifact = artifact_path.resolve()
    except OSError:
        expected_artifact = artifact_path.absolute()
    try:
        recorded_resolved = recorded_artifact.resolve()
    except OSError:
        recorded_resolved = recorded_artifact.absolute()

    if expected_artifact != recorded_resolved:
        raise ValueError(
            "artifact_path does not match tracked artifact "
            f"({payload['artifact_path']} != {artifact_path.as_posix()})"
        )


def _artifact_line(relative_path: str, *, exists: bool) -> str:
    status = "present" if exists else "missing"
    entry = next(
        (
            item
            for item in artifact_catalog_entries(include_inputs=True)
            if item.path == relative_path
        ),
        None,
    )
    if entry is None:
        return f"- [{status}] {relative_path}"
    consumed_by = ", ".join(entry.consumed_by)
    schema_ref = entry.schema_ref or "none"
    details = (
        f"canonical={str(entry.canonical).lower()} | reproducible={str(entry.reproducible).lower()} "
        f"| required={entry.required} | consumed_by={consumed_by} | schema_ref={schema_ref}"
    )
    if relative_path not in PROVENANCE_TRACKED_ARTIFACTS:
        return f"- [{status}] {relative_path} | {details}"

    path = PROJECT_ROOT / relative_path
    if not exists:
        return f"- [{status}] {relative_path} | provenance=n/a | {details}"

    provenance_exists = _provenance_sidecar_path(path).exists()
    provenance_status = "present" if provenance_exists else "missing"
    return f"- [{status}] {relative_path} | provenance={provenance_status} | {details}"


def _artifact_provenance_problem(relative_path: str) -> str | None:
    if relative_path not in PROVENANCE_TRACKED_ARTIFACTS:
        return None

    artifact_path = PROJECT_ROOT / relative_path
    if not artifact_path.exists():
        return None

    provenance_path = _provenance_sidecar_path(artifact_path)
    if not provenance_path.exists():
        return "missing"

    try:
        payload = _load_json_object(provenance_path)
        _validate_provenance_payload(payload, artifact_path=artifact_path)
    except ValueError:
        return "invalid"
    return None


def _list_artifacts(
    kind: str,
    *,
    missing_only: bool,
    provenance_missing_only: bool,
    provenance_invalid_only: bool,
    as_json: bool,
    sync_catalog: bool,
) -> int:
    if provenance_missing_only and provenance_invalid_only:
        print(
            "`--provenance-missing-only` and `--provenance-invalid-only` cannot be combined.",
            file=sys.stderr,
        )
        return 2

    if sync_catalog:
        sync_artifact_catalog_surfaces(
            doc_path=ARTIFACT_CATALOG_DOC_PATH,
            json_path=ARTIFACT_CATALOG_JSON_PATH,
        )

    rows: list[dict[str, object]] = []
    for group_name, relative_paths in _artifact_groups_for_kind(kind):
        for relative_path in relative_paths:
            entry = next(
                (
                    item
                    for item in artifact_catalog_entries(include_inputs=True)
                    if item.path == relative_path
                ),
                None,
            )
            if entry is None:
                continue
            path = PROJECT_ROOT / relative_path
            exists = path.exists()
            if missing_only and exists:
                continue
            if provenance_missing_only:
                if _artifact_provenance_problem(relative_path) != "missing":
                    continue
            if provenance_invalid_only:
                if _artifact_provenance_problem(relative_path) != "invalid":
                    continue

            provenance_status = None
            if relative_path in PROVENANCE_TRACKED_ARTIFACTS:
                provenance_status = (
                    "missing"
                    if not exists
                    else (
                        "present"
                        if _artifact_provenance_problem(relative_path) is None
                        else _artifact_provenance_problem(relative_path)
                    )
                )

            rows.append(
                {
                    "kind": group_name,
                    "path": relative_path,
                    "present": exists,
                    "producer": entry.producer,
                    "consumed_by": list(entry.consumed_by),
                    "required": entry.required,
                    "canonical": entry.canonical,
                    "schema_ref": entry.schema_ref,
                    "reproducible": entry.reproducible,
                    "human_readable": entry.human_readable,
                    "machine_readable": entry.machine_readable,
                    "regenerable": entry.regenerable,
                    "provenance": provenance_status,
                }
            )

    if not rows:
        if missing_only:
            print("No missing artifacts in the selected group.")
        elif provenance_missing_only:
            print("No tracked artifacts with missing provenance in the selected group.")
        elif provenance_invalid_only:
            print("No tracked artifacts with invalid provenance in the selected group.")
        elif as_json:
            print("[]")
        return 0

    if as_json:
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0

    grouped_rows: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        grouped_rows.setdefault(str(row["kind"]), []).append(row)

    for group_name in ("input", "operational", "manuscript"):
        group_rows = grouped_rows.get(group_name, [])
        if not group_rows:
            continue
        print(f"{group_name}:")
        for row in group_rows:
            print(_artifact_line(str(row["path"]), exists=bool(row["present"])))

    return 0


def _run_validate(target: str, validate_args: list[str]) -> int:
    normalized_args = _normalize_passthrough_args(validate_args)
    targets = {
        "csv": ("validate_csv_inputs",),
        "extraction": ("validate_extraction",),
        "all": ("validate_csv_inputs", "validate_extraction"),
    }[target]

    exit_code = 0
    for script_name in targets:
        result = _run_script(script_name, normalized_args)
        if result != 0:
            exit_code = result
            break

    return exit_code


def _run_status(status_args: list[str], *, config_path: str | None = None) -> int:
    normalized_args = _normalize_passthrough_args(status_args)
    if config_path is None:
        return _run_script("status_cli", normalized_args)

    try:
        review_config = load_review_config(config_path)
    except ReviewConfigError as exc:
        print(
            _doctor_classified_line(
                "error",
                "review config",
                str(exc),
                failure_class="config error",
            ),
            file=sys.stderr,
        )
        return 1

    routed_args = list(normalized_args)
    if not _has_passthrough_option(routed_args, "--input"):
        routed_args.extend(["--input", str(review_config.outputs_root / "status_summary.json")])
    if not _has_passthrough_option(routed_args, "--fail-on"):
        routed_args.extend(["--fail-on", review_config.fail_on])
    if not _has_passthrough_option(routed_args, "--auto-generate-missing"):
        routed_args.append("--no-auto-generate-missing")
    return _run_script("status_cli", routed_args)


def _resolve_run_events_path(*, input_path: str | None, config_path: str | None) -> Path:
    if input_path is not None:
        return Path(input_path).resolve()
    if config_path is not None:
        review_config = load_review_config(config_path)
        return review_config.outputs_root / "run_events.jsonl"
    return PROJECT_ROOT / "03_analysis" / "outputs" / "run_events.jsonl"


def _observability_run_root(events_path: Path) -> Path:
    if events_path.parent.name == "outputs":
        return events_path.parent.parent
    return events_path.parent


def _resolve_observability_artifact_path(events_path: Path, output_ref: str) -> Path:
    candidate = Path(output_ref)
    if candidate.is_absolute():
        return candidate

    run_root = _observability_run_root(events_path)
    if candidate.parts and candidate.parts[0] == "outputs":
        return run_root / candidate
    return run_root / candidate


def _provenance_status_for_artifact(artifact_path: Path) -> str:
    provenance_path = _provenance_sidecar_path(artifact_path)
    if not provenance_path.exists():
        return "missing"
    try:
        payload = _load_json_object(provenance_path)
        _validate_provenance_payload(payload, artifact_path=artifact_path)
    except ValueError:
        return "invalid"
    return "present"


def _summarize_provenance_statuses(statuses: list[str]) -> str:
    present = sum(1 for status in statuses if status == "present")
    missing = sum(1 for status in statuses if status == "missing")
    invalid = sum(1 for status in statuses if status == "invalid")
    return f"tracked={len(statuses)}, present={present}, missing={missing}, invalid={invalid}"


def _load_run_events(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        raise FileNotFoundError(f"Run events file not found: {path}")

    events: list[dict[str, object]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in run events at line {line_number}: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"Invalid run event at line {line_number}: expected JSON object.")
        events.append(parsed)
    return events


def _run_observability(*, input_path: str | None, config_path: str | None, last: int) -> int:
    if last <= 0:
        print("`--last` must be a positive integer.", file=sys.stderr)
        return 2

    try:
        events_path = _resolve_run_events_path(input_path=input_path, config_path=config_path)
        events = _load_run_events(events_path)
    except ReviewConfigError as exc:
        print(
            _doctor_classified_line(
                "error",
                "review config",
                str(exc),
                failure_class="config error",
            ),
            file=sys.stderr,
        )
        return 1
    except FileNotFoundError as exc:
        print(
            _doctor_classified_line(
                "error",
                "run events",
                str(exc),
                failure_class="missing artifact",
            ),
            file=sys.stderr,
        )
        return 1
    except ValueError as exc:
        print(
            _doctor_classified_line(
                "error",
                "run events",
                str(exc),
                failure_class="partial run or stale outputs",
            ),
            file=sys.stderr,
        )
        return 1

    if not events:
        print("SyReTo observability")
        print("")
        print(f"Run events: {events_path}")
        print("Events: 0")
        print("No recorded steps found.")
        return 0

    recent_events = events[-last:]
    success_count = sum(1 for event in events if event.get("status") == "success")
    failure_events = [event for event in events if event.get("status") != "success"]
    total_duration = sum(
        float(event.get("duration", 0.0))
        for event in events
        if isinstance(event.get("duration"), int | float)
    )
    last_event = events[-1]
    last_failure = failure_events[-1] if failure_events else None

    lines = [
        "SyReTo observability",
        "",
        f"Run events: {events_path}",
        f"Run id: {last_event.get('run_id', 'unknown')}",
        f"Review mode: {last_event.get('review_mode', 'unknown')}",
        f"Events: {len(events)}",
        f"Successful steps: {success_count}",
        f"Failed or non-success steps: {len(failure_events)}",
        f"Total recorded duration: {total_duration:.2f}s",
        "",
        "Recent steps",
    ]

    for event in recent_events:
        step = str(event.get("step", "unknown"))
        step_kind = str(event.get("step_kind", "unknown"))
        status = str(event.get("status", "unknown"))
        duration = event.get("duration", 0.0)
        started_at = str(event.get("started_at", "unknown"))
        failure_reason = event.get("failure_reason")
        suffix = f" | failure_reason={failure_reason}" if failure_reason else ""
        lines.append(
            f"- step={step} | kind={step_kind} | status={status} | duration={duration}s | started_at={started_at}{suffix}"
        )

    lines.append("")
    lines.append("Postmortem")
    if last_failure is None:
        lines.append("- No failed steps recorded in the current event stream.")
    else:
        lines.append(
            f"- Last failed step: {last_failure.get('step', 'unknown')} "
            f"({last_failure.get('status', 'unknown')}; kind={last_failure.get('step_kind', 'unknown')})"
        )
        lines.append(f"- Failure reason: {last_failure.get('failure_reason') or 'not provided'}")
        inputs_read = last_failure.get("inputs_read")
        if isinstance(inputs_read, list) and inputs_read:
            lines.append(f"- Inputs read: {', '.join(str(item) for item in inputs_read)}")
        else:
            lines.append("- Inputs read: none recorded")
        outputs_touched = last_failure.get("outputs_touched")
        if isinstance(outputs_touched, list) and outputs_touched:
            lines.append(f"- Outputs touched: {', '.join(str(item) for item in outputs_touched)}")
        else:
            lines.append("- Outputs touched: none recorded")

    touched_outputs: list[str] = []
    for event in recent_events:
        outputs_touched = event.get("outputs_touched")
        if not isinstance(outputs_touched, list):
            continue
        for item in outputs_touched:
            text = str(item).strip()
            if text and text not in touched_outputs:
                touched_outputs.append(text)

    lines.append("")
    lines.append("Provenance snapshot")
    if not touched_outputs:
        lines.append("- No outputs recorded in the recent event slice.")
    else:
        provenance_statuses: list[str] = []
        for output_ref in touched_outputs:
            artifact_path = _resolve_observability_artifact_path(events_path, output_ref)
            provenance_status = _provenance_status_for_artifact(artifact_path)
            provenance_statuses.append(provenance_status)
            lines.append(
                f"- {output_ref}: provenance={provenance_status} ({artifact_path.as_posix()})"
            )
        lines.append(f"- Summary: {_summarize_provenance_statuses(provenance_statuses)}")

    print("\n".join(lines))
    return 0


def _validate_run_config_compatibility(review_config: ReviewConfig) -> None:
    incompatible_paths: list[str] = []
    if review_config.data_root != CURRENT_SPINE_DATA_ROOT:
        incompatible_paths.append(
            f"data_root={review_config.data_root} (expected {CURRENT_SPINE_DATA_ROOT})"
        )
    if review_config.protocol_root != CURRENT_SPINE_PROTOCOL_ROOT:
        incompatible_paths.append(
            f"protocol_root={review_config.protocol_root} (expected {CURRENT_SPINE_PROTOCOL_ROOT})"
        )
    if review_config.outputs_root != CURRENT_SPINE_OUTPUTS_ROOT:
        incompatible_paths.append(
            f"outputs_root={review_config.outputs_root} (expected {CURRENT_SPINE_OUTPUTS_ROOT})"
        )
    if review_config.manuscript_root != CURRENT_SPINE_MANUSCRIPT_ROOT:
        incompatible_paths.append(
            f"manuscript_root={review_config.manuscript_root} (expected {CURRENT_SPINE_MANUSCRIPT_ROOT})"
        )

    if incompatible_paths:
        raise ReviewConfigError(
            "Current daily_run.sh spine only supports repository-aligned paths; "
            f"incompatible config paths: {'; '.join(incompatible_paths)}"
        )

    disabled_stages = [name for name, enabled in review_config.stages.items() if not enabled]
    if disabled_stages:
        raise ReviewConfigError(
            "Current daily_run.sh spine does not yet support stage toggles in review.toml; "
            f"disabled stages found: {', '.join(disabled_stages)}"
        )


def _run_review_pipeline(review_args: list[str], *, config_path: str | None = None) -> int:
    normalized_args = _normalize_passthrough_args(review_args)
    if not DAILY_RUN_SCRIPT.exists():
        print(f"Review runner not found: {DAILY_RUN_SCRIPT}", file=sys.stderr)
        return 2

    env = None
    if config_path is not None:
        try:
            review_config = load_review_config(config_path)
            _validate_run_config_compatibility(review_config)
        except ReviewConfigError as exc:
            print(
                _doctor_classified_line(
                    "error",
                    "review config",
                    str(exc),
                    failure_class="config error",
                ),
                file=sys.stderr,
            )
            return 1

        env = os.environ.copy()
        env["REVIEW_MODE"] = review_config.review_mode
        env["STATUS_FAIL_ON"] = review_config.fail_on
        if review_config.priority_policy:
            env["STATUS_PRIORITY_POLICY"] = review_config.priority_policy
        env["SYRETO_REVIEW_CONFIG"] = str(review_config.config_path)

    result = subprocess.run(
        ["bash", str(DAILY_RUN_SCRIPT), *normalized_args],
        cwd=str(DAILY_RUN_SCRIPT.parent),
        check=False,
        text=True,
        env=env,
    )
    return int(result.returncode)


def _doctor_line(level: str, label: str, detail: str) -> str:
    return f"[{level}] {label}: {detail}"


def _doctor_classified_line(
    level: str,
    label: str,
    detail: str,
    *,
    failure_class: str | None = None,
) -> str:
    if failure_class is None:
        return _doctor_line(level, label, detail)

    semantics = FAILURE_SEMANTICS[failure_class]
    return (
        f"[{level}] {label}: {detail} "
        f"[class={failure_class}; severity={semantics['severity']}; recovery={semantics['recovery']}]"
    )


def _module_available(module_name: str) -> bool:
    return find_spec(module_name) is not None


def _command_available(command_name: str) -> bool:
    return shutil.which(command_name) is not None


def _doctor_schema_contract_paths(
    review_config: ReviewConfig | None,
) -> tuple[tuple[object, Path], ...]:
    data_root = PROJECT_ROOT / "02_data"
    if review_config is not None:
        data_root = review_config.data_root
    return schema_contract_paths(data_root)


def _path_mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except OSError:
        return None


def _doctor_provenance_candidates(
    review_config: ReviewConfig | None,
) -> tuple[tuple[str, Path], ...]:
    project_root = PROJECT_ROOT if review_config is None else review_config.review_root
    candidates: list[tuple[str, Path]] = []
    for relative_path in sorted(PROVENANCE_TRACKED_ARTIFACTS):
        path_obj = Path(relative_path)
        label = path_obj.name.replace("_", " ").replace(".", " ")
        candidates.append((label, project_root / relative_path))
    return tuple(candidates)


def _doctor_required_paths(review_config: ReviewConfig | None) -> tuple[tuple[str, Path], ...]:
    if review_config is None:
        existing_paths = {path.resolve() for _, path in DOCTOR_REQUIRED_PATHS}
        artifact_paths = [
            (entry.path, PROJECT_ROOT / entry.path)
            for entry in required_artifact_entries(include_expected=False)
            if entry.kind == "input" and (PROJECT_ROOT / entry.path).resolve() not in existing_paths
        ]
        return DOCTOR_REQUIRED_PATHS + tuple(
            (f"artifact `{label}`", path) for label, path in artifact_paths
        )

    return (
        ("project root", PROJECT_ROOT),
        ("analysis dir", PROJECT_ROOT / "03_analysis"),
        ("daily run script", PROJECT_ROOT / "03_analysis/daily_run.sh"),
        ("review config", review_config.config_path),
        ("review root", review_config.review_root),
        ("data root", review_config.data_root),
        ("protocol root", review_config.protocol_root),
        ("outputs root", review_config.outputs_root),
        ("manuscript root", review_config.manuscript_root),
    )


def _doctor_optional_paths(review_config: ReviewConfig | None) -> tuple[tuple[str, Path], ...]:
    if review_config is None:
        return DOCTOR_OPTIONAL_PATHS

    return (
        ("status summary", review_config.outputs_root / "status_summary.json"),
        ("status report", review_config.outputs_root / "status_report.md"),
        ("todo action plan", review_config.outputs_root / "todo_action_plan.md"),
        ("run events", review_config.outputs_root / "run_events.jsonl"),
        ("manuscript root", review_config.manuscript_root),
    )


def _doctor_artifact_contract_entries(
    review_config: ReviewConfig | None,
) -> tuple[tuple[object, Path], ...]:
    project_root = PROJECT_ROOT if review_config is None else review_config.review_root
    selected = []
    for entry in artifact_catalog_entries(include_inputs=True):
        if entry.kind == "input" or entry.required != "yes":
            continue
        selected.append((entry, project_root / entry.path))
    return tuple(selected)


def _run_doctor(*, strict: bool, config_path: str | None = None) -> int:
    errors = 0
    warnings = 0
    failure_counts: dict[str, int] = {}
    lines = ["SyReTo doctor", ""]

    def record_failure(failure_class: str) -> None:
        failure_counts[failure_class] = failure_counts.get(failure_class, 0) + 1

    lines.append(f"Version: {getattr(sys.modules.get('syreto'), '__version__', 'unknown')}")
    lines.append(f"Project root: {PROJECT_ROOT}")
    lines.append("")
    lines.append("Preflight question")
    lines.append(
        "- Can this review be run honestly with the current environment, config, inputs, and outputs?"
    )
    lines.append("")

    review_config = None
    if config_path is not None:
        lines.append("Review config")
        try:
            review_config = load_review_config(config_path)
        except ReviewConfigError as exc:
            errors += 1
            record_failure("config error")
            lines.append(
                _doctor_classified_line(
                    "error",
                    "review config",
                    str(exc),
                    failure_class="config error",
                )
            )
            lines.append("")
            lines.append(
                f"Summary: errors={errors}, warnings={warnings}, available_scripts={len(AVAILABLE_SCRIPTS)}"
            )
            print("\n".join(lines))
            return 1

        lines.append(_doctor_line("ok", "review config", review_config.config_path.as_posix()))
        lines.append(_doctor_line("ok", "review id", review_config.review_id))
        lines.append(_doctor_line("ok", "title", review_config.title))
        lines.append(_doctor_line("ok", "review root", review_config.review_root.as_posix()))
        lines.append(_doctor_line("ok", "review mode", review_config.review_mode))
        lines.append(_doctor_line("ok", "status fail_on", review_config.fail_on))
        enabled_stages = (
            ", ".join(name for name, enabled in review_config.stages.items() if enabled) or "none"
        )
        lines.append(_doctor_line("ok", "enabled stages", enabled_stages))
        lines.append("")
        lines.append("Config preflight")
        try:
            _validate_run_config_compatibility(review_config)
            lines.append(
                _doctor_line(
                    "ok",
                    "config compatibility",
                    "compatible with the current repository-aligned orchestration spine",
                )
            )
        except ReviewConfigError as exc:
            errors += 1
            record_failure("config error")
            lines.append(
                _doctor_classified_line(
                    "error",
                    "config compatibility",
                    str(exc),
                    failure_class="config error",
                )
            )
        lines.append("")

    lines.append("Environment")
    uv_path = Path.home() / ".local/bin/uv"
    if uv_path.exists():
        lines.append(_doctor_line("ok", "uv", uv_path.as_posix()))
    else:
        warnings += 1
        record_failure("environment problem")
        lines.append(
            _doctor_classified_line(
                "warn",
                "uv",
                "not found at ~/.local/bin/uv",
                failure_class="environment problem",
            )
        )

    if _command_available("bash"):
        lines.append(_doctor_line("ok", "bash", "available on PATH"))
    else:
        errors += 1
        record_failure("environment problem")
        lines.append(
            _doctor_classified_line(
                "error",
                "bash",
                "not available on PATH",
                failure_class="environment problem",
            )
        )

    if _module_available("pre_commit"):
        lines.append(_doctor_line("ok", "pre-commit", "available in Python environment"))
    else:
        warnings += 1
        record_failure("environment problem")
        lines.append(
            _doctor_classified_line(
                "warn",
                "pre-commit",
                "not importable; use `uv sync --all-groups` or `uv run pre-commit ...`",
                failure_class="environment problem",
            )
        )

    if _module_available("pytest"):
        lines.append(_doctor_line("ok", "pytest", "available in Python environment"))
    else:
        warnings += 1
        record_failure("environment problem")
        lines.append(
            _doctor_classified_line(
                "warn",
                "pytest",
                "not importable in current Python environment",
                failure_class="environment problem",
            )
        )

    lines.append("")
    lines.append("Required tools")
    lines.append(_doctor_line("ok", "tooling posture", "core runtime and developer tools checked"))
    lines.append("")
    lines.append("Required checks")
    required_paths = _doctor_required_paths(review_config)
    for label, path in required_paths:
        if path.exists():
            lines.append(_doctor_line("ok", label, path.as_posix()))
        else:
            errors += 1
            record_failure("missing artifact")
            lines.append(
                _doctor_classified_line(
                    "error",
                    label,
                    f"missing at {path.as_posix()}",
                    failure_class="missing artifact",
                )
            )

    lines.append("")
    lines.append("Schema contract")
    for contract, path in _doctor_schema_contract_paths(review_config):
        if not path.exists():
            continue
        findings = validate_csv_schema_contract(path, contract)
        if not findings:
            lines.append(
                _doctor_line(
                    "ok",
                    f"{contract.label} schema",
                    f"contract checks passed in {path.as_posix()}",
                )
            )
            continue

        for finding in findings:
            if finding.level == "error":
                errors += 1
            else:
                warnings += 1
            record_failure("schema violation")
            lines.append(
                _doctor_classified_line(
                    "error" if finding.level == "error" else "warn",
                    f"{contract.label} schema",
                    f"{finding.detail} ({path.as_posix()})",
                    failure_class="schema violation",
                )
            )

    lines.append("")
    lines.append("Optional checks")
    optional_paths = _doctor_optional_paths(review_config)
    for label, path in optional_paths:
        if path.exists():
            lines.append(_doctor_line("ok", label, path.as_posix()))
        else:
            warnings += 1
            record_failure("missing artifact")
            lines.append(
                _doctor_classified_line(
                    "warn",
                    label,
                    f"not present at {path.as_posix()}",
                    failure_class="missing artifact",
                )
            )

    lines.append("")
    lines.append("Artifact contract")
    contract_entries = _doctor_artifact_contract_entries(review_config)
    canonical_count = 0
    reproducible_count = 0
    present_count = 0
    for entry, path in contract_entries:
        if entry.canonical:
            canonical_count += 1
        if entry.reproducible:
            reproducible_count += 1
        if path.exists():
            present_count += 1
            lines.append(
                _doctor_line(
                    "ok",
                    f"artifact `{entry.path}`",
                    "present; "
                    f"canonical={str(entry.canonical).lower()}; "
                    f"reproducible={str(entry.reproducible).lower()}; "
                    f"schema_ref={entry.schema_ref or 'none'}",
                )
            )
        else:
            warnings += 1
            record_failure("missing artifact")
            lines.append(
                _doctor_classified_line(
                    "warn",
                    f"artifact `{entry.path}`",
                    "missing trust-bearing generated artifact; "
                    f"canonical={str(entry.canonical).lower()}; "
                    f"reproducible={str(entry.reproducible).lower()}; "
                    f"schema_ref={entry.schema_ref or 'none'}",
                    failure_class="missing artifact",
                )
            )
    lines.append(
        _doctor_line(
            "ok",
            "artifact contract summary",
            f"tracked={len(contract_entries)}, present={present_count}, "
            f"canonical={canonical_count}, reproducible={reproducible_count}",
        )
    )

    lines.append("")
    lines.append("Provenance coverage")
    provenance_candidates = _doctor_provenance_candidates(review_config)
    provenance_statuses: list[str] = []
    for label, artifact_path in provenance_candidates:
        if not artifact_path.exists():
            lines.append(
                _doctor_line(
                    "ok",
                    f"{label} provenance",
                    f"artifact not present; no provenance expected at {artifact_path.as_posix()}",
                )
            )
            continue

        provenance_path = _provenance_sidecar_path(artifact_path)
        if provenance_path.exists():
            try:
                provenance_payload = _load_json_object(provenance_path)
                _validate_provenance_payload(provenance_payload, artifact_path=artifact_path)
            except ValueError as exc:
                provenance_statuses.append("invalid")
                warnings += 1
                record_failure("schema violation")
                lines.append(
                    _doctor_classified_line(
                        "warn",
                        f"{label} provenance",
                        str(exc),
                        failure_class="schema violation",
                    )
                )
            else:
                provenance_statuses.append("present")
                lines.append(_doctor_line("ok", f"{label} provenance", provenance_path.as_posix()))
        else:
            provenance_statuses.append("missing")
            warnings += 1
            record_failure("missing artifact")
            lines.append(
                _doctor_classified_line(
                    "warn",
                    f"{label} provenance",
                    f"missing sidecar for generated artifact {artifact_path.as_posix()}",
                    failure_class="missing artifact",
                )
            )
    if provenance_statuses:
        lines.append(
            _doctor_line(
                "ok", "provenance summary", _summarize_provenance_statuses(provenance_statuses)
            )
        )

    lines.append("")
    lines.append("Registry checks")
    for script_name in ("status_cli", "validate_csv_inputs", "validate_extraction"):
        if script_name in AVAILABLE_SCRIPTS:
            lines.append(_doctor_line("ok", f"script `{script_name}`", "registered"))
        else:
            errors += 1
            record_failure("config error")
            lines.append(
                _doctor_classified_line(
                    "error",
                    f"script `{script_name}`",
                    "not registered",
                    failure_class="config error",
                )
            )

    run_failed_marker = PROJECT_ROOT / "outputs/daily_run_failed.marker"
    run_events_path = PROJECT_ROOT / "outputs/run_events.jsonl"
    status_summary_path = PROJECT_ROOT / "outputs/status_summary.json"
    if review_config is not None:
        run_failed_marker = review_config.outputs_root / "daily_run_failed.marker"
        run_events_path = review_config.outputs_root / "run_events.jsonl"
        status_summary_path = review_config.outputs_root / "status_summary.json"
    lines.append("")
    lines.append("Run-state checks")
    if run_failed_marker.exists():
        errors += 1
        record_failure("partial run or stale outputs")
        lines.append(
            _doctor_classified_line(
                "error",
                "daily run failed marker",
                f"present at {run_failed_marker.as_posix()}",
                failure_class="partial run or stale outputs",
            )
        )
    else:
        lines.append(_doctor_line("ok", "daily run failed marker", "not present"))

    if run_events_path.exists():
        lines.append(_doctor_line("ok", "run events", run_events_path.as_posix()))
    else:
        warnings += 1
        record_failure("partial run or stale outputs")
        lines.append(
            _doctor_classified_line(
                "warn",
                "run events",
                f"not present at {run_events_path.as_posix()}",
                failure_class="partial run or stale outputs",
            )
        )

    lines.append("")
    lines.append("Minimal review integrity")
    audit_log_path = PROJECT_ROOT / "02_data/processed/audit_log.csv"
    record_id_map_path = PROJECT_ROOT / "02_data/processed/record_id_map.csv"
    if review_config is not None:
        audit_log_path = review_config.data_root / "processed/audit_log.csv"
        record_id_map_path = review_config.data_root / "processed/record_id_map.csv"

    for label, path in (("audit log", audit_log_path), ("record id map", record_id_map_path)):
        if path.exists():
            lines.append(_doctor_line("ok", label, path.as_posix()))
        else:
            errors += 1
            record_failure("integrity guard failure")
            lines.append(
                _doctor_classified_line(
                    "error",
                    label,
                    f"missing at {path.as_posix()}",
                    failure_class="integrity guard failure",
                )
            )

    lines.append("")
    lines.append("Stale or conflicting outputs")
    input_candidates: list[tuple[Path, float]] = []
    for _, path in required_paths:
        if path.exists():
            mtime = _path_mtime(path)
            if mtime is not None:
                input_candidates.append((path, mtime))

    status_summary_mtime = (
        _path_mtime(status_summary_path) if status_summary_path.exists() else None
    )
    run_events_mtime = _path_mtime(run_events_path) if run_events_path.exists() else None

    if status_summary_mtime is not None and input_candidates:
        newest_input_path, newest_input_mtime = max(input_candidates, key=lambda item: item[1])
        if newest_input_mtime > status_summary_mtime:
            warnings += 1
            record_failure("partial run or stale outputs")
            lines.append(
                _doctor_classified_line(
                    "warn",
                    "status summary freshness",
                    f"older than required input {newest_input_path.as_posix()}",
                    failure_class="partial run or stale outputs",
                )
            )
        else:
            lines.append(
                _doctor_line("ok", "status summary freshness", "not older than required inputs")
            )

    if run_events_mtime is not None and status_summary_mtime is not None:
        if run_events_mtime < status_summary_mtime:
            warnings += 1
            record_failure("partial run or stale outputs")
            lines.append(
                _doctor_classified_line(
                    "warn",
                    "run events freshness",
                    "older than status summary; execution history may be incomplete",
                    failure_class="partial run or stale outputs",
                )
            )
        else:
            lines.append(
                _doctor_line(
                    "ok", "run events freshness", "consistent with current status artifacts"
                )
            )

    if errors == 0 and status_summary_path.exists():
        lines.append(
            _doctor_line(
                "ok",
                "status posture",
                "repository surface is ready for `syreto status` interpretation",
            )
        )
    elif status_summary_path.exists():
        lines.append(
            _doctor_classified_line(
                "warn",
                "status posture",
                "status artifacts exist, but doctor findings mean the run surface still needs review",
                failure_class="partial run or stale outputs",
            )
        )
        warnings += 1
        record_failure("partial run or stale outputs")

    lines.append("")
    lines.append("Next steps")
    if errors > 0:
        lines.append("- Fix missing required paths before trusting pipeline outputs.")
    if not status_summary_path.exists():
        if review_config is None:
            lines.append(
                "- Run `cd 03_analysis && bash daily_run.sh` to generate core status artifacts."
            )
        else:
            lines.append(
                "- Generate review-instance outputs before expecting `syreto status` to report a complete posture for this config."
            )
    if not _module_available("pre_commit"):
        lines.append("- Run `uv sync --all-groups` to ensure development tools are installed.")
    if errors == 0 and warnings == 0:
        lines.append("- Environment and repository surface look healthy.")
    elif errors == 0:
        lines.append(
            "- Repository is usable, but some optional operational signals are still missing."
        )
    if failure_counts:
        lines.append(
            "- Use the failure classes above to decide whether to start with `syreto doctor`, "
            "`syreto validate`, `syreto status`, or the run-state artifacts."
        )

    lines.append("")
    if failure_counts:
        lines.append("Failure classification")
        for failure_class in sorted(failure_counts):
            semantics = FAILURE_SEMANTICS[failure_class]
            lines.append(
                f"- {failure_class}: count={failure_counts[failure_class]}, "
                f"severity={semantics['severity']}, recovery={semantics['recovery']}"
            )
        lines.append("")
    lines.append(
        f"Summary: errors={errors}, warnings={warnings}, available_scripts={len(AVAILABLE_SCRIPTS)}"
    )
    if errors > 0:
        verdict = "not ready for an honest run"
    elif warnings > 0:
        verdict = "ready with warnings"
    else:
        verdict = "ready for an honest run"
    lines.append(f"Preflight verdict: {verdict}")
    print("\n".join(lines))

    if errors > 0:
        return 1
    if strict and warnings > 0:
        return 1
    return 0


def _alias_argv(argv: list[str] | None) -> list[str]:
    if argv is not None:
        return list(argv)
    return list(sys.argv[1:])


def main_status(argv: list[str] | None = None) -> int:
    return _run_status(_alias_argv(argv))


def main_draft(argv: list[str] | None = None) -> int:
    return _run_script("prospero_submission_drafter", _alias_argv(argv))


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    command = args.command or "list"

    if command == "list":
        return _list_scripts()
    if command == "path":
        return _script_path(args.script)
    if command == "run":
        return _run_script(args.script, args.script_args)
    if command == "status":
        return _run_status(args.status_args, config_path=args.config)
    if command == "artifacts":
        return _list_artifacts(
            args.kind,
            missing_only=bool(args.missing_only),
            provenance_missing_only=bool(getattr(args, "provenance_missing_only", False)),
            provenance_invalid_only=bool(getattr(args, "provenance_invalid_only", False)),
            as_json=bool(getattr(args, "json", False)),
            sync_catalog=bool(getattr(args, "sync_catalog", False)),
        )
    if command == "validate":
        return _run_validate(args.target, args.validate_args)
    if command == "doctor":
        return _run_doctor(strict=bool(args.strict), config_path=args.config)
    if command == "observability":
        return _run_observability(
            input_path=getattr(args, "input", None),
            config_path=getattr(args, "config", None),
            last=int(getattr(args, "last", 5)),
        )
    if command == "analytics":
        analytics_command = args.analytics_command or "descriptives"
        if analytics_command == "descriptives":
            return _run_script("review_descriptives_builder", getattr(args, "analytics_args", []))
    if command == "review":
        review_command = args.review_command or "run"
        if review_command == "run":
            return _run_review_pipeline(
                getattr(args, "review_args", []),
                config_path=getattr(args, "config", None),
            )

    raise SystemExit(f"Unknown command: {command}")


if __name__ == "__main__":
    raise SystemExit(main())

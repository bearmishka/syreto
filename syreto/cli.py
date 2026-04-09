from __future__ import annotations

import argparse
import sys
from importlib.util import find_spec
from pathlib import Path

from .scripts import AVAILABLE_SCRIPTS, run_script, script_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

ARTIFACT_GROUPS = {
    "operational": (
        "outputs/status_summary.json",
        "outputs/status_report.md",
        "outputs/todo_action_plan.md",
        "outputs/progress_history.csv",
        "outputs/progress_history_summary.md",
        "outputs/dedup_merge_summary.md",
        "outputs/dedup_stats_summary.md",
        "outputs/epistemic_consistency_report.md",
        "outputs/prisma_flow_diagram.svg",
        "outputs/prisma_flow_diagram.tex",
    ),
    "manuscript": (
        "04_manuscript/tables/prisma_counts_table.tex",
        "04_manuscript/tables/fulltext_exclusion_table.tex",
        "04_manuscript/tables/study_characteristics_table.tex",
        "04_manuscript/tables/grade_evidence_profile_table.tex",
        "04_manuscript/tables/results_summary_table.tex",
        "04_manuscript/tables/decision_trace_table.tex",
        "04_manuscript/tables/analysis_trace_table.tex",
        "04_manuscript/sections/03c_interpretation_auto.tex",
    ),
}

DOCTOR_REQUIRED_PATHS = (
    ("project root", PROJECT_ROOT),
    ("analysis dir", PROJECT_ROOT / "03_analysis"),
    ("data dir", PROJECT_ROOT / "02_data"),
    ("daily run script", PROJECT_ROOT / "03_analysis/daily_run.sh"),
    ("search log", PROJECT_ROOT / "02_data/processed/search_log.csv"),
    ("master records", PROJECT_ROOT / "02_data/processed/master_records.csv"),
    ("extraction template", PROJECT_ROOT / "02_data/codebook/extraction_template.csv"),
)

DOCTOR_OPTIONAL_PATHS = (
    ("status summary", PROJECT_ROOT / "outputs/status_summary.json"),
    ("status report", PROJECT_ROOT / "outputs/status_report.md"),
    ("todo action plan", PROJECT_ROOT / "outputs/todo_action_plan.md"),
    ("manuscript dir", PROJECT_ROOT / "04_manuscript"),
)


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
        help="Run the packaged status CLI.",
    )
    status_parser.add_argument(
        "status_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed through to status_cli; use `--` before status flags.",
    )

    artifacts_parser = subparsers.add_parser(
        "artifacts",
        help="List key operational and manuscript-facing artifacts.",
    )
    artifacts_parser.add_argument(
        "--kind",
        choices=["all", "operational", "manuscript"],
        default="all",
        help="Which artifact group to show.",
    )
    artifacts_parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Show only missing artifacts.",
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
        help="Run a quick repository and pipeline readiness diagnostic.",
    )
    doctor_parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat warnings as failures.",
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


def _artifact_groups_for_kind(kind: str) -> list[tuple[str, tuple[str, ...]]]:
    if kind == "all":
        return [
            ("operational", ARTIFACT_GROUPS["operational"]),
            ("manuscript", ARTIFACT_GROUPS["manuscript"]),
        ]
    return [(kind, ARTIFACT_GROUPS[kind])]


def _list_artifacts(kind: str, *, missing_only: bool) -> int:
    printed_any = False
    for group_name, relative_paths in _artifact_groups_for_kind(kind):
        group_lines: list[str] = []
        for relative_path in relative_paths:
            path = PROJECT_ROOT / relative_path
            exists = path.exists()
            if missing_only and exists:
                continue

            status = "present" if exists else "missing"
            group_lines.append(f"- [{status}] {relative_path}")

        if not group_lines:
            continue

        printed_any = True
        print(f"{group_name}:")
        for line in group_lines:
            print(line)

    if not printed_any and missing_only:
        print("No missing artifacts in the selected group.")

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


def _doctor_line(level: str, label: str, detail: str) -> str:
    return f"[{level}] {label}: {detail}"


def _module_available(module_name: str) -> bool:
    return find_spec(module_name) is not None


def _run_doctor(*, strict: bool) -> int:
    errors = 0
    warnings = 0
    lines = ["SyReTo doctor", ""]

    lines.append(f"Version: {getattr(sys.modules.get('syreto'), '__version__', 'unknown')}")
    lines.append(f"Project root: {PROJECT_ROOT}")
    lines.append("")

    lines.append("Environment")
    uv_path = Path.home() / ".local/bin/uv"
    if uv_path.exists():
        lines.append(_doctor_line("ok", "uv", uv_path.as_posix()))
    else:
        warnings += 1
        lines.append(_doctor_line("warn", "uv", "not found at ~/.local/bin/uv"))

    if _module_available("pre_commit"):
        lines.append(_doctor_line("ok", "pre-commit", "available in Python environment"))
    else:
        warnings += 1
        lines.append(
            _doctor_line(
                "warn",
                "pre-commit",
                "not importable; use `uv sync --all-groups` or `uv run pre-commit ...`",
            )
        )

    if _module_available("pytest"):
        lines.append(_doctor_line("ok", "pytest", "available in Python environment"))
    else:
        warnings += 1
        lines.append(_doctor_line("warn", "pytest", "not importable in current Python environment"))

    lines.append("")
    lines.append("Required checks")
    for label, path in DOCTOR_REQUIRED_PATHS:
        if path.exists():
            lines.append(_doctor_line("ok", label, path.as_posix()))
        else:
            errors += 1
            lines.append(_doctor_line("error", label, f"missing at {path.as_posix()}"))

    lines.append("")
    lines.append("Optional checks")
    for label, path in DOCTOR_OPTIONAL_PATHS:
        if path.exists():
            lines.append(_doctor_line("ok", label, path.as_posix()))
        else:
            warnings += 1
            lines.append(_doctor_line("warn", label, f"not present at {path.as_posix()}"))

    lines.append("")
    lines.append("Registry checks")
    for script_name in ("status_cli", "validate_csv_inputs", "validate_extraction"):
        if script_name in AVAILABLE_SCRIPTS:
            lines.append(_doctor_line("ok", f"script `{script_name}`", "registered"))
        else:
            errors += 1
            lines.append(_doctor_line("error", f"script `{script_name}`", "not registered"))

    lines.append("")
    lines.append("Next steps")
    if errors > 0:
        lines.append("- Fix missing required paths before trusting pipeline outputs.")
    if not (PROJECT_ROOT / "outputs/status_summary.json").exists():
        lines.append(
            "- Run `cd 03_analysis && bash daily_run.sh` to generate core status artifacts."
        )
    if not _module_available("pre_commit"):
        lines.append("- Run `uv sync --all-groups` to ensure development tools are installed.")
    if errors == 0 and warnings == 0:
        lines.append("- Environment and repository surface look healthy.")
    elif errors == 0:
        lines.append(
            "- Repository is usable, but some optional operational signals are still missing."
        )

    lines.append("")
    lines.append(
        f"Summary: errors={errors}, warnings={warnings}, available_scripts={len(AVAILABLE_SCRIPTS)}"
    )
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
    return _run_script("status_cli", _alias_argv(argv))


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
        return _run_script("status_cli", args.status_args)
    if command == "artifacts":
        return _list_artifacts(args.kind, missing_only=bool(args.missing_only))
    if command == "validate":
        return _run_validate(args.target, args.validate_args)
    if command == "doctor":
        return _run_doctor(strict=bool(args.strict))

    raise SystemExit(f"Unknown command: {command}")


if __name__ == "__main__":
    raise SystemExit(main())

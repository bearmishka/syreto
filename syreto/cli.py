from __future__ import annotations

import argparse
import subprocess
import sys
from importlib.util import find_spec
from pathlib import Path

from .review_config import ReviewConfig, ReviewConfigError, load_review_config
from .scripts import AVAILABLE_SCRIPTS, run_script, script_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

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

ARTIFACT_GROUPS = {
    "operational": (
        "outputs/status_summary.json",
        "outputs/status_report.md",
        "outputs/todo_action_plan.md",
        "outputs/review_descriptives.json",
        "outputs/review_descriptives.md",
        "outputs/figures/year_distribution.png",
        "outputs/figures/study_design_distribution.png",
        "outputs/figures/country_distribution.png",
        "outputs/figures/quality_band_distribution.png",
        "outputs/figures/predictor_outcome_heatmap.png",
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

DAILY_RUN_SCRIPT = PROJECT_ROOT / "03_analysis" / "daily_run.sh"


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
    status_parser.add_argument(
        "--config",
        help="Path to review.toml for review-instance-aware status checks.",
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
    doctor_parser.add_argument(
        "--config",
        help="Path to review.toml for review-instance-aware diagnostics.",
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


def _run_review_pipeline(review_args: list[str]) -> int:
    normalized_args = _normalize_passthrough_args(review_args)
    if not DAILY_RUN_SCRIPT.exists():
        print(f"Review runner not found: {DAILY_RUN_SCRIPT}", file=sys.stderr)
        return 2

    result = subprocess.run(
        ["bash", str(DAILY_RUN_SCRIPT), *normalized_args],
        cwd=str(DAILY_RUN_SCRIPT.parent),
        check=False,
        text=True,
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


def _doctor_required_paths(review_config: ReviewConfig | None) -> tuple[tuple[str, Path], ...]:
    if review_config is None:
        return DOCTOR_REQUIRED_PATHS

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
        return _list_artifacts(args.kind, missing_only=bool(args.missing_only))
    if command == "validate":
        return _run_validate(args.target, args.validate_args)
    if command == "doctor":
        return _run_doctor(strict=bool(args.strict), config_path=args.config)
    if command == "analytics":
        analytics_command = args.analytics_command or "descriptives"
        if analytics_command == "descriptives":
            return _run_script("review_descriptives_builder", getattr(args, "analytics_args", []))
    if command == "review":
        review_command = args.review_command or "run"
        if review_command == "run":
            return _run_review_pipeline(getattr(args, "review_args", []))

    raise SystemExit(f"Unknown command: {command}")


if __name__ == "__main__":
    raise SystemExit(main())

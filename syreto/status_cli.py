import argparse
import json
import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
SEVERITY_ORDER = {"minor": 1, "major": 2, "critical": 3}
DEFAULT_CHECKLIST_SEVERITY = "major"
DEFAULT_FAIL_THRESHOLD = "major"
DEFAULT_HEALTH_LEVEL_SEVERITY = {"warning": "major", "error": "critical"}
QUICK_FIX_COMMANDS = {
    "search_totals": "python validate_csv_inputs.py",
    "master_records": "python dedup_merge.py --if-new-exports",
    "master_columns": "python dedup_merge.py --if-new-exports --record-id-map ../02_data/processed/record_id_map.csv",
    "csv_input_validation": "python validate_csv_inputs.py",
    "extraction_validation": "python validate_extraction.py",
    "quality_appraisal": "python quality_appraisal.py",
    "effect_size_conversion": "python effect_size_converter.py",
    "screening_log": "python screening_metrics.py",
    "dual_reviewer": "python reviewer_workload_balancer.py",
    "cohen_kappa": "python screening_disagreement_analyzer.py",
    "semantic_placeholders": "python template_term_guard.py --check-placeholders --no-check-banned-terms --scan-path ../04_manuscript --fail-on-match",
    "prisma_sync": "python dedup_stats.py --flow-backend both --flow-style journal --flow-output outputs/prisma_flow_diagram.svg --apply --backup",
}


def load_summary(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Status summary file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_severity(value: str, *, default: str) -> str:
    lowered = value.strip().lower()
    if lowered in SEVERITY_ORDER:
        return lowered
    return default


def normalize_priority_policy(raw_policy: object) -> dict:
    if not isinstance(raw_policy, dict):
        raw_policy = {}

    checklist_raw = raw_policy.get("checklist_priority")
    if not isinstance(checklist_raw, dict):
        checklist_raw = raw_policy.get("checklist")

    checklist_priority: dict[str, str] = {}
    if isinstance(checklist_raw, dict):
        for item_id_raw, severity_raw in checklist_raw.items():
            item_id = str(item_id_raw).strip()
            if not item_id:
                continue
            checklist_priority[item_id] = normalize_severity(
                str(severity_raw), default=DEFAULT_CHECKLIST_SEVERITY
            )
    if "default" not in checklist_priority:
        checklist_priority["default"] = DEFAULT_CHECKLIST_SEVERITY

    fail_thresholds_raw = raw_policy.get("fail_thresholds")
    if not isinstance(fail_thresholds_raw, dict):
        fail_thresholds_raw = {}

    raw_default_threshold = fail_thresholds_raw.get("default")
    if raw_default_threshold is None:
        warnings_raw = raw_policy.get("warnings")
        if isinstance(warnings_raw, dict):
            raw_default_threshold = warnings_raw.get("default")

    normalized_default_threshold = normalize_severity(
        str(raw_default_threshold if raw_default_threshold is not None else DEFAULT_FAIL_THRESHOLD),
        default=DEFAULT_FAIL_THRESHOLD,
    )
    fail_thresholds = {"default": normalized_default_threshold}

    health_raw = raw_policy.get("health_level_severity")
    if not isinstance(health_raw, dict):
        health_raw = raw_policy.get("health_to_severity")

    health_level_severity: dict[str, str] = {}
    for level, default_severity in DEFAULT_HEALTH_LEVEL_SEVERITY.items():
        raw_severity = None
        if isinstance(health_raw, dict):
            raw_severity = health_raw.get(level)
        health_level_severity[level] = normalize_severity(
            str(raw_severity if raw_severity is not None else default_severity),
            default=default_severity,
        )

    return {
        "checklist_priority": checklist_priority,
        "fail_thresholds": fail_thresholds,
        "health_level_severity": health_level_severity,
    }


def resolve_input_path(path: Path) -> Path:
    if path.is_absolute() or path.exists():
        return path
    return SCRIPT_DIR / path


def resolve_script_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    if path.exists():
        return path.resolve()
    return SCRIPT_DIR / path


def generate_summary_if_missing(
    summary_path: Path,
    *,
    auto_generate_missing: bool,
    status_report_script: Path,
) -> None:
    if summary_path.exists():
        return

    if not auto_generate_missing:
        raise FileNotFoundError(f"Status summary file not found: {summary_path}")

    print(
        f"[status_cli] Missing status summary at `{summary_path.as_posix()}`; running status_report.py...",
        file=sys.stderr,
    )

    status_report_script = resolve_script_path(status_report_script)
    if not status_report_script.exists():
        raise FileNotFoundError(f"status_report script not found: {status_report_script}")

    result = subprocess.run(
        [sys.executable, status_report_script.name],
        cwd=status_report_script.parent,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"status_report.py failed (exit={result.returncode}). {detail}")

    if not summary_path.exists():
        raise FileNotFoundError(
            f"status_report.py completed but summary file is still missing: {summary_path}"
        )


def load_priority_policy(path: Path) -> dict:
    if not path.exists():
        return normalize_priority_policy({})
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return normalize_priority_policy({})
    return normalize_priority_policy(parsed)


def checklist_priority(item_id: str, priority_policy: dict) -> str:
    checklist_policy = priority_policy.get("checklist_priority", {})
    fail_thresholds = priority_policy.get("fail_thresholds", {})

    if isinstance(checklist_policy, dict):
        raw_priority = checklist_policy.get(item_id)
        if raw_priority is None:
            raw_priority = checklist_policy.get("default")
    else:
        raw_priority = None

    if raw_priority is None and isinstance(fail_thresholds, dict):
        raw_priority = fail_thresholds.get("default")

    if raw_priority is None:
        raw_priority = DEFAULT_CHECKLIST_SEVERITY

    return normalize_severity(str(raw_priority), default=DEFAULT_CHECKLIST_SEVERITY)


def collect_findings(summary: dict, priority_policy: dict) -> list[dict]:
    priority_policy = normalize_priority_policy(priority_policy)
    findings: list[dict] = []
    health_level_severity = priority_policy.get(
        "health_level_severity", DEFAULT_HEALTH_LEVEL_SEVERITY
    )
    if not isinstance(health_level_severity, dict):
        health_level_severity = DEFAULT_HEALTH_LEVEL_SEVERITY

    for health_check in summary.get("health_checks", []):
        if not isinstance(health_check, dict):
            continue
        level = str(health_check.get("level", "")).strip().lower()
        severity = health_level_severity.get(level)
        if severity is None:
            continue
        message = str(health_check.get("message") or "Unnamed health finding")
        findings.append(
            {
                "severity": severity,
                "source": "health",
                "id": level,
                "message": message,
            }
        )

    for checklist_item in summary.get("input_checklist", []):
        if not isinstance(checklist_item, dict):
            continue
        if checklist_item.get("done"):
            continue
        item_id = str(checklist_item.get("id") or "unknown")
        severity = checklist_priority(item_id, priority_policy)
        title = str(checklist_item.get("title") or "Unnamed checklist item")
        details = str(checklist_item.get("details") or "")
        message = f"{title} ({details})" if details else title
        findings.append(
            {
                "severity": severity,
                "source": "checklist",
                "id": item_id,
                "message": message,
            }
        )

    return findings


def find_blockers(summary: dict, fail_on: str, priority_policy: dict) -> list[dict]:
    priority_policy = normalize_priority_policy(priority_policy)
    if fail_on == "none":
        return []

    fail_thresholds = priority_policy.get("fail_thresholds", {})
    default_threshold = DEFAULT_FAIL_THRESHOLD
    if isinstance(fail_thresholds, dict):
        default_threshold = normalize_severity(
            str(fail_thresholds.get("default", default_threshold)),
            default=DEFAULT_FAIL_THRESHOLD,
        )

    threshold = normalize_severity(fail_on, default=default_threshold)
    threshold_rank = SEVERITY_ORDER[threshold]

    blockers: list[dict] = []
    for finding in collect_findings(summary, priority_policy):
        finding_rank = SEVERITY_ORDER.get(finding["severity"], 0)
        if finding_rank >= threshold_rank:
            blockers.append(finding)
    return blockers


def as_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def health_counts(health_checks: list[dict]) -> dict:
    counts = {"ok": 0, "warning": 0, "error": 0, "info": 0}
    for check in health_checks:
        level = str(check.get("level", "")).strip().lower()
        if level in counts:
            counts[level] += 1
    return counts


def grouped_todo_items(
    checklist: list[dict], *, todo_only: bool
) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for raw_item in checklist:
        if not isinstance(raw_item, dict):
            continue
        if todo_only and raw_item.get("done"):
            continue

        title = str(raw_item.get("title") or "Unnamed item")
        details = str(raw_item.get("details") or "").strip()
        hint = str(raw_item.get("hint") or "").strip()
        item_id = str(raw_item.get("id") or "").strip()
        file_path = str(raw_item.get("file") or "(unspecified)").strip() or "(unspecified)"
        quick_fix = QUICK_FIX_COMMANDS.get(item_id, "")
        if not quick_fix:
            quick_fix = hint

        grouped.setdefault(file_path, []).append(
            {
                "title": title,
                "details": details,
                "hint": hint,
                "quick_fix": quick_fix,
                "done": "x" if raw_item.get("done") else " ",
            }
        )

    for file_path in grouped:
        grouped[file_path] = sorted(grouped[file_path], key=lambda item: item["title"])

    return dict(sorted(grouped.items(), key=lambda pair: pair[0]))


def build_cli_output(summary: dict, todo_only: bool = False) -> str:
    snapshot = summary.get("data_snapshot", {})
    registration = summary.get("registration", {})
    reviewer_agreement = summary.get("reviewer_agreement", {})
    stage_assessment = summary.get("stage_assessment", {})
    csv_input_validation = summary.get("csv_input_validation", {})
    extraction_validation = summary.get("extraction_validation", {})
    effect_size_conversion = summary.get("effect_size_conversion", {})
    kappa = reviewer_agreement.get("cohen_kappa", {})
    warnings = summary.get("warnings", [])
    next_steps = summary.get("suggested_next_step", [])
    health = summary.get("health_checks", [])
    checklist = summary.get("input_checklist", [])
    counts = health_counts(health)

    if registration.get("registered"):
        registration_label = str(registration.get("registration_id") or "registered")
    else:
        registration_label = "missing"

    stage_label = str(stage_assessment.get("label") or "not assessed")
    stage_id = str(stage_assessment.get("id") or "unknown")

    title_reviewers = reviewer_agreement.get("title_abstract_reviewers") or []
    if isinstance(title_reviewers, list):
        reviewer_count = len(title_reviewers)
    else:
        reviewer_count = 0

    if kappa.get("available"):
        kappa_label = f"{kappa.get('kappa')} ({kappa.get('pair')}, n={kappa.get('paired_records')})"
    else:
        kappa_label = "not available"

    if extraction_validation.get("present") and extraction_validation.get("parsed"):
        extraction_label = (
            f"errors={as_int(extraction_validation.get('errors'))}, "
            f"warnings={as_int(extraction_validation.get('warnings'))}"
        )
    elif extraction_validation.get("present"):
        extraction_label = "present (parse failed)"
    else:
        extraction_label = "missing"

    if not csv_input_validation.get("present"):
        csv_validation_label = "ERROR (missing)"
    elif not csv_input_validation.get("parsed"):
        csv_validation_label = "WARN (parse failed)"
    else:
        csv_errors = as_int(csv_input_validation.get("errors"))
        csv_warnings = as_int(csv_input_validation.get("warnings"))
        if csv_errors > 0:
            csv_validation_label = f"ERROR (errors={csv_errors}, warnings={csv_warnings})"
        elif csv_warnings > 0:
            csv_validation_label = f"WARN (errors=0, warnings={csv_warnings})"
        else:
            csv_validation_label = "OK (errors=0, warnings=0)"

    if not effect_size_conversion:
        effect_size_label = "missing"
    else:
        summary_present = bool(effect_size_conversion.get("summary_present"))
        converted_present = bool(effect_size_conversion.get("converted_present"))
        effect_size_details = str(effect_size_conversion.get("details") or "").strip()

        if summary_present and converted_present:
            effect_size_label = f"OK ({effect_size_details})" if effect_size_details else "OK"
        elif summary_present or converted_present:
            effect_size_label = (
                f"WARN ({effect_size_details})" if effect_size_details else "WARN (partial)"
            )
        else:
            effect_size_label = "ERROR (missing)"

    lines = []
    lines.append("Status summary")
    lines.append(f"Generated: {summary.get('generated_at', '—')}")
    lines.append("")
    lines.append("Key numbers")
    lines.append(f"- Search total: {as_int(snapshot.get('search_results_total'))}")
    lines.append(f"- Unique after dedup: {as_int(snapshot.get('unique_records_after_dedup'))}")
    lines.append(f"- Screened: {as_int(snapshot.get('records_screened'))}")
    lines.append(f"- Includes: {as_int(snapshot.get('includes'))}")
    lines.append(f"- Stage: {stage_label} ({stage_id})")
    lines.append(f"- PROSPERO: {registration_label}")
    lines.append(f"- Title/abstract reviewers: {reviewer_count}")
    lines.append(f"- Cohen's kappa: {kappa_label}")
    lines.append(f"- CSV validation: {csv_validation_label}")
    lines.append(f"- Extraction validation: {extraction_label}")
    lines.append(f"- Effect-size conversion: {effect_size_label}")
    lines.append("")
    lines.append("Health")
    lines.append(
        f"- ok: {counts['ok']}, warning: {counts['warning']}, error: {counts['error']}, info: {counts['info']}"
    )

    lines.append("")
    lines.append("Checklist")
    if checklist:
        active_items = (
            [item for item in checklist if not item.get("done")] if todo_only else checklist
        )
        if active_items:
            for item in active_items:
                mark = "x" if item.get("done") else " "
                title = str(item.get("title", "Unnamed item"))
                file_path = str(item.get("file", ""))
                details = str(item.get("details", ""))
                hint = str(item.get("hint", "")).strip()
                item_id = str(item.get("id") or "").strip()
                quick_fix = QUICK_FIX_COMMANDS.get(item_id, hint)
                base_line = f"- [{mark}] {title} ({details}) -> {file_path}"
                if quick_fix:
                    base_line = f"{base_line} | quick-fix: {quick_fix}"
                lines.append(base_line)
        else:
            lines.append("- no pending checklist items")
    else:
        lines.append("- checklist not available")

    grouped_todos = grouped_todo_items(
        checklist if isinstance(checklist, list) else [], todo_only=todo_only
    )
    lines.append("")
    lines.append("TODO by file")
    if grouped_todos:
        for file_path, items in grouped_todos.items():
            lines.append(f"- {file_path}")
            for item in items:
                detail_suffix = f" | {item['details']}" if item["details"] else ""
                quick_fix_suffix = f" | quick-fix: {item['quick_fix']}" if item["quick_fix"] else ""
                lines.append(
                    f"  - [{item['done']}] {item['title']}{detail_suffix}{quick_fix_suffix}"
                )
    else:
        lines.append("- no pending TODO items")

    lines.append("")
    lines.append("Warnings")
    if warnings:
        for warning in warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- none")

    lines.append("")
    lines.append("Next")
    if next_steps:
        for step in next_steps:
            lines.append(f"- {step}")
    else:
        lines.append("- no action required")

    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print a concise CLI summary from status_summary.json."
    )
    parser.add_argument(
        "--input",
        default="outputs/status_summary.json",
        help="Path to status summary JSON",
    )
    parser.add_argument(
        "--todo-only",
        action="store_true",
        help="Show only pending checklist items",
    )
    parser.add_argument(
        "--fail-on",
        choices=["none", "critical", "major", "minor"],
        default="none",
        help="Exit with code 1 if unresolved findings meet or exceed this severity threshold",
    )
    parser.add_argument(
        "--priority-policy",
        default="priority_policy.json",
        help="Path to severity mapping policy JSON",
    )
    parser.add_argument(
        "--status-report-script",
        default="status_report.py",
        help="Path to status_report.py used for auto-generation when summary is missing",
    )
    parser.add_argument(
        "--auto-generate-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Auto-run status_report.py when the summary JSON is missing",
    )
    args = parser.parse_args()

    summary_path = resolve_input_path(Path(args.input))
    status_report_script = resolve_script_path(Path(args.status_report_script))
    generate_summary_if_missing(
        summary_path,
        auto_generate_missing=bool(args.auto_generate_missing),
        status_report_script=status_report_script,
    )
    summary = load_summary(summary_path)
    print(build_cli_output(summary, todo_only=args.todo_only), end="")

    priority_policy_path = resolve_input_path(Path(args.priority_policy))
    priority_policy = load_priority_policy(priority_policy_path)
    blockers = find_blockers(summary, args.fail_on, priority_policy)
    if blockers:
        print(
            f"[status_cli] FAIL: {len(blockers)} finding(s) at/above `{args.fail_on}`.",
            file=sys.stderr,
        )
        for blocker in blockers[:10]:
            print(
                f"- [{blocker['severity']}] {blocker['message']} "
                f"({blocker['source']}:{blocker['id']})",
                file=sys.stderr,
            )
        raise SystemExit(1)


if __name__ == "__main__":
    main()

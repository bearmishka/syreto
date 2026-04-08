import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path


def parse_today(value: str | None) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def week_window(today_value: date) -> tuple[date, date]:
    days_since_friday = (today_value.weekday() - 4) % 7
    week_end = today_value - timedelta(days=days_since_friday)
    week_start = week_end - timedelta(days=6)
    return week_start, week_end


def as_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def health_counts(health_checks: list[dict]) -> dict[str, int]:
    counts = {"error": 0, "warning": 0, "info": 0, "ok": 0}
    for check in health_checks:
        level = str(check.get("level", "")).strip().lower()
        if level in counts:
            counts[level] += 1
    return counts


def top_risks(summary: dict, *, max_items: int) -> list[str]:
    items: list[str] = []

    for check in summary.get("health_checks", []):
        level = str(check.get("level", "")).strip().lower()
        if level not in {"error", "warning"}:
            continue
        message = str(check.get("message", "")).strip()
        if not message:
            continue
        marker = "🔴" if level == "error" else "🟠"
        items.append(f"{marker} {message}")

    for warning in summary.get("warnings", []):
        text = str(warning).strip()
        if not text:
            continue
        items.append(f"🟠 {text}")

    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        deduped.append(item)
        seen.add(item)
        if len(deduped) >= max_items:
            break
    return deduped


def pending_checklist(summary: dict, *, max_items: int) -> list[str]:
    pending: list[str] = []
    for item in summary.get("input_checklist", []):
        if item.get("done"):
            continue
        title = str(item.get("title", "Unnamed item")).strip()
        details = str(item.get("details", "")).strip()
        file_path = str(item.get("file", "")).strip()
        rendered = title
        if details:
            rendered += f" ({details})"
        if file_path:
            rendered += f" — `{file_path}`"
        pending.append(rendered)
        if len(pending) >= max_items:
            break
    return pending


def build_digest(
    summary: dict,
    *,
    today_value: date,
    max_risks: int,
    max_actions: int,
    source_error: str = "",
) -> str:
    week_start, week_end = week_window(today_value)
    snapshot = summary.get("data_snapshot", {})
    stage_assessment = summary.get("stage_assessment", {})
    project_posture = summary.get("project_posture", {})
    semantic_completeness = (
        project_posture.get("semantic_completeness", {})
        if isinstance(project_posture, dict)
        else {}
    )
    registration = summary.get("registration", {})
    reviewer_agreement = summary.get("reviewer_agreement", {})
    kappa = reviewer_agreement.get("cohen_kappa", {})

    counts = health_counts(summary.get("health_checks", []))
    risks = top_risks(summary, max_items=max_risks)
    actions = [
        str(step).strip() for step in summary.get("suggested_next_step", []) if str(step).strip()
    ][:max_actions]
    pending = pending_checklist(summary, max_items=max_actions)

    if registration.get("registered"):
        registration_label = str(registration.get("registration_id") or "registered")
    else:
        registration_label = "missing"

    if kappa.get("available"):
        kappa_label = f"{kappa.get('kappa')}"
    else:
        kappa_label = "not available"

    lines: list[str] = []
    lines.append("# Weekly Risk Digest")
    lines.append("")
    lines.append(f"- Week window: {week_start.isoformat()} → {week_end.isoformat()} (Fri)")
    lines.append(f"- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("")

    if source_error:
        lines.append("## Status")
        lines.append("")
        lines.append(f"- ⚠️ Digest fallback: {source_error}")
        lines.append("- Run `python status_report.py` before generating the weekly digest.")
        lines.append("")
        return "\n".join(lines) + "\n"

    lines.append("## Executive Snapshot")
    lines.append("")
    lines.append(f"- Search total: {as_int(snapshot.get('search_results_total'))}")
    lines.append(f"- Unique after dedup: {as_int(snapshot.get('unique_records_after_dedup'))}")
    lines.append(f"- Screened: {as_int(snapshot.get('records_screened'))}")
    lines.append(f"- Includes: {as_int(snapshot.get('includes'))}")
    lines.append(
        f"- Stage: {str(stage_assessment.get('label') or 'not assessed')} ({str(stage_assessment.get('id') or 'unknown')})"
    )
    lines.append(f"- PROSPERO: {registration_label}")
    lines.append(f"- Cohen's kappa: {kappa_label}")
    lines.append("")

    lines.append("## Project Posture")
    lines.append("")
    if isinstance(project_posture, dict) and project_posture:
        summary_en = str(project_posture.get("summary_en") or "").strip()
        primary_blocker = str(project_posture.get("primary_blocker") or "none").strip() or "none"
        semantic_complete = bool(semantic_completeness.get("complete"))
        protocol_placeholders = as_int(semantic_completeness.get("protocol_placeholder_count"))
        manuscript_placeholders = as_int(semantic_completeness.get("manuscript_placeholder_count"))
        placeholder_examples = semantic_completeness.get("placeholder_examples") or []

        if summary_en:
            lines.append(f"- Summary: {summary_en}")
        lines.append(f"- Primary blocker: {primary_blocker}")
        lines.append(f"- Semantic completeness: {'complete' if semantic_complete else 'pending'}")
        lines.append(
            "- Unresolved placeholders: "
            f"protocol={protocol_placeholders}, manuscript={manuscript_placeholders}"
        )

        if isinstance(placeholder_examples, list) and placeholder_examples:
            preview = ", ".join(str(item) for item in placeholder_examples[:5])
            lines.append(f"- Placeholder examples: {preview}")
    else:
        lines.append("- not available in current status summary")
    lines.append("")

    lines.append("## Risk Scoreboard")
    lines.append("")
    lines.append(f"- Critical (error): {counts['error']}")
    lines.append(f"- Elevated (warning): {counts['warning']}")
    lines.append(f"- Informational: {counts['info']}")
    lines.append("")

    lines.append("## Top Risks")
    lines.append("")
    if risks:
        for item in risks:
            lines.append(f"- {item}")
    else:
        lines.append("- No active critical/warning risks in current status summary.")
    lines.append("")

    lines.append("## Priority Actions")
    lines.append("")
    if actions:
        for action in actions:
            lines.append(f"- {action}")
    else:
        lines.append("- No immediate action required.")
    lines.append("")

    lines.append("## Open Checklist")
    lines.append("")
    if pending:
        for item in pending:
            lines.append(f"- [ ] {item}")
    else:
        lines.append("- [x] No pending checklist items.")
    lines.append("")

    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a concise weekly risk digest for PI updates."
    )
    parser.add_argument(
        "--status-summary",
        default="outputs/status_summary.json",
        help="Path to status summary JSON (from status_report.py)",
    )
    parser.add_argument(
        "--output",
        default="outputs/weekly_risk_digest.md",
        help="Path to weekly digest markdown output",
    )
    parser.add_argument("--today", default=None, help="Override current date (YYYY-MM-DD)")
    parser.add_argument("--max-risks", type=int, default=5, help="Maximum number of risk bullets")
    parser.add_argument(
        "--max-actions", type=int, default=5, help="Maximum number of action bullets"
    )
    args = parser.parse_args(argv)

    status_summary_path = Path(args.status_summary)
    output_path = Path(args.output)

    source_error = ""
    summary: dict = {}

    if not status_summary_path.exists():
        source_error = f"status summary file not found: {status_summary_path}"
    else:
        try:
            summary = json.loads(status_summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            source_error = f"status summary JSON parse failed: {exc}"

    digest_text = build_digest(
        summary,
        today_value=parse_today(args.today),
        max_risks=max(1, int(args.max_risks)),
        max_actions=max(1, int(args.max_actions)),
        source_error=source_error,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(digest_text, encoding="utf-8")
    print(f"Wrote: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

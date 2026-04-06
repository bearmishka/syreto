import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import tempfile


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


def atomic_replace_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.tmp.", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    atomic_replace_bytes(path, text.encode(encoding))


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def load_status_summary(path: Path) -> dict:
    if not path.exists():
        return {}

    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    if isinstance(parsed, dict):
        return parsed
    return {}


def collect_pending_items(summary: dict) -> list[dict[str, str]]:
    checklist = summary.get("input_checklist", [])
    if not isinstance(checklist, list):
        return []

    pending: list[dict[str, str]] = []
    for raw_item in checklist:
        if not isinstance(raw_item, dict):
            continue
        if raw_item.get("done"):
            continue

        item_id = normalize(raw_item.get("id", ""))
        file_path = normalize(raw_item.get("file", "")) or "(unspecified)"
        title = normalize(raw_item.get("title", "")) or "Unnamed item"
        details = normalize(raw_item.get("details", ""))
        hint = normalize(raw_item.get("hint", ""))

        quick_fix = QUICK_FIX_COMMANDS.get(item_id, "")
        if not quick_fix and hint:
            quick_fix = hint

        pending.append(
            {
                "id": item_id,
                "file": file_path,
                "title": title,
                "details": details,
                "hint": hint,
                "quick_fix": quick_fix,
            }
        )

    return pending


def group_by_file(pending_items: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    groups: dict[str, list[dict[str, str]]] = {}
    for item in pending_items:
        file_path = item["file"]
        groups.setdefault(file_path, []).append(item)

    for file_path in groups:
        groups[file_path] = sorted(groups[file_path], key=lambda item: (item["title"], item["id"]))

    return dict(sorted(groups.items(), key=lambda pair: pair[0]))


def build_markdown(*, status_summary_path: Path, pending_items: list[dict[str, str]]) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = []
    lines.append("# TODO Action Plan")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Source")
    lines.append("")
    lines.append(f"- Status summary: `{status_summary_path.as_posix()}`")
    lines.append("")
    lines.append("## Overview")
    lines.append("")
    lines.append(f"- Pending checklist items: {len(pending_items)}")

    if not pending_items:
        lines.append("- ✅ No open TODO items.")
        lines.append("")
        return "\n".join(lines)

    grouped = group_by_file(pending_items)
    lines.append(f"- Files with open TODOs: {len(grouped)}")
    lines.append("")
    lines.append("## Grouped TODOs")
    lines.append("")

    for file_path, items in grouped.items():
        lines.append(f"### `{file_path}`")
        lines.append("")
        for item in items:
            lines.append(f"- [ ] **{item['title']}**")
            if item["details"]:
                lines.append(f"  - Details: {item['details']}")
            if item["hint"]:
                lines.append(f"  - Hint: {item['hint']}")
            if item["quick_fix"]:
                lines.append(f"  - Quick fix: `{item['quick_fix']}`")
            else:
                lines.append("  - Quick fix: review item details and update source file.")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build grouped TODO action plan from status_summary.json")
    parser.add_argument(
        "--input",
        default="outputs/status_summary.json",
        help="Path to status summary JSON",
    )
    parser.add_argument(
        "--output",
        default="outputs/todo_action_plan.md",
        help="Path to markdown TODO action plan",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    summary = load_status_summary(input_path)
    pending_items = collect_pending_items(summary)
    markdown = build_markdown(status_summary_path=input_path, pending_items=pending_items)

    atomic_write_text(output_path, markdown)
    print(f"Wrote: {output_path}")
    print(f"Pending TODO items: {len(pending_items)}")


if __name__ == "__main__":
    main()
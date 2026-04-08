import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import tempfile

import pandas as pd


OUTPUT_COLUMNS = [
    "run_started_at_utc",
    "run_updated_at_utc",
    "run_id",
    "state",
    "review_mode",
    "pipeline_exit_code",
    "status_checkpoint_exit_code",
    "final_exit_code",
    "failure_phase",
    "rollback_applied",
    "transactional_mode",
    "search_results_total",
    "unique_records_after_dedup",
    "records_screened",
    "includes",
    "excludes",
    "maybe",
    "pending",
    "stage_id",
    "health_ok",
    "health_warning",
    "health_error",
    "health_info",
    "warnings_count",
    "todo_open_count",
    "delta_search_results_total",
    "delta_unique_records_after_dedup",
    "delta_records_screened",
    "delta_includes",
    "delta_excludes",
    "delta_maybe",
    "delta_pending",
    "delta_health_warning",
    "delta_health_error",
    "delta_todo_open_count",
]

DELTA_FIELDS = [
    "search_results_total",
    "unique_records_after_dedup",
    "records_screened",
    "includes",
    "excludes",
    "maybe",
    "pending",
    "health_warning",
    "health_error",
    "todo_open_count",
]


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


def atomic_write_dataframe_csv(frame: pd.DataFrame, path: Path, *, index: bool = False) -> None:
    atomic_write_text(path, frame.to_csv(index=index))


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def parse_int_or_none(value: object) -> int | None:
    text = normalize(value)
    if not text:
        return None
    numeric = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return int(float(numeric))


def parse_json_object_stream(text: str) -> tuple[list[dict], str | None]:
    decoder = json.JSONDecoder()
    index = 0
    length = len(text)
    objects: list[dict] = []

    while index < length:
        while index < length and text[index].isspace():
            index += 1
        if index >= length:
            break

        try:
            parsed, consumed = decoder.raw_decode(text, index)
        except json.JSONDecodeError as exc:
            return objects, str(exc)

        if not isinstance(parsed, dict):
            return objects, "JSON stream element is not an object."

        objects.append(parsed)
        index = consumed

    if not objects:
        return [], "No JSON object found in stream."

    return objects, None


def read_manifest(path: Path) -> dict:
    fallback = {
        "run_id": "",
        "state": "",
        "started_at_utc": "",
        "updated_at_utc": "",
        "pipeline_exit_code": "",
        "status_checkpoint_exit_code": "",
        "final_exit_code": "",
        "failure_phase": "",
        "rollback_applied": "",
        "transactional_mode": "",
    }

    if not path.exists():
        return fallback

    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError:
        return fallback

    payloads, parse_error = parse_json_object_stream(raw_text)
    if parse_error is not None or not payloads:
        return fallback

    payload = payloads[-1]
    return {
        "run_id": normalize(payload.get("run_id", "")),
        "state": normalize(payload.get("state", "")),
        "started_at_utc": normalize(payload.get("started_at_utc", "")),
        "updated_at_utc": normalize(payload.get("updated_at_utc", "")),
        "pipeline_exit_code": normalize(payload.get("pipeline_exit_code", "")),
        "status_checkpoint_exit_code": normalize(payload.get("status_checkpoint_exit_code", "")),
        "final_exit_code": normalize(payload.get("final_exit_code", "")),
        "failure_phase": normalize(payload.get("failure_phase", "")),
        "rollback_applied": normalize(payload.get("rollback_applied", "")),
        "transactional_mode": normalize(payload.get("transactional_mode", "")),
    }


def read_status_summary(path: Path) -> dict:
    if not path.exists():
        return {}

    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}

    if isinstance(parsed, dict):
        return parsed
    return {}


def parse_health_counts(status_summary: dict) -> dict[str, int]:
    counts = {"ok": 0, "warning": 0, "error": 0, "info": 0}
    health_checks = status_summary.get("health_checks", [])
    if not isinstance(health_checks, list):
        return counts

    for item in health_checks:
        if not isinstance(item, dict):
            continue
        level = normalize(item.get("level", "")).lower()
        if level in counts:
            counts[level] += 1

    return counts


def count_open_checklist(status_summary: dict) -> int:
    checklist = status_summary.get("input_checklist", [])
    if not isinstance(checklist, list):
        return 0

    pending = 0
    for item in checklist:
        if not isinstance(item, dict):
            continue
        if item.get("done"):
            continue
        pending += 1
    return pending


def build_current_row(*, manifest: dict, status_summary: dict, review_mode: str) -> dict[str, str]:
    snapshot = status_summary.get("data_snapshot", {})
    if not isinstance(snapshot, dict):
        snapshot = {}

    stage_assessment = status_summary.get("stage_assessment", {})
    if not isinstance(stage_assessment, dict):
        stage_assessment = {}

    warnings = status_summary.get("warnings", [])
    if not isinstance(warnings, list):
        warnings = []

    health_counts = parse_health_counts(status_summary)

    started_at = normalize(manifest.get("started_at_utc", ""))
    updated_at = normalize(manifest.get("updated_at_utc", ""))
    if not started_at:
        started_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    if not updated_at:
        updated_at = started_at

    row = {
        "run_started_at_utc": started_at,
        "run_updated_at_utc": updated_at,
        "run_id": normalize(manifest.get("run_id", "")),
        "state": normalize(manifest.get("state", "")),
        "review_mode": normalize(review_mode),
        "pipeline_exit_code": normalize(manifest.get("pipeline_exit_code", "")),
        "status_checkpoint_exit_code": normalize(manifest.get("status_checkpoint_exit_code", "")),
        "final_exit_code": normalize(manifest.get("final_exit_code", "")),
        "failure_phase": normalize(manifest.get("failure_phase", "")),
        "rollback_applied": normalize(manifest.get("rollback_applied", "")),
        "transactional_mode": normalize(manifest.get("transactional_mode", "")),
        "search_results_total": normalize(snapshot.get("search_results_total", "")),
        "unique_records_after_dedup": normalize(snapshot.get("unique_records_after_dedup", "")),
        "records_screened": normalize(snapshot.get("records_screened", "")),
        "includes": normalize(snapshot.get("includes", "")),
        "excludes": normalize(snapshot.get("excludes", "")),
        "maybe": normalize(snapshot.get("maybe", "")),
        "pending": normalize(snapshot.get("pending", "")),
        "stage_id": normalize(stage_assessment.get("id", "")),
        "health_ok": str(health_counts["ok"]),
        "health_warning": str(health_counts["warning"]),
        "health_error": str(health_counts["error"]),
        "health_info": str(health_counts["info"]),
        "warnings_count": str(len(warnings)),
        "todo_open_count": str(count_open_checklist(status_summary)),
    }

    for field in DELTA_FIELDS:
        row[f"delta_{field}"] = ""

    return row


def load_history(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    try:
        frame = pd.read_csv(path, dtype=str)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    for column in OUTPUT_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""

    return frame[OUTPUT_COLUMNS].fillna("")


def upsert_history_row(history_df: pd.DataFrame, row: dict[str, str]) -> pd.DataFrame:
    if history_df.empty:
        return pd.DataFrame([row], columns=OUTPUT_COLUMNS)

    working = history_df.copy()
    run_id = normalize(row.get("run_id", ""))

    if run_id and "run_id" in working.columns:
        matches = working["run_id"].fillna("").astype(str).str.strip().eq(run_id)
        if matches.any():
            working.loc[matches, list(row.keys())] = [row[key] for key in row.keys()]
        else:
            working = pd.concat(
                [working, pd.DataFrame([row], columns=OUTPUT_COLUMNS)], ignore_index=True
            )
    else:
        working = pd.concat(
            [working, pd.DataFrame([row], columns=OUTPUT_COLUMNS)], ignore_index=True
        )

    working["_sort_dt"] = pd.to_datetime(working["run_updated_at_utc"], errors="coerce", utc=True)
    working["_sort_seq"] = pd.Series(range(len(working)), dtype="int64")
    working = working.sort_values(by=["_sort_dt", "_sort_seq"], na_position="last").drop(
        columns=["_sort_dt", "_sort_seq"]
    )
    return working.reset_index(drop=True)


def apply_deltas(history_df: pd.DataFrame) -> pd.DataFrame:
    working = history_df.copy()

    for field in DELTA_FIELDS:
        delta_column = f"delta_{field}"
        deltas: list[str] = []
        previous: int | None = None

        for raw_value in working[field].tolist():
            current = parse_int_or_none(raw_value)
            if previous is None or current is None:
                deltas.append("")
            else:
                deltas.append(str(current - previous))

            if current is not None:
                previous = current

        working[delta_column] = deltas

    return working


def format_delta(value: object) -> str:
    parsed = parse_int_or_none(value)
    if parsed is None:
        return "—"
    if parsed > 0:
        return f"+{parsed}"
    return str(parsed)


def build_summary(history_df: pd.DataFrame) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    total_runs = int(history_df.shape[0])

    lines: list[str] = []
    lines.append("# Progress History")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Coverage")
    lines.append("")
    lines.append(f"- Runs tracked: {total_runs}")

    if total_runs == 0:
        lines.append("- No run history available yet.")
        lines.append("")
        return "\n".join(lines)

    latest = history_df.iloc[-1]
    lines.append(f"- Latest run: `{normalize(latest.get('run_id', '')) or 'unknown'}`")
    lines.append(f"- Latest state: `{normalize(latest.get('state', '')) or 'unknown'}`")
    lines.append(
        f"- Latest update: `{normalize(latest.get('run_updated_at_utc', '')) or 'unknown'}`"
    )
    lines.append("")

    lines.append("## Latest Snapshot")
    lines.append("")
    lines.append(f"- Stage: `{normalize(latest.get('stage_id', '')) or 'unknown'}`")
    lines.append(
        f"- Search results total: {normalize(latest.get('search_results_total', '')) or '—'}"
    )
    lines.append(
        f"- Unique after dedup: {normalize(latest.get('unique_records_after_dedup', '')) or '—'}"
    )
    lines.append(f"- Records screened: {normalize(latest.get('records_screened', '')) or '—'}")
    lines.append(f"- Includes: {normalize(latest.get('includes', '')) or '—'}")
    lines.append(f"- Excludes: {normalize(latest.get('excludes', '')) or '—'}")
    lines.append(
        f"- Pending checklist items: {normalize(latest.get('todo_open_count', '')) or '—'}"
    )
    lines.append("")

    lines.append("## Deltas vs Previous Run")
    lines.append("")
    if total_runs < 2:
        lines.append("- First recorded run; deltas will appear after the next run.")
        lines.append("")
        return "\n".join(lines)

    previous = history_df.iloc[-2]
    lines.append(f"- Previous run: `{normalize(previous.get('run_id', '')) or 'unknown'}`")
    lines.append(
        f"- Search results total: {format_delta(latest.get('delta_search_results_total', ''))}"
    )
    lines.append(
        f"- Unique after dedup: {format_delta(latest.get('delta_unique_records_after_dedup', ''))}"
    )
    lines.append(f"- Records screened: {format_delta(latest.get('delta_records_screened', ''))}")
    lines.append(f"- Includes: {format_delta(latest.get('delta_includes', ''))}")
    lines.append(f"- Excludes: {format_delta(latest.get('delta_excludes', ''))}")
    lines.append(
        f"- Pending checklist items: {format_delta(latest.get('delta_todo_open_count', ''))}"
    )
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build run-progress history and latest deltas from status/manifest artifacts."
    )
    parser.add_argument(
        "--manifest",
        default="outputs/daily_run_manifest.json",
        help="Path to daily-run manifest JSON",
    )
    parser.add_argument(
        "--status-summary",
        default="outputs/status_summary.json",
        help="Path to status summary JSON",
    )
    parser.add_argument(
        "--history-output",
        default="outputs/progress_history.csv",
        help="Path to progress history CSV",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/progress_history_summary.md",
        help="Path to markdown summary with latest deltas",
    )
    parser.add_argument(
        "--review-mode",
        default=os.getenv("REVIEW_MODE", "template"),
        help="Active review mode label for history rows",
    )
    args = parser.parse_args()

    manifest = read_manifest(Path(args.manifest))
    status_summary = read_status_summary(Path(args.status_summary))

    current_row = build_current_row(
        manifest=manifest, status_summary=status_summary, review_mode=args.review_mode
    )
    history_df = load_history(Path(args.history_output))
    history_df = upsert_history_row(history_df, current_row)
    history_df = apply_deltas(history_df)
    history_df = history_df[OUTPUT_COLUMNS]

    summary_text = build_summary(history_df)

    atomic_write_dataframe_csv(history_df, Path(args.history_output), index=False)
    atomic_write_text(Path(args.summary_output), summary_text)

    print(f"Wrote: {args.history_output}")
    print(f"Wrote: {args.summary_output}")
    print(f"Runs tracked: {int(history_df.shape[0])}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
import re

import pandas as pd


MISSING_CODES = {
    "",
    "nan",
    "na",
    "n/a",
    "nr",
    "none",
    "unclear",
    "missing",
    "not reported",
    "not_reported",
    "not applicable",
    "not_applicable",
}

SEARCH_LOG_COLUMNS = [
    "database",
    "date_searched",
    "query_version",
    "start_year",
    "end_date",
    "filters_applied",
    "results_total",
    "results_exported",
    "export_filename",
    "notes",
]

SESSION_COLUMNS = [
    "database",
    "database_key",
    "search_date",
    "search_date_iso",
    "query_version",
    "filters_applied",
    "results_total_int",
    "results_exported_int",
]

DIFF_COLUMNS = [
    "database",
    "previous_search_date",
    "current_search_date",
    "days_between_searches",
    "previous_query_version",
    "current_query_version",
    "query_version_changed",
    "previous_filters_applied",
    "current_filters_applied",
    "filters_changed",
    "drift_severity",
    "previous_results_total",
    "current_results_total",
    "delta_results_total",
    "previous_results_exported",
    "current_results_exported",
    "delta_results_exported",
]

SCHEDULE_COLUMNS = [
    "database",
    "anchor_search_date",
    "cycle_index",
    "scheduled_search_date",
    "cadence_days",
    "days_until_due",
    "schedule_status",
    "last_query_version",
    "last_results_total",
    "last_results_exported",
]

CADENCE_CHECK_COLUMNS = [
    "database",
    "last_search_date",
    "next_due_date",
    "cadence_days",
    "days_since_last_search",
    "days_until_due",
    "cadence_status",
    "last_query_version",
    "last_results_total",
    "last_results_exported",
]

STANDARD_MODE_SIGNAL_PATTERNS = [
    re.compile(r"\breview[_\s-]*mode\s*[:=]\s*standard\b", flags=re.IGNORECASE),
    re.compile(r"\bnon[-\s]*living\b", flags=re.IGNORECASE),
]

LIVING_MODE_SIGNAL_PATTERNS = [
    re.compile(r"\breview[_\s-]*mode\s*[:=]\s*living\b", flags=re.IGNORECASE),
    re.compile(r"\bliving[-\s]+review\b", flags=re.IGNORECASE),
    re.compile(r"\bcontin(?:ual|uous(?:ly)?)\s+update(?:d|s)?\b", flags=re.IGNORECASE),
    re.compile(r"\bregularly\s+updated\s+review\b", flags=re.IGNORECASE),
    re.compile(r"\bongoing\s+evidence\s+update(?:d|s)?\b", flags=re.IGNORECASE),
]

HIGH_DELTA_RATIO_THRESHOLD = 0.50
MEDIUM_DELTA_RATIO_THRESHOLD = 0.20
HIGH_DELTA_ABSOLUTE_THRESHOLD = 100
HIGH_DELTA_ABSOLUTE_FALLBACK = 200
MEDIUM_DELTA_ABSOLUTE_THRESHOLD = 50


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def normalize_lower(value: object) -> str:
    return normalize(value).lower()


def is_missing(value: object) -> bool:
    return normalize_lower(value) in MISSING_CODES


def parse_date(value: object) -> date | None:
    text = normalize(value)
    if not text or is_missing(text):
        return None
    parsed = pd.to_datetime(pd.Series([text]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    return parsed.date()


def parse_int(value: object) -> int | None:
    text = normalize(value)
    if not text or is_missing(text):
        return None
    parsed = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    return int(float(parsed))


def format_int(value: int | None) -> str:
    if value is None:
        return ""
    return str(int(value))


def format_delta(current_value: int | None, previous_value: int | None) -> str:
    if current_value is None or previous_value is None:
        return ""
    return str(int(current_value - previous_value))


def delta_ratio(current_value: int | None, previous_value: int | None) -> float | None:
    if current_value is None or previous_value is None:
        return None
    if previous_value == 0:
        return None
    return abs(float(current_value - previous_value) / float(previous_value))


def classify_drift_severity(
    *,
    query_changed: bool,
    filters_changed: bool,
    previous_results_total: int | None,
    current_results_total: int | None,
    previous_results_exported: int | None,
    current_results_exported: int | None,
) -> str:
    delta_total_abs = (
        abs(int(current_results_total - previous_results_total))
        if current_results_total is not None and previous_results_total is not None
        else 0
    )
    delta_exported_abs = (
        abs(int(current_results_exported - previous_results_exported))
        if current_results_exported is not None and previous_results_exported is not None
        else 0
    )

    ratio_total = delta_ratio(current_results_total, previous_results_total)
    ratio_exported = delta_ratio(current_results_exported, previous_results_exported)

    if query_changed and filters_changed:
        return "high"

    if delta_total_abs >= HIGH_DELTA_ABSOLUTE_FALLBACK:
        return "high"

    if ratio_total is not None and ratio_total >= HIGH_DELTA_RATIO_THRESHOLD and delta_total_abs >= HIGH_DELTA_ABSOLUTE_THRESHOLD:
        return "high"

    if (
        ratio_exported is not None
        and ratio_exported >= HIGH_DELTA_RATIO_THRESHOLD
        and delta_exported_abs >= HIGH_DELTA_ABSOLUTE_THRESHOLD
    ):
        return "high"

    if (query_changed or filters_changed) and ratio_total is not None and ratio_total >= 0.30:
        return "high"

    if query_changed or filters_changed:
        return "medium"

    if ratio_total is not None and ratio_total >= MEDIUM_DELTA_RATIO_THRESHOLD:
        return "medium"

    if ratio_exported is not None and ratio_exported >= MEDIUM_DELTA_RATIO_THRESHOLD:
        return "medium"

    if delta_total_abs >= MEDIUM_DELTA_ABSOLUTE_THRESHOLD or delta_exported_abs >= MEDIUM_DELTA_ABSOLUTE_THRESHOLD:
        return "medium"

    return "low"


def unique_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def detect_protocol_review_mode(protocol_text: str) -> tuple[str | None, str]:
    if not protocol_text.strip():
        return None, ""

    for pattern in STANDARD_MODE_SIGNAL_PATTERNS:
        match = pattern.search(protocol_text)
        if match:
            return "standard", normalize(match.group(0))

    for pattern in LIVING_MODE_SIGNAL_PATTERNS:
        match = pattern.search(protocol_text)
        if match:
            return "living", normalize(match.group(0))

    return None, ""


def resolve_review_mode(requested_mode: str, protocol_path: Path) -> tuple[str, str, str]:
    normalized_mode = normalize_lower(requested_mode)
    if normalized_mode in {"standard", "living"}:
        return normalized_mode, "cli", ""

    if normalized_mode != "auto":
        return "standard", "default", ""

    protocol_text = ""
    if protocol_path.exists() and protocol_path.is_file():
        try:
            protocol_text = protocol_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            protocol_text = ""

    detected_mode, signal_text = detect_protocol_review_mode(protocol_text)
    if detected_mode:
        return detected_mode, "protocol", signal_text

    return "standard", "default", ""


def latest_session_by_database(sessions_df: pd.DataFrame) -> dict[str, pd.Series]:
    latest_by_database: dict[str, pd.Series] = {}
    if sessions_df.empty:
        return latest_by_database

    for _, group_df in sessions_df.groupby("database_key", sort=False):
        ordered_group = group_df.sort_values(["search_date", "search_date_iso"], kind="stable").reset_index(drop=True)
        latest_by_database[str(ordered_group.loc[ordered_group.shape[0] - 1, "database_key"])] = ordered_group.loc[
            ordered_group.shape[0] - 1
        ]

    return latest_by_database


def read_search_log(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"search_log file not found: {path}")

    try:
        search_log_df = pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        search_log_df = pd.DataFrame()

    for column in SEARCH_LOG_COLUMNS:
        if column not in search_log_df.columns:
            search_log_df[column] = ""

    return search_log_df


def prepare_search_sessions(search_log_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for _, row in search_log_df.iterrows():
        database = normalize(row.get("database", ""))
        if not database:
            continue

        searched_date = parse_date(row.get("date_searched", ""))
        if searched_date is None:
            continue

        rows.append(
            {
                "database": database,
                "database_key": database.lower(),
                "search_date": searched_date,
                "search_date_iso": searched_date.isoformat(),
                "query_version": normalize(row.get("query_version", "")),
                "filters_applied": normalize(row.get("filters_applied", "")),
                "results_total_int": parse_int(row.get("results_total", "")),
                "results_exported_int": parse_int(row.get("results_exported", "")),
            }
        )

    sessions_df = pd.DataFrame(rows, columns=SESSION_COLUMNS)
    if sessions_df.empty:
        return sessions_df

    sessions_df = sessions_df.sort_values(["database_key", "search_date", "search_date_iso"], kind="stable").reset_index(drop=True)
    return sessions_df


def build_search_diffs(sessions_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, str]] = []

    if sessions_df.empty:
        return pd.DataFrame(columns=DIFF_COLUMNS)

    for _, group_df in sessions_df.groupby("database_key", sort=False):
        ordered_group = group_df.sort_values(["search_date", "search_date_iso"], kind="stable").reset_index(drop=True)
        if ordered_group.shape[0] < 2:
            continue

        for index in range(1, ordered_group.shape[0]):
            previous_row = ordered_group.loc[index - 1]
            current_row = ordered_group.loc[index]

            previous_query = normalize(previous_row.get("query_version", ""))
            current_query = normalize(current_row.get("query_version", ""))
            previous_filters = normalize(previous_row.get("filters_applied", ""))
            current_filters = normalize(current_row.get("filters_applied", ""))

            previous_date = previous_row["search_date"]
            current_date = current_row["search_date"]
            days_between = int((current_date - previous_date).days)

            previous_total = previous_row.get("results_total_int")
            current_total = current_row.get("results_total_int")
            previous_exported = previous_row.get("results_exported_int")
            current_exported = current_row.get("results_exported_int")

            query_changed = previous_query != current_query
            filters_changed = previous_filters != current_filters
            drift_severity = classify_drift_severity(
                query_changed=query_changed,
                filters_changed=filters_changed,
                previous_results_total=previous_total,
                current_results_total=current_total,
                previous_results_exported=previous_exported,
                current_results_exported=current_exported,
            )

            rows.append(
                {
                    "database": normalize(current_row.get("database", "")),
                    "previous_search_date": previous_date.isoformat(),
                    "current_search_date": current_date.isoformat(),
                    "days_between_searches": str(days_between),
                    "previous_query_version": previous_query,
                    "current_query_version": current_query,
                    "query_version_changed": "yes" if query_changed else "no",
                    "previous_filters_applied": previous_filters,
                    "current_filters_applied": current_filters,
                    "filters_changed": "yes" if filters_changed else "no",
                    "drift_severity": drift_severity,
                    "previous_results_total": format_int(previous_total),
                    "current_results_total": format_int(current_total),
                    "delta_results_total": format_delta(current_total, previous_total),
                    "previous_results_exported": format_int(previous_exported),
                    "current_results_exported": format_int(current_exported),
                    "delta_results_exported": format_delta(current_exported, previous_exported),
                }
            )

    return pd.DataFrame(rows, columns=DIFF_COLUMNS)


def build_living_schedule(
    sessions_df: pd.DataFrame,
    *,
    include_databases: list[str],
    cadence_days: int,
    horizon_cycles: int,
    today: date,
    review_mode: str,
) -> pd.DataFrame:
    if review_mode != "living":
        return pd.DataFrame(columns=SCHEDULE_COLUMNS)

    latest_by_database = latest_session_by_database(sessions_df)

    rows: list[dict[str, str]] = []
    cadence_days = max(int(cadence_days), 1)
    horizon_cycles = max(int(horizon_cycles), 1)

    for database in include_databases:
        database_key = database.lower()
        latest_row = latest_by_database.get(database_key)

        if latest_row is None:
            rows.append(
                {
                    "database": database,
                    "anchor_search_date": "",
                    "cycle_index": "1",
                    "scheduled_search_date": today.isoformat(),
                    "cadence_days": str(cadence_days),
                    "days_until_due": "0",
                    "schedule_status": "no_prior_completed_search",
                    "last_query_version": "",
                    "last_results_total": "",
                    "last_results_exported": "",
                }
            )
            continue

        anchor_date = latest_row["search_date"]
        last_query_version = normalize(latest_row.get("query_version", ""))
        last_results_total = format_int(latest_row.get("results_total_int"))
        last_results_exported = format_int(latest_row.get("results_exported_int"))

        for cycle_index in range(1, horizon_cycles + 1):
            scheduled_date = anchor_date + timedelta(days=cadence_days * cycle_index)
            days_until_due = int((scheduled_date - today).days)
            if days_until_due < 0:
                schedule_status = "overdue"
            elif days_until_due == 0:
                schedule_status = "due_today"
            else:
                schedule_status = "upcoming"

            rows.append(
                {
                    "database": normalize(latest_row.get("database", database)),
                    "anchor_search_date": anchor_date.isoformat(),
                    "cycle_index": str(cycle_index),
                    "scheduled_search_date": scheduled_date.isoformat(),
                    "cadence_days": str(cadence_days),
                    "days_until_due": str(days_until_due),
                    "schedule_status": schedule_status,
                    "last_query_version": last_query_version,
                    "last_results_total": last_results_total,
                    "last_results_exported": last_results_exported,
                }
            )

    return pd.DataFrame(rows, columns=SCHEDULE_COLUMNS)


def build_cadence_check(
    sessions_df: pd.DataFrame,
    *,
    include_databases: list[str],
    cadence_days: int,
    today: date,
) -> pd.DataFrame:
    latest_by_database = latest_session_by_database(sessions_df)
    cadence_days = max(int(cadence_days), 1)

    rows: list[dict[str, str]] = []
    for database in include_databases:
        latest_row = latest_by_database.get(database.lower())
        if latest_row is None:
            rows.append(
                {
                    "database": database,
                    "last_search_date": "",
                    "next_due_date": "",
                    "cadence_days": str(cadence_days),
                    "days_since_last_search": "",
                    "days_until_due": "",
                    "cadence_status": "no_prior_completed_search",
                    "last_query_version": "",
                    "last_results_total": "",
                    "last_results_exported": "",
                }
            )
            continue

        last_search_date = latest_row["search_date"]
        next_due_date = last_search_date + timedelta(days=cadence_days)
        days_since_last_search = int((today - last_search_date).days)
        days_until_due = int((next_due_date - today).days)

        if days_until_due < 0:
            cadence_status = "overdue"
        elif days_until_due == 0:
            cadence_status = "due_today"
        else:
            cadence_status = "upcoming"

        rows.append(
            {
                "database": normalize(latest_row.get("database", database)),
                "last_search_date": last_search_date.isoformat(),
                "next_due_date": next_due_date.isoformat(),
                "cadence_days": str(cadence_days),
                "days_since_last_search": str(days_since_last_search),
                "days_until_due": str(days_until_due),
                "cadence_status": cadence_status,
                "last_query_version": normalize(latest_row.get("query_version", "")),
                "last_results_total": format_int(latest_row.get("results_total_int")),
                "last_results_exported": format_int(latest_row.get("results_exported_int")),
            }
        )

    return pd.DataFrame(rows, columns=CADENCE_CHECK_COLUMNS)


def first_cycle_rows(schedule_df: pd.DataFrame) -> pd.DataFrame:
    if schedule_df.empty or "cycle_index" not in schedule_df.columns:
        return pd.DataFrame(columns=schedule_df.columns)
    return schedule_df.loc[schedule_df["cycle_index"].astype(str).eq("1")].copy()


def render_summary(
    *,
    search_log_path: Path,
    schedule_output_path: Path,
    diffs_output_path: Path,
    summary_output_path: Path,
    requested_review_mode: str,
    resolved_review_mode: str,
    review_mode_source: str,
    review_mode_signal: str,
    protocol_path: Path,
    cadence_check_enabled: bool,
    cadence_days: int,
    horizon_cycles: int,
    today: date,
    total_search_log_rows: int,
    include_databases: list[str],
    sessions_df: pd.DataFrame,
    diffs_df: pd.DataFrame,
    schedule_df: pd.DataFrame,
    cadence_check_df: pd.DataFrame,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = []
    lines.append("# Living Review Scheduler Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Search log input: `{search_log_path.as_posix()}`")
    lines.append(f"- Schedule output: `{schedule_output_path.as_posix()}`")
    lines.append(f"- Session diffs output: `{diffs_output_path.as_posix()}`")
    lines.append(f"- Summary output: `{summary_output_path.as_posix()}`")
    lines.append("")
    lines.append("## Policy")
    lines.append("")
    lines.append(f"- Requested review mode: `{requested_review_mode}`")
    lines.append(f"- Resolved review mode: `{resolved_review_mode}`")
    lines.append(f"- Review-mode source: `{review_mode_source}`")
    lines.append(f"- Protocol mode source file: `{protocol_path.as_posix()}`")
    if review_mode_signal:
        lines.append(f"- Protocol mode signal: `{review_mode_signal}`")
    lines.append(f"- Cadence check enabled: {'yes' if cadence_check_enabled else 'no'}")
    lines.append(f"- Cadence days: {cadence_days}")
    lines.append(f"- Horizon cycles: {horizon_cycles}")
    lines.append(f"- Today reference date: {today.isoformat()}")
    lines.append("")
    lines.append("## Coverage")
    lines.append("")
    lines.append(f"- Search log rows: {total_search_log_rows}")
    lines.append(f"- Databases listed in search log: {len(include_databases)}")
    lines.append(f"- Completed search sessions (dated rows): {int(sessions_df.shape[0])}")
    lines.append(f"- Session diff rows: {int(diffs_df.shape[0])}")
    lines.append(f"- Schedule rows: {int(schedule_df.shape[0])}")

    if include_databases:
        lines.append("")
        lines.append("## Databases")
        lines.append("")
        for database in include_databases:
            lines.append(f"- `{database}`")

    if not diffs_df.empty:
        query_changed = int(diffs_df["query_version_changed"].astype(str).str.lower().eq("yes").sum())
        filters_changed = int(diffs_df["filters_changed"].astype(str).str.lower().eq("yes").sum())
        severity_counts: Counter[str] = Counter(diffs_df.get("drift_severity", pd.Series(dtype=str)).fillna("").astype(str).str.strip())
        lines.append("")
        lines.append("## Session Diffs")
        lines.append("")
        lines.append(f"- Query-version changes detected: {query_changed}")
        lines.append(f"- Filter changes detected: {filters_changed}")
        for severity in ["high", "medium", "low"]:
            lines.append(f"- Drift severity `{severity}`: {severity_counts.get(severity, 0)}")

    if not schedule_df.empty:
        status_counts: Counter[str] = Counter(
            schedule_df["schedule_status"].fillna("").astype(str).str.strip().tolist()
        )
        lines.append("")
        lines.append("## Schedule Status")
        lines.append("")
        for status, count in sorted(status_counts.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"- `{status}`: {count}")

        initial_cycle_df = first_cycle_rows(schedule_df)
        if not initial_cycle_df.empty:
            lines.append("")
            lines.append("## Next Due Search")
            lines.append("")
            ordered_cycle_df = initial_cycle_df.sort_values(["database", "scheduled_search_date"], kind="stable")
            for _, row in ordered_cycle_df.iterrows():
                lines.append(
                    f"- `{normalize(row.get('database', ''))}`: "
                    f"{normalize(row.get('scheduled_search_date', ''))} "
                    f"({normalize(row.get('schedule_status', ''))})"
                )

    if cadence_check_enabled:
        lines.append("")
        lines.append("## Cadence Check")
        lines.append("")
        lines.append(f"- Databases evaluated: {int(cadence_check_df.shape[0])}")

        if cadence_check_df.empty:
            lines.append("- No database rows available for cadence check.")
        else:
            cadence_status_counts: Counter[str] = Counter(
                cadence_check_df["cadence_status"].fillna("").astype(str).str.strip().tolist()
            )
            for status, count in sorted(cadence_status_counts.items(), key=lambda item: (-item[1], item[0])):
                lines.append(f"- `{status}`: {count}")

            overdue_df = cadence_check_df.loc[
                cadence_check_df["cadence_status"].astype(str).str.lower().eq("overdue")
            ].sort_values(["database", "next_due_date"], kind="stable")
            if not overdue_df.empty:
                lines.append("")
                lines.append("### Overdue Databases")
                lines.append("")
                for _, row in overdue_df.iterrows():
                    days_until_due = normalize(row.get("days_until_due", ""))
                    overdue_days = ""
                    if days_until_due:
                        overdue_days = str(abs(int(days_until_due)))
                    lines.append(
                        f"- `{normalize(row.get('database', ''))}`: "
                        f"next due {normalize(row.get('next_due_date', ''))}"
                        f" ({overdue_days} days overdue)"
                    )

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Session diffs are computed per database from consecutive dated rows in `search_log.csv`.")
    lines.append("- Drift severity is deterministic (`high`/`medium`/`low`) and uses both strategy flags and result deltas (including absolute `delta_results_total >= 200` fallback).")
    lines.append("- Living schedule rows are generated only when resolved review mode is `living`.")
    lines.append("- Cadence check uses the latest dated search per database against `--cadence-days`.")
    lines.append("- Use this output for planning repeat searches; final search strategy decisions remain reviewer-led.")
    lines.append("")
    return "\n".join(lines)


def write_csv(path: Path, frame: pd.DataFrame, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    output_df = frame.copy()
    for column in columns:
        if column not in output_df.columns:
            output_df[column] = ""
    output_df = output_df.loc[:, columns]
    output_df.to_csv(path, index=False)


def parser() -> argparse.ArgumentParser:
    cli_parser = argparse.ArgumentParser(
        description=(
            "Generate repeat-search schedule for living reviews and calculate session-level diffs "
            "between search sessions in search_log.csv."
        )
    )
    cli_parser.add_argument(
        "--search-log",
        default="../02_data/processed/search_log.csv",
        help="Path to search_log.csv.",
    )
    cli_parser.add_argument(
        "--review-mode",
        choices=["auto", "standard", "living"],
        default="auto",
        help=(
            "Review mode (`auto` resolves from protocol.md living signals; "
            "`living` enables schedule generation; `standard` leaves schedule empty)."
        ),
    )
    cli_parser.add_argument(
        "--protocol",
        default="../01_protocol/protocol.md",
        help="Path to protocol markdown used for `--review-mode auto` resolution.",
    )
    cli_parser.add_argument(
        "--cadence-days",
        type=int,
        default=90,
        help="Days between repeat searches in living mode.",
    )
    cli_parser.add_argument(
        "--horizon-cycles",
        type=int,
        default=4,
        help="Number of future cycles to generate per database in living mode.",
    )
    cli_parser.add_argument(
        "--today",
        default="",
        help="Optional reference date in YYYY-MM-DD for deterministic scheduling.",
    )
    cli_parser.add_argument(
        "--check-cadence",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Evaluate cadence status (overdue/due_today/upcoming) from latest search dates and include in summary.",
    )
    cli_parser.add_argument(
        "--schedule-output",
        default="outputs/living_review_schedule.csv",
        help="Path to living-review schedule CSV output.",
    )
    cli_parser.add_argument(
        "--diffs-output",
        default="outputs/living_review_search_diffs.csv",
        help="Path to search-session diff CSV output.",
    )
    cli_parser.add_argument(
        "--summary-output",
        default="outputs/living_review_scheduler_summary.md",
        help="Path to scheduler markdown summary output.",
    )
    return cli_parser


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)

    if int(args.cadence_days) <= 0:
        raise SystemExit("--cadence-days must be > 0")
    if int(args.horizon_cycles) <= 0:
        raise SystemExit("--horizon-cycles must be > 0")

    search_log_path = Path(args.search_log)
    protocol_path = Path(args.protocol)
    schedule_output_path = Path(args.schedule_output)
    diffs_output_path = Path(args.diffs_output)
    summary_output_path = Path(args.summary_output)

    resolved_review_mode, review_mode_source, review_mode_signal = resolve_review_mode(args.review_mode, protocol_path)

    reference_today = parse_date(args.today) if normalize(args.today) else date.today()
    if reference_today is None:
        raise SystemExit("--today must be in a parseable date format (recommended YYYY-MM-DD)")

    search_log_df = read_search_log(search_log_path)
    total_search_log_rows = int(search_log_df.shape[0])

    databases = unique_preserve_order(
        [normalize(value) for value in search_log_df.get("database", pd.Series(dtype=str)).tolist() if normalize(value)]
    )

    sessions_df = prepare_search_sessions(search_log_df)
    if not databases and not sessions_df.empty:
        databases = unique_preserve_order(sessions_df["database"].astype(str).tolist())

    diffs_df = build_search_diffs(sessions_df)
    schedule_df = build_living_schedule(
        sessions_df,
        include_databases=databases,
        cadence_days=int(args.cadence_days),
        horizon_cycles=int(args.horizon_cycles),
        today=reference_today,
        review_mode=resolved_review_mode,
    )

    cadence_check_df = pd.DataFrame(columns=CADENCE_CHECK_COLUMNS)
    if args.check_cadence:
        cadence_check_df = build_cadence_check(
            sessions_df,
            include_databases=databases,
            cadence_days=int(args.cadence_days),
            today=reference_today,
        )

    write_csv(schedule_output_path, schedule_df, SCHEDULE_COLUMNS)
    write_csv(diffs_output_path, diffs_df, DIFF_COLUMNS)

    summary_text = render_summary(
        search_log_path=search_log_path,
        schedule_output_path=schedule_output_path,
        diffs_output_path=diffs_output_path,
        summary_output_path=summary_output_path,
        requested_review_mode=args.review_mode,
        resolved_review_mode=resolved_review_mode,
        review_mode_source=review_mode_source,
        review_mode_signal=review_mode_signal,
        protocol_path=protocol_path,
        cadence_check_enabled=bool(args.check_cadence),
        cadence_days=int(args.cadence_days),
        horizon_cycles=int(args.horizon_cycles),
        today=reference_today,
        total_search_log_rows=total_search_log_rows,
        include_databases=databases,
        sessions_df=sessions_df,
        diffs_df=diffs_df,
        schedule_df=schedule_df,
        cadence_check_df=cadence_check_df,
    )
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(summary_text, encoding="utf-8")

    print(f"Wrote: {schedule_output_path}")
    print(f"Wrote: {diffs_output_path}")
    print(f"Wrote: {summary_output_path}")
    overdue_count = 0
    if not cadence_check_df.empty and "cadence_status" in cadence_check_df.columns:
        overdue_count = int(cadence_check_df["cadence_status"].astype(str).str.lower().eq("overdue").sum())
    print(
        "Living scheduler stats: "
        f"requested_mode={args.review_mode}, "
        f"resolved_mode={resolved_review_mode}, "
        f"mode_source={review_mode_source}, "
        f"cadence_check={'on' if args.check_cadence else 'off'}, "
        f"overdue={overdue_count}, "
        f"sessions={int(sessions_df.shape[0])}, "
        f"diff_rows={int(diffs_df.shape[0])}, "
        f"schedule_rows={int(schedule_df.shape[0])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
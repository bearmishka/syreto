import argparse
from pathlib import Path

import pandas as pd


EMPTY_VALUES = {"", "nan", "none"}
RESULT_COLUMNS = [
    "record_id",
    "reviewer1_decision",
    "reviewer2_decision",
    "conflict",
    "conflict_resolver",
    "resolution_decision",
    "final_decision",
    "exclusion_reason",
]
DUAL_REQUIRED_COLUMNS = {"record_id", "reviewer", "title_abstract_decision"}
DECISION_NORMALIZATION = {
    "include": "include",
    "included": "include",
    "incl": "include",
    "in": "include",
    "yes": "include",
    "exclude": "exclude",
    "excluded": "exclude",
    "excl": "exclude",
    "ex": "exclude",
    "no": "exclude",
    "maybe": "uncertain",
    "uncertain": "uncertain",
    "unclear": "uncertain",
    "pending": "uncertain",
    "undecided": "uncertain",
}


def clean_text(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() in EMPTY_VALUES else text


def normalize_decision(value: object) -> str:
    text = clean_text(value).lower()
    return DECISION_NORMALIZATION.get(text, "")


def read_csv_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def empty_results_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=RESULT_COLUMNS)


def prepare_dual_log(dual_df: pd.DataFrame) -> pd.DataFrame:
    if dual_df.empty or not DUAL_REQUIRED_COLUMNS.issubset(dual_df.columns):
        return pd.DataFrame(columns=["record_id", "reviewer", "decision", "decision_date_parsed"])

    prepared = dual_df.copy()
    prepared["record_id"] = prepared["record_id"].apply(clean_text)
    prepared["reviewer"] = prepared["reviewer"].apply(clean_text)
    prepared["decision"] = prepared["title_abstract_decision"].apply(normalize_decision)
    prepared["_row_order"] = range(len(prepared))

    if "decision_date" in prepared.columns:
        prepared["decision_date_parsed"] = pd.to_datetime(prepared["decision_date"], errors="coerce")
        prepared = prepared.sort_values(
            ["record_id", "reviewer", "decision_date_parsed", "_row_order"],
            kind="stable",
            na_position="last",
        )
    else:
        prepared["decision_date_parsed"] = pd.NaT
        prepared = prepared.sort_values(["record_id", "reviewer", "_row_order"], kind="stable")

    prepared = prepared[
        prepared["record_id"].ne("")
        & prepared["reviewer"].ne("")
        & prepared["decision"].ne("")
    ]
    if prepared.empty:
        return pd.DataFrame(columns=["record_id", "reviewer", "decision", "decision_date_parsed"])

    prepared = prepared.drop_duplicates(["record_id", "reviewer"], keep="last")
    return prepared[["record_id", "reviewer", "decision", "decision_date_parsed"]].copy()


def choose_two_reviewer_decisions(record_df: pd.DataFrame) -> tuple[str, str]:
    if record_df.empty:
        return "", ""

    ordered = record_df.copy()
    if "decision_date_parsed" in ordered.columns and ordered["decision_date_parsed"].notna().any():
        ordered = ordered.sort_values(["decision_date_parsed", "reviewer"], kind="stable", na_position="last")
    else:
        ordered = ordered.sort_values(["reviewer"], kind="stable")

    decisions = ordered["decision"].tolist()
    reviewer1_decision = decisions[0] if len(decisions) >= 1 else ""
    reviewer2_decision = decisions[1] if len(decisions) >= 2 else ""
    return reviewer1_decision, reviewer2_decision


def clean_existing_results(existing_df: pd.DataFrame) -> pd.DataFrame:
    if existing_df.empty or "record_id" not in existing_df.columns:
        return pd.DataFrame(columns=RESULT_COLUMNS)

    existing = existing_df.copy()
    for column in RESULT_COLUMNS:
        if column not in existing.columns:
            existing[column] = ""

    existing["record_id"] = existing["record_id"].apply(clean_text)
    existing = existing[existing["record_id"].ne("")].copy()
    if existing.empty:
        return pd.DataFrame(columns=RESULT_COLUMNS)

    for decision_column in ["reviewer1_decision", "reviewer2_decision", "resolution_decision", "final_decision"]:
        existing[decision_column] = existing[decision_column].apply(normalize_decision)

    for text_column in ["conflict", "conflict_resolver", "exclusion_reason"]:
        existing[text_column] = existing[text_column].apply(clean_text)

    existing = existing.drop_duplicates(["record_id"], keep="last")
    return existing.loc[:, RESULT_COLUMNS].copy()


def build_consensus_results(
    prepared_dual_log: pd.DataFrame,
    existing_results_df: pd.DataFrame,
    *,
    default_conflict_resolver: str,
    default_resolution_decision: str,
) -> pd.DataFrame:
    if prepared_dual_log.empty:
        return empty_results_frame()

    existing = clean_existing_results(existing_results_df)
    existing_by_record = existing.set_index("record_id") if not existing.empty else pd.DataFrame()

    rows: list[dict[str, str]] = []
    for record_id, group in prepared_dual_log.groupby("record_id", sort=True):
        reviewer1_decision, reviewer2_decision = choose_two_reviewer_decisions(group)

        has_two_reviewers = bool(reviewer1_decision and reviewer2_decision)
        has_conflict = has_two_reviewers and reviewer1_decision != reviewer2_decision
        conflict_flag = "yes" if has_conflict else ("no" if has_two_reviewers else "")

        previous = existing_by_record.loc[record_id] if (not existing_by_record.empty and record_id in existing_by_record.index) else None
        previous_resolution = normalize_decision(previous["resolution_decision"]) if previous is not None else ""
        previous_resolver = clean_text(previous["conflict_resolver"]) if previous is not None else ""
        previous_exclusion_reason = clean_text(previous["exclusion_reason"]) if previous is not None else ""

        if has_conflict:
            resolution_decision = previous_resolution or default_resolution_decision
            conflict_resolver = previous_resolver or default_conflict_resolver
            final_decision = resolution_decision
        else:
            resolution_decision = ""
            conflict_resolver = ""
            if reviewer1_decision and reviewer2_decision and reviewer1_decision == reviewer2_decision:
                final_decision = reviewer1_decision
            elif reviewer1_decision and not reviewer2_decision:
                final_decision = reviewer1_decision
            else:
                final_decision = ""

        exclusion_reason = previous_exclusion_reason if final_decision == "exclude" else ""

        rows.append(
            {
                "record_id": record_id,
                "reviewer1_decision": reviewer1_decision,
                "reviewer2_decision": reviewer2_decision,
                "conflict": conflict_flag,
                "conflict_resolver": conflict_resolver,
                "resolution_decision": resolution_decision,
                "final_decision": final_decision,
                "exclusion_reason": exclusion_reason,
            }
        )

    return pd.DataFrame(rows, columns=RESULT_COLUMNS)


def build_summary(
    *,
    dual_log_path: Path,
    existing_results_path: Path,
    results_output_path: Path,
    summary_path: Path,
    results_df: pd.DataFrame,
    default_conflict_resolver: str,
    default_resolution_decision: str,
) -> str:
    total_records = int(results_df.shape[0])
    conflict_mask = results_df["conflict"].fillna("").astype(str).str.lower().eq("yes") if not results_df.empty else pd.Series(dtype=bool)
    conflict_records = int(conflict_mask.sum()) if not results_df.empty else 0

    pending_conflicts = 0
    if not results_df.empty and conflict_records > 0:
        pending_conflicts = int(
            results_df.loc[conflict_mask, "conflict_resolver"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .eq(default_conflict_resolver.lower())
            .sum()
        )

    lines: list[str] = []
    lines.append("# Title/Abstract Consensus Consolidation Summary")
    lines.append("")
    lines.append("## I/O")
    lines.append("")
    lines.append(f"- Dual log input: `{dual_log_path.as_posix()}`")
    lines.append(f"- Existing results baseline: `{existing_results_path.as_posix()}`")
    lines.append(f"- Results output: `{results_output_path.as_posix()}`")
    lines.append(f"- Summary output: `{summary_path.as_posix()}`")
    lines.append("")
    lines.append("## Snapshot")
    lines.append("")
    lines.append(f"- Consolidated records: {total_records}")
    lines.append(f"- Conflicts detected: {conflict_records}")
    lines.append(f"- Conflicts with pending placeholder resolver: {pending_conflicts}")
    lines.append(f"- Default conflict resolver marker: `{default_conflict_resolver}`")
    lines.append(f"- Default resolution decision on conflict: `{default_resolution_decision}`")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- For unresolved disagreements, replace placeholder resolver IDs after adjudication.")
    lines.append("- `final_decision` is always synchronized to `resolution_decision` for conflict rows.")
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Consolidate dual title/abstract screening into consensus results CSV.")
    parser.add_argument(
        "--dual-log",
        default="../02_data/processed/screening_title_abstract_dual_log.csv",
        help="Path to dual title/abstract log CSV",
    )
    parser.add_argument(
        "--results-output",
        default="../02_data/processed/screening_title_abstract_results.csv",
        help="Path to consolidated title/abstract results CSV",
    )
    parser.add_argument(
        "--existing-results",
        default="",
        help="Optional baseline results CSV for preserving manual fields; defaults to --results-output if file exists.",
    )
    parser.add_argument(
        "--summary",
        default="outputs/title_abstract_consensus_consolidation_summary.md",
        help="Path to markdown summary",
    )
    parser.add_argument(
        "--default-conflict-resolver",
        default="consensus_pending",
        help="Placeholder resolver ID used when disagreements have no preserved resolver yet.",
    )
    parser.add_argument(
        "--default-resolution-decision",
        default="uncertain",
        choices=["include", "exclude", "uncertain"],
        help="Fallback resolution_decision for unresolved disagreements.",
    )
    args = parser.parse_args(argv)

    dual_log_path = Path(args.dual_log)
    results_output_path = Path(args.results_output)
    summary_path = Path(args.summary)

    if args.existing_results:
        existing_results_path = Path(args.existing_results)
    elif results_output_path.exists():
        existing_results_path = results_output_path
    else:
        existing_results_path = Path(args.results_output)

    dual_df = read_csv_or_empty(dual_log_path)
    missing_required = sorted(DUAL_REQUIRED_COLUMNS - set(dual_df.columns)) if not dual_df.empty else []
    if dual_df.empty:
        if not dual_log_path.exists():
            raise FileNotFoundError(f"Dual log not found: {dual_log_path}")
        if missing_required:
            raise ValueError(f"Dual log is missing required columns: {', '.join(missing_required)}")

    if missing_required:
        raise ValueError(f"Dual log is missing required columns: {', '.join(missing_required)}")

    existing_results_df = read_csv_or_empty(existing_results_path)
    prepared_dual_log = prepare_dual_log(dual_df)
    results_df = build_consensus_results(
        prepared_dual_log,
        existing_results_df,
        default_conflict_resolver=clean_text(args.default_conflict_resolver),
        default_resolution_decision=normalize_decision(args.default_resolution_decision),
    )

    if results_df.empty:
        results_df = empty_results_frame()

    results_output_path.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(results_output_path, index=False)

    summary = build_summary(
        dual_log_path=dual_log_path,
        existing_results_path=existing_results_path,
        results_output_path=results_output_path,
        summary_path=summary_path,
        results_df=results_df,
        default_conflict_resolver=clean_text(args.default_conflict_resolver),
        default_resolution_decision=normalize_decision(args.default_resolution_decision),
    )
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(summary, encoding="utf-8")

    print(f"Wrote: {results_output_path}")
    print(f"Wrote: {summary_path}")
    print(f"Consolidated records: {int(results_df.shape[0])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
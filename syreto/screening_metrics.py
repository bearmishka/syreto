import argparse
from pathlib import Path
import pandas as pd


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
    "unclear": "uncertain",
    "pending": "uncertain",
    "undecided": "uncertain",
    "uncertain": "uncertain",
}


def pct(numerator: float, denominator: float) -> str:
    if denominator <= 0:
        return "0.0%"
    return f"{(100.0 * numerator / denominator):.1f}%"


def read_csv_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def normalize_lower(value: object) -> str:
    return str(value if value is not None else "").strip().lower()


def normalize_decision(value: object) -> str:
    return DECISION_NORMALIZATION.get(normalize_lower(value), "")


def is_conflict_value(value: object) -> bool:
    return normalize_lower(value) in {"yes", "y", "1", "true"}


def consensus_stats_from_results(df: pd.DataFrame) -> dict:
    result = {
        "available": False,
        "records_screened": 0,
        "records_excluded": 0,
        "conflicts": 0,
        "conflict_rate": "0.0%",
        "reason": "No title/abstract consensus results found.",
    }

    if df.empty:
        return result

    if "record_id" not in df.columns:
        result["reason"] = (
            "Missing required `record_id` column in title/abstract consensus results."
        )
        return result

    prepared = df.copy()
    prepared["record_id"] = prepared["record_id"].fillna("").astype(str).str.strip()
    prepared = prepared[prepared["record_id"].ne("")].copy()
    if prepared.empty:
        result["reason"] = "No non-empty `record_id` values in title/abstract consensus results."
        return result

    prepared = prepared.drop_duplicates(["record_id"], keep="last")
    prepared["final_decision_norm"] = prepared.get("final_decision", "").apply(normalize_decision)

    if "conflict" in prepared.columns:
        prepared["conflict_norm"] = prepared["conflict"].apply(is_conflict_value)
    else:
        reviewer1 = prepared.get("reviewer1_decision", "").apply(normalize_decision)
        reviewer2 = prepared.get("reviewer2_decision", "").apply(normalize_decision)
        prepared["conflict_norm"] = reviewer1.ne("") & reviewer2.ne("") & reviewer1.ne(reviewer2)

    records_screened = int(prepared.shape[0])
    records_excluded = int(prepared["final_decision_norm"].eq("exclude").sum())
    conflicts = int(prepared["conflict_norm"].sum())

    result["available"] = True
    result["records_screened"] = records_screened
    result["records_excluded"] = records_excluded
    result["conflicts"] = conflicts
    result["conflict_rate"] = pct(float(conflicts), float(records_screened))
    result["reason"] = "Computed from title/abstract consensus results."
    return result


def cohen_kappa_from_dual_log(df: pd.DataFrame) -> dict:
    result = {
        "available": False,
        "pair": None,
        "paired_records": 0,
        "kappa": None,
        "reason": "No record-level screening data found.",
    }

    required = {"record_id", "reviewer", "title_abstract_decision"}
    if df.empty:
        return result
    if not required.issubset(df.columns):
        result["reason"] = "Missing required columns in dual-screening log."
        return result

    log = df.copy()
    log["record_id"] = log["record_id"].fillna("").astype(str).str.strip()
    log["reviewer"] = log["reviewer"].fillna("").astype(str).str.strip()
    log["title_abstract_decision"] = (
        log["title_abstract_decision"].fillna("").astype(str).str.strip().str.lower()
    )

    log = log[
        log["record_id"].ne("") & log["reviewer"].ne("") & log["title_abstract_decision"].ne("")
    ]
    if log.empty:
        result["reason"] = "No non-empty title/abstract decisions in dual-screening log."
        return result

    deduped = log.drop_duplicates(["record_id", "reviewer"], keep="last")
    reviewers = sorted(deduped["reviewer"].unique())
    if len(reviewers) < 2:
        result["reason"] = "Fewer than two reviewers in title/abstract decisions."
        return result

    pivot = deduped.pivot(index="record_id", columns="reviewer", values="title_abstract_decision")

    best_pair: tuple[str, str] | None = None
    best_overlap = 0
    for index, reviewer_a in enumerate(reviewers):
        for reviewer_b in reviewers[index + 1 :]:
            overlap = int(pivot[[reviewer_a, reviewer_b]].dropna().shape[0])
            if overlap > best_overlap:
                best_overlap = overlap
                best_pair = (reviewer_a, reviewer_b)

    if not best_pair or best_overlap == 0:
        result["reason"] = "No overlapping records with independent reviewer decisions."
        return result

    pair_df = pivot[[best_pair[0], best_pair[1]]].dropna()
    decisions_a = pair_df[best_pair[0]]
    decisions_b = pair_df[best_pair[1]]

    observed = float((decisions_a == decisions_b).mean())
    categories = sorted(set(decisions_a.unique()) | set(decisions_b.unique()))
    expected = 0.0
    for category in categories:
        p_a = float((decisions_a == category).mean())
        p_b = float((decisions_b == category).mean())
        expected += p_a * p_b

    if expected >= 1.0:
        result["reason"] = "Cohen's kappa undefined because expected agreement is 1.0."
        return result

    kappa = (observed - expected) / (1.0 - expected)
    result["available"] = True
    result["pair"] = f"{best_pair[0]} vs {best_pair[1]}"
    result["paired_records"] = int(pair_df.shape[0])
    result["kappa"] = round(float(kappa), 3)
    result["reason"] = "Computed from record-level title/abstract dual log."
    return result


def consensus_stats_block(consensus_stats: dict, consensus_input_path: Path) -> str:
    lines: list[str] = []
    lines.append("## Screening Statistics (Title/Abstract Consensus)")
    lines.append("")
    lines.append(f"- Source: `{consensus_input_path.as_posix()}`")

    if consensus_stats["available"]:
        lines.append(f"- records_screened: {consensus_stats['records_screened']}")
        lines.append(f"- records_excluded: {consensus_stats['records_excluded']}")
        lines.append(f"- conflicts: {consensus_stats['conflicts']}")
        lines.append(f"- conflict_rate: {consensus_stats['conflict_rate']}")
    else:
        lines.append(f"- Not available ({consensus_stats['reason']})")

    return "\n".join(lines) + "\n"


def screening_statistics_frame(consensus_stats: dict) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "records_screened": int(consensus_stats.get("records_screened", 0)),
                "records_excluded": int(consensus_stats.get("records_excluded", 0)),
                "conflicts": int(consensus_stats.get("conflicts", 0)),
                "conflict_rate": str(consensus_stats.get("conflict_rate", "0.0%")),
            }
        ]
    )


def build_summary(
    df: pd.DataFrame,
    kappa_stats: dict,
    agreement_input_path: Path,
    consensus_stats: dict,
    consensus_input_path: Path,
) -> str:
    sessions = len(df)
    total_records = float(df["records_screened"].sum())
    total_minutes = float(df["time_spent_minutes"].sum())
    total_includes = float(df["include_n"].sum())
    total_excludes = float(df["exclude_n"].sum())
    total_maybe = float(df["maybe_n"].sum())
    total_pending = float(df["pending_n"].sum())

    records_per_hour = (60.0 * total_records / total_minutes) if total_minutes > 0 else 0.0
    active_days = int(df["date_parsed"].dropna().nunique())
    avg_records_per_day = (total_records / active_days) if active_days > 0 else 0.0

    lines = []
    lines.append("# Screening Metrics Summary")
    lines.append("")
    lines.append("## Overall")
    lines.append("")
    lines.append(f"- Sessions: {sessions}")
    lines.append(f"- Records screened: {int(total_records)}")
    lines.append(f"- Time spent (minutes): {int(total_minutes)}")
    lines.append(f"- Records per hour: {records_per_hour:.1f}")
    lines.append(f"- Active screening days: {active_days}")
    lines.append(f"- Average records per active day: {avg_records_per_day:.1f}")
    lines.append("")
    lines.append("## Decision Profile")
    lines.append("")
    lines.append(f"- Includes: {int(total_includes)} ({pct(total_includes, total_records)})")
    lines.append(f"- Excludes: {int(total_excludes)} ({pct(total_excludes, total_records)})")
    lines.append(f"- Maybe: {int(total_maybe)} ({pct(total_maybe, total_records)})")
    lines.append(f"- Pending: {int(total_pending)} ({pct(total_pending, total_records)})")

    lines.append("")
    lines.append("## Screening Statistics (Title/Abstract Consensus)")
    lines.append("")
    lines.append(f"- Source: `{consensus_input_path.as_posix()}`")
    if consensus_stats["available"]:
        lines.append(f"- records_screened: {consensus_stats['records_screened']}")
        lines.append(f"- records_excluded: {consensus_stats['records_excluded']}")
        lines.append(f"- conflicts: {consensus_stats['conflicts']}")
        lines.append(f"- conflict_rate: {consensus_stats['conflict_rate']}")
    else:
        lines.append(f"- Not available ({consensus_stats['reason']})")

    if "reviewer" in df.columns and df["reviewer"].fillna("").astype(str).str.strip().ne("").any():
        grouped = (
            df.assign(reviewer=df["reviewer"].fillna("").astype(str).str.strip())
            .query("reviewer != ''")
            .groupby("reviewer", as_index=False)
            .agg(
                sessions=("reviewer", "count"),
                records_screened=("records_screened", "sum"),
                include_n=("include_n", "sum"),
                time_spent_minutes=("time_spent_minutes", "sum"),
            )
        )

        if not grouped.empty:
            lines.append("")
            lines.append("## Reviewer Breakdown")
            lines.append("")
            lines.append(
                "| Reviewer | Sessions | Records | Minutes | Records/hour | Include rate |"
            )
            lines.append("|---|---:|---:|---:|---:|---:|")
            for _, row in grouped.iterrows():
                rec = float(row["records_screened"])
                mins = float(row["time_spent_minutes"])
                inc = float(row["include_n"])
                rph = (60.0 * rec / mins) if mins > 0 else 0.0
                lines.append(
                    f"| {row['reviewer']} | {int(row['sessions'])} | {int(rec)} | {int(mins)} | {rph:.1f} | {pct(inc, rec)} |"
                )

    lines.append("")
    lines.append("## Inter-rater Reliability")
    lines.append("")
    lines.append("- Stage: title/abstract")
    lines.append(f"- Source: `{agreement_input_path.as_posix()}`")
    if kappa_stats["available"]:
        lines.append(f"- Reviewer pair: {kappa_stats['pair']}")
        lines.append(f"- Paired records: {kappa_stats['paired_records']}")
        lines.append(f"- Cohen's kappa: {kappa_stats['kappa']}")
    else:
        lines.append(f"- Cohen's kappa: not available ({kappa_stats['reason']})")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Generated automatically from `02_data/processed/screening_daily_log.csv`.")
    lines.append("- Empty rows are ignored.")

    return "\n".join(lines) + "\n"


def inter_rater_block(kappa_stats: dict, agreement_input_path: Path) -> str:
    lines = []
    lines.append("## Inter-rater Reliability")
    lines.append("")
    lines.append("- Stage: title/abstract")
    lines.append(f"- Source: `{agreement_input_path.as_posix()}`")
    if kappa_stats["available"]:
        lines.append(f"- Reviewer pair: {kappa_stats['pair']}")
        lines.append(f"- Paired records: {kappa_stats['paired_records']}")
        lines.append(f"- Cohen's kappa: {kappa_stats['kappa']}")
    else:
        lines.append(f"- Cohen's kappa: not available ({kappa_stats['reason']})")

    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate screening metrics summary from daily screening log."
    )
    parser.add_argument(
        "--input",
        default="../02_data/processed/screening_daily_log.csv",
        help="Path to screening daily log CSV",
    )
    parser.add_argument(
        "--output",
        default="outputs/screening_metrics_summary.md",
        help="Path to markdown summary output",
    )
    parser.add_argument(
        "--stats-output",
        default="outputs/screening_statistics.csv",
        help="Path to CSV output with screening statistics from title/abstract consensus",
    )
    parser.add_argument(
        "--agreement-input",
        default="../02_data/processed/screening_title_abstract_dual_log.csv",
        help="Path to record-level dual-screening decisions CSV",
    )
    parser.add_argument(
        "--consensus-input",
        default="../02_data/processed/screening_title_abstract_results.csv",
        help="Path to consolidated title/abstract consensus results CSV",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    stats_output_path = Path(args.stats_output)
    agreement_input_path = Path(args.agreement_input)
    consensus_input_path = Path(args.consensus_input)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pd.read_csv(input_path)
    agreement_df = read_csv_or_empty(agreement_input_path)
    consensus_df = read_csv_or_empty(consensus_input_path)
    kappa_stats = cohen_kappa_from_dual_log(agreement_df)
    consensus_stats = consensus_stats_from_results(consensus_df)

    if df.empty:
        summary = "# Screening Metrics Summary\n\nNo screening sessions logged yet.\n\n"
        summary += consensus_stats_block(consensus_stats, consensus_input_path)
        summary += "\n"
        summary += inter_rater_block(kappa_stats, agreement_input_path)
    else:
        key_cols = [
            "date",
            "reviewer",
            "stage",
            "records_screened",
            "include_n",
            "exclude_n",
            "maybe_n",
            "pending_n",
            "time_spent_minutes",
        ]

        for col in key_cols:
            if col not in df.columns:
                df[col] = ""

        raw = df[key_cols].copy()
        for col in key_cols:
            raw[col] = raw[col].fillna("").astype(str).str.strip()

        non_empty = raw.apply(
            lambda row: any(cell != "" and cell.lower() != "nan" for cell in row), axis=1
        )
        df = df[non_empty].copy()

        if df.empty:
            summary = "# Screening Metrics Summary\n\nNo screening sessions logged yet.\n\n"
            summary += consensus_stats_block(consensus_stats, consensus_input_path)
            summary += "\n"
            summary += inter_rater_block(kappa_stats, agreement_input_path)
        else:
            for col in [
                "records_screened",
                "include_n",
                "exclude_n",
                "maybe_n",
                "pending_n",
                "time_spent_minutes",
            ]:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            df["date"] = df["date"].fillna("").astype(str).str.strip()
            df["date_parsed"] = pd.to_datetime(df["date"], errors="coerce")

            summary = build_summary(
                df,
                kappa_stats,
                agreement_input_path,
                consensus_stats,
                consensus_input_path,
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(summary, encoding="utf-8")
    stats_output_path.parent.mkdir(parents=True, exist_ok=True)
    screening_statistics_frame(consensus_stats).to_csv(stats_output_path, index=False)
    print(f"Wrote: {output_path}")
    print(f"Wrote: {stats_output_path}")


if __name__ == "__main__":
    main()

import argparse
import re
from datetime import datetime
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
    "maybe": "maybe",
    "unclear": "maybe",
    "pending": "maybe",
    "undecided": "maybe",
}

ARTICLE_TYPE_RULES: list[tuple[str, tuple[str, ...]]] = [
    (
        "protocol/methods",
        (
            r"\bstudy protocol\b",
            r"\bprotocol\b",
            r"\bmethods? paper\b",
            r"\bmethodological\b",
        ),
    ),
    (
        "review/meta-analysis",
        (
            r"\bsystematic review\b",
            r"\bmeta[ -]?analysis\b",
            r"\bscoping review\b",
            r"\bnarrative review\b",
            r"\breview\b",
        ),
    ),
    (
        "case report/series",
        (
            r"\bcase report\b",
            r"\bcase series\b",
        ),
    ),
    (
        "randomized/intervention trial",
        (
            r"\brandomi[sz]ed\b",
            r"\bclinical trial\b",
            r"\bcontrolled trial\b",
            r"\bintervention\b",
            r"\bpilot trial\b",
        ),
    ),
    (
        "observational cohort/cross-sectional",
        (
            r"\bcohort\b",
            r"\bcross[ -]?sectional\b",
            r"\bcase[ -]?control\b",
            r"\bobservational\b",
            r"\blongitudinal\b",
            r"\bregistry\b",
        ),
    ),
    (
        "qualitative/mixed methods",
        (
            r"\bqualitative\b",
            r"\binterview\b",
            r"\bfocus group\b",
            r"\bthematic\b",
            r"\bmixed[ -]?methods?\b",
        ),
    ),
    (
        "guideline/editorial/commentary",
        (
            r"\bguideline\b",
            r"\bconsensus\b",
            r"\bposition statement\b",
            r"\beditorial\b",
            r"\bcommentary\b",
            r"\bletter\b",
        ),
    ),
    (
        "animal/preclinical",
        (
            r"\bmouse\b",
            r"\bmice\b",
            r"\brat\b",
            r"\brodent\b",
            r"\banimal model\b",
            r"\bpreclinical\b",
            r"\bin vitro\b",
        ),
    ),
    (
        "conference abstract",
        (
            r"\bconference\b",
            r"\bmeeting abstract\b",
            r"\bposter\b",
        ),
    ),
]


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


def normalize_decision(value: object) -> str:
    text = str(value).strip().lower()
    if text in DECISION_NORMALIZATION:
        return DECISION_NORMALIZATION[text]
    return ""


def canonical_decision_pair(decision_a: str, decision_b: str) -> str:
    ordered = sorted([decision_a, decision_b])
    if ordered[0] == ordered[1]:
        return f"{ordered[0]} = {ordered[1]}"
    return f"{ordered[0]} vs {ordered[1]}"


def normalize_free_text(value: object) -> str:
    text = str(value).strip().lower()
    return re.sub(r"\s+", " ", text)


def classify_article_type(*, title: object, abstract: object, journal: object) -> str:
    blob = " ".join(
        part
        for part in [
            normalize_free_text(title),
            normalize_free_text(abstract),
            normalize_free_text(journal),
        ]
        if part
    )
    if not blob:
        return "other/unclear"

    for label, patterns in ARTICLE_TYPE_RULES:
        if any(re.search(pattern, blob) for pattern in patterns):
            return label
    return "other/unclear"


def prepare_dual_log(df: pd.DataFrame) -> pd.DataFrame:
    required = {"record_id", "reviewer", "title_abstract_decision"}
    if df.empty or not required.issubset(df.columns):
        return pd.DataFrame(columns=["record_id", "reviewer", "decision"])

    prepared = df.copy()
    prepared["record_id"] = prepared["record_id"].fillna("").astype(str).str.strip()
    prepared["reviewer"] = prepared["reviewer"].fillna("").astype(str).str.strip()
    prepared["decision"] = prepared["title_abstract_decision"].apply(normalize_decision)

    if "decision_date" in prepared.columns:
        prepared["decision_date"] = pd.to_datetime(prepared["decision_date"], errors="coerce")
        prepared = prepared.sort_values(["record_id", "reviewer", "decision_date"], kind="stable")
    else:
        prepared = prepared.sort_values(["record_id", "reviewer"], kind="stable")

    prepared = prepared[
        prepared["record_id"].ne("") & prepared["reviewer"].ne("") & prepared["decision"].ne("")
    ]
    if prepared.empty:
        return pd.DataFrame(columns=["record_id", "reviewer", "decision"])

    prepared = prepared.drop_duplicates(["record_id", "reviewer"], keep="last")
    return prepared[["record_id", "reviewer", "decision"]].copy()


def best_reviewer_pair(prepared_dual_log: pd.DataFrame) -> tuple[str, str] | None:
    if prepared_dual_log.empty:
        return None

    reviewers = sorted(prepared_dual_log["reviewer"].unique())
    if len(reviewers) < 2:
        return None

    pivot = prepared_dual_log.pivot(index="record_id", columns="reviewer", values="decision")

    best_pair: tuple[str, str] | None = None
    best_overlap = 0
    for index, reviewer_a in enumerate(reviewers):
        for reviewer_b in reviewers[index + 1 :]:
            overlap = int(pivot[[reviewer_a, reviewer_b]].dropna().shape[0])
            if overlap > best_overlap:
                best_overlap = overlap
                best_pair = (reviewer_a, reviewer_b)

    return best_pair


def disagreement_analysis(
    dual_log_df: pd.DataFrame,
    master_records_df: pd.DataFrame,
    *,
    top_records: int,
) -> dict:
    analysis = {
        "available": False,
        "reason": "No dual-screening data available.",
        "reviewer_pair": None,
        "paired_records": 0,
        "agreements": 0,
        "disagreements": 0,
        "decision_distribution": pd.DataFrame(columns=["decision_pair", "records", "share"]),
        "type_patterns": pd.DataFrame(
            columns=[
                "article_type",
                "paired_records",
                "disagreements",
                "disagreement_rate",
                "top_conflict",
            ]
        ),
        "meeting_records": pd.DataFrame(),
    }

    prepared = prepare_dual_log(dual_log_df)
    if prepared.empty:
        analysis["reason"] = "No valid dual-screening decisions after cleaning."
        return analysis

    pair = best_reviewer_pair(prepared)
    if pair is None:
        analysis["reason"] = "Fewer than two reviewers with overlapping decisions."
        return analysis

    pivot = prepared.pivot(index="record_id", columns="reviewer", values="decision")
    pair_df = (
        pivot[[pair[0], pair[1]]]
        .dropna()
        .reset_index()
        .rename(columns={pair[0]: "decision_a", pair[1]: "decision_b"})
    )

    if pair_df.empty:
        analysis["reason"] = "No overlapping records for reviewer pair."
        return analysis

    pair_df["agreement"] = pair_df["decision_a"].eq(pair_df["decision_b"])
    pair_df["decision_pair"] = pair_df.apply(
        lambda row: canonical_decision_pair(row["decision_a"], row["decision_b"]),
        axis=1,
    )

    analysis["available"] = True
    analysis["reason"] = "Computed from record-level title/abstract dual log."
    analysis["reviewer_pair"] = f"{pair[0]} vs {pair[1]}"
    analysis["paired_records"] = int(pair_df.shape[0])
    analysis["agreements"] = int(pair_df["agreement"].sum())
    analysis["disagreements"] = int((~pair_df["agreement"]).sum())

    decision_distribution = (
        pair_df.groupby("decision_pair", as_index=False)
        .size()
        .rename(columns={"size": "records"})
        .sort_values("records", ascending=False, kind="stable")
    )
    decision_distribution["share"] = decision_distribution["records"].apply(
        lambda value: pct(float(value), float(pair_df.shape[0]))
    )
    analysis["decision_distribution"] = decision_distribution

    metadata_columns = ["record_id", "title", "abstract", "journal", "source_database", "year"]
    if master_records_df.empty or "record_id" not in master_records_df.columns:
        metadata = pd.DataFrame(columns=metadata_columns)
    else:
        metadata = master_records_df.copy()
        metadata["record_id"] = metadata["record_id"].fillna("").astype(str).str.strip()
        for column in metadata_columns:
            if column not in metadata.columns:
                metadata[column] = ""
        metadata = metadata[metadata_columns].drop_duplicates("record_id", keep="first")

    merged = pair_df.merge(metadata, on="record_id", how="left")
    merged["article_type"] = merged.apply(
        lambda row: classify_article_type(
            title=row.get("title", ""),
            abstract=row.get("abstract", ""),
            journal=row.get("journal", ""),
        ),
        axis=1,
    )

    per_type = (
        merged.groupby("article_type", as_index=False)
        .agg(
            paired_records=("record_id", "count"),
            disagreements=("agreement", lambda values: int((~values).sum())),
        )
        .sort_values(
            ["disagreements", "paired_records", "article_type"], ascending=[False, False, True]
        )
    )
    per_type["disagreement_rate"] = per_type.apply(
        lambda row: pct(float(row["disagreements"]), float(row["paired_records"])),
        axis=1,
    )

    disagreement_only = merged[~merged["agreement"]].copy()

    top_conflicts_by_type: dict[str, str] = {}
    if not disagreement_only.empty:
        conflict_counts = (
            disagreement_only.groupby(["article_type", "decision_pair"], as_index=False)
            .size()
            .rename(columns={"size": "n"})
            .sort_values(["article_type", "n", "decision_pair"], ascending=[True, False, True])
        )
        for article_type, group_df in conflict_counts.groupby("article_type"):
            top_row = group_df.iloc[0]
            top_conflicts_by_type[article_type] = (
                f"{top_row['decision_pair']} ({int(top_row['n'])})"
            )

    per_type["top_conflict"] = per_type["article_type"].map(top_conflicts_by_type).fillna("—")
    analysis["type_patterns"] = per_type

    if disagreement_only.empty:
        analysis["meeting_records"] = pd.DataFrame(
            columns=[
                "record_id",
                "article_type",
                "decision_a",
                "decision_b",
                "source_database",
                "year",
                "journal",
                "title",
            ]
        )
    else:
        for column in ["source_database", "year", "journal", "title", "abstract"]:
            if column not in disagreement_only.columns:
                disagreement_only[column] = ""
            disagreement_only[column] = disagreement_only[column].fillna("").astype(str).str.strip()

        disagreement_only["title"] = disagreement_only["title"].apply(
            lambda text: text if len(text) <= 140 else f"{text[:137]}..."
        )
        disagreement_only = disagreement_only.sort_values(
            ["article_type", "decision_pair", "record_id"],
            kind="stable",
        )

        analysis["meeting_records"] = disagreement_only[
            [
                "record_id",
                "article_type",
                "decision_a",
                "decision_b",
                "source_database",
                "year",
                "journal",
                "title",
            ]
        ].head(top_records)

    return analysis


def build_markdown_report(
    *,
    analysis: dict,
    dual_log_path: Path,
    master_records_path: Path,
    output_path: Path,
    patterns_output_path: Path,
    records_output_path: Path,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines: list[str] = []

    lines.append("# Screening Disagreement Analyzer Report")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    lines.append(f"- Dual log: `{dual_log_path.as_posix()}`")
    lines.append(f"- Master records: `{master_records_path.as_posix()}`")
    lines.append(f"- Patterns CSV: `{patterns_output_path.as_posix()}`")
    lines.append(f"- Calibration records CSV: `{records_output_path.as_posix()}`")
    lines.append(f"- Report path: `{output_path.as_posix()}`")
    lines.append("")

    lines.append("## Overview")
    lines.append("")
    if analysis["available"]:
        lines.append(f"- Reviewer pair: {analysis['reviewer_pair']}")
        lines.append(f"- Paired records: {analysis['paired_records']}")
        lines.append(f"- Agreements: {analysis['agreements']}")
        lines.append(f"- Disagreements: {analysis['disagreements']}")
        lines.append(
            f"- Disagreement rate: {pct(float(analysis['disagreements']), float(analysis['paired_records']))}"
        )
    else:
        lines.append(f"- Analysis unavailable: {analysis['reason']}")
        lines.append("")
        return "\n".join(lines) + "\n"

    lines.append("")
    lines.append("## Decision Pair Distribution")
    lines.append("")
    lines.append("| Decision pair | Records | Share |")
    lines.append("|---|---:|---:|")
    for _, row in analysis["decision_distribution"].iterrows():
        lines.append(f"| {row['decision_pair']} | {int(row['records'])} | {row['share']} |")

    lines.append("")
    lines.append("## Calibration Table — Disagreement by Article Type")
    lines.append("")
    lines.append(
        "| Article type | Paired records | Disagreements | Disagreement rate | Most frequent conflict |"
    )
    lines.append("|---|---:|---:|---:|---|")
    for _, row in analysis["type_patterns"].iterrows():
        lines.append(
            f"| {row['article_type']} | {int(row['paired_records'])} | {int(row['disagreements'])} | {row['disagreement_rate']} | {row['top_conflict']} |"
        )

    lines.append("")
    lines.append("## Priority Records for Calibration Meeting")
    lines.append("")
    meeting_records = analysis["meeting_records"]
    if meeting_records.empty:
        lines.append("- No disagreement records found for reviewer pair.")
    else:
        lines.append(
            "| Record ID | Article type | Reviewer A | Reviewer B | Source | Year | Journal | Title |"
        )
        lines.append("|---|---|---|---|---|---|---|---|")
        for _, row in meeting_records.iterrows():
            title = str(row["title"]).replace("|", "\\|")
            journal = str(row["journal"]).replace("|", "\\|")
            lines.append(
                f"| {row['record_id']} | {row['article_type']} | {row['decision_a']} | {row['decision_b']} | {row['source_database']} | {row['year']} | {journal} | {title} |"
            )

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Article types are assigned heuristically from title/abstract/journal text.")
    lines.append(
        "- Use this output for calibration discussion, then record rule updates in protocol/screening docs."
    )
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze dual-screening disagreements and generate calibration meeting tables."
    )
    parser.add_argument(
        "--dual-log",
        default="../02_data/processed/screening_title_abstract_dual_log.csv",
        help="Path to title/abstract dual-screening log CSV.",
    )
    parser.add_argument(
        "--master-records",
        default="../02_data/processed/master_records.csv",
        help="Path to master records CSV with title/abstract metadata.",
    )
    parser.add_argument(
        "--output",
        default="outputs/screening_disagreement_report.md",
        help="Path to markdown report output.",
    )
    parser.add_argument(
        "--patterns-output",
        default="outputs/screening_disagreement_patterns.csv",
        help="Path to disagreement pattern table CSV output.",
    )
    parser.add_argument(
        "--records-output",
        default="outputs/screening_disagreement_records.csv",
        help="Path to calibration record list CSV output.",
    )
    parser.add_argument(
        "--top-records",
        type=int,
        default=25,
        help="Maximum disagreement records to include in report/CSV for calibration meeting.",
    )
    args = parser.parse_args()

    dual_log_path = Path(args.dual_log)
    master_records_path = Path(args.master_records)
    output_path = Path(args.output)
    patterns_output_path = Path(args.patterns_output)
    records_output_path = Path(args.records_output)

    dual_df = read_csv_or_empty(dual_log_path)
    master_df = read_csv_or_empty(master_records_path)

    analysis = disagreement_analysis(
        dual_df,
        master_df,
        top_records=max(1, int(args.top_records)),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    patterns_output_path.parent.mkdir(parents=True, exist_ok=True)
    records_output_path.parent.mkdir(parents=True, exist_ok=True)

    analysis["type_patterns"].to_csv(patterns_output_path, index=False)
    analysis["meeting_records"].to_csv(records_output_path, index=False)

    report = build_markdown_report(
        analysis=analysis,
        dual_log_path=dual_log_path,
        master_records_path=master_records_path,
        output_path=output_path,
        patterns_output_path=patterns_output_path,
        records_output_path=records_output_path,
    )
    output_path.write_text(report, encoding="utf-8")

    print(f"Wrote: {output_path}")
    print(f"Wrote: {patterns_output_path}")
    print(f"Wrote: {records_output_path}")
    if analysis["available"]:
        print(
            "Disagreements: "
            f"{analysis['disagreements']}/{analysis['paired_records']} "
            f"({pct(float(analysis['disagreements']), float(analysis['paired_records']))})"
        )
    else:
        print(f"Analysis unavailable: {analysis['reason']}")


if __name__ == "__main__":
    main()

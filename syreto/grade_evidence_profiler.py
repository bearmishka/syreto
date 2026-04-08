import argparse
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

MISSING_VALUES = {
    "",
    "nan",
    "na",
    "n/a",
    "nr",
    "none",
    "not reported",
    "not_reported",
    "unclear",
}

INCLUDE_CODES = {
    "include",
    "included",
    "yes",
    "y",
    "1",
    "final_include",
    "consensus_include",
}

CERTAINTY_BY_POINTS = {
    4: "high",
    3: "moderate",
    2: "low",
    1: "very low",
    0: "very low",
}

PROFILE_COLUMNS = [
    "study_id",
    "study_design",
    "predictor_construct",
    "outcome_construct",
    "sample_size",
    "effect_direction",
    "baseline_points",
    "baseline_certainty",
    "risk_of_bias_downgrade",
    "risk_of_bias",
    "risk_of_bias_notes",
    "inconsistency_downgrade",
    "inconsistency",
    "inconsistency_notes",
    "indirectness_downgrade",
    "indirectness",
    "indirectness_notes",
    "imprecision_downgrade",
    "imprecision",
    "imprecision_notes",
    "total_downgrade",
    "overall_points",
    "overall_certainty",
    "quality_band",
    "quality_score_pct",
]

EXTRACTION_REQUIRED_COLUMNS = [
    "study_id",
    "study_design",
    "predictor_construct",
    "outcome_construct",
    "sample_size",
    "effect_direction",
    "main_effect_value",
    "main_effect_metric",
    "ci_lower",
    "ci_upper",
    "p_value",
    "condition_diagnostic_method",
    "condition_diagnostic_system",
    "condition_definition",
    "predictor_instrument_type",
    "predictor_instrument_name",
    "outcome_measure",
    "consensus_status",
    "quality_appraisal",
]

QUALITY_REQUIRED_COLUMNS = ["study_id", "quality_band", "score_pct"]


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def normalize_lower(value: object) -> str:
    return normalize(value).lower()


def is_missing(value: object) -> bool:
    return normalize_lower(value) in MISSING_VALUES


def read_csv_or_empty(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)

    try:
        dataframe = pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=columns)

    for column in columns:
        if column not in dataframe.columns:
            dataframe[column] = ""

    ordered = columns + [column for column in dataframe.columns if column not in columns]
    return dataframe[ordered]


def parse_float_or_none(value: object) -> float | None:
    text = normalize(value)
    if not text:
        return None

    candidate = text.replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", candidate)
    if not match:
        return None

    try:
        return float(match.group(0))
    except ValueError:
        return None


def parse_int_or_none(value: object) -> int | None:
    numeric = parse_float_or_none(value)
    if numeric is None:
        return None
    return int(round(numeric))


def domain_label(downgrade: int) -> str:
    if downgrade <= 0:
        return "not serious"
    if downgrade == 1:
        return "serious"
    return "very serious"


def baseline_points_from_design(study_design: object) -> int:
    text = normalize_lower(study_design)
    if not text:
        return 2

    randomized_markers = ["randomized", "randomised", "rct", "clinical trial", "controlled trial"]
    if any(marker in text for marker in randomized_markers):
        return 4

    very_low_markers = [
        "case report",
        "case series",
        "qualitative",
        "editorial",
        "commentary",
        "letter",
        "protocol",
        "review",
    ]
    if any(marker in text for marker in very_low_markers):
        return 1

    observational_markers = [
        "cohort",
        "case-control",
        "case control",
        "cross",
        "observational",
        "longitudinal",
        "registry",
    ]
    if any(marker in text for marker in observational_markers):
        return 2

    return 2


def certainty_label_from_points(points: int) -> str:
    clamped = max(0, min(4, int(points)))
    return CERTAINTY_BY_POINTS[clamped]


def normalize_effect_direction(value: object) -> str:
    text = normalize_lower(value)
    if not text:
        return ""
    if text in {"positive", "pos", "+"}:
        return "positive"
    if text in {"negative", "neg", "-"}:
        return "negative"
    if text in {"null", "none", "no_effect", "no effect", "nonsignificant", "non-significant"}:
        return "null"
    if text in {"mixed", "inconsistent", "heterogeneous"}:
        return "mixed"
    return ""


def normalize_group_value(value: object, fallback: str) -> str:
    text = normalize_lower(value)
    return text if text else fallback


def get_included_studies(extraction_df: pd.DataFrame) -> pd.DataFrame:
    dataframe = extraction_df.copy()
    for column in EXTRACTION_REQUIRED_COLUMNS:
        if column not in dataframe.columns:
            dataframe[column] = ""

    if dataframe.empty:
        return dataframe

    dataframe["study_id"] = dataframe["study_id"].fillna("").astype(str).str.strip()
    dataframe = dataframe[dataframe["study_id"].ne("")].copy()
    if dataframe.empty:
        return dataframe

    non_empty_consensus = (
        dataframe["consensus_status"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .loc[lambda values: values.ne("")]
    )

    if not non_empty_consensus.empty:
        if non_empty_consensus.isin(INCLUDE_CODES).any():
            dataframe = dataframe[
                dataframe["consensus_status"]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.lower()
                .isin(INCLUDE_CODES)
            ].copy()

    if dataframe.empty:
        return dataframe

    return dataframe.drop_duplicates(subset=["study_id"], keep="last")


def quality_lookup(scored_df: pd.DataFrame) -> dict[str, dict[str, str]]:
    lookup: dict[str, dict[str, str]] = {}
    if scored_df.empty:
        return lookup

    working = scored_df.copy()
    for column in QUALITY_REQUIRED_COLUMNS:
        if column not in working.columns:
            working[column] = ""

    for _, row in working.iterrows():
        study_id = normalize(row.get("study_id", ""))
        if not study_id:
            continue
        lookup[study_id] = {
            "quality_band": normalize_lower(row.get("quality_band", "")),
            "score_pct": normalize(row.get("score_pct", "")),
        }
    return lookup


def detect_quality_band_from_text(value: object) -> str:
    text = normalize_lower(value)
    if not text:
        return ""
    if "high" in text:
        return "high"
    if "moderate" in text:
        return "moderate"
    if "low" in text:
        return "low"
    score_match = re.search(r"(\d{1,3}(?:\.\d+)?)\s*%", text)
    if score_match:
        try:
            score = float(score_match.group(1))
        except ValueError:
            return ""
        if score >= 75:
            return "high"
        if score >= 50:
            return "moderate"
        return "low"
    return ""


def risk_of_bias_downgrade(
    study_row: pd.Series, quality_info: dict[str, dict[str, str]]
) -> tuple[int, str, str, str]:
    study_id = normalize(study_row.get("study_id", ""))
    from_lookup = quality_info.get(study_id, {})

    band = normalize_lower(from_lookup.get("quality_band", ""))
    score_pct = normalize(from_lookup.get("score_pct", ""))

    if not band:
        band = detect_quality_band_from_text(study_row.get("quality_appraisal", ""))

    if band == "high":
        return 0, domain_label(0), "JBI appraisal indicates high quality.", band
    if band == "moderate":
        return 1, domain_label(1), "JBI appraisal indicates moderate quality.", band
    if band == "low":
        return 2, domain_label(2), "JBI appraisal indicates low quality.", band

    parsed_score = parse_float_or_none(score_pct)
    if parsed_score is not None:
        if parsed_score >= 75:
            return 0, domain_label(0), "JBI score suggests high quality.", band
        if parsed_score >= 50:
            return 1, domain_label(1), "JBI score suggests moderate quality.", band
        return 2, domain_label(2), "JBI score suggests low quality.", band

    return 1, domain_label(1), "No structured quality appraisal score available.", band


def build_direction_group_stats(df: pd.DataFrame) -> dict[str, dict[str, object]]:
    stats: dict[str, dict[str, object]] = {}
    if df.empty:
        return stats

    working = df.copy()
    working["predictor_group"] = working["predictor_construct"].apply(
        lambda value: normalize_group_value(value, fallback="unspecified_predictor")
    )
    working["outcome_group"] = working["outcome_construct"].apply(
        lambda value: normalize_group_value(value, fallback="unspecified_outcome")
    )
    working["group_key"] = working["predictor_group"] + "||" + working["outcome_group"]
    working["effect_direction_norm"] = working["effect_direction"].apply(normalize_effect_direction)

    for group_key, group_df in working.groupby("group_key"):
        valid = group_df[group_df["effect_direction_norm"].ne("")]
        valid_count = int(valid.shape[0])
        if valid_count == 0:
            stats[group_key] = {
                "n_valid": 0,
                "dominant_direction": "",
                "dominant_share": 0.0,
            }
            continue

        counts = valid["effect_direction_norm"].value_counts()
        dominant_direction = str(counts.index[0])
        dominant_share = float(counts.iloc[0] / valid_count)
        stats[group_key] = {
            "n_valid": valid_count,
            "dominant_direction": dominant_direction,
            "dominant_share": dominant_share,
        }

    return stats


def inconsistency_downgrade(
    study_row: pd.Series, group_stats: dict[str, dict[str, object]]
) -> tuple[int, str, str]:
    predictor_group = normalize_group_value(
        study_row.get("predictor_construct", ""), "unspecified_predictor"
    )
    outcome_group = normalize_group_value(
        study_row.get("outcome_construct", ""), "unspecified_outcome"
    )
    group_key = f"{predictor_group}||{outcome_group}"

    direction = normalize_effect_direction(study_row.get("effect_direction", ""))
    stats = group_stats.get(
        group_key, {"n_valid": 0, "dominant_direction": "", "dominant_share": 0.0}
    )

    n_valid = int(stats.get("n_valid", 0))
    dominant_direction = str(stats.get("dominant_direction", ""))
    dominant_share = float(stats.get("dominant_share", 0.0))

    if n_valid <= 1:
        return 0, domain_label(0), "Single-study signal in predictor/outcome group."

    if not direction:
        return 1, domain_label(1), "Effect direction missing for multi-study group."

    if direction == "mixed":
        return 1, domain_label(1), "Study reports mixed direction of effect."

    if dominant_share < 0.6:
        return 1, domain_label(1), "Group-level directional agreement is weak."

    if n_valid >= 3 and direction != dominant_direction:
        return 1, domain_label(1), "Study direction diverges from group-dominant direction."

    return 0, domain_label(0), "Direction aligns with group-level pattern."


def indirectness_downgrade(study_row: pd.Series) -> tuple[int, str, str]:
    downgrade = 0
    reasons: list[str] = []

    design = normalize_lower(study_row.get("study_design", ""))
    if any(marker in design for marker in ["animal", "preclinical", "in vitro"]):
        return 2, domain_label(2), "Preclinical or non-human design relative to target question."

    if any(marker in design for marker in ["case report", "case series"]):
        downgrade += 1
        reasons.append("Case-based design reduces direct applicability.")

    diagnostics_missing = all(
        is_missing(study_row.get(column, ""))
        for column in [
            "condition_diagnostic_method",
            "condition_diagnostic_system",
            "condition_definition",
        ]
    )
    if diagnostics_missing:
        downgrade += 1
        reasons.append("Condition diagnostic framing is not clearly specified.")

    outcome_missing = is_missing(study_row.get("outcome_measure", ""))
    predictor_measure_missing = is_missing(
        study_row.get("predictor_instrument_name", "")
    ) and is_missing(study_row.get("predictor_instrument_type", ""))
    if outcome_missing or predictor_measure_missing:
        downgrade += 1
        reasons.append("Measurement details are incomplete for predictor/outcome.")

    downgrade = min(downgrade, 2)
    if not reasons:
        reasons.append("Population, condition, and measurement mapping is sufficiently direct.")
    return downgrade, domain_label(downgrade), " ".join(reasons)


def imprecision_downgrade(study_row: pd.Series) -> tuple[int, str, str]:
    downgrade = 0
    reasons: list[str] = []

    sample_size = parse_int_or_none(study_row.get("sample_size", ""))
    if sample_size is None:
        downgrade += 1
        reasons.append("Sample size is missing.")
    elif sample_size < 50:
        downgrade += 2
        reasons.append("Very small sample size (<50).")
    elif sample_size < 100:
        downgrade += 1
        reasons.append("Small sample size (<100).")

    ci_lower = parse_float_or_none(study_row.get("ci_lower", ""))
    ci_upper = parse_float_or_none(study_row.get("ci_upper", ""))
    p_value = parse_float_or_none(study_row.get("p_value", ""))
    if ci_lower is None or ci_upper is None:
        if p_value is None:
            downgrade += 1
            reasons.append("No confidence interval or p-value reported.")

    effect_available = not is_missing(study_row.get("main_effect_value", "")) or not is_missing(
        study_row.get("main_effect_metric", "")
    )
    if (
        not effect_available
        and normalize_effect_direction(study_row.get("effect_direction", "")) == ""
    ):
        downgrade += 1
        reasons.append("Effect estimate and direction are both missing.")

    downgrade = min(downgrade, 2)
    if not reasons:
        reasons.append("Precision indicators are acceptable for this study.")
    return downgrade, domain_label(downgrade), " ".join(reasons)


def build_profile(
    extraction_df: pd.DataFrame,
    quality_scored_df: pd.DataFrame,
) -> pd.DataFrame:
    included = get_included_studies(extraction_df)
    if included.empty:
        return pd.DataFrame(columns=PROFILE_COLUMNS)

    quality_info = quality_lookup(quality_scored_df)
    group_stats = build_direction_group_stats(included)

    rows: list[dict[str, object]] = []
    for _, row in included.iterrows():
        study_id = normalize(row.get("study_id", ""))
        baseline_points = baseline_points_from_design(row.get("study_design", ""))
        baseline_certainty = certainty_label_from_points(baseline_points)

        rob_downgrade, rob_label, rob_note, band = risk_of_bias_downgrade(row, quality_info)
        incons_downgrade, incons_label, incons_note = inconsistency_downgrade(row, group_stats)
        indir_downgrade, indir_label, indir_note = indirectness_downgrade(row)
        impre_downgrade, impre_label, impre_note = imprecision_downgrade(row)

        total_downgrade = rob_downgrade + incons_downgrade + indir_downgrade + impre_downgrade
        overall_points = max(0, baseline_points - total_downgrade)
        overall_certainty = certainty_label_from_points(overall_points)

        quality_score = quality_info.get(study_id, {}).get("score_pct", "")

        rows.append(
            {
                "study_id": study_id,
                "study_design": normalize(row.get("study_design", "")),
                "predictor_construct": normalize(row.get("predictor_construct", "")),
                "outcome_construct": normalize(row.get("outcome_construct", "")),
                "sample_size": normalize(row.get("sample_size", "")),
                "effect_direction": normalize_effect_direction(row.get("effect_direction", "")),
                "baseline_points": baseline_points,
                "baseline_certainty": baseline_certainty,
                "risk_of_bias_downgrade": rob_downgrade,
                "risk_of_bias": rob_label,
                "risk_of_bias_notes": rob_note,
                "inconsistency_downgrade": incons_downgrade,
                "inconsistency": incons_label,
                "inconsistency_notes": incons_note,
                "indirectness_downgrade": indir_downgrade,
                "indirectness": indir_label,
                "indirectness_notes": indir_note,
                "imprecision_downgrade": impre_downgrade,
                "imprecision": impre_label,
                "imprecision_notes": impre_note,
                "total_downgrade": total_downgrade,
                "overall_points": overall_points,
                "overall_certainty": overall_certainty,
                "quality_band": band,
                "quality_score_pct": quality_score,
            }
        )

    profile_df = pd.DataFrame(rows)
    certainty_order = {"high": 0, "moderate": 1, "low": 2, "very low": 3}
    profile_df["_sort_certainty"] = profile_df["overall_certainty"].map(certainty_order).fillna(9)
    profile_df = profile_df.sort_values(["_sort_certainty", "study_id"], kind="stable")
    return profile_df.drop(columns=["_sort_certainty"])


def latex_escape(value: object) -> str:
    text = normalize(value)
    text = text.replace("\n", " ").replace("\r", " ")
    text = text.replace("\\", "\\textbackslash{}")
    text = text.replace("&", "\\&")
    text = text.replace("%", "\\%")
    text = text.replace("$", "\\$")
    text = text.replace("#", "\\#")
    text = text.replace("_", "\\_")
    text = text.replace("{", "\\{")
    text = text.replace("}", "\\}")
    text = text.replace("~", "\\textasciitilde{}")
    text = text.replace("^", "\\textasciicircum{}")
    return text


def render_latex_table(profile_df: pd.DataFrame, extraction_path: Path) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = []
    lines.append("% Auto-generated by 03_analysis/grade_evidence_profiler.py")
    lines.append(f"% Source: {extraction_path.as_posix()}")
    lines.append(f"% Generated: {generated_at}")
    lines.append(r"\begin{table}[htbp]")
    lines.append(r"\centering")
    lines.append(r"\caption{GRADE evidence profile by included study (auto-generated)}")
    lines.append(r"\label{tab:grade_evidence_profile}")
    lines.append(r"\setlength{\tabcolsep}{3pt}")
    lines.append(r"\renewcommand{\arraystretch}{1.10}")
    lines.append(r"\scriptsize")
    lines.append(
        r"\begin{tabular}{p{0.11\textwidth}p{0.14\textwidth}p{0.11\textwidth}p{0.11\textwidth}p{0.11\textwidth}p{0.11\textwidth}p{0.15\textwidth}}"
    )
    lines.append(r"\toprule")
    lines.append(
        r"Study & Design & Risk of bias & Inconsistency & Indirectness & Imprecision & Overall certainty \\"
    )
    lines.append(r"\midrule")

    if profile_df.empty:
        lines.append(
            r"\multicolumn{7}{p{0.93\textwidth}}{No included studies with non-empty study\_id are available for GRADE profiling yet.} \\"
        )
    else:
        for _, row in profile_df.iterrows():
            lines.append(
                " & ".join(
                    [
                        latex_escape(row.get("study_id", "")),
                        latex_escape(row.get("study_design", "")),
                        latex_escape(row.get("risk_of_bias", "")),
                        latex_escape(row.get("inconsistency", "")),
                        latex_escape(row.get("indirectness", "")),
                        latex_escape(row.get("imprecision", "")),
                        latex_escape(row.get("overall_certainty", "")),
                    ]
                )
                + r" \\"
            )

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")
    lines.append("")
    return "\n".join(lines)


def build_summary(
    profile_df: pd.DataFrame,
    *,
    extraction_path: Path,
    quality_input_path: Path,
    profile_output_path: Path,
    latex_output_path: Path,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines: list[str] = []
    lines.append("# GRADE Evidence Profile Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    lines.append(f"- Extraction source: `{extraction_path.as_posix()}`")
    lines.append(f"- Quality appraisal source: `{quality_input_path.as_posix()}`")
    lines.append(f"- Profile output: `{profile_output_path.as_posix()}`")
    lines.append(f"- Manuscript table output: `{latex_output_path.as_posix()}`")
    lines.append("")

    if profile_df.empty:
        lines.append("## Overview")
        lines.append("")
        lines.append("- No included studies with non-empty `study_id` available for profiling.")
        lines.append("")
        lines.append("## Notes")
        lines.append("")
        lines.append(
            "- Populate extraction rows and rerun profiler to generate per-study GRADE entries."
        )
        lines.append("")
        return "\n".join(lines)

    certainty_counts = (
        profile_df["overall_certainty"]
        .value_counts()
        .rename_axis("overall_certainty")
        .reset_index(name="studies")
    )
    certainty_counts["share"] = certainty_counts["studies"].apply(
        lambda value: f"{(100.0 * float(value) / float(profile_df.shape[0])):.1f}%"
    )

    lines.append("## Overview")
    lines.append("")
    lines.append(f"- Included studies profiled: {profile_df.shape[0]}")
    lines.append("")
    lines.append("| Overall certainty | Studies | Share |")
    lines.append("|---|---:|---:|")
    for _, row in certainty_counts.iterrows():
        lines.append(f"| {row['overall_certainty']} | {int(row['studies'])} | {row['share']} |")

    lines.append("")
    lines.append("## Downgrade Burden by Domain")
    lines.append("")
    lines.append(
        f"- Risk of bias downgrades (sum): {int(profile_df['risk_of_bias_downgrade'].sum())}"
    )
    lines.append(
        f"- Inconsistency downgrades (sum): {int(profile_df['inconsistency_downgrade'].sum())}"
    )
    lines.append(
        f"- Indirectness downgrades (sum): {int(profile_df['indirectness_downgrade'].sum())}"
    )
    lines.append(
        f"- Imprecision downgrades (sum): {int(profile_df['imprecision_downgrade'].sum())}"
    )

    lines.append("")
    lines.append("## Priority Studies for Calibration")
    lines.append("")
    priority = profile_df[profile_df["overall_certainty"].isin({"very low", "low"})].copy()
    if priority.empty:
        lines.append("- No low/very low certainty studies detected.")
    else:
        lines.append("| Study ID | Overall certainty | Main downgrade drivers |")
        lines.append("|---|---|---|")
        for _, row in priority.head(20).iterrows():
            drivers: list[str] = []
            if int(row["risk_of_bias_downgrade"]) > 0:
                drivers.append("risk of bias")
            if int(row["inconsistency_downgrade"]) > 0:
                drivers.append("inconsistency")
            if int(row["indirectness_downgrade"]) > 0:
                drivers.append("indirectness")
            if int(row["imprecision_downgrade"]) > 0:
                drivers.append("imprecision")
            driver_text = ", ".join(drivers) if drivers else "none"
            lines.append(f"| {row['study_id']} | {row['overall_certainty']} | {driver_text} |")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append(
        "- This profiler provides a transparent, reproducible heuristic approximation of GRADE domains."
    )
    lines.append(
        "- Use results as calibration support, then finalize evidence judgments in manuscript consensus review."
    )
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build per-study GRADE evidence profile (risk of bias, inconsistency, indirectness, imprecision)."
    )
    parser.add_argument(
        "--extraction",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction CSV.",
    )
    parser.add_argument(
        "--quality-scored",
        default="outputs/quality_appraisal_scored.csv",
        help="Path to scored quality appraisal CSV (from quality_appraisal.py).",
    )
    parser.add_argument(
        "--profile-output",
        default="outputs/grade_evidence_profile.csv",
        help="Path to per-study GRADE profile CSV output.",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/grade_evidence_profile_summary.md",
        help="Path to markdown summary output.",
    )
    parser.add_argument(
        "--latex-output",
        default="../04_manuscript/tables/grade_evidence_profile_table.tex",
        help="Path to manuscript-ready LaTeX table output.",
    )
    args = parser.parse_args()

    extraction_path = Path(args.extraction)
    quality_path = Path(args.quality_scored)
    profile_output_path = Path(args.profile_output)
    summary_output_path = Path(args.summary_output)
    latex_output_path = Path(args.latex_output)

    if not extraction_path.exists():
        raise FileNotFoundError(f"Extraction file not found: {extraction_path}")

    extraction_df = read_csv_or_empty(extraction_path, EXTRACTION_REQUIRED_COLUMNS)
    quality_df = read_csv_or_empty(quality_path, QUALITY_REQUIRED_COLUMNS)

    profile_df = build_profile(extraction_df, quality_df)

    profile_output_path.parent.mkdir(parents=True, exist_ok=True)
    profile_df.to_csv(profile_output_path, index=False)

    latex_text = render_latex_table(profile_df, extraction_path)
    latex_output_path.parent.mkdir(parents=True, exist_ok=True)
    latex_output_path.write_text(latex_text, encoding="utf-8")

    summary_text = build_summary(
        profile_df,
        extraction_path=extraction_path,
        quality_input_path=quality_path,
        profile_output_path=profile_output_path,
        latex_output_path=latex_output_path,
    )
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(summary_text, encoding="utf-8")

    print(f"Wrote: {profile_output_path}")
    print(f"Wrote: {latex_output_path}")
    print(f"Wrote: {summary_output_path}")
    print(f"Profiled studies: {profile_df.shape[0]}")


if __name__ == "__main__":
    main()

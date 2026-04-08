import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

ITEM_COLUMNS = [f"item_{index:02d}" for index in range(1, 12)]
JBI_COLUMNS = [
    "study_id",
    "study_design",
    "jbi_tool",
    *ITEM_COLUMNS,
    "appraiser_id",
    "checked_by",
    "appraisal_notes",
]

MISSING_VALUES = {"", "nan", "na", "n/a", "none", "not_reported", "not reported", "missing"}

TOOL_BY_DESIGN = {
    "cross_sectional": ("jbi_analytical_cross_sectional", 8),
    "case_control": ("jbi_case_control", 10),
    "cohort": ("jbi_cohort", 11),
}


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def normalize_lower(value: object) -> str:
    return normalize(value).lower()


def is_missing(value: object) -> bool:
    return normalize_lower(value) in MISSING_VALUES


def parse_int_or_none(value: object) -> int | None:
    text = normalize(value)
    if is_missing(text):
        return None

    parsed = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    return int(round(float(parsed)))


def detect_design_key(study_design: object) -> str:
    text = normalize_lower(study_design)
    if "case" in text and "control" in text:
        return "case_control"
    if (
        "cohort" in text
        or "longitudinal" in text
        or "prospective" in text
        or "retrospective" in text
    ):
        return "cohort"
    return "cross_sectional"


def derive_overall_risk(row: pd.Series) -> str:
    overall = normalize_lower(row.get("overall_risk", ""))
    if overall and overall not in {"na", "n/a"}:
        return overall

    stars = parse_int_or_none(row.get("nos_total_stars", ""))
    if stars is None:
        return "moderate"
    if stars >= 7:
        return "low"
    if stars >= 5:
        return "moderate"
    return "serious"


def risk_to_response(risk: object) -> str:
    text = normalize_lower(risk)
    if text in {"low"}:
        return "yes"
    if text in {"moderate", "some_concerns", "some concerns", "unclear", "unknown"}:
        return "unclear"
    if text in {"serious", "high", "critical", "very_high", "very high"}:
        return "no"
    if text in {"na", "n/a", "not_applicable", "not applicable"}:
        return "na"
    if text == "":
        return "unclear"
    return "unclear"


def row_to_jbi(row: pd.Series) -> dict[str, str]:
    study_id = normalize(row.get("study_id", ""))
    study_design = normalize(row.get("study_design", ""))

    design_key = detect_design_key(study_design)
    tool_label, expected_items = TOOL_BY_DESIGN[design_key]

    selection_response = risk_to_response(row.get("selection_bias", ""))
    performance_response = risk_to_response(row.get("performance_bias", ""))
    detection_response = risk_to_response(row.get("detection_bias", ""))
    attrition_response = risk_to_response(row.get("attrition_bias", ""))
    reporting_response = risk_to_response(row.get("reporting_bias", ""))
    overall_response = risk_to_response(derive_overall_risk(row))

    mapped_core = [
        selection_response,
        selection_response,
        selection_response,
        performance_response,
        detection_response,
        attrition_response,
        reporting_response,
        overall_response,
    ]

    item_values = ["na"] * 11
    for index in range(expected_items):
        fallback = overall_response if index >= len(mapped_core) else mapped_core[index]
        item_values[index] = fallback

    notes = normalize(row.get("appraisal_notes", ""))
    notes_suffix = "Converted from NOS template to JBI-compatible checklist."
    merged_notes = notes_suffix if not notes else f"{notes} {notes_suffix}"

    converted: dict[str, str] = {
        "study_id": study_id,
        "study_design": design_key,
        "jbi_tool": tool_label,
        "appraiser_id": normalize(row.get("appraiser_id", "")),
        "checked_by": normalize(row.get("checked_by", "")),
        "appraisal_notes": merged_notes,
    }
    for column, value in zip(ITEM_COLUMNS, item_values):
        converted[column] = value

    return converted


def build_summary(
    *,
    nos_input_path: Path,
    jbi_output_path: Path,
    summary_output_path: Path,
    source_df: pd.DataFrame,
    converted_df: pd.DataFrame,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines: list[str] = []

    lines.append("# NOS to JBI Conversion Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- NOS input: `{nos_input_path.as_posix()}`")
    lines.append(f"- JBI output: `{jbi_output_path.as_posix()}`")
    lines.append(f"- Summary output: `{summary_output_path.as_posix()}`")
    lines.append("")
    lines.append("## Counts")
    lines.append("")
    lines.append(f"- Source rows: {int(source_df.shape[0])}")
    lines.append(f"- Converted rows: {int(converted_df.shape[0])}")

    if not converted_df.empty:
        design_counts = (
            converted_df["study_design"].fillna("").astype(str).str.strip().value_counts()
        )
        lines.append("")
        lines.append("## Converted study designs")
        lines.append("")
        for design, count in design_counts.items():
            lines.append(f"- `{design or 'unknown'}`: {int(count)}")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append(
        "- Conversion is heuristic and preserves reviewer-driven risk judgments from NOS domains."
    )
    lines.append(
        "- `quality_appraisal.py` scoring remains based on JBI checklist fields in the converted output."
    )
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert NOS-oriented quality appraisal CSV into JBI-compatible template CSV."
    )
    parser.add_argument(
        "--nos-input",
        default="../02_data/codebook/quality_appraisal_template_nos.csv",
        help="Path to NOS-oriented appraisal CSV.",
    )
    parser.add_argument(
        "--jbi-output",
        default="../02_data/codebook/quality_appraisal_template.csv",
        help="Path to JBI-compatible appraisal CSV output.",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/nos_to_jbi_conversion_summary.md",
        help="Path to conversion summary markdown output.",
    )
    args = parser.parse_args()

    nos_input_path = Path(args.nos_input)
    jbi_output_path = Path(args.jbi_output)
    summary_output_path = Path(args.summary_output)

    if not nos_input_path.exists():
        raise FileNotFoundError(f"NOS input file not found: {nos_input_path}")

    source_df = pd.read_csv(nos_input_path, dtype=str).fillna("")
    if source_df.empty or "study_id" not in source_df.columns:
        converted_df = pd.DataFrame(columns=JBI_COLUMNS)
    else:
        deduped = source_df.drop_duplicates(subset=["study_id"], keep="last").copy()
        converted_rows = [
            row_to_jbi(row) for _, row in deduped.iterrows() if normalize(row.get("study_id", ""))
        ]
        converted_df = pd.DataFrame(converted_rows, columns=JBI_COLUMNS)

    jbi_output_path.parent.mkdir(parents=True, exist_ok=True)
    converted_df.to_csv(jbi_output_path, index=False)

    summary_text = build_summary(
        nos_input_path=nos_input_path,
        jbi_output_path=jbi_output_path,
        summary_output_path=summary_output_path,
        source_df=source_df,
        converted_df=converted_df,
    )
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(summary_text, encoding="utf-8")

    print(f"Wrote: {jbi_output_path}")
    print(f"Wrote: {summary_output_path}")
    print(f"Converted rows: {converted_df.shape[0]}")


if __name__ == "__main__":
    main()

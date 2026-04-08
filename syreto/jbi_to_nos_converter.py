import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd


ITEM_COLUMNS = [f"item_{index:02d}" for index in range(1, 12)]
NOS_COLUMNS = [
    "study_id",
    "study_design",
    "appraisal_framework",
    "selection_bias",
    "performance_bias",
    "detection_bias",
    "attrition_bias",
    "reporting_bias",
    "overall_risk",
    "nos_selection_stars",
    "nos_comparability_stars",
    "nos_outcome_exposure_stars",
    "nos_total_stars",
    "appraiser_id",
    "checked_by",
    "appraisal_notes",
]

MISSING_VALUES = {"", "nan", "na", "n/a", "none", "not_reported", "not reported", "missing"}


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def normalize_lower(value: object) -> str:
    return normalize(value).lower()


def is_missing(value: object) -> bool:
    return normalize_lower(value) in MISSING_VALUES


def detect_design_key(study_design: object, jbi_tool: object) -> str:
    design_text = normalize_lower(study_design)
    tool_text = normalize_lower(jbi_tool)

    if "case" in design_text and "control" in design_text:
        return "case_control"
    if (
        "cohort" in design_text
        or "longitudinal" in design_text
        or "prospective" in design_text
        or "retrospective" in design_text
    ):
        return "cohort"

    if "case" in tool_text and "control" in tool_text:
        return "case_control"
    if "cohort" in tool_text:
        return "cohort"

    return "cross_sectional"


def response_to_risk(response: object) -> str:
    text = normalize_lower(response)
    if text == "yes":
        return "low"
    if text in {"unclear", "na", "n/a", ""}:
        return "moderate"
    if text == "no":
        return "serious"
    return "moderate"


def worst_risk(*risks: str) -> str:
    priority = {"low": 0, "moderate": 1, "serious": 2}
    selected = "low"
    for risk in risks:
        if priority.get(risk, 1) >= priority[selected]:
            selected = risk
    return selected


def star_count(responses: list[str]) -> int:
    return int(sum(1 for value in responses if normalize_lower(value) == "yes"))


def row_to_nos(row: pd.Series) -> dict[str, str]:
    study_id = normalize(row.get("study_id", ""))
    study_design = normalize(row.get("study_design", ""))
    jbi_tool = normalize(row.get("jbi_tool", ""))

    design_key = detect_design_key(study_design, jbi_tool)

    item_values = {column: normalize_lower(row.get(column, "")) for column in ITEM_COLUMNS}

    selection_values = [item_values["item_01"], item_values["item_02"], item_values["item_03"]]
    comparability_values = [item_values["item_04"], item_values["item_05"]]
    outcome_values = [item_values["item_06"], item_values["item_07"], item_values["item_08"]]

    selection_bias = worst_risk(*[response_to_risk(value) for value in selection_values])
    performance_bias = response_to_risk(item_values["item_04"])
    detection_bias = response_to_risk(item_values["item_05"])
    attrition_bias = response_to_risk(item_values["item_06"])
    reporting_bias = response_to_risk(item_values["item_07"])

    item_08_risk = response_to_risk(item_values["item_08"])
    overall_risk = item_08_risk
    if is_missing(item_values["item_08"]):
        overall_risk = worst_risk(
            selection_bias, performance_bias, detection_bias, attrition_bias, reporting_bias
        )

    selection_stars = star_count(selection_values)
    comparability_stars = star_count(comparability_values)
    outcome_stars = star_count(outcome_values)
    total_stars = selection_stars + comparability_stars + outcome_stars

    notes = normalize(row.get("appraisal_notes", ""))
    notes_suffix = "Converted from JBI template to NOS-oriented worksheet."
    merged_notes = notes_suffix if not notes else f"{notes} {notes_suffix}"

    return {
        "study_id": study_id,
        "study_design": design_key,
        "appraisal_framework": "newcastle_ottawa_scale_adapted",
        "selection_bias": selection_bias,
        "performance_bias": performance_bias,
        "detection_bias": detection_bias,
        "attrition_bias": attrition_bias,
        "reporting_bias": reporting_bias,
        "overall_risk": overall_risk,
        "nos_selection_stars": str(selection_stars),
        "nos_comparability_stars": str(comparability_stars),
        "nos_outcome_exposure_stars": str(outcome_stars),
        "nos_total_stars": str(total_stars),
        "appraiser_id": normalize(row.get("appraiser_id", "")),
        "checked_by": normalize(row.get("checked_by", "")),
        "appraisal_notes": merged_notes,
    }


def build_summary(
    *,
    jbi_input_path: Path,
    nos_output_path: Path,
    summary_output_path: Path,
    source_df: pd.DataFrame,
    converted_df: pd.DataFrame,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines: list[str] = []

    lines.append("# JBI to NOS Conversion Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- JBI input: `{jbi_input_path.as_posix()}`")
    lines.append(f"- NOS output: `{nos_output_path.as_posix()}`")
    lines.append(f"- Summary output: `{summary_output_path.as_posix()}`")
    lines.append("")
    lines.append("## Counts")
    lines.append("")
    lines.append(f"- Source rows: {int(source_df.shape[0])}")
    lines.append(f"- Converted rows: {int(converted_df.shape[0])}")

    if not converted_df.empty:
        risk_counts = converted_df["overall_risk"].fillna("").astype(str).str.strip().value_counts()
        lines.append("")
        lines.append("## Overall risk distribution")
        lines.append("")
        for risk, count in risk_counts.items():
            lines.append(f"- `{risk or 'unknown'}`: {int(count)}")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append(
        "- Conversion is heuristic and recodes JBI checklist responses into NOS-oriented bias domains."
    )
    lines.append("- This output is intended for narrative appraisal reporting and traceability.")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert JBI-compatible quality appraisal CSV into NOS-oriented worksheet CSV."
    )
    parser.add_argument(
        "--jbi-input",
        default="../02_data/codebook/quality_appraisal_template.csv",
        help="Path to JBI-compatible appraisal CSV.",
    )
    parser.add_argument(
        "--nos-output",
        default="../02_data/codebook/quality_appraisal_template_nos.csv",
        help="Path to NOS-oriented appraisal CSV output.",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/jbi_to_nos_conversion_summary.md",
        help="Path to conversion summary markdown output.",
    )
    args = parser.parse_args()

    jbi_input_path = Path(args.jbi_input)
    nos_output_path = Path(args.nos_output)
    summary_output_path = Path(args.summary_output)

    if not jbi_input_path.exists():
        raise FileNotFoundError(f"JBI input file not found: {jbi_input_path}")

    source_df = pd.read_csv(jbi_input_path, dtype=str).fillna("")
    if source_df.empty or "study_id" not in source_df.columns:
        converted_df = pd.DataFrame(columns=NOS_COLUMNS)
    else:
        deduped = source_df.drop_duplicates(subset=["study_id"], keep="last").copy()
        converted_rows = [
            row_to_nos(row) for _, row in deduped.iterrows() if normalize(row.get("study_id", ""))
        ]
        converted_df = pd.DataFrame(converted_rows, columns=NOS_COLUMNS)

    nos_output_path.parent.mkdir(parents=True, exist_ok=True)
    converted_df.to_csv(nos_output_path, index=False)

    summary_text = build_summary(
        jbi_input_path=jbi_input_path,
        nos_output_path=nos_output_path,
        summary_output_path=summary_output_path,
        source_df=source_df,
        converted_df=converted_df,
    )
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(summary_text, encoding="utf-8")

    print(f"Wrote: {nos_output_path}")
    print(f"Wrote: {summary_output_path}")
    print(f"Converted rows: {converted_df.shape[0]}")


if __name__ == "__main__":
    main()

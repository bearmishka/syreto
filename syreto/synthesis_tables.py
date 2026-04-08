import argparse
from datetime import datetime
from pathlib import Path

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
MISSING_PLACEHOLDER = "---"

TABLE_COLUMNS = [
    "Study",
    "Design/setting",
    "Sample",
    "Condition criteria",
    "Predictor/exposure",
    "Outcome",
]

EXTRACTION_COLUMNS = [
    "study_id",
    "first_author",
    "year",
    "country",
    "study_design",
    "setting",
    "framework",
    "sample_size",
    "age_mean",
    "age_range",
    "sex_distribution",
    "condition_diagnostic_method",
    "condition_diagnostic_system",
    "diagnostic_frame_detail",
    "condition_definition",
    "predictor_construct",
    "predictor_instrument_type",
    "predictor_instrument_name",
    "predictor_subscale",
    "predictor_respondent_type",
    "outcome_construct",
    "outcome_measure",
]


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def is_missing(value: object) -> bool:
    return normalize(value).lower() in MISSING_CODES


def latex_escape(value: str) -> str:
    text = value.replace("\n", " ").replace("\r", " ")

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


def join_non_missing(parts: list[str], sep: str = "; ", fallback: str = MISSING_PLACEHOLDER) -> str:
    clean_parts = [normalize(part) for part in parts if not is_missing(part)]
    return sep.join(clean_parts) if clean_parts else fallback


def non_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    mask = df.apply(lambda row: any(not is_missing(value) for value in row), axis=1)
    return df[mask].copy()


def read_extraction(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Extraction file not found: {path}")

    try:
        extraction_df = pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        extraction_df = pd.DataFrame(columns=EXTRACTION_COLUMNS)

    legacy_to_generic = {
        "theoretical_orientation": "framework",
        "bn_diagnostic_method": "condition_diagnostic_method",
        "bn_dsm_icd_version": "condition_diagnostic_system",
        "bn_definition": "condition_definition",
        "object_relation_construct": "predictor_construct",
        "object_relation_instrument_type": "predictor_instrument_type",
        "object_relation_instrument_name": "predictor_instrument_name",
        "object_relation_subscale": "predictor_subscale",
        "object_relation_respondent_type": "predictor_respondent_type",
        "identity_construct": "outcome_construct",
        "identity_measure": "outcome_measure",
    }
    for legacy, generic in legacy_to_generic.items():
        if generic not in extraction_df.columns and legacy in extraction_df.columns:
            extraction_df[generic] = extraction_df[legacy]

    for column in EXTRACTION_COLUMNS:
        if column not in extraction_df.columns:
            extraction_df[column] = ""

    return extraction_df


def sort_extraction_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    sorted_df = df.copy()
    sorted_df["_sort_year"] = pd.to_numeric(sorted_df["year"], errors="coerce")
    sorted_df["_sort_year"] = sorted_df["_sort_year"].fillna(9999)
    sorted_df["_sort_author"] = sorted_df["first_author"].fillna("").astype(str).str.lower()
    sorted_df["_sort_study_id"] = sorted_df["study_id"].fillna("").astype(str).str.lower()

    sorted_df = sorted_df.sort_values(
        by=["_sort_year", "_sort_author", "_sort_study_id"], kind="stable"
    )
    return sorted_df.drop(columns=["_sort_year", "_sort_author", "_sort_study_id"])


def format_study_cell(row: pd.Series) -> str:
    first_author = normalize(row.get("first_author", ""))
    year = normalize(row.get("year", ""))
    study_id = normalize(row.get("study_id", ""))
    country = normalize(row.get("country", ""))

    if not is_missing(first_author) and not is_missing(year):
        author_year = f"{first_author} ({year})"
    elif not is_missing(first_author):
        author_year = first_author
    elif not is_missing(year):
        author_year = year
    else:
        author_year = study_id

    return join_non_missing([author_year, country])


def format_design_setting_cell(row: pd.Series) -> str:
    orientation = normalize(row.get("framework", ""))
    orientation_text = "" if is_missing(orientation) else f"framework: {orientation}"
    return join_non_missing(
        [
            normalize(row.get("study_design", "")),
            normalize(row.get("setting", "")),
            orientation_text,
        ]
    )


def format_sample_cell(row: pd.Series) -> str:
    parts: list[str] = []
    sample_size = normalize(row.get("sample_size", ""))
    age_mean = normalize(row.get("age_mean", ""))
    age_range = normalize(row.get("age_range", ""))
    sex_distribution = normalize(row.get("sex_distribution", ""))

    if not is_missing(sample_size):
        parts.append(f"n={sample_size}")
    if not is_missing(age_mean):
        parts.append(f"mean age={age_mean}")
    if not is_missing(age_range):
        parts.append(f"age range={age_range}")
    if not is_missing(sex_distribution):
        parts.append(f"sex={sex_distribution}")

    return join_non_missing(parts)


def format_condition_diagnosis_cell(row: pd.Series) -> str:
    diagnostic_frame = normalize(row.get("diagnostic_frame_detail", ""))
    dsm_icd_version = normalize(row.get("condition_diagnostic_system", ""))
    diagnostic_method = normalize(row.get("condition_diagnostic_method", ""))
    definition = normalize(row.get("condition_definition", ""))

    parts: list[str] = []
    if not is_missing(diagnostic_frame):
        parts.append(diagnostic_frame)
    else:
        if not is_missing(dsm_icd_version) and not is_missing(diagnostic_method):
            parts.append(f"{dsm_icd_version} + {diagnostic_method}")
        else:
            if not is_missing(dsm_icd_version):
                parts.append(dsm_icd_version)
            if not is_missing(diagnostic_method):
                parts.append(diagnostic_method)

    if not is_missing(definition):
        parts.append(f"definition: {definition}")

    return join_non_missing(parts)


def format_predictor_cell(row: pd.Series) -> str:
    instrument_type = normalize(row.get("predictor_instrument_type", ""))
    instrument_type_text = "" if is_missing(instrument_type) else f"type: {instrument_type}"
    return join_non_missing(
        [
            normalize(row.get("predictor_construct", "")),
            instrument_type_text,
            normalize(row.get("predictor_instrument_name", "")),
            normalize(row.get("predictor_subscale", "")),
            normalize(row.get("predictor_respondent_type", "")),
        ]
    )


def format_outcome_cell(row: pd.Series) -> str:
    return join_non_missing(
        [
            normalize(row.get("outcome_construct", "")),
            normalize(row.get("outcome_measure", "")),
        ]
    )


def build_table_rows(df: pd.DataFrame) -> list[list[str]]:
    rows: list[list[str]] = []
    for _, row in df.iterrows():
        rows.append(
            [
                format_study_cell(row),
                format_design_setting_cell(row),
                format_sample_cell(row),
                format_condition_diagnosis_cell(row),
                format_predictor_cell(row),
                format_outcome_cell(row),
            ]
        )
    return rows


def render_latex_table(table_rows: list[list[str]], input_path: Path) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = []
    lines.append("% Auto-generated by 03_analysis/synthesis_tables.py")
    lines.append(f"% Source: {input_path.as_posix()}")
    lines.append(f"% Generated: {generated_at}")
    lines.append(r"\begin{table}[htbp]")
    lines.append(r"\centering")
    lines.append(r"\caption{Characteristics of included studies (auto-generated)}")
    lines.append(r"\label{tab:study_characteristics}")
    lines.append(r"\setlength{\tabcolsep}{3pt}")
    lines.append(r"\renewcommand{\arraystretch}{1.12}")
    lines.append(r"\scriptsize")
    lines.append(
        r"\begin{tabular}{p{0.13\textwidth}p{0.14\textwidth}p{0.12\textwidth}p{0.15\textwidth}p{0.17\textwidth}p{0.17\textwidth}}"
    )
    lines.append(r"\toprule")
    lines.append(" & ".join(TABLE_COLUMNS) + r" \\")
    lines.append(r"\midrule")

    if table_rows:
        for row in table_rows:
            escaped_row = [latex_escape(value) for value in row]
            lines.append(" & ".join(escaped_row) + r" \\")
    else:
        lines.append(
            r"\multicolumn{6}{p{0.95\textwidth}}{No included studies with non-empty study\_id are coded in extraction\_template.csv yet.} \\"
        )

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")
    lines.append("")
    return "\n".join(lines)


def render_summary(
    *,
    input_path: Path,
    output_path: Path,
    total_rows: int,
    non_empty_rows_count: int,
    skipped_missing_study_id: int,
    exported_rows: int,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines: list[str] = []
    lines.append("# Synthesis Tables Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Extraction input: `{input_path.as_posix()}`")
    lines.append(f"- LaTeX output: `{output_path.as_posix()}`")
    lines.append("")
    lines.append("## Row Counts")
    lines.append("")
    lines.append(f"- CSV rows (raw): {total_rows}")
    lines.append(f"- Non-empty rows: {non_empty_rows_count}")
    lines.append(f"- Skipped rows (missing `study_id`): {skipped_missing_study_id}")
    lines.append(f"- Exported table rows: {exported_rows}")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append(
        "- Table is generated from extraction coding fields and formatted for direct `\\input{}` in manuscript."
    )
    lines.append("- Missing/empty values are rendered as `---` in table cells.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Auto-generate LaTeX study-characteristics table from extraction_template.csv"
    )
    parser.add_argument(
        "--input",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction template CSV",
    )
    parser.add_argument(
        "--output",
        default="../04_manuscript/tables/study_characteristics_table.tex",
        help="Path to generated LaTeX table",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/synthesis_tables_summary.md",
        help="Path to generation summary markdown",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    summary_output_path = Path(args.summary_output)

    extraction_df = read_extraction(input_path)
    total_rows = int(extraction_df.shape[0])

    non_empty_df = non_empty_rows(extraction_df)
    non_empty_rows_count = int(non_empty_df.shape[0])

    missing_study_id_mask = non_empty_df["study_id"].apply(is_missing)
    skipped_missing_study_id = int(missing_study_id_mask.sum())

    included_df = non_empty_df[~missing_study_id_mask].copy()
    included_df = sort_extraction_rows(included_df)

    table_rows = build_table_rows(included_df)
    latex_table = render_latex_table(table_rows, input_path=input_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(latex_table, encoding="utf-8")

    summary_text = render_summary(
        input_path=input_path,
        output_path=output_path,
        total_rows=total_rows,
        non_empty_rows_count=non_empty_rows_count,
        skipped_missing_study_id=skipped_missing_study_id,
        exported_rows=len(table_rows),
    )
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(summary_text, encoding="utf-8")

    print(f"Wrote: {output_path}")
    print(f"Wrote: {summary_output_path}")
    print(
        "Row stats: "
        f"raw={total_rows}, non_empty={non_empty_rows_count}, "
        f"skipped_missing_study_id={skipped_missing_study_id}, exported={len(table_rows)}"
    )


if __name__ == "__main__":
    main()

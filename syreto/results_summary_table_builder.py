import argparse
from datetime import datetime
from pathlib import Path
import os
import tempfile

import pandas as pd


RESULT_COLUMNS = [
    "outcome",
    "studies",
    "participants",
    "effect",
    "ci",
    "certainty_grade",
]

LATEX_TABLE_CAPTION = "Final results summary across outcomes (auto-generated)"
LATEX_TABLE_LABEL = "tab:final_results_summary"

CERTAINTY_ORDER = {
    "very low": 0,
    "low": 1,
    "moderate": 2,
    "high": 3,
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


def atomic_write_dataframe_csv(frame: pd.DataFrame, path: Path, *, index: bool = False) -> None:
    atomic_write_text(path, frame.to_csv(index=index))


def read_csv_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


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


def parse_int(value: object) -> int | None:
    text = normalize(value)
    if not text:
        return None

    numeric = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None

    parsed = int(float(numeric))
    return parsed if parsed >= 0 else None


def parse_float(value: object) -> float | None:
    text = normalize(value)
    if not text:
        return None

    numeric = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return float(numeric)


def extract_outcome_metadata(extraction_df: pd.DataFrame) -> tuple[dict[str, int], dict[str, int]]:
    participants_by_outcome: dict[str, int] = {}
    studies_by_outcome: dict[str, set[str]] = {}

    if extraction_df.empty:
        return {}, {}

    if "outcome_construct" not in extraction_df.columns:
        extraction_df["outcome_construct"] = ""
    if "study_id" not in extraction_df.columns:
        extraction_df["study_id"] = ""
    if "sample_size" not in extraction_df.columns:
        extraction_df["sample_size"] = ""

    per_study_sample: dict[tuple[str, str], int] = {}
    for _, row in extraction_df.iterrows():
        outcome = normalize(row.get("outcome_construct", ""))
        study_id = normalize(row.get("study_id", ""))
        if not outcome or not study_id:
            continue

        sample_size = parse_int(row.get("sample_size", ""))
        if sample_size is None:
            sample_size = 0

        key = (outcome, study_id)
        if key not in per_study_sample:
            per_study_sample[key] = sample_size
        else:
            per_study_sample[key] = max(per_study_sample[key], sample_size)

    for (outcome, study_id), sample_size in per_study_sample.items():
        if outcome not in studies_by_outcome:
            studies_by_outcome[outcome] = set()
        studies_by_outcome[outcome].add(study_id)

        participants_by_outcome[outcome] = participants_by_outcome.get(outcome, 0) + sample_size

    study_count_by_outcome = {
        outcome: len(study_ids) for outcome, study_ids in studies_by_outcome.items()
    }
    return participants_by_outcome, study_count_by_outcome


def aggregate_certainty_by_outcome(grade_df: pd.DataFrame) -> dict[str, str]:
    if grade_df.empty:
        return {}

    if "outcome_construct" not in grade_df.columns:
        grade_df["outcome_construct"] = ""
    if "overall_certainty" not in grade_df.columns:
        grade_df["overall_certainty"] = ""

    worst_by_outcome: dict[str, str] = {}
    for _, row in grade_df.iterrows():
        outcome = normalize(row.get("outcome_construct", ""))
        certainty = normalize(row.get("overall_certainty", "")).lower()
        if not outcome or certainty not in CERTAINTY_ORDER:
            continue

        if outcome not in worst_by_outcome:
            worst_by_outcome[outcome] = certainty
            continue

        current = worst_by_outcome[outcome]
        if CERTAINTY_ORDER[certainty] < CERTAINTY_ORDER[current]:
            worst_by_outcome[outcome] = certainty

    return worst_by_outcome


def format_effect(effect_value: float | None, *, effect_label: str, decimals: int) -> str:
    if effect_value is None:
        return "NR"
    return f"{effect_label}={effect_value:.{decimals}f}"


def format_ci(ci_low: float | None, ci_high: float | None, *, decimals: int) -> str:
    if ci_low is None or ci_high is None:
        return "NR"
    return f"[{ci_low:.{decimals}f}, {ci_high:.{decimals}f}]"


def build_results_table(
    *,
    meta_df: pd.DataFrame,
    participants_by_outcome: dict[str, int],
    study_count_by_outcome: dict[str, int],
    certainty_by_outcome: dict[str, str],
    effect_label: str,
    decimals: int,
) -> tuple[pd.DataFrame, list[str]]:
    warnings: list[str] = []
    rows: list[dict[str, str]] = []
    seen_outcomes: set[str] = set()

    if not meta_df.empty:
        for required_col in ["outcome", "k_studies", "pooled_effect", "ci_low", "ci_high"]:
            if required_col not in meta_df.columns:
                meta_df[required_col] = ""

        for _, row in meta_df.iterrows():
            outcome = normalize(row.get("outcome", ""))
            if not outcome:
                continue
            if outcome in seen_outcomes:
                warnings.append(f"Duplicate outcome in meta results ignored: {outcome}")
                continue

            seen_outcomes.add(outcome)

            meta_studies = parse_int(row.get("k_studies", ""))
            extraction_studies = study_count_by_outcome.get(outcome)
            studies_value = meta_studies if meta_studies is not None else extraction_studies

            participants_value = participants_by_outcome.get(outcome)
            effect_value = parse_float(row.get("pooled_effect", ""))
            ci_low = parse_float(row.get("ci_low", ""))
            ci_high = parse_float(row.get("ci_high", ""))

            rows.append(
                {
                    "outcome": outcome,
                    "studies": str(studies_value) if studies_value is not None else "NR",
                    "participants": str(participants_value)
                    if participants_value not in (None, 0)
                    else "NR",
                    "effect": format_effect(
                        effect_value, effect_label=effect_label, decimals=decimals
                    ),
                    "ci": format_ci(ci_low, ci_high, decimals=decimals),
                    "certainty_grade": certainty_by_outcome.get(outcome, "NR"),
                }
            )

    for outcome in sorted(study_count_by_outcome.keys()):
        if outcome in seen_outcomes:
            continue

        rows.append(
            {
                "outcome": outcome,
                "studies": str(study_count_by_outcome.get(outcome, 0)),
                "participants": str(participants_by_outcome.get(outcome, 0) or "NR"),
                "effect": "NR",
                "ci": "NR",
                "certainty_grade": certainty_by_outcome.get(outcome, "NR"),
            }
        )

    if not rows:
        return pd.DataFrame(columns=RESULT_COLUMNS), warnings

    results_df = pd.DataFrame(rows)
    results_df = results_df[RESULT_COLUMNS]
    return results_df, warnings


def build_summary(
    *,
    meta_input: Path,
    extraction_input: Path,
    grade_input: Path,
    output_path: Path,
    latex_output_path: Path,
    summary_output: Path,
    results_df: pd.DataFrame,
    warnings: list[str],
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = []
    lines.append("# Final Results Summary Table")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Meta-analysis input: `{meta_input.as_posix()}`")
    lines.append(f"- Extraction input: `{extraction_input.as_posix()}`")
    lines.append(f"- GRADE input: `{grade_input.as_posix()}`")
    lines.append(f"- Results output: `{output_path.as_posix()}`")
    lines.append(f"- Manuscript table output: `{latex_output_path.as_posix()}`")
    lines.append(f"- Summary output: `{summary_output.as_posix()}`")
    lines.append("")
    lines.append("## Counts")
    lines.append("")
    lines.append(f"- Outcome rows exported: {int(results_df.shape[0])}")
    lines.append("")
    lines.append("## Warnings")
    lines.append("")
    if warnings:
        for warning in warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- ✅ No warnings.")
    lines.append("")
    return "\n".join(lines)


def render_latex_table(results_df: pd.DataFrame, *, source_csv_path: Path) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = []
    lines.append("% Auto-generated by 03_analysis/results_summary_table_builder.py")
    lines.append(f"% Source: {source_csv_path.as_posix()}")
    lines.append(f"% Generated: {generated_at}")
    lines.append(r"\begin{table}[htbp]")
    lines.append(r"\centering")
    lines.append(rf"\caption{{{LATEX_TABLE_CAPTION}}}")
    lines.append(rf"\label{{{LATEX_TABLE_LABEL}}}")
    lines.append(r"\setlength{\tabcolsep}{4pt}")
    lines.append(r"\renewcommand{\arraystretch}{1.10}")
    lines.append(r"\scriptsize")
    lines.append(
        r"\begin{tabular}{p{0.23\textwidth}p{0.07\textwidth}p{0.10\textwidth}p{0.12\textwidth}p{0.19\textwidth}p{0.16\textwidth}}"
    )
    lines.append(r"\toprule")
    lines.append(r"Outcome & Studies & Participants & Effect & 95\% CI & Certainty grade \\")
    lines.append(r"\midrule")

    if results_df.empty:
        lines.append(
            r"\multicolumn{6}{p{0.87\textwidth}}{No outcome rows are available yet. Run meta-analysis/evidence profiling steps and regenerate this table.} \\"
        )
    else:
        for _, row in results_df.iterrows():
            lines.append(
                " & ".join(
                    [
                        latex_escape(row.get("outcome", "")),
                        latex_escape(row.get("studies", "")),
                        latex_escape(row.get("participants", "")),
                        latex_escape(row.get("effect", "")),
                        latex_escape(row.get("ci", "")),
                        latex_escape(row.get("certainty_grade", "")),
                    ]
                )
                + r" \\"
            )

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build compact manuscript-facing final results summary table (outcome, studies, participants, effect, CI, certainty)."
    )
    parser.add_argument(
        "--meta-input",
        default="outputs/meta_analysis_results.csv",
        help="Path to meta-analysis aggregate table (used for effect/CI/study counts).",
    )
    parser.add_argument(
        "--extraction-input",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction CSV (used for participant counts and fallback study counts).",
    )
    parser.add_argument(
        "--grade-input",
        default="outputs/grade_evidence_profile.csv",
        help="Path to GRADE evidence profile CSV (used for certainty_grade by outcome).",
    )
    parser.add_argument(
        "--effect-label",
        default="d",
        help="Label prefix for pooled effect value (default: d).",
    )
    parser.add_argument(
        "--decimals",
        type=int,
        default=2,
        help="Decimal precision for effect and CI formatting (default: 2).",
    )
    parser.add_argument(
        "--output",
        default="outputs/results_summary_table.csv",
        help="Path to final results summary CSV output.",
    )
    parser.add_argument(
        "--latex-output",
        default="../04_manuscript/tables/results_summary_table.tex",
        help="Path to manuscript-ready LaTeX table output.",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/results_summary_table_summary.md",
        help="Path to markdown summary output.",
    )
    args = parser.parse_args()

    meta_input = Path(args.meta_input)
    extraction_input = Path(args.extraction_input)
    grade_input = Path(args.grade_input)
    output_path = Path(args.output)
    latex_output_path = Path(args.latex_output)
    summary_output = Path(args.summary_output)

    warnings: list[str] = []

    meta_df = read_csv_or_empty(meta_input)
    if meta_df.empty:
        warnings.append(
            "Meta-analysis input is missing or empty; effect/CI values fall back to `NR`."
        )

    extraction_df = read_csv_or_empty(extraction_input)
    if extraction_df.empty:
        warnings.append(
            "Extraction input is missing or empty; participant/study counts may be incomplete."
        )

    grade_df = read_csv_or_empty(grade_input)
    if grade_df.empty:
        warnings.append(
            "GRADE profile input is missing or empty; certainty grades fall back to `NR`."
        )

    participants_by_outcome, study_count_by_outcome = extract_outcome_metadata(extraction_df)
    certainty_by_outcome = aggregate_certainty_by_outcome(grade_df)

    results_df, build_warnings = build_results_table(
        meta_df=meta_df,
        participants_by_outcome=participants_by_outcome,
        study_count_by_outcome=study_count_by_outcome,
        certainty_by_outcome=certainty_by_outcome,
        effect_label=normalize(args.effect_label) or "d",
        decimals=max(0, int(args.decimals)),
    )
    warnings.extend(build_warnings)

    atomic_write_dataframe_csv(results_df, output_path, index=False)

    latex_text = render_latex_table(results_df, source_csv_path=output_path)
    atomic_write_text(latex_output_path, latex_text)

    summary_text = build_summary(
        meta_input=meta_input,
        extraction_input=extraction_input,
        grade_input=grade_input,
        output_path=output_path,
        latex_output_path=latex_output_path,
        summary_output=summary_output,
        results_df=results_df,
        warnings=warnings,
    )
    atomic_write_text(summary_output, summary_text)

    print(f"Wrote: {output_path}")
    print(f"Wrote: {latex_output_path}")
    print(f"Wrote: {summary_output}")
    print(f"Outcome rows: {int(results_df.shape[0])}")
    print(f"Warnings: {len(warnings)}")


if __name__ == "__main__":
    main()

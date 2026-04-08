import argparse
from datetime import datetime
from pathlib import Path
import re
import tempfile
import os

import pandas as pd


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


def normalize_lower(value: object) -> str:
    return normalize(value).lower()


def numeric_or_none(value: object) -> float | None:
    text = normalize(value)
    if not text:
        return None
    parsed = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


def parse_int_or_none(value: object) -> int | None:
    numeric = numeric_or_none(value)
    if numeric is None:
        return None
    parsed = int(round(numeric))
    return parsed if parsed >= 0 else None


def parse_ci_text(ci_text: object) -> tuple[float | None, float | None]:
    text = normalize(ci_text)
    if not text:
        return None, None

    numbers = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", text)
    if len(numbers) < 2:
        return None, None

    lower = numeric_or_none(numbers[0])
    upper = numeric_or_none(numbers[1])
    if lower is None or upper is None:
        return None, None
    return (lower, upper) if lower <= upper else (upper, lower)


def parse_effect_text(effect_text: object) -> float | None:
    text = normalize(effect_text)
    if not text:
        return None
    numbers = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", text)
    if not numbers:
        return None
    return numeric_or_none(numbers[0])


def latex_escape(value: object) -> str:
    text = normalize(value)
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


def latex_escape_with_i2_math(value: object) -> str:
    text = normalize(value)
    marker = "ZZZI2MATHZZZ"
    text = text.replace("I$^2$", marker)
    escaped = latex_escape(text)
    return escaped.replace(marker, "I$^2$")


def effect_direction_label(effect: float | None) -> str:
    if effect is None:
        return "uncertain direction"
    if effect > 0.05:
        return "increase"
    if effect < -0.05:
        return "decrease"
    return "little-to-no change"


def effect_magnitude_label(effect: float | None) -> str:
    if effect is None:
        return "uncertain"
    absolute = abs(effect)
    if absolute < 0.2:
        return "small"
    if absolute < 0.5:
        return "small-to-moderate"
    if absolute < 0.8:
        return "moderate"
    return "large"


def heterogeneity_phrase(k_studies: int | None, i2: float | None) -> str:
    if k_studies is None or k_studies <= 1:
        return "Estimate was based on a single study, so between-study consistency and heterogeneity could not be robustly assessed."
    if i2 is None:
        return "Heterogeneity could not be quantified from available inputs."
    if i2 < 30.0:
        return f"Findings were broadly consistent across studies (I² = {i2:.1f}%)."
    if i2 < 60.0:
        return f"Heterogeneity was moderate (I² = {i2:.1f}%), suggesting partial inconsistency across studies."
    return f"Heterogeneity remained substantial (I² = {i2:.1f}%), indicating notable between-study variability."


def publication_bias_phrase(flag: str) -> str:
    norm_flag = normalize_lower(flag)
    if norm_flag == "possible_asymmetry":
        return "Publication-bias diagnostics suggested possible funnel asymmetry."
    if norm_flag == "possible_asymmetry_low_power":
        return "Publication-bias diagnostics suggested possible asymmetry, but inference remains low-power."
    if norm_flag == "no_significant_asymmetry":
        return "No significant funnel asymmetry was detected."
    if norm_flag == "no_significant_asymmetry_low_power":
        return "No significant asymmetry was detected, though tests were likely underpowered."
    if norm_flag:
        return "Publication-bias diagnostics were not informative for this outcome."
    return ""


def outcome_records(
    *,
    meta_df: pd.DataFrame,
    results_df: pd.DataFrame,
    publication_bias_df: pd.DataFrame,
) -> list[dict[str, object]]:
    certainty_by_outcome: dict[str, str] = {}
    if not results_df.empty and "outcome" in results_df.columns:
        for _, row in results_df.iterrows():
            outcome = normalize(row.get("outcome", ""))
            if not outcome:
                continue
            certainty = normalize_lower(row.get("certainty_grade", ""))
            if certainty:
                certainty_by_outcome[outcome] = certainty

    bias_by_outcome: dict[str, str] = {}
    if not publication_bias_df.empty and "outcome" in publication_bias_df.columns:
        for _, row in publication_bias_df.iterrows():
            outcome = normalize(row.get("outcome", ""))
            if not outcome:
                continue
            bias_by_outcome[outcome] = normalize(row.get("funnel_asymmetry", ""))

    records: list[dict[str, object]] = []
    seen_outcomes: set[str] = set()

    if not meta_df.empty and "outcome" in meta_df.columns:
        for _, row in meta_df.iterrows():
            outcome = normalize(row.get("outcome", ""))
            if not outcome or outcome in seen_outcomes:
                continue
            seen_outcomes.add(outcome)

            records.append(
                {
                    "outcome": outcome,
                    "k_studies": parse_int_or_none(row.get("k_studies", "")),
                    "effect": numeric_or_none(row.get("pooled_effect", "")),
                    "ci_low": numeric_or_none(row.get("ci_low", "")),
                    "ci_high": numeric_or_none(row.get("ci_high", "")),
                    "i2": numeric_or_none(row.get("i2", "")),
                    "certainty": certainty_by_outcome.get(outcome, ""),
                    "funnel_asymmetry": bias_by_outcome.get(outcome, ""),
                }
            )

    if not results_df.empty and "outcome" in results_df.columns:
        for _, row in results_df.iterrows():
            outcome = normalize(row.get("outcome", ""))
            if not outcome or outcome in seen_outcomes:
                continue
            seen_outcomes.add(outcome)

            ci_low, ci_high = parse_ci_text(row.get("ci", ""))
            records.append(
                {
                    "outcome": outcome,
                    "k_studies": parse_int_or_none(row.get("studies", "")),
                    "effect": parse_effect_text(row.get("effect", "")),
                    "ci_low": ci_low,
                    "ci_high": ci_high,
                    "i2": None,
                    "certainty": normalize_lower(row.get("certainty_grade", "")),
                    "funnel_asymmetry": bias_by_outcome.get(outcome, ""),
                }
            )

    return records


def overall_interpretation(records: list[dict[str, object]]) -> str:
    if not records:
        return "No synthesized outcomes were available for narrative interpretation in this run."

    positive = 0
    negative = 0
    neutral = 0
    certainty_low_or_very_low = 0

    for record in records:
        effect = record.get("effect")
        if isinstance(effect, (int, float)):
            if effect > 0.05:
                positive += 1
            elif effect < -0.05:
                negative += 1
            else:
                neutral += 1
        else:
            neutral += 1

        certainty = normalize_lower(record.get("certainty", ""))
        if certainty in {"low", "very low"}:
            certainty_low_or_very_low += 1

    summary = (
        f"Across {len(records)} synthesized outcomes, {positive} showed directional increases, "
        f"{negative} showed directional decreases, and {neutral} were near-null or directionally uncertain."
    )

    if certainty_low_or_very_low > 0:
        summary += (
            f" Most outcomes were supported by low-certainty evidence "
            f"({certainty_low_or_very_low}/{len(records)} low or very low)."
        )

    return summary


def outcome_sentence(record: dict[str, object]) -> str:
    outcome = normalize(record.get("outcome", ""))
    effect = record.get("effect")
    ci_low = record.get("ci_low")
    ci_high = record.get("ci_high")
    k_studies = record.get("k_studies")
    i2 = record.get("i2")
    certainty = normalize_lower(record.get("certainty", ""))
    funnel_asymmetry = normalize(record.get("funnel_asymmetry", ""))

    direction = effect_direction_label(effect if isinstance(effect, (int, float)) else None)
    magnitude = effect_magnitude_label(effect if isinstance(effect, (int, float)) else None)

    if isinstance(effect, (int, float)):
        if isinstance(ci_low, (int, float)) and isinstance(ci_high, (int, float)):
            base = (
                f"For {outcome}, we observed a {magnitude} {direction} "
                f"(pooled effect = {effect:.2f}, 95% CI [{ci_low:.2f}, {ci_high:.2f}])."
            )
        else:
            base = f"For {outcome}, we observed a {magnitude} {direction} (pooled effect = {effect:.2f})."
    else:
        base = f"For {outcome}, quantitative effect magnitude was not estimable from available pooled inputs."

    heterogeneity = heterogeneity_phrase(
        k_studies if isinstance(k_studies, int) else None,
        i2 if isinstance(i2, (int, float)) else None,
    )

    certainty_phrase = ""
    if certainty:
        certainty_phrase = f" Certainty of evidence was {certainty}."

    bias_phrase = ""
    if funnel_asymmetry:
        bias_phrase = " " + publication_bias_phrase(funnel_asymmetry)

    return f"{base} {heterogeneity}{certainty_phrase}{bias_phrase}".strip()


def build_markdown(records: list[dict[str, object]]) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines: list[str] = []
    lines.append("# Results Interpretation Layer")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Overall interpretation")
    lines.append("")
    lines.append(overall_interpretation(records))
    lines.append("")
    lines.append("## Outcome-level interpretation")
    lines.append("")

    if records:
        for record in records:
            lines.append(f"- {outcome_sentence(record)}")
    else:
        lines.append("- No synthesized outcomes available.")

    lines.append("")
    return "\n".join(lines)


def build_tex(records: list[dict[str, object]]) -> str:
    lines: list[str] = []
    lines.append("% Auto-generated by 03_analysis/results_interpretation_layer.py")
    lines.append(r"\paragraph{Interpretation layer}")
    lines.append(rf"\noindent {latex_escape(overall_interpretation(records))}")
    lines.append("")
    lines.append(r"\begin{itemize}")

    if records:
        for record in records:
            sentence = outcome_sentence(record)
            sentence = sentence.replace("I²", "I$^2$")
            lines.append(rf"  \item {latex_escape_with_i2_math(sentence)}")
    else:
        lines.append(r"  \item No synthesized outcomes available for interpretation in this run.")

    lines.append(r"\end{itemize}")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate an interpretation narrative layer for manuscript Results from synthesis artifacts."
    )
    parser.add_argument(
        "--meta-input",
        default="outputs/meta_analysis_results.csv",
        help="Path to meta-analysis results CSV.",
    )
    parser.add_argument(
        "--results-summary-input",
        default="outputs/results_summary_table.csv",
        help="Path to results summary table CSV.",
    )
    parser.add_argument(
        "--publication-bias-input",
        default="outputs/publication_bias_results.csv",
        help="Path to publication-bias results CSV.",
    )
    parser.add_argument(
        "--markdown-output",
        default="outputs/results_interpretation_layer.md",
        help="Path to markdown narrative output.",
    )
    parser.add_argument(
        "--tex-output",
        default="../04_manuscript/sections/03c_interpretation_auto.tex",
        help="Path to manuscript TeX narrative output.",
    )
    args = parser.parse_args()

    meta_df = read_csv_or_empty(Path(args.meta_input))
    results_df = read_csv_or_empty(Path(args.results_summary_input))
    publication_bias_df = read_csv_or_empty(Path(args.publication_bias_input))

    records = outcome_records(
        meta_df=meta_df,
        results_df=results_df,
        publication_bias_df=publication_bias_df,
    )

    markdown_text = build_markdown(records)
    tex_text = build_tex(records)

    markdown_output = Path(args.markdown_output)
    tex_output = Path(args.tex_output)

    atomic_write_text(markdown_output, markdown_text)
    atomic_write_text(tex_output, tex_text)

    print(f"Wrote: {markdown_output}")
    print(f"Wrote: {tex_output}")
    print(f"Interpreted outcomes: {len(records)}")


if __name__ == "__main__":
    main()

import argparse
import math
from datetime import datetime
from pathlib import Path
import os
import tempfile

import pandas as pd

import publication_bias_assessment as pba


RESULT_COLUMNS = [
    "analysis",
    "included_studies",
    "effect",
    "ci_low",
    "ci_high",
    "notes",
]

SUPPORTED_METRICS = ["converted_d", "converted_r", "fisher_z", "converted_or"]


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


def should_fail(mode: str, errors: int, warnings: int) -> bool:
    norm_mode = pba.normalize_lower(mode)
    if norm_mode == "none":
        return False
    if norm_mode == "warning":
        return (errors + warnings) > 0
    return errors > 0


def inverse_transform_metric(metric: str, value: float) -> float:
    if metric == "converted_or":
        return float(math.exp(value))
    return float(value)


def prepare_poolable_data(data_df: pd.DataFrame) -> pd.DataFrame:
    if data_df.empty:
        return pd.DataFrame(columns=["study_id", "effect_analysis", "se"])

    working = data_df.copy()
    working["effect_analysis"] = pd.to_numeric(working["effect_analysis"], errors="coerce")
    working["se"] = pd.to_numeric(working["se"], errors="coerce")
    working = working[(working["effect_analysis"].notna()) & (working["se"].notna()) & (working["se"] > 0)].copy()

    if "study_id" not in working.columns:
        working["study_id"] = ""

    return working[["study_id", "effect_analysis", "se"]].reset_index(drop=True)


def pool_effect(data_df: pd.DataFrame, *, metric: str, model: str) -> dict[str, float] | None:
    if data_df.empty:
        return None

    effects = data_df["effect_analysis"].astype(float).to_list()
    variances = (data_df["se"].astype(float) ** 2).to_list()

    if not effects or not variances:
        return None

    weights_fixed = [1.0 / value for value in variances]
    sum_w_fixed = sum(weights_fixed)
    if sum_w_fixed <= 0:
        return None

    fixed_pooled = sum(weight * effect for weight, effect in zip(weights_fixed, effects)) / sum_w_fixed

    q_stat = sum(weight * ((effect - fixed_pooled) ** 2) for weight, effect in zip(weights_fixed, effects))
    degrees_freedom = max(0, len(effects) - 1)

    tau2 = 0.0
    if model == "random_effects" and len(effects) > 1:
        sum_w2 = sum(weight * weight for weight in weights_fixed)
        correction = sum_w_fixed - (sum_w2 / sum_w_fixed) if sum_w_fixed > 0 else 0.0
        if correction > 0:
            tau2 = max((q_stat - degrees_freedom) / correction, 0.0)

    if model == "random_effects":
        weights = [1.0 / (value + tau2) for value in variances]
    else:
        weights = weights_fixed

    sum_weights = sum(weights)
    if sum_weights <= 0:
        return None

    pooled = sum(weight * effect for weight, effect in zip(weights, effects)) / sum_weights
    pooled_se = math.sqrt(1.0 / sum_weights)

    ci_low = pooled - (1.96 * pooled_se)
    ci_high = pooled + (1.96 * pooled_se)

    return {
        "effect": inverse_transform_metric(metric, pooled),
        "ci_low": inverse_transform_metric(metric, ci_low),
        "ci_high": inverse_transform_metric(metric, ci_high),
    }


def leave_one_out_summary(data_df: pd.DataFrame, *, metric: str, model: str) -> dict[str, object]:
    if data_df.empty:
        return {
            "included_studies": 0,
            "effect": "",
            "ci_low": "",
            "ci_high": "",
            "notes": "No poolable studies with estimable standard errors.",
        }

    unique_studies = [study for study in data_df["study_id"].astype(str).tolist() if study]
    unique_count = len(set(unique_studies))
    if unique_count <= 1:
        return {
            "included_studies": unique_count,
            "effect": "",
            "ci_low": "",
            "ci_high": "",
            "notes": "Need at least 2 studies for leave-one-out sensitivity.",
        }

    pooled_values: list[float] = []
    for study_id in sorted(set(unique_studies)):
        subset = data_df[data_df["study_id"].astype(str) != study_id].copy()
        pooled = pool_effect(subset, metric=metric, model=model)
        if pooled is None:
            continue
        pooled_values.append(float(pooled["effect"]))

    if not pooled_values:
        return {
            "included_studies": unique_count,
            "effect": "",
            "ci_low": "",
            "ci_high": "",
            "notes": "Leave-one-out runs did not yield poolable models.",
        }

    return {
        "included_studies": unique_count,
        "effect": float(sum(pooled_values) / len(pooled_values)),
        "ci_low": float(min(pooled_values)),
        "ci_high": float(max(pooled_values)),
        "notes": f"Range across {len(pooled_values)} leave-one-out re-fits ({model.replace('_', '-')}).",
    }


def build_summary(
    *,
    metric: str,
    converted_path: Path,
    extraction_path: Path,
    quality_path: Path,
    output_path: Path,
    summary_path: Path,
    raw_rows: int,
    poolable_rows: int,
    results_df: pd.DataFrame,
    warnings: list[str],
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = []
    lines.append("# Sensitivity Analysis Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Converted input: `{converted_path.as_posix()}`")
    lines.append(f"- Extraction input: `{extraction_path.as_posix()}`")
    lines.append(f"- Quality input: `{quality_path.as_posix()}`")
    lines.append(f"- Metric: `{metric}`")
    lines.append(f"- Results output: `{output_path.as_posix()}`")
    lines.append(f"- Summary output: `{summary_path.as_posix()}`")
    lines.append("")
    lines.append("## Counts")
    lines.append("")
    lines.append(f"- Raw publication-bias rows: {raw_rows}")
    lines.append(f"- Poolable rows with SE: {poolable_rows}")
    lines.append(f"- Sensitivity analyses exported: {int(results_df.shape[0])}")
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build sensitivity analysis table (leave-one-out, high-quality-only, random-effects-only)."
    )
    parser.add_argument(
        "--converted-input",
        default="outputs/effect_size_converted.csv",
        help="Path to converted effect-size CSV.",
    )
    parser.add_argument(
        "--extraction-input",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction CSV for CI/sample-size metadata.",
    )
    parser.add_argument(
        "--quality-input",
        default="outputs/quality_appraisal_scored.csv",
        help="Path to quality appraisal scored CSV.",
    )
    parser.add_argument(
        "--metric",
        default="converted_d",
        choices=SUPPORTED_METRICS,
        help="Effect metric used for pooling.",
    )
    parser.add_argument(
        "--high-quality-labels",
        default="high",
        help="Comma-separated quality_band labels treated as high quality (default: high).",
    )
    parser.add_argument(
        "--output",
        default="outputs/sensitivity_analysis.csv",
        help="Path to sensitivity analysis CSV output.",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/sensitivity_analysis_summary.md",
        help="Path to markdown summary output.",
    )
    parser.add_argument(
        "--fail-on",
        default="none",
        choices=["none", "warning", "error"],
        help="Fail mode: none (default), warning, error.",
    )
    args = parser.parse_args()

    converted_path = Path(args.converted_input)
    extraction_path = Path(args.extraction_input)
    quality_path = Path(args.quality_input)
    output_path = Path(args.output)
    summary_path = Path(args.summary_output)

    if not converted_path.exists():
        raise FileNotFoundError(f"Converted input not found: {converted_path}")
    if not extraction_path.exists():
        raise FileNotFoundError(f"Extraction input not found: {extraction_path}")

    converted_df = pba.read_csv_or_empty(converted_path)
    extraction_df = pba.read_csv_or_empty(extraction_path)
    quality_df = pba.read_csv_or_empty(quality_path)

    bias_data_df, _ = pba.prepare_bias_data(
        converted_df,
        extraction_df,
        metric=args.metric,
        max_studies=0,
    )
    poolable_df = prepare_poolable_data(bias_data_df)

    warnings: list[str] = []
    rows: list[dict[str, object]] = []

    # 1) random_effects_only
    random_effect = pool_effect(poolable_df, metric=args.metric, model="random_effects")
    if random_effect is None:
        rows.append(
            {
                "analysis": "random_effects_only",
                "included_studies": int(poolable_df.shape[0]),
                "effect": "",
                "ci_low": "",
                "ci_high": "",
                "notes": "No poolable studies with estimable standard errors.",
            }
        )
        warnings.append("random_effects_only: no poolable studies with SE.")
    else:
        rows.append(
            {
                "analysis": "random_effects_only",
                "included_studies": int(poolable_df.shape[0]),
                "effect": float(random_effect["effect"]),
                "ci_low": float(random_effect["ci_low"]),
                "ci_high": float(random_effect["ci_high"]),
                "notes": "Primary random-effects pooled estimate.",
            }
        )

    # 2) high_quality_only
    high_labels = {
        pba.normalize_lower(label)
        for label in str(args.high_quality_labels).split(",")
        if pba.normalize(label)
    }
    if not high_labels:
        high_labels = {"high"}

    high_quality_ids: set[str] = set()
    if quality_df.empty or "study_id" not in quality_df.columns or "quality_band" not in quality_df.columns:
        warnings.append("high_quality_only: quality file missing or schema incomplete; analysis could not be filtered.")
    else:
        quality_working = quality_df.copy()
        quality_working["quality_band"] = quality_working["quality_band"].fillna("").astype(str).str.strip().str.lower()
        for _, row in quality_working.iterrows():
            study_id = pba.normalize(row.get("study_id", ""))
            quality_band = pba.normalize_lower(row.get("quality_band", ""))
            if study_id and quality_band in high_labels:
                high_quality_ids.add(study_id)

    high_quality_df = poolable_df[poolable_df["study_id"].astype(str).isin(high_quality_ids)].copy()
    high_quality_effect = pool_effect(high_quality_df, metric=args.metric, model="random_effects")

    if high_quality_effect is None:
        rows.append(
            {
                "analysis": "high_quality_only",
                "included_studies": int(high_quality_df.shape[0]),
                "effect": "",
                "ci_low": "",
                "ci_high": "",
                "notes": "No poolable high-quality studies under selected quality labels.",
            }
        )
        warnings.append("high_quality_only: no poolable high-quality studies.")
    else:
        rows.append(
            {
                "analysis": "high_quality_only",
                "included_studies": int(high_quality_df.shape[0]),
                "effect": float(high_quality_effect["effect"]),
                "ci_low": float(high_quality_effect["ci_low"]),
                "ci_high": float(high_quality_effect["ci_high"]),
                "notes": f"Filtered by quality_band in {sorted(high_labels)}.",
            }
        )

    # 3) leave-one-out
    leave_one_out = leave_one_out_summary(poolable_df, metric=args.metric, model="random_effects")
    rows.append(
        {
            "analysis": "leave-one-out",
            "included_studies": int(leave_one_out["included_studies"]),
            "effect": leave_one_out["effect"],
            "ci_low": leave_one_out["ci_low"],
            "ci_high": leave_one_out["ci_high"],
            "notes": str(leave_one_out["notes"]),
        }
    )

    results_df = pd.DataFrame(rows, columns=RESULT_COLUMNS)
    atomic_write_dataframe_csv(results_df, output_path, index=False)

    summary_text = build_summary(
        metric=args.metric,
        converted_path=converted_path,
        extraction_path=extraction_path,
        quality_path=quality_path,
        output_path=output_path,
        summary_path=summary_path,
        raw_rows=int(bias_data_df.shape[0]),
        poolable_rows=int(poolable_df.shape[0]),
        results_df=results_df,
        warnings=warnings,
    )
    atomic_write_text(summary_path, summary_text)

    error_count = 0
    warning_count = len(warnings)

    print(f"Wrote: {output_path}")
    print(f"Wrote: {summary_path}")
    print(f"Sensitivity analyses exported: {int(results_df.shape[0])}")
    print(f"Warnings: {warning_count}")

    if should_fail(args.fail_on, errors=error_count, warnings=warning_count):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
import argparse
import math
import os
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import publication_bias_assessment as pba

RESULT_COLUMNS = [
    "subgroup",
    "k",
    "effect",
    "ci_low",
    "ci_high",
    "i2",
]

SUPPORTED_METRICS = ["converted_d", "converted_r", "fisher_z", "converted_or"]

SUBGROUP_SPECS: list[tuple[str, list[str]]] = [
    ("region", ["country", "region"]),
    ("study_design", ["study_design"]),
    ("population_type", ["population_type", "setting", "population"]),
    ("followup_duration", ["followup_duration", "follow_up", "followup", "followup_time"]),
]


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
    for column in ["study_id", "effect_analysis", "se"]:
        if column not in working.columns:
            working[column] = ""

    working["effect_analysis"] = pd.to_numeric(working["effect_analysis"], errors="coerce")
    working["se"] = pd.to_numeric(working["se"], errors="coerce")
    working["study_id"] = working["study_id"].fillna("").astype(str).str.strip()
    working = working[
        (working["study_id"] != "")
        & (working["effect_analysis"].notna())
        & (working["se"].notna())
        & (working["se"] > 0)
    ].copy()

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

    fixed_pooled = (
        sum(weight * effect for weight, effect in zip(weights_fixed, effects)) / sum_w_fixed
    )
    q_stat = sum(
        weight * ((effect - fixed_pooled) ** 2) for weight, effect in zip(weights_fixed, effects)
    )
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

    if q_stat <= 0 or degrees_freedom <= 0:
        i2 = 0.0
    else:
        i2 = max(((q_stat - degrees_freedom) / q_stat) * 100.0, 0.0)

    return {
        "effect": inverse_transform_metric(metric, pooled),
        "ci_low": inverse_transform_metric(metric, ci_low),
        "ci_high": inverse_transform_metric(metric, ci_high),
        "i2": float(i2),
    }


def resolve_subgroup_column(extraction_df: pd.DataFrame, candidates: list[str]) -> str:
    for column in candidates:
        if column in extraction_df.columns:
            return column
    return ""


def build_study_to_group_map(extraction_df: pd.DataFrame, group_column: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    if (
        extraction_df.empty
        or "study_id" not in extraction_df.columns
        or group_column not in extraction_df.columns
    ):
        return mapping

    for _, row in extraction_df.iterrows():
        study_id = pba.normalize(row.get("study_id", ""))
        if not study_id:
            continue

        group_value = pba.normalize(row.get(group_column, ""))
        if pba.is_missing(group_value):
            continue

        if study_id not in mapping:
            mapping[study_id] = group_value

    return mapping


def build_summary(
    *,
    metric: str,
    model: str,
    data_input_path: Path,
    extraction_input_path: Path,
    output_path: Path,
    summary_path: Path,
    raw_rows: int,
    poolable_rows: int,
    subgroup_rows: int,
    warnings: list[str],
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = []
    lines.append("# Subgroup Analysis Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Data input: `{data_input_path.as_posix()}`")
    lines.append(f"- Extraction input: `{extraction_input_path.as_posix()}`")
    lines.append(f"- Metric: `{metric}`")
    lines.append(f"- Model: `{model}`")
    lines.append(f"- Results output: `{output_path.as_posix()}`")
    lines.append(f"- Summary output: `{summary_path.as_posix()}`")
    lines.append("")
    lines.append("## Counts")
    lines.append("")
    lines.append(f"- Raw rows in subgroup input data: {raw_rows}")
    lines.append(f"- Poolable rows with estimable SE: {poolable_rows}")
    lines.append(f"- Subgroup rows exported: {subgroup_rows}")
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
        description="Build subgroup-analysis table (region, study_design, population_type, followup_duration)."
    )
    parser.add_argument(
        "--data-input",
        default="outputs/publication_bias_data.csv",
        help="Path to prepared publication-bias data CSV (must include effect_analysis + se).",
    )
    parser.add_argument(
        "--converted-input",
        default="outputs/effect_size_converted.csv",
        help="Path to converted effect-size CSV used as fallback when data-input is missing.",
    )
    parser.add_argument(
        "--extraction-input",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction CSV for subgroup metadata.",
    )
    parser.add_argument(
        "--metric",
        default="converted_d",
        choices=SUPPORTED_METRICS,
        help="Metric used to derive publication-bias data fallback.",
    )
    parser.add_argument(
        "--model",
        default="random_effects",
        choices=["random_effects", "fixed_effects"],
        help="Pooling model for subgroup estimates.",
    )
    parser.add_argument(
        "--output",
        default="outputs/subgroup_analysis.csv",
        help="Path to subgroup-analysis CSV output.",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/subgroup_analysis_summary.md",
        help="Path to markdown summary output.",
    )
    parser.add_argument(
        "--fail-on",
        default="none",
        choices=["none", "warning", "error"],
        help="Fail mode: none (default), warning, error.",
    )
    args = parser.parse_args()

    data_input_path = Path(args.data_input)
    converted_input_path = Path(args.converted_input)
    extraction_input_path = Path(args.extraction_input)
    output_path = Path(args.output)
    summary_path = Path(args.summary_output)

    if not extraction_input_path.exists():
        raise FileNotFoundError(f"Extraction input not found: {extraction_input_path}")

    extraction_df = pba.read_csv_or_empty(extraction_input_path)

    if data_input_path.exists():
        subgroup_input_df = pba.read_csv_or_empty(data_input_path)
    else:
        if not converted_input_path.exists():
            raise FileNotFoundError(
                f"Neither subgroup data input `{data_input_path}` nor converted input `{converted_input_path}` exists."
            )
        converted_df = pba.read_csv_or_empty(converted_input_path)
        subgroup_input_df, _ = pba.prepare_bias_data(
            converted_df,
            extraction_df,
            metric=args.metric,
            max_studies=0,
        )

    poolable_df = prepare_poolable_data(subgroup_input_df)

    warnings: list[str] = []
    rows: list[dict[str, object]] = []

    if poolable_df.empty:
        warnings.append(
            "No poolable rows with estimable SE; subgroup analysis table contains only placeholders."
        )

    for subgroup_name, candidate_columns in SUBGROUP_SPECS:
        group_column = resolve_subgroup_column(extraction_df, candidate_columns)
        if not group_column:
            rows.append(
                {
                    "subgroup": f"{subgroup_name}: not_available",
                    "k": 0,
                    "effect": "",
                    "ci_low": "",
                    "ci_high": "",
                    "i2": "",
                }
            )
            warnings.append(
                f"{subgroup_name}: no matching extraction column found among {candidate_columns}."
            )
            continue

        mapping = build_study_to_group_map(extraction_df, group_column)
        if not mapping:
            rows.append(
                {
                    "subgroup": f"{subgroup_name}: not_available",
                    "k": 0,
                    "effect": "",
                    "ci_low": "",
                    "ci_high": "",
                    "i2": "",
                }
            )
            warnings.append(
                f"{subgroup_name}: selected column `{group_column}` has no usable values."
            )
            continue

        working = poolable_df.copy()
        working["group_value"] = working["study_id"].map(mapping).fillna("")
        working = working[working["group_value"].astype(str).str.strip() != ""].copy()

        if working.empty:
            rows.append(
                {
                    "subgroup": f"{subgroup_name}: not_available",
                    "k": 0,
                    "effect": "",
                    "ci_low": "",
                    "ci_high": "",
                    "i2": "",
                }
            )
            warnings.append(
                f"{subgroup_name}: no poolable studies matched `{group_column}` values."
            )
            continue

        for group_value, group_df in working.groupby("group_value", sort=True):
            pooled = pool_effect(group_df, metric=args.metric, model=args.model)
            k_count = int(group_df.shape[0])
            if pooled is None:
                rows.append(
                    {
                        "subgroup": f"{subgroup_name}: {pba.normalize(group_value)}",
                        "k": k_count,
                        "effect": "",
                        "ci_low": "",
                        "ci_high": "",
                        "i2": "",
                    }
                )
                warnings.append(
                    f"{subgroup_name}: `{group_value}` could not be pooled ({args.model})."
                )
            else:
                rows.append(
                    {
                        "subgroup": f"{subgroup_name}: {pba.normalize(group_value)}",
                        "k": k_count,
                        "effect": float(pooled["effect"]),
                        "ci_low": float(pooled["ci_low"]),
                        "ci_high": float(pooled["ci_high"]),
                        "i2": float(pooled["i2"]),
                    }
                )

    results_df = pd.DataFrame(rows, columns=RESULT_COLUMNS)
    atomic_write_dataframe_csv(results_df, output_path, index=False)

    summary_text = build_summary(
        metric=args.metric,
        model=args.model,
        data_input_path=data_input_path,
        extraction_input_path=extraction_input_path,
        output_path=output_path,
        summary_path=summary_path,
        raw_rows=int(subgroup_input_df.shape[0]),
        poolable_rows=int(poolable_df.shape[0]),
        subgroup_rows=int(results_df.shape[0]),
        warnings=warnings,
    )
    atomic_write_text(summary_path, summary_text)

    error_count = 0
    warning_count = len(warnings)

    print(f"Wrote: {output_path}")
    print(f"Wrote: {summary_path}")
    print(f"Subgroup rows exported: {int(results_df.shape[0])}")
    print(f"Warnings: {warning_count}")

    if should_fail(args.fail_on, errors=error_count, warnings=warning_count):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

import argparse
import math
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats


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

METRIC_LABELS = {
    "converted_d": "Standardized mean difference (d)",
    "converted_r": "Correlation (r)",
    "fisher_z": "Fisher z",
    "converted_or": "Odds ratio (OR)",
}

SUPPORTED_METRICS = ["converted_d", "converted_r", "fisher_z", "converted_or"]

PUBLICATION_BIAS_RESULTS_COLUMNS = [
    "outcome",
    "k_studies",
    "n_with_se",
    "egger_test_p",
    "begg_test_p",
    "funnel_asymmetry",
]


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def normalize_lower(value: object) -> str:
    return normalize(value).lower()


def is_missing(value: object) -> bool:
    return normalize_lower(value) in MISSING_CODES


def read_csv_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def numeric_or_none(value: object) -> float | None:
    text = normalize(value)
    if is_missing(text):
        return None

    parsed = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


def signed_direction(effect_direction: object) -> int | None:
    direction = normalize_lower(effect_direction)
    if direction == "positive":
        return 1
    if direction == "negative":
        return -1
    return None


def convert_from_r(value_r: float) -> dict[str, float | None]:
    result: dict[str, float | None] = {
        "converted_r": value_r,
        "converted_d": None,
        "converted_or": None,
        "converted_eta2": value_r * value_r,
        "fisher_z": None,
    }

    if abs(value_r) < 1.0:
        result["converted_d"] = (2.0 * value_r) / math.sqrt(1.0 - (value_r * value_r))
        result["converted_or"] = math.exp(result["converted_d"] * math.pi / math.sqrt(3.0))
        result["fisher_z"] = math.atanh(value_r)

    return result


def convert_from_d(value_d: float) -> dict[str, float | None]:
    converted_r = value_d / math.sqrt((value_d * value_d) + 4.0)
    converted_or = math.exp(value_d * math.pi / math.sqrt(3.0))
    return {
        "converted_r": converted_r,
        "converted_d": value_d,
        "converted_or": converted_or,
        "converted_eta2": converted_r * converted_r,
        "fisher_z": math.atanh(converted_r),
    }


def convert_from_or(value_or: float) -> dict[str, float | None]:
    if value_or <= 0:
        return {
            "converted_r": None,
            "converted_d": None,
            "converted_or": None,
            "converted_eta2": None,
            "fisher_z": None,
        }

    converted_d = math.log(value_or) * math.sqrt(3.0) / math.pi
    converted_r = converted_d / math.sqrt((converted_d * converted_d) + 4.0)
    return {
        "converted_r": converted_r,
        "converted_d": converted_d,
        "converted_or": value_or,
        "converted_eta2": converted_r * converted_r,
        "fisher_z": math.atanh(converted_r),
    }


def convert_from_eta2(value_eta2: float, direction_sign: int | None) -> dict[str, float | None]:
    if value_eta2 < 0:
        return {
            "converted_r": None,
            "converted_d": None,
            "converted_or": None,
            "converted_eta2": None,
            "fisher_z": None,
        }

    if value_eta2 == 0:
        return {
            "converted_r": 0.0,
            "converted_d": 0.0,
            "converted_or": 1.0,
            "converted_eta2": 0.0,
            "fisher_z": 0.0,
        }

    if direction_sign is None:
        return {
            "converted_r": None,
            "converted_d": None,
            "converted_or": None,
            "converted_eta2": value_eta2,
            "fisher_z": None,
        }

    converted_r = float(direction_sign) * math.sqrt(value_eta2)
    converted = convert_from_r(converted_r)
    converted["converted_eta2"] = value_eta2
    return converted


def convert_source_value(
    *,
    source_metric: str,
    source_value: float,
    direction_sign: int | None,
    target_metric: str,
) -> float | None:
    if source_metric == "r":
        converted = convert_from_r(source_value)
    elif source_metric == "d":
        converted = convert_from_d(source_value)
    elif source_metric == "or":
        converted = convert_from_or(source_value)
    elif source_metric == "eta2":
        converted = convert_from_eta2(source_value, direction_sign)
    else:
        return None

    return converted.get(target_metric)


def approximate_ci(metric: str, effect_value: float, sample_size: int | None) -> tuple[float, float] | None:
    if sample_size is None or sample_size <= 0:
        return None

    if metric == "converted_d":
        se_d = 2.0 / math.sqrt(float(sample_size))
        margin = 1.96 * se_d
        return effect_value - margin, effect_value + margin

    if metric == "fisher_z":
        if sample_size <= 3:
            return None
        se_z = 1.0 / math.sqrt(float(sample_size - 3))
        margin = 1.96 * se_z
        return effect_value - margin, effect_value + margin

    if metric == "converted_r":
        if sample_size <= 3 or abs(effect_value) >= 1.0:
            return None
        fisher_z = math.atanh(effect_value)
        se_z = 1.0 / math.sqrt(float(sample_size - 3))
        margin = 1.96 * se_z
        lower = math.tanh(fisher_z - margin)
        upper = math.tanh(fisher_z + margin)
        return lower, upper

    if metric == "converted_or":
        if effect_value <= 0:
            return None
        d_value = math.log(effect_value) * math.sqrt(3.0) / math.pi
        se_d = 2.0 / math.sqrt(float(sample_size))
        margin = 1.96 * se_d
        lower_d = d_value - margin
        upper_d = d_value + margin
        lower_or = math.exp(lower_d * math.pi / math.sqrt(3.0))
        upper_or = math.exp(upper_d * math.pi / math.sqrt(3.0))
        return lower_or, upper_or

    return None


def parse_row_index(value: object) -> int | None:
    numeric = numeric_or_none(value)
    if numeric is None:
        return None
    row_number = int(round(numeric))
    index = row_number - 2
    return index if index >= 0 else None


def parse_sample_size(value: object) -> int | None:
    numeric = numeric_or_none(value)
    if numeric is None:
        return None
    sample = int(round(numeric))
    return sample if sample > 0 else None


def build_study_label(study_id: str, first_author: str, year: str) -> str:
    if first_author and year:
        return f"{first_author} ({year}) [{study_id}]"
    if first_author:
        return f"{first_author} [{study_id}]"
    return study_id


def analysis_metric_label(metric: str) -> str:
    if metric == "converted_or":
        return "log(OR)"
    return METRIC_LABELS.get(metric, metric)


def normalize_source_metric(value: object) -> str:
    text = normalize_lower(value)
    return text if text in {"r", "d", "or", "eta2"} else ""


def infer_outcome_label(metadata: dict[str, object]) -> str:
    for column in [
        "outcome_construct",
        "outcome_measure",
        "outcome",
        "identity_construct",
        "identity_measure",
    ]:
        value = normalize(metadata.get(column, ""))
        if not is_missing(value):
            return value
    return "overall"


def transform_for_analysis(metric: str, effect: float, ci_lower: float | None, ci_upper: float | None) -> tuple[float | None, float | None, float | None]:
    if metric != "converted_or":
        return effect, ci_lower, ci_upper

    if effect <= 0:
        return None, None, None

    transformed_effect = math.log(effect)

    if ci_lower is None or ci_upper is None or ci_lower <= 0 or ci_upper <= 0:
        return transformed_effect, None, None

    lower = math.log(min(ci_lower, ci_upper))
    upper = math.log(max(ci_lower, ci_upper))
    return transformed_effect, lower, upper


def prepare_bias_data(
    converted_df: pd.DataFrame,
    extraction_df: pd.DataFrame,
    *,
    metric: str,
    max_studies: int,
) -> tuple[pd.DataFrame, dict[str, int]]:
    stats_dict = {
        "input_rows": 0,
        "eligible_rows": 0,
        "with_raw_ci": 0,
        "with_approx_ci": 0,
        "with_se": 0,
        "without_se": 0,
        "trimmed_rows": 0,
    }

    required_converted = {
        "row",
        "study_id",
        "first_author",
        "year",
        "source_metric_canonical",
        "effect_direction",
        "conversion_status",
        metric,
    }
    if converted_df.empty or not required_converted.issubset(converted_df.columns):
        return pd.DataFrame(), stats_dict

    stats_dict["input_rows"] = int(converted_df.shape[0])

    working = converted_df.copy()
    working["conversion_status"] = working["conversion_status"].fillna("").astype(str).str.strip().str.lower()
    working = working[working["conversion_status"].isin({"converted", "partial"})].copy()

    working["effect_value"] = pd.to_numeric(working[metric], errors="coerce")
    working = working[working["effect_value"].notna()].copy()
    if working.empty:
        return pd.DataFrame(), stats_dict

    stats_dict["eligible_rows"] = int(working.shape[0])

    metadata_map: dict[int, dict[str, object]] = {}
    if not extraction_df.empty:
        extraction = extraction_df.copy()
        for column in [
            "ci_lower",
            "ci_upper",
            "sample_size",
            "effect_direction",
            "outcome_construct",
            "outcome_measure",
            "outcome",
            "identity_construct",
            "identity_measure",
        ]:
            if column not in extraction.columns:
                extraction[column] = ""

        for idx, row in extraction.iterrows():
            metadata_map[int(idx)] = {
                "ci_lower": row.get("ci_lower", ""),
                "ci_upper": row.get("ci_upper", ""),
                "sample_size": row.get("sample_size", ""),
                "effect_direction": row.get("effect_direction", ""),
                "outcome_construct": row.get("outcome_construct", ""),
                "outcome_measure": row.get("outcome_measure", ""),
                "outcome": row.get("outcome", ""),
                "identity_construct": row.get("identity_construct", ""),
                "identity_measure": row.get("identity_measure", ""),
            }

    rows: list[dict[str, object]] = []
    for _, row in working.iterrows():
        effect_value = float(row["effect_value"])
        source_metric = normalize_source_metric(row.get("source_metric_canonical", ""))
        direction_sign = signed_direction(row.get("effect_direction", ""))

        extraction_index = parse_row_index(row.get("row", ""))
        metadata = metadata_map.get(extraction_index, {}) if extraction_index is not None else {}
        raw_ci_lower = numeric_or_none(metadata.get("ci_lower", ""))
        raw_ci_upper = numeric_or_none(metadata.get("ci_upper", ""))
        raw_sample_size = parse_sample_size(metadata.get("sample_size", ""))
        if direction_sign is None:
            direction_sign = signed_direction(metadata.get("effect_direction", ""))

        outcome = infer_outcome_label(metadata)

        ci_lower: float | None = None
        ci_upper: float | None = None
        ci_source = "none"

        if raw_ci_lower is not None and raw_ci_upper is not None and source_metric:
            lower_converted = convert_source_value(
                source_metric=source_metric,
                source_value=raw_ci_lower,
                direction_sign=direction_sign,
                target_metric=metric,
            )
            upper_converted = convert_source_value(
                source_metric=source_metric,
                source_value=raw_ci_upper,
                direction_sign=direction_sign,
                target_metric=metric,
            )
            if lower_converted is not None and upper_converted is not None:
                ci_lower, ci_upper = sorted([float(lower_converted), float(upper_converted)])
                ci_source = "converted_raw_ci"
                stats_dict["with_raw_ci"] += 1

        if ci_lower is None or ci_upper is None:
            approx = approximate_ci(metric, effect_value, raw_sample_size)
            if approx is not None:
                ci_lower, ci_upper = approx
                ci_source = "approx_sample"
                stats_dict["with_approx_ci"] += 1

        analysis_effect, analysis_ci_lower, analysis_ci_upper = transform_for_analysis(
            metric,
            effect_value,
            ci_lower,
            ci_upper,
        )

        se_value: float | None = None
        if analysis_ci_lower is not None and analysis_ci_upper is not None:
            margin = (analysis_ci_upper - analysis_ci_lower) / (2.0 * 1.96)
            if math.isfinite(margin) and margin > 0:
                se_value = float(margin)

        if se_value is None:
            stats_dict["without_se"] += 1
        else:
            stats_dict["with_se"] += 1

        precision = (1.0 / se_value) if se_value is not None else None
        snd = None
        if se_value is not None and analysis_effect is not None and math.isfinite(analysis_effect):
            snd = analysis_effect / se_value

        study_id = normalize(row.get("study_id", ""))
        first_author = normalize(row.get("first_author", ""))
        year = normalize(row.get("year", ""))
        rows.append(
            {
                "study_id": study_id,
                "first_author": first_author,
                "year": year,
                "study_label": build_study_label(study_id, first_author, year),
                "outcome": outcome,
                "metric": metric,
                "analysis_metric": "log_converted_or" if metric == "converted_or" else metric,
                "source_metric_canonical": source_metric,
                "effect_raw": effect_value,
                "effect_analysis": "" if analysis_effect is None else analysis_effect,
                "ci_lower_raw": "" if ci_lower is None else ci_lower,
                "ci_upper_raw": "" if ci_upper is None else ci_upper,
                "ci_lower_analysis": "" if analysis_ci_lower is None else analysis_ci_lower,
                "ci_upper_analysis": "" if analysis_ci_upper is None else analysis_ci_upper,
                "se": "" if se_value is None else se_value,
                "precision": "" if precision is None else precision,
                "snd": "" if snd is None else snd,
                "ci_source": ci_source,
                "sample_size": "" if raw_sample_size is None else raw_sample_size,
            }
        )

    data_df = pd.DataFrame(rows)
    if data_df.empty:
        return data_df, stats_dict

    data_df = data_df.sort_values(["study_id", "year"], kind="stable").reset_index(drop=True)

    if max_studies > 0 and data_df.shape[0] > max_studies:
        stats_dict["trimmed_rows"] = int(data_df.shape[0] - max_studies)
        data_df = data_df.head(max_studies).copy()

    return data_df, stats_dict


def egger_regression(data_df: pd.DataFrame, *, min_studies: int) -> dict[str, object]:
    if data_df.empty:
        return {
            "status": "not_computed",
            "reason": "no_rows",
            "n_studies": 0,
            "min_studies": min_studies,
        }

    working = data_df.copy()
    working["effect_analysis"] = pd.to_numeric(working["effect_analysis"], errors="coerce")
    working["se"] = pd.to_numeric(working["se"], errors="coerce")
    working = working[(working["se"].notna()) & (working["se"] > 0) & (working["effect_analysis"].notna())].copy()

    n = int(working.shape[0])
    if n < 3:
        return {
            "status": "not_computed",
            "reason": "requires_at_least_3_studies_with_se",
            "n_studies": n,
            "min_studies": min_studies,
        }

    se = working["se"].astype(float).to_numpy()
    effect = working["effect_analysis"].astype(float).to_numpy()

    precision = 1.0 / se
    snd = effect / se

    if np.allclose(precision, precision[0]):
        return {
            "status": "not_computed",
            "reason": "precision_has_no_variance",
            "n_studies": n,
            "min_studies": min_studies,
        }

    design = np.column_stack([np.ones(n), precision])

    try:
        beta, _, rank, _ = np.linalg.lstsq(design, snd, rcond=None)
    except np.linalg.LinAlgError:
        return {
            "status": "not_computed",
            "reason": "linear_algebra_failure",
            "n_studies": n,
            "min_studies": min_studies,
        }

    if int(rank) < 2:
        return {
            "status": "not_computed",
            "reason": "rank_deficient_design",
            "n_studies": n,
            "min_studies": min_studies,
        }

    intercept = float(beta[0])
    slope = float(beta[1])
    residuals = snd - (design @ beta)
    df_resid = n - 2
    if df_resid <= 0:
        return {
            "status": "not_computed",
            "reason": "insufficient_residual_df",
            "n_studies": n,
            "min_studies": min_studies,
        }

    rss = float(np.sum(residuals**2))
    xtx = design.T @ design

    try:
        xtx_inv = np.linalg.inv(xtx)
    except np.linalg.LinAlgError:
        return {
            "status": "not_computed",
            "reason": "noninvertible_information_matrix",
            "n_studies": n,
            "min_studies": min_studies,
        }

    if rss <= 0:
        se_intercept = 0.0
        if intercept == 0:
            t_stat = 0.0
            p_value = 1.0
        else:
            t_stat = math.copysign(float("inf"), intercept)
            p_value = 0.0
        ci_lower = intercept
        ci_upper = intercept
    else:
        mse = rss / float(df_resid)
        var_intercept = float(mse * xtx_inv[0, 0])
        if var_intercept < 0:
            return {
                "status": "not_computed",
                "reason": "negative_intercept_variance",
                "n_studies": n,
                "min_studies": min_studies,
            }

        se_intercept = float(math.sqrt(var_intercept))
        if se_intercept <= 0:
            if intercept == 0:
                t_stat = 0.0
                p_value = 1.0
            else:
                t_stat = math.copysign(float("inf"), intercept)
                p_value = 0.0
            ci_lower = intercept
            ci_upper = intercept
        else:
            t_stat = intercept / se_intercept
            p_value = float(2.0 * stats.t.sf(abs(t_stat), df_resid))
            t_crit = float(stats.t.ppf(0.975, df_resid))
            ci_lower = intercept - t_crit * se_intercept
            ci_upper = intercept + t_crit * se_intercept

    return {
        "status": "computed",
        "reason": "",
        "n_studies": n,
        "min_studies": min_studies,
        "intercept": intercept,
        "slope": slope,
        "se_intercept": se_intercept,
        "t_stat": float(t_stat),
        "df": int(df_resid),
        "p_value": float(p_value),
        "ci_lower": float(ci_lower),
        "ci_upper": float(ci_upper),
        "low_power_flag": bool(n < min_studies),
    }


def pooled_effect_fixed(data_df: pd.DataFrame) -> float | None:
    if data_df.empty:
        return None

    working = data_df.copy()
    working["effect_analysis"] = pd.to_numeric(working["effect_analysis"], errors="coerce")
    working["se"] = pd.to_numeric(working["se"], errors="coerce")
    working = working[(working["effect_analysis"].notna()) & (working["se"].notna()) & (working["se"] > 0)].copy()
    if working.empty:
        return None

    effects = working["effect_analysis"].astype(float).to_numpy()
    ses = working["se"].astype(float).to_numpy()
    weights = 1.0 / (ses**2)
    total_weight = float(np.sum(weights))
    if total_weight <= 0:
        return None

    return float(np.sum(weights * effects) / total_weight)


def x_limits(data_df: pd.DataFrame, pooled: float | None) -> tuple[float, float]:
    if data_df.empty:
        return -1.0, 1.0

    effects = pd.to_numeric(data_df["effect_analysis"], errors="coerce").dropna().astype(float).tolist()
    se_values = pd.to_numeric(data_df["se"], errors="coerce").dropna().astype(float).tolist()

    if not effects:
        return -1.0, 1.0

    points = list(effects)
    if pooled is not None and se_values:
        max_se = max(se_values)
        points.extend([pooled - (1.96 * max_se), pooled + (1.96 * max_se), pooled])

    minimum = min(points)
    maximum = max(points)

    if minimum == maximum:
        minimum -= 0.5
        maximum += 0.5

    span = maximum - minimum
    padding = span * 0.08
    return minimum - padding, maximum + padding


def render_png(
    *,
    data_df: pd.DataFrame,
    metric: str,
    pooled_effect: float | None,
    output_path: Path,
) -> None:
    working = data_df.copy()
    working["effect_analysis"] = pd.to_numeric(working["effect_analysis"], errors="coerce")
    working["se"] = pd.to_numeric(working["se"], errors="coerce")
    working = working[(working["effect_analysis"].notna()) & (working["se"].notna()) & (working["se"] > 0)].copy()

    if working.empty:
        fig, ax = plt.subplots(figsize=(8, 3.5))
        ax.text(0.5, 0.5, "No studies with estimable standard errors for funnel plot", ha="center", va="center")
        ax.axis("off")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close(fig)
        return

    x = working["effect_analysis"].astype(float).to_numpy()
    se = working["se"].astype(float).to_numpy()
    labels = working["study_label"].astype(str).tolist()

    x_min, x_max = x_limits(working, pooled_effect)
    max_se = float(np.max(se))
    y_max = max(0.05, max_se * 1.08)

    fig, ax = plt.subplots(figsize=(8.8, 6.2))
    ax.scatter(x, se, s=28, color="#1f4e79", edgecolor="#1f4e79", alpha=0.92)

    if pooled_effect is not None and math.isfinite(pooled_effect):
        ax.axvline(pooled_effect, color="#2f2f2f", linestyle="-", linewidth=1.2)
        ax.plot(
            [pooled_effect - 1.96 * y_max, pooled_effect],
            [y_max, 0.0],
            linestyle="--",
            color="#7a7a7a",
            linewidth=1.0,
        )
        ax.plot(
            [pooled_effect + 1.96 * y_max, pooled_effect],
            [y_max, 0.0],
            linestyle="--",
            color="#7a7a7a",
            linewidth=1.0,
        )

    for index, label in enumerate(labels):
        ax.annotate(label, (x[index], se[index]), textcoords="offset points", xytext=(4, 3), fontsize=7)

    ax.set_xlim(x_min, x_max)
    ax.set_ylim(y_max, 0.0)
    ax.set_xlabel(analysis_metric_label(metric), fontsize=10)
    ax.set_ylabel("Standard error", fontsize=10)
    ax.set_title("Funnel plot (auto-generated)", fontsize=12)
    ax.grid(axis="both", color="#dddddd", linestyle="-", linewidth=0.6, alpha=0.7)
    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


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


def normalized_x(value: float, x_min: float, x_max: float) -> float:
    if x_max <= x_min:
        return 0.5
    return (value - x_min) / (x_max - x_min)


def normalized_y(se: float, max_se: float) -> float:
    if max_se <= 0:
        return 0.5
    return 1.0 - (se / max_se)


def render_tikz(
    *,
    data_df: pd.DataFrame,
    metric: str,
    pooled_effect: float | None,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines: list[str] = []
    lines.append("% Auto-generated by 03_analysis/publication_bias_assessment.py")
    lines.append(f"% Generated: {generated_at}")

    working = data_df.copy()
    working["effect_analysis"] = pd.to_numeric(working["effect_analysis"], errors="coerce")
    working["se"] = pd.to_numeric(working["se"], errors="coerce")
    working = working[(working["effect_analysis"].notna()) & (working["se"].notna()) & (working["se"] > 0)].copy()

    if working.empty:
        lines.append(r"\begin{tikzpicture}")
        lines.append(r"\node[anchor=west] at (0,0) {No studies with estimable standard errors for funnel plot.};")
        lines.append(r"\end{tikzpicture}")
        lines.append("")
        output_path.write_text("\n".join(lines), encoding="utf-8")
        return

    x_values = working["effect_analysis"].astype(float).to_numpy()
    se_values = working["se"].astype(float).to_numpy()
    labels = working["study_label"].astype(str).tolist()

    max_se = float(np.max(se_values))
    y_max = max(0.05, max_se * 1.08)
    x_min, x_max = x_limits(working, pooled_effect)

    plot_left = 0.08
    plot_right = 0.88
    plot_bottom = 0.12
    plot_top = 0.90
    plot_width = plot_right - plot_left
    plot_height = plot_top - plot_bottom

    lines.append(r"\begin{tikzpicture}[x=13cm,y=8cm]")
    lines.append(r"\node[anchor=west, font=\small\bfseries] at (0.0,0.98) {Funnel plot (auto-generated)};")
    lines.append(rf"\draw[->] ({plot_left:.6f},{plot_bottom:.6f}) -- ({plot_right + 0.03:.6f},{plot_bottom:.6f});")
    lines.append(rf"\draw[->] ({plot_left:.6f},{plot_bottom:.6f}) -- ({plot_left:.6f},{plot_top + 0.03:.6f});")

    for tick in [0.0, 0.25, 0.5, 0.75, 1.0]:
        tick_value = x_min + tick * (x_max - x_min)
        x_tick = plot_left + tick * plot_width
        lines.append(rf"\draw ({x_tick:.6f},{plot_bottom - 0.01:.6f}) -- ({x_tick:.6f},{plot_bottom + 0.01:.6f});")
        lines.append(rf"\node[anchor=north, font=\scriptsize] at ({x_tick:.6f},{plot_bottom - 0.014:.6f}) {{{latex_escape(f'{tick_value:.2f}')}}};")

    for tick in [0.0, 0.25, 0.5, 0.75, 1.0]:
        se_tick = tick * y_max
        y_norm = normalized_y(se_tick, y_max)
        y_tick = plot_bottom + y_norm * plot_height
        lines.append(rf"\draw ({plot_left - 0.008:.6f},{y_tick:.6f}) -- ({plot_left + 0.008:.6f},{y_tick:.6f});")
        lines.append(rf"\node[anchor=east, font=\scriptsize] at ({plot_left - 0.012:.6f},{y_tick:.6f}) {{{latex_escape(f'{se_tick:.2f}')}}};")

    lines.append(rf"\node[anchor=north, font=\scriptsize] at ({(plot_left + plot_right) / 2:.6f},{plot_bottom - 0.05:.6f}) {{{latex_escape(analysis_metric_label(metric))}}};")
    lines.append(rf"\node[anchor=south, rotate=90, font=\scriptsize] at ({plot_left - 0.06:.6f},{(plot_bottom + plot_top) / 2:.6f}) {{Standard error}};")

    if pooled_effect is not None and math.isfinite(pooled_effect):
        x_pool = plot_left + normalized_x(pooled_effect, x_min, x_max) * plot_width
        y_top_line = plot_bottom + normalized_y(0.0, y_max) * plot_height
        y_bottom_line = plot_bottom + normalized_y(y_max, y_max) * plot_height
        lines.append(rf"\draw[dashed, gray!70] ({x_pool:.6f},{y_bottom_line:.6f}) -- ({x_pool:.6f},{y_top_line:.6f});")

        left_boundary_bottom = pooled_effect - (1.96 * y_max)
        right_boundary_bottom = pooled_effect + (1.96 * y_max)
        x_left_bottom = plot_left + normalized_x(left_boundary_bottom, x_min, x_max) * plot_width
        x_right_bottom = plot_left + normalized_x(right_boundary_bottom, x_min, x_max) * plot_width
        lines.append(rf"\draw[dashed, gray!60] ({x_pool:.6f},{y_top_line:.6f}) -- ({x_left_bottom:.6f},{y_bottom_line:.6f});")
        lines.append(rf"\draw[dashed, gray!60] ({x_pool:.6f},{y_top_line:.6f}) -- ({x_right_bottom:.6f},{y_bottom_line:.6f});")

    for idx, (effect, se_val, label) in enumerate(zip(x_values, se_values, labels), start=1):
        x_point = plot_left + normalized_x(float(effect), x_min, x_max) * plot_width
        y_point = plot_bottom + normalized_y(float(se_val), y_max) * plot_height
        lines.append(rf"\filldraw[fill=blue!65, draw=blue!80!black] ({x_point:.6f},{y_point:.6f}) circle (0.0065);")
        if idx <= 12:
            lines.append(rf"\node[anchor=west, font=\tiny] at ({x_point + 0.010:.6f},{y_point + 0.005:.6f}) {{{latex_escape(label)}}};")

    lines.append(r"\end{tikzpicture}")
    lines.append("")
    output_path.write_text("\n".join(lines), encoding="utf-8")


def format_p_value(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return "NA"
    if value < 0.001:
        return "<0.001"
    return f"{value:.3f}"


def humanize_reason(reason: str) -> str:
    mapping = {
        "no_rows": "no eligible rows",
        "requires_at_least_3_studies_with_se": "requires at least 3 studies with estimable SE",
        "invalid_rank_correlation": "invalid rank correlation estimate",
        "precision_has_no_variance": "precision has no variance",
        "linear_algebra_failure": "linear algebra failure during regression",
        "rank_deficient_design": "rank-deficient regression design",
        "insufficient_residual_df": "insufficient residual degrees of freedom",
        "noninvertible_information_matrix": "non-invertible information matrix",
        "negative_intercept_variance": "negative estimated intercept variance",
    }
    if reason in mapping:
        return mapping[reason]
    return reason.replace("_", " ").strip()


def egger_interpretation(result: dict[str, object], *, min_studies: int) -> str:
    status = str(result.get("status", ""))
    if status != "computed":
        reason = str(result.get("reason", "not_computed"))
        return f"Egger's test not computed ({humanize_reason(reason)})."

    p_value = float(result.get("p_value", float("nan")))
    n_studies = int(result.get("n_studies", 0))
    if p_value < 0.05:
        message = "Egger intercept differs from zero (possible small-study effects/publication bias)."
    else:
        message = "No statistically significant funnel asymmetry detected by Egger test."

    if n_studies < min_studies:
        message += f" Interpret cautiously: only {n_studies} studies with estimable SE (<{min_studies})."

    return message


def begg_test(data_df: pd.DataFrame, *, min_studies: int) -> dict[str, object]:
    if data_df.empty:
        return {
            "status": "not_computed",
            "reason": "no_rows",
            "n_studies": 0,
            "min_studies": min_studies,
        }

    working = data_df.copy()
    working["effect_analysis"] = pd.to_numeric(working["effect_analysis"], errors="coerce")
    working["se"] = pd.to_numeric(working["se"], errors="coerce")
    working = working[(working["effect_analysis"].notna()) & (working["se"].notna()) & (working["se"] > 0)].copy()

    n = int(working.shape[0])
    if n < 3:
        return {
            "status": "not_computed",
            "reason": "requires_at_least_3_studies_with_se",
            "n_studies": n,
            "min_studies": min_studies,
        }

    tau, p_value = stats.kendalltau(
        working["effect_analysis"].astype(float).to_numpy(),
        working["se"].astype(float).to_numpy(),
    )

    if tau is None or p_value is None or not math.isfinite(float(p_value)):
        return {
            "status": "not_computed",
            "reason": "invalid_rank_correlation",
            "n_studies": n,
            "min_studies": min_studies,
        }

    return {
        "status": "computed",
        "reason": "",
        "n_studies": n,
        "min_studies": min_studies,
        "tau": float(tau),
        "p_value": float(p_value),
        "low_power_flag": bool(n < min_studies),
    }


def funnel_asymmetry_flag(egger_result: dict[str, object], begg_result: dict[str, object]) -> str:
    computed_p_values: list[float] = []
    low_power_flags: list[bool] = []

    if str(egger_result.get("status", "")) == "computed":
        p_egger = float(egger_result.get("p_value", float("nan")))
        if math.isfinite(p_egger):
            computed_p_values.append(p_egger)
            low_power_flags.append(bool(egger_result.get("low_power_flag", False)))

    if str(begg_result.get("status", "")) == "computed":
        p_begg = float(begg_result.get("p_value", float("nan")))
        if math.isfinite(p_begg):
            computed_p_values.append(p_begg)
            low_power_flags.append(bool(begg_result.get("low_power_flag", False)))

    if not computed_p_values:
        return "not_assessed"

    has_significant = any(value < 0.05 for value in computed_p_values)
    has_low_power = any(low_power_flags)

    if has_significant and has_low_power:
        return "possible_asymmetry_low_power"
    if has_significant:
        return "possible_asymmetry"
    if has_low_power:
        return "no_significant_asymmetry_low_power"
    return "no_significant_asymmetry"


def publication_bias_results_by_outcome(
    data_df: pd.DataFrame,
    *,
    min_studies_egger: int,
    min_studies_begg: int,
) -> pd.DataFrame:
    if data_df.empty:
        return pd.DataFrame(
            [
                {
                    "outcome": "overall",
                    "k_studies": 0,
                    "n_with_se": 0,
                    "egger_test_p": "",
                    "begg_test_p": "",
                    "funnel_asymmetry": "not_assessed",
                }
            ],
            columns=PUBLICATION_BIAS_RESULTS_COLUMNS,
        )

    working = data_df.copy()
    if "outcome" not in working.columns:
        working["outcome"] = "overall"
    working["outcome"] = working["outcome"].fillna("").astype(str).str.strip()
    working.loc[working["outcome"] == "", "outcome"] = "overall"

    rows: list[dict[str, object]] = []
    for outcome, group in working.groupby("outcome", sort=True):
        outcome_df = group.reset_index(drop=True)
        n_with_se = int(
            (
                pd.to_numeric(outcome_df.get("se", pd.Series(dtype=float)), errors="coerce")
                > 0
            ).sum()
        )

        egger_result = egger_regression(outcome_df, min_studies=min_studies_egger)
        begg_result = begg_test(outcome_df, min_studies=min_studies_begg)
        asymmetry = funnel_asymmetry_flag(egger_result, begg_result)

        egger_p = ""
        if str(egger_result.get("status", "")) == "computed":
            p_value = float(egger_result.get("p_value", float("nan")))
            if math.isfinite(p_value):
                egger_p = p_value

        begg_p = ""
        if str(begg_result.get("status", "")) == "computed":
            p_value = float(begg_result.get("p_value", float("nan")))
            if math.isfinite(p_value):
                begg_p = p_value

        rows.append(
            {
                "outcome": normalize(outcome),
                "k_studies": int(outcome_df.shape[0]),
                "n_with_se": n_with_se,
                "egger_test_p": egger_p,
                "begg_test_p": begg_p,
                "funnel_asymmetry": asymmetry,
            }
        )

    if not rows:
        return pd.DataFrame(columns=PUBLICATION_BIAS_RESULTS_COLUMNS)

    return pd.DataFrame(rows, columns=PUBLICATION_BIAS_RESULTS_COLUMNS)


def render_latex_table(
    *,
    metric: str,
    egger_result: dict[str, object],
    output_path: Path,
) -> None:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    interpretation = egger_interpretation(egger_result, min_studies=int(egger_result.get("min_studies", 10)))

    lines: list[str] = []
    lines.append("% Auto-generated by 03_analysis/publication_bias_assessment.py")
    lines.append(f"% Generated: {generated_at}")
    lines.append(r"\begin{table}[htbp]")
    lines.append(r"\centering")
    lines.append(r"\caption{Publication-bias assessment (funnel asymmetry and Egger regression)}")
    lines.append(r"\label{tab:publication_bias_assessment}")
    lines.append(r"\setlength{\tabcolsep}{4pt}")
    lines.append(r"\renewcommand{\arraystretch}{1.08}")
    lines.append(r"\small")
    lines.append(r"\begin{tabular}{p{0.31\textwidth}p{0.61\textwidth}}")
    lines.append(r"\toprule")
    lines.append(r"Item & Value \\")
    lines.append(r"\midrule")

    status = str(egger_result.get("status", ""))
    lines.append(rf"Metric for asymmetry assessment & {latex_escape(analysis_metric_label(metric))} \\")

    if status == "computed":
        lines.append(rf"Studies with estimable SE & {int(egger_result.get('n_studies', 0))} \\")
        lines.append(rf"Egger intercept & {float(egger_result.get('intercept', float('nan'))):.3f} \\")
        lines.append(
            rf"95\% CI (intercept) & [{float(egger_result.get('ci_lower', float('nan'))):.3f}, {float(egger_result.get('ci_upper', float('nan'))):.3f}] \\")
        lines.append(
            rf"Test statistic & $t({int(egger_result.get('df', 0))})={float(egger_result.get('t_stat', float('nan'))):.3f}$ \\")
        lines.append(rf"p-value & {latex_escape(format_p_value(float(egger_result.get('p_value', float('nan')))))} \\")
    else:
        lines.append(
            rf"Egger test status & {latex_escape(humanize_reason(str(egger_result.get('reason', 'not_computed'))))} \\")
        lines.append(rf"Studies with estimable SE & {int(egger_result.get('n_studies', 0))} \\")

    lines.append(rf"Interpretation & {latex_escape(interpretation)} \\")
    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")
    lines.append("")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


def build_summary(
    *,
    converted_input_path: Path,
    extraction_input_path: Path,
    metric: str,
    stats_dict: dict[str, int],
    data_df: pd.DataFrame,
    egger_result: dict[str, object],
    begg_result: dict[str, object],
    publication_bias_results_df: pd.DataFrame,
    data_output_path: Path,
    results_output_path: Path,
    png_output_path: Path,
    tikz_output_path: Path,
    table_output_path: Path,
    summary_output_path: Path,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    interpretation = egger_interpretation(egger_result, min_studies=int(egger_result.get("min_studies", 10)))

    lines: list[str] = []
    lines.append("# Publication Bias Assessment Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Converted input: `{converted_input_path.as_posix()}`")
    lines.append(f"- Extraction input: `{extraction_input_path.as_posix()}`")
    lines.append(f"- Metric: `{metric}` ({analysis_metric_label(metric)})")
    lines.append(f"- Data output: `{data_output_path.as_posix()}`")
    lines.append(f"- Results output: `{results_output_path.as_posix()}`")
    lines.append(f"- Funnel PNG output: `{png_output_path.as_posix()}`")
    lines.append(f"- Funnel TikZ output: `{tikz_output_path.as_posix()}`")
    lines.append(f"- Manuscript table output: `{table_output_path.as_posix()}`")
    lines.append(f"- Summary output: `{summary_output_path.as_posix()}`")
    lines.append("")

    lines.append("## Counts")
    lines.append("")
    lines.append(f"- Rows in converted CSV: {stats_dict['input_rows']}")
    lines.append(f"- Eligible converted rows: {stats_dict['eligible_rows']}")
    lines.append(f"- Rows retained for bias data table: {int(data_df.shape[0])}")
    lines.append(f"- Rows with converted raw CI: {stats_dict['with_raw_ci']}")
    lines.append(f"- Rows with approximate CI from sample size: {stats_dict['with_approx_ci']}")
    lines.append(f"- Rows with estimable SE: {stats_dict['with_se']}")
    lines.append(f"- Rows without estimable SE: {stats_dict['without_se']}")
    lines.append(f"- Outcome rows in results table: {int(publication_bias_results_df.shape[0])}")
    if stats_dict["trimmed_rows"] > 0:
        lines.append(f"- Rows trimmed by max-study limit: {stats_dict['trimmed_rows']}")

    lines.append("")
    lines.append("## Egger Regression")
    lines.append("")
    status = str(egger_result.get("status", ""))
    if status == "computed":
        lines.append("- Status: computed")
        lines.append(f"- Studies in test: {int(egger_result.get('n_studies', 0))}")
        lines.append(f"- Intercept: {float(egger_result.get('intercept', float('nan'))):.4f}")
        lines.append(
            f"- 95% CI: [{float(egger_result.get('ci_lower', float('nan'))):.4f}, {float(egger_result.get('ci_upper', float('nan'))):.4f}]"
        )
        lines.append(f"- t-statistic: {float(egger_result.get('t_stat', float('nan'))):.4f} (df={int(egger_result.get('df', 0))})")
        lines.append(f"- p-value: {format_p_value(float(egger_result.get('p_value', float('nan'))))}")
        lines.append(f"- Low-power flag (<{int(egger_result.get('min_studies', 10))} studies): {'yes' if bool(egger_result.get('low_power_flag', False)) else 'no'}")
    else:
        lines.append("- Status: not computed")
        lines.append(
            f"- Reason: {humanize_reason(str(egger_result.get('reason', 'not_computed')))}"
        )
        lines.append(f"- Studies with estimable SE: {int(egger_result.get('n_studies', 0))}")

    lines.append(f"- Interpretation: {interpretation}")

    lines.append("")
    lines.append("## Begg Rank Correlation")
    lines.append("")
    begg_status = str(begg_result.get("status", ""))
    if begg_status == "computed":
        lines.append("- Status: computed")
        lines.append(f"- Studies in test: {int(begg_result.get('n_studies', 0))}")
        lines.append(f"- Kendall tau: {float(begg_result.get('tau', float('nan'))):.4f}")
        lines.append(f"- p-value: {format_p_value(float(begg_result.get('p_value', float('nan'))))}")
        lines.append(f"- Low-power flag (<{int(begg_result.get('min_studies', 10))} studies): {'yes' if bool(begg_result.get('low_power_flag', False)) else 'no'}")
    else:
        lines.append("- Status: not computed")
        lines.append(
            f"- Reason: {humanize_reason(str(begg_result.get('reason', 'not_computed')))}"
        )
        lines.append(f"- Studies with estimable SE: {int(begg_result.get('n_studies', 0))}")

    lines.append("")
    lines.append("## Outcome-Level Results")
    lines.append("")
    if publication_bias_results_df.empty:
        lines.append("- No outcome-level publication bias results were generated.")
    else:
        lines.append("| outcome | k_studies | n_with_se | egger_test_p | begg_test_p | funnel_asymmetry |")
        lines.append("|---|---:|---:|---:|---:|---|")
        for _, row in publication_bias_results_df.iterrows():
            egger_p = numeric_or_none(row.get("egger_test_p", ""))
            begg_p = numeric_or_none(row.get("begg_test_p", ""))
            lines.append(
                "| "
                + f"{normalize(row.get('outcome', '')).replace('|', '\\|')} | "
                + f"{int(numeric_or_none(row.get('k_studies', '')) or 0)} | "
                + f"{int(numeric_or_none(row.get('n_with_se', '')) or 0)} | "
                + f"{format_p_value(egger_p)} | "
                + f"{format_p_value(begg_p)} | "
                + f"{normalize(row.get('funnel_asymmetry', '')).replace('|', '\\|')} |"
            )

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Funnel asymmetry is visualized on effect-size scale with standard error on y-axis (inverted).")
    lines.append("- Egger regression uses SND = effect/SE regressed on precision = 1/SE.")
    lines.append("- This is a screening tool for small-study effects, not definitive proof of publication bias.")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run publication-bias assessment: funnel plot + Egger's regression test."
    )
    parser.add_argument(
        "--input",
        default="outputs/effect_size_converted.csv",
        help="Path to converted effect-size CSV.",
    )
    parser.add_argument(
        "--extraction",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction template CSV (for CI/sample-size metadata).",
    )
    parser.add_argument(
        "--metric",
        default="converted_d",
        choices=SUPPORTED_METRICS,
        help="Effect metric column to assess for asymmetry.",
    )
    parser.add_argument(
        "--min-studies-egger",
        type=int,
        default=10,
        help="Threshold used to flag low-power Egger interpretation (default: 10).",
    )
    parser.add_argument(
        "--max-studies",
        type=int,
        default=0,
        help="Maximum number of rows retained for assessment (0 = all).",
    )
    parser.add_argument(
        "--data-output",
        default="outputs/publication_bias_data.csv",
        help="Path to prepared publication-bias data CSV output.",
    )
    parser.add_argument(
        "--results-output",
        default="outputs/publication_bias_results.csv",
        help="Path to outcome-level publication-bias results CSV output.",
    )
    parser.add_argument(
        "--png-output",
        default="outputs/publication_bias_funnel.png",
        help="Path to funnel plot PNG output.",
    )
    parser.add_argument(
        "--tikz-output",
        default="outputs/publication_bias_funnel.tikz",
        help="Path to funnel plot TikZ output.",
    )
    parser.add_argument(
        "--table-output",
        default="../04_manuscript/tables/publication_bias_assessment_table.tex",
        help="Path to manuscript-ready publication-bias LaTeX table output.",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/publication_bias_summary.md",
        help="Path to markdown summary output.",
    )
    args = parser.parse_args()

    converted_input_path = Path(args.input)
    extraction_input_path = Path(args.extraction)
    data_output_path = Path(args.data_output)
    results_output_path = Path(args.results_output)
    png_output_path = Path(args.png_output)
    tikz_output_path = Path(args.tikz_output)
    table_output_path = Path(args.table_output)
    summary_output_path = Path(args.summary_output)

    converted_df = read_csv_or_empty(converted_input_path)
    extraction_df = read_csv_or_empty(extraction_input_path)

    data_df, stats_dict = prepare_bias_data(
        converted_df,
        extraction_df,
        metric=args.metric,
        max_studies=max(0, int(args.max_studies)),
    )

    egger_result = egger_regression(data_df, min_studies=max(3, int(args.min_studies_egger)))
    begg_result = begg_test(data_df, min_studies=max(3, int(args.min_studies_egger)))
    publication_bias_results_df = publication_bias_results_by_outcome(
        data_df,
        min_studies_egger=max(3, int(args.min_studies_egger)),
        min_studies_begg=max(3, int(args.min_studies_egger)),
    )
    pooled = pooled_effect_fixed(data_df)

    data_output_path.parent.mkdir(parents=True, exist_ok=True)
    data_df.to_csv(data_output_path, index=False)
    results_output_path.parent.mkdir(parents=True, exist_ok=True)
    publication_bias_results_df.to_csv(results_output_path, index=False)

    render_png(
        data_df=data_df,
        metric=args.metric,
        pooled_effect=pooled,
        output_path=png_output_path,
    )
    render_tikz(
        data_df=data_df,
        metric=args.metric,
        pooled_effect=pooled,
        output_path=tikz_output_path,
    )
    render_latex_table(
        metric=args.metric,
        egger_result=egger_result,
        output_path=table_output_path,
    )

    summary_text = build_summary(
        converted_input_path=converted_input_path,
        extraction_input_path=extraction_input_path,
        metric=args.metric,
        stats_dict=stats_dict,
        data_df=data_df,
        egger_result=egger_result,
        begg_result=begg_result,
        publication_bias_results_df=publication_bias_results_df,
        data_output_path=data_output_path,
        results_output_path=results_output_path,
        png_output_path=png_output_path,
        tikz_output_path=tikz_output_path,
        table_output_path=table_output_path,
        summary_output_path=summary_output_path,
    )
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(summary_text, encoding="utf-8")

    print(f"Wrote: {data_output_path}")
    print(f"Wrote: {results_output_path}")
    print(f"Wrote: {png_output_path}")
    print(f"Wrote: {tikz_output_path}")
    print(f"Wrote: {table_output_path}")
    print(f"Wrote: {summary_output_path}")
    print(f"Rows with estimable SE: {stats_dict['with_se']}")
    print(f"Egger status: {egger_result.get('status', 'not_computed')}")
    print(f"Begg status: {begg_result.get('status', 'not_computed')}")


if __name__ == "__main__":
    main()
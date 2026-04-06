import argparse
import math
import re
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
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

METRIC_LABELS = {
    "converted_d": "Standardized mean difference (d)",
    "converted_r": "Correlation (r)",
    "fisher_z": "Fisher z",
    "converted_or": "Odds ratio (OR)",
}

TARGET_NULL_VALUE = {
    "converted_d": 0.0,
    "converted_r": 0.0,
    "fisher_z": 0.0,
    "converted_or": 1.0,
}

METRIC_ALIASES = {
    "r": {
        "r",
        "pearson_r",
        "pearsonr",
        "corr",
        "correlation",
        "correlation_coefficient",
        "rho",
    },
    "d": {
        "d",
        "cohen_d",
        "cohens_d",
        "cohen_s_d",
        "hedges_g",
        "g",
        "smd",
        "standardized_mean_difference",
    },
    "or": {
        "or",
        "odds_ratio",
        "oddsratio",
    },
    "eta2": {
        "eta2",
        "eta_2",
        "eta^2",
        "eta_squared",
        "eta_sq",
        "eta²",
        "partial_eta2",
        "partial_eta_2",
        "partial_eta^2",
        "partial_eta_squared",
        "partial_eta_sq",
        "partial_eta²",
    },
}

EXTRACTION_METADATA_COLUMNS = (
    "ci_lower",
    "ci_upper",
    "sample_size",
    "effect_direction",
)

EFFECT_CANONICAL_COLUMNS = (
    "main_effect_metric",
    "main_effect_value",
)

EFFECT_COLUMN_ALIASES = {
    "main_effect_metric": ("effect_measure", "effect_metric"),
    "main_effect_value": ("effect_value",),
}


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def normalize_lower(value: object) -> str:
    return normalize(value).lower()


def is_missing(value: object) -> bool:
    return normalize_lower(value) in MISSING_CODES


def missing_required_columns(df: pd.DataFrame, required_columns: tuple[str, ...]) -> list[str]:
    return [column for column in required_columns if column not in df.columns]


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


def canonical_metric(raw_metric: object) -> str | None:
    text = normalize_lower(raw_metric)
    if not text:
        return None

    normalized = text.replace("-", "_").replace(" ", "_").replace("²", "2")

    for metric_key, aliases in METRIC_ALIASES.items():
        if text in aliases or normalized in aliases:
            return metric_key

    if "odds" in normalized or normalized == "or":
        return "or"
    if "eta" in normalized and "2" in normalized:
        return "eta2"
    if "cohen" in normalized or "hedges" in normalized or normalized in {"d", "g", "smd"}:
        return "d"
    if normalized in {"r", "corr", "correlation", "rho"}:
        return "r"

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


def build_study_label(study_id: str, first_author: str, year: str) -> str:
    if first_author and year:
        return f"{first_author} ({year}) [{study_id}]"
    if first_author:
        return f"{first_author} [{study_id}]"
    return study_id


def build_effect_text(effect: float, ci_lower: float | None, ci_upper: float | None, metric: str) -> str:
    if metric == "converted_or":
        value_text = f"{effect:.2f}"
    else:
        value_text = f"{effect:.3f}"

    if ci_lower is None or ci_upper is None:
        return value_text

    if metric == "converted_or":
        return f"{value_text} [{ci_lower:.2f}, {ci_upper:.2f}]"
    return f"{value_text} [{ci_lower:.3f}, {ci_upper:.3f}]"


def parse_confidence_interval_bounds(value: object) -> tuple[float | None, float | None]:
    text = normalize(value)
    if is_missing(text):
        return None, None

    numbers = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", text)
    if len(numbers) < 2:
        return None, None

    lower = numeric_or_none(numbers[0])
    upper = numeric_or_none(numbers[1])
    if lower is None or upper is None:
        return None, None

    return (lower, upper) if lower <= upper else (upper, lower)


def effect_direction_from_value(value: object) -> str:
    numeric = numeric_or_none(value)
    if numeric is None:
        return ""
    if numeric > 0:
        return "positive"
    if numeric < 0:
        return "negative"
    return "null"


def harmonize_extraction_metadata(extraction_df: pd.DataFrame) -> pd.DataFrame:
    if extraction_df.empty:
        return extraction_df

    extraction = extraction_df.copy()

    if "author" in extraction.columns and "first_author" not in extraction.columns:
        extraction["first_author"] = extraction["author"]

    for canonical_column in EFFECT_CANONICAL_COLUMNS:
        if canonical_column not in extraction.columns:
            extraction[canonical_column] = ""

    for canonical_column, aliases in EFFECT_COLUMN_ALIASES.items():
        for alias in aliases:
            if alias not in extraction.columns:
                continue
            target_series = extraction[canonical_column].fillna("").astype(str).str.strip()
            alias_series = extraction[alias].fillna("").astype(str)
            mask = target_series.eq("") & alias_series.str.strip().ne("")
            if mask.any():
                extraction.loc[mask, canonical_column] = alias_series.loc[mask]

    for column in EXTRACTION_METADATA_COLUMNS:
        if column not in extraction.columns:
            extraction[column] = ""

    if "study_id" not in extraction.columns:
        extraction["study_id"] = ""

    for index, row in extraction.iterrows():
        ci_lower = row.get("ci_lower", "")
        ci_upper = row.get("ci_upper", "")
        if is_missing(ci_lower) or is_missing(ci_upper):
            lower, upper = parse_confidence_interval_bounds(row.get("confidence_interval", ""))
            if lower is not None and is_missing(ci_lower):
                extraction.at[index, "ci_lower"] = str(lower)
            if upper is not None and is_missing(ci_upper):
                extraction.at[index, "ci_upper"] = str(upper)

        if is_missing(row.get("effect_direction", "")):
            extraction.at[index, "effect_direction"] = effect_direction_from_value(row.get("main_effect_value", ""))

    return extraction


def build_converted_from_extraction(extraction_df: pd.DataFrame, *, metric: str) -> pd.DataFrame:
    if extraction_df.empty:
        return pd.DataFrame()

    required = {"study_id", "main_effect_metric", "main_effect_value"}
    if not required.issubset(extraction_df.columns):
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    for index, row in extraction_df.iterrows():
        study_id = normalize(row.get("study_id", ""))
        if not study_id:
            continue

        source_metric = canonical_metric(row.get("main_effect_metric", ""))
        source_value = numeric_or_none(row.get("main_effect_value", ""))
        if source_metric is None or source_value is None:
            continue

        effect_direction = normalize_lower(row.get("effect_direction", ""))
        if not effect_direction:
            effect_direction = effect_direction_from_value(source_value)

        converted = convert_source_value(
            source_metric=source_metric,
            source_value=source_value,
            direction_sign=signed_direction(effect_direction),
            target_metric=metric,
        )
        if converted is None:
            continue

        first_author = normalize(row.get("first_author", ""))
        if not first_author:
            first_author = normalize(row.get("author", ""))

        rows.append(
            {
                "row": str(int(index) + 2),
                "study_id": study_id,
                "first_author": first_author,
                "year": normalize(row.get("year", "")),
                "source_metric_canonical": source_metric,
                "effect_direction": effect_direction,
                "conversion_status": "converted",
                metric: float(converted),
            }
        )

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def extraction_unit_mismatch_issues(extraction_df: pd.DataFrame) -> list[str]:
    issues: list[str] = []
    if extraction_df.empty:
        return issues

    working = extraction_df.copy()
    if "study_id" not in working.columns:
        working["study_id"] = ""
    for column in ("main_effect_metric", "main_effect_value", "ci_lower", "ci_upper"):
        if column not in working.columns:
            working[column] = ""

    for index, row in working.iterrows():
        metric_raw = normalize(row.get("main_effect_metric", ""))
        source_metric = canonical_metric(metric_raw)
        if source_metric is None:
            continue

        row_label = f"row {index + 2}"
        study_id = normalize(row.get("study_id", ""))
        if study_id:
            row_label = f"{row_label} ({study_id})"

        value_map = {
            "main_effect_value": numeric_or_none(row.get("main_effect_value", "")),
            "ci_lower": numeric_or_none(row.get("ci_lower", "")),
            "ci_upper": numeric_or_none(row.get("ci_upper", "")),
        }

        for column_name, numeric_value in value_map.items():
            if numeric_value is None:
                continue

            if source_metric == "r" and (numeric_value < -1.0 or numeric_value > 1.0):
                issues.append(
                    f"{row_label}: `{column_name}`={numeric_value} outside [-1, 1] for metric `{metric_raw}`."
                )
            elif source_metric == "eta2" and (numeric_value < 0.0 or numeric_value > 1.0):
                issues.append(
                    f"{row_label}: `{column_name}`={numeric_value} outside [0, 1] for metric `{metric_raw}`."
                )
            elif source_metric == "or" and numeric_value <= 0.0:
                issues.append(
                    f"{row_label}: `{column_name}`={numeric_value} must be > 0 for metric `{metric_raw}`."
                )

    return issues


def prepare_plot_data(
    converted_df: pd.DataFrame,
    extraction_df: pd.DataFrame,
    *,
    metric: str,
    max_studies: int,
) -> tuple[pd.DataFrame, dict[str, int]]:
    stats = {
        "input_rows": 0,
        "eligible_rows": 0,
        "with_raw_ci": 0,
        "with_approx_ci": 0,
        "without_ci": 0,
        "trimmed_rows": 0,
    }

    required_converted = {
        "study_id",
        "first_author",
        "year",
        "source_metric_canonical",
        "effect_direction",
        "conversion_status",
        metric,
    }
    if converted_df.empty or not required_converted.issubset(converted_df.columns):
        return pd.DataFrame(), stats

    stats["input_rows"] = int(converted_df.shape[0])

    working = converted_df.copy()
    working["conversion_status"] = working["conversion_status"].fillna("").astype(str).str.strip().str.lower()
    working = working[working["conversion_status"].isin({"converted", "partial"})].copy()

    working["effect_value"] = pd.to_numeric(working[metric], errors="coerce")
    working = working[working["effect_value"].notna()].copy()
    if working.empty:
        return pd.DataFrame(), stats

    stats["eligible_rows"] = int(working.shape[0])

    metadata_by_row: dict[int, dict[str, object]] = {}
    metadata_by_study: dict[str, dict[str, object]] = {}
    if not extraction_df.empty:
        extraction = extraction_df.copy()
        for column in EXTRACTION_METADATA_COLUMNS:
            if column not in extraction.columns:
                extraction[column] = ""
        if "study_id" not in extraction.columns:
            extraction["study_id"] = ""

        for idx, row in extraction.iterrows():
            metadata = {
                "ci_lower": row.get("ci_lower", ""),
                "ci_upper": row.get("ci_upper", ""),
                "sample_size": row.get("sample_size", ""),
                "effect_direction": row.get("effect_direction", ""),
            }
            metadata_by_row[int(idx)] = metadata
            study_id_key = normalize(row.get("study_id", ""))
            if study_id_key:
                metadata_by_study[study_id_key] = metadata

    rows: list[dict[str, object]] = []
    for _, row in working.iterrows():
        source_metric = normalize_lower(row.get("source_metric_canonical", ""))
        if source_metric not in {"r", "d", "or", "eta2"}:
            source_metric = ""

        study_id = normalize(row.get("study_id", ""))
        first_author = normalize(row.get("first_author", ""))
        year = normalize(row.get("year", ""))
        direction_sign = signed_direction(row.get("effect_direction", ""))
        effect_value = float(row["effect_value"])

        extraction_index = parse_row_index(row.get("row", ""))
        ci_lower = None
        ci_upper = None
        ci_source = "none"

        metadata: dict[str, object] = {}
        if extraction_index is not None:
            metadata = metadata_by_row.get(extraction_index, {})
        if not metadata and study_id:
            metadata = metadata_by_study.get(study_id, {})
        raw_ci_lower = numeric_or_none(metadata.get("ci_lower", ""))
        raw_ci_upper = numeric_or_none(metadata.get("ci_upper", ""))
        raw_sample_size = parse_sample_size(metadata.get("sample_size", ""))
        if direction_sign is None:
            direction_sign = signed_direction(metadata.get("effect_direction", ""))

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
                stats["with_raw_ci"] += 1

        if ci_lower is None or ci_upper is None:
            approx = approximate_ci(metric, effect_value, raw_sample_size)
            if approx is not None:
                ci_lower, ci_upper = approx
                ci_source = "approx_sample"
                stats["with_approx_ci"] += 1

        if ci_lower is None or ci_upper is None:
            stats["without_ci"] += 1

        rows.append(
            {
                "study_id": study_id,
                "first_author": first_author,
                "year": year,
                "study_label": build_study_label(study_id, first_author, year),
                "metric": metric,
                "effect": effect_value,
                "ci_lower": ci_lower,
                "ci_upper": ci_upper,
                "ci_source": ci_source,
                "source_metric_canonical": source_metric,
                "sample_size": raw_sample_size if raw_sample_size is not None else "",
                "effect_text": build_effect_text(effect_value, ci_lower, ci_upper, metric),
            }
        )

    plot_df = pd.DataFrame(rows)
    if plot_df.empty:
        return plot_df, stats

    plot_df = plot_df.sort_values(["effect", "study_label"], kind="stable").reset_index(drop=True)
    if plot_df.shape[0] > max_studies:
        stats["trimmed_rows"] = int(plot_df.shape[0] - max_studies)
        plot_df = plot_df.head(max_studies).copy()

    return plot_df, stats


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


def x_limits_for_plot(plot_df: pd.DataFrame, null_value: float, metric: str) -> tuple[float, float]:
    if plot_df.empty:
        if metric == "converted_or":
            return 0.5, 2.0
        return -1.0, 1.0

    points = plot_df["effect"].astype(float).tolist()
    ci_lower_values = [float(value) for value in plot_df["ci_lower"].dropna().tolist()]
    ci_upper_values = [float(value) for value in plot_df["ci_upper"].dropna().tolist()]

    min_value = min(points + ci_lower_values + [null_value])
    max_value = max(points + ci_upper_values + [null_value])
    if min_value == max_value:
        if metric == "converted_or":
            min_value *= 0.8
            max_value *= 1.2
        else:
            min_value -= 0.5
            max_value += 0.5

    span = max_value - min_value
    padding = span * 0.08

    x_min = min_value - padding
    x_max = max_value + padding

    if metric == "converted_or":
        x_min = max(1e-6, x_min)

    return x_min, x_max


def render_png(
    *,
    plot_df: pd.DataFrame,
    metric: str,
    output_path: Path,
) -> None:
    if plot_df.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No effect-size rows available for forest plot", ha="center", va="center")
        ax.axis("off")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close(fig)
        return

    null_value = TARGET_NULL_VALUE[metric]
    x_min, x_max = x_limits_for_plot(plot_df, null_value, metric)

    n_studies = int(plot_df.shape[0])
    fig_height = max(4.5, 1.8 + 0.45 * n_studies)
    fig, ax = plt.subplots(figsize=(10.5, fig_height))

    y_positions = list(range(n_studies))

    for y_pos, (_, row) in zip(y_positions, plot_df.iterrows()):
        effect = float(row["effect"])
        ci_lower = row["ci_lower"]
        ci_upper = row["ci_upper"]

        if pd.notna(ci_lower) and pd.notna(ci_upper):
            ax.hlines(y=y_pos, xmin=float(ci_lower), xmax=float(ci_upper), color="#4c4c4c", linewidth=1.5)

        marker_size = 6.5 if row["ci_source"] == "converted_raw_ci" else 5.5
        ax.plot(effect, y_pos, marker="s", markersize=marker_size, color="#1f4e79", markeredgecolor="#1f4e79")

    ax.axvline(null_value, color="#2f2f2f", linestyle="--", linewidth=1.2)
    ax.set_xlim(x_min, x_max)
    ax.set_yticks(y_positions)
    ax.set_yticklabels(plot_df["study_label"].tolist(), fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel(METRIC_LABELS[metric], fontsize=10)
    ax.set_title("Forest plot (auto-generated)", fontsize=12)
    ax.grid(axis="x", color="#d9d9d9", linestyle="-", linewidth=0.7, alpha=0.7)

    for y_pos, text in zip(y_positions, plot_df["effect_text"].tolist()):
        ax.text(x_max, y_pos, text, va="center", ha="left", fontsize=8, color="#2f2f2f")

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def latex_escape(value: str) -> str:
    text = value.replace("\\", "\\textbackslash{}")
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


def render_tikz(
    *,
    plot_df: pd.DataFrame,
    metric: str,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines: list[str] = []
    lines.append("% Auto-generated by 03_analysis/forest_plot_generator.py")
    lines.append(f"% Generated: {generated_at}")

    if plot_df.empty:
        lines.append(r"\begin{tikzpicture}")
        lines.append(r"\node[anchor=west] at (0,0) {No effect-size rows available for forest plot.};")
        lines.append(r"\end{tikzpicture}")
        lines.append("")
        output_path.write_text("\n".join(lines), encoding="utf-8")
        return

    null_value = TARGET_NULL_VALUE[metric]
    x_min, x_max = x_limits_for_plot(plot_df, null_value, metric)

    n = int(plot_df.shape[0])
    y_step = 0.65
    y_top = 0.7 + n * y_step
    axis_y = 0.25

    lines.append(r"\begin{tikzpicture}[x=12cm,y=1cm]")
    lines.append(rf"\node[anchor=west, font=\small\bfseries] at (0,{y_top + 0.55:.3f}) {{Forest plot (auto-generated)}};")
    lines.append(rf"\draw[->] (0,{axis_y:.3f}) -- (1.03,{axis_y:.3f});")

    null_x = normalized_x(null_value, x_min, x_max)
    lines.append(rf"\draw[dashed, gray!70] ({null_x:.6f},{axis_y:.3f}) -- ({null_x:.6f},{y_top:.3f});")

    tick_positions = [0.0, 0.25, 0.5, 0.75, 1.0]
    for tick in tick_positions:
        tick_value = x_min + tick * (x_max - x_min)
        lines.append(rf"\draw ({tick:.6f},{axis_y - 0.03:.3f}) -- ({tick:.6f},{axis_y + 0.03:.3f});")
        if metric == "converted_or":
            label = f"{tick_value:.2f}"
        else:
            label = f"{tick_value:.2f}"
        lines.append(rf"\node[anchor=north, font=\scriptsize] at ({tick:.6f},{axis_y - 0.05:.3f}) {{{latex_escape(label)}}};")

    lines.append(rf"\node[anchor=north west, font=\scriptsize] at (1.05,{axis_y - 0.05:.3f}) {{{latex_escape(METRIC_LABELS[metric])}}};")

    for idx, (_, row) in enumerate(plot_df.iterrows(), start=1):
        y = y_top - idx * y_step + 0.3
        effect = float(row["effect"])
        x_effect = normalized_x(effect, x_min, x_max)

        ci_lower = row["ci_lower"]
        ci_upper = row["ci_upper"]
        if pd.notna(ci_lower) and pd.notna(ci_upper):
            x_ci_lower = normalized_x(float(ci_lower), x_min, x_max)
            x_ci_upper = normalized_x(float(ci_upper), x_min, x_max)
            lines.append(rf"\draw[thick, gray!70] ({x_ci_lower:.6f},{y:.3f}) -- ({x_ci_upper:.6f},{y:.3f});")

        square_half = 0.008 if row["ci_source"] == "converted_raw_ci" else 0.006
        lines.append(
            rf"\filldraw[fill=blue!60, draw=blue!80!black] ({x_effect - square_half:.6f},{y - square_half:.3f}) rectangle ({x_effect + square_half:.6f},{y + square_half:.3f});"
        )

        study_label = latex_escape(str(row["study_label"]))
        effect_text = latex_escape(str(row["effect_text"]))
        lines.append(rf"\node[anchor=east, font=\scriptsize] at (-0.02,{y:.3f}) {{{study_label}}};")
        lines.append(rf"\node[anchor=west, font=\scriptsize] at (1.05,{y:.3f}) {{{effect_text}}};")

    lines.append(r"\end{tikzpicture}")
    lines.append("")
    output_path.write_text("\n".join(lines), encoding="utf-8")


def build_summary(
    *,
    converted_input_path: Path,
    extraction_input_path: Path,
    metric: str,
    stats: dict[str, int],
    plot_df: pd.DataFrame,
    png_output_path: Path,
    tikz_output_path: Path,
    data_output_path: Path,
    summary_output_path: Path,
    converted_from_extraction: bool,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = []
    lines.append("# Forest Plot Generator Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Converted input: `{converted_input_path.as_posix()}`")
    lines.append(f"- Extraction input: `{extraction_input_path.as_posix()}`")
    if converted_from_extraction:
        lines.append(
            "- Converted rows source: fallback generated from extraction table (`main_effect_metric` + `main_effect_value`; legacy aliases are harmonized)."
        )
    lines.append(f"- Metric: `{metric}` ({METRIC_LABELS[metric]})")
    metadata_columns_literal = ", ".join(f"`{column}`" for column in EXTRACTION_METADATA_COLUMNS)
    lines.append(f"- Required extraction metadata columns: {metadata_columns_literal}")
    lines.append(f"- Data output: `{data_output_path.as_posix()}`")
    lines.append(f"- PNG output: `{png_output_path.as_posix()}`")
    lines.append(f"- TikZ output: `{tikz_output_path.as_posix()}`")
    lines.append(f"- Summary output: `{summary_output_path.as_posix()}`")
    lines.append("")

    lines.append("## Counts")
    lines.append("")
    lines.append(f"- Rows in converted CSV: {stats['input_rows']}")
    lines.append(f"- Eligible converted rows: {stats['eligible_rows']}")
    lines.append(f"- Plotted studies: {int(plot_df.shape[0])}")
    lines.append(f"- CIs from converted raw CI columns: {stats['with_raw_ci']}")
    lines.append(f"- CIs approximated from sample size: {stats['with_approx_ci']}")
    lines.append(f"- Rows without CI: {stats['without_ci']}")
    if stats["trimmed_rows"] > 0:
        lines.append(f"- Rows trimmed by max-study limit: {stats['trimmed_rows']}")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    if converted_from_extraction:
        lines.append(
            "- Point estimates are read from extraction (`main_effect_metric`, `main_effect_value`; legacy aliases are harmonized) and converted to the selected metric."
        )
    else:
        lines.append("- Point estimates are read from `effect_size_converted.csv` selected metric column.")
    lines.append("- CI preference: converted raw CI bounds (if available) → sample-size approximation fallback.")
    lines.append("- Use PNG for direct manuscripts and TikZ for TeX-native figure embedding/customization.")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate publication-ready forest plot from converted effect sizes (PNG + TikZ)."
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
        choices=["converted_d", "converted_r", "fisher_z", "converted_or"],
        help="Effect metric column to plot.",
    )
    parser.add_argument(
        "--max-studies",
        type=int,
        default=25,
        help="Maximum number of studies to display in one plot.",
    )
    parser.add_argument(
        "--png-output",
        default="outputs/forest_plot.png",
        help="Path to PNG forest plot output.",
    )
    parser.add_argument(
        "--tikz-output",
        default="outputs/forest_plot.tikz",
        help="Path to TikZ forest plot output.",
    )
    parser.add_argument(
        "--data-output",
        default="outputs/forest_plot_data.csv",
        help="Path to prepared plotting data CSV output.",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/forest_plot_summary.md",
        help="Path to markdown summary output.",
    )
    args = parser.parse_args()

    converted_input_path = Path(args.input)
    extraction_input_path = Path(args.extraction)
    png_output_path = Path(args.png_output)
    tikz_output_path = Path(args.tikz_output)
    data_output_path = Path(args.data_output)
    summary_output_path = Path(args.summary_output)

    converted_df = read_csv_or_empty(converted_input_path)
    extraction_df = harmonize_extraction_metadata(read_csv_or_empty(extraction_input_path))

    required_converted_columns = {
        "study_id",
        "first_author",
        "year",
        "source_metric_canonical",
        "effect_direction",
        "conversion_status",
        args.metric,
    }
    converted_from_extraction = False
    fallback_converted = build_converted_from_extraction(extraction_df, metric=args.metric)
    if not fallback_converted.empty:
        converted_df = fallback_converted
        converted_from_extraction = True
    elif converted_df.empty or not required_converted_columns.issubset(converted_df.columns):
        converted_df = pd.DataFrame()

    missing_metadata_columns = missing_required_columns(extraction_df, EXTRACTION_METADATA_COLUMNS)
    if missing_metadata_columns:
        missing_literal = ", ".join(missing_metadata_columns)
        raise SystemExit(
            "Extraction schema mismatch for forest plot metadata: missing columns "
            f"{missing_literal} in {extraction_input_path.as_posix()}."
        )

    unit_mismatch_issues = extraction_unit_mismatch_issues(extraction_df)
    if unit_mismatch_issues:
        preview = "\n".join(f"- {issue}" for issue in unit_mismatch_issues[:10])
        extra_count = len(unit_mismatch_issues) - 10
        suffix = ""
        if extra_count > 0:
            suffix = f"\n- ... and {extra_count} more issue(s)."
        raise SystemExit(
            "Extraction metric/CI unit mismatch for forest plot (check metric scale consistency):\n"
            f"{preview}{suffix}"
        )

    plot_df, stats = prepare_plot_data(
        converted_df,
        extraction_df,
        metric=args.metric,
        max_studies=max(1, int(args.max_studies)),
    )

    data_output_path.parent.mkdir(parents=True, exist_ok=True)
    plot_df.to_csv(data_output_path, index=False)

    render_png(plot_df=plot_df, metric=args.metric, output_path=png_output_path)
    render_tikz(plot_df=plot_df, metric=args.metric, output_path=tikz_output_path)

    summary_text = build_summary(
        converted_input_path=converted_input_path,
        extraction_input_path=extraction_input_path,
        metric=args.metric,
        stats=stats,
        plot_df=plot_df,
        png_output_path=png_output_path,
        tikz_output_path=tikz_output_path,
        data_output_path=data_output_path,
        summary_output_path=summary_output_path,
        converted_from_extraction=converted_from_extraction,
    )
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(summary_text, encoding="utf-8")

    print(f"Wrote: {data_output_path}")
    print(f"Wrote: {png_output_path}")
    print(f"Wrote: {tikz_output_path}")
    print(f"Wrote: {summary_output_path}")
    print(f"Plotted studies: {plot_df.shape[0]}")


if __name__ == "__main__":
    main()
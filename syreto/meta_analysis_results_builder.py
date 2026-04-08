import argparse
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import os
import tempfile

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

SUPPORTED_METRICS = ["converted_d", "converted_r", "fisher_z", "converted_or"]
SUPPORTED_SOURCE_METRICS = {"r", "d", "or", "eta2"}

RESULT_COLUMNS = [
    "outcome",
    "k_studies",
    "pooled_effect",
    "ci_low",
    "ci_high",
    "p_value",
    "i2",
    "tau2",
    "model",
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


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def normalize_lower(value: object) -> str:
    return normalize(value).lower()


def is_missing(value: object) -> bool:
    return normalize_lower(value) in MISSING_CODES


def numeric_or_none(value: object) -> float | None:
    text = normalize(value)
    if is_missing(text):
        return None

    parsed = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


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

    sample_size = int(round(numeric))
    return sample_size if sample_size > 0 else None


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
    fisher_z = math.atanh(converted_r) if abs(converted_r) < 1.0 else None
    return {
        "converted_r": converted_r,
        "converted_d": value_d,
        "converted_or": converted_or,
        "fisher_z": fisher_z,
    }


def convert_from_or(value_or: float) -> dict[str, float | None]:
    if value_or <= 0:
        return {
            "converted_r": None,
            "converted_d": None,
            "converted_or": None,
            "fisher_z": None,
        }

    converted_d = math.log(value_or) * math.sqrt(3.0) / math.pi
    converted_r = converted_d / math.sqrt((converted_d * converted_d) + 4.0)
    fisher_z = math.atanh(converted_r) if abs(converted_r) < 1.0 else None
    return {
        "converted_r": converted_r,
        "converted_d": converted_d,
        "converted_or": value_or,
        "fisher_z": fisher_z,
    }


def convert_from_eta2(value_eta2: float, direction_sign: int | None) -> dict[str, float | None]:
    if value_eta2 < 0:
        return {
            "converted_r": None,
            "converted_d": None,
            "converted_or": None,
            "fisher_z": None,
        }

    if value_eta2 == 0:
        return {
            "converted_r": 0.0,
            "converted_d": 0.0,
            "converted_or": 1.0,
            "fisher_z": 0.0,
        }

    if direction_sign is None:
        return {
            "converted_r": None,
            "converted_d": None,
            "converted_or": None,
            "fisher_z": None,
        }

    converted_r = float(direction_sign) * math.sqrt(value_eta2)
    return convert_from_r(converted_r)


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


def approximate_ci(
    metric: str, effect_value: float, sample_size: int | None
) -> tuple[float, float] | None:
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
        return math.tanh(fisher_z - margin), math.tanh(fisher_z + margin)

    if metric == "converted_or":
        if effect_value <= 0:
            return None
        d_value = math.log(effect_value) * math.sqrt(3.0) / math.pi
        se_d = 2.0 / math.sqrt(float(sample_size))
        margin = 1.96 * se_d
        lower_d = d_value - margin
        upper_d = d_value + margin
        return (
            math.exp(lower_d * math.pi / math.sqrt(3.0)),
            math.exp(upper_d * math.pi / math.sqrt(3.0)),
        )

    return None


def transform_for_analysis(
    metric: str,
    effect: float,
    ci_low: float | None,
    ci_high: float | None,
) -> tuple[float | None, float | None, float | None]:
    if metric != "converted_or":
        return effect, ci_low, ci_high

    if effect <= 0:
        return None, None, None

    transformed_effect = math.log(effect)
    if ci_low is None or ci_high is None or ci_low <= 0 or ci_high <= 0:
        return transformed_effect, None, None

    return transformed_effect, math.log(min(ci_low, ci_high)), math.log(max(ci_low, ci_high))


def inverse_transform_for_output(metric: str, value: float) -> float:
    if metric == "converted_or":
        return float(math.exp(value))
    return float(value)


def infer_outcome_label(metadata: dict[str, object], preferred_field: str) -> str:
    candidates = [
        preferred_field,
        "outcome_construct",
        "outcome_measure",
        "outcome",
        "identity_construct",
        "identity_measure",
    ]
    for column in candidates:
        text = normalize(metadata.get(column, ""))
        if not is_missing(text):
            return text
    return "unspecified_outcome"


def normalize_source_metric(value: object) -> str:
    text = normalize_lower(value)
    return text if text in SUPPORTED_SOURCE_METRICS else ""


def normalize_exclusion_reason(value: object) -> str:
    text = normalize(value)
    if not text or is_missing(text):
        return ""

    lowered = normalize_lower(text)
    if lowered in {"included_primary", "included", "include", "yes", "true", "1"}:
        return ""

    return text


def normalize_included_flag(value: object) -> bool | None:
    lowered = normalize_lower(value)
    if lowered in {"yes", "y", "true", "1"}:
        return True
    if lowered in {"no", "n", "false", "0"}:
        return False
    return None


def add_trace_reason(
    mapping: defaultdict[str, defaultdict[str, set[str]]],
    *,
    outcome: str,
    study_id: str,
    reason: str,
) -> None:
    outcome_key = normalize(outcome) or "unspecified_outcome"
    study_key = normalize(study_id)
    reason_key = normalize(reason)
    if not study_key or not reason_key:
        return

    mapping[outcome_key][study_key].add(reason_key)


def build_analysis_trace_payload(
    *,
    metric: str,
    model: str,
    converted_path: Path,
    extraction_path: Path,
    meta_results_path: Path,
    outcomes: list[str],
    included_studies_by_outcome: defaultdict[str, set[str]],
    runtime_exclusions_by_outcome: defaultdict[str, defaultdict[str, set[str]]],
    extraction_candidates_by_outcome: defaultdict[str, set[str]],
    extraction_reasons_by_outcome: defaultdict[str, dict[str, str]],
    extraction_included_flag_by_outcome: defaultdict[str, dict[str, bool | None]],
    k_studies_by_outcome: dict[str, int],
) -> dict[str, object]:
    payload: dict[str, object] = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "metric": metric,
        "source_files": {
            "converted_input": converted_path.as_posix(),
            "extraction_input": extraction_path.as_posix(),
            "meta_results": meta_results_path.as_posix(),
        },
    }

    for index, outcome in enumerate(outcomes, start=1):
        included_studies = sorted(included_studies_by_outcome.get(outcome, set()))

        candidate_studies = set(extraction_candidates_by_outcome.get(outcome, set()))
        candidate_studies.update(runtime_exclusions_by_outcome.get(outcome, {}).keys())

        excluded_studies = sorted(
            study for study in candidate_studies if study not in included_studies
        )

        reason_by_study: dict[str, str] = {}
        for study_id in excluded_studies:
            runtime_reasons = sorted(
                runtime_exclusions_by_outcome.get(outcome, {}).get(study_id, set())
            )

            if runtime_reasons:
                reason_by_study[study_id] = "; ".join(runtime_reasons)
                continue

            extraction_reason = extraction_reasons_by_outcome.get(outcome, {}).get(study_id, "")
            if extraction_reason:
                reason_by_study[study_id] = extraction_reason
                continue

            included_flag = extraction_included_flag_by_outcome.get(outcome, {}).get(study_id)
            if included_flag is False:
                reason_by_study[study_id] = "flagged_not_included_in_meta"
            else:
                reason_by_study[study_id] = "not_pooled"

        unique_reasons = sorted(set(reason_by_study.values()))
        if not excluded_studies:
            reason_excluded = ""
        elif len(unique_reasons) == 1:
            reason_excluded = unique_reasons[0]
        else:
            reason_excluded = "multiple"

        payload[f"outcome_{index}"] = {
            "outcome": outcome,
            "studies": included_studies,
            "model": model,
            "k_studies": int(k_studies_by_outcome.get(outcome, len(included_studies))),
            "excluded": excluded_studies,
            "reason_excluded": reason_excluded,
            "reason_excluded_by_study": reason_by_study,
        }

    return payload


def add_issue(
    issues: list[dict],
    *,
    level: str,
    outcome: str,
    study_id: str,
    message: str,
) -> None:
    issues.append(
        {
            "level": level,
            "outcome": outcome,
            "study_id": study_id,
            "message": message,
        }
    )


def pool_group(
    *,
    effects: list[float],
    variances: list[float],
    model: str,
) -> dict[str, float]:
    weights_fixed = [1.0 / value for value in variances]
    sum_w_fixed = sum(weights_fixed)
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
    pooled = sum(weight * effect for weight, effect in zip(weights, effects)) / sum_weights
    pooled_se = math.sqrt(1.0 / sum_weights)

    z_value = pooled / pooled_se if pooled_se > 0 else float("nan")
    p_value = 2.0 * (1.0 - stats.norm.cdf(abs(z_value))) if math.isfinite(z_value) else float("nan")

    ci_low = pooled - (1.96 * pooled_se)
    ci_high = pooled + (1.96 * pooled_se)

    if q_stat <= 0 or degrees_freedom <= 0:
        i2 = 0.0
    else:
        i2 = max(((q_stat - degrees_freedom) / q_stat) * 100.0, 0.0)

    return {
        "pooled": pooled,
        "ci_low": ci_low,
        "ci_high": ci_high,
        "p_value": p_value,
        "i2": i2,
        "tau2": tau2,
    }


def should_fail(mode: str, errors: int, warnings: int) -> bool:
    norm_mode = normalize_lower(mode)
    if norm_mode == "none":
        return False
    if norm_mode == "warning":
        return (errors + warnings) > 0
    return errors > 0


def build_summary(
    *,
    metric: str,
    model: str,
    converted_path: Path,
    extraction_path: Path,
    output_path: Path,
    trace_output_path: Path,
    raw_rows: int,
    eligible_rows: int,
    rows_with_se: int,
    results_df: pd.DataFrame,
    outcome_counts: Counter,
    issues: list[dict],
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    error_count = sum(1 for issue in issues if issue["level"] == "error")
    warning_count = sum(1 for issue in issues if issue["level"] == "warning")

    lines: list[str] = []
    lines.append("# Meta-Analysis Results Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Converted input: `{converted_path.as_posix()}`")
    lines.append(f"- Extraction input: `{extraction_path.as_posix()}`")
    lines.append(f"- Results output: `{output_path.as_posix()}`")
    lines.append(f"- Analysis trace output: `{trace_output_path.as_posix()}`")
    lines.append("")
    lines.append("## Settings")
    lines.append("")
    lines.append(f"- Metric: `{metric}`")
    lines.append(f"- Model: `{model}`")
    lines.append("")
    lines.append("## Counts")
    lines.append("")
    lines.append(f"- Raw converted rows: {raw_rows}")
    lines.append(f"- Eligible rows (converted/partial + numeric metric): {eligible_rows}")
    lines.append(f"- Rows with usable SE: {rows_with_se}")
    lines.append(f"- Outcome groups exported: {int(results_df.shape[0])}")
    lines.append(f"- Issues: errors={error_count}, warnings={warning_count}")

    lines.append("")
    lines.append("## Outcome Coverage")
    lines.append("")
    if outcome_counts:
        for outcome, count in sorted(outcome_counts.items()):
            lines.append(f"- `{outcome}`: {int(count)} candidate rows")
    else:
        lines.append("- No eligible outcome rows detected.")

    lines.append("")
    lines.append("## Issues")
    lines.append("")
    if issues:
        lines.append("| level | outcome | study_id | message |")
        lines.append("|---|---|---|---|")
        for issue in issues:
            lines.append(
                f"| {issue['level']} | {normalize(issue['outcome']).replace('|', '\\|')} | {normalize(issue['study_id']).replace('|', '\\|')} | {issue['message']} |"
            )
    else:
        lines.append("- ✅ No issues found.")

    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build per-outcome pooled meta-analysis table from converted effect sizes."
    )
    parser.add_argument(
        "--converted-input",
        default="outputs/effect_size_converted.csv",
        help="Path to converted effect sizes CSV",
    )
    parser.add_argument(
        "--extraction-input",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction CSV",
    )
    parser.add_argument(
        "--metric",
        default="converted_d",
        choices=SUPPORTED_METRICS,
        help="Converted metric to pool",
    )
    parser.add_argument(
        "--model",
        default="random_effects",
        choices=["random_effects", "fixed_effects"],
        help="Pooling model",
    )
    parser.add_argument(
        "--outcome-field",
        default="outcome_construct",
        help="Preferred extraction column for outcome grouping",
    )
    parser.add_argument(
        "--output",
        default="outputs/meta_analysis_results.csv",
        help="Path to pooled meta-analysis results CSV",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/meta_analysis_results_summary.md",
        help="Path to markdown summary",
    )
    parser.add_argument(
        "--trace-output",
        default="",
        help="Path to analysis trace JSON (default: alongside --output as analysis_trace.json)",
    )
    parser.add_argument(
        "--fail-on",
        default="none",
        choices=["none", "warning", "error"],
        help="Fail mode: none (default), warning, error",
    )
    args = parser.parse_args()

    converted_path = Path(args.converted_input)
    extraction_path = Path(args.extraction_input)
    output_path = Path(args.output)
    summary_path = Path(args.summary_output)
    trace_path = (
        Path(args.trace_output)
        if normalize(args.trace_output)
        else output_path.with_name("analysis_trace.json")
    )

    if not converted_path.exists():
        raise FileNotFoundError(f"Converted input not found: {converted_path}")
    if not extraction_path.exists():
        raise FileNotFoundError(f"Extraction input not found: {extraction_path}")

    try:
        converted_df = pd.read_csv(converted_path, dtype=str)
    except pd.errors.EmptyDataError:
        converted_df = pd.DataFrame()

    try:
        extraction_df = pd.read_csv(extraction_path, dtype=str)
    except pd.errors.EmptyDataError:
        extraction_df = pd.DataFrame()

    for column in [
        "row",
        "study_id",
        "source_metric_canonical",
        "main_effect_metric",
        "effect_direction",
        "conversion_status",
        args.metric,
    ]:
        if column not in converted_df.columns:
            converted_df[column] = ""

    extraction_required = {
        args.outcome_field,
        "outcome_construct",
        "outcome_measure",
        "outcome",
        "identity_construct",
        "identity_measure",
        "ci_lower",
        "ci_upper",
        "sample_size",
        "effect_direction",
        "study_id",
        "included_in_meta",
        "exclusion_reason",
    }
    for column in extraction_required:
        if column not in extraction_df.columns:
            extraction_df[column] = ""

    metadata_by_index: dict[int, dict[str, object]] = {}
    metadata_by_study: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
    extraction_candidates_by_outcome: defaultdict[str, set[str]] = defaultdict(set)
    extraction_reasons_by_outcome: defaultdict[str, dict[str, str]] = defaultdict(dict)
    extraction_included_flag_by_outcome: defaultdict[str, dict[str, bool | None]] = defaultdict(
        dict
    )

    for index, row in extraction_df.iterrows():
        payload = {
            "study_id": normalize(row.get("study_id", "")),
            args.outcome_field: row.get(args.outcome_field, ""),
            "outcome_construct": row.get("outcome_construct", ""),
            "outcome_measure": row.get("outcome_measure", ""),
            "outcome": row.get("outcome", ""),
            "identity_construct": row.get("identity_construct", ""),
            "identity_measure": row.get("identity_measure", ""),
            "ci_lower": row.get("ci_lower", ""),
            "ci_upper": row.get("ci_upper", ""),
            "sample_size": row.get("sample_size", ""),
            "effect_direction": row.get("effect_direction", ""),
            "included_in_meta": row.get("included_in_meta", ""),
            "exclusion_reason": row.get("exclusion_reason", ""),
        }
        metadata_by_index[int(index)] = payload
        study_id = normalize(payload["study_id"])
        if study_id:
            metadata_by_study[study_id].append(payload)

            outcome = infer_outcome_label(payload, args.outcome_field)
            extraction_candidates_by_outcome[outcome].add(study_id)

            extraction_reason = normalize_exclusion_reason(payload.get("exclusion_reason", ""))
            if extraction_reason:
                extraction_reasons_by_outcome[outcome][study_id] = extraction_reason

            included_flag = normalize_included_flag(payload.get("included_in_meta", ""))
            existing_flag = extraction_included_flag_by_outcome[outcome].get(study_id)
            if existing_flag is None:
                extraction_included_flag_by_outcome[outcome][study_id] = included_flag
            elif existing_flag is True and included_flag is False:
                extraction_included_flag_by_outcome[outcome][study_id] = False

    def resolve_row_metadata(study_id: str, extraction_index: int | None) -> dict[str, object]:
        if extraction_index is not None and extraction_index in metadata_by_index:
            return metadata_by_index[extraction_index]

        if study_id and study_id in metadata_by_study:
            return metadata_by_study[study_id][0]

        return {}

    trace_included_studies: defaultdict[str, set[str]] = defaultdict(set)
    trace_runtime_exclusions: defaultdict[str, defaultdict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )

    issues: list[dict] = []
    pooled_inputs: defaultdict[str, list[dict[str, float]]] = defaultdict(list)
    outcome_counts: Counter = Counter()

    working = converted_df.copy()
    working["conversion_status"] = (
        working["conversion_status"].fillna("").astype(str).str.strip().str.lower()
    )
    working = working[working["conversion_status"].isin({"converted", "partial"})].copy()

    raw_rows = int(converted_df.shape[0])
    eligible_rows = 0
    rows_with_se = 0

    converted_trace_df = converted_df.copy()
    converted_trace_df["conversion_status"] = (
        converted_trace_df["conversion_status"].fillna("").astype(str).str.strip().str.lower()
    )
    for _, row in converted_trace_df.iterrows():
        study_id = normalize(row.get("study_id", ""))
        extraction_index = parse_row_index(row.get("row", ""))
        metadata = resolve_row_metadata(study_id, extraction_index)
        outcome = infer_outcome_label(metadata, args.outcome_field)

        conversion_status = normalize_lower(row.get("conversion_status", ""))
        if conversion_status not in {"converted", "partial"}:
            status_label = conversion_status if conversion_status else "missing"
            add_trace_reason(
                trace_runtime_exclusions,
                outcome=outcome,
                study_id=study_id,
                reason=f"conversion_status_{status_label}",
            )
            continue

        if numeric_or_none(row.get(args.metric, "")) is None:
            add_trace_reason(
                trace_runtime_exclusions,
                outcome=outcome,
                study_id=study_id,
                reason=f"missing_{args.metric}",
            )

    for _, row in working.iterrows():
        effect_value = numeric_or_none(row.get(args.metric, ""))
        if effect_value is None:
            continue
        eligible_rows += 1

        study_id = normalize(row.get("study_id", ""))
        extraction_index = parse_row_index(row.get("row", ""))

        metadata: dict[str, object] = {}
        if extraction_index is not None and extraction_index in metadata_by_index:
            metadata = metadata_by_index[extraction_index]
        elif study_id and study_id in metadata_by_study:
            if len(metadata_by_study[study_id]) > 1:
                add_issue(
                    issues,
                    level="warning",
                    outcome="",
                    study_id=study_id,
                    message="Multiple extraction rows found for study_id; using first row for metadata.",
                )
            metadata = metadata_by_study[study_id][0]
        else:
            add_issue(
                issues,
                level="warning",
                outcome="",
                study_id=study_id,
                message="Extraction metadata not found; CI/sample-size fallback may be unavailable.",
            )

        outcome = infer_outcome_label(metadata, args.outcome_field)
        outcome_counts[outcome] += 1

        source_metric = normalize_source_metric(row.get("source_metric_canonical", ""))
        if not source_metric:
            source_metric = normalize_source_metric(row.get("main_effect_metric", ""))

        direction_sign = signed_direction(row.get("effect_direction", ""))
        if direction_sign is None:
            direction_sign = signed_direction(metadata.get("effect_direction", ""))

        raw_ci_low = numeric_or_none(metadata.get("ci_lower", ""))
        raw_ci_high = numeric_or_none(metadata.get("ci_upper", ""))
        ci_low: float | None = None
        ci_high: float | None = None

        if raw_ci_low is not None and raw_ci_high is not None and source_metric:
            converted_low = convert_source_value(
                source_metric=source_metric,
                source_value=raw_ci_low,
                direction_sign=direction_sign,
                target_metric=args.metric,
            )
            converted_high = convert_source_value(
                source_metric=source_metric,
                source_value=raw_ci_high,
                direction_sign=direction_sign,
                target_metric=args.metric,
            )
            if converted_low is not None and converted_high is not None:
                ci_low, ci_high = sorted([float(converted_low), float(converted_high)])

        if ci_low is None or ci_high is None:
            sample_size = parse_sample_size(metadata.get("sample_size", ""))
            approx = approximate_ci(args.metric, effect_value, sample_size)
            if approx is not None:
                ci_low, ci_high = approx

        effect_analysis, ci_analysis_low, ci_analysis_high = transform_for_analysis(
            args.metric,
            effect_value,
            ci_low,
            ci_high,
        )

        if effect_analysis is None:
            add_trace_reason(
                trace_runtime_exclusions,
                outcome=outcome,
                study_id=study_id,
                reason="effect_not_transformable",
            )
            add_issue(
                issues,
                level="warning",
                outcome=outcome,
                study_id=study_id,
                message="Effect cannot be transformed to analysis scale.",
            )
            continue

        se_value: float | None = None
        if ci_analysis_low is not None and ci_analysis_high is not None:
            margin = (ci_analysis_high - ci_analysis_low) / (2.0 * 1.96)
            if math.isfinite(margin) and margin > 0:
                se_value = float(margin)

        if se_value is None:
            add_trace_reason(
                trace_runtime_exclusions,
                outcome=outcome,
                study_id=study_id,
                reason="missing_variance",
            )
            add_issue(
                issues,
                level="warning",
                outcome=outcome,
                study_id=study_id,
                message="No usable CI/sample-size for standard error derivation; row skipped from pooling.",
            )
            continue

        rows_with_se += 1
        if study_id:
            trace_included_studies[outcome].add(study_id)
            trace_runtime_exclusions[outcome].pop(study_id, None)

        pooled_inputs[outcome].append(
            {
                "effect": float(effect_analysis),
                "variance": float(se_value * se_value),
            }
        )

    all_outcomes = sorted(outcome_counts.keys())
    result_rows: list[dict[str, object]] = []
    for outcome in all_outcomes:
        group = pooled_inputs.get(outcome, [])
        if not group:
            result_rows.append(
                {
                    "outcome": outcome,
                    "k_studies": 0,
                    "pooled_effect": "",
                    "ci_low": "",
                    "ci_high": "",
                    "p_value": "",
                    "i2": "",
                    "tau2": "",
                    "model": args.model,
                }
            )
            continue

        stats_result = pool_group(
            effects=[item["effect"] for item in group],
            variances=[item["variance"] for item in group],
            model=args.model,
        )

        pooled_effect = inverse_transform_for_output(args.metric, stats_result["pooled"])
        ci_low_out = inverse_transform_for_output(args.metric, stats_result["ci_low"])
        ci_high_out = inverse_transform_for_output(args.metric, stats_result["ci_high"])

        result_rows.append(
            {
                "outcome": outcome,
                "k_studies": int(len(group)),
                "pooled_effect": pooled_effect,
                "ci_low": ci_low_out,
                "ci_high": ci_high_out,
                "p_value": float(stats_result["p_value"]),
                "i2": float(stats_result["i2"]),
                "tau2": float(stats_result["tau2"]),
                "model": args.model,
            }
        )

    results_df = pd.DataFrame(result_rows)
    if results_df.empty:
        results_df = pd.DataFrame(columns=RESULT_COLUMNS)
    else:
        results_df = results_df[RESULT_COLUMNS]

    atomic_write_dataframe_csv(results_df, output_path, index=False)

    k_studies_by_outcome: dict[str, int] = {}
    if not results_df.empty:
        for _, row in results_df.iterrows():
            outcome_name = normalize(row.get("outcome", ""))
            if not outcome_name:
                continue
            k_value = numeric_or_none(row.get("k_studies", ""))
            if k_value is None:
                continue
            k_studies_by_outcome[outcome_name] = int(round(k_value))

    trace_payload = build_analysis_trace_payload(
        metric=args.metric,
        model=args.model,
        converted_path=converted_path,
        extraction_path=extraction_path,
        meta_results_path=output_path,
        outcomes=all_outcomes,
        included_studies_by_outcome=trace_included_studies,
        runtime_exclusions_by_outcome=trace_runtime_exclusions,
        extraction_candidates_by_outcome=extraction_candidates_by_outcome,
        extraction_reasons_by_outcome=extraction_reasons_by_outcome,
        extraction_included_flag_by_outcome=extraction_included_flag_by_outcome,
        k_studies_by_outcome=k_studies_by_outcome,
    )
    atomic_write_text(trace_path, json.dumps(trace_payload, ensure_ascii=False, indent=2) + "\n")

    summary_text = build_summary(
        metric=args.metric,
        model=args.model,
        converted_path=converted_path,
        extraction_path=extraction_path,
        output_path=output_path,
        trace_output_path=trace_path,
        raw_rows=raw_rows,
        eligible_rows=eligible_rows,
        rows_with_se=rows_with_se,
        results_df=results_df,
        outcome_counts=outcome_counts,
        issues=issues,
    )
    atomic_write_text(summary_path, summary_text)

    error_count = sum(1 for issue in issues if issue["level"] == "error")
    warning_count = sum(1 for issue in issues if issue["level"] == "warning")

    print(f"Wrote: {output_path}")
    print(f"Wrote: {summary_path}")
    print(f"Wrote: {trace_path}")
    print(
        "Meta-analysis rows: "
        f"eligible={eligible_rows}, with_se={rows_with_se}, outcomes={int(results_df.shape[0])}"
    )
    print(f"Issues: errors={error_count}, warnings={warning_count}")

    if should_fail(args.fail_on, errors=error_count, warnings=warning_count):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

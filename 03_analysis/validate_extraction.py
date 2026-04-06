import argparse
from datetime import datetime
from pathlib import Path
import re

import pandas as pd


REQUIRED_COLUMNS = [
    "study_id",
    "source_id",
    "included_in_meta",
    "included_in_bias",
    "included_in_grade",
    "exclusion_reason",
    "decision_justification",
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
    "main_effect_metric",
    "main_effect_value",
    "effect_direction",
    "ci_lower",
    "ci_upper",
    "adjusted_unadjusted",
    "model_type",
    "p_value",
    "covariates",
    "quality_appraisal",
    "extractor_id",
    "checked_by",
    "consensus_status",
    "adjudication_notes",
    "notes",
]

MISSING_CODES = {"na", "nr", "unclear", "nan", "not applicable", "not_applicable"}
ALLOWED_ORIENTATION = {"theoretical", "methodological", "mixed", "other"}
ALLOWED_INSTRUMENT_TYPE = {"questionnaire", "interview", "registry", "other"}
ALLOWED_RESPONDENT_TYPE = {"self-report", "self_report", "clinician", "interview", "mixed"}
ALLOWED_DSM_ICD_VERSION = {
    "dsm-iii",
    "dsm-iii-r",
    "dsm-iv",
    "dsm-iv-tr",
    "dsm-5",
    "dsm-5-tr",
    "icd-10",
    "icd-11",
    "registry-based",
    "protocol-defined",
    "mixed",
    "other",
}
ALLOWED_EFFECT_DIRECTION = {"positive", "negative", "null", "mixed"}
ALLOWED_ADJUSTMENT = {"adjusted", "unadjusted", "mixed"}
ALLOWED_INCLUSION_FLAG = {"yes", "no"}
ALLOWED_EXCLUSION_REASON = {
    "included_primary",
    "included_contextual",
    "wrong_population",
    "wrong_outcome",
    "wrong_study_design",
    "not_empirical",
    "duplicate",
    "full_text_unavailable",
    "insufficient_data",
    "insufficient_data_for_meta",
    "insufficient_data_for_bias",
    "insufficient_data_for_grade",
    "other",
}
ALLOWED_CONSENSUS_STATUS = {
    "single_extractor",
    "double_extracted_agree",
    "double_extracted_disagree",
    "adjudicated",
}

LEGACY_TO_GENERIC_COLUMN_MAP = {
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
    "author": "first_author",
    "effect_measure": "main_effect_metric",
    "effect_metric": "main_effect_metric",
    "effect_value": "main_effect_value",
    "adjustment": "adjusted_unadjusted",
    "adjustment_status": "adjusted_unadjusted",
    "analysis_model": "model_type",
    "risk_of_bias": "quality_appraisal",
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

COMPATIBILITY_COLUMNS = set(LEGACY_TO_GENERIC_COLUMN_MAP.keys()) | {"confidence_interval"}


def harmonize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    working = df.copy()
    for legacy, generic in LEGACY_TO_GENERIC_COLUMN_MAP.items():
        if legacy not in working.columns:
            continue

        if generic not in working.columns:
            working[generic] = working[legacy]
            continue

        generic_values = working[generic].fillna("").astype(str).str.strip()
        legacy_values = working[legacy].fillna("").astype(str).str.strip()
        fill_mask = (generic_values == "") & (legacy_values != "")
        if fill_mask.any():
            working.loc[fill_mask, generic] = working.loc[fill_mask, legacy]

    if "confidence_interval" in working.columns:
        if "ci_lower" not in working.columns:
            working["ci_lower"] = ""
        if "ci_upper" not in working.columns:
            working["ci_upper"] = ""

        for index, row in working.iterrows():
            lower_raw = normalize(row.get("ci_lower", ""))
            upper_raw = normalize(row.get("ci_upper", ""))
            if lower_raw and upper_raw:
                continue

            parsed_lower, parsed_upper = parse_confidence_interval_bounds(row.get("confidence_interval", ""))
            if parsed_lower is not None and not lower_raw:
                working.at[index, "ci_lower"] = str(parsed_lower)
            if parsed_upper is not None and not upper_raw:
                working.at[index, "ci_upper"] = str(parsed_upper)

    return working


def normalize(value: object) -> str:
    return str(value).strip()


def normalize_lower(value: object) -> str:
    return normalize(value).lower()


def parse_float_or_none(value: object) -> float | None:
    text = normalize(value)
    if is_empty_or_missing(text):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def is_empty_cell(value: object) -> bool:
    text = normalize(value)
    return text == "" or text.lower() == "nan"


def is_missing_code(value: object) -> bool:
    text = normalize_lower(value)
    return text in MISSING_CODES


def is_empty_or_missing(value: object) -> bool:
    return is_empty_cell(value) or is_missing_code(value)


def non_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    mask = df.apply(lambda row: any(not is_empty_cell(cell) for cell in row), axis=1)
    return df[mask].copy()


def validate_schema(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    extra = [
        column
        for column in df.columns
        if column not in REQUIRED_COLUMNS and column not in COMPATIBILITY_COLUMNS
    ]
    return missing, extra


def parse_confidence_interval_bounds(value: object) -> tuple[float | None, float | None]:
    text = normalize(value)
    if is_empty_or_missing(text):
        return None, None

    numbers = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", text)
    if len(numbers) < 2:
        return None, None

    lower = parse_float_or_none(numbers[0])
    upper = parse_float_or_none(numbers[1])
    if lower is None or upper is None:
        return None, None

    return (lower, upper) if lower <= upper else (upper, lower)


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


def metric_scale_error(metric_key: str, value: float) -> str | None:
    if metric_key == "r":
        if value < -1.0 or value > 1.0:
            return "Value is outside valid range [-1, 1] for `r`."
        return None

    if metric_key == "or":
        if value <= 0.0:
            return "Value must be > 0 for `OR`."
        return None

    if metric_key == "eta2":
        if value < 0.0 or value > 1.0:
            return "Value is outside valid range [0, 1] for `eta²`."
        return None

    return None


def add_issue(issues: list[dict], row_number: int, level: str, column: str, message: str, value: str = "") -> None:
    issues.append(
        {
            "row": row_number,
            "level": level,
            "column": column,
            "message": message,
            "value": value,
        }
    )


def validate_rows(df: pd.DataFrame) -> list[dict]:
    issues: list[dict] = []

    priority_fields = [
        "framework",
        "predictor_instrument_type",
        "condition_diagnostic_system",
    ]

    for index, row in df.iterrows():
        row_number = int(index) + 2

        orientation = normalize(row.get("framework", ""))
        instrument_type = normalize(row.get("predictor_instrument_type", ""))
        instrument_name = normalize(row.get("predictor_instrument_name", ""))
        instrument_subscale = normalize(row.get("predictor_subscale", ""))
        instrument_respondent_type = normalize(row.get("predictor_respondent_type", ""))
        dsm_icd_version = normalize(row.get("condition_diagnostic_system", ""))
        diagnostic_frame_detail = normalize(row.get("diagnostic_frame_detail", ""))
        effect_metric = normalize(row.get("main_effect_metric", ""))
        effect_value = normalize(row.get("main_effect_value", ""))
        effect_direction = normalize(row.get("effect_direction", ""))
        ci_lower = normalize(row.get("ci_lower", ""))
        ci_upper = normalize(row.get("ci_upper", ""))
        adjusted_unadjusted = normalize(row.get("adjusted_unadjusted", ""))
        model_type = normalize(row.get("model_type", ""))
        source_id = normalize(row.get("source_id", ""))
        included_in_meta = normalize(row.get("included_in_meta", ""))
        included_in_bias = normalize(row.get("included_in_bias", ""))
        included_in_grade = normalize(row.get("included_in_grade", ""))
        exclusion_reason = normalize(row.get("exclusion_reason", ""))
        decision_justification = normalize(row.get("decision_justification", ""))
        extractor_id = normalize(row.get("extractor_id", ""))
        checked_by = normalize(row.get("checked_by", ""))
        consensus_status = normalize(row.get("consensus_status", ""))
        adjudication_notes = normalize(row.get("adjudication_notes", ""))
        notes = normalize(row.get("notes", ""))

        if is_empty_or_missing(source_id):
            add_issue(
                issues,
                row_number,
                "error",
                "source_id",
                "source_id is required.",
                source_id,
            )

        for field_name, field_value in (
            ("included_in_meta", included_in_meta),
            ("included_in_bias", included_in_bias),
            ("included_in_grade", included_in_grade),
        ):
            normalized_flag = normalize_lower(field_value)
            if is_empty_or_missing(field_value):
                add_issue(
                    issues,
                    row_number,
                    "error",
                    field_name,
                    "Required inclusion flag is missing. Use 'yes' or 'no'.",
                    field_value,
                )
            elif normalized_flag not in ALLOWED_INCLUSION_FLAG:
                add_issue(
                    issues,
                    row_number,
                    "error",
                    field_name,
                    "Invalid value. Allowed: yes/no.",
                    field_value,
                )

        exclusion_reason_norm = normalize_lower(exclusion_reason)
        if is_empty_or_missing(exclusion_reason):
            add_issue(
                issues,
                row_number,
                "error",
                "exclusion_reason",
                "Decision reason is required. Use controlled vocabulary.",
                exclusion_reason,
            )
        elif exclusion_reason_norm not in ALLOWED_EXCLUSION_REASON:
            add_issue(
                issues,
                row_number,
                "error",
                "exclusion_reason",
                "Invalid value. Use controlled vocabulary from extraction_data_dictionary.md.",
                exclusion_reason,
            )

        if is_empty_or_missing(decision_justification):
            add_issue(
                issues,
                row_number,
                "error",
                "decision_justification",
                "Decision justification is required (1–2 lines).",
                decision_justification,
            )
        else:
            newline_count = decision_justification.count("\n")
            if newline_count > 1:
                add_issue(
                    issues,
                    row_number,
                    "warning",
                    "decision_justification",
                    "Keep decision justification concise (1–2 lines).",
                    decision_justification,
                )

            text_len = len(decision_justification)
            if text_len < 20:
                add_issue(
                    issues,
                    row_number,
                    "warning",
                    "decision_justification",
                    "Decision justification seems too short; clarify inclusion/exclusion logic.",
                    decision_justification,
                )
            elif text_len > 320:
                add_issue(
                    issues,
                    row_number,
                    "warning",
                    "decision_justification",
                    "Decision justification is too long; keep to 1–2 lines.",
                    decision_justification,
                )

        inclusion_flags = {
            "included_in_meta": normalize_lower(included_in_meta),
            "included_in_bias": normalize_lower(included_in_bias),
            "included_in_grade": normalize_lower(included_in_grade),
        }

        has_exclusion = any(flag_value == "no" for flag_value in inclusion_flags.values())
        if has_exclusion and exclusion_reason_norm in {"included_primary", "included_contextual"}:
            add_issue(
                issues,
                row_number,
                "warning",
                "exclusion_reason",
                "Use an exclusion reason when any included_in_* flag is 'no'.",
                exclusion_reason,
            )

        if not has_exclusion and exclusion_reason_norm not in {"included_primary", "included_contextual"}:
            add_issue(
                issues,
                row_number,
                "warning",
                "exclusion_reason",
                "Use an inclusion reason (included_primary/included_contextual) when all included_in_* flags are 'yes'.",
                exclusion_reason,
            )

        for field in priority_fields:
            value = normalize(row.get(field, ""))
            if is_empty_cell(value):
                add_issue(
                    issues,
                    row_number,
                    "warning",
                    field,
                    "Empty priority field. Use explicit code or value.",
                    value,
                )

        orientation_norm = normalize_lower(orientation)
        if not is_empty_or_missing(orientation) and orientation_norm not in ALLOWED_ORIENTATION:
            add_issue(
                issues,
                row_number,
                "error",
                "framework",
                "Invalid value. Allowed: theoretical/methodological/mixed/other (+ NR/NA/UNCLEAR).",
                orientation,
            )

        instrument_norm = normalize_lower(instrument_type)
        if not is_empty_or_missing(instrument_type) and instrument_norm not in ALLOWED_INSTRUMENT_TYPE:
            add_issue(
                issues,
                row_number,
                "error",
                "predictor_instrument_type",
                "Invalid value. Allowed: questionnaire/interview/registry/other (+ NR/NA/UNCLEAR).",
                instrument_type,
            )

        dsm_icd_norm = normalize_lower(dsm_icd_version)
        if not is_empty_or_missing(dsm_icd_version) and dsm_icd_norm not in ALLOWED_DSM_ICD_VERSION:
            add_issue(
                issues,
                row_number,
                "error",
                "condition_diagnostic_system",
                "Invalid value. Use DSM/ICD edition from dictionary (+ NR/NA/UNCLEAR).",
                dsm_icd_version,
            )

        if not is_empty_or_missing(dsm_icd_version) and is_empty_or_missing(diagnostic_frame_detail):
            add_issue(
                issues,
                row_number,
                "warning",
                "diagnostic_frame_detail",
                "Add diagnostic frame detail (system + operational source) to refine condition_diagnostic_system.",
                diagnostic_frame_detail,
            )

        if is_empty_or_missing(dsm_icd_version) and not is_empty_or_missing(diagnostic_frame_detail):
            add_issue(
                issues,
                row_number,
                "warning",
                "condition_diagnostic_system",
                "condition_diagnostic_system is missing while diagnostic_frame_detail is filled.",
                dsm_icd_version,
            )

        respondent_type_norm = normalize_lower(instrument_respondent_type)
        if (
            not is_empty_or_missing(instrument_respondent_type)
            and respondent_type_norm not in ALLOWED_RESPONDENT_TYPE
        ):
            add_issue(
                issues,
                row_number,
                "error",
                "predictor_respondent_type",
                "Invalid value. Allowed: self-report/clinician/interview/mixed (+ NR/NA/UNCLEAR).",
                instrument_respondent_type,
            )

        if not is_empty_or_missing(instrument_type) and is_empty_or_missing(instrument_name):
            add_issue(
                issues,
                row_number,
                "warning",
                "predictor_instrument_name",
                "Instrument name is expected when instrument type is provided.",
                instrument_name,
            )

        if not is_empty_or_missing(instrument_name) and is_empty_or_missing(instrument_respondent_type):
            add_issue(
                issues,
                row_number,
                "warning",
                "predictor_respondent_type",
                "Respondent type is expected when a predictor instrument is named.",
                instrument_respondent_type,
            )

        if not is_empty_or_missing(instrument_subscale) and is_empty_or_missing(instrument_name):
            add_issue(
                issues,
                row_number,
                "warning",
                "predictor_instrument_name",
                "Instrument name should be filled when subscale is provided.",
                instrument_name,
            )

        effect_direction_norm = normalize_lower(effect_direction)
        if not is_empty_or_missing(effect_direction) and effect_direction_norm not in ALLOWED_EFFECT_DIRECTION:
            add_issue(
                issues,
                row_number,
                "error",
                "effect_direction",
                "Invalid value. Allowed: positive/negative/null/mixed (+ NR/NA/UNCLEAR).",
                effect_direction,
            )

        metric_key = None
        if is_empty_or_missing(effect_metric) and not is_empty_or_missing(effect_value):
            add_issue(
                issues,
                row_number,
                "error",
                "main_effect_metric",
                "main_effect_metric is required when main_effect_value is provided.",
                effect_metric,
            )
        elif not is_empty_or_missing(effect_metric):
            metric_key = canonical_metric(effect_metric)
            if metric_key is None:
                add_issue(
                    issues,
                    row_number,
                    "error",
                    "main_effect_metric",
                    "Unsupported main_effect_metric. Allowed families: r, d, OR, eta2 (with common aliases).",
                    effect_metric,
                )

        effect_value_num = parse_float_or_none(effect_value)
        if not is_empty_or_missing(effect_metric) and is_empty_or_missing(effect_value):
            add_issue(
                issues,
                row_number,
                "error",
                "main_effect_value",
                "main_effect_value is required when main_effect_metric is provided.",
                effect_value,
            )
        elif not is_empty_or_missing(effect_value) and effect_value_num is None:
            add_issue(
                issues,
                row_number,
                "error",
                "main_effect_value",
                "main_effect_value must be numeric or a missing code.",
                effect_value,
            )
        elif metric_key is not None and effect_value_num is not None:
            metric_value_scale_error = metric_scale_error(metric_key, effect_value_num)
            if metric_value_scale_error is not None:
                add_issue(
                    issues,
                    row_number,
                    "error",
                    "main_effect_value",
                    metric_value_scale_error,
                    effect_value,
                )

        adjustment_norm = normalize_lower(adjusted_unadjusted)
        if not is_empty_or_missing(adjusted_unadjusted) and adjustment_norm not in ALLOWED_ADJUSTMENT:
            add_issue(
                issues,
                row_number,
                "error",
                "adjusted_unadjusted",
                "Invalid value. Allowed: adjusted/unadjusted/mixed (+ NR/NA/UNCLEAR).",
                adjusted_unadjusted,
            )

        ci_lower_num = parse_float_or_none(ci_lower)
        ci_upper_num = parse_float_or_none(ci_upper)

        if not is_empty_or_missing(ci_lower) and ci_lower_num is None:
            add_issue(
                issues,
                row_number,
                "error",
                "ci_lower",
                "CI lower must be numeric or a missing code.",
                ci_lower,
            )

        if not is_empty_or_missing(ci_upper) and ci_upper_num is None:
            add_issue(
                issues,
                row_number,
                "error",
                "ci_upper",
                "CI upper must be numeric or a missing code.",
                ci_upper,
            )

        has_ci_lower = not is_empty_or_missing(ci_lower)
        has_ci_upper = not is_empty_or_missing(ci_upper)
        if has_ci_lower and not has_ci_upper:
            add_issue(
                issues,
                row_number,
                "warning",
                "ci_upper",
                "CI upper is missing while CI lower is present.",
                ci_upper,
            )
        elif has_ci_upper and not has_ci_lower:
            add_issue(
                issues,
                row_number,
                "warning",
                "ci_lower",
                "CI lower is missing while CI upper is present.",
                ci_lower,
            )

        if ci_lower_num is not None and ci_upper_num is not None and ci_lower_num > ci_upper_num:
            add_issue(
                issues,
                row_number,
                "error",
                "ci_lower",
                "CI bounds are inconsistent: ci_lower > ci_upper.",
                f"{ci_lower} > {ci_upper}",
            )

        if metric_key is not None:
            if ci_lower_num is not None:
                lower_scale_error = metric_scale_error(metric_key, ci_lower_num)
                if lower_scale_error is not None:
                    add_issue(
                        issues,
                        row_number,
                        "error",
                        "ci_lower",
                        f"{lower_scale_error} Keep CI in the same units as main_effect_metric.",
                        ci_lower,
                    )

            if ci_upper_num is not None:
                upper_scale_error = metric_scale_error(metric_key, ci_upper_num)
                if upper_scale_error is not None:
                    add_issue(
                        issues,
                        row_number,
                        "error",
                        "ci_upper",
                        f"{upper_scale_error} Keep CI in the same units as main_effect_metric.",
                        ci_upper,
                    )

        has_primary_effect = not is_empty_or_missing(effect_metric) or not is_empty_or_missing(effect_value)
        if has_primary_effect and is_empty_or_missing(effect_direction):
            add_issue(
                issues,
                row_number,
                "warning",
                "effect_direction",
                "Set effect direction for extracted primary effect.",
                effect_direction,
            )

        if has_primary_effect and is_empty_or_missing(adjusted_unadjusted):
            add_issue(
                issues,
                row_number,
                "warning",
                "adjusted_unadjusted",
                "Specify whether extracted effect is adjusted or unadjusted.",
                adjusted_unadjusted,
            )

        if (has_primary_effect or not is_empty_or_missing(adjusted_unadjusted)) and is_empty_or_missing(model_type):
            add_issue(
                issues,
                row_number,
                "warning",
                "model_type",
                "Add model_type for meta-ready standardization.",
                model_type,
            )

        if instrument_norm == "other" and is_empty_or_missing(instrument_name):
            add_issue(
                issues,
                row_number,
                "error",
                "predictor_instrument_name",
                "Required when predictor_instrument_type is 'other'.",
                instrument_name,
            )

        if orientation_norm in {"mixed", "other"} and is_empty_cell(notes):
            add_issue(
                issues,
                row_number,
                "warning",
                "notes",
                "Add brief rationale for mixed/other framework value.",
                notes,
            )

        if is_empty_cell(extractor_id):
            add_issue(
                issues,
                row_number,
                "warning",
                "extractor_id",
                "Primary extractor is missing.",
                extractor_id,
            )

        consensus_norm = normalize_lower(consensus_status)
        if is_empty_cell(consensus_status):
            add_issue(
                issues,
                row_number,
                "warning",
                "consensus_status",
                "Consensus status is missing.",
                consensus_status,
            )
        elif not is_missing_code(consensus_status) and consensus_norm not in ALLOWED_CONSENSUS_STATUS:
            add_issue(
                issues,
                row_number,
                "error",
                "consensus_status",
                "Invalid value. Allowed: single_extractor/double_extracted_agree/double_extracted_disagree/adjudicated (+ NR/NA/UNCLEAR).",
                consensus_status,
            )

        if consensus_norm == "single_extractor" and is_empty_or_missing(checked_by):
            pass
        elif consensus_norm in {"double_extracted_agree", "double_extracted_disagree", "adjudicated"} and is_empty_or_missing(checked_by):
            add_issue(
                issues,
                row_number,
                "warning",
                "checked_by",
                "Checker ID is expected for double extraction/adjudication states.",
                checked_by,
            )

        if not is_empty_or_missing(extractor_id) and not is_empty_or_missing(checked_by):
            if normalize_lower(extractor_id) == normalize_lower(checked_by):
                add_issue(
                    issues,
                    row_number,
                    "warning",
                    "checked_by",
                    "Checker should differ from extractor for independent verification.",
                    checked_by,
                )

        if consensus_norm == "adjudicated" and is_empty_or_missing(adjudication_notes):
            add_issue(
                issues,
                row_number,
                "warning",
                "adjudication_notes",
                "Adjudication notes are required when consensus_status is 'adjudicated'.",
                adjudication_notes,
            )

    return issues


def build_summary(
    *,
    input_path: Path,
    total_rows: int,
    checked_rows: int,
    missing_columns: list[str],
    extra_columns: list[str],
    issues: list[dict],
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    error_count = sum(1 for issue in issues if issue["level"] == "error")
    warning_count = sum(1 for issue in issues if issue["level"] == "warning")
    schema_ok = not missing_columns

    lines = []
    lines.append("# Extraction Validation Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Input")
    lines.append("")
    lines.append(f"- File: `{input_path.as_posix()}`")
    lines.append(f"- Total rows: {total_rows}")
    lines.append(f"- Non-empty rows checked: {checked_rows}")
    lines.append("")
    lines.append("## Schema Checks")
    lines.append("")
    if schema_ok:
        lines.append("- ✅ Required columns: all present")
    else:
        lines.append("- ❌ Required columns: missing entries found")
        for column in missing_columns:
            lines.append(f"  - Missing: `{column}`")

    if extra_columns:
        lines.append("- ℹ️ Extra columns detected (not blocking):")
        for column in extra_columns:
            lines.append(f"  - `{column}`")
    else:
        lines.append("- ✅ Extra columns: none")

    lines.append("")
    lines.append("## Value Checks")
    lines.append("")
    lines.append(f"- Errors: {error_count}")
    lines.append(f"- Warnings: {warning_count}")

    if issues:
        lines.append("")
        lines.append("## Issues")
        lines.append("")
        lines.append("| Row | Level | Column | Message | Value |")
        lines.append("|---:|---|---|---|---|")
        for issue in issues:
            value = issue["value"].replace("|", "\\|") if issue["value"] else ""
            lines.append(
                f"| {issue['row']} | {issue['level']} | `{issue['column']}` | {issue['message']} | `{value}` |"
            )
    else:
        lines.append("")
        lines.append("## Issues")
        lines.append("")
        lines.append("- ✅ No issues found.")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Rules follow `02_data/codebook/extraction_data_dictionary.md`.")
    lines.append("- Effect-size checks enforce metric/CI scale consistency (`main_effect_metric`, `main_effect_value`, `ci_lower`, `ci_upper`).")
    lines.append("- Empty rows are ignored.")

    return "\n".join(lines) + "\n"


def should_fail(fail_on: str, error_count: int, warning_count: int) -> bool:
    fail_on = fail_on.strip().lower()
    if fail_on == "none":
        return False
    if fail_on == "warning":
        return (error_count + warning_count) > 0
    return error_count > 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate extraction table values and schema.")
    parser.add_argument(
        "--input",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction CSV",
    )
    parser.add_argument(
        "--output",
        default="outputs/extraction_validation_summary.md",
        help="Path to markdown validation summary",
    )
    parser.add_argument(
        "--fail-on",
        default="error",
        choices=["none", "warning", "error"],
        help="Fail mode: error (default), warning, none",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    try:
        df = pd.read_csv(input_path, dtype=str)
    except pd.errors.EmptyDataError:
        df = pd.DataFrame()

    df = harmonize_columns(df)

    missing_columns, extra_columns = validate_schema(df)
    checked_df = non_empty_rows(df)
    issues = validate_rows(checked_df) if not checked_df.empty else []

    if missing_columns:
        for column in missing_columns:
            add_issue(
                issues,
                row_number=1,
                level="error",
                column=column,
                message="Required column is missing.",
            )

    summary = build_summary(
        input_path=input_path,
        total_rows=int(df.shape[0]),
        checked_rows=int(checked_df.shape[0]),
        missing_columns=missing_columns,
        extra_columns=extra_columns,
        issues=issues,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(summary, encoding="utf-8")

    error_count = sum(1 for issue in issues if issue["level"] == "error")
    warning_count = sum(1 for issue in issues if issue["level"] == "warning")

    print(f"Wrote: {output_path}")
    print(f"Validation issues: errors={error_count}, warnings={warning_count}")

    if should_fail(args.fail_on, error_count=error_count, warning_count=warning_count):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
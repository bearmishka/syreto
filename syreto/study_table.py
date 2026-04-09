from __future__ import annotations

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

INCLUDE_CODES = {
    "include",
    "included",
    "yes",
    "y",
    "1",
    "final_include",
    "consensus_include",
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
    "risk_of_bias": "quality_appraisal",
}


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def normalize_lower(value: object) -> str:
    return normalize(value).lower()


def is_missing(value: object) -> bool:
    return normalize_lower(value) in MISSING_CODES


def read_csv_or_empty(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)

    try:
        dataframe = pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=columns)

    return harmonize_study_columns(dataframe, columns)


def harmonize_study_columns(df: pd.DataFrame, required_columns: list[str]) -> pd.DataFrame:
    working = df.copy()

    for legacy, generic in LEGACY_TO_GENERIC_COLUMN_MAP.items():
        if legacy not in working.columns:
            continue
        if generic not in working.columns:
            working[generic] = working[legacy]
            continue

        generic_values = working[generic].fillna("").astype(str).str.strip()
        legacy_values = working[legacy].fillna("").astype(str).str.strip()
        fill_mask = generic_values.eq("") & legacy_values.ne("")
        if fill_mask.any():
            working.loc[fill_mask, generic] = working.loc[fill_mask, legacy]

    for column in required_columns:
        if column not in working.columns:
            working[column] = ""

    ordered = required_columns + [
        column for column in working.columns if column not in required_columns
    ]
    return working[ordered]


def load_study_table(path: Path, required_columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Extraction file not found: {path}")
    return read_csv_or_empty(path, required_columns)


def included_study_table(df: pd.DataFrame, required_columns: list[str]) -> pd.DataFrame:
    working = harmonize_study_columns(df, required_columns)
    if working.empty:
        return working

    working["study_id"] = working["study_id"].fillna("").astype(str).str.strip()
    working = working[working["study_id"].ne("")].copy()
    if working.empty:
        return working

    if "consensus_status" in working.columns:
        non_empty_consensus = (
            working["consensus_status"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .loc[lambda values: values.ne("")]
        )
        if not non_empty_consensus.empty and non_empty_consensus.isin(INCLUDE_CODES).any():
            working = working[
                working["consensus_status"]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.lower()
                .isin(INCLUDE_CODES)
            ].copy()

    if working.empty:
        return working

    return working.drop_duplicates(subset=["study_id"], keep="last")


def sort_study_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    sorted_df = df.copy()
    for column in ("year", "first_author", "study_id"):
        if column not in sorted_df.columns:
            sorted_df[column] = ""

    sorted_df["_sort_year"] = pd.to_numeric(sorted_df["year"], errors="coerce").fillna(9999)
    sorted_df["_sort_author"] = sorted_df["first_author"].fillna("").astype(str).str.lower()
    sorted_df["_sort_study_id"] = sorted_df["study_id"].fillna("").astype(str).str.lower()
    sorted_df = sorted_df.sort_values(
        by=["_sort_year", "_sort_author", "_sort_study_id"], kind="stable"
    )
    return sorted_df.drop(columns=["_sort_year", "_sort_author", "_sort_study_id"])

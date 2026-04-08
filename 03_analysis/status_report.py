import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

EMPTY_VALUES = {"", "nan", "none"}
YES_VALUES = {"yes", "y", "1", "true"}
VALID_DUPLICATE_VALUES = YES_VALUES | {"no", "n", "0", "false"}
PROSPERO_ID_PATTERN = re.compile(r"\bCRD\d{8,14}\b", re.IGNORECASE)
DEMO_RECORD_ID_PATTERN = re.compile(r"^MR_DEMO_", re.IGNORECASE)
DEMO_TEXT_PATTERN = re.compile(r"\b(demo|placeholder)\b", re.IGNORECASE)
PLACEHOLDER_TOKEN_PATTERN = re.compile(r"\[([A-Z][A-Z0-9_ ]{2,})\]")
EXTRACTION_ERRORS_PATTERN = re.compile(r"-\s*Errors:\s*(\d+)", re.IGNORECASE)
EXTRACTION_WARNINGS_PATTERN = re.compile(r"-\s*Warnings:\s*(\d+)", re.IGNORECASE)
REVIEWER_WORKLOAD_STAGE_PATTERN = re.compile(r"-\s*Stage filter:\s*`([^`]+)`", re.IGNORECASE)
REVIEWER_WORKLOAD_REVIEWERS_PATTERN = re.compile(r"-\s*Reviewers observed:\s*(\d+)", re.IGNORECASE)
REVIEWER_WORKLOAD_TOTAL_PATTERN = re.compile(
    r"-\s*Total screened records in scope:\s*(\d+)", re.IGNORECASE
)
REVIEWER_WORKLOAD_NON_BLOCKING_PATTERN = re.compile(
    r"-\s*Non-blocking fallback active:\s*(yes|no)",
    re.IGNORECASE,
)
PRISMA_KEYS = [
    "records_identified_databases",
    "duplicates_removed",
    "records_screened_title_abstract",
    "records_excluded_title_abstract",
    "reports_assessed_full_text",
    "studies_included_qualitative_synthesis",
]
MASTER_RECORD_COLUMNS = [
    "record_id",
    "source_database",
    "source_record_id",
    "title",
    "abstract",
    "authors",
    "year",
    "journal",
    "doi",
    "pmid",
    "normalized_title",
    "normalized_first_author",
    "is_duplicate",
    "duplicate_of_record_id",
    "dedup_reason",
    "notes",
]
PROJECT_POSTURE_SUMMARY_EN = "Production-capable review scaffold with enforced status integrity, but not yet domain-instantiated."
PROJECT_POSTURE_SUMMARY_RU = (
    "архитектурно зрелый и дисциплинированный каркас systematic review, "
    "готовый к переходу в production после подстановки реальных исследовательских параметров."
)
VALID_REVIEW_MODES = {"template", "production"}


def normalize_review_mode(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if text in VALID_REVIEW_MODES:
        return text
    return "template"


def parse_json_object_stream(text: str) -> tuple[list[dict], str | None]:
    decoder = json.JSONDecoder()
    index = 0
    length = len(text)
    objects: list[dict] = []

    while index < length:
        while index < length and text[index].isspace():
            index += 1
        if index >= length:
            break

        try:
            parsed, consumed = decoder.raw_decode(text, index)
        except json.JSONDecodeError as exc:
            return objects, str(exc)

        if not isinstance(parsed, dict):
            return objects, "JSON stream element is not an object."

        objects.append(parsed)
        index = consumed

    if not objects:
        return [], "No JSON object found in stream."

    return objects, None


def is_non_empty_value(value: object) -> bool:
    return str(value).strip().lower() not in EMPTY_VALUES


def non_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    mask = df.apply(lambda row: any(is_non_empty_value(value) for value in row), axis=1)
    return df[mask].copy()


def read_csv_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def safe_sum(df: pd.DataFrame, column: str) -> int:
    if column not in df.columns or df.empty:
        return 0
    series = pd.to_numeric(df[column], errors="coerce").fillna(0)
    return int(series.sum())


def parse_yes_count(df: pd.DataFrame, column: str) -> int:
    if column not in df.columns or df.empty:
        return 0
    values = df[column].fillna("").astype(str).str.strip().str.lower()
    return int(values.isin({"yes", "y", "1", "true"}).sum())


def normalize_count_value(value: object) -> str:
    text = str(value).strip()
    if text.lower() in EMPTY_VALUES:
        return ""

    numeric = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.notna(numeric):
        as_float = float(numeric)
        if as_float.is_integer():
            return str(int(as_float))
        return f"{as_float:g}"

    return text


def parse_int_or_none(value: str) -> int | None:
    if value.strip() == "":
        return None
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return int(float(numeric))


def prisma_value(prisma_df: pd.DataFrame, stage: str) -> str:
    if prisma_df.empty or "stage" not in prisma_df.columns or "count" not in prisma_df.columns:
        return ""

    mask = prisma_df["stage"].fillna("").astype(str).str.strip().eq(stage)
    if not mask.any():
        return ""

    value = prisma_df.loc[mask, "count"].iloc[0]
    return normalize_count_value(value)


def prisma_int(prisma_df: pd.DataFrame, stage: str) -> int | None:
    return parse_int_or_none(prisma_value(prisma_df, stage))


def count_or_dash(value: int | None) -> str:
    return str(value) if value is not None else "—"


def pct(part: int | None, whole: int | None) -> str:
    if part is None or whole is None or whole <= 0:
        return "—"
    return f"{(100.0 * part / whole):.1f}%"


def unique_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def artifact_entry(path: Path) -> dict:
    if not path.exists():
        return {
            "path": path.as_posix(),
            "present": False,
            "updated": None,
        }

    updated = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
    return {
        "path": path.as_posix(),
        "present": True,
        "updated": updated,
    }


def artifact_status_line(artifact: dict) -> str:
    if not artifact["present"]:
        return f"- `{artifact['path']}`: missing"
    return f"- `{artifact['path']}`: present (updated {artifact['updated']})"


def parse_daily_run_manifest(path: Path) -> dict:
    manifest = {
        "path": path.as_posix(),
        "present": path.exists(),
        "parsed": False,
        "stream_object_count": 0,
        "run_id": None,
        "state": None,
        "started_at_utc": None,
        "updated_at_utc": None,
        "pipeline_exit_code": None,
        "status_checkpoint_exit_code": None,
        "final_exit_code": None,
        "failure_phase": None,
        "rollback_applied": None,
        "transactional_mode": None,
        "message": None,
    }

    if not path.exists():
        manifest["message"] = "Daily-run manifest is missing."
        return manifest

    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        manifest["message"] = f"Failed to read daily-run manifest: {exc}"
        return manifest

    payloads, parse_error = parse_json_object_stream(raw_text)
    if parse_error is not None:
        manifest["message"] = f"Failed to parse daily-run manifest: {parse_error}"
        return manifest

    payload = payloads[-1]
    manifest["stream_object_count"] = len(payloads)

    manifest["parsed"] = True
    manifest["run_id"] = str(payload.get("run_id", "")).strip() or None
    manifest["state"] = str(payload.get("state", "")).strip() or None
    manifest["started_at_utc"] = str(payload.get("started_at_utc", "")).strip() or None
    manifest["updated_at_utc"] = str(payload.get("updated_at_utc", "")).strip() or None
    manifest["pipeline_exit_code"] = payload.get("pipeline_exit_code")
    manifest["status_checkpoint_exit_code"] = payload.get("status_checkpoint_exit_code")
    manifest["final_exit_code"] = payload.get("final_exit_code")
    manifest["failure_phase"] = str(payload.get("failure_phase", "")).strip() or None
    rollback_value = payload.get("rollback_applied")
    if isinstance(rollback_value, bool):
        manifest["rollback_applied"] = rollback_value
    elif isinstance(rollback_value, str):
        normalized_rollback = rollback_value.strip().lower()
        if normalized_rollback in {"true", "1", "yes", "y"}:
            manifest["rollback_applied"] = True
        elif normalized_rollback in {"false", "0", "no", "n"}:
            manifest["rollback_applied"] = False
    manifest["transactional_mode"] = str(payload.get("transactional_mode", "")).strip() or None
    if manifest["stream_object_count"] > 1:
        manifest["message"] = (
            f"Parsed daily-run manifest from concatenated object stream "
            f"(count={manifest['stream_object_count']}); using last object."
        )
    else:
        manifest["message"] = "Parsed daily-run manifest."
    return manifest


def parse_daily_run_failed_marker(path: Path) -> dict:
    marker = {
        "path": path.as_posix(),
        "present": path.exists(),
        "parsed": False,
        "stream_object_count": 0,
        "run_id": None,
        "failed_at_utc": None,
        "pipeline_exit_code": None,
        "status_checkpoint_exit_code": None,
        "final_exit_code": None,
        "failure_phase": None,
        "message": None,
    }

    if not path.exists():
        marker["message"] = "Daily-run failed marker is absent."
        return marker

    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        marker["message"] = f"Failed to read daily-run failed marker: {exc}"
        return marker

    payloads, parse_error = parse_json_object_stream(raw_text)
    if parse_error is not None:
        marker["message"] = f"Failed to parse daily-run failed marker: {parse_error}"
        return marker

    payload = payloads[-1]
    marker["stream_object_count"] = len(payloads)

    marker["parsed"] = True
    marker["run_id"] = str(payload.get("run_id", "")).strip() or None
    marker["failed_at_utc"] = str(payload.get("failed_at_utc", "")).strip() or None
    marker["pipeline_exit_code"] = payload.get("pipeline_exit_code")
    marker["status_checkpoint_exit_code"] = payload.get("status_checkpoint_exit_code")
    marker["final_exit_code"] = payload.get("final_exit_code")
    marker["failure_phase"] = str(payload.get("failure_phase", "")).strip() or None
    if marker["stream_object_count"] > 1:
        marker["message"] = (
            f"Parsed daily-run failed marker from concatenated object stream "
            f"(count={marker['stream_object_count']}); using last object."
        )
    else:
        marker["message"] = "Parsed daily-run failed marker."
    return marker


def parse_extraction_validation_summary(path: Path) -> dict:
    summary = {
        "path": path.as_posix(),
        "present": path.exists(),
        "parsed": False,
        "errors": None,
        "warnings": None,
        "message": None,
    }

    if not path.exists():
        summary["message"] = "Extraction validation summary is missing."
        return summary

    text = path.read_text(encoding="utf-8")
    errors_match = EXTRACTION_ERRORS_PATTERN.search(text)
    warnings_match = EXTRACTION_WARNINGS_PATTERN.search(text)

    if not errors_match or not warnings_match:
        summary["message"] = "Could not parse errors/warnings counters."
        return summary

    summary["parsed"] = True
    summary["errors"] = int(errors_match.group(1))
    summary["warnings"] = int(warnings_match.group(1))
    summary["message"] = "Parsed validation counters successfully."
    return summary


def parse_csv_input_validation_summary(path: Path) -> dict:
    summary = {
        "path": path.as_posix(),
        "present": path.exists(),
        "parsed": False,
        "errors": None,
        "warnings": None,
        "message": None,
    }

    if not path.exists():
        summary["message"] = "CSV input validation summary is missing."
        return summary

    text = path.read_text(encoding="utf-8")
    errors_match = EXTRACTION_ERRORS_PATTERN.search(text)
    warnings_match = EXTRACTION_WARNINGS_PATTERN.search(text)

    if not errors_match or not warnings_match:
        summary["message"] = "Could not parse errors/warnings counters."
        return summary

    summary["parsed"] = True
    summary["errors"] = int(errors_match.group(1))
    summary["warnings"] = int(warnings_match.group(1))
    summary["message"] = "Parsed validation counters successfully."
    return summary


def parse_reviewer_workload_balancer_summary(path: Path) -> dict:
    summary = {
        "path": path.as_posix(),
        "present": path.exists(),
        "parsed": False,
        "stage_filter": None,
        "reviewers_observed": None,
        "total_screened_records_in_scope": None,
        "non_blocking_fallback_active": None,
        "message": None,
    }

    if not path.exists():
        summary["message"] = "Reviewer workload balancer summary is missing."
        return summary

    text = path.read_text(encoding="utf-8")
    stage_match = REVIEWER_WORKLOAD_STAGE_PATTERN.search(text)
    reviewers_match = REVIEWER_WORKLOAD_REVIEWERS_PATTERN.search(text)
    total_match = REVIEWER_WORKLOAD_TOTAL_PATTERN.search(text)
    non_blocking_match = REVIEWER_WORKLOAD_NON_BLOCKING_PATTERN.search(text)

    if not stage_match or not reviewers_match or not total_match or not non_blocking_match:
        summary["message"] = "Could not parse reviewer workload metrics."
        return summary

    summary["parsed"] = True
    summary["stage_filter"] = stage_match.group(1).strip()
    summary["reviewers_observed"] = int(reviewers_match.group(1))
    summary["total_screened_records_in_scope"] = int(total_match.group(1))
    summary["non_blocking_fallback_active"] = non_blocking_match.group(1).strip().lower() == "yes"
    summary["message"] = "Parsed reviewer workload metrics successfully."
    return summary


def checklist_details(value: str) -> str:
    return value if value.strip() else "No details"


def parse_prospero_registration(protocol_path: Path) -> dict:
    registration = {
        "protocol_path": protocol_path.as_posix(),
        "protocol_present": protocol_path.exists(),
        "registered": False,
        "registration_id": None,
    }

    if not protocol_path.exists():
        return registration

    text = protocol_path.read_text(encoding="utf-8")
    match = PROSPERO_ID_PATTERN.search(text)
    if match:
        registration["registered"] = True
        registration["registration_id"] = match.group(0).upper()

    return registration


def reviewer_set(df: pd.DataFrame, column: str = "reviewer") -> set[str]:
    if df.empty or column not in df.columns:
        return set()
    values = df[column].fillna("").astype(str).str.strip()
    return {value for value in values if value}


def title_abstract_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "stage" not in df.columns:
        return pd.DataFrame(columns=df.columns)

    stage_series = df["stage"].fillna("").astype(str).str.strip().str.lower()
    mask = stage_series.str.contains("title") | stage_series.str.contains("abstract")
    return df[mask].copy()


def cohen_kappa_from_screening_log(df: pd.DataFrame) -> dict:
    result = {
        "available": False,
        "pair": None,
        "paired_records": 0,
        "kappa": None,
        "reason": "No record-level screening data found.",
    }

    required = {"record_id", "reviewer", "title_abstract_decision"}
    if df.empty:
        return result
    if not required.issubset(df.columns):
        result["reason"] = "Missing required columns in screening log template."
        return result

    log = df.copy()
    log["record_id"] = log["record_id"].fillna("").astype(str).str.strip()
    log["reviewer"] = log["reviewer"].fillna("").astype(str).str.strip()
    log["title_abstract_decision"] = (
        log["title_abstract_decision"].fillna("").astype(str).str.strip().str.lower()
    )

    log = log[
        log["record_id"].ne("") & log["reviewer"].ne("") & log["title_abstract_decision"].ne("")
    ]
    if log.empty:
        result["reason"] = "No non-empty title/abstract decisions in record-level log."
        return result

    deduped = log.drop_duplicates(["record_id", "reviewer"], keep="last")
    reviewers = sorted(deduped["reviewer"].unique())
    if len(reviewers) < 2:
        result["reason"] = "Fewer than two reviewers in record-level title/abstract decisions."
        return result

    pivot = deduped.pivot(index="record_id", columns="reviewer", values="title_abstract_decision")

    best_pair: tuple[str, str] | None = None
    best_overlap = 0
    for index, reviewer_a in enumerate(reviewers):
        for reviewer_b in reviewers[index + 1 :]:
            overlap = int(pivot[[reviewer_a, reviewer_b]].dropna().shape[0])
            if overlap > best_overlap:
                best_overlap = overlap
                best_pair = (reviewer_a, reviewer_b)

    if not best_pair or best_overlap == 0:
        result["reason"] = "No overlapping records with independent title/abstract decisions."
        return result

    pair_df = pivot[[best_pair[0], best_pair[1]]].dropna()
    decisions_a = pair_df[best_pair[0]]
    decisions_b = pair_df[best_pair[1]]

    observed = float((decisions_a == decisions_b).mean())
    categories = sorted(set(decisions_a.unique()) | set(decisions_b.unique()))
    expected = 0.0
    for category in categories:
        p_a = float((decisions_a == category).mean())
        p_b = float((decisions_b == category).mean())
        expected += p_a * p_b

    if expected >= 1.0:
        result["reason"] = "Cohen's kappa undefined because expected agreement is 1.0."
        return result

    kappa = (observed - expected) / (1.0 - expected)
    result["available"] = True
    result["pair"] = f"{best_pair[0]} vs {best_pair[1]}"
    result["paired_records"] = int(pair_df.shape[0])
    result["kappa"] = round(float(kappa), 3)
    result["reason"] = "Computed from record-level title/abstract decisions."
    return result


def master_column_structure(df: pd.DataFrame) -> dict:
    actual_columns = [str(column).strip() for column in df.columns.tolist()]
    missing = [column for column in MASTER_RECORD_COLUMNS if column not in actual_columns]
    extra = [column for column in actual_columns if column not in MASTER_RECORD_COLUMNS]
    order_matches = actual_columns == MASTER_RECORD_COLUMNS
    matches = not missing and not extra and order_matches

    details_parts: list[str] = []
    if missing:
        details_parts.append("Missing: " + ", ".join(missing))
    if extra:
        details_parts.append("Extra: " + ", ".join(extra))
    if not order_matches and not missing and not extra:
        details_parts.append("Column order differs from template")

    if not details_parts and matches:
        details_parts.append("Columns match dedup workflow template")

    return {
        "matches": matches,
        "missing": missing,
        "extra": extra,
        "order_matches": order_matches,
        "actual_columns": actual_columns,
        "details": "; ".join(details_parts),
    }


def has_demo_like_master_records(df: pd.DataFrame) -> bool:
    if df.empty:
        return False

    text_columns = [
        column
        for column in ["record_id", "source_record_id", "title", "abstract", "notes"]
        if column in df.columns
    ]
    if not text_columns:
        return False

    for _, row in df[text_columns].iterrows():
        record_id = str(row.get("record_id", "")).strip()
        if record_id and DEMO_RECORD_ID_PATTERN.match(record_id):
            return True

        for column in text_columns:
            value = str(row.get(column, "")).strip()
            if value and DEMO_TEXT_PATTERN.search(value):
                return True

    return False


def assess_project_stage(
    *,
    identified_total: int,
    unique_records: int,
    sessions: int,
    screened_records: int,
    includes: int,
    demo_like_master: bool,
    analytics_artifacts_ready: bool,
) -> dict:
    if includes > 0 or screened_records > 0 or sessions > 0:
        stage_id = "screening_or_beyond"
        label = "Screening In Progress / Later"
    elif unique_records > 0 and identified_total > 0:
        stage_id = "pre_screening_ready"
        label = "Pre-screening Ready"
    elif unique_records > 0 and identified_total == 0 and demo_like_master:
        stage_id = "bootstrap_demo"
        label = "Bootstrap / Demo Calibration"
    elif unique_records > 0 and identified_total == 0:
        stage_id = "pre_screening_unlogged_search"
        label = "Pre-screening (Search Totals Pending)"
    elif identified_total > 0:
        stage_id = "search_logged_merge_pending"
        label = "Search Logged (Dedup Merge Pending)"
    else:
        stage_id = "template_setup"
        label = "Template Setup"

    reasons: list[str] = []

    if stage_id == "bootstrap_demo":
        reasons.append(
            "Master records include demo/template markers while `search_log.csv` totals remain zero."
        )
    elif stage_id == "pre_screening_unlogged_search":
        reasons.append("Master records exist, but `search_log.csv` totals are still zero.")
    elif stage_id == "pre_screening_ready":
        reasons.append(
            "Search totals and deduplicated records are available; screening has not started yet."
        )
    elif stage_id == "screening_or_beyond":
        reasons.append("Screening decisions are logged, so the review moved beyond setup stage.")
    elif stage_id == "search_logged_merge_pending":
        reasons.append(
            "Search totals are logged, but deduplicated master records are not populated yet."
        )
    else:
        reasons.append(
            "Protocol/search templates are present, but no operational review data is logged yet."
        )

    if analytics_artifacts_ready and stage_id in {
        "template_setup",
        "bootstrap_demo",
        "pre_screening_unlogged_search",
        "search_logged_merge_pending",
    }:
        reasons.append(
            "Analysis artifacts are present, but this stage is still setup/calibration rather than a completed evidence synthesis."
        )

    return {
        "id": stage_id,
        "label": label,
        "reasons": reasons,
    }


def unresolved_placeholder_tokens(path: Path) -> list[str]:
    if not path.exists():
        return []

    text = path.read_text(encoding="utf-8")
    tokens: list[str] = []

    for match in PLACEHOLDER_TOKEN_PATTERN.finditer(text):
        token = match.group(1).strip()
        if not any(char.isalpha() for char in token):
            continue
        if "_" not in token and " " not in token and len(token) < 4:
            continue
        tokens.append(f"[{token}]")

    return unique_preserve_order(tokens)


def assess_semantic_completeness(protocol_path: Path, manuscript_path: Path | None) -> dict:
    protocol_tokens = unresolved_placeholder_tokens(protocol_path)
    manuscript_tokens = (
        unresolved_placeholder_tokens(manuscript_path) if manuscript_path is not None else []
    )

    complete = not protocol_tokens and not manuscript_tokens

    return {
        "complete": complete,
        "primary_blocker": None if complete else "semantic_completeness",
        "protocol_path": protocol_path.as_posix(),
        "manuscript_path": manuscript_path.as_posix() if manuscript_path is not None else None,
        "protocol_placeholder_count": len(protocol_tokens),
        "manuscript_placeholder_count": len(manuscript_tokens),
        "placeholder_examples": (protocol_tokens + manuscript_tokens)[:8],
    }


def build_project_posture(stage_assessment: dict, semantic_completeness: dict) -> dict:
    if semantic_completeness["complete"]:
        summary_en = "Domain-instantiated review workflow with enforced status integrity."
        summary_ru = (
            "доменно заполненный и дисциплинированный workflow systematic review "
            "с включёнными контрольными статус-гейтами."
        )
    else:
        summary_en = PROJECT_POSTURE_SUMMARY_EN
        summary_ru = PROJECT_POSTURE_SUMMARY_RU

    return {
        "summary_en": summary_en,
        "summary_ru": summary_ru,
        "stage_id": stage_assessment["id"],
        "stage_label": stage_assessment["label"],
        "primary_blocker": semantic_completeness["primary_blocker"],
        "semantic_completeness": semantic_completeness,
    }


def build_status_report(
    screening_df: pd.DataFrame,
    screening_records_df: pd.DataFrame,
    master_df: pd.DataFrame,
    search_df: pd.DataFrame,
    prisma_df: pd.DataFrame,
    protocol_path: Path,
    screening_summary_path: Path,
    csv_input_validation_summary_path: Path,
    extraction_validation_summary_path: Path,
    quality_appraisal_summary_path: Path,
    quality_appraisal_scored_path: Path,
    effect_size_conversion_summary_path: Path,
    effect_size_converted_path: Path,
    dedup_summary_path: Path,
    prisma_flow_path: Path,
    reviewer_workload_summary_path: Path | None = None,
    manuscript_path: Path | None = None,
    daily_run_manifest_path: Path | None = None,
    daily_run_failed_marker_path: Path | None = None,
    review_mode: str | None = None,
) -> tuple[str, dict]:
    screening_non_empty = non_empty_rows(screening_df)
    screening_records_non_empty = non_empty_rows(screening_records_df)
    master_non_empty = non_empty_rows(master_df)
    search_non_empty = non_empty_rows(search_df)

    sessions = len(screening_non_empty)
    screened_records = safe_sum(screening_non_empty, "records_screened")
    includes = safe_sum(screening_non_empty, "include_n")
    excludes = safe_sum(screening_non_empty, "exclude_n")
    maybe = safe_sum(screening_non_empty, "maybe_n")
    pending = safe_sum(screening_non_empty, "pending_n")

    master_rows = len(master_non_empty)
    duplicates = parse_yes_count(master_non_empty, "is_duplicate")
    unique_records = max(master_rows - duplicates, 0)
    demo_like_master = has_demo_like_master_records(master_non_empty)

    identified_total = safe_sum(search_non_empty, "results_total")

    latest_search_date = "—"
    if "date_searched" in search_non_empty.columns and not search_non_empty.empty:
        date_series = search_non_empty["date_searched"].fillna("").astype(str).str.strip()
        date_series = date_series[date_series.ne("")]
        if not date_series.empty:
            latest_search_date = date_series.max()

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    prospero_registration = parse_prospero_registration(protocol_path)

    title_abstract_df = title_abstract_rows(screening_non_empty)
    title_abstract_sessions = len(title_abstract_df)
    title_abstract_reviewers = reviewer_set(title_abstract_df)
    all_reviewers = reviewer_set(screening_non_empty)
    kappa_stats = cohen_kappa_from_screening_log(screening_records_non_empty)
    master_structure = master_column_structure(master_df)
    csv_input_validation = parse_csv_input_validation_summary(csv_input_validation_summary_path)
    extraction_validation = parse_extraction_validation_summary(extraction_validation_summary_path)
    if reviewer_workload_summary_path is None:
        reviewer_workload_summary_path = (
            screening_summary_path.parent / "reviewer_workload_balancer_summary.md"
        )
    reviewer_workload_balancer = parse_reviewer_workload_balancer_summary(
        reviewer_workload_summary_path
    )

    prisma_counts = {key: prisma_int(prisma_df, key) for key in PRISMA_KEYS}
    prisma_identified = prisma_counts["records_identified_databases"]
    prisma_duplicates = prisma_counts["duplicates_removed"]
    prisma_screened = prisma_counts["records_screened_title_abstract"]

    prisma_progress = [
        {
            "label": "identified_from_databases",
            "count": prisma_identified,
            "percent": "—",
            "percent_base": None,
        },
        {
            "label": "duplicates_removed",
            "count": prisma_duplicates,
            "percent": pct(prisma_duplicates, prisma_identified),
            "percent_base": "identified_from_databases",
        },
        {
            "label": "screened_title_abstract",
            "count": prisma_screened,
            "percent": pct(prisma_screened, prisma_identified),
            "percent_base": "identified_from_databases",
        },
        {
            "label": "excluded_title_abstract",
            "count": prisma_counts["records_excluded_title_abstract"],
            "percent": pct(prisma_counts["records_excluded_title_abstract"], prisma_screened),
            "percent_base": "screened_title_abstract",
        },
        {
            "label": "assessed_full_text",
            "count": prisma_counts["reports_assessed_full_text"],
            "percent": pct(prisma_counts["reports_assessed_full_text"], prisma_screened),
            "percent_base": "screened_title_abstract",
        },
        {
            "label": "included_qualitative",
            "count": prisma_counts["studies_included_qualitative_synthesis"],
            "percent": pct(
                prisma_counts["studies_included_qualitative_synthesis"],
                prisma_counts["reports_assessed_full_text"],
            ),
            "percent_base": "assessed_full_text",
        },
    ]

    health_checks: list[dict] = []
    warnings: list[str] = []

    def add_health(level: str, message: str) -> None:
        health_checks.append({"level": level, "message": message})

    if identified_total > 0:
        add_health("ok", "Search totals are filled (`results_total` > 0).")
    else:
        add_health("warning", "Search totals are empty (`results_total` = 0).")
        warnings.append("Enter `results_total` values in `02_data/processed/search_log.csv`.")

    if duplicates <= master_rows:
        add_health("ok", "Dedup flags are internally consistent (`duplicates <= master rows`).")
    else:
        add_health("error", "Dedup inconsistency: duplicates exceed master rows.")
        warnings.append(
            "Fix duplicate flags in `02_data/processed/master_records.csv` (`duplicates` cannot exceed rows)."
        )

    if master_structure["matches"]:
        add_health("ok", "Master record columns match dedup workflow/template.")
    elif not master_structure["actual_columns"]:
        add_health(
            "warning", "Master record header is missing; expected dedup columns are not present."
        )
        warnings.append(
            "Restore header columns in `02_data/processed/master_records.csv` from `02_data/processed/master_records_template.csv`."
        )
    else:
        add_health(
            "warning", f"Master record columns differ from template: {master_structure['details']}."
        )
        warnings.append(
            "Align `02_data/processed/master_records.csv` columns with `01_protocol/dedup_workflow.md` (or copy header from `02_data/processed/master_records_template.csv`)."
        )

    screening_decision_total = includes + excludes + maybe + pending
    if sessions == 0:
        add_health("info", "Screening sessions are not logged yet.")
        warnings.append("Log at least one session in `02_data/processed/screening_daily_log.csv`.")
    elif screening_decision_total == screened_records:
        add_health(
            "ok",
            "Screening totals are consistent (`include + exclude + maybe + pending = screened`).",
        )
    else:
        add_health(
            "error",
            "Screening totals mismatch (`include + exclude + maybe + pending != screened`).",
        )
        warnings.append(
            "Reconcile screening counts in `02_data/processed/screening_daily_log.csv` so decision totals match `records_screened`."
        )

    if not prospero_registration["protocol_present"]:
        add_health("error", "Protocol file is missing; cannot verify PROSPERO registration.")
        warnings.append(
            "Restore `01_protocol/protocol.md` and record PROSPERO registration ID before screening."
        )
    elif prospero_registration["registered"]:
        registration_id = prospero_registration["registration_id"]
        add_health("ok", f"PROSPERO registration recorded (`{registration_id}`).")
    elif sessions > 0 or screened_records > 0:
        add_health("error", "PROSPERO registration ID is missing and screening has started.")
        warnings.append(
            "Register protocol in PROSPERO immediately and record the ID (e.g., `CRD420...`) in `01_protocol/protocol.md`; preregistration is usually required before screening."
        )
    else:
        add_health(
            "warning",
            "PROSPERO registration ID is missing; register before first screening session.",
        )
        warnings.append(
            "Register protocol in PROSPERO before screening and record the ID in `01_protocol/protocol.md`."
        )

    if title_abstract_sessions > 0 and len(title_abstract_reviewers) >= 2:
        add_health(
            "ok",
            f"Independent title/abstract screening logged with {len(title_abstract_reviewers)} reviewers.",
        )
    elif title_abstract_sessions > 0 and len(title_abstract_reviewers) == 1:
        reviewer = next(iter(title_abstract_reviewers))
        add_health(
            "warning",
            f"Only one title/abstract reviewer logged ({reviewer}); many journals expect independent dual screening.",
        )
        warnings.append(
            "Add an independent second reviewer for title/abstract screening and report Cohen's kappa; if this remains a single-reviewer pilot, state it explicitly as a limitation."
        )
    elif title_abstract_sessions == 0 and sessions == 0:
        add_health(
            "info",
            "Title/abstract screening has not started; assign a second independent reviewer before launch.",
        )
    elif title_abstract_sessions == 0 and sessions > 0:
        add_health(
            "warning",
            "No title/abstract stage labels detected; dual-screening status cannot be verified.",
        )
        warnings.append(
            "Fill the `stage` column (e.g., `title_abstract`) in `02_data/processed/screening_daily_log.csv` so reviewer independence can be tracked."
        )

    if kappa_stats["available"]:
        add_health(
            "ok",
            f"Cohen's kappa is available: {kappa_stats['kappa']} ({kappa_stats['pair']}, n={kappa_stats['paired_records']}).",
        )
    elif title_abstract_sessions > 0:
        add_health("warning", f"Cohen's kappa is not available yet: {kappa_stats['reason']}")
        warnings.append(
            "Log paired title/abstract decisions by record and reviewer in `02_data/processed/screening_title_abstract_dual_log.csv` to compute Cohen's kappa."
        )

    csv_input_validation_ok = False
    csv_input_validation_detail = "Validation summary not found."
    if not csv_input_validation["present"]:
        add_health("error", "CSV input validation summary is missing.")
        warnings.append(
            "Run CSV input validation (`python 03_analysis/validate_csv_inputs.py`) or `make daily` to regenerate `03_analysis/outputs/csv_input_validation_summary.md`."
        )
    elif not csv_input_validation["parsed"]:
        add_health(
            "warning", "CSV input validation summary exists, but counters could not be parsed."
        )
        warnings.append(
            "Regenerate `03_analysis/outputs/csv_input_validation_summary.md` with `python 03_analysis/validate_csv_inputs.py`."
        )
        csv_input_validation_detail = "Summary present, parse failed"
    else:
        csv_input_errors = int(csv_input_validation["errors"] or 0)
        csv_input_warnings = int(csv_input_validation["warnings"] or 0)
        csv_input_validation_detail = f"errors={csv_input_errors}; warnings={csv_input_warnings}"
        if csv_input_errors > 0:
            add_health(
                "error",
                f"CSV input validation reports {csv_input_errors} error(s) and {csv_input_warnings} warning(s).",
            )
            warnings.append(
                "Fix CSV input validation errors in `03_analysis/outputs/csv_input_validation_summary.md` and rerun `make daily`."
            )
        elif csv_input_warnings > 0:
            add_health(
                "warning",
                f"CSV input validation reports {csv_input_warnings} warning(s) (errors: 0).",
            )
            warnings.append(
                "Review CSV input warnings in `03_analysis/outputs/csv_input_validation_summary.md` and update processed CSV files where needed."
            )
        else:
            add_health("ok", "CSV input validation passed (0 errors, 0 warnings).")
            csv_input_validation_ok = True

    extraction_validation_ok = False
    extraction_validation_detail = "Validation summary not found."
    if not extraction_validation["present"]:
        add_health("error", "Extraction validation summary is missing.")
        warnings.append(
            "Run extraction validation (`python 03_analysis/validate_extraction.py`) or `make daily` to regenerate `03_analysis/outputs/extraction_validation_summary.md`."
        )
    elif not extraction_validation["parsed"]:
        add_health(
            "warning", "Extraction validation summary exists, but counters could not be parsed."
        )
        warnings.append(
            "Regenerate `03_analysis/outputs/extraction_validation_summary.md` with `python 03_analysis/validate_extraction.py`."
        )
        extraction_validation_detail = "Summary present, parse failed"
    else:
        extraction_errors = int(extraction_validation["errors"] or 0)
        extraction_warnings = int(extraction_validation["warnings"] or 0)
        extraction_validation_detail = f"errors={extraction_errors}; warnings={extraction_warnings}"
        if extraction_errors > 0:
            add_health(
                "error",
                f"Extraction validation reports {extraction_errors} error(s) and {extraction_warnings} warning(s).",
            )
            warnings.append(
                "Fix extraction validation errors in `02_data/codebook/extraction_template.csv` and rerun `make daily`."
            )
        elif extraction_warnings > 0:
            add_health(
                "warning",
                f"Extraction validation reports {extraction_warnings} warning(s) (errors: 0).",
            )
            warnings.append(
                "Review extraction warnings in `03_analysis/outputs/extraction_validation_summary.md` and update extraction coding where needed."
            )
        else:
            add_health("ok", "Extraction validation passed (0 errors, 0 warnings).")
            extraction_validation_ok = True

    quality_appraisal_summary_present = quality_appraisal_summary_path.exists()
    quality_appraisal_scored_present = quality_appraisal_scored_path.exists()
    quality_appraisal_ok = quality_appraisal_summary_present and quality_appraisal_scored_present
    quality_appraisal_detail = (
        f"summary: {'present' if quality_appraisal_summary_present else 'missing'}; "
        f"scored CSV: {'present' if quality_appraisal_scored_present else 'missing'}"
    )
    if quality_appraisal_ok:
        add_health("ok", "Quality appraisal artifacts are present (summary + scored CSV).")
    else:
        missing_quality: list[str] = []
        if not quality_appraisal_summary_present:
            missing_quality.append(f"`{quality_appraisal_summary_path.as_posix()}`")
        if not quality_appraisal_scored_present:
            missing_quality.append(f"`{quality_appraisal_scored_path.as_posix()}`")
        joined_missing_quality = ", ".join(missing_quality)
        add_health("error", f"Quality appraisal artifact(s) missing: {joined_missing_quality}.")
        warnings.append(
            "Run quality appraisal (`python 03_analysis/quality_appraisal.py`) or `make quality` to regenerate missing outputs."
        )

    effect_size_summary_present = effect_size_conversion_summary_path.exists()
    effect_size_converted_present = effect_size_converted_path.exists()
    effect_size_conversion_ok = effect_size_summary_present and effect_size_converted_present
    effect_size_conversion_detail = (
        f"summary: {'present' if effect_size_summary_present else 'missing'}; "
        f"converted CSV: {'present' if effect_size_converted_present else 'missing'}"
    )
    if effect_size_conversion_ok:
        add_health("ok", "Effect-size conversion artifacts are present (summary + converted CSV).")
    else:
        missing_effect_size: list[str] = []
        if not effect_size_summary_present:
            missing_effect_size.append(f"`{effect_size_conversion_summary_path.as_posix()}`")
        if not effect_size_converted_present:
            missing_effect_size.append(f"`{effect_size_converted_path.as_posix()}`")
        joined_missing_effect_size = ", ".join(missing_effect_size)
        add_health(
            "error", f"Effect-size conversion artifact(s) missing: {joined_missing_effect_size}."
        )
        warnings.append(
            "Run effect-size conversion (`python 03_analysis/effect_size_converter.py`) or `make daily` to regenerate missing outputs."
        )

    missing_prisma_keys = [
        key
        for key in [
            "records_identified_databases",
            "duplicates_removed",
            "records_screened_title_abstract",
        ]
        if prisma_counts[key] is None
    ]

    mismatch_messages: list[str] = []
    prisma_sync_ok = False
    identified_check_deferred = False
    screened_progress_note: str | None = None

    if missing_prisma_keys:
        joined = ", ".join(f"`{key}`" for key in missing_prisma_keys)
        add_health("warning", f"PRISMA key counts missing: {joined}.")
        warnings.append(
            f"Fill PRISMA counts for: {joined} in `02_data/processed/prisma_counts_template.csv`."
        )
    else:
        if identified_total > 0:
            if prisma_identified != identified_total:
                mismatch_messages.append(
                    f"`records_identified_databases` is {prisma_identified}, but `search_log.csv` sums to {identified_total}"
                )
        else:
            identified_check_deferred = True
            add_health(
                "warning",
                "PRISMA identified count cannot be fully verified while `search_log.csv` totals remain 0.",
            )
        if prisma_duplicates != duplicates:
            mismatch_messages.append(
                f"`duplicates_removed` is {prisma_duplicates}, but `master_records.csv` flags {duplicates} duplicates"
            )
        if prisma_screened is not None:
            if prisma_screened > unique_records:
                mismatch_messages.append(
                    f"`records_screened_title_abstract` is {prisma_screened}, which exceeds computed unique records ({unique_records})"
                )
            elif prisma_screened < unique_records:
                screened_progress_note = (
                    f"Title/abstract screening is in progress: `records_screened_title_abstract`={prisma_screened} "
                    f"of {unique_records} unique deduplicated records."
                )
                add_health("info", screened_progress_note)

        if mismatch_messages:
            add_health("error", "PRISMA counts are out of sync with source data.")
            warnings.extend(mismatch_messages)
        else:
            if identified_check_deferred:
                add_health("ok", "PRISMA duplicates/screened counts match available source data.")
            else:
                add_health("ok", "PRISMA key counts are in sync with source data.")
            prisma_sync_ok = True

    artifacts = [
        artifact_entry(screening_summary_path),
        artifact_entry(csv_input_validation_summary_path),
        artifact_entry(extraction_validation_summary_path),
        artifact_entry(quality_appraisal_summary_path),
        artifact_entry(quality_appraisal_scored_path),
        artifact_entry(effect_size_conversion_summary_path),
        artifact_entry(effect_size_converted_path),
        artifact_entry(dedup_summary_path),
        artifact_entry(prisma_flow_path),
    ]
    missing_artifacts = [artifact["path"] for artifact in artifacts if not artifact["present"]]
    if missing_artifacts:
        joined = ", ".join(f"`{path}`" for path in missing_artifacts)
        add_health("error", f"Missing expected artifact(s): {joined}.")
        warnings.append(f"Regenerate missing artifact(s): {joined}.")
    else:
        add_health("ok", "Required analysis artifacts are present.")

    daily_run_integrity: dict | None = None
    if daily_run_manifest_path is not None or daily_run_failed_marker_path is not None:
        manifest = (
            parse_daily_run_manifest(daily_run_manifest_path)
            if daily_run_manifest_path is not None
            else None
        )
        failed_marker = (
            parse_daily_run_failed_marker(daily_run_failed_marker_path)
            if daily_run_failed_marker_path is not None
            else None
        )

        integrity_ok = True

        if failed_marker is not None and failed_marker["present"]:
            integrity_ok = False
            run_id = (
                failed_marker.get("run_id")
                or (manifest.get("run_id") if manifest else None)
                or "unknown"
            )
            if failed_marker.get("parsed"):
                if int(failed_marker.get("stream_object_count") or 0) > 1:
                    add_health(
                        "warning",
                        "Daily-run failed marker contains concatenated JSON objects; using the last object for integrity state.",
                    )
                    warnings.append(
                        "Clean run metadata by rerunning `make daily` to rewrite failed-marker metadata atomically."
                    )

                add_health(
                    "error",
                    f"Latest daily run is marked failed (run_id={run_id}); outputs may be partially updated.",
                )
            else:
                add_health(
                    "error",
                    "Daily-run failed marker exists but is unreadable; outputs may be partially updated.",
                )
            warnings.append(
                "Treat current analysis artifacts as potentially stale; resolve the failure and rerun `make daily`."
            )

            if (
                manifest is not None
                and manifest.get("parsed")
                and manifest.get("rollback_applied") is True
            ):
                add_health(
                    "warning",
                    "Failure-state rollback was applied; visible artifacts may be from the previous run snapshot.",
                )
                warnings.append(
                    "Rollback restored tracked outputs, so current artifacts can look valid while latest run still failed."
                )
        elif manifest is not None and manifest["present"]:
            if not manifest["parsed"]:
                integrity_ok = False
                add_health(
                    "warning",
                    "Daily-run manifest is present but unreadable; run integrity cannot be verified.",
                )
                warnings.append("Regenerate run metadata by rerunning `make daily`.")
            else:
                if int(manifest.get("stream_object_count") or 0) > 1:
                    integrity_ok = False
                    add_health(
                        "warning",
                        "Daily-run manifest contains concatenated JSON objects; using the last object for integrity state.",
                    )
                    warnings.append(
                        "Clean run metadata by rerunning `make daily` to rewrite manifest atomically."
                    )

                state = str(manifest.get("state") or "").strip().lower()
                if state == "success":
                    add_health("ok", "Daily run manifest indicates a clean completed run.")
                elif state == "running":
                    current_run_id = str(os.environ.get("DAILY_RUN_ID", "")).strip()
                    manifest_run_id = str(manifest.get("run_id") or "").strip()
                    if current_run_id and manifest_run_id and current_run_id == manifest_run_id:
                        add_health(
                            "info",
                            "Daily-run manifest indicates the current in-progress run (`state=running`).",
                        )
                    else:
                        integrity_ok = False
                        add_health(
                            "warning",
                            "Daily-run manifest indicates an unfinished run (`state=running`).",
                        )
                        warnings.append(
                            "Finish or rerun `make daily` before trusting the latest artifact set."
                        )
                elif state == "failed":
                    integrity_ok = False
                    add_health(
                        "error",
                        "Daily-run manifest indicates failure; outputs may be partially updated.",
                    )
                    warnings.append("Resolve run failures and rerun `make daily`.")

                    rollback_applied = manifest.get("rollback_applied")
                    if rollback_applied is True:
                        add_health(
                            "warning",
                            "Failure-state rollback was applied; visible artifacts may be from the previous run snapshot.",
                        )
                        warnings.append(
                            "Rollback restored tracked outputs, so current artifacts can look valid while latest run still failed."
                        )
                else:
                    integrity_ok = False
                    add_health(
                        "warning",
                        f"Daily-run manifest state is unexpected (`{manifest.get('state')}`).",
                    )
                    warnings.append(
                        "Verify run metadata and rerun `make daily` to refresh integrity state."
                    )

        details_parts: list[str] = []
        if manifest is not None:
            if manifest["present"] and manifest["parsed"]:
                rollback_label = "unknown"
                rollback_value = manifest.get("rollback_applied")
                if rollback_value is True:
                    rollback_label = "true"
                elif rollback_value is False:
                    rollback_label = "false"

                details_parts.append(
                    f"manifest state={manifest.get('state') or 'unknown'}"
                    f" (run_id={manifest.get('run_id') or 'unknown'})"
                    f" (rollback_applied={rollback_label})"
                    f" (objects={int(manifest.get('stream_object_count') or 1)})"
                )
            elif manifest["present"]:
                details_parts.append("manifest present, parse failed")
            else:
                details_parts.append("manifest missing")
        if failed_marker is not None:
            if failed_marker["present"] and failed_marker["parsed"]:
                details_parts.append(
                    f"failed marker present"
                    f" (run_id={failed_marker.get('run_id') or 'unknown'})"
                    f" (objects={int(failed_marker.get('stream_object_count') or 1)})"
                )
            elif failed_marker["present"]:
                details_parts.append("failed marker present, parse failed")
            else:
                details_parts.append("failed marker absent")

        daily_run_integrity = {
            "ok": integrity_ok,
            "details": "; ".join(details_parts)
            if details_parts
            else "No run-integrity metadata configured.",
            "manifest": manifest,
            "failed_marker": failed_marker,
        }

    analytics_artifacts_ready = (
        quality_appraisal_ok and effect_size_conversion_ok and not missing_artifacts
    )
    stage_assessment = assess_project_stage(
        identified_total=identified_total,
        unique_records=unique_records,
        sessions=sessions,
        screened_records=screened_records,
        includes=includes,
        demo_like_master=demo_like_master,
        analytics_artifacts_ready=analytics_artifacts_ready,
    )
    semantic_completeness = assess_semantic_completeness(protocol_path, manuscript_path)
    project_posture = build_project_posture(stage_assessment, semantic_completeness)
    active_review_mode = normalize_review_mode(
        review_mode if review_mode is not None else os.getenv("REVIEW_MODE")
    )

    unresolved_placeholder_total = (
        semantic_completeness["protocol_placeholder_count"]
        + semantic_completeness["manuscript_placeholder_count"]
    )
    placeholders_allowed_in_mode = active_review_mode == "template"
    placeholder_policy_done = semantic_completeness["complete"] or placeholders_allowed_in_mode

    if not semantic_completeness["complete"]:
        if placeholders_allowed_in_mode:
            add_health(
                "info",
                "Unresolved placeholders are allowed in REVIEW_MODE=template and tracked as setup debt.",
            )
        else:
            add_health(
                "warning",
                "Unresolved placeholders are not allowed in REVIEW_MODE=production.",
            )
            warnings.append(
                "Resolve unresolved placeholders in protocol/manuscript before production sign-off."
            )

    if stage_assessment["id"] == "bootstrap_demo":
        add_health(
            "info",
            "Workspace appears to be in bootstrap/demo mode: advanced artifacts can exist before production search/screening logs.",
        )
    elif stage_assessment["id"] == "pre_screening_unlogged_search":
        add_health(
            "warning",
            "Master records are populated, but search totals are not logged yet; complete search log before stage promotion.",
        )

    warnings = unique_preserve_order(warnings)

    prisma_detail = "Core PRISMA counts are aligned."
    if missing_prisma_keys:
        prisma_detail = "Missing counts: " + ", ".join(missing_prisma_keys)
    elif mismatch_messages:
        prisma_detail = mismatch_messages[0]
    elif screened_progress_note:
        prisma_detail = screened_progress_note
    elif identified_check_deferred:
        prisma_detail = "Identified-count check deferred until `search_log.csv` totals are filled."

    input_checklist = [
        {
            "id": "search_totals",
            "title": "Fill search totals (`results_total`)",
            "file": "02_data/processed/search_log.csv",
            "done": identified_total > 0,
            "details": checklist_details(f"Current sum(results_total): {identified_total}"),
            "hint": "Enter per-database hit counts from search exports.",
        },
        {
            "id": "master_records",
            "title": "Add master records and duplicate flags",
            "file": "02_data/processed/master_records.csv",
            "done": master_rows > 0,
            "details": checklist_details(
                f"Current non-empty rows: {master_rows}; duplicates: {duplicates}"
            ),
            "hint": "Merge exported records and mark duplicates (`is_duplicate=yes/no`).",
        },
        {
            "id": "master_columns",
            "title": "Keep master columns aligned with dedup workflow",
            "file": "02_data/processed/master_records.csv",
            "done": master_structure["matches"],
            "details": checklist_details(master_structure["details"]),
            "hint": "Use header from `02_data/processed/master_records_template.csv` and keep column order stable.",
        },
        {
            "id": "csv_input_validation",
            "title": "Keep CSV input validation clean",
            "file": "03_analysis/outputs/csv_input_validation_summary.md",
            "done": csv_input_validation_ok,
            "details": checklist_details(csv_input_validation_detail),
            "hint": "Run `python 03_analysis/validate_csv_inputs.py` and resolve reported schema/value issues in processed CSV files.",
        },
        {
            "id": "extraction_validation",
            "title": "Keep extraction validation clean",
            "file": "03_analysis/outputs/extraction_validation_summary.md",
            "done": extraction_validation_ok,
            "details": checklist_details(extraction_validation_detail),
            "hint": "Run `python 03_analysis/validate_extraction.py` and resolve reported errors/warnings in extraction coding.",
        },
        {
            "id": "quality_appraisal",
            "title": "Refresh quality appraisal outputs",
            "file": "03_analysis/outputs/quality_appraisal_summary.md",
            "done": quality_appraisal_ok,
            "details": checklist_details(quality_appraisal_detail),
            "hint": "Run `python 03_analysis/quality_appraisal.py` and confirm both summary and scored CSV are generated.",
        },
        {
            "id": "effect_size_conversion",
            "title": "Refresh effect-size conversion outputs",
            "file": "03_analysis/outputs/effect_size_conversion_summary.md",
            "done": effect_size_conversion_ok,
            "details": checklist_details(effect_size_conversion_detail),
            "hint": "Run `python 03_analysis/effect_size_converter.py` and confirm both summary and converted CSV are generated.",
        },
        {
            "id": "screening_log",
            "title": "Log at least one screening session",
            "file": "02_data/processed/screening_daily_log.csv",
            "done": sessions > 0,
            "details": checklist_details(
                f"Current sessions: {sessions}; screened: {screened_records}"
            ),
            "hint": "Add date, reviewer, and decision counts for each session.",
        },
        {
            "id": "dual_reviewer",
            "title": "Use an independent second reviewer (title/abstract)",
            "file": "02_data/processed/screening_daily_log.csv",
            "done": title_abstract_sessions > 0 and len(title_abstract_reviewers) >= 2,
            "details": checklist_details(
                f"Title/abstract sessions: {title_abstract_sessions}; unique reviewers: {len(title_abstract_reviewers)}"
            ),
            "hint": "Log at least two independent reviewers for title/abstract screening.",
        },
        {
            "id": "cohen_kappa",
            "title": "Calculate Cohen's kappa",
            "file": "02_data/processed/screening_title_abstract_dual_log.csv",
            "done": bool(kappa_stats["available"]),
            "details": checklist_details(
                f"{kappa_stats['reason']}"
                if not kappa_stats["available"]
                else f"kappa={kappa_stats['kappa']} ({kappa_stats['pair']}, n={kappa_stats['paired_records']})"
            ),
            "hint": "Enter paired title/abstract decisions by record and reviewer to compute agreement.",
        },
        {
            "id": "semantic_placeholders",
            "title": "Resolve placeholders before production",
            "file": semantic_completeness["protocol_path"],
            "done": placeholder_policy_done,
            "details": checklist_details(
                f"REVIEW_MODE={active_review_mode}; unresolved placeholders: {unresolved_placeholder_total}"
            ),
            "hint": "Template mode allows placeholders; production mode requires all placeholders to be replaced.",
        },
        {
            "id": "prisma_sync",
            "title": "Keep PRISMA core counts in sync",
            "file": "02_data/processed/prisma_counts_template.csv",
            "done": prisma_sync_ok,
            "details": checklist_details(prisma_detail),
            "hint": "Ensure identified, duplicates, and screened counts match computed values.",
        },
    ]

    if warnings:
        suggested_next_step = [
            warnings[0],
            "After fixing the warning above, rerun `make status`.",
        ]
    elif master_rows == 0:
        suggested_next_step = [
            "Fill `02_data/processed/master_records.csv`, flag duplicates, then rerun `make status`.",
        ]
    else:
        suggested_next_step = [
            "Status is up to date; continue screening and rerun `make status` after each update.",
        ]

    emoji_by_level = {
        "ok": "✅",
        "warning": "⚠️",
        "error": "❌",
        "info": "ℹ️",
    }

    lines = []
    lines.append("# Review Status Report")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Data Snapshot")
    lines.append("")
    lines.append(f"- Search rows logged: {len(search_non_empty)}")
    lines.append(f"- Search results total (`results_total`): {identified_total}")
    lines.append(f"- Latest search date: {latest_search_date}")
    lines.append(f"- Master rows (non-empty): {master_rows}")
    lines.append(f"- Duplicates flagged: {duplicates}")
    lines.append(f"- Unique records after dedup: {unique_records}")
    lines.append(f"- Screening sessions: {sessions}")
    lines.append(f"- Records screened: {screened_records}")
    lines.append(f"- Includes: {includes}")
    lines.append(f"- Excludes: {excludes}")
    lines.append(f"- Maybe: {maybe}")
    lines.append(f"- Pending: {pending}")
    lines.append("")
    lines.append("## Stage Assessment")
    lines.append("")
    lines.append(f"- Stage: {stage_assessment['label']} (`{stage_assessment['id']}`)")
    for reason in stage_assessment["reasons"]:
        lines.append(f"- {reason}")
    lines.append("")
    lines.append("## Project Posture")
    lines.append("")
    lines.append(f"- Summary (EN): {project_posture['summary_en']}")
    lines.append(f"- Summary (RU): {project_posture['summary_ru']}")
    lines.append(f"- REVIEW_MODE policy context: `{active_review_mode}`")
    blocker = project_posture["primary_blocker"]
    lines.append(f"- Primary blocker: {blocker if blocker is not None else 'none'}")
    semantic_label = "complete" if semantic_completeness["complete"] else "pending"
    lines.append(f"- Semantic completeness: {semantic_label}")
    lines.append(
        "- Unresolved placeholders: "
        f"protocol={semantic_completeness['protocol_placeholder_count']}, "
        f"manuscript={semantic_completeness['manuscript_placeholder_count']}"
    )
    if semantic_completeness["placeholder_examples"]:
        examples = ", ".join(
            f"`{token}`" for token in semantic_completeness["placeholder_examples"]
        )
        lines.append(f"- Placeholder examples: {examples}")
    if blocker == "semantic_completeness":
        lines.append(
            "- Action: replace unresolved placeholders in "
            f"`{semantic_completeness['protocol_path']}`"
            + (
                f" and `{semantic_completeness['manuscript_path']}`"
                if semantic_completeness["manuscript_path"]
                else ""
            )
            + "."
        )
    lines.append("")
    lines.append("## Registration")
    lines.append("")
    lines.append(f"- Protocol file: `{prospero_registration['protocol_path']}`")
    if prospero_registration["registered"]:
        lines.append(f"- PROSPERO registration ID: `{prospero_registration['registration_id']}`")
        lines.append("- Registration timing: complete before screening ✅")
    else:
        lines.append("- PROSPERO registration ID: missing")
        lines.append("- Registration timing: must be completed before screening ❗")
    lines.append("")
    lines.append("## Reviewer Agreement")
    lines.append("")
    lines.append(f"- Title/abstract sessions logged: {title_abstract_sessions}")
    lines.append(f"- Unique title/abstract reviewers: {len(title_abstract_reviewers)}")
    if kappa_stats["available"]:
        lines.append(
            f"- Cohen's kappa: {kappa_stats['kappa']} ({kappa_stats['pair']}, n={kappa_stats['paired_records']})"
        )
    else:
        lines.append(f"- Cohen's kappa: not available ({kappa_stats['reason']})")
    lines.append("")
    lines.append("## PRISMA Key Counts")
    lines.append("")
    for key in PRISMA_KEYS:
        lines.append(f"- `{key}`: {count_or_dash(prisma_counts[key])}")
    lines.append("")
    lines.append("## PRISMA Progress")
    lines.append("")
    lines.append(
        f"- Identified from databases: {count_or_dash(prisma_progress[0]['count'])} (baseline)"
    )
    lines.append(
        f"- Duplicates removed: {count_or_dash(prisma_progress[1]['count'])} ({prisma_progress[1]['percent']} of identified)"
    )
    lines.append(
        f"- Screened title/abstract: {count_or_dash(prisma_progress[2]['count'])} ({prisma_progress[2]['percent']} of identified)"
    )
    lines.append(
        f"- Excluded title/abstract: {count_or_dash(prisma_progress[3]['count'])} ({prisma_progress[3]['percent']} of screened)"
    )
    lines.append(
        f"- Full-text assessed: {count_or_dash(prisma_progress[4]['count'])} ({prisma_progress[4]['percent']} of screened)"
    )
    lines.append(
        f"- Included qualitative: {count_or_dash(prisma_progress[5]['count'])} ({prisma_progress[5]['percent']} of full-text)"
    )
    lines.append("")
    lines.append("## Health Checks")
    lines.append("")
    for check in health_checks:
        emoji = emoji_by_level.get(check["level"], "•")
        lines.append(f"- {emoji} {check['message']}")
    lines.append("")
    lines.append("## Input Checklist")
    lines.append("")
    for item in input_checklist:
        mark = "x" if item["done"] else " "
        lines.append(f"- [{mark}] {item['title']} — `{item['file']}` ({item['details']})")
    lines.append("")
    lines.append("## Warnings")
    lines.append("")
    if warnings:
        for warning in warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- No warnings.")
    lines.append("")
    lines.append("## Artifact Status")
    lines.append("")
    for artifact in artifacts:
        lines.append(artifact_status_line(artifact))
    lines.append("")
    if daily_run_integrity is not None:
        lines.append("## Daily Run Integrity")
        lines.append("")
        manifest = daily_run_integrity.get("manifest")
        failed_marker = daily_run_integrity.get("failed_marker")
        if manifest is not None:
            if manifest["present"] and manifest["parsed"]:
                manifest_objects = int(manifest.get("stream_object_count") or 1)
                lines.append(
                    f"- Manifest: present (`state={manifest.get('state') or 'unknown'}`, "
                    f"`run_id={manifest.get('run_id') or 'unknown'}`, "
                    f"`objects={manifest_objects}`)."
                )
            elif manifest["present"]:
                lines.append("- Manifest: present, but unreadable.")
            else:
                lines.append("- Manifest: missing.")
        if failed_marker is not None:
            if failed_marker["present"] and failed_marker["parsed"]:
                marker_objects = int(failed_marker.get("stream_object_count") or 1)
                lines.append(
                    f"- Failed marker: present (`run_id={failed_marker.get('run_id') or 'unknown'}`, "
                    f"`phase={failed_marker.get('failure_phase') or 'unknown'}`, "
                    f"`objects={marker_objects}`)."
                )
            elif failed_marker["present"]:
                lines.append("- Failed marker: present, but unreadable.")
            else:
                lines.append("- Failed marker: absent.")
        lines.append("")
    lines.append("## Suggested Next Step")
    lines.append("")
    for step in suggested_next_step:
        lines.append(f"- {step}")
    lines.append("")

    status_summary = {
        "generated_at": generated_at,
        "review_mode": active_review_mode,
        "data_snapshot": {
            "search_rows_logged": len(search_non_empty),
            "search_results_total": identified_total,
            "latest_search_date": latest_search_date,
            "master_rows_non_empty": master_rows,
            "duplicates_flagged": duplicates,
            "unique_records_after_dedup": unique_records,
            "screening_sessions": sessions,
            "records_screened": screened_records,
            "includes": includes,
            "excludes": excludes,
            "maybe": maybe,
            "pending": pending,
        },
        "prisma_key_counts": prisma_counts,
        "prisma_progress": prisma_progress,
        "stage_assessment": stage_assessment,
        "project_posture": project_posture,
        "registration": prospero_registration,
        "reviewer_agreement": {
            "title_abstract_sessions": title_abstract_sessions,
            "title_abstract_reviewers": sorted(title_abstract_reviewers),
            "all_screening_reviewers": sorted(all_reviewers),
            "cohen_kappa": kappa_stats,
        },
        "reviewer_workload_balancer": reviewer_workload_balancer,
        "csv_input_validation": csv_input_validation,
        "extraction_validation": extraction_validation,
        "quality_appraisal": {
            "summary_path": quality_appraisal_summary_path.as_posix(),
            "summary_present": quality_appraisal_summary_present,
            "scored_path": quality_appraisal_scored_path.as_posix(),
            "scored_present": quality_appraisal_scored_present,
            "ok": quality_appraisal_ok,
            "details": quality_appraisal_detail,
        },
        "effect_size_conversion": {
            "summary_path": effect_size_conversion_summary_path.as_posix(),
            "summary_present": effect_size_summary_present,
            "converted_path": effect_size_converted_path.as_posix(),
            "converted_present": effect_size_converted_present,
            "ok": effect_size_conversion_ok,
            "details": effect_size_conversion_detail,
        },
        "health_checks": health_checks,
        "input_checklist": input_checklist,
        "warnings": warnings,
        "artifacts": artifacts,
        "daily_run_integrity": daily_run_integrity,
        "suggested_next_step": suggested_next_step,
    }

    return "\n".join(lines), status_summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a consolidated status report for the review workspace."
    )
    parser.add_argument(
        "--screening-log",
        default="../02_data/processed/screening_daily_log.csv",
        help="Path to screening daily log CSV",
    )
    parser.add_argument(
        "--screening-record-log",
        default="../02_data/processed/screening_title_abstract_dual_log.csv",
        help="Path to record-level dual-screening log CSV",
    )
    parser.add_argument(
        "--master",
        default="../02_data/processed/master_records.csv",
        help="Path to master records CSV",
    )
    parser.add_argument(
        "--search-log", default="../02_data/processed/search_log.csv", help="Path to search log CSV"
    )
    parser.add_argument(
        "--prisma",
        default="../02_data/processed/prisma_counts_template.csv",
        help="Path to PRISMA counts CSV",
    )
    parser.add_argument(
        "--protocol", default="../01_protocol/protocol.md", help="Path to protocol markdown file"
    )
    parser.add_argument(
        "--manuscript", default="../04_manuscript/main.tex", help="Path to manuscript main tex file"
    )
    parser.add_argument(
        "--screening-summary",
        default="outputs/screening_metrics_summary.md",
        help="Path to screening metrics summary markdown",
    )
    parser.add_argument(
        "--csv-input-validation-summary",
        default="outputs/csv_input_validation_summary.md",
        help="Path to CSV input validation summary markdown",
    )
    parser.add_argument(
        "--extraction-validation-summary",
        default="outputs/extraction_validation_summary.md",
        help="Path to extraction validation summary markdown",
    )
    parser.add_argument(
        "--quality-appraisal-summary",
        default="outputs/quality_appraisal_summary.md",
        help="Path to quality appraisal summary markdown",
    )
    parser.add_argument(
        "--quality-appraisal-scored",
        default="outputs/quality_appraisal_scored.csv",
        help="Path to quality appraisal scored CSV",
    )
    parser.add_argument(
        "--effect-size-summary",
        default="outputs/effect_size_conversion_summary.md",
        help="Path to effect-size conversion summary markdown",
    )
    parser.add_argument(
        "--effect-size-converted",
        default="outputs/effect_size_converted.csv",
        help="Path to converted effect-size CSV",
    )
    parser.add_argument(
        "--reviewer-workload-summary",
        default="outputs/reviewer_workload_balancer_summary.md",
        help="Path to reviewer workload balancer summary markdown",
    )
    parser.add_argument(
        "--dedup-summary",
        default="outputs/dedup_stats_summary.md",
        help="Path to dedup stats summary markdown",
    )
    parser.add_argument(
        "--prisma-flow",
        default="outputs/prisma_flow_diagram.tex",
        help="Path to PRISMA flow diagram artifact (TikZ/TeX or image)",
    )
    parser.add_argument(
        "--daily-run-manifest",
        default="outputs/daily_run_manifest.json",
        help="Path to daily-run manifest JSON (run integrity metadata)",
    )
    parser.add_argument(
        "--daily-run-failed-marker",
        default="outputs/daily_run_failed.marker",
        help="Path to daily-run failure marker JSON",
    )
    parser.add_argument(
        "--review-mode",
        default=None,
        choices=["template", "production"],
        help="Override REVIEW_MODE policy context for placeholder handling",
    )
    parser.add_argument(
        "--output", default="outputs/status_report.md", help="Path to status report markdown"
    )
    parser.add_argument(
        "--json-output",
        default="outputs/status_summary.json",
        help="Path to JSON status summary output",
    )
    args = parser.parse_args()

    screening_log_path = Path(args.screening_log)
    screening_record_log_path = Path(args.screening_record_log)
    master_path = Path(args.master)
    search_log_path = Path(args.search_log)
    prisma_path = Path(args.prisma)
    protocol_path = Path(args.protocol)
    manuscript_path = Path(args.manuscript)
    screening_summary_path = Path(args.screening_summary)
    csv_input_validation_summary_path = Path(args.csv_input_validation_summary)
    extraction_validation_summary_path = Path(args.extraction_validation_summary)
    quality_appraisal_summary_path = Path(args.quality_appraisal_summary)
    quality_appraisal_scored_path = Path(args.quality_appraisal_scored)
    effect_size_summary_path = Path(args.effect_size_summary)
    effect_size_converted_path = Path(args.effect_size_converted)
    reviewer_workload_summary_path = Path(args.reviewer_workload_summary)
    dedup_summary_path = Path(args.dedup_summary)
    prisma_flow_path = Path(args.prisma_flow)
    daily_run_manifest_path = Path(args.daily_run_manifest)
    daily_run_failed_marker_path = Path(args.daily_run_failed_marker)
    output_path = Path(args.output)
    json_output_path = Path(args.json_output)

    report, status_summary = build_status_report(
        screening_df=read_csv_or_empty(screening_log_path),
        screening_records_df=read_csv_or_empty(screening_record_log_path),
        master_df=read_csv_or_empty(master_path),
        search_df=read_csv_or_empty(search_log_path),
        prisma_df=read_csv_or_empty(prisma_path),
        protocol_path=protocol_path,
        screening_summary_path=screening_summary_path,
        csv_input_validation_summary_path=csv_input_validation_summary_path,
        extraction_validation_summary_path=extraction_validation_summary_path,
        quality_appraisal_summary_path=quality_appraisal_summary_path,
        quality_appraisal_scored_path=quality_appraisal_scored_path,
        effect_size_conversion_summary_path=effect_size_summary_path,
        effect_size_converted_path=effect_size_converted_path,
        dedup_summary_path=dedup_summary_path,
        prisma_flow_path=prisma_flow_path,
        reviewer_workload_summary_path=reviewer_workload_summary_path,
        manuscript_path=manuscript_path,
        daily_run_manifest_path=daily_run_manifest_path,
        daily_run_failed_marker_path=daily_run_failed_marker_path,
        review_mode=args.review_mode,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")

    json_output_path.parent.mkdir(parents=True, exist_ok=True)
    json_output_path.write_text(
        json.dumps(status_summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    print(f"Wrote: {output_path}")
    print(f"Wrote: {json_output_path}")


if __name__ == "__main__":
    main()

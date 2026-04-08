import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

EMPTY_VALUES = {"", "nan", "none"}
TEMPLATE_EXPORT_FILENAME_TOKENS = ("yyyy-mm-dd", "yyyy_mm_dd", "yyyy.mm.dd", "yyyymmdd")

SEARCH_LOG_COLUMNS = [
    "database",
    "date_searched",
    "query_version",
    "start_year",
    "end_date",
    "filters_applied",
    "results_total",
    "results_exported",
    "export_filename",
    "notes",
]

SCREENING_DAILY_COLUMNS = [
    "date",
    "reviewer",
    "stage",
    "records_screened",
    "include_n",
    "exclude_n",
    "maybe_n",
    "pending_n",
    "time_spent_minutes",
    "notes",
]

SCREENING_DUAL_COLUMNS = [
    "record_id",
    "reviewer",
    "title_abstract_decision",
    "decision_date",
    "notes",
]

SCREENING_TITLE_ABSTRACT_RESULTS_COLUMNS = [
    "record_id",
    "reviewer1_decision",
    "reviewer2_decision",
    "conflict",
    "conflict_resolver",
    "resolution_decision",
    "final_decision",
    "exclusion_reason",
]

SCREENING_FULLTEXT_COLUMNS = [
    "record_id",
    "fulltext_available",
    "include",
    "exclusion_reason",
    "reviewer",
    "notes",
]

DECISION_LOG_COLUMNS = [
    "record_id",
    "stage",
    "decision",
    "reason",
    "reviewer",
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

PRISMA_COLUMNS = ["stage", "count", "notes"]
FULLTEXT_REASON_COLUMNS = ["reason", "count", "notes"]

ALLOWED_DATABASES = {"pubmed", "embase", "scopus", "psycinfo", "web of science"}
ALLOWED_SCREENING_STAGE = {
    "title_abstract",
    "title/abstract",
    "full_text",
    "full-text",
    "full text",
}
ALLOWED_TA_DECISIONS = {"include", "exclude", "maybe"}
ALLOWED_TA_RESULTS_DECISIONS = {"include", "exclude", "uncertain"}
ALLOWED_DUPLICATE_FLAG = {"yes", "y", "1", "true", "no", "n", "0", "false"}
ALLOWED_CONFLICT_FLAG = {"yes", "y", "1", "true", "no", "n", "0", "false"}
ALLOWED_FULLTEXT_AVAILABLE = {"yes", "y", "1", "true", "no", "n", "0", "false"}
ALLOWED_FULLTEXT_INCLUDE = {
    "include",
    "included",
    "exclude",
    "excluded",
    "yes",
    "y",
    "1",
    "true",
    "no",
    "n",
    "0",
    "false",
}
ALLOWED_SCREENING_FULLTEXT_EXCLUSION_REASONS = {
    "wrong population",
    "wrong outcome",
    "wrong study design",
    "not empirical",
    "duplicate",
    "full text unavailable",
    "other",
}
ALLOWED_DECISION_LOG_STAGE = {
    "screening",
    "title_abstract",
    "title/abstract",
    "fulltext",
    "full_text",
    "full-text",
    "full text",
}
ALLOWED_DECISION_LOG_DECISION = {"include", "exclude", "maybe", "uncertain"}
ALLOWED_PRISMA_STAGE = {
    "records_identified_databases",
    "records_identified_other_sources",
    "duplicates_removed",
    "records_screened_title_abstract",
    "records_excluded_title_abstract",
    "reports_sought_for_retrieval",
    "reports_not_retrieved",
    "reports_assessed_full_text",
    "reports_excluded_full_text",
    "studies_included_qualitative_synthesis",
    "studies_included_quantitative_synthesis",
}
ALLOWED_FULLTEXT_REASONS = {
    "No eligible population/context",
    "No eligible outcome",
    "No eligible exposure/concept",
    "Theoretical/non-empirical paper",
    "Case report with n<5",
    "Duplicate dataset or overlapping sample",
    "Full text unavailable",
    "Other",
    # backward-compatible legacy labels
    "No BN-specific data",
    "No identity-related outcome",
    "No object-relations/attachment construct",
}

FILE_SPECS = [
    {
        "name": "search_log",
        "path": "../02_data/processed/search_log.csv",
        "required": SEARCH_LOG_COLUMNS,
        "allowed": {"database": ALLOWED_DATABASES},
        "non_negative_int": ["start_year", "results_total", "results_exported"],
        "date_columns": ["date_searched", "end_date"],
    },
    {
        "name": "screening_daily_log",
        "path": "../02_data/processed/screening_daily_log.csv",
        "required": SCREENING_DAILY_COLUMNS,
        "allowed": {"stage": ALLOWED_SCREENING_STAGE},
        "non_negative_int": [
            "records_screened",
            "include_n",
            "exclude_n",
            "maybe_n",
            "pending_n",
            "time_spent_minutes",
        ],
        "date_columns": ["date"],
    },
    {
        "name": "screening_title_abstract_dual_log",
        "path": "../02_data/processed/screening_title_abstract_dual_log.csv",
        "required": SCREENING_DUAL_COLUMNS,
        "allowed": {"title_abstract_decision": ALLOWED_TA_DECISIONS},
        "non_negative_int": [],
        "date_columns": ["decision_date"],
    },
    {
        "name": "screening_title_abstract_results",
        "path": "../02_data/processed/screening_title_abstract_results.csv",
        "required": SCREENING_TITLE_ABSTRACT_RESULTS_COLUMNS,
        "allowed": {
            "reviewer1_decision": ALLOWED_TA_RESULTS_DECISIONS,
            "reviewer2_decision": ALLOWED_TA_RESULTS_DECISIONS,
            "resolution_decision": ALLOWED_TA_RESULTS_DECISIONS,
            "final_decision": ALLOWED_TA_RESULTS_DECISIONS,
            "conflict": ALLOWED_CONFLICT_FLAG,
        },
        "non_negative_int": [],
        "date_columns": [],
    },
    {
        "name": "screening_fulltext_log",
        "path": "../02_data/processed/screening_fulltext_log.csv",
        "required": SCREENING_FULLTEXT_COLUMNS,
        "allowed": {
            "fulltext_available": ALLOWED_FULLTEXT_AVAILABLE,
            "include": ALLOWED_FULLTEXT_INCLUDE,
            "exclusion_reason": {
                value.lower() for value in ALLOWED_SCREENING_FULLTEXT_EXCLUSION_REASONS
            },
        },
        "non_negative_int": [],
        "date_columns": [],
    },
    {
        "name": "decision_log",
        "path": "../02_data/processed/decision_log.csv",
        "required": DECISION_LOG_COLUMNS,
        "allowed": {
            "stage": ALLOWED_DECISION_LOG_STAGE,
            "decision": ALLOWED_DECISION_LOG_DECISION,
        },
        "non_negative_int": [],
        "date_columns": [],
    },
    {
        "name": "master_records",
        "path": "../02_data/processed/master_records.csv",
        "required": MASTER_RECORD_COLUMNS,
        "legacy_optional_missing": {
            "abstract": (
                "Legacy v3 master schema detected (`abstract` missing). "
                "Column is treated as empty for backward compatibility."
            )
        },
        "allowed": {
            "source_database": ALLOWED_DATABASES,
            "is_duplicate": ALLOWED_DUPLICATE_FLAG,
        },
        "non_negative_int": ["year"],
        "date_columns": [],
    },
    {
        "name": "prisma_counts_template",
        "path": "../02_data/processed/prisma_counts_template.csv",
        "required": PRISMA_COLUMNS,
        "allowed": {"stage": ALLOWED_PRISMA_STAGE},
        "non_negative_int": ["count"],
        "date_columns": [],
    },
    {
        "name": "full_text_exclusion_reasons",
        "path": "../02_data/processed/full_text_exclusion_reasons.csv",
        "required": FULLTEXT_REASON_COLUMNS,
        "allowed": {"reason": {value.lower() for value in ALLOWED_FULLTEXT_REASONS}},
        "non_negative_int": ["count"],
        "date_columns": [],
    },
]
RAW_EXPORTS_DIR = Path("../02_data/raw")
MIN_VALID_YEAR = 1900


def normalize(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass

    text = str(value).strip()
    if text.lower() in EMPTY_VALUES:
        return ""
    return text


def normalize_lower(value: object) -> str:
    return normalize(value).lower()


def is_empty(value: object) -> bool:
    text = normalize_lower(value)
    return text in EMPTY_VALUES


def is_template_export_filename(value: object) -> bool:
    text = normalize_lower(value)
    if not text:
        return False

    if "<" in text and ">" in text:
        return True
    if "{" in text and "}" in text:
        return True
    if any(token in text for token in TEMPLATE_EXPORT_FILENAME_TOKENS):
        return True
    return ("yyyy" in text) and ("mm" in text) and ("dd" in text)


def parse_iso_date_or_none(value: object) -> datetime | None:
    text = normalize(value)
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return None


def parse_non_negative_int_or_none(value: object) -> int | None:
    if is_empty(value):
        return None

    numeric = pd.to_numeric(pd.Series([normalize(value)]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None

    number = float(numeric)
    if number < 0 or not number.is_integer():
        return None

    return int(number)


def is_non_empty_row(row: pd.Series) -> bool:
    return any(not is_empty(cell) for cell in row)


def non_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    mask = df.apply(is_non_empty_row, axis=1)
    return df[mask].copy()


def add_issue(
    issues: list[dict],
    *,
    file_name: str,
    level: str,
    row: int,
    column: str,
    message: str,
    value: str = "",
) -> None:
    issues.append(
        {
            "file": file_name,
            "level": level,
            "row": row,
            "column": column,
            "message": message,
            "value": value,
        }
    )


def validate_allowed_values(
    df: pd.DataFrame,
    *,
    file_name: str,
    allowed_rules: dict[str, set[str]],
    issues: list[dict],
) -> None:
    for column, allowed_values in allowed_rules.items():
        if column not in df.columns:
            continue

        allowed_lower = {value.lower() for value in allowed_values}
        for index, raw_value in df[column].items():
            if is_empty(raw_value):
                continue
            value = normalize(raw_value)
            if normalize_lower(value) not in allowed_lower:
                add_issue(
                    issues,
                    file_name=file_name,
                    level="error",
                    row=int(index) + 2,
                    column=column,
                    message="Invalid value.",
                    value=value,
                )


def validate_non_negative_int_columns(
    df: pd.DataFrame,
    *,
    file_name: str,
    columns: list[str],
    issues: list[dict],
) -> None:
    for column in columns:
        if column not in df.columns:
            continue

        for index, raw_value in df[column].items():
            if is_empty(raw_value):
                continue

            numeric = pd.to_numeric(pd.Series([normalize(raw_value)]), errors="coerce").iloc[0]
            if pd.isna(numeric):
                add_issue(
                    issues,
                    file_name=file_name,
                    level="error",
                    row=int(index) + 2,
                    column=column,
                    message="Value must be an integer >= 0.",
                    value=normalize(raw_value),
                )
                continue

            number = float(numeric)
            if number < 0 or not number.is_integer():
                add_issue(
                    issues,
                    file_name=file_name,
                    level="error",
                    row=int(index) + 2,
                    column=column,
                    message="Value must be an integer >= 0.",
                    value=normalize(raw_value),
                )


def validate_date_columns(
    df: pd.DataFrame,
    *,
    file_name: str,
    columns: list[str],
    issues: list[dict],
) -> None:
    today = datetime.now().date()
    for column in columns:
        if column not in df.columns:
            continue

        for index, raw_value in df[column].items():
            if is_empty(raw_value):
                continue

            parsed = parse_iso_date_or_none(raw_value)
            if parsed is None:
                add_issue(
                    issues,
                    file_name=file_name,
                    level="error",
                    row=int(index) + 2,
                    column=column,
                    message="Use date format YYYY-MM-DD.",
                    value=normalize(raw_value),
                )
                continue

            parsed_date = parsed.date()
            if parsed_date.year < MIN_VALID_YEAR:
                add_issue(
                    issues,
                    file_name=file_name,
                    level="error",
                    row=int(index) + 2,
                    column=column,
                    message=f"Date must be in realistic range (year >= {MIN_VALID_YEAR}).",
                    value=normalize(raw_value),
                )

            if parsed_date > today:
                add_issue(
                    issues,
                    file_name=file_name,
                    level="warning",
                    row=int(index) + 2,
                    column=column,
                    message="Date is in the future.",
                    value=normalize(raw_value),
                )


def validate_search_log_export_files(
    df: pd.DataFrame,
    *,
    file_name: str,
    raw_exports_dir: Path,
    issues: list[dict],
    template_filename_as_info: bool = True,
) -> None:
    if "export_filename" not in df.columns:
        return

    rows_with_export: list[tuple[int, str, int | None]] = []
    for index, raw_value in df["export_filename"].items():
        if is_empty(raw_value):
            continue

        filename = normalize(raw_value)
        exported_value = (
            df.loc[index, "results_exported"] if "results_exported" in df.columns else ""
        )
        exported_count = parse_non_negative_int_or_none(exported_value)
        rows_with_export.append((int(index), filename, exported_count))

    if not rows_with_export:
        return

    if not raw_exports_dir.exists():
        has_exported_results = any((count or 0) > 0 for _, _, count in rows_with_export)
        level = "error" if has_exported_results else "warning"
        if (
            template_filename_as_info
            and not has_exported_results
            and all(is_template_export_filename(filename) for _, filename, _ in rows_with_export)
        ):
            level = "info"

        add_issue(
            issues,
            file_name=file_name,
            level=level,
            row=1,
            column="export_filename",
            message=f"Raw exports directory is missing: {raw_exports_dir.as_posix()}",
            value=raw_exports_dir.as_posix(),
        )
        return

    raw_dir_resolved = raw_exports_dir.resolve()
    for index, filename, exported_count in rows_with_export:
        row_number = index + 2
        level = "error" if (exported_count or 0) > 0 else "warning"
        candidate = Path(filename)

        if candidate.is_absolute():
            add_issue(
                issues,
                file_name=file_name,
                level="error",
                row=row_number,
                column="export_filename",
                message="Use a relative filename located under ../02_data/raw/.",
                value=filename,
            )
            continue

        resolved_path = (raw_exports_dir / candidate).resolve()
        if resolved_path != raw_dir_resolved and raw_dir_resolved not in resolved_path.parents:
            add_issue(
                issues,
                file_name=file_name,
                level="error",
                row=row_number,
                column="export_filename",
                message="Path must stay inside ../02_data/raw/.",
                value=filename,
            )
            continue

        if not resolved_path.exists():
            issue_level = level
            if (
                template_filename_as_info
                and issue_level == "warning"
                and is_template_export_filename(filename)
            ):
                issue_level = "info"

            add_issue(
                issues,
                file_name=file_name,
                level=issue_level,
                row=row_number,
                column="export_filename",
                message="Referenced export file not found in ../02_data/raw/.",
                value=filename,
            )


def validate_search_log_ranges(
    df: pd.DataFrame,
    *,
    file_name: str,
    issues: list[dict],
) -> None:
    if df.empty:
        return

    current_year = datetime.now().year

    for index, row in df.iterrows():
        row_number = int(index) + 2

        start_year = parse_non_negative_int_or_none(row.get("start_year", ""))
        results_total = parse_non_negative_int_or_none(row.get("results_total", ""))
        results_exported = parse_non_negative_int_or_none(row.get("results_exported", ""))
        date_searched = parse_iso_date_or_none(row.get("date_searched", ""))
        end_date = parse_iso_date_or_none(row.get("end_date", ""))

        if start_year is not None:
            if start_year < MIN_VALID_YEAR or start_year > current_year:
                add_issue(
                    issues,
                    file_name=file_name,
                    level="error",
                    row=row_number,
                    column="start_year",
                    message=(
                        f"start_year must be in realistic range ({MIN_VALID_YEAR}..{current_year})."
                    ),
                    value=str(start_year),
                )

            if end_date is not None and start_year > end_date.year:
                add_issue(
                    issues,
                    file_name=file_name,
                    level="error",
                    row=row_number,
                    column="start_year",
                    message="start_year cannot be later than end_date year.",
                    value=f"start_year={start_year}; end_date={normalize(row.get('end_date', ''))}",
                )

            if date_searched is not None and start_year > date_searched.year:
                add_issue(
                    issues,
                    file_name=file_name,
                    level="error",
                    row=row_number,
                    column="start_year",
                    message="start_year cannot be later than date_searched year.",
                    value=f"start_year={start_year}; date_searched={normalize(row.get('date_searched', ''))}",
                )

        if (
            date_searched is not None
            and end_date is not None
            and end_date.date() > date_searched.date()
        ):
            add_issue(
                issues,
                file_name=file_name,
                level="error",
                row=row_number,
                column="end_date",
                message="end_date cannot be later than date_searched.",
                value=(
                    f"end_date={normalize(row.get('end_date', ''))}; "
                    f"date_searched={normalize(row.get('date_searched', ''))}"
                ),
            )

        if (
            results_total is not None
            and results_exported is not None
            and results_exported > results_total
        ):
            add_issue(
                issues,
                file_name=file_name,
                level="error",
                row=row_number,
                column="results_exported",
                message="results_exported cannot exceed results_total.",
                value=f"results_exported={results_exported}; results_total={results_total}",
            )


def validate_screening_daily_ranges(
    df: pd.DataFrame,
    *,
    file_name: str,
    issues: list[dict],
) -> None:
    if df.empty:
        return

    for index, row in df.iterrows():
        row_number = int(index) + 2

        records_screened = parse_non_negative_int_or_none(row.get("records_screened", ""))
        include_n = parse_non_negative_int_or_none(row.get("include_n", ""))
        exclude_n = parse_non_negative_int_or_none(row.get("exclude_n", ""))
        maybe_n = parse_non_negative_int_or_none(row.get("maybe_n", ""))
        pending_n = parse_non_negative_int_or_none(row.get("pending_n", ""))
        time_spent = parse_non_negative_int_or_none(row.get("time_spent_minutes", ""))

        parts = [value for value in [include_n, exclude_n, maybe_n, pending_n] if value is not None]
        if records_screened is not None and len(parts) == 4:
            decision_total = include_n + exclude_n + maybe_n + pending_n
            if decision_total != records_screened:
                add_issue(
                    issues,
                    file_name=file_name,
                    level="error",
                    row=row_number,
                    column="records_screened",
                    message="records_screened must equal include_n + exclude_n + maybe_n + pending_n.",
                    value=(f"records_screened={records_screened}; sum={decision_total}"),
                )

        if records_screened is not None and records_screened > 0 and time_spent == 0:
            add_issue(
                issues,
                file_name=file_name,
                level="warning",
                row=row_number,
                column="time_spent_minutes",
                message="time_spent_minutes should be > 0 when records_screened > 0.",
                value=f"records_screened={records_screened}; time_spent_minutes=0",
            )


def validate_master_records_rules(
    df: pd.DataFrame,
    *,
    file_name: str,
    issues: list[dict],
) -> None:
    if df.empty:
        return

    current_year = datetime.now().year
    record_ids = {
        normalize(record_id)
        for record_id in df.get("record_id", pd.Series(dtype=str)).fillna("").astype(str)
        if normalize(record_id)
    }

    for index, row in df.iterrows():
        row_number = int(index) + 2
        year_value = parse_non_negative_int_or_none(row.get("year", ""))
        if year_value is not None and (year_value < MIN_VALID_YEAR or year_value > current_year):
            add_issue(
                issues,
                file_name=file_name,
                level="error",
                row=row_number,
                column="year",
                message=f"year must be in realistic range ({MIN_VALID_YEAR}..{current_year}).",
                value=str(year_value),
            )

        is_duplicate = normalize_lower(row.get("is_duplicate", "")) in {"yes", "y", "1", "true"}
        duplicate_of = normalize(row.get("duplicate_of_record_id", ""))
        record_id = normalize(row.get("record_id", ""))

        if is_duplicate and not duplicate_of:
            add_issue(
                issues,
                file_name=file_name,
                level="error",
                row=row_number,
                column="duplicate_of_record_id",
                message="Set duplicate_of_record_id when is_duplicate=yes.",
                value=duplicate_of,
            )

        if not is_duplicate and duplicate_of:
            add_issue(
                issues,
                file_name=file_name,
                level="warning",
                row=row_number,
                column="duplicate_of_record_id",
                message="Leave duplicate_of_record_id empty when is_duplicate=no.",
                value=duplicate_of,
            )

        if duplicate_of and record_id and duplicate_of == record_id:
            add_issue(
                issues,
                file_name=file_name,
                level="error",
                row=row_number,
                column="duplicate_of_record_id",
                message="duplicate_of_record_id cannot reference the same record_id.",
                value=duplicate_of,
            )

        if duplicate_of and duplicate_of not in record_ids:
            add_issue(
                issues,
                file_name=file_name,
                level="error",
                row=row_number,
                column="duplicate_of_record_id",
                message="duplicate_of_record_id must reference an existing record_id.",
                value=duplicate_of,
            )


def is_conflict_value(value: object) -> bool:
    return normalize_lower(value) in {"yes", "y", "1", "true"}


def normalize_fulltext_available(value: object) -> str:
    normalized = normalize_lower(value)
    if normalized in {"yes", "y", "1", "true"}:
        return "yes"
    if normalized in {"no", "n", "0", "false"}:
        return "no"
    return ""


def normalize_fulltext_include(value: object) -> str:
    normalized = normalize_lower(value)
    if normalized in {"include", "included", "yes", "y", "1", "true"}:
        return "include"
    if normalized in {"exclude", "excluded", "no", "n", "0", "false"}:
        return "exclude"
    return ""


def normalize_screening_decision(value: object) -> str:
    normalized = normalize_lower(value)
    if normalized in {"include", "included", "yes", "y", "1", "true"}:
        return "include"
    if normalized in {"exclude", "excluded", "no", "n", "0", "false"}:
        return "exclude"
    if normalized in {"maybe", "uncertain", "pending"}:
        return "uncertain"
    return ""


def validate_title_abstract_results_rules(
    df: pd.DataFrame,
    *,
    file_name: str,
    issues: list[dict],
) -> None:
    if df.empty:
        return

    for index, row in df.iterrows():
        row_number = int(index) + 2
        reviewer1_decision = normalize_lower(row.get("reviewer1_decision", ""))
        reviewer2_decision = normalize_lower(row.get("reviewer2_decision", ""))
        conflict_flag = row.get("conflict", "")
        conflict_resolver = normalize(row.get("conflict_resolver", ""))
        resolution_decision = normalize_lower(row.get("resolution_decision", ""))
        final_decision = normalize_lower(row.get("final_decision", ""))
        exclusion_reason = normalize(row.get("exclusion_reason", ""))

        if reviewer1_decision and reviewer2_decision:
            expected_conflict = reviewer1_decision != reviewer2_decision
            observed_conflict = is_conflict_value(conflict_flag)
            if expected_conflict != observed_conflict:
                add_issue(
                    issues,
                    file_name=file_name,
                    level="error",
                    row=row_number,
                    column="conflict",
                    message="Conflict flag must reflect disagreement between reviewer decisions.",
                    value=f"{reviewer1_decision} vs {reviewer2_decision}; conflict={normalize(conflict_flag)}",
                )

            if expected_conflict:
                if is_empty(conflict_resolver):
                    add_issue(
                        issues,
                        file_name=file_name,
                        level="error",
                        row=row_number,
                        column="conflict_resolver",
                        message="Provide conflict_resolver when reviewer decisions disagree.",
                        value=conflict_resolver,
                    )

                if is_empty(resolution_decision):
                    add_issue(
                        issues,
                        file_name=file_name,
                        level="error",
                        row=row_number,
                        column="resolution_decision",
                        message="Provide resolution_decision when reviewer decisions disagree.",
                        value=resolution_decision,
                    )

                if resolution_decision and final_decision and final_decision != resolution_decision:
                    add_issue(
                        issues,
                        file_name=file_name,
                        level="warning",
                        row=row_number,
                        column="final_decision",
                        message="For conflicts, final_decision should match resolution_decision.",
                        value=f"resolution={resolution_decision}; final={final_decision}",
                    )

            if not expected_conflict:
                if not is_empty(conflict_resolver):
                    add_issue(
                        issues,
                        file_name=file_name,
                        level="warning",
                        row=row_number,
                        column="conflict_resolver",
                        message="Leave conflict_resolver empty when reviewers agree.",
                        value=conflict_resolver,
                    )

                if not is_empty(resolution_decision):
                    add_issue(
                        issues,
                        file_name=file_name,
                        level="warning",
                        row=row_number,
                        column="resolution_decision",
                        message="Leave resolution_decision empty when reviewers agree.",
                        value=resolution_decision,
                    )

                if final_decision and final_decision != reviewer1_decision:
                    add_issue(
                        issues,
                        file_name=file_name,
                        level="warning",
                        row=row_number,
                        column="final_decision",
                        message="When reviewers agree, final_decision should match the agreed decision.",
                        value=f"agreed={reviewer1_decision}; final={final_decision}",
                    )

        if final_decision == "exclude" and is_empty(exclusion_reason):
            add_issue(
                issues,
                file_name=file_name,
                level="warning",
                row=row_number,
                column="exclusion_reason",
                message="Provide exclusion_reason when final_decision=exclude.",
                value=exclusion_reason,
            )


def validate_screening_fulltext_rules(
    df: pd.DataFrame,
    *,
    file_name: str,
    issues: list[dict],
) -> None:
    if df.empty:
        return

    for index, row in df.iterrows():
        row_number = int(index) + 2
        fulltext_available = normalize_fulltext_available(row.get("fulltext_available", ""))
        include_decision = normalize_fulltext_include(row.get("include", ""))
        exclusion_reason = normalize(row.get("exclusion_reason", ""))

        if include_decision == "exclude" and is_empty(exclusion_reason):
            add_issue(
                issues,
                file_name=file_name,
                level="warning",
                row=row_number,
                column="exclusion_reason",
                message="Provide exclusion_reason when include=exclude.",
                value=exclusion_reason,
            )

        if fulltext_available == "no" and include_decision == "include":
            add_issue(
                issues,
                file_name=file_name,
                level="error",
                row=row_number,
                column="include",
                message="Cannot set include when fulltext_available=no.",
                value=normalize(row.get("include", "")),
            )


def validate_decision_log_rules(
    df: pd.DataFrame,
    *,
    file_name: str,
    issues: list[dict],
) -> None:
    if df.empty:
        return

    for index, row in df.iterrows():
        row_number = int(index) + 2
        record_id = normalize(row.get("record_id", ""))
        reviewer = normalize(row.get("reviewer", ""))
        stage = normalize_lower(row.get("stage", ""))
        decision_raw = normalize_lower(row.get("decision", ""))
        decision = normalize_screening_decision(row.get("decision", ""))
        reason = normalize(row.get("reason", ""))

        if is_empty(record_id):
            add_issue(
                issues,
                file_name=file_name,
                level="error",
                row=row_number,
                column="record_id",
                message="record_id is required for decision trace rows.",
                value=record_id,
            )

        if is_empty(reviewer):
            add_issue(
                issues,
                file_name=file_name,
                level="error",
                row=row_number,
                column="reviewer",
                message="reviewer is required for decision trace rows.",
                value=reviewer,
            )

        if is_empty(reason):
            level = "error" if decision in {"exclude", "uncertain"} else "warning"
            add_issue(
                issues,
                file_name=file_name,
                level=level,
                row=row_number,
                column="reason",
                message="Provide a concise reason for each decision row.",
                value=reason,
            )

        if stage in {"fulltext", "full_text", "full-text", "full text"} and decision_raw in {
            "maybe",
            "uncertain",
            "pending",
        }:
            add_issue(
                issues,
                file_name=file_name,
                level="warning",
                row=row_number,
                column="decision",
                message="Avoid `maybe` at full-text stage; resolve to include/exclude.",
                value=normalize(row.get("decision", "")),
            )


def validate_decision_log_master_alignment(
    *,
    decision_log_df: pd.DataFrame | None,
    master_df: pd.DataFrame | None,
    issues: list[dict],
) -> None:
    if decision_log_df is None or decision_log_df.empty:
        return
    if master_df is None or master_df.empty:
        return
    if "record_id" not in decision_log_df.columns or "record_id" not in master_df.columns:
        return

    master_ids = {
        normalize(record_id)
        for record_id in master_df["record_id"].fillna("").astype(str)
        if normalize(record_id)
    }
    if not master_ids:
        return

    working = non_empty_rows(decision_log_df.copy())
    for index, row in working.iterrows():
        row_number = int(index) + 2
        record_id = normalize(row.get("record_id", ""))
        if record_id and record_id not in master_ids:
            add_issue(
                issues,
                file_name="decision_log",
                level="warning",
                row=row_number,
                column="record_id",
                message="record_id is missing from master_records.csv.",
                value=record_id,
            )


def fulltext_log_prisma_metrics(df: pd.DataFrame | None) -> dict[str, int]:
    metrics = {
        "unique_records": 0,
        "reports_sought_for_retrieval": 0,
        "reports_not_retrieved": 0,
        "reports_assessed_full_text": 0,
        "reports_excluded_full_text": 0,
        "studies_included_qualitative_synthesis": 0,
    }

    if df is None or df.empty:
        return metrics

    required = {"record_id", "fulltext_available", "include"}
    if not required.issubset(df.columns):
        return metrics

    working = non_empty_rows(df.copy())
    working["record_id"] = working["record_id"].fillna("").astype(str).str.strip()
    working = working[working["record_id"].ne("")].copy()
    if working.empty:
        return metrics

    working = working.drop_duplicates(["record_id"], keep="last")
    working["fulltext_available_norm"] = working["fulltext_available"].apply(
        normalize_fulltext_available
    )
    working["include_norm"] = working["include"].apply(normalize_fulltext_include)

    not_retrieved = working["fulltext_available_norm"].eq("no")
    assessed = working["fulltext_available_norm"].eq("yes") & working["include_norm"].isin(
        {"include", "exclude"}
    )
    excluded = working["fulltext_available_norm"].eq("yes") & working["include_norm"].eq("exclude")
    included = working["fulltext_available_norm"].eq("yes") & working["include_norm"].eq("include")

    metrics["unique_records"] = int(working.shape[0])
    metrics["reports_not_retrieved"] = int(not_retrieved.sum())
    metrics["reports_assessed_full_text"] = int(assessed.sum())
    metrics["reports_excluded_full_text"] = int(excluded.sum())
    metrics["studies_included_qualitative_synthesis"] = int(included.sum())
    metrics["reports_sought_for_retrieval"] = (
        metrics["reports_assessed_full_text"] + metrics["reports_not_retrieved"]
    )
    return metrics


def sum_non_negative_int_column(df: pd.DataFrame, column: str) -> int:
    if df.empty or column not in df.columns:
        return 0

    total = 0
    for raw_value in df[column]:
        parsed = parse_non_negative_int_or_none(raw_value)
        if parsed is None:
            continue
        total += parsed
    return total


def prisma_stage_row_number(prisma_df: pd.DataFrame, stage: str) -> int:
    if prisma_df.empty or "stage" not in prisma_df.columns:
        return 1

    stage_series = prisma_df["stage"].fillna("").astype(str).str.strip()
    mask = stage_series.eq(stage)
    if not mask.any():
        return 1
    return int(prisma_df.index[mask][0]) + 2


def prisma_stage_count(prisma_df: pd.DataFrame, stage: str) -> int | None:
    if prisma_df.empty or "stage" not in prisma_df.columns or "count" not in prisma_df.columns:
        return None

    stage_series = prisma_df["stage"].fillna("").astype(str).str.strip()
    mask = stage_series.eq(stage)
    if not mask.any():
        return None

    return parse_non_negative_int_or_none(prisma_df.loc[mask, "count"].iloc[0])


def title_abstract_results_metrics(df: pd.DataFrame | None) -> dict[str, object]:
    metrics: dict[str, object] = {
        "unique_records": 0,
        "records_excluded": 0,
        "reports_sought_for_retrieval": 0,
        "included_record_ids": set(),
        "record_ids": set(),
    }

    if df is None or df.empty:
        return metrics

    if "record_id" not in df.columns:
        return metrics

    working = non_empty_rows(df.copy())
    working["record_id"] = working["record_id"].fillna("").astype(str).str.strip()
    working = working[working["record_id"].ne("")].copy()
    if working.empty:
        return metrics

    working = working.drop_duplicates(["record_id"], keep="last")

    if "final_decision" in working.columns:
        decision_series = working["final_decision"]
    elif "resolution_decision" in working.columns:
        decision_series = working["resolution_decision"]
    else:
        decision_series = working.get(
            "reviewer1_decision", pd.Series(["" for _ in range(len(working))])
        )

    decision_norm = decision_series.apply(normalize_screening_decision)
    record_ids = set(working["record_id"].tolist())
    included_ids = set(working.loc[decision_norm.eq("include"), "record_id"].tolist())

    metrics["unique_records"] = int(working.shape[0])
    metrics["records_excluded"] = int(decision_norm.eq("exclude").sum())
    metrics["reports_sought_for_retrieval"] = int(decision_norm.eq("include").sum())
    metrics["included_record_ids"] = included_ids
    metrics["record_ids"] = record_ids
    return metrics


def full_text_exclusion_reason_total(df: pd.DataFrame | None) -> int | None:
    if df is None or df.empty or "count" not in df.columns:
        return None

    working = non_empty_rows(df.copy())
    if working.empty:
        return 0

    total = 0
    seen_numeric = False
    for raw_count in working["count"]:
        parsed = parse_non_negative_int_or_none(raw_count)
        if parsed is None:
            continue
        total += parsed
        seen_numeric = True

    if seen_numeric:
        return total
    return 0


def title_abstract_stage_rows(screening_df: pd.DataFrame) -> pd.DataFrame:
    if screening_df.empty or "stage" not in screening_df.columns:
        return screening_df.iloc[0:0].copy()

    stage_series = screening_df["stage"].fillna("").astype(str).str.strip().str.lower()
    mask = stage_series.str.contains("title") | stage_series.str.contains("abstract")
    return screening_df[mask].copy()


def full_text_stage_rows(screening_df: pd.DataFrame) -> pd.DataFrame:
    if screening_df.empty or "stage" not in screening_df.columns:
        return screening_df.iloc[0:0].copy()

    stage_series = screening_df["stage"].fillna("").astype(str).str.strip().str.lower()
    mask = stage_series.str.contains("full") & stage_series.str.contains("text")
    return screening_df[mask].copy()


def validate_prisma_cross_file_consistency(
    *,
    search_df: pd.DataFrame,
    screening_df: pd.DataFrame,
    prisma_df: pd.DataFrame,
    fulltext_df: pd.DataFrame | None = None,
    title_abstract_results_df: pd.DataFrame | None = None,
    fulltext_reasons_df: pd.DataFrame | None = None,
    master_df: pd.DataFrame | None = None,
    issues: list[dict],
) -> None:
    search_non_empty = non_empty_rows(search_df)
    screening_non_empty = non_empty_rows(screening_df)
    prisma_non_empty = non_empty_rows(prisma_df)

    identified_from_search = sum_non_negative_int_column(search_non_empty, "results_total")
    duplicates_removed = prisma_stage_count(prisma_non_empty, "duplicates_removed")
    screened_in_prisma = prisma_stage_count(prisma_non_empty, "records_screened_title_abstract")
    identified_in_prisma = prisma_stage_count(prisma_non_empty, "records_identified_databases")
    sought_in_prisma = prisma_stage_count(prisma_non_empty, "reports_sought_for_retrieval")
    not_retrieved_in_prisma = prisma_stage_count(prisma_non_empty, "reports_not_retrieved")
    assessed_in_prisma = prisma_stage_count(prisma_non_empty, "reports_assessed_full_text")
    excluded_fulltext_in_prisma = prisma_stage_count(prisma_non_empty, "reports_excluded_full_text")
    excluded_ta_in_prisma = prisma_stage_count(prisma_non_empty, "records_excluded_title_abstract")
    included_in_prisma = prisma_stage_count(
        prisma_non_empty, "studies_included_qualitative_synthesis"
    )

    ta_metrics = title_abstract_results_metrics(title_abstract_results_df)
    fulltext_reasons_total = full_text_exclusion_reason_total(fulltext_reasons_df)

    if identified_in_prisma is not None and identified_in_prisma != identified_from_search:
        add_issue(
            issues,
            file_name="prisma_counts_template",
            level="error",
            row=prisma_stage_row_number(prisma_non_empty, "records_identified_databases"),
            column="count",
            message="PRISMA identified count must equal sum(`search_log.csv.results_total`).",
            value=f"prisma={identified_in_prisma}; search_log_sum={identified_from_search}",
        )

    if identified_in_prisma is None and identified_from_search > 0:
        add_issue(
            issues,
            file_name="prisma_counts_template",
            level="warning",
            row=1,
            column="count",
            message="Cannot verify identified consistency: `records_identified_databases` is empty.",
            value=f"search_log_sum={identified_from_search}",
        )

    if duplicates_removed is not None and screened_in_prisma is not None:
        if duplicates_removed > identified_from_search:
            add_issue(
                issues,
                file_name="prisma_counts_template",
                level="error",
                row=prisma_stage_row_number(prisma_non_empty, "duplicates_removed"),
                column="count",
                message="`duplicates_removed` cannot exceed identified count from search log.",
                value=f"duplicates_removed={duplicates_removed}; identified={identified_from_search}",
            )
        else:
            deduplicated_expected = identified_from_search - duplicates_removed
            if screened_in_prisma != deduplicated_expected:
                add_issue(
                    issues,
                    file_name="prisma_counts_template",
                    level="error",
                    row=prisma_stage_row_number(
                        prisma_non_empty, "records_screened_title_abstract"
                    ),
                    column="count",
                    message="Deduplicated count mismatch: `identified - duplicates_removed` must equal `records_screened_title_abstract`.",
                    value=(
                        f"identified={identified_from_search}; duplicates_removed={duplicates_removed}; "
                        f"expected_screened={deduplicated_expected}; prisma_screened={screened_in_prisma}"
                    ),
                )

    title_abstract_screening = title_abstract_stage_rows(screening_non_empty)
    screened_in_log = sum_non_negative_int_column(title_abstract_screening, "records_screened")
    if screened_in_prisma is not None:
        if title_abstract_screening.empty:
            if screened_in_prisma > 0:
                add_issue(
                    issues,
                    file_name="screening_daily_log",
                    level="warning",
                    row=1,
                    column="stage",
                    message=(
                        "Cannot verify screened consistency: `screening_daily_log.csv` has no title/abstract stage rows."
                    ),
                    value=f"prisma_screened={screened_in_prisma}",
                )
        elif screened_in_log != screened_in_prisma:
            add_issue(
                issues,
                file_name="screening_daily_log",
                level="error",
                row=1,
                column="records_screened",
                message=(
                    "`screening_daily_log.csv` title/abstract `records_screened` sum must match "
                    "`prisma_counts_template.csv:records_screened_title_abstract`."
                ),
                value=f"screening_sum={screened_in_log}; prisma_screened={screened_in_prisma}",
            )

    ta_screened = int(ta_metrics["unique_records"])
    ta_excluded = int(ta_metrics["records_excluded"])
    ta_sought = int(ta_metrics["reports_sought_for_retrieval"])
    if ta_screened > 0 and screened_in_prisma is not None and ta_screened != screened_in_prisma:
        add_issue(
            issues,
            file_name="screening_title_abstract_results",
            level="error",
            row=1,
            column="record_id",
            message=(
                "`screening_title_abstract_results.csv` unique records must match "
                "`prisma_counts_template.csv:records_screened_title_abstract`."
            ),
            value=f"ta_results_unique={ta_screened}; prisma_screened={screened_in_prisma}",
        )

    if (
        ta_screened > 0
        and excluded_ta_in_prisma is not None
        and ta_excluded != excluded_ta_in_prisma
    ):
        add_issue(
            issues,
            file_name="screening_title_abstract_results",
            level="error",
            row=1,
            column="final_decision",
            message=(
                "`screening_title_abstract_results.csv` exclude decisions must match "
                "`prisma_counts_template.csv:records_excluded_title_abstract`."
            ),
            value=f"ta_results_excluded={ta_excluded}; prisma_excluded={excluded_ta_in_prisma}",
        )

    full_text_screening = full_text_stage_rows(screening_non_empty)
    included_in_log = sum_non_negative_int_column(full_text_screening, "include_n")
    if included_in_prisma is not None:
        if full_text_screening.empty:
            if included_in_prisma > 0:
                add_issue(
                    issues,
                    file_name="screening_daily_log",
                    level="warning",
                    row=1,
                    column="stage",
                    message=(
                        "Cannot verify included consistency: `screening_daily_log.csv` has no full-text stage rows."
                    ),
                    value=f"prisma_included={included_in_prisma}",
                )
        elif included_in_log != included_in_prisma:
            add_issue(
                issues,
                file_name="screening_daily_log",
                level="error",
                row=1,
                column="include_n",
                message=(
                    "`screening_daily_log.csv` full-text `include_n` sum must match "
                    "`prisma_counts_template.csv:studies_included_qualitative_synthesis`."
                ),
                value=f"screening_included_sum={included_in_log}; prisma_included={included_in_prisma}",
            )

    fulltext_metrics = fulltext_log_prisma_metrics(fulltext_df)
    if fulltext_metrics["unique_records"] > 0:
        prisma_vs_fulltext = [
            (
                "reports_sought_for_retrieval",
                sought_in_prisma,
                fulltext_metrics["reports_sought_for_retrieval"],
            ),
            (
                "reports_not_retrieved",
                not_retrieved_in_prisma,
                fulltext_metrics["reports_not_retrieved"],
            ),
            (
                "reports_assessed_full_text",
                assessed_in_prisma,
                fulltext_metrics["reports_assessed_full_text"],
            ),
            (
                "reports_excluded_full_text",
                excluded_fulltext_in_prisma,
                fulltext_metrics["reports_excluded_full_text"],
            ),
            (
                "studies_included_qualitative_synthesis",
                included_in_prisma,
                fulltext_metrics["studies_included_qualitative_synthesis"],
            ),
        ]
        for stage, prisma_value, derived_value in prisma_vs_fulltext:
            if prisma_value is None:
                continue
            if prisma_value != derived_value:
                add_issue(
                    issues,
                    file_name="screening_fulltext_log",
                    level="error",
                    row=1,
                    column="record_id",
                    message=(
                        f"`screening_fulltext_log.csv` derived count must match "
                        f"`prisma_counts_template.csv:{stage}`."
                    ),
                    value=f"screening_fulltext={derived_value}; prisma={prisma_value}",
                )

        included_ids = ta_metrics.get("included_record_ids", set())
        if isinstance(included_ids, set):
            fulltext_working = (
                non_empty_rows(fulltext_df.copy())
                if isinstance(fulltext_df, pd.DataFrame)
                else pd.DataFrame()
            )
            if not fulltext_working.empty and "record_id" in fulltext_working.columns:
                fulltext_ids = {
                    record_id
                    for record_id in fulltext_working["record_id"]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    if record_id
                }
                if included_ids:
                    unexpected_ids = sorted(fulltext_ids - included_ids)
                    if unexpected_ids:
                        add_issue(
                            issues,
                            file_name="screening_fulltext_log",
                            level="warning",
                            row=1,
                            column="record_id",
                            message=(
                                "Full-text record_ids should come from title/abstract includes "
                                "(`screening_title_abstract_results.csv`)."
                            ),
                            value=f"unexpected_fulltext_ids={','.join(unexpected_ids[:5])}",
                        )
    elif ta_screened > 0 and sought_in_prisma is not None and ta_sought != sought_in_prisma:
        add_issue(
            issues,
            file_name="screening_title_abstract_results",
            level="error",
            row=1,
            column="final_decision",
            message=(
                "Without full-text log rows, title/abstract include decisions must match "
                "`prisma_counts_template.csv:reports_sought_for_retrieval`."
            ),
            value=f"ta_results_include={ta_sought}; prisma_sought={sought_in_prisma}",
        )

    if fulltext_reasons_total is not None and excluded_fulltext_in_prisma is not None:
        if fulltext_reasons_total > 0 and fulltext_reasons_total != excluded_fulltext_in_prisma:
            add_issue(
                issues,
                file_name="full_text_exclusion_reasons",
                level="error",
                row=1,
                column="count",
                message=(
                    "`full_text_exclusion_reasons.csv` total count must match "
                    "`prisma_counts_template.csv:reports_excluded_full_text`."
                ),
                value=f"reasons_total={fulltext_reasons_total}; prisma_excluded_full_text={excluded_fulltext_in_prisma}",
            )
        if excluded_fulltext_in_prisma > 0 and fulltext_reasons_total == 0:
            add_issue(
                issues,
                file_name="full_text_exclusion_reasons",
                level="warning",
                row=1,
                column="count",
                message="Populate exclusion-reason counts when reports_excluded_full_text > 0.",
                value=f"prisma_excluded_full_text={excluded_fulltext_in_prisma}",
            )

    if (
        fulltext_reasons_total is not None
        and fulltext_metrics["unique_records"] > 0
        and fulltext_reasons_total > 0
    ):
        fulltext_excluded = int(fulltext_metrics["reports_excluded_full_text"])
        if fulltext_reasons_total != fulltext_excluded:
            add_issue(
                issues,
                file_name="full_text_exclusion_reasons",
                level="error",
                row=1,
                column="count",
                message=(
                    "`full_text_exclusion_reasons.csv` total count must match derived exclusions "
                    "from `screening_fulltext_log.csv`."
                ),
                value=f"reasons_total={fulltext_reasons_total}; screening_fulltext_excluded={fulltext_excluded}",
            )

    if isinstance(master_df, pd.DataFrame) and not master_df.empty:
        master_working = non_empty_rows(master_df.copy())
        if "record_id" in master_working.columns:
            master_ids = {
                record_id
                for record_id in master_working["record_id"].fillna("").astype(str).str.strip()
                if record_id
            }
            ta_record_ids = ta_metrics.get("record_ids", set())
            if isinstance(ta_record_ids, set) and ta_record_ids and master_ids:
                missing_in_master = sorted(ta_record_ids - master_ids)
                if missing_in_master:
                    add_issue(
                        issues,
                        file_name="screening_title_abstract_results",
                        level="warning",
                        row=1,
                        column="record_id",
                        message="Some title/abstract record_ids are missing from master_records.csv.",
                        value=f"missing_record_ids={','.join(missing_in_master[:5])}",
                    )

    if (
        included_in_prisma is not None
        and screened_in_prisma is not None
        and included_in_prisma > screened_in_prisma
    ):
        add_issue(
            issues,
            file_name="prisma_counts_template",
            level="error",
            row=prisma_stage_row_number(prisma_non_empty, "studies_included_qualitative_synthesis"),
            column="count",
            message="Included studies cannot exceed screened records.",
            value=f"included={included_in_prisma}; screened={screened_in_prisma}",
        )


def read_csv_for_cross_check(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None

    try:
        return pd.read_csv(path, dtype=str)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return None


def validate_file(spec: dict, *, template_filename_as_info: bool = True) -> tuple[dict, list[dict]]:
    path = Path(spec["path"])
    file_name = spec["name"]
    required_columns = spec["required"]
    issues: list[dict] = []

    result = {
        "name": file_name,
        "path": path.as_posix(),
        "exists": path.exists(),
        "total_rows": 0,
        "checked_rows": 0,
        "missing_columns": [],
    }

    if not path.exists():
        add_issue(
            issues,
            file_name=file_name,
            level="error",
            row=1,
            column="file",
            message="CSV file is missing.",
            value=path.as_posix(),
        )
        return result, issues

    try:
        raw_df = pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        raw_df = pd.DataFrame()
    except pd.errors.ParserError as error:
        add_issue(
            issues,
            file_name=file_name,
            level="error",
            row=1,
            column="file",
            message="CSV parsing failed.",
            value=str(error),
        )
        return result, issues

    result["total_rows"] = int(raw_df.shape[0])
    missing_columns = [column for column in required_columns if column not in raw_df.columns]

    legacy_optional_missing: dict[str, str] = spec.get("legacy_optional_missing", {})
    compatible_missing = [column for column in missing_columns if column in legacy_optional_missing]
    for column in compatible_missing:
        if column not in raw_df.columns:
            raw_df[column] = ""
        add_issue(
            issues,
            file_name=file_name,
            level="warning",
            row=1,
            column=column,
            message=legacy_optional_missing[column],
        )

    missing_columns = [column for column in missing_columns if column not in compatible_missing]
    result["missing_columns"] = missing_columns

    for column in missing_columns:
        add_issue(
            issues,
            file_name=file_name,
            level="error",
            row=1,
            column=column,
            message="Required column is missing.",
        )

    if missing_columns:
        return result, issues

    checked_df = non_empty_rows(raw_df)
    result["checked_rows"] = int(checked_df.shape[0])

    validate_allowed_values(
        checked_df,
        file_name=file_name,
        allowed_rules=spec["allowed"],
        issues=issues,
    )
    validate_non_negative_int_columns(
        checked_df,
        file_name=file_name,
        columns=spec["non_negative_int"],
        issues=issues,
    )
    validate_date_columns(
        checked_df,
        file_name=file_name,
        columns=spec["date_columns"],
        issues=issues,
    )
    if file_name == "search_log":
        validate_search_log_export_files(
            checked_df,
            file_name=file_name,
            raw_exports_dir=RAW_EXPORTS_DIR,
            issues=issues,
            template_filename_as_info=template_filename_as_info,
        )
        validate_search_log_ranges(
            checked_df,
            file_name=file_name,
            issues=issues,
        )
    if file_name == "screening_daily_log":
        validate_screening_daily_ranges(
            checked_df,
            file_name=file_name,
            issues=issues,
        )
    if file_name == "screening_title_abstract_results":
        validate_title_abstract_results_rules(
            checked_df,
            file_name=file_name,
            issues=issues,
        )
    if file_name == "screening_fulltext_log":
        validate_screening_fulltext_rules(
            checked_df,
            file_name=file_name,
            issues=issues,
        )
    if file_name == "decision_log":
        validate_decision_log_rules(
            checked_df,
            file_name=file_name,
            issues=issues,
        )
    if file_name == "master_records":
        validate_master_records_rules(
            checked_df,
            file_name=file_name,
            issues=issues,
        )

    return result, issues


def migrate_master_records_abstract_column(path: Path) -> tuple[bool, str]:
    if not path.exists():
        return False, f"Migration skipped: file not found ({path.as_posix()})."

    try:
        frame = pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        frame = pd.DataFrame()
    except pd.errors.ParserError as error:
        return False, f"Migration skipped: CSV parsing failed ({error})."

    if "abstract" in frame.columns:
        return False, "Migration skipped: `abstract` column already present."

    insert_at = (
        int(frame.columns.get_loc("title")) + 1 if "title" in frame.columns else len(frame.columns)
    )
    frame.insert(insert_at, "abstract", "")
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)

    return True, f"Migrated: added `abstract` column to {path.as_posix()}."


def build_summary(results: list[dict], issues: list[dict]) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    errors = [issue for issue in issues if issue["level"] == "error"]
    warnings = [issue for issue in issues if issue["level"] == "warning"]
    infos = [issue for issue in issues if issue["level"] == "info"]

    lines = []
    lines.append("# CSV Input Validation Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Files")
    lines.append("")
    lines.append("| File | Path | Rows | Checked rows | Status |")
    lines.append("|---|---|---:|---:|---|")

    issue_count_by_file: dict[str, int] = {}
    for issue in issues:
        issue_count_by_file[issue["file"]] = issue_count_by_file.get(issue["file"], 0) + 1

    for result in results:
        count = issue_count_by_file.get(result["name"], 0)
        status = "✅ ok" if count == 0 else f"❌ {count} issue(s)"
        lines.append(
            f"| {result['name']} | `{result['path']}` | {result['total_rows']} | {result['checked_rows']} | {status} |"
        )

    lines.append("")
    lines.append("## Totals")
    lines.append("")
    lines.append(f"- Files checked: {len(results)}")
    lines.append(f"- Errors: {len(errors)}")
    lines.append(f"- Warnings: {len(warnings)}")
    lines.append(f"- Info: {len(infos)}")

    lines.append("")
    lines.append("## Issues")
    lines.append("")
    if issues:
        lines.append("| File | Row | Level | Column | Message | Value |")
        lines.append("|---|---:|---|---|---|---|")
        for issue in issues:
            value = issue["value"].replace("|", "\\|") if issue["value"] else ""
            lines.append(
                f"| {issue['file']} | {issue['row']} | {issue['level']} | `{issue['column']}` | {issue['message']} | `{value}` |"
            )
    else:
        lines.append("- ✅ No issues found.")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Empty rows are ignored.")
    lines.append(
        "- Validation covers required columns, allowed categorical values, integer counters, date format (`YYYY-MM-DD`), and temporal/numeric range checks."
    )
    lines.append(
        "- Cross-file consistency checks compare `search_log.csv`, `screening_daily_log.csv`, `screening_title_abstract_results.csv`, `screening_fulltext_log.csv`, `full_text_exclusion_reasons.csv`, `master_records.csv`, and `prisma_counts_template.csv` for PRISMA stage math and record-link coherence."
    )
    lines.append(
        "- Decision trace checks validate `decision_log.csv` stage/decision vocabulary, per-row reasons, and record_id alignment with `master_records.csv`."
    )
    lines.append(
        "- For `search_log.csv`, `export_filename` is checked against files in `../02_data/raw/`."
    )
    lines.append(
        "- Placeholder export filenames (e.g., with `YYYY-MM-DD`) are reported as info when `results_exported` is empty."
    )
    lines.append(
        "- Backward compatibility: legacy `master_records.csv` files without `abstract` are accepted with a warning; use `--migrate-master-records` to persist schema upgrade."
    )

    return "\n".join(lines) + "\n"


def should_fail(fail_on: str, error_count: int, warning_count: int) -> bool:
    mode = fail_on.strip().lower()
    if mode == "none":
        return False
    if mode == "warning":
        return (error_count + warning_count) > 0
    return error_count > 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate processed CSV inputs (schema + allowed values)."
    )
    parser.add_argument(
        "--output",
        default="outputs/csv_input_validation_summary.md",
        help="Path to markdown validation summary",
    )
    parser.add_argument(
        "--fail-on",
        default="error",
        choices=["none", "warning", "error"],
        help="Fail mode: error (default), warning, none",
    )
    parser.add_argument(
        "--template-export-filename-info",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Treat placeholder export filenames (e.g., YYYY-MM-DD templates) as info when results_exported is empty.",
    )
    parser.add_argument(
        "--migrate-master-records",
        action="store_true",
        help="Auto-migrate legacy master_records.csv by adding missing `abstract` column before validation.",
    )
    args = parser.parse_args()

    if args.migrate_master_records:
        master_spec = next((spec for spec in FILE_SPECS if spec["name"] == "master_records"), None)
        if master_spec is not None:
            _, migration_message = migrate_master_records_abstract_column(Path(master_spec["path"]))
            print(migration_message)

    all_results: list[dict] = []
    all_issues: list[dict] = []

    for spec in FILE_SPECS:
        result, issues = validate_file(
            spec, template_filename_as_info=args.template_export_filename_info
        )
        all_results.append(result)
        all_issues.extend(issues)

    files_with_schema_errors = {issue["file"] for issue in all_issues if issue["level"] == "error"}
    consistency_files = {
        "search_log",
        "screening_daily_log",
        "screening_title_abstract_results",
        "screening_fulltext_log",
        "decision_log",
        "prisma_counts_template",
        "full_text_exclusion_reasons",
        "master_records",
    }
    if not (files_with_schema_errors & consistency_files):
        spec_paths = {spec["name"]: Path(spec["path"]) for spec in FILE_SPECS}
        search_df = read_csv_for_cross_check(spec_paths["search_log"])
        screening_df = read_csv_for_cross_check(spec_paths["screening_daily_log"])
        title_abstract_results_df = read_csv_for_cross_check(
            spec_paths["screening_title_abstract_results"]
        )
        fulltext_df = read_csv_for_cross_check(spec_paths["screening_fulltext_log"])
        decision_log_df = read_csv_for_cross_check(spec_paths["decision_log"])
        fulltext_reasons_df = read_csv_for_cross_check(spec_paths["full_text_exclusion_reasons"])
        master_df = read_csv_for_cross_check(spec_paths["master_records"])
        prisma_df = read_csv_for_cross_check(spec_paths["prisma_counts_template"])
        if search_df is not None and screening_df is not None and prisma_df is not None:
            validate_prisma_cross_file_consistency(
                search_df=search_df,
                screening_df=screening_df,
                prisma_df=prisma_df,
                fulltext_df=fulltext_df,
                title_abstract_results_df=title_abstract_results_df,
                fulltext_reasons_df=fulltext_reasons_df,
                master_df=master_df,
                issues=all_issues,
            )
        validate_decision_log_master_alignment(
            decision_log_df=decision_log_df,
            master_df=master_df,
            issues=all_issues,
        )

    summary = build_summary(all_results, all_issues)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(summary, encoding="utf-8")

    error_count = sum(1 for issue in all_issues if issue["level"] == "error")
    warning_count = sum(1 for issue in all_issues if issue["level"] == "warning")

    print(f"Wrote: {output_path}")
    print(f"Validation issues: errors={error_count}, warnings={warning_count}")

    if should_fail(args.fail_on, error_count=error_count, warning_count=warning_count):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

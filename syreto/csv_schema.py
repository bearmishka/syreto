from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

EMPTY_VALUES = {"", "nan", "none"}


@dataclass(frozen=True)
class CsvSchemaContract:
    label: str
    relative_path: str
    required_columns: tuple[str, ...]
    required_non_empty: tuple[str, ...] = ()
    unique_key_columns: tuple[str, ...] = ()
    legacy_optional_missing: tuple[str, ...] = ()


@dataclass(frozen=True)
class CsvSchemaFinding:
    level: str
    detail: str


CSV_SCHEMA_CONTRACTS: tuple[CsvSchemaContract, ...] = (
    CsvSchemaContract(
        label="search log",
        relative_path="processed/search_log.csv",
        required_columns=(
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
        ),
        required_non_empty=("database", "date_searched", "results_total", "results_exported"),
    ),
    CsvSchemaContract(
        label="master records",
        relative_path="processed/master_records.csv",
        required_columns=(
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
        ),
        required_non_empty=("record_id", "title", "authors", "year"),
        unique_key_columns=("record_id",),
        legacy_optional_missing=("abstract",),
    ),
    CsvSchemaContract(
        label="screening title/abstract results",
        relative_path="processed/screening_title_abstract_results.csv",
        required_columns=(
            "record_id",
            "reviewer1_decision",
            "reviewer2_decision",
            "conflict",
            "conflict_resolver",
            "resolution_decision",
            "final_decision",
            "exclusion_reason",
        ),
        required_non_empty=("record_id", "final_decision", "conflict"),
        unique_key_columns=("record_id",),
    ),
    CsvSchemaContract(
        label="screening fulltext log",
        relative_path="processed/screening_fulltext_log.csv",
        required_columns=(
            "record_id",
            "fulltext_available",
            "include",
            "exclusion_reason",
            "reviewer",
            "notes",
        ),
        required_non_empty=("record_id", "fulltext_available", "include"),
        unique_key_columns=("record_id",),
    ),
    CsvSchemaContract(
        label="decision log",
        relative_path="processed/decision_log.csv",
        required_columns=("record_id", "stage", "decision", "reason", "reviewer"),
        required_non_empty=("record_id", "stage", "decision", "reviewer"),
    ),
    CsvSchemaContract(
        label="prisma counts template",
        relative_path="processed/prisma_counts_template.csv",
        required_columns=("stage", "count", "notes"),
        required_non_empty=("stage", "count"),
        unique_key_columns=("stage",),
    ),
    CsvSchemaContract(
        label="full text exclusion reasons",
        relative_path="processed/full_text_exclusion_reasons.csv",
        required_columns=("reason", "count", "notes"),
        required_non_empty=("reason", "count"),
        unique_key_columns=("reason",),
    ),
    CsvSchemaContract(
        label="extraction template",
        relative_path="codebook/extraction_template.csv",
        required_columns=("ci_lower", "ci_upper", "sample_size", "effect_direction"),
    ),
)


def normalize_csv_value(value: object) -> str:
    text = str(value or "").strip()
    if text.lower() in EMPTY_VALUES:
        return ""
    return text


def _row_has_any_data(row: dict[str, str | None]) -> bool:
    return any(normalize_csv_value(value) for value in row.values())


def schema_contract_paths(data_root: Path) -> tuple[tuple[CsvSchemaContract, Path], ...]:
    return tuple(
        (contract, data_root / contract.relative_path) for contract in CSV_SCHEMA_CONTRACTS
    )


def validate_csv_schema_contract(path: Path, contract: CsvSchemaContract) -> list[CsvSchemaFinding]:
    findings: list[CsvSchemaFinding] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                return [CsvSchemaFinding("error", "missing header row")]

            header = {str(field).strip() for field in reader.fieldnames if field is not None}
            missing_columns = sorted(set(contract.required_columns) - header)
            legacy_missing = sorted(
                column for column in missing_columns if column in contract.legacy_optional_missing
            )
            hard_missing = [
                column
                for column in missing_columns
                if column not in contract.legacy_optional_missing
            ]

            for column in legacy_missing:
                findings.append(
                    CsvSchemaFinding("warn", f"legacy-compatible missing column: {column}")
                )

            if hard_missing:
                findings.append(
                    CsvSchemaFinding(
                        "error", f"missing required columns: {', '.join(hard_missing)}"
                    )
                )
                return findings

            seen_keys: dict[tuple[str, ...], int] = {}
            for row_index, row in enumerate(reader, start=2):
                if not _row_has_any_data(row):
                    continue

                for column in contract.required_non_empty:
                    if normalize_csv_value(row.get(column)) == "":
                        findings.append(
                            CsvSchemaFinding(
                                "error",
                                f"row {row_index}: required field is empty: {column}",
                            )
                        )

                if contract.unique_key_columns:
                    key = tuple(
                        normalize_csv_value(row.get(column))
                        for column in contract.unique_key_columns
                    )
                    if any(part == "" for part in key):
                        continue
                    if key in seen_keys:
                        findings.append(
                            CsvSchemaFinding(
                                "error",
                                "row "
                                f"{row_index}: duplicate key for {', '.join(contract.unique_key_columns)} "
                                f"(first seen at row {seen_keys[key]}): {' | '.join(key)}",
                            )
                        )
                    else:
                        seen_keys[key] = row_index
    except OSError as exc:
        return [CsvSchemaFinding("error", f"failed to read CSV: {exc}")]
    except UnicodeDecodeError as exc:
        return [CsvSchemaFinding("error", f"invalid UTF-8 CSV content: {exc}")]

    return findings

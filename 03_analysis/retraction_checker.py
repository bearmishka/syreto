from __future__ import annotations

import argparse
import csv
import io
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

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

YES_VALUES = {"yes", "y", "1", "true"}
YEAR_PATTERN = re.compile(r"(19|20)\d{2}")
DOI_PATTERN = re.compile(r"10\.\d{4,9}/[^\s]+", re.IGNORECASE)

DEFAULT_RETRACTION_DB_URL = "https://gitlab.com/crossref/retraction-watch-data/-/raw/main/retraction_watch.csv"

PREFERRED_DOI_COLUMNS = [
    "OriginalPaperDOI",
    "Original Paper DOI",
    "original_paper_doi",
    "doi",
]

RECORD_ID_COLUMNS = ["Record ID", "record_id", "RecordID"]
TITLE_COLUMNS = ["Title", "title"]
RETRACTION_DATE_COLUMNS = ["RetractionDate", "Retraction Date", "date_retracted"]
RETRACTION_REASON_COLUMNS = [
    "RetractionNature",
    "Retraction Nature",
    "Reason",
    "Reasons",
    "RetractionReason",
]
URL_COLUMNS = ["URLS", "URL", "RetractionURL", "Retraction URL"]


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def normalize_lower(value: object) -> str:
    return normalize(value).lower()


def is_missing(value: object) -> bool:
    return normalize_lower(value) in MISSING_CODES


def parse_year(value: object) -> str:
    text = normalize(value)
    if not text:
        return ""
    match = YEAR_PATTERN.search(text)
    return match.group(0) if match else ""


def normalize_doi(value: object) -> str:
    text = normalize_lower(value)
    if not text:
        return ""
    text = text.replace("https://doi.org/", "").replace("http://doi.org/", "")
    text = text.replace("doi:", "").strip()
    match = DOI_PATTERN.search(text)
    if not match:
        return ""
    return match.group(0).rstrip(".,);")


def normalize_title(value: object) -> str:
    text = normalize_lower(value)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_first_author(value: object) -> str:
    text = normalize(value)
    if not text:
        return ""

    first = re.split(r";|\||\sand\s", text, maxsplit=1)[0].strip()
    if "," in first:
        surname = first.split(",", maxsplit=1)[0].strip()
    else:
        tokens = [token for token in re.split(r"\s+", first) if token]
        if len(tokens) >= 2 and len(tokens[-1]) <= 2:
            surname = tokens[0]
        elif tokens:
            surname = tokens[-1]
        else:
            surname = ""
    return re.sub(r"[^a-z0-9]", "", surname.lower())


def non_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    mask = df.apply(lambda row: any(not is_missing(value) for value in row), axis=1)
    return df[mask].copy()


def normalize_column_name(name: object) -> str:
    return re.sub(r"[^a-z0-9]", "", normalize_lower(name))


def find_first_matching_column(fieldnames: list[str], candidates: list[str]) -> str:
    normalized_field_map = {normalize_column_name(name): name for name in fieldnames}
    for candidate in candidates:
        match = normalized_field_map.get(normalize_column_name(candidate))
        if match:
            return match
    return ""


def find_doi_columns(fieldnames: list[str]) -> list[str]:
    selected: list[str] = []
    for candidate in PREFERRED_DOI_COLUMNS:
        column = find_first_matching_column(fieldnames, [candidate])
        if column and column not in selected:
            selected.append(column)

    if selected:
        return selected

    for column in fieldnames:
        if "doi" in normalize_column_name(column):
            selected.append(column)
    return selected


def read_csv_with_fallback(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")
    try:
        return pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def load_extraction(path: Path) -> pd.DataFrame:
    extraction_df = read_csv_with_fallback(path)

    required = [
        "study_id",
        "first_author",
        "year",
        "title",
        "doi",
        "record_id",
        "source_record_id",
    ]
    for column in required:
        if column not in extraction_df.columns:
            extraction_df[column] = ""

    return extraction_df


def load_master(path: Path, include_duplicates: bool) -> pd.DataFrame:
    master_df = read_csv_with_fallback(path)
    if master_df.empty:
        return master_df

    required = [
        "record_id",
        "source_record_id",
        "title",
        "authors",
        "year",
        "doi",
        "is_duplicate",
    ]
    for column in required:
        if column not in master_df.columns:
            master_df[column] = ""

    if include_duplicates:
        return master_df

    is_dup = master_df["is_duplicate"].fillna("").astype(str).str.strip().str.lower().isin(YES_VALUES)
    return master_df.loc[~is_dup].copy()


def select_included_extraction_rows(extraction_df: pd.DataFrame) -> pd.DataFrame:
    non_empty_df = non_empty_rows(extraction_df)
    if non_empty_df.empty:
        return non_empty_df
    mask = ~non_empty_df["study_id"].apply(is_missing)
    return non_empty_df.loc[mask].copy()


def build_master_indexes(master_df: pd.DataFrame) -> dict[str, dict[str, list[int]]]:
    indexes = {
        "record_id": {},
        "source_record_id": {},
        "doi": {},
        "author_year": {},
        "title_author_year": {},
    }

    if master_df.empty:
        return indexes

    for row_idx, row in master_df.iterrows():
        record_id = normalize(row.get("record_id", ""))
        source_record_id = normalize(row.get("source_record_id", ""))
        doi = normalize_doi(row.get("doi", ""))
        year = parse_year(row.get("year", ""))
        author_norm = normalize_first_author(row.get("authors", ""))
        title_norm = normalize_title(row.get("title", ""))

        if record_id:
            indexes["record_id"].setdefault(record_id, []).append(row_idx)
        if source_record_id:
            indexes["source_record_id"].setdefault(source_record_id, []).append(row_idx)
        if doi:
            indexes["doi"].setdefault(doi, []).append(row_idx)
        if author_norm and year:
            author_year_key = f"{author_norm}|{year}"
            indexes["author_year"].setdefault(author_year_key, []).append(row_idx)
            if title_norm:
                title_author_year_key = f"{title_norm}|{author_norm}|{year}"
                indexes["title_author_year"].setdefault(title_author_year_key, []).append(row_idx)

    return indexes


def pick_master_match(
    extraction_row: pd.Series,
    master_df: pd.DataFrame,
    indexes: dict[str, dict[str, list[int]]],
) -> tuple[pd.Series | None, str]:
    if master_df.empty:
        return None, "master_empty"

    ext_record_id = normalize(extraction_row.get("record_id", ""))
    ext_source_record_id = normalize(extraction_row.get("source_record_id", ""))
    ext_doi = normalize_doi(extraction_row.get("doi", ""))
    ext_author = normalize_first_author(extraction_row.get("first_author", ""))
    ext_year = parse_year(extraction_row.get("year", ""))
    ext_title = normalize_title(extraction_row.get("title", ""))

    if ext_record_id and ext_record_id in indexes["record_id"]:
        return master_df.loc[indexes["record_id"][ext_record_id][0]], "record_id"

    if ext_source_record_id and ext_source_record_id in indexes["source_record_id"]:
        return master_df.loc[indexes["source_record_id"][ext_source_record_id][0]], "source_record_id"

    if ext_doi and ext_doi in indexes["doi"]:
        return master_df.loc[indexes["doi"][ext_doi][0]], "doi"

    if ext_author and ext_year and ext_title:
        key = f"{ext_title}|{ext_author}|{ext_year}"
        if key in indexes["title_author_year"]:
            return master_df.loc[indexes["title_author_year"][key][0]], "title_author_year"

    if ext_author and ext_year:
        key = f"{ext_author}|{ext_year}"
        matches = indexes["author_year"].get(key, [])
        if len(matches) == 1:
            return master_df.loc[matches[0]], "author_year_unique"
        if len(matches) > 1:
            return master_df.loc[matches[0]], "author_year_ambiguous_first"

    return None, "no_match"


def resolve_source_studies(
    included_df: pd.DataFrame,
    master_df: pd.DataFrame,
    indexes: dict[str, dict[str, list[int]]],
) -> tuple[list[dict[str, str]], Counter[str]]:
    source_rows: list[dict[str, str]] = []
    doi_source_counts: Counter[str] = Counter()

    for _, row in included_df.iterrows():
        study_id = normalize(row.get("study_id", ""))
        first_author = normalize(row.get("first_author", ""))
        year = parse_year(row.get("year", ""))

        extraction_doi = normalize_doi(row.get("doi", ""))
        match_method = "extraction_doi"
        master_row = None
        if not extraction_doi:
            master_row, match_method = pick_master_match(row, master_df, indexes)

        master_doi = ""
        master_record_id = ""
        master_source_record_id = ""
        if master_row is not None:
            master_doi = normalize_doi(master_row.get("doi", ""))
            master_record_id = normalize(master_row.get("record_id", ""))
            master_source_record_id = normalize(master_row.get("source_record_id", ""))

        source_doi = extraction_doi or master_doi
        doi_source = match_method if source_doi else "unresolved"

        source_rows.append(
            {
                "study_id": study_id,
                "first_author": first_author,
                "year": year,
                "source_doi": source_doi,
                "doi_source": doi_source,
                "match_method": match_method,
                "master_record_id": master_record_id,
                "master_source_record_id": master_source_record_id,
            }
        )
        if source_doi:
            doi_source_counts[doi_source] += 1

    return source_rows, doi_source_counts


def fetch_retraction_watch_csv(url: str, timeout: int) -> str:
    request = Request(url, headers={"User-Agent": "syreto-retraction-checker/1.0"})
    try:
        with urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")
    except HTTPError as error:
        raise RuntimeError(f"Retraction DB HTTP error {error.code}: {error.reason}") from error
    except URLError as error:
        raise RuntimeError(f"Retraction DB network error: {error.reason}") from error


def first_non_empty_from_row(row: dict[str, object], fieldnames: list[str], candidates: list[str]) -> str:
    selected = find_first_matching_column(fieldnames, candidates)
    if not selected:
        return ""
    return normalize(row.get(selected, ""))


def parse_retraction_database(csv_text: str) -> tuple[dict[str, list[dict[str, str]]], dict[str, object]]:
    reader = csv.DictReader(io.StringIO(csv_text))
    fieldnames = list(reader.fieldnames or [])
    if not fieldnames:
        raise RuntimeError("Retraction DB payload has no CSV header.")

    doi_columns = find_doi_columns(fieldnames)
    if not doi_columns:
        raise RuntimeError("Retraction DB payload has no DOI-like columns.")

    index: dict[str, list[dict[str, str]]] = {}
    rows_scanned = 0
    rows_with_doi = 0

    for row in reader:
        rows_scanned += 1
        doi_values = {
            normalize_doi(row.get(column, ""))
            for column in doi_columns
            if normalize_doi(row.get(column, ""))
        }
        if not doi_values:
            continue
        rows_with_doi += 1

        entry = {
            "record_id": first_non_empty_from_row(row, fieldnames, RECORD_ID_COLUMNS),
            "title": first_non_empty_from_row(row, fieldnames, TITLE_COLUMNS),
            "retraction_date": first_non_empty_from_row(row, fieldnames, RETRACTION_DATE_COLUMNS),
            "retraction_reason": first_non_empty_from_row(row, fieldnames, RETRACTION_REASON_COLUMNS),
            "url": first_non_empty_from_row(row, fieldnames, URL_COLUMNS),
        }

        for doi in sorted(doi_values):
            index.setdefault(doi, []).append(entry)

    metadata = {
        "rows_scanned": rows_scanned,
        "rows_with_doi": rows_with_doi,
        "doi_columns": doi_columns,
        "unique_doi_count": len(index),
    }
    return index, metadata


def dedupe_join(values: list[str], *, separator: str = " | ") -> str:
    deduped = [value for value in dict.fromkeys(value for value in values if normalize(value))]
    return separator.join(deduped)


def build_result_rows(
    source_rows: list[dict[str, str]],
    retraction_index: dict[str, list[dict[str, str]]],
    *,
    api_error: str,
) -> tuple[list[dict[str, str]], Counter[str]]:
    result_rows: list[dict[str, str]] = []
    status_counts: Counter[str] = Counter()

    for source in source_rows:
        doi = normalize_doi(source.get("source_doi", ""))
        hits = retraction_index.get(doi, []) if doi else []

        if not doi:
            status = "missing_doi"
            notes = "No DOI resolved for this included study."
        elif api_error:
            status = "check_not_performed_api_error"
            notes = api_error
        elif hits:
            status = "retracted"
            notes = "Retraction signal found in Retraction Watch dataset."
        else:
            status = "not_retracted_in_dataset"
            notes = "No matching DOI found in Retraction Watch dataset snapshot."

        status_counts[status] += 1

        result_rows.append(
            {
                "study_id": normalize(source.get("study_id", "")),
                "first_author": normalize(source.get("first_author", "")),
                "year": normalize(source.get("year", "")),
                "source_doi": doi,
                "doi_source": normalize(source.get("doi_source", "")),
                "match_method": normalize(source.get("match_method", "")),
                "master_record_id": normalize(source.get("master_record_id", "")),
                "master_source_record_id": normalize(source.get("master_source_record_id", "")),
                "retraction_status": status,
                "retraction_hit_count": str(len(hits)) if hits else "0",
                "retraction_record_ids": dedupe_join([normalize(hit.get("record_id", "")) for hit in hits]),
                "retraction_dates": dedupe_join([normalize(hit.get("retraction_date", "")) for hit in hits]),
                "retraction_reasons": dedupe_join([normalize(hit.get("retraction_reason", "")) for hit in hits]),
                "retraction_titles": dedupe_join([normalize(hit.get("title", "")) for hit in hits]),
                "retraction_urls": dedupe_join([normalize(hit.get("url", "")) for hit in hits]),
                "notes": notes,
            }
        )

    return result_rows, status_counts


def render_summary(
    *,
    extraction_path: Path,
    master_path: Path,
    database_source: str,
    database_fetch_performed: bool,
    database_metadata: dict[str, object],
    api_error: str,
    included_rows: int,
    with_doi_rows: int,
    status_counts: Counter[str],
    doi_source_counts: Counter[str],
    result_output_path: Path,
    summary_output_path: Path,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = []
    lines.append("# Retraction Checker Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Extraction input: `{extraction_path.as_posix()}`")
    lines.append(f"- Master records input: `{master_path.as_posix()}`")
    lines.append(f"- Retraction dataset source: `{database_source}`")
    lines.append(f"- Results output: `{result_output_path.as_posix()}`")
    lines.append(f"- Summary output: `{summary_output_path.as_posix()}`")
    lines.append("")
    lines.append("## Coverage")
    lines.append("")
    lines.append(f"- Included studies evaluated: {included_rows}")
    lines.append(f"- Included studies with resolved DOI: {with_doi_rows}")
    lines.append(f"- Retraction dataset fetch performed: {'yes' if database_fetch_performed else 'no'}")
    lines.append("")
    lines.append("## Retraction Dataset")
    lines.append("")
    lines.append(f"- CSV rows scanned: {int(database_metadata.get('rows_scanned', 0))}")
    lines.append(f"- CSV rows with DOI values: {int(database_metadata.get('rows_with_doi', 0))}")
    lines.append(f"- Unique DOI values indexed: {int(database_metadata.get('unique_doi_count', 0))}")
    doi_columns = database_metadata.get("doi_columns", [])
    if isinstance(doi_columns, list) and doi_columns:
        lines.append(f"- DOI columns used: {', '.join(str(value) for value in doi_columns)}")
    else:
        lines.append("- DOI columns used: none")
    lines.append("")
    lines.append("## Result Counts")
    lines.append("")
    if status_counts:
        for key, value in sorted(status_counts.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"- `{key}`: {value}")
    else:
        lines.append("- No included studies were evaluated.")
    lines.append("")
    lines.append("## DOI Resolution")
    lines.append("")
    if doi_source_counts:
        for key, value in sorted(doi_source_counts.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"- `{key}`: {value}")
    else:
        lines.append("- No DOI-resolved included studies.")

    if api_error:
        lines.append("")
        lines.append("## API Error")
        lines.append("")
        lines.append(f"- {api_error}")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- This checker performs a single network request to fetch the Retraction Watch DOI dataset snapshot.")
    lines.append("- Results are deterministic DOI matches against the fetched CSV snapshot.")
    lines.append("- Final inclusion/exclusion decisions remain reviewer-led.")
    lines.append("")
    return "\n".join(lines)


def write_results_csv(path: Path, rows: list[dict[str, str]]) -> None:
    columns = [
        "study_id",
        "first_author",
        "year",
        "source_doi",
        "doi_source",
        "match_method",
        "master_record_id",
        "master_source_record_id",
        "retraction_status",
        "retraction_hit_count",
        "retraction_record_ids",
        "retraction_dates",
        "retraction_reasons",
        "retraction_titles",
        "retraction_urls",
        "notes",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=columns).to_csv(path, index=False)


def parser() -> argparse.ArgumentParser:
    cli_parser = argparse.ArgumentParser(
        description=(
            "Check included studies for DOI-level retraction signals "
            "using a single Retraction Watch dataset fetch."
        )
    )
    cli_parser.add_argument(
        "--extraction",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction template CSV.",
    )
    cli_parser.add_argument(
        "--master",
        default="../02_data/processed/master_records.csv",
        help="Path to deduplicated master records CSV.",
    )
    cli_parser.add_argument(
        "--database-url",
        default=DEFAULT_RETRACTION_DB_URL,
        help="URL for Retraction Watch dataset snapshot CSV.",
    )
    cli_parser.add_argument(
        "--database-snapshot",
        default="",
        help="Optional local CSV snapshot path (skip network fetch if provided).",
    )
    cli_parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="HTTP timeout in seconds for dataset fetch.",
    )
    cli_parser.add_argument(
        "--max-studies",
        type=int,
        default=0,
        help="Optional cap for included studies processed (0 = all).",
    )
    cli_parser.add_argument(
        "--include-duplicate-master",
        action="store_true",
        help="Include duplicate master records for DOI matching.",
    )
    cli_parser.add_argument(
        "--strict-api-errors",
        action="store_true",
        help="Exit non-zero when retraction dataset fetch/parsing fails.",
    )
    cli_parser.add_argument(
        "--fail-on-retracted",
        action="store_true",
        help="Exit non-zero when at least one included study is flagged as retracted.",
    )
    cli_parser.add_argument(
        "--results-output",
        default="outputs/retraction_check_results.csv",
        help="Path to record-level retraction-check CSV output.",
    )
    cli_parser.add_argument(
        "--summary-output",
        default="outputs/retraction_check_summary.md",
        help="Path to markdown summary output.",
    )
    return cli_parser


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)

    extraction_path = Path(args.extraction)
    master_path = Path(args.master)
    results_output_path = Path(args.results_output)
    summary_output_path = Path(args.summary_output)

    extraction_df = load_extraction(extraction_path)
    included_df = select_included_extraction_rows(extraction_df)
    if args.max_studies > 0 and not included_df.empty:
        included_df = included_df.head(args.max_studies).copy()

    master_df = load_master(master_path, include_duplicates=bool(args.include_duplicate_master))
    master_indexes = build_master_indexes(master_df)
    source_rows, doi_source_counts = resolve_source_studies(included_df, master_df, master_indexes)

    api_error = ""
    database_fetch_performed = False
    database_source = args.database_url
    database_metadata: dict[str, object] = {
        "rows_scanned": 0,
        "rows_with_doi": 0,
        "unique_doi_count": 0,
        "doi_columns": [],
    }
    retraction_index: dict[str, list[dict[str, str]]] = {}

    doi_to_check = {normalize_doi(row.get("source_doi", "")) for row in source_rows if normalize_doi(row.get("source_doi", ""))}

    if doi_to_check:
        try:
            if normalize(args.database_snapshot):
                snapshot_path = Path(args.database_snapshot)
                database_source = snapshot_path.as_posix()
                payload = snapshot_path.read_text(encoding="utf-8")
            else:
                database_fetch_performed = True
                payload = fetch_retraction_watch_csv(args.database_url, timeout=max(int(args.timeout), 1))

            retraction_index, database_metadata = parse_retraction_database(payload)
        except (OSError, RuntimeError) as error:
            api_error = str(error)
            if args.strict_api_errors:
                raise SystemExit(api_error) from error

    result_rows, status_counts = build_result_rows(source_rows, retraction_index, api_error=api_error)

    write_results_csv(results_output_path, result_rows)

    summary_text = render_summary(
        extraction_path=extraction_path,
        master_path=master_path,
        database_source=database_source,
        database_fetch_performed=database_fetch_performed,
        database_metadata=database_metadata,
        api_error=api_error,
        included_rows=int(included_df.shape[0]),
        with_doi_rows=sum(1 for row in source_rows if normalize_doi(row.get("source_doi", ""))),
        status_counts=status_counts,
        doi_source_counts=doi_source_counts,
        result_output_path=results_output_path,
        summary_output_path=summary_output_path,
    )
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(summary_text, encoding="utf-8")

    retracted_n = int(status_counts.get("retracted", 0))

    print(f"Wrote: {results_output_path}")
    print(f"Wrote: {summary_output_path}")
    print(
        "Retraction check stats: "
        f"included={int(included_df.shape[0])}, "
        f"with_doi={sum(1 for row in source_rows if normalize_doi(row.get('source_doi', '')))}, "
        f"retracted={retracted_n}, "
        f"api_error={'yes' if api_error else 'no'}"
    )

    if args.fail_on_retracted and retracted_n > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
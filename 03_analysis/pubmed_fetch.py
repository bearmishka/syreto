import argparse
import hashlib
import json
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd


EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
DOI_PATTERN = re.compile(r"10\.\d{4,9}/[^\s]+", re.IGNORECASE)
YEAR_PATTERN = re.compile(r"(19|20)\d{2}")
MEDLINE_TAG_PATTERN = re.compile(r"^([A-Z0-9]{2,4})\s*-\s?(.*)$")

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


def clean_text(value: object) -> str:
    return str(value if value is not None else "").strip()


def normalize_doi(value: object) -> str:
    text = clean_text(value)
    if not text:
        return ""
    text = text.replace("https://doi.org/", "").replace("http://doi.org/", "")
    text = text.replace("doi:", "").strip()
    match = DOI_PATTERN.search(text)
    if not match:
        return ""
    return match.group(0).rstrip(".,);")


def extract_year(value: object) -> str:
    text = clean_text(value)
    if not text:
        return ""
    match = YEAR_PATTERN.search(text)
    return match.group(0) if match else ""


def safe_query_token(query_version: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9._-]+", "_", clean_text(query_version))
    return token or "query"


def compact_query_text(query_text: str) -> str:
    return re.sub(r"\s+", " ", clean_text(query_text)).strip()


def query_preview(query_text: str, limit: int = 160) -> str:
    compact = compact_query_text(query_text)
    if len(compact) <= limit:
        return compact
    return compact[: max(limit - 1, 1)].rstrip() + "…"


def derive_query_version(query_text: str) -> str:
    compact = compact_query_text(query_text).lower()
    digest = hashlib.sha1(compact.encode("utf-8")).hexdigest()[:10]
    return f"adhoc_{digest}"


class EutilsClient:
    def __init__(self, *, tool: str, email: str, api_key: str, timeout: int) -> None:
        self.tool = tool
        self.email = email
        self.api_key = api_key
        self.timeout = timeout
        self.min_interval_seconds = 0.11 if api_key else 0.34
        self.last_call_time = 0.0

    def _throttle(self) -> None:
        now = time.time()
        elapsed = now - self.last_call_time
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed)

    def _request(self, endpoint: str, params: dict[str, object]) -> str:
        payload = {key: value for key, value in params.items() if value not in {None, ""}}
        payload["tool"] = self.tool
        if self.email:
            payload["email"] = self.email
        if self.api_key:
            payload["api_key"] = self.api_key

        url = f"{EUTILS_BASE}/{endpoint}?{urlencode(payload)}"
        request = Request(url, headers={"User-Agent": "prism-pubmed-fetch/1.0"})

        self._throttle()
        try:
            with urlopen(request, timeout=self.timeout) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                body = response.read().decode(charset, errors="replace")
        except HTTPError as error:
            raise RuntimeError(f"E-utilities HTTP error {error.code} for {endpoint}: {error.reason}") from error
        except URLError as error:
            raise RuntimeError(f"E-utilities network error for {endpoint}: {error.reason}") from error

        self.last_call_time = time.time()
        return body

    def search(self, query: str) -> tuple[int, str, str]:
        text = self._request(
            "esearch.fcgi",
            {
                "db": "pubmed",
                "term": query,
                "usehistory": "y",
                "retmode": "json",
                "retmax": 0,
            },
        )
        payload = json.loads(text)
        result = payload.get("esearchresult", {})

        count_text = clean_text(result.get("count"))
        webenv = clean_text(result.get("webenv"))
        query_key = clean_text(result.get("querykey"))

        if not count_text.isdigit() or not webenv or not query_key:
            raise RuntimeError("Unexpected ESearch response: missing count/query history values.")

        return int(count_text), webenv, query_key

    def fetch_medline_batch(self, *, webenv: str, query_key: str, retstart: int, retmax: int) -> str:
        return self._request(
            "efetch.fcgi",
            {
                "db": "pubmed",
                "query_key": query_key,
                "WebEnv": webenv,
                "rettype": "medline",
                "retmode": "text",
                "retstart": retstart,
                "retmax": retmax,
            },
        )


def parse_medline_records(text: str) -> list[dict[str, list[str]]]:
    records: list[dict[str, list[str]]] = []
    current: dict[str, list[str]] = {}
    last_tag = ""

    for raw_line in text.splitlines():
        if not raw_line.strip():
            if current:
                records.append(current)
                current = {}
                last_tag = ""
            continue

        match = MEDLINE_TAG_PATTERN.match(raw_line)
        if match:
            tag, value = match.group(1), clean_text(match.group(2))
            if tag == "PMID" and "PMID" in current:
                records.append(current)
                current = {}
            current.setdefault(tag, []).append(value)
            last_tag = tag
            continue

        if raw_line.startswith("      ") and last_tag and last_tag in current and current[last_tag]:
            continuation = clean_text(raw_line)
            if continuation:
                current[last_tag][-1] = f"{current[last_tag][-1]} {continuation}".strip()

    if current:
        records.append(current)

    return records


def first_non_empty(record: dict[str, list[str]], keys: list[str]) -> str:
    for key in keys:
        for value in record.get(key, []):
            text = clean_text(value)
            if text:
                return text
    return ""


def pick_doi(record: dict[str, list[str]]) -> str:
    for value in record.get("AID", []) + record.get("LID", []) + record.get("SO", []):
        text = clean_text(value)
        if not text:
            continue
        if "[doi]" in text.lower():
            text = text.split("[", maxsplit=1)[0].strip()
        doi = normalize_doi(text)
        if doi:
            return doi
    return ""


def ris_lines_from_record(record: dict[str, list[str]]) -> list[str]:
    pmid = first_non_empty(record, ["PMID"])
    title = first_non_empty(record, ["TI"])
    journal = first_non_empty(record, ["JT", "TA"])
    year = extract_year(first_non_empty(record, ["DP", "PHST"]))
    doi = pick_doi(record)

    authors = [clean_text(value) for value in record.get("FAU", []) if clean_text(value)]
    if not authors:
        authors = [clean_text(value) for value in record.get("AU", []) if clean_text(value)]

    lines = ["TY  - JOUR"]
    if title:
        lines.append(f"TI  - {title}")
    for author in authors:
        lines.append(f"AU  - {author}")
    if year:
        lines.append(f"PY  - {year}")
    if journal:
        lines.append(f"JO  - {journal}")
    if doi:
        lines.append(f"DO  - {doi}")
    if pmid:
        lines.append(f"PM  - {pmid}")
        lines.append(f"ID  - {pmid}")
        lines.append(f"UR  - https://pubmed.ncbi.nlm.nih.gov/{pmid}/")

    abstract = first_non_empty(record, ["AB"])
    if abstract:
        lines.append(f"AB  - {abstract}")

    for keyword in record.get("OT", []):
        text = clean_text(keyword)
        if text:
            lines.append(f"KW  - {text}")

    lines.append("ER  -")
    return lines


def build_ris_text(records: list[dict[str, list[str]]]) -> str:
    chunks: list[str] = []
    for record in records:
        chunks.extend(ris_lines_from_record(record))
        chunks.append("")
    return "\n".join(chunks).strip() + "\n"


def choose_output_path(raw_dir: Path, requested_name: str, overwrite: bool) -> Path:
    candidate = raw_dir / requested_name
    if overwrite or not candidate.exists():
        return candidate

    stem = candidate.stem
    suffix = candidate.suffix
    index = 2
    while True:
        option = raw_dir / f"{stem}_{index}{suffix}"
        if not option.exists():
            return option
        index += 1


def load_query_text(query_version: str, query_file: str) -> str:
    if query_file:
        path = Path(query_file)
    else:
        if not clean_text(query_version):
            raise ValueError("query_version is required when --query-file is not provided")
        path = Path(f"../01_protocol/pubmed_query_{query_version}.txt")

    if not path.exists():
        raise FileNotFoundError(f"PubMed query file not found: {path}")

    text = compact_query_text(path.read_text(encoding="utf-8"))
    if not text:
        raise ValueError(f"PubMed query file is empty: {path}")

    return text


def resolve_query_inputs(*, query: str, query_file: str, query_version: str) -> tuple[str, str, str]:
    inline_query = compact_query_text(query)
    file_path = clean_text(query_file)
    resolved_version = clean_text(query_version)

    if inline_query and file_path:
        raise ValueError("Use either --query or --query-file, not both.")

    if inline_query:
        resolved_query = inline_query
        query_source = "cli-query"
        if not resolved_version:
            resolved_version = derive_query_version(resolved_query)
        return resolved_query, resolved_version, query_source

    if not resolved_version and not file_path:
        raise ValueError("Provide one of: query_version, --query-file, or --query.")

    resolved_query = load_query_text(resolved_version, file_path)
    query_source = "query-file"
    if not resolved_version:
        resolved_version = derive_query_version(resolved_query)
    return resolved_query, resolved_version, query_source


def ensure_search_log_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in SEARCH_LOG_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    ordered = SEARCH_LOG_COLUMNS + [column for column in df.columns if column not in SEARCH_LOG_COLUMNS]
    return df[ordered]


def update_search_log(
    *,
    search_log_path: Path,
    query_version: str,
    run_date: str,
    results_total: int,
    results_exported: int,
    export_filename: str,
    start_year_default: str,
    filters_default: str,
    notes_suffix: str,
) -> None:
    if search_log_path.exists():
        try:
            log_df = pd.read_csv(search_log_path, dtype=str)
        except pd.errors.EmptyDataError:
            log_df = pd.DataFrame(columns=SEARCH_LOG_COLUMNS)
    else:
        log_df = pd.DataFrame(columns=SEARCH_LOG_COLUMNS)

    log_df = ensure_search_log_columns(log_df)
    database_series = log_df["database"].fillna("").astype(str).str.strip().str.lower()
    query_series = log_df["query_version"].fillna("").astype(str).str.strip()
    mask = database_series.eq("pubmed") & query_series.eq(query_version)

    if mask.any():
        row_index = int(log_df.index[mask][-1])
    else:
        row_index = int(log_df.shape[0])
        log_df.loc[row_index, "database"] = "PubMed"
        log_df.loc[row_index, "query_version"] = query_version

    if not clean_text(log_df.loc[row_index, "start_year"]):
        log_df.loc[row_index, "start_year"] = start_year_default
    if not clean_text(log_df.loc[row_index, "filters_applied"]):
        log_df.loc[row_index, "filters_applied"] = filters_default

    log_df.loc[row_index, "database"] = "PubMed"
    log_df.loc[row_index, "query_version"] = query_version
    log_df.loc[row_index, "date_searched"] = run_date
    log_df.loc[row_index, "end_date"] = run_date
    log_df.loc[row_index, "results_total"] = str(results_total)
    log_df.loc[row_index, "results_exported"] = str(results_exported)
    log_df.loc[row_index, "export_filename"] = export_filename

    existing_notes = clean_text(log_df.loc[row_index, "notes"])
    merged_notes = notes_suffix if not existing_notes else f"{existing_notes} | {notes_suffix}"
    log_df.loc[row_index, "notes"] = merged_notes

    search_log_path.parent.mkdir(parents=True, exist_ok=True)
    log_df.to_csv(search_log_path, index=False)


def fetch_medline_text(
    *,
    client: EutilsClient,
    webenv: str,
    query_key: str,
    records_to_export: int,
    batch_size: int,
) -> str:
    chunks: list[str] = []
    for start in range(0, records_to_export, batch_size):
        size = min(batch_size, records_to_export - start)
        text = client.fetch_medline_batch(webenv=webenv, query_key=query_key, retstart=start, retmax=size)
        if text:
            chunks.append(text.strip())
    if not chunks:
        return ""
    return "\n\n".join(chunks).strip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch PubMed records via NCBI E-utilities, save RIS to 02_data/raw, and update search_log.csv."
    )
    parser.add_argument("query_version", nargs="?", default="", help="Optional query version token, e.g., v0.2")
    parser.add_argument(
        "--query-version",
        dest="query_version_override",
        default="",
        help="Optional query version token (overrides positional query_version)",
    )
    parser.add_argument("--query", default="", help="Direct PubMed query text (inline, no query file needed)")
    parser.add_argument("--query-file", default="", help="Optional explicit query file path")
    parser.add_argument("--raw-dir", default="../02_data/raw", help="Directory for RIS export")
    parser.add_argument("--search-log", default="../02_data/processed/search_log.csv", help="Path to search_log.csv")
    parser.add_argument("--run-date", default=datetime.now().strftime("%Y-%m-%d"), help="Run date (YYYY-MM-DD)")
    parser.add_argument("--email", default="", help="Contact email for NCBI requests")
    parser.add_argument("--api-key", default="", help="NCBI API key (optional)")
    parser.add_argument("--tool", default="prism_pubmed_fetch", help="Tool name sent to E-utilities")
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout in seconds")
    parser.add_argument("--batch-size", type=int, default=200, help="EFetch batch size")
    parser.add_argument("--max-records", type=int, default=0, help="Optional max number of records to export (0 = all)")
    parser.add_argument("--export-filename", default="", help="Optional RIS filename override")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output filename if it already exists")
    parser.add_argument("--skip-log-update", action="store_true", help="Skip updating search_log.csv")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and convert records without writing files")
    parser.add_argument("--start-year-default", default="1980", help="Default start_year for a new PubMed row")
    parser.add_argument(
        "--filters-default",
        default="NCBI E-utilities automated PubMed fetch",
        help="Default filters_applied for a new PubMed row",
    )
    args = parser.parse_args()

    if args.batch_size <= 0:
        raise ValueError("--batch-size must be >= 1")
    if args.max_records < 0:
        raise ValueError("--max-records must be >= 0")

    requested_version = clean_text(args.query_version_override) or clean_text(args.query_version)
    query_text, query_version, query_source = resolve_query_inputs(
        query=args.query,
        query_file=args.query_file,
        query_version=requested_version,
    )

    client = EutilsClient(tool=args.tool, email=args.email, api_key=args.api_key, timeout=args.timeout)
    results_total, webenv, query_key = client.search(query_text)

    records_to_export = results_total
    if args.max_records > 0:
        records_to_export = min(results_total, args.max_records)

    medline_text = fetch_medline_text(
        client=client,
        webenv=webenv,
        query_key=query_key,
        records_to_export=records_to_export,
        batch_size=args.batch_size,
    )
    medline_records = parse_medline_records(medline_text)
    ris_text = build_ris_text(medline_records)

    run_date = clean_text(args.run_date)
    default_name = f"pubmed_{safe_query_token(query_version)}_{run_date}.ris"
    requested_name = clean_text(args.export_filename) or default_name

    raw_dir = Path(args.raw_dir)
    output_path = choose_output_path(raw_dir, requested_name, overwrite=args.overwrite)

    if not args.dry_run:
        raw_dir.mkdir(parents=True, exist_ok=True)
        output_path.write_text(ris_text, encoding="utf-8")

        if not args.skip_log_update:
            notes = f"Auto-fetched via pubmed_fetch.py on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            notes += f" | source={query_source}"
            notes += f" | query=\"{query_preview(query_text, limit=120)}\""
            if args.max_records > 0 and args.max_records < results_total:
                notes += f" (max_records={args.max_records})"

            update_search_log(
                search_log_path=Path(args.search_log),
                query_version=query_version,
                run_date=run_date,
                results_total=results_total,
                results_exported=len(medline_records),
                export_filename=output_path.name,
                start_year_default=args.start_year_default,
                filters_default=args.filters_default,
                notes_suffix=notes,
            )

    print(f"Query version: {query_version}")
    print(f"Query source: {query_source}")
    print(f"Query preview: {query_preview(query_text)}")
    print(f"Total results (ESearch): {results_total}")
    print(f"Exported records (RIS): {len(medline_records)}")
    print(f"Output file: {output_path}")
    if args.max_records > 0 and args.max_records < results_total:
        print(f"Export cap applied: max_records={args.max_records}")
    if args.dry_run:
        print("Dry run: no files were written.")
    else:
        print(f"Wrote: {output_path}")
        if args.skip_log_update:
            print("Skipped: search_log update")
        else:
            print(f"Updated: {args.search_log}")


if __name__ == "__main__":
    main()
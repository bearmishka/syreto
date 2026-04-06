import argparse
import json
import re
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
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


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def is_missing(value: object) -> bool:
    return normalize(value).lower() in MISSING_CODES


def parse_year(value: object) -> str:
    text = normalize(value)
    if not text:
        return ""
    match = YEAR_PATTERN.search(text)
    return match.group(0) if match else ""


def normalize_doi(value: object) -> str:
    text = normalize(value).lower()
    if not text:
        return ""
    text = text.replace("https://doi.org/", "").replace("http://doi.org/", "")
    text = text.replace("doi:", "").strip()
    return text.rstrip(".,);")


def normalize_pmid(value: object) -> str:
    text = normalize(value)
    if not text:
        return ""
    return re.sub(r"\D+", "", text)


def normalize_title(value: object) -> str:
    text = normalize(value).lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


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


def load_extraction(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Extraction file not found: {path}")

    try:
        extraction_df = pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        extraction_df = pd.DataFrame()

    required = [
        "study_id",
        "first_author",
        "year",
        "title",
        "doi",
        "pmid",
        "record_id",
        "source_record_id",
    ]
    for column in required:
        if column not in extraction_df.columns:
            extraction_df[column] = ""

    return extraction_df


def load_master(path: Path, include_duplicates: bool) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    try:
        master_df = pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()

    if master_df.empty:
        return master_df

    required = [
        "record_id",
        "source_record_id",
        "title",
        "authors",
        "year",
        "doi",
        "pmid",
        "is_duplicate",
    ]
    for column in required:
        if column not in master_df.columns:
            master_df[column] = ""

    if not include_duplicates:
        is_dup = master_df["is_duplicate"].fillna("").astype(str).str.strip().str.lower().isin(YES_VALUES)
        master_df = master_df.loc[~is_dup].copy()

    return master_df


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
        "pmid": {},
        "author_year": {},
        "title_author_year": {},
    }

    if master_df.empty:
        return indexes

    for row_idx, row in master_df.iterrows():
        record_id = normalize(row.get("record_id", ""))
        source_record_id = normalize(row.get("source_record_id", ""))
        doi = normalize_doi(row.get("doi", ""))
        pmid = normalize_pmid(row.get("pmid", ""))
        year = parse_year(row.get("year", ""))
        author_norm = normalize_first_author(row.get("authors", ""))
        title_norm = normalize_title(row.get("title", ""))

        if record_id:
            indexes["record_id"].setdefault(record_id, []).append(row_idx)
        if source_record_id:
            indexes["source_record_id"].setdefault(source_record_id, []).append(row_idx)
        if doi:
            indexes["doi"].setdefault(doi, []).append(row_idx)
        if pmid:
            indexes["pmid"].setdefault(pmid, []).append(row_idx)
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
    ext_pmid = normalize_pmid(extraction_row.get("pmid", ""))
    ext_author = normalize_first_author(extraction_row.get("first_author", ""))
    ext_year = parse_year(extraction_row.get("year", ""))
    ext_title = normalize_title(extraction_row.get("title", ""))

    if ext_record_id and ext_record_id in indexes["record_id"]:
        return master_df.loc[indexes["record_id"][ext_record_id][0]], "record_id"

    if ext_source_record_id and ext_source_record_id in indexes["source_record_id"]:
        return master_df.loc[indexes["source_record_id"][ext_source_record_id][0]], "source_record_id"

    if ext_doi and ext_doi in indexes["doi"]:
        return master_df.loc[indexes["doi"][ext_doi][0]], "doi"

    if ext_pmid and ext_pmid in indexes["pmid"]:
        return master_df.loc[indexes["pmid"][ext_pmid][0]], "pmid"

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


def build_manual_query(*, first_author: str, year: str, doi: str) -> str:
    if doi:
        return f'"{doi}"'
    parts = [part for part in [first_author, year, "target population", "target concept", "target outcome"] if part]
    return " ".join(parts)


def resolve_source_studies(
    included_df: pd.DataFrame,
    master_df: pd.DataFrame,
    indexes: dict[str, dict[str, list[int]]],
) -> tuple[list[dict[str, str]], list[dict[str, str]], Counter[str]]:
    source_rows: list[dict[str, str]] = []
    grey_logs: list[dict[str, str]] = []
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

        if source_doi:
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
            doi_source_counts[doi_source] += 1
            continue

        grey_logs.append(
            {
                "log_type": "missing_source_doi",
                "study_id": study_id,
                "first_author": first_author,
                "year": year,
                "source_doi": "",
                "related_doi": "",
                "endpoint": "",
                "note": f"No DOI resolved (match_method={match_method}).",
                "manual_query": build_manual_query(first_author=first_author, year=year, doi=""),
                "manual_link": "",
            }
        )

    return source_rows, grey_logs, doi_source_counts


class OpenCitationsClient:
    def __init__(
        self,
        *,
        api_base: str,
        timeout: int,
        max_retries: int,
        min_interval_seconds: float,
    ):
        self.api_base = api_base.rstrip("/")
        self.timeout = timeout
        self.max_retries = max(0, max_retries)
        self.min_interval_seconds = max(0.0, min_interval_seconds)
        self.last_call_time = 0.0

    def _throttle(self) -> None:
        elapsed = time.time() - self.last_call_time
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed)

    def fetch(self, endpoint: str, doi: str) -> list[dict[str, object]]:
        encoded_doi = quote(doi, safe="")
        url = f"{self.api_base}/{endpoint}/{encoded_doi}"
        request = Request(url, headers={"User-Agent": "prism-citation-tracker/1.0"})
        attempts = self.max_retries + 1
        last_error = ""

        for attempt in range(1, attempts + 1):
            self._throttle()
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    charset = response.headers.get_content_charset() or "utf-8"
                    body = response.read().decode(charset, errors="replace")
                self.last_call_time = time.time()

                payload = json.loads(body) if body.strip() else []
                if isinstance(payload, list):
                    return payload
                raise RuntimeError(f"Unexpected payload type from OpenCitations: {type(payload).__name__}")

            except HTTPError as error:
                self.last_call_time = time.time()
                if error.code == 404:
                    return []
                last_error = f"HTTP {error.code} {error.reason}"
            except URLError as error:
                self.last_call_time = time.time()
                last_error = f"Network error: {error.reason}"
            except json.JSONDecodeError as error:
                self.last_call_time = time.time()
                last_error = f"Invalid JSON response: {error}"

            if attempt < attempts:
                time.sleep(min(2.0, 0.3 * attempt))

        raise RuntimeError(f"OpenCitations request failed for {endpoint}/{doi}: {last_error}")


def crawl_citations(
    source_rows: list[dict[str, str]],
    client: OpenCitationsClient,
    *,
    dry_run: bool,
    strict_api_errors: bool,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], Counter[str], set[str]]:
    forward_rows: list[dict[str, str]] = []
    backward_rows: list[dict[str, str]] = []
    grey_logs: list[dict[str, str]] = []
    counters: Counter[str] = Counter()
    related_dois: set[str] = set()

    cache: dict[str, dict[str, object]] = {}

    for source in source_rows:
        source_doi = source["source_doi"]
        if source_doi not in cache:
            cache[source_doi] = {
                "citations": [],
                "references": [],
                "error_citations": "",
                "error_references": "",
            }
            if dry_run:
                counters["dry_run_skipped_calls"] += 2
            else:
                for endpoint in ["citations", "references"]:
                    try:
                        cache[source_doi][endpoint] = client.fetch(endpoint, source_doi)
                    except RuntimeError as error:
                        message = str(error)
                        cache[source_doi][f"error_{endpoint}"] = message
                        counters["api_errors"] += 1
                        if strict_api_errors:
                            raise

        cached = cache[source_doi]

        for endpoint in ["citations", "references"]:
            error_message = normalize(cached.get(f"error_{endpoint}", ""))
            if error_message:
                grey_logs.append(
                    {
                        "log_type": "api_error",
                        "study_id": source["study_id"],
                        "first_author": source["first_author"],
                        "year": source["year"],
                        "source_doi": source_doi,
                        "related_doi": "",
                        "endpoint": endpoint,
                        "note": error_message,
                        "manual_query": build_manual_query(
                            first_author=source["first_author"],
                            year=source["year"],
                            doi=source_doi,
                        ),
                        "manual_link": f"https://doi.org/{source_doi}",
                    }
                )
                continue

            records = cached.get(endpoint, [])
            if not isinstance(records, list):
                continue

            counters[f"api_{endpoint}_records"] += len(records)

            for item in records:
                if not isinstance(item, dict):
                    continue

                citing = normalize_doi(item.get("citing", ""))
                cited = normalize_doi(item.get("cited", ""))

                if endpoint == "citations":
                    related_doi = citing
                    if not related_doi:
                        counters["forward_missing_related_doi"] += 1
                        grey_logs.append(
                            {
                                "log_type": "missing_forward_doi",
                                "study_id": source["study_id"],
                                "first_author": source["first_author"],
                                "year": source["year"],
                                "source_doi": source_doi,
                                "related_doi": "",
                                "endpoint": endpoint,
                                "note": "OpenCitations citation item has no `citing` DOI.",
                                "manual_query": build_manual_query(
                                    first_author=source["first_author"],
                                    year=source["year"],
                                    doi=source_doi,
                                ),
                                "manual_link": f"https://doi.org/{source_doi}",
                            }
                        )
                        continue

                    forward_rows.append(
                        {
                            "source_study_id": source["study_id"],
                            "source_first_author": source["first_author"],
                            "source_year": source["year"],
                            "source_doi": source_doi,
                            "source_doi_source": source["doi_source"],
                            "source_match_method": source["match_method"],
                            "citing_doi": related_doi,
                            "cited_doi": cited,
                            "oci": normalize(item.get("oci", "")),
                            "creation": normalize(item.get("creation", "")),
                            "timespan": normalize(item.get("timespan", "")),
                            "journal_sc": normalize(item.get("journal_sc", "")),
                            "author_sc": normalize(item.get("author_sc", "")),
                        }
                    )
                    related_dois.add(related_doi)
                    counters["forward_rows"] += 1
                else:
                    related_doi = cited
                    if not related_doi:
                        counters["backward_missing_related_doi"] += 1
                        grey_logs.append(
                            {
                                "log_type": "missing_backward_doi",
                                "study_id": source["study_id"],
                                "first_author": source["first_author"],
                                "year": source["year"],
                                "source_doi": source_doi,
                                "related_doi": "",
                                "endpoint": endpoint,
                                "note": "OpenCitations reference item has no `cited` DOI.",
                                "manual_query": build_manual_query(
                                    first_author=source["first_author"],
                                    year=source["year"],
                                    doi=source_doi,
                                ),
                                "manual_link": f"https://doi.org/{source_doi}",
                            }
                        )
                        continue

                    backward_rows.append(
                        {
                            "source_study_id": source["study_id"],
                            "source_first_author": source["first_author"],
                            "source_year": source["year"],
                            "source_doi": source_doi,
                            "source_doi_source": source["doi_source"],
                            "source_match_method": source["match_method"],
                            "cited_doi": related_doi,
                            "citing_doi": citing,
                            "oci": normalize(item.get("oci", "")),
                            "creation": normalize(item.get("creation", "")),
                            "timespan": normalize(item.get("timespan", "")),
                            "journal_sc": normalize(item.get("journal_sc", "")),
                            "author_sc": normalize(item.get("author_sc", "")),
                        }
                    )
                    related_dois.add(related_doi)
                    counters["backward_rows"] += 1

    return forward_rows, backward_rows, grey_logs, counters, related_dois


def deduplicate_rows(rows: list[dict[str, str]], key_fields: list[str]) -> tuple[list[dict[str, str]], int]:
    seen: set[tuple[str, ...]] = set()
    deduped: list[dict[str, str]] = []
    dropped = 0
    for row in rows:
        key = tuple(normalize(row.get(field, "")) for field in key_fields)
        if key in seen:
            dropped += 1
            continue
        seen.add(key)
        deduped.append(row)
    return deduped, dropped


def add_candidate_grey_rows(related_dois: set[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for related_doi in sorted(related_dois):
        rows.append(
            {
                "log_type": "citation_candidate",
                "study_id": "",
                "first_author": "",
                "year": "",
                "source_doi": "",
                "related_doi": related_doi,
                "endpoint": "",
                "note": "Citation-linked DOI for optional grey-search follow-up.",
                "manual_query": f'"{related_doi}"',
                "manual_link": f"https://doi.org/{related_doi}",
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict[str, str]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=columns)
    frame.to_csv(path, index=False)


def render_summary(
    *,
    extraction_path: Path,
    master_path: Path,
    forward_output_path: Path,
    backward_output_path: Path,
    grey_output_path: Path,
    total_extraction_rows: int,
    included_rows: int,
    source_rows: int,
    unresolved_rows: int,
    doi_source_counts: Counter[str],
    counters: Counter[str],
    forward_rows: int,
    backward_rows: int,
    grey_rows: int,
    dropped_forward_duplicates: int,
    dropped_backward_duplicates: int,
    dropped_grey_duplicates: int,
    log_type_counts: Counter[str],
    dry_run: bool,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = []
    lines.append("# Citation Tracker Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Extraction input: `{extraction_path.as_posix()}`")
    lines.append(f"- Master records input: `{master_path.as_posix()}`")
    lines.append(f"- Forward citations output: `{forward_output_path.as_posix()}`")
    lines.append(f"- Backward references output: `{backward_output_path.as_posix()}`")
    lines.append(f"- Grey-search log output: `{grey_output_path.as_posix()}`")
    lines.append("")
    lines.append("## Row Counts")
    lines.append("")
    lines.append(f"- Extraction rows (raw): {total_extraction_rows}")
    lines.append(f"- Included studies (non-empty `study_id`): {included_rows}")
    lines.append(f"- Included studies with resolved DOI: {source_rows}")
    lines.append(f"- Included studies without DOI (manual follow-up): {unresolved_rows}")
    lines.append(f"- Forward citation rows: {forward_rows}")
    lines.append(f"- Backward reference rows: {backward_rows}")
    lines.append(f"- Grey-search log rows: {grey_rows}")
    lines.append("")
    lines.append("## DOI Resolution")
    lines.append("")
    if doi_source_counts:
        for key, value in sorted(doi_source_counts.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"- `{key}`: {value}")
    else:
        lines.append("- No DOI-resolved included studies.")
    lines.append("")
    lines.append("## OpenCitations API")
    lines.append("")
    lines.append(f"- Dry run mode: {'yes' if dry_run else 'no'}")
    lines.append(f"- API records fetched (`citations`): {counters.get('api_citations_records', 0)}")
    lines.append(f"- API records fetched (`references`): {counters.get('api_references_records', 0)}")
    lines.append(f"- API errors: {counters.get('api_errors', 0)}")
    lines.append(f"- Skipped API calls in dry run: {counters.get('dry_run_skipped_calls', 0)}")
    lines.append("")
    lines.append("## Deduplication")
    lines.append("")
    lines.append(f"- Dropped duplicate forward rows: {dropped_forward_duplicates}")
    lines.append(f"- Dropped duplicate backward rows: {dropped_backward_duplicates}")
    lines.append(f"- Dropped duplicate grey-log rows: {dropped_grey_duplicates}")
    lines.append("")
    lines.append("## Grey-Search Log Types")
    lines.append("")
    if log_type_counts:
        for key, value in sorted(log_type_counts.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"- `{key}`: {value}")
    else:
        lines.append("- No grey-search log entries.")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Forward citation list is built from OpenCitations `citations/{doi}` (citing records).")
    lines.append("- Backward reference list is built from OpenCitations `references/{doi}` (cited records).")
    lines.append("- Grey-search log combines unresolved DOI studies, API issues, and citation-derived DOI candidates.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run backward/forward citation chasing for included studies via OpenCitations API."
    )
    parser.add_argument(
        "--extraction",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction template CSV",
    )
    parser.add_argument(
        "--master",
        default="../02_data/processed/master_records.csv",
        help="Path to deduplicated master records CSV",
    )
    parser.add_argument(
        "--forward-output",
        default="outputs/citation_forward.csv",
        help="Path to forward citations CSV output",
    )
    parser.add_argument(
        "--backward-output",
        default="outputs/citation_backward.csv",
        help="Path to backward references CSV output",
    )
    parser.add_argument(
        "--grey-log-output",
        default="outputs/citation_grey_search_log.csv",
        help="Path to grey-search tracking CSV output",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/citation_tracker_summary.md",
        help="Path to markdown summary output",
    )
    parser.add_argument(
        "--api-base",
        default="https://opencitations.net/index/coci/api/v1",
        help="OpenCitations API base URL",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds for OpenCitations calls",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="Retries per endpoint request after the first failed attempt",
    )
    parser.add_argument(
        "--min-interval",
        type=float,
        default=0.25,
        help="Minimum delay between API calls (seconds)",
    )
    parser.add_argument(
        "--include-duplicate-master",
        action="store_true",
        help="Include duplicate master records as DOI candidates",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve included studies and DOI matching without calling OpenCitations",
    )
    parser.add_argument(
        "--strict-api-errors",
        action="store_true",
        help="Raise on first API error instead of logging and continuing",
    )
    parser.add_argument(
        "--max-studies",
        type=int,
        default=0,
        help="Optional cap for included studies processed (0 = all)",
    )

    args = parser.parse_args()

    extraction_path = Path(args.extraction)
    master_path = Path(args.master)
    forward_output_path = Path(args.forward_output)
    backward_output_path = Path(args.backward_output)
    grey_log_output_path = Path(args.grey_log_output)
    summary_output_path = Path(args.summary_output)

    extraction_df = load_extraction(extraction_path)
    total_extraction_rows = int(extraction_df.shape[0])

    included_df = select_included_extraction_rows(extraction_df)
    if args.max_studies > 0 and not included_df.empty:
        included_df = included_df.head(args.max_studies).copy()

    master_df = load_master(master_path, include_duplicates=args.include_duplicate_master)
    master_indexes = build_master_indexes(master_df)

    source_rows, unresolved_logs, doi_source_counts = resolve_source_studies(included_df, master_df, master_indexes)

    client = OpenCitationsClient(
        api_base=args.api_base,
        timeout=args.timeout,
        max_retries=args.max_retries,
        min_interval_seconds=args.min_interval,
    )

    forward_rows, backward_rows, crawl_logs, counters, related_dois = crawl_citations(
        source_rows,
        client,
        dry_run=args.dry_run,
        strict_api_errors=args.strict_api_errors,
    )

    grey_logs = unresolved_logs + crawl_logs + add_candidate_grey_rows(related_dois)

    forward_rows, dropped_forward_duplicates = deduplicate_rows(
        forward_rows,
        ["source_study_id", "source_doi", "citing_doi", "cited_doi"],
    )
    backward_rows, dropped_backward_duplicates = deduplicate_rows(
        backward_rows,
        ["source_study_id", "source_doi", "cited_doi", "citing_doi"],
    )
    grey_logs, dropped_grey_duplicates = deduplicate_rows(
        grey_logs,
        ["log_type", "study_id", "source_doi", "related_doi", "endpoint", "note"],
    )

    forward_columns = [
        "source_study_id",
        "source_first_author",
        "source_year",
        "source_doi",
        "source_doi_source",
        "source_match_method",
        "citing_doi",
        "cited_doi",
        "oci",
        "creation",
        "timespan",
        "journal_sc",
        "author_sc",
    ]
    backward_columns = [
        "source_study_id",
        "source_first_author",
        "source_year",
        "source_doi",
        "source_doi_source",
        "source_match_method",
        "cited_doi",
        "citing_doi",
        "oci",
        "creation",
        "timespan",
        "journal_sc",
        "author_sc",
    ]
    grey_columns = [
        "log_type",
        "study_id",
        "first_author",
        "year",
        "source_doi",
        "related_doi",
        "endpoint",
        "note",
        "manual_query",
        "manual_link",
    ]

    write_csv(forward_output_path, forward_rows, forward_columns)
    write_csv(backward_output_path, backward_rows, backward_columns)
    write_csv(grey_log_output_path, grey_logs, grey_columns)

    log_type_counts: Counter[str] = Counter()
    for row in grey_logs:
        log_type_counts[normalize(row.get("log_type", "unknown")) or "unknown"] += 1

    summary_text = render_summary(
        extraction_path=extraction_path,
        master_path=master_path,
        forward_output_path=forward_output_path,
        backward_output_path=backward_output_path,
        grey_output_path=grey_log_output_path,
        total_extraction_rows=total_extraction_rows,
        included_rows=int(included_df.shape[0]),
        source_rows=len(source_rows),
        unresolved_rows=sum(1 for row in grey_logs if row.get("log_type") == "missing_source_doi"),
        doi_source_counts=doi_source_counts,
        counters=counters,
        forward_rows=len(forward_rows),
        backward_rows=len(backward_rows),
        grey_rows=len(grey_logs),
        dropped_forward_duplicates=dropped_forward_duplicates,
        dropped_backward_duplicates=dropped_backward_duplicates,
        dropped_grey_duplicates=dropped_grey_duplicates,
        log_type_counts=log_type_counts,
        dry_run=args.dry_run,
    )
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(summary_text, encoding="utf-8")

    print(f"Wrote: {forward_output_path}")
    print(f"Wrote: {backward_output_path}")
    print(f"Wrote: {grey_log_output_path}")
    print(f"Wrote: {summary_output_path}")
    print(
        "Citation stats: "
        f"included={int(included_df.shape[0])}, "
        f"with_doi={len(source_rows)}, "
        f"forward={len(forward_rows)}, "
        f"backward={len(backward_rows)}, "
        f"grey_log={len(grey_logs)}, "
        f"api_errors={counters.get('api_errors', 0)}"
    )


if __name__ == "__main__":
    main()
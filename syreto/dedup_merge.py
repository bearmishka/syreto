import argparse
import io
import os
import re
import tempfile
from difflib import SequenceMatcher
from datetime import datetime
from pathlib import Path

import pandas as pd

try:
    from rapidfuzz import fuzz

    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False


MASTER_COLUMNS = [
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

RECORD_ID_MAP_COLUMNS = ["stable_key", "record_id", "first_seen_date"]
RECORD_ID_MAP_HEADER = ",".join(RECORD_ID_MAP_COLUMNS)
TRIAGE_COLUMNS = [
    "record_id",
    "source_database",
    "source_record_id",
    "title",
    "authors",
    "year",
    "journal",
    "doi",
    "pmid",
    "triage_reason",
]

MERGE_ORDER = ["PubMed", "Embase", "Scopus", "PsycINFO", "Web of Science"]
EMPTY_VALUES = {"", "nan", "none"}
RIS_LINE_PATTERN = re.compile(r"^([A-Z0-9]{2})\s*-\s?(.*)$")
DOI_PATTERN = re.compile(r"10\.\d{4,9}/[^\s;]+", re.IGNORECASE)
YEAR_PATTERN = re.compile(r"(19|20)\d{2}")
RECORD_ID_PATTERN = re.compile(r"^R(\d+)$")
DEFAULT_TITLE_FUZZY_THRESHOLD = 90.0


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
    csv_text = frame.to_csv(index=index)
    atomic_write_text(path, csv_text)


def clean_text(value: object) -> str:
    text = str(value if value is not None else "")
    text = (
        text.replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2019", "'")
        .replace("\u2018", "'")
    )
    text = re.sub(r"\s+", " ", text).strip()
    if text.lower() in EMPTY_VALUES:
        return ""
    return text


def parse_year(value: object) -> str:
    text = clean_text(value)
    if not text:
        return ""
    match = YEAR_PATTERN.search(text)
    return match.group(0) if match else ""


def normalize_doi(value: object) -> str:
    text = clean_text(value).lower()
    if not text:
        return ""
    text = text.replace("https://doi.org/", "").replace("http://doi.org/", "")
    text = text.replace("doi:", "").strip()
    match = DOI_PATTERN.search(text)
    if match:
        return match.group(0).rstrip(".,);")
    return text.rstrip(".,);")


def normalize_pmid(value: object) -> str:
    text = clean_text(value)
    if not text:
        return ""
    digits = re.sub(r"\D+", "", text)
    return digits


def normalize_title(value: object) -> str:
    text = clean_text(value).lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_first_author(value: object) -> str:
    text = clean_text(value)
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


def stable_key_token(value: object) -> str:
    return re.sub(r"\s+", " ", clean_text(value).lower()).strip()


def build_base_stable_key(
    *,
    doi: str,
    pmid: str,
    normalized_title: str,
    normalized_first_author: str,
    year: str,
    source_database: str,
    source_record_id: str,
) -> str:
    if doi:
        return f"doi:{doi}"
    if pmid:
        return f"pmid:{pmid}"
    if normalized_title and normalized_first_author:
        return f"title-author-year:{normalized_title}|{normalized_first_author}|{year or 'na'}"

    source_key = build_row_stable_key(
        source_database=source_database,
        source_record_id=source_record_id,
        base_stable_key="",
    )
    if source_key:
        return source_key

    if normalized_title:
        return f"title-only:{normalized_title}|{normalized_first_author or 'na'}"
    return ""


def build_row_stable_key(source_database: str, source_record_id: str, base_stable_key: str) -> str:
    database_token = stable_key_token(source_database) or "unknown"
    source_token = stable_key_token(source_record_id)
    if source_token:
        return f"source:{database_token}|{source_token}"
    if base_stable_key:
        return f"row:{base_stable_key}|{database_token}"
    return ""


def parse_record_id_number(record_id: object) -> int | None:
    match = RECORD_ID_PATTERN.match(clean_text(record_id))
    if not match:
        return None
    return int(match.group(1))


def read_record_id_map(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=RECORD_ID_MAP_COLUMNS)

    try:
        df = pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=RECORD_ID_MAP_COLUMNS)
    except pd.errors.ParserError:
        raw_text = path.read_text(encoding="utf-8", errors="replace")
        if not raw_text.strip():
            return pd.DataFrame(columns=RECORD_ID_MAP_COLUMNS)

        header_parts = raw_text.split(RECORD_ID_MAP_HEADER)
        if len(header_parts) > 2:
            raw_text = RECORD_ID_MAP_HEADER + header_parts[1]
            for suffix in header_parts[2:]:
                raw_text += f"\n{RECORD_ID_MAP_HEADER}{suffix}"

        try:
            df = pd.read_csv(io.StringIO(raw_text), dtype=str, engine="python", on_bad_lines="skip")
        except pd.errors.EmptyDataError:
            return pd.DataFrame(columns=RECORD_ID_MAP_COLUMNS)

    if "stable_key" in df.columns:
        df = df.loc[df["stable_key"].astype(str).str.lower() != "stable_key"].copy()

    for column in RECORD_ID_MAP_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    return df.loc[:, RECORD_ID_MAP_COLUMNS]


def build_record_id_lookup(map_df: pd.DataFrame) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for _, row in map_df.iterrows():
        stable_key = clean_text(row.get("stable_key", ""))
        record_id = clean_text(row.get("record_id", ""))
        if not stable_key or not record_id:
            continue
        lookup.setdefault(stable_key, record_id)
    return lookup


def append_record_id_map_entries(path: Path, entries: list[dict[str, str]]) -> int:
    if not entries:
        return 0

    output_df = pd.DataFrame(entries, columns=RECORD_ID_MAP_COLUMNS)
    existing_payload = b""
    has_existing_content = path.exists() and path.stat().st_size > 0

    if has_existing_content:
        existing_payload = path.read_bytes()
        if existing_payload and not existing_payload.endswith(b"\n"):
            existing_payload += b"\n"

    new_rows_payload = output_df.to_csv(index=False, header=not has_existing_content).encode(
        "utf-8"
    )
    atomic_replace_bytes(path, existing_payload + new_rows_payload)
    return int(output_df.shape[0])


def read_master_records(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=MASTER_COLUMNS)

    try:
        df = pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=MASTER_COLUMNS)

    for column in MASTER_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    return df.loc[:, MASTER_COLUMNS]


def canonical_master_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=MASTER_COLUMNS)

    duplicate_flags = (
        df.get("is_duplicate", pd.Series(dtype=str)).fillna("").astype(str).str.strip().str.lower()
    )
    return df.loc[~duplicate_flags.eq("yes")].copy()


def build_new_record_triage(
    current_master_df: pd.DataFrame, previous_master_df: pd.DataFrame
) -> pd.DataFrame:
    current_unique_df = canonical_master_rows(current_master_df)
    previous_unique_df = canonical_master_rows(previous_master_df)

    previous_ids = {
        clean_text(record_id)
        for record_id in previous_unique_df.get("record_id", pd.Series(dtype=str)).tolist()
        if clean_text(record_id)
    }

    rows: list[dict[str, str]] = []
    for _, row in current_unique_df.iterrows():
        record_id = clean_text(row.get("record_id", ""))
        if not record_id or record_id in previous_ids:
            continue

        rows.append(
            {
                "record_id": record_id,
                "source_database": clean_text(row.get("source_database", "")),
                "source_record_id": clean_text(row.get("source_record_id", "")),
                "title": clean_text(row.get("title", "")),
                "authors": clean_text(row.get("authors", "")),
                "year": clean_text(row.get("year", "")),
                "journal": clean_text(row.get("journal", "")),
                "doi": clean_text(row.get("doi", "")),
                "pmid": clean_text(row.get("pmid", "")),
                "triage_reason": "new_unique_record_since_previous_merge",
            }
        )

    return pd.DataFrame(rows, columns=TRIAGE_COLUMNS)


def write_triage_csv(path: Path, triage_df: pd.DataFrame) -> None:
    output_df = triage_df.copy()
    for column in TRIAGE_COLUMNS:
        if column not in output_df.columns:
            output_df[column] = ""
    output_df = output_df.loc[:, TRIAGE_COLUMNS]
    atomic_write_dataframe_csv(output_df, path, index=False)


def bootstrap_record_id_map_from_master(
    record_id_map_path: Path, master_path: Path, *, first_seen_date: str
) -> int:
    existing_map_df = read_record_id_map(record_id_map_path)
    if not existing_map_df.empty:
        return 0

    if not master_path.exists():
        return 0

    try:
        master_df = pd.read_csv(master_path, dtype=str)
    except pd.errors.EmptyDataError:
        return 0

    if master_df.empty:
        return 0

    seen_keys: set[str] = set()
    entries: list[dict[str, str]] = []

    for _, row in master_df.iterrows():
        record_id = clean_text(row.get("record_id", ""))
        if not record_id:
            continue

        doi = normalize_doi(row.get("doi", ""))
        pmid = normalize_pmid(row.get("pmid", ""))
        normalized_title = normalize_title(row.get("normalized_title", "") or row.get("title", ""))
        normalized_first_author = normalize_first_author(
            row.get("normalized_first_author", "") or row.get("authors", "")
        )
        year = parse_year(row.get("year", ""))
        source_database = clean_text(row.get("source_database", ""))
        source_record_id = clean_text(row.get("source_record_id", ""))

        base_stable_key = build_base_stable_key(
            doi=doi,
            pmid=pmid,
            normalized_title=normalized_title,
            normalized_first_author=normalized_first_author,
            year=year,
            source_database=source_database,
            source_record_id=source_record_id,
        )
        row_stable_key = build_row_stable_key(source_database, source_record_id, base_stable_key)

        for stable_key in [base_stable_key, row_stable_key]:
            if not stable_key or stable_key in seen_keys:
                continue
            entries.append(
                {
                    "stable_key": stable_key,
                    "record_id": record_id,
                    "first_seen_date": first_seen_date,
                }
            )
            seen_keys.add(stable_key)

    return append_record_id_map_entries(record_id_map_path, entries)


def register_map_entry(
    *,
    stable_key: str,
    record_id: str,
    first_seen_date: str,
    key_to_record_id: dict[str, str],
    new_entries: list[dict[str, str]],
) -> None:
    if not stable_key or stable_key in key_to_record_id:
        return

    key_to_record_id[stable_key] = record_id
    new_entries.append(
        {
            "stable_key": stable_key,
            "record_id": record_id,
            "first_seen_date": first_seen_date,
        }
    )


def next_available_record_id(
    *,
    next_record_number: int,
    used_record_ids: set[str],
) -> tuple[str, int]:
    candidate_number = max(int(next_record_number), 1)
    while True:
        candidate_id = f"R{candidate_number:05d}"
        candidate_number += 1
        if candidate_id in used_record_ids:
            continue
        return candidate_id, candidate_number


def normalized_column_name(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).strip().lower())


def pick_column(df: pd.DataFrame, aliases: list[str]) -> str | None:
    columns = {normalized_column_name(column): column for column in df.columns}
    for alias in aliases:
        key = normalized_column_name(alias)
        if key in columns:
            return str(columns[key])
    return None


def is_non_empty_record(record: dict[str, str]) -> bool:
    for key in [
        "source_record_id",
        "title",
        "abstract",
        "authors",
        "year",
        "journal",
        "doi",
        "pmid",
    ]:
        if clean_text(record.get(key, "")):
            return True
    return False


def standardize_database_name(value: object, fallback_name: str = "") -> str:
    text = clean_text(value)
    probe = (text or fallback_name).lower()

    if "pubmed" in probe or "medline" in probe:
        return "PubMed"
    if "embase" in probe:
        return "Embase"
    if "scopus" in probe:
        return "Scopus"
    if "psyc" in probe:
        return "PsycINFO"
    if "web of science" in probe or "wos" in probe:
        return "Web of Science"
    if probe:
        return text or fallback_name
    return "Unknown"


def parse_ris_file(path: Path, source_database: str) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    current: dict[str, list[str]] = {}

    text = path.read_text(encoding="utf-8", errors="ignore")
    for raw_line in text.splitlines():
        match = RIS_LINE_PATTERN.match(raw_line.rstrip())
        if not match:
            continue

        tag, value = match.group(1).strip(), match.group(2).strip()
        if tag == "ER":
            if current:
                records.append(
                    build_record_from_ris(current, source_database, path, len(records) + 1)
                )
                current = {}
            continue

        current.setdefault(tag, []).append(value)

    if current:
        records.append(build_record_from_ris(current, source_database, path, len(records) + 1))

    return [record for record in records if is_non_empty_record(record)]


def first_tag(tags: dict[str, list[str]], keys: list[str]) -> str:
    for key in keys:
        values = tags.get(key, [])
        for value in values:
            text = clean_text(value)
            if text:
                return text
    return ""


def join_tags(tags: dict[str, list[str]], keys: list[str]) -> str:
    all_values: list[str] = []
    for key in keys:
        for value in tags.get(key, []):
            text = clean_text(value)
            if text:
                all_values.append(text)
    return "; ".join(all_values)


def build_record_from_ris(
    tags: dict[str, list[str]], source_database: str, path: Path, index: int
) -> dict[str, str]:
    title = first_tag(tags, ["TI", "T1", "CT", "BT"])
    abstract = first_tag(tags, ["AB", "N2", "N1"])
    authors = join_tags(tags, ["AU", "A1", "A2", "A3"])
    year = parse_year(first_tag(tags, ["PY", "Y1", "DA", "Y2"]))
    journal = first_tag(tags, ["JO", "JF", "JA", "T2"])
    doi = normalize_doi(first_tag(tags, ["DO", "LID", "M3"]))
    pmid = normalize_pmid(first_tag(tags, ["PM", "AN"]))
    source_record_id = first_tag(tags, ["ID", "AN", "UT", "SN"])
    if not source_record_id:
        source_record_id = f"{path.stem}-{index}"

    return {
        "source_database": source_database,
        "source_record_id": source_record_id,
        "title": title,
        "abstract": abstract,
        "authors": authors,
        "year": year,
        "journal": journal,
        "doi": doi,
        "pmid": pmid,
        "notes": "",
    }


def parse_csv_file(path: Path, source_database: str) -> list[dict[str, str]]:
    try:
        df = pd.read_csv(path, low_memory=False)
    except pd.errors.EmptyDataError:
        return []

    if df.empty:
        return []

    source_id_col = pick_column(
        df, ["source_record_id", "record_id", "id", "eid", "accession number", "an", "ut"]
    )
    title_col = pick_column(df, ["title", "article title", "document title", "ti"])
    abstract_col = pick_column(df, ["abstract", "summary", "description", "ab"])
    authors_col = pick_column(df, ["authors", "author", "author names", "au"])
    year_col = pick_column(df, ["year", "publication year", "pubyear", "py", "date"])
    journal_col = pick_column(
        df, ["journal", "source title", "publication title", "journal title", "so"]
    )
    doi_col = pick_column(df, ["doi", "doi link", "elocation id"])
    pmid_col = pick_column(df, ["pmid", "pubmed id", "pubmed"])

    records: list[dict[str, str]] = []
    for idx, row in df.iterrows():
        source_record_id = clean_text(row[source_id_col]) if source_id_col else ""
        if not source_record_id:
            source_record_id = f"{path.stem}-{idx + 1}"

        title = clean_text(row[title_col]) if title_col else ""
        abstract = clean_text(row[abstract_col]) if abstract_col else ""
        authors = clean_text(row[authors_col]) if authors_col else ""
        year = parse_year(row[year_col]) if year_col else ""
        journal = clean_text(row[journal_col]) if journal_col else ""
        doi = normalize_doi(row[doi_col]) if doi_col else ""
        pmid = normalize_pmid(row[pmid_col]) if pmid_col else ""

        record = {
            "source_database": source_database,
            "source_record_id": source_record_id,
            "title": title,
            "abstract": abstract,
            "authors": authors,
            "year": year,
            "journal": journal,
            "doi": doi,
            "pmid": pmid,
            "notes": "",
        }
        if is_non_empty_record(record):
            records.append(record)

    return records


def load_source_files(
    search_log_path: Path, raw_dir: Path
) -> tuple[list[dict[str, object]], list[str]]:
    sources: list[dict[str, object]] = []
    missing: list[str] = []

    known_paths: set[Path] = set()
    if search_log_path.exists():
        search_df = pd.read_csv(search_log_path)
        has_required_cols = {"database", "export_filename"}.issubset(search_df.columns)
        if has_required_cols:
            for _, row in search_df.iterrows():
                export_name = clean_text(row.get("export_filename", ""))
                if not export_name:
                    continue

                path = raw_dir / export_name
                source_database = standardize_database_name(row.get("database", ""), export_name)
                resolved_path = path.resolve()
                if resolved_path in known_paths:
                    continue
                if path.exists() and path.is_file():
                    sources.append({"database": source_database, "path": path})
                    known_paths.add(resolved_path)
                else:
                    missing.append(f"{source_database}: {path.as_posix()}")

    if raw_dir.exists():
        for path in sorted(raw_dir.glob("*")):
            if not path.is_file() or path.suffix.lower() not in {".ris", ".csv"}:
                continue
            if path.resolve() in known_paths:
                continue
            source_database = standardize_database_name("", path.name)
            sources.append({"database": source_database, "path": path})

    order_map = {name.lower(): index for index, name in enumerate(MERGE_ORDER)}

    def source_key(item: dict[str, object]) -> tuple[int, str, str]:
        database = str(item["database"])
        rank = order_map.get(database.lower(), len(order_map) + 1)
        path = Path(item["path"])
        return rank, database.lower(), path.name.lower()

    sources.sort(key=source_key)
    return sources, missing


def parse_source(path: Path, source_database: str) -> list[dict[str, str]]:
    suffix = path.suffix.lower()
    if suffix == ".ris":
        return parse_ris_file(path, source_database)
    if suffix == ".csv":
        return parse_csv_file(path, source_database)
    return []


def title_similarity(left_title: str, right_title: str) -> float:
    left_clean = clean_text(left_title).lower()
    right_clean = clean_text(right_title).lower()
    if not left_clean or not right_clean:
        return 0.0

    if RAPIDFUZZ_AVAILABLE:
        ratio_score = float(fuzz.ratio(left_clean, right_clean))
        token_sort_score = float(fuzz.token_sort_ratio(left_clean, right_clean))
        return max(ratio_score, token_sort_score)

    ratio_score = 100.0 * SequenceMatcher(None, left_clean, right_clean).ratio()
    left_sorted = " ".join(sorted(left_clean.split()))
    right_sorted = " ".join(sorted(right_clean.split()))
    token_sort_score = 100.0 * SequenceMatcher(None, left_sorted, right_sorted).ratio()
    return max(ratio_score, token_sort_score)


def years_compatible(left_year: str, right_year: str) -> bool:
    left = parse_year(left_year)
    right = parse_year(right_year)
    if not left or not right:
        return True
    return left == right


def deduplicate(
    records: list[dict[str, str]],
    *,
    title_fuzzy_threshold: float = DEFAULT_TITLE_FUZZY_THRESHOLD,
    stable_key_to_record_id: dict[str, str],
    used_record_ids: set[str],
    next_record_number: int,
    first_seen_date: str,
) -> tuple[pd.DataFrame, dict[str, int], list[dict[str, str]], int]:
    doi_index: dict[str, str] = {}
    pmid_index: dict[str, str] = {}
    title_author_index: dict[str, list[tuple[str, str]]] = {}
    title_candidates: dict[str, list[tuple[str, str, str]]] = {}

    rows: list[dict[str, str]] = []
    new_map_entries: list[dict[str, str]] = []
    reason_counts = {
        "DOI match": 0,
        "PMID match": 0,
        "Title-author-year match": 0,
        "Manual judgement": 0,
    }

    for idx, record in enumerate(records, start=1):
        title_norm = normalize_title(record.get("title", ""))
        author_norm = normalize_first_author(record.get("authors", ""))
        year = parse_year(record.get("year", ""))

        doi = normalize_doi(record.get("doi", ""))
        pmid = normalize_pmid(record.get("pmid", ""))
        title_author_key = "|".join([title_norm, author_norm]) if title_norm and author_norm else ""
        source_database = clean_text(record.get("source_database", ""))
        source_record_id = clean_text(record.get("source_record_id", ""))

        base_stable_key = build_base_stable_key(
            doi=doi,
            pmid=pmid,
            normalized_title=title_norm,
            normalized_first_author=author_norm,
            year=year,
            source_database=source_database,
            source_record_id=source_record_id,
        )
        row_stable_key = build_row_stable_key(source_database, source_record_id, base_stable_key)

        duplicate_of = ""
        dedup_reason = ""
        if doi and doi in doi_index:
            duplicate_of = doi_index[doi]
            dedup_reason = "DOI match"
        elif pmid and pmid in pmid_index:
            duplicate_of = pmid_index[pmid]
            dedup_reason = "PMID match"
        elif title_author_key:
            exact_candidates = title_author_index.get(title_author_key, [])
            for existing_record_id, existing_year in exact_candidates:
                if years_compatible(year, existing_year):
                    duplicate_of = existing_record_id
                    dedup_reason = "Title-author-year match"
                    break

        if not duplicate_of and title_norm and author_norm and title_fuzzy_threshold > 0:
            candidates = title_candidates.get(author_norm, [])
            for existing_record_id, existing_title_norm, existing_year in candidates:
                if not years_compatible(year, existing_year):
                    continue
                if title_similarity(title_norm, existing_title_norm) >= title_fuzzy_threshold:
                    duplicate_of = existing_record_id
                    dedup_reason = "Title-author-year match"
                    break

        record_id = ""

        base_mapped_id = stable_key_to_record_id.get(base_stable_key, "") if base_stable_key else ""
        if base_mapped_id and base_mapped_id not in used_record_ids:
            record_id = base_mapped_id
        elif base_stable_key and base_stable_key not in stable_key_to_record_id:
            record_id, next_record_number = next_available_record_id(
                next_record_number=next_record_number,
                used_record_ids=used_record_ids,
            )
            register_map_entry(
                stable_key=base_stable_key,
                record_id=record_id,
                first_seen_date=first_seen_date,
                key_to_record_id=stable_key_to_record_id,
                new_entries=new_map_entries,
            )

        if not record_id and row_stable_key:
            row_mapped_id = stable_key_to_record_id.get(row_stable_key, "")
            if row_mapped_id and row_mapped_id not in used_record_ids:
                record_id = row_mapped_id

                if base_stable_key and base_stable_key not in stable_key_to_record_id:
                    register_map_entry(
                        stable_key=base_stable_key,
                        record_id=record_id,
                        first_seen_date=first_seen_date,
                        key_to_record_id=stable_key_to_record_id,
                        new_entries=new_map_entries,
                    )

        if not record_id:
            record_id, next_record_number = next_available_record_id(
                next_record_number=next_record_number,
                used_record_ids=used_record_ids,
            )

            if row_stable_key:
                register_map_entry(
                    stable_key=row_stable_key,
                    record_id=record_id,
                    first_seen_date=first_seen_date,
                    key_to_record_id=stable_key_to_record_id,
                    new_entries=new_map_entries,
                )
            elif base_stable_key:
                register_map_entry(
                    stable_key=base_stable_key,
                    record_id=record_id,
                    first_seen_date=first_seen_date,
                    key_to_record_id=stable_key_to_record_id,
                    new_entries=new_map_entries,
                )

        used_record_ids.add(record_id)

        if row_stable_key and row_stable_key not in stable_key_to_record_id:
            register_map_entry(
                stable_key=row_stable_key,
                record_id=record_id,
                first_seen_date=first_seen_date,
                key_to_record_id=stable_key_to_record_id,
                new_entries=new_map_entries,
            )

        is_duplicate = "yes" if duplicate_of else "no"

        row = {
            "record_id": record_id,
            "source_database": source_database,
            "source_record_id": source_record_id,
            "title": clean_text(record.get("title", "")),
            "abstract": clean_text(record.get("abstract", "")),
            "authors": clean_text(record.get("authors", "")),
            "year": year,
            "journal": clean_text(record.get("journal", "")),
            "doi": doi,
            "pmid": pmid,
            "normalized_title": title_norm,
            "normalized_first_author": author_norm,
            "is_duplicate": is_duplicate,
            "duplicate_of_record_id": duplicate_of,
            "dedup_reason": dedup_reason,
            "notes": clean_text(record.get("notes", "")),
        }
        rows.append(row)

        if is_duplicate == "yes":
            reason_counts[dedup_reason] += 1
            continue

        if doi and doi not in doi_index:
            doi_index[doi] = record_id
        if pmid and pmid not in pmid_index:
            pmid_index[pmid] = record_id
        if title_author_key:
            title_author_index.setdefault(title_author_key, []).append((record_id, year))
        if title_norm and author_norm:
            title_candidates.setdefault(author_norm, []).append((record_id, title_norm, year))

    df = pd.DataFrame(rows, columns=MASTER_COLUMNS)
    return df, reason_counts, new_map_entries, next_record_number


def build_summary(
    generated_at: str,
    source_stats: list[dict[str, object]],
    missing_sources: list[str],
    wrote_master: bool,
    master_path: Path,
    record_id_map_path: Path,
    record_id_map_entries_added: int,
    triage_output_path: Path,
    triage_rows: int,
    loaded_records: int,
    output_df: pd.DataFrame,
    reason_counts: dict[str, int],
    title_fuzzy_threshold: float,
    fuzzy_enabled: bool,
    fuzzy_backend: str,
) -> str:
    duplicates_total = int((output_df["is_duplicate"] == "yes").sum()) if not output_df.empty else 0
    unique_total = max(loaded_records - duplicates_total, 0)

    lines = []
    lines.append("# Dedup Merge Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Source Files")
    lines.append("")

    if source_stats:
        for item in source_stats:
            lines.append(
                f"- {item['database']}: `{item['path']}` ({item['records']} record(s) parsed)"
            )
    else:
        lines.append("- No source exports were found.")

    lines.append("")
    lines.append("## Missing Referenced Exports")
    lines.append("")
    if missing_sources:
        for item in missing_sources:
            lines.append(f"- {item}")
    else:
        lines.append("- None.")

    lines.append("")
    lines.append("## Merge Result")
    lines.append("")
    lines.append(f"- Loaded records from source exports: {loaded_records}")
    lines.append(f"- Duplicate rows flagged: {duplicates_total}")
    lines.append(f"- Unique records retained: {unique_total}")
    lines.append(f"- Master file updated: {'yes' if wrote_master else 'no'}")
    lines.append(f"- Master path: `{master_path.as_posix()}`")
    lines.append(f"- Record-ID map path: `{record_id_map_path.as_posix()}`")
    lines.append(f"- Record-ID map rows added this run: {record_id_map_entries_added}")
    lines.append(f"- New-record triage path: `{triage_output_path.as_posix()}`")
    lines.append(f"- New unique records since previous merge: {triage_rows}")

    lines.append("")
    lines.append("## Duplicate Reasons")
    lines.append("")
    for reason in ["DOI match", "PMID match", "Title-author-year match", "Manual judgement"]:
        lines.append(f"- {reason}: {reason_counts.get(reason, 0)}")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Merge order follows `01_protocol/dedup_workflow.md`.")
    lines.append(
        "- Duplicate priority is DOI → PMID → normalized title+first author (+year compatibility when available)."
    )
    if fuzzy_enabled:
        lines.append(
            f"- Fuzzy title matching is enabled for same first-author pairs with year-compatibility check (backend: `{fuzzy_backend}`, threshold: {title_fuzzy_threshold:.1f})."
        )
    else:
        lines.append("- Fuzzy title matching is disabled (threshold <= 0).")
    lines.append(
        "- `record_id_map.csv` is append-only: existing stable-key mappings are preserved and only unseen keys receive new IDs."
    )
    lines.append(
        "- New-record triage includes canonical records whose `record_id` was absent from previous master canonical rows."
    )
    lines.append("- When no source exports are available, existing master is left unchanged.")

    return "\n".join(lines) + "\n"


def build_skip_summary(
    generated_at: str,
    source_files: list[dict[str, object]],
    missing_sources: list[str],
    master_path: Path,
    record_id_map_path: Path,
    record_id_map_entries_added: int,
    triage_output_path: Path,
    triage_rows: int,
    skip_reason: str,
) -> str:
    lines = []
    lines.append("# Dedup Merge Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Source Files")
    lines.append("")

    if source_files:
        for item in source_files:
            lines.append(f"- {item['database']}: `{Path(item['path']).as_posix()}`")
    else:
        lines.append("- No source exports were found.")

    lines.append("")
    lines.append("## Missing Referenced Exports")
    lines.append("")
    if missing_sources:
        for item in missing_sources:
            lines.append(f"- {item}")
    else:
        lines.append("- None.")

    lines.append("")
    lines.append("## Merge Result")
    lines.append("")
    lines.append("- Loaded records from source exports: 0")
    lines.append("- Duplicate rows flagged: 0")
    lines.append("- Unique records retained: 0")
    lines.append("- Master file updated: no")
    lines.append(f"- Master path: `{master_path.as_posix()}`")
    lines.append(f"- Record-ID map path: `{record_id_map_path.as_posix()}`")
    lines.append(f"- Record-ID map rows added this run: {record_id_map_entries_added}")
    lines.append(f"- New-record triage path: `{triage_output_path.as_posix()}`")
    lines.append(f"- New unique records since previous merge: {triage_rows}")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Merge order follows `01_protocol/dedup_workflow.md`.")
    lines.append("- `record_id_map.csv` remains append-only even when merge is skipped.")
    lines.append(f"- Merge skipped in `--if-new-exports` mode: {skip_reason}.")

    return "\n".join(lines) + "\n"


def should_run_if_new_exports(
    source_files: list[dict[str, object]], master_path: Path
) -> tuple[bool, str]:
    if not source_files:
        return False, "no source exports found"

    if not master_path.exists():
        return True, "master file is missing"

    latest_source_mtime: float | None = None
    for item in source_files:
        path = Path(item["path"])
        if not path.exists() or not path.is_file():
            continue
        mtime = path.stat().st_mtime
        latest_source_mtime = (
            mtime if latest_source_mtime is None else max(latest_source_mtime, mtime)
        )

    if latest_source_mtime is None:
        return False, "no readable source export files"

    master_mtime = master_path.stat().st_mtime
    if latest_source_mtime > master_mtime:
        return True, "new source exports detected"

    return False, "master_records.csv is up to date (no newer exports)"


def read_master_record_ids(master_path: Path) -> set[str]:
    master_df = read_master_records(master_path)
    if "record_id" not in master_df.columns:
        return set()
    return {clean_text(value) for value in master_df["record_id"].tolist() if clean_text(value)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Merge raw exports and auto-flag duplicates in master_records.csv."
    )
    parser.add_argument(
        "--raw-dir", default="../02_data/raw", help="Directory with raw export files (RIS/CSV)"
    )
    parser.add_argument(
        "--search-log",
        default="../02_data/processed/search_log.csv",
        help="Search log CSV with export filenames",
    )
    parser.add_argument(
        "--master",
        default="../02_data/processed/master_records.csv",
        help="Master records output CSV",
    )
    parser.add_argument(
        "--record-id-map",
        default="../02_data/processed/record_id_map.csv",
        help="Persistent stable-key → record_id map (append-only).",
    )
    parser.add_argument(
        "--triage-output",
        default="outputs/new_record_triage.csv",
        help="CSV queue of newly added unique records since previous merge.",
    )
    parser.add_argument(
        "--summary", default="outputs/dedup_merge_summary.md", help="Dedup merge summary markdown"
    )
    parser.add_argument(
        "--allow-empty-overwrite",
        action="store_true",
        help="Overwrite master with header-only CSV when no source records are found",
    )
    parser.add_argument(
        "--if-new-exports",
        action="store_true",
        help="Run merge only when at least one source export is newer than master_records.csv",
    )
    parser.add_argument(
        "--title-fuzzy-threshold",
        type=float,
        default=DEFAULT_TITLE_FUZZY_THRESHOLD,
        help="Fuzzy title-match threshold (0-100) for same first-author+year candidates",
    )
    args = parser.parse_args(argv)

    raw_dir = Path(args.raw_dir)
    search_log_path = Path(args.search_log)
    master_path = Path(args.master)
    record_id_map_path = Path(args.record_id_map)
    triage_output_path = Path(args.triage_output)
    summary_path = Path(args.summary)

    previous_master_df = read_master_records(master_path)

    first_seen_date = datetime.now().date().isoformat()
    bootstrap_rows_added = bootstrap_record_id_map_from_master(
        record_id_map_path,
        master_path,
        first_seen_date=first_seen_date,
    )

    source_files, missing_sources = load_source_files(search_log_path, raw_dir)

    if args.if_new_exports:
        should_run, reason = should_run_if_new_exports(source_files, master_path)
        if not should_run:
            triage_df = pd.DataFrame(columns=TRIAGE_COLUMNS)
            write_triage_csv(triage_output_path, triage_df)
            generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
            summary = build_skip_summary(
                generated_at=generated_at,
                source_files=source_files,
                missing_sources=missing_sources,
                master_path=master_path,
                record_id_map_path=record_id_map_path,
                record_id_map_entries_added=bootstrap_rows_added,
                triage_output_path=triage_output_path,
                triage_rows=0,
                skip_reason=reason,
            )
            atomic_write_text(summary_path, summary)
            print(f"Wrote: {triage_output_path}")
            print(f"Wrote: {summary_path}")
            print(f"Skipped merge (`--if-new-exports`): {reason}.")
            if bootstrap_rows_added:
                print(
                    f"Updated: {record_id_map_path} (+{bootstrap_rows_added} row(s) bootstrapped)"
                )
            return 0

    source_stats: list[dict[str, object]] = []
    parsed_records: list[dict[str, str]] = []
    for item in source_files:
        database = str(item["database"])
        path = Path(item["path"])
        records = parse_source(path, database)
        parsed_records.extend(records)
        source_stats.append(
            {"database": database, "path": path.as_posix(), "records": len(records)}
        )

    threshold = max(0.0, min(100.0, float(args.title_fuzzy_threshold)))
    fuzzy_backend = "rapidfuzz" if RAPIDFUZZ_AVAILABLE else "difflib-fallback"
    if threshold > 0 and not RAPIDFUZZ_AVAILABLE:
        print(
            "Warning: rapidfuzz is not installed; using difflib fallback for fuzzy title matching."
        )

    record_id_map_df = read_record_id_map(record_id_map_path)
    stable_key_to_record_id = build_record_id_lookup(record_id_map_df)

    used_record_ids = {record_id for record_id in stable_key_to_record_id.values() if record_id}
    used_record_ids.update(read_master_record_ids(master_path))

    max_record_number = 0
    for record_id in used_record_ids:
        parsed_number = parse_record_id_number(record_id)
        if parsed_number is not None:
            max_record_number = max(max_record_number, parsed_number)

    output_df, reason_counts, new_record_id_map_entries, _ = deduplicate(
        records=parsed_records,
        title_fuzzy_threshold=threshold,
        stable_key_to_record_id=stable_key_to_record_id,
        used_record_ids=set(),
        next_record_number=max_record_number + 1,
        first_seen_date=first_seen_date,
    )

    triage_df = build_new_record_triage(output_df, previous_master_df)
    write_triage_csv(triage_output_path, triage_df)
    triage_rows = int(triage_df.shape[0])

    appended_rows = append_record_id_map_entries(record_id_map_path, new_record_id_map_entries)
    total_record_id_map_rows_added = bootstrap_rows_added + appended_rows

    wrote_master = False
    if not parsed_records and not args.allow_empty_overwrite:
        wrote_master = False
    else:
        atomic_write_dataframe_csv(output_df, master_path, index=False)
        wrote_master = True

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    summary = build_summary(
        generated_at=generated_at,
        source_stats=source_stats,
        missing_sources=missing_sources,
        wrote_master=wrote_master,
        master_path=master_path,
        record_id_map_path=record_id_map_path,
        record_id_map_entries_added=total_record_id_map_rows_added,
        triage_output_path=triage_output_path,
        triage_rows=triage_rows,
        loaded_records=len(parsed_records),
        output_df=output_df,
        reason_counts=reason_counts,
        title_fuzzy_threshold=threshold,
        fuzzy_enabled=threshold > 0,
        fuzzy_backend=fuzzy_backend,
    )
    atomic_write_text(summary_path, summary)

    print(f"Wrote: {triage_output_path}")
    print(f"Wrote: {summary_path}")
    if wrote_master:
        print(f"Updated: {master_path}")
    else:
        print(
            "Skipped master update (no source records loaded; use --allow-empty-overwrite to force)."
        )
    if total_record_id_map_rows_added:
        print(f"Updated: {record_id_map_path} (+{total_record_id_map_rows_added} row(s))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

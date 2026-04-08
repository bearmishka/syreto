import argparse
import re
from collections import Counter
from datetime import datetime
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

YES_VALUES = {"yes", "y", "1", "true"}
YEAR_PATTERN = re.compile(r"(19|20)\d{2}")


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def is_missing(value: object) -> bool:
    return normalize(value).lower() in MISSING_CODES


def non_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    mask = df.apply(lambda row: any(not is_missing(value) for value in row), axis=1)
    return df[mask].copy()


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


def split_authors(value: object) -> list[str]:
    text = normalize(value)
    if not text:
        return []

    if ";" in text:
        parts = [item.strip() for item in text.split(";")]
    elif "|" in text:
        parts = [item.strip() for item in text.split("|")]
    elif " and " in text.lower():
        parts = [item.strip() for item in re.split(r"\band\b", text, flags=re.IGNORECASE)]
    else:
        parts = [text]

    return [item for item in parts if item]


def parse_keywords(value: object) -> list[str]:
    text = normalize(value)
    if not text:
        return []
    parts = re.split(r";|,|\|", text)
    keywords = [part.strip() for part in parts if part.strip()]
    deduped: list[str] = []
    seen: set[str] = set()
    for keyword in keywords:
        key = keyword.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(keyword)
    return deduped


def ris_line(tag: str, value: str) -> str:
    clean = value.replace("\r", " ").replace("\n", " ").strip()
    return f"{tag}  - {clean}"


def load_extraction(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Extraction file not found: {path}")

    try:
        extraction_df = pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        extraction_df = pd.DataFrame()

    legacy_to_generic = {
        "object_relation_construct": "predictor_construct",
        "identity_construct": "outcome_construct",
    }
    for legacy, generic in legacy_to_generic.items():
        if generic not in extraction_df.columns and legacy in extraction_df.columns:
            extraction_df[generic] = extraction_df[legacy]

    required = [
        "study_id",
        "first_author",
        "year",
        "country",
        "study_design",
        "predictor_construct",
        "outcome_construct",
        "notes",
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
        "abstract",
        "authors",
        "year",
        "journal",
        "doi",
        "pmid",
        "source_database",
        "is_duplicate",
    ]
    for column in required:
        if column not in master_df.columns:
            master_df[column] = ""

    if not include_duplicates:
        is_dup = (
            master_df["is_duplicate"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .isin(YES_VALUES)
        )
        master_df = master_df.loc[~is_dup].copy()

    return master_df


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
            key = f"{author_norm}|{year}"
            indexes["author_year"].setdefault(key, []).append(row_idx)
            if title_norm:
                key2 = f"{title_norm}|{author_norm}|{year}"
                indexes["title_author_year"].setdefault(key2, []).append(row_idx)

    return indexes


def pick_master_match(
    extraction_row: pd.Series,
    master_df: pd.DataFrame,
    indexes: dict[str, dict[str, list[int]]],
) -> tuple[pd.Series | None, str]:
    if master_df.empty:
        return None, "no_master"

    ext_record_id = normalize(extraction_row.get("record_id", ""))
    if ext_record_id and ext_record_id in indexes["record_id"]:
        return master_df.loc[indexes["record_id"][ext_record_id][0]], "record_id"

    ext_source_record_id = normalize(extraction_row.get("source_record_id", ""))
    if ext_source_record_id and ext_source_record_id in indexes["source_record_id"]:
        return master_df.loc[
            indexes["source_record_id"][ext_source_record_id][0]
        ], "source_record_id"

    ext_doi = normalize_doi(extraction_row.get("doi", ""))
    if ext_doi and ext_doi in indexes["doi"]:
        return master_df.loc[indexes["doi"][ext_doi][0]], "doi"

    ext_pmid = normalize_pmid(extraction_row.get("pmid", ""))
    if ext_pmid and ext_pmid in indexes["pmid"]:
        return master_df.loc[indexes["pmid"][ext_pmid][0]], "pmid"

    ext_author = normalize_first_author(extraction_row.get("first_author", ""))
    ext_year = parse_year(extraction_row.get("year", ""))
    ext_title = normalize_title(extraction_row.get("title", ""))

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


def select_included_extraction_rows(extraction_df: pd.DataFrame) -> pd.DataFrame:
    non_empty_df = non_empty_rows(extraction_df)
    if non_empty_df.empty:
        return non_empty_df
    mask = ~non_empty_df["study_id"].apply(is_missing)
    return non_empty_df.loc[mask].copy()


def sort_included_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    sorted_df = df.copy()
    sorted_df["_sort_year"] = pd.to_numeric(sorted_df["year"], errors="coerce").fillna(9999)
    sorted_df["_sort_author"] = sorted_df["first_author"].fillna("").astype(str).str.lower()
    sorted_df["_sort_study_id"] = sorted_df["study_id"].fillna("").astype(str).str.lower()
    sorted_df = sorted_df.sort_values(
        by=["_sort_year", "_sort_author", "_sort_study_id"], kind="stable"
    )
    return sorted_df.drop(columns=["_sort_year", "_sort_author", "_sort_study_id"])


def render_ris_records(
    included_df: pd.DataFrame,
    master_df: pd.DataFrame,
    indexes: dict[str, dict[str, list[int]]],
) -> tuple[str, Counter[str], int, int]:
    lines: list[str] = []
    match_counts: Counter[str] = Counter()
    missing_title_count = 0
    no_author_count = 0

    for _, ext_row in included_df.iterrows():
        master_row, match_method = pick_master_match(ext_row, master_df, indexes)
        match_counts[match_method] += 1

        study_id = normalize(ext_row.get("study_id", ""))
        first_author = normalize(ext_row.get("first_author", ""))
        year = parse_year(ext_row.get("year", ""))
        country = normalize(ext_row.get("country", ""))
        study_design = normalize(ext_row.get("study_design", ""))

        if master_row is not None:
            title = normalize(master_row.get("title", ""))
            abstract = normalize(master_row.get("abstract", ""))
            authors = split_authors(master_row.get("authors", ""))
            journal = normalize(master_row.get("journal", ""))
            doi = normalize_doi(master_row.get("doi", ""))
            pmid = normalize_pmid(master_row.get("pmid", ""))
            source_database = normalize(master_row.get("source_database", ""))
        else:
            title = normalize(ext_row.get("title", ""))
            abstract = normalize(ext_row.get("abstract", ""))
            authors = split_authors(ext_row.get("authors", ""))
            journal = normalize(ext_row.get("journal", ""))
            doi = normalize_doi(ext_row.get("doi", ""))
            pmid = normalize_pmid(ext_row.get("pmid", ""))
            source_database = ""

        if not title:
            if first_author and year:
                title = f"{first_author} ({year})"
            elif study_id:
                title = study_id
            else:
                title = "Untitled included study"
            missing_title_count += 1

        if not authors and first_author:
            authors = [first_author]
        if not authors:
            no_author_count += 1

        keywords = []
        keywords.extend(parse_keywords(ext_row.get("predictor_construct", "")))
        keywords.extend(parse_keywords(ext_row.get("outcome_construct", "")))
        if study_design:
            keywords.extend(parse_keywords(study_design))

        ris_type = "JOUR"

        lines.append(ris_line("TY", ris_type))
        if study_id:
            lines.append(ris_line("ID", study_id))
        lines.append(ris_line("T1", title))
        for author in authors:
            lines.append(ris_line("AU", author))
        if year:
            lines.append(ris_line("PY", year))
            lines.append(ris_line("Y1", f"{year}///"))
        if journal:
            lines.append(ris_line("JF", journal))
        if doi:
            lines.append(ris_line("DO", doi))
            lines.append(ris_line("UR", f"https://doi.org/{doi}"))
        if pmid:
            lines.append(ris_line("AN", pmid))
        if abstract:
            lines.append(ris_line("AB", abstract))
        if country:
            lines.append(ris_line("C1", country))
        if source_database:
            lines.append(ris_line("DB", source_database))
        for keyword in keywords:
            lines.append(ris_line("KW", keyword))

        extraction_note = normalize(ext_row.get("notes", ""))
        if extraction_note:
            lines.append(ris_line("N1", extraction_note))
        if match_method not in {
            "record_id",
            "source_record_id",
            "doi",
            "pmid",
            "title_author_year",
            "author_year_unique",
        }:
            lines.append(ris_line("N1", f"export_match={match_method}"))

        lines.append(ris_line("ER", ""))
        lines.append("")

    return "\n".join(lines), match_counts, missing_title_count, no_author_count


def render_summary(
    *,
    extraction_path: Path,
    master_path: Path,
    output_path: Path,
    total_extraction_rows: int,
    included_rows: int,
    exported_rows: int,
    master_rows_used: int,
    match_counts: Counter[str],
    missing_title_count: int,
    no_author_count: int,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = []
    lines.append("# Export to RIS Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Extraction input: `{extraction_path.as_posix()}`")
    lines.append(f"- Master records input: `{master_path.as_posix()}`")
    lines.append(f"- RIS output: `{output_path.as_posix()}`")
    lines.append("")
    lines.append("## Row Counts")
    lines.append("")
    lines.append(f"- Extraction rows (raw): {total_extraction_rows}")
    lines.append(f"- Included rows (non-empty `study_id`): {included_rows}")
    lines.append(f"- Exported RIS records: {exported_rows}")
    lines.append(f"- Non-duplicate master rows available for matching: {master_rows_used}")
    lines.append("")
    lines.append("## Matching")
    lines.append("")
    if match_counts:
        for key, value in sorted(match_counts.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"- `{key}`: {value}")
    else:
        lines.append("- No records exported.")
    lines.append("")
    lines.append("## Data Quality Notes")
    lines.append("")
    lines.append(f"- Records with fallback title placeholder: {missing_title_count}")
    lines.append(f"- Records without author field in RIS: {no_author_count}")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append(
        "- Selection rule for included studies follows synthesis workflow: non-empty `study_id` rows in extraction table."
    )
    lines.append(
        "- Bibliographic enrichment uses `master_records.csv` when a reliable match is available."
    )
    lines.append("- RIS structure is Zotero/EndNote compatible (`TY/T1/AU/PY/JF/DO/AB/KW/.../ER`).")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export included studies to RIS (Zotero/EndNote compatible), synthesisr-style reverse export."
    )
    parser.add_argument(
        "--extraction",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction template CSV",
    )
    parser.add_argument(
        "--master",
        default="../02_data/processed/master_records.csv",
        help="Path to deduplicated master records CSV (used for bibliographic enrichment)",
    )
    parser.add_argument(
        "--output",
        default="outputs/included_studies_export.ris",
        help="Path to RIS output file",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/export_to_ris_summary.md",
        help="Path to markdown summary output",
    )
    parser.add_argument(
        "--include-duplicate-master",
        action="store_true",
        help="Allow duplicate rows from master records as match candidates",
    )
    args = parser.parse_args()

    extraction_path = Path(args.extraction)
    master_path = Path(args.master)
    output_path = Path(args.output)
    summary_output_path = Path(args.summary_output)

    extraction_df = load_extraction(extraction_path)
    total_extraction_rows = int(extraction_df.shape[0])

    included_df = select_included_extraction_rows(extraction_df)
    included_df = sort_included_rows(included_df)

    master_df = load_master(master_path, include_duplicates=args.include_duplicate_master)
    indexes = build_master_indexes(master_df)

    ris_text, match_counts, missing_title_count, no_author_count = render_ris_records(
        included_df,
        master_df,
        indexes,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(ris_text, encoding="utf-8")

    summary_text = render_summary(
        extraction_path=extraction_path,
        master_path=master_path,
        output_path=output_path,
        total_extraction_rows=total_extraction_rows,
        included_rows=int(included_df.shape[0]),
        exported_rows=int(included_df.shape[0]),
        master_rows_used=int(master_df.shape[0]),
        match_counts=match_counts,
        missing_title_count=missing_title_count,
        no_author_count=no_author_count,
    )

    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(summary_text, encoding="utf-8")

    print(f"Wrote: {output_path}")
    print(f"Wrote: {summary_output_path}")
    print(
        "Export stats: "
        f"raw_extraction={total_extraction_rows}, "
        f"included={int(included_df.shape[0])}, "
        f"exported={int(included_df.shape[0])}, "
        f"master_candidates={int(master_df.shape[0])}"
    )


if __name__ == "__main__":
    main()

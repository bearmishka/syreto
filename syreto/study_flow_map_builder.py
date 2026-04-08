import argparse
from datetime import datetime
import os
from pathlib import Path
import tempfile

import pandas as pd


OUTPUT_COLUMNS = [
    "study_id",
    "found_in_search",
    "passed_screening",
    "included_in_review",
    "included_in_meta",
    "included_in_bias",
]


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
    atomic_write_text(path, frame.to_csv(index=index))


def read_csv_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def normalize_lower(value: object) -> str:
    return normalize(value).lower()


def is_yes(value: object) -> bool:
    return normalize_lower(value) in {"1", "true", "yes", "y", "include", "included"}


def yes_no(flag: bool) -> str:
    return "yes" if flag else "no"


def is_included_in_review(row: pd.Series) -> bool:
    exclusion_reason = normalize_lower(row.get("exclusion_reason", ""))
    if exclusion_reason.startswith("included_"):
        return True

    return any(
        is_yes(row.get(column, ""))
        for column in ["included_in_meta", "included_in_bias", "included_in_grade"]
    )


def build_source_to_record_id_map(
    master_df: pd.DataFrame, record_id_map_df: pd.DataFrame
) -> dict[str, str]:
    mapping: dict[str, str] = {}

    def add_mapping(key: str, record_id: str) -> None:
        normalized_key = normalize_lower(key)
        normalized_record_id = normalize(record_id)
        if not normalized_key or not normalized_record_id:
            return
        if normalized_key not in mapping:
            mapping[normalized_key] = normalized_record_id

    if not master_df.empty:
        for _, row in master_df.iterrows():
            record_id = normalize(row.get("record_id", ""))
            source_record_id = normalize(row.get("source_record_id", ""))
            source_database = normalize_lower(row.get("source_database", ""))

            add_mapping(record_id, record_id)
            add_mapping(source_record_id, record_id)
            if source_database and source_record_id:
                add_mapping(f"source:{source_database}|{source_record_id}", record_id)

    if not record_id_map_df.empty:
        for _, row in record_id_map_df.iterrows():
            stable_key = normalize(row.get("stable_key", ""))
            record_id = normalize(row.get("record_id", ""))

            add_mapping(stable_key, record_id)
            add_mapping(record_id, record_id)

            stable_key_lower = stable_key.lower()
            if stable_key_lower.startswith("source:") and "|" in stable_key_lower:
                _, source_record_id = stable_key_lower.split("|", 1)
                add_mapping(source_record_id, record_id)

    return mapping


def build_screening_decisions(screening_df: pd.DataFrame) -> dict[str, str]:
    if screening_df.empty:
        return {}

    decisions: dict[str, str] = {}

    for _, row in screening_df.iterrows():
        record_id = normalize(row.get("record_id", ""))
        if not record_id:
            continue

        decision = normalize_lower(row.get("final_decision", ""))
        if not decision:
            decision = normalize_lower(row.get("resolution_decision", ""))
        if not decision:
            decision = normalize_lower(row.get("reviewer1_decision", ""))

        if decision:
            decisions[record_id] = decision

    return decisions


def has_search_hits(search_log_df: pd.DataFrame) -> bool:
    if search_log_df.empty or "results_total" not in search_log_df.columns:
        return False

    numeric = pd.to_numeric(search_log_df["results_total"], errors="coerce")
    return bool((numeric.fillna(0) > 0).any())


def build_study_flow_map(
    *,
    extraction_df: pd.DataFrame,
    source_to_record_id: dict[str, str],
    screening_decisions: dict[str, str],
    search_hits_present: bool,
) -> tuple[pd.DataFrame, dict[str, int]]:
    if extraction_df.empty:
        empty = pd.DataFrame(columns=OUTPUT_COLUMNS)
        return empty, {
            "total_studies": 0,
            "mapped_source_links": 0,
            "heuristic_screening_assumptions": 0,
            "found_in_search_yes": 0,
            "passed_screening_yes": 0,
            "included_in_review_yes": 0,
            "included_in_meta_yes": 0,
            "included_in_bias_yes": 0,
        }

    if "study_id" not in extraction_df.columns:
        extraction_df["study_id"] = ""

    grouped: dict[str, dict[str, object]] = {}
    for _, row in extraction_df.iterrows():
        study_id = normalize(row.get("study_id", ""))
        if not study_id:
            continue

        if study_id not in grouped:
            grouped[study_id] = {
                "source_ids": set(),
                "included_in_review": False,
                "included_in_meta": False,
                "included_in_bias": False,
            }

        source_id = normalize(row.get("source_id", ""))
        if source_id:
            grouped[study_id]["source_ids"].add(source_id)

        grouped[study_id]["included_in_review"] = bool(
            grouped[study_id]["included_in_review"] or is_included_in_review(row)
        )
        grouped[study_id]["included_in_meta"] = bool(
            grouped[study_id]["included_in_meta"] or is_yes(row.get("included_in_meta", ""))
        )
        grouped[study_id]["included_in_bias"] = bool(
            grouped[study_id]["included_in_bias"] or is_yes(row.get("included_in_bias", ""))
        )

    has_screening_include = any(decision == "include" for decision in screening_decisions.values())

    rows: list[dict[str, str]] = []
    mapped_source_links = 0
    heuristic_screening_assumptions = 0

    for study_id in sorted(grouped.keys()):
        details = grouped[study_id]
        source_ids: set[str] = details["source_ids"]  # type: ignore[assignment]
        included_in_review = bool(details["included_in_review"])
        included_in_meta = bool(details["included_in_meta"])
        included_in_bias = bool(details["included_in_bias"])

        mapped_record_ids: set[str] = set()
        for source_id in source_ids:
            key = normalize_lower(source_id)
            if key in source_to_record_id:
                mapped_record_ids.add(source_to_record_id[key])
            elif source_id in screening_decisions:
                mapped_record_ids.add(source_id)

        has_explicit_source_link = bool(mapped_record_ids)
        if has_explicit_source_link:
            mapped_source_links += 1

        found_in_search = bool(source_ids) and (has_explicit_source_link or search_hits_present)

        if has_explicit_source_link:
            linked_decisions = [
                screening_decisions.get(record_id, "")
                for record_id in mapped_record_ids
                if record_id in screening_decisions
            ]
            if linked_decisions:
                passed_screening = any(decision == "include" for decision in linked_decisions)
            else:
                passed_screening = False
        else:
            passed_screening = found_in_search and included_in_review and has_screening_include
            if passed_screening:
                heuristic_screening_assumptions += 1

        rows.append(
            {
                "study_id": study_id,
                "found_in_search": yes_no(found_in_search),
                "passed_screening": yes_no(passed_screening),
                "included_in_review": yes_no(included_in_review),
                "included_in_meta": yes_no(included_in_meta),
                "included_in_bias": yes_no(included_in_bias),
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        frame = pd.DataFrame(columns=OUTPUT_COLUMNS)
    else:
        frame = frame[OUTPUT_COLUMNS]

    metrics = {
        "total_studies": len(frame),
        "mapped_source_links": mapped_source_links,
        "heuristic_screening_assumptions": heuristic_screening_assumptions,
        "found_in_search_yes": int((frame["found_in_search"] == "yes").sum())
        if not frame.empty
        else 0,
        "passed_screening_yes": int((frame["passed_screening"] == "yes").sum())
        if not frame.empty
        else 0,
        "included_in_review_yes": int((frame["included_in_review"] == "yes").sum())
        if not frame.empty
        else 0,
        "included_in_meta_yes": int((frame["included_in_meta"] == "yes").sum())
        if not frame.empty
        else 0,
        "included_in_bias_yes": int((frame["included_in_bias"] == "yes").sum())
        if not frame.empty
        else 0,
    }

    return frame, metrics


def build_summary(
    *,
    extraction_input: Path,
    master_records_input: Path,
    record_id_map_input: Path,
    screening_input: Path,
    search_log_input: Path,
    output_path: Path,
    metrics: dict[str, int],
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = []
    lines.append("# Study Flow Map")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Extraction input: `{extraction_input.as_posix()}`")
    lines.append(f"- Master-records input: `{master_records_input.as_posix()}`")
    lines.append(f"- Record-ID map input: `{record_id_map_input.as_posix()}`")
    lines.append(f"- Screening input: `{screening_input.as_posix()}`")
    lines.append(f"- Search-log input: `{search_log_input.as_posix()}`")
    lines.append(f"- Output: `{output_path.as_posix()}`")
    lines.append("")
    lines.append("## Coverage")
    lines.append("")
    lines.append(f"- Total studies: {metrics['total_studies']}")
    lines.append(f"- Found in search (`yes`): {metrics['found_in_search_yes']}")
    lines.append(f"- Passed screening (`yes`): {metrics['passed_screening_yes']}")
    lines.append(f"- Included in review (`yes`): {metrics['included_in_review_yes']}")
    lines.append(f"- Included in meta (`yes`): {metrics['included_in_meta_yes']}")
    lines.append(f"- Included in bias (`yes`): {metrics['included_in_bias_yes']}")
    lines.append("")
    lines.append("## Linkage Quality")
    lines.append("")
    lines.append(f"- Explicit source-to-record links: {metrics['mapped_source_links']}")
    lines.append(f"- Heuristic screening assumptions: {metrics['heuristic_screening_assumptions']}")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build study-level flow map across search, screening, inclusion, extraction, and analysis flags."
    )
    parser.add_argument(
        "--extraction-input",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction template CSV.",
    )
    parser.add_argument(
        "--master-records-input",
        default="../02_data/processed/master_records.csv",
        help="Path to master records CSV.",
    )
    parser.add_argument(
        "--record-id-map-input",
        default="../02_data/processed/record_id_map.csv",
        help="Path to stable key/record ID map CSV.",
    )
    parser.add_argument(
        "--screening-input",
        default="../02_data/processed/screening_title_abstract_results.csv",
        help="Path to title/abstract screening consensus CSV.",
    )
    parser.add_argument(
        "--search-log-input",
        default="../02_data/processed/search_log.csv",
        help="Path to search log CSV.",
    )
    parser.add_argument(
        "--output",
        default="outputs/study_flow_map.csv",
        help="Path to study flow map CSV output.",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/study_flow_map_summary.md",
        help="Path to markdown summary output.",
    )
    args = parser.parse_args()

    extraction_input = Path(args.extraction_input)
    master_records_input = Path(args.master_records_input)
    record_id_map_input = Path(args.record_id_map_input)
    screening_input = Path(args.screening_input)
    search_log_input = Path(args.search_log_input)
    output_path = Path(args.output)
    summary_output = Path(args.summary_output)

    extraction_df = read_csv_or_empty(extraction_input)
    master_df = read_csv_or_empty(master_records_input)
    record_id_map_df = read_csv_or_empty(record_id_map_input)
    screening_df = read_csv_or_empty(screening_input)
    search_log_df = read_csv_or_empty(search_log_input)

    source_to_record_id = build_source_to_record_id_map(master_df, record_id_map_df)
    screening_decisions = build_screening_decisions(screening_df)
    search_hits_present = has_search_hits(search_log_df)

    flow_map_df, metrics = build_study_flow_map(
        extraction_df=extraction_df,
        source_to_record_id=source_to_record_id,
        screening_decisions=screening_decisions,
        search_hits_present=search_hits_present,
    )

    summary_text = build_summary(
        extraction_input=extraction_input,
        master_records_input=master_records_input,
        record_id_map_input=record_id_map_input,
        screening_input=screening_input,
        search_log_input=search_log_input,
        output_path=output_path,
        metrics=metrics,
    )

    atomic_write_dataframe_csv(flow_map_df, output_path)
    atomic_write_text(summary_output, summary_text)

    print(f"Wrote: {output_path}")
    print(f"Wrote: {summary_output}")
    print(f"Mapped studies: {metrics['total_studies']}")


if __name__ == "__main__":
    main()

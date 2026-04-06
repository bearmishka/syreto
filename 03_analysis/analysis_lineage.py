import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile

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


def is_missing(value: object) -> bool:
    return normalize_lower(value) in MISSING_CODES


def normalized_outcome(value: object) -> str:
    text = normalize(value)
    return text if text and not is_missing(text) else "unspecified_outcome"


def append_mapping(mapping: dict[str, set[str]], outcome: str, study_id: str) -> None:
    if outcome not in mapping:
        mapping[outcome] = set()
    mapping[outcome].add(study_id)


def extraction_outcome_maps(extraction_df: pd.DataFrame) -> tuple[dict[str, list[str]], dict[str, set[str]]]:
    study_to_outcomes: dict[str, set[str]] = {}
    outcome_to_studies: dict[str, set[str]] = {}

    if extraction_df.empty:
        return {}, {}

    working = extraction_df.copy()
    for column in ["study_id", "outcome_construct", "outcome_measure", "outcome"]:
        if column not in working.columns:
            working[column] = ""

    for _, row in working.iterrows():
        study_id = normalize(row.get("study_id", ""))
        if not study_id:
            continue

        outcome = normalized_outcome(row.get("outcome_construct", ""))
        if outcome == "unspecified_outcome":
            outcome = normalized_outcome(row.get("outcome_measure", ""))
        if outcome == "unspecified_outcome":
            outcome = normalized_outcome(row.get("outcome", ""))

        study_to_outcomes.setdefault(study_id, set()).add(outcome)
        append_mapping(outcome_to_studies, outcome, study_id)

    sorted_study_map = {
        study_id: sorted(outcomes)
        for study_id, outcomes in sorted(study_to_outcomes.items())
    }
    return sorted_study_map, outcome_to_studies


def grouped_outcome_to_studies(
    dataframe: pd.DataFrame,
    *,
    outcome_column: str,
    study_column: str = "study_id",
) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = {}
    if dataframe.empty:
        return mapping

    working = dataframe.copy()
    if outcome_column not in working.columns:
        return mapping
    if study_column not in working.columns:
        return mapping

    for _, row in working.iterrows():
        study_id = normalize(row.get(study_column, ""))
        if not study_id:
            continue
        outcome = normalized_outcome(row.get(outcome_column, ""))
        append_mapping(mapping, outcome, study_id)

    return mapping


def forest_outcome_to_studies(
    forest_df: pd.DataFrame,
    *,
    extraction_study_to_outcomes: dict[str, list[str]],
) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = {}
    if forest_df.empty:
        return mapping

    working = forest_df.copy()
    if "study_id" not in working.columns:
        return mapping

    has_outcome_column = "outcome" in working.columns and working["outcome"].fillna("").astype(str).str.strip().ne("").any()

    for _, row in working.iterrows():
        study_id = normalize(row.get("study_id", ""))
        if not study_id:
            continue

        outcomes: list[str] = []
        if has_outcome_column:
            outcomes = [normalized_outcome(row.get("outcome", ""))]
        elif study_id in extraction_study_to_outcomes:
            outcomes = extraction_study_to_outcomes[study_id]

        if not outcomes:
            outcomes = ["unmapped_outcome"]

        for outcome in outcomes:
            append_mapping(mapping, outcome, study_id)

    return mapping


def records_from_mapping(mapping: dict[str, set[str]]) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for outcome in sorted(mapping.keys()):
        records.append(
            {
                "outcome": outcome,
                "studies_used": sorted(mapping[outcome]),
            }
        )
    return records


def meta_records(
    meta_df: pd.DataFrame,
    *,
    publication_bias_map: dict[str, set[str]],
    forest_map: dict[str, set[str]],
    grade_map: dict[str, set[str]],
    extraction_map: dict[str, set[str]],
) -> list[dict[str, object]]:
    if meta_df.empty or "outcome" not in meta_df.columns:
        return []

    outcomes = [
        normalized_outcome(value)
        for value in meta_df["outcome"].tolist()
    ]
    unique_outcomes = sorted(set(outcomes))
    records: list[dict[str, object]] = []

    for outcome in unique_outcomes:
        studies: set[str] = set()
        evidence_sources: list[str] = []

        if outcome in publication_bias_map:
            studies.update(publication_bias_map[outcome])
            evidence_sources.append("publication_bias_data")
        if not studies and outcome in forest_map:
            studies.update(forest_map[outcome])
            evidence_sources.append("forest_plot_data")
        if not studies and outcome in grade_map:
            studies.update(grade_map[outcome])
            evidence_sources.append("grade_evidence_profile")
        if not studies and outcome in extraction_map:
            studies.update(extraction_map[outcome])
            evidence_sources.append("extraction_template")

        records.append(
            {
                "outcome": outcome,
                "studies_used": sorted(studies),
                "evidence_sources": evidence_sources,
            }
        )

    return records


def build_lineage_payload(
    *,
    forest_path: Path,
    meta_path: Path,
    publication_bias_path: Path,
    grade_path: Path,
    extraction_path: Path,
) -> dict[str, object]:
    forest_df = read_csv_or_empty(forest_path)
    meta_df = read_csv_or_empty(meta_path)
    publication_bias_df = read_csv_or_empty(publication_bias_path)
    grade_df = read_csv_or_empty(grade_path)
    extraction_df = read_csv_or_empty(extraction_path)

    extraction_study_map, extraction_outcome_map = extraction_outcome_maps(extraction_df)
    forest_map = forest_outcome_to_studies(forest_df, extraction_study_to_outcomes=extraction_study_map)
    publication_bias_map = grouped_outcome_to_studies(publication_bias_df, outcome_column="outcome")
    grade_map = grouped_outcome_to_studies(grade_df, outcome_column="outcome_construct")

    payload: dict[str, object] = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "forest_plot": {
            "source_file": forest_path.as_posix(),
            "records": records_from_mapping(forest_map),
        },
        "meta_analysis": {
            "source_file": meta_path.as_posix(),
            "records": meta_records(
                meta_df,
                publication_bias_map=publication_bias_map,
                forest_map=forest_map,
                grade_map=grade_map,
                extraction_map=extraction_outcome_map,
            ),
        },
        "publication_bias": {
            "source_file": publication_bias_path.as_posix(),
            "records": records_from_mapping(publication_bias_map),
        },
        "grade_profile": {
            "source_file": grade_path.as_posix(),
            "records": records_from_mapping(grade_map),
        },
    }
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build analysis lineage JSON with per-outcome study IDs used in downstream analyses."
    )
    parser.add_argument(
        "--forest-input",
        default="outputs/forest_plot_data.csv",
        help="Path to forest plot data CSV",
    )
    parser.add_argument(
        "--meta-input",
        default="outputs/meta_analysis_results.csv",
        help="Path to meta-analysis results CSV",
    )
    parser.add_argument(
        "--publication-bias-input",
        default="outputs/publication_bias_data.csv",
        help="Path to publication-bias input data CSV",
    )
    parser.add_argument(
        "--grade-input",
        default="outputs/grade_evidence_profile.csv",
        help="Path to GRADE evidence profile CSV",
    )
    parser.add_argument(
        "--extraction-input",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction CSV",
    )
    parser.add_argument(
        "--output",
        default="outputs/analysis_lineage.json",
        help="Path to lineage JSON output",
    )
    args = parser.parse_args()

    forest_path = Path(args.forest_input)
    meta_path = Path(args.meta_input)
    publication_bias_path = Path(args.publication_bias_input)
    grade_path = Path(args.grade_input)
    extraction_path = Path(args.extraction_input)
    output_path = Path(args.output)

    lineage_payload = build_lineage_payload(
        forest_path=forest_path,
        meta_path=meta_path,
        publication_bias_path=publication_bias_path,
        grade_path=grade_path,
        extraction_path=extraction_path,
    )

    atomic_write_text(output_path, json.dumps(lineage_payload, ensure_ascii=False, indent=2) + "\n")
    print(f"Wrote: {output_path}")


if __name__ == "__main__":
    main()
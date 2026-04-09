import argparse
import json
import os
import tempfile
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from study_table import included_study_table, normalize
else:
    from .study_table import included_study_table, normalize

STUDY_COLUMNS = [
    "study_id",
    "first_author",
    "year",
    "country",
    "study_design",
    "sample_size",
    "predictor_construct",
    "outcome_construct",
    "consensus_status",
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


def read_csv_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def parse_int_or_none(value: object) -> int | None:
    text = normalize(value)
    if not text:
        return None
    numeric = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    parsed = int(float(numeric))
    return parsed if parsed >= 0 else None


def normalized_counter(values: pd.Series) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for value in values.tolist():
        text = normalize(value)
        if not text:
            continue
        counts[text] += 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0].lower())))


def top_pair_counter(frame: pd.DataFrame, left: str, right: str) -> list[dict[str, object]]:
    counts: Counter[tuple[str, str]] = Counter()
    for _, row in frame.iterrows():
        left_value = normalize(row.get(left, ""))
        right_value = normalize(row.get(right, ""))
        if not left_value or not right_value:
            continue
        counts[(left_value, right_value)] += 1

    ranked = sorted(
        counts.items(), key=lambda item: (-item[1], item[0][0].lower(), item[0][1].lower())
    )
    return [
        {"predictor_construct": predictor, "outcome_construct": outcome, "count": count}
        for (predictor, outcome), count in ranked
    ]


def sample_size_summary(studies_df: pd.DataFrame) -> dict[str, int | float | None]:
    parsed = [
        parse_int_or_none(value) for value in studies_df.get("sample_size", pd.Series(dtype=str))
    ]
    valid = [value for value in parsed if value is not None]
    if not valid:
        return {
            "studies_with_sample_size": 0,
            "min": None,
            "median": None,
            "max": None,
            "total_reported_participants": 0,
        }

    series = pd.Series(valid, dtype=float)
    return {
        "studies_with_sample_size": len(valid),
        "min": int(series.min()),
        "median": float(series.median()),
        "max": int(series.max()),
        "total_reported_participants": int(series.sum()),
    }


def build_descriptives_payload(studies_df: pd.DataFrame) -> dict[str, object]:
    included_count = int(studies_df.shape[0])
    payload: dict[str, object] = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "included_study_count": included_count,
        "missing_counts": {
            "year": int(studies_df["year"].fillna("").astype(str).str.strip().eq("").sum())
            if "year" in studies_df.columns
            else 0,
            "country": int(studies_df["country"].fillna("").astype(str).str.strip().eq("").sum())
            if "country" in studies_df.columns
            else 0,
            "study_design": int(
                studies_df["study_design"].fillna("").astype(str).str.strip().eq("").sum()
            )
            if "study_design" in studies_df.columns
            else 0,
            "sample_size": int(
                studies_df["sample_size"].fillna("").astype(str).str.strip().eq("").sum()
            )
            if "sample_size" in studies_df.columns
            else 0,
        },
        "distributions": {
            "year": normalized_counter(studies_df.get("year", pd.Series(dtype=str))),
            "country": normalized_counter(studies_df.get("country", pd.Series(dtype=str))),
            "study_design": normalized_counter(
                studies_df.get("study_design", pd.Series(dtype=str))
            ),
            "predictor_construct": normalized_counter(
                studies_df.get("predictor_construct", pd.Series(dtype=str))
            ),
            "outcome_construct": normalized_counter(
                studies_df.get("outcome_construct", pd.Series(dtype=str))
            ),
        },
        "sample_size_summary": sample_size_summary(studies_df),
        "predictor_outcome_pairs": top_pair_counter(
            studies_df, "predictor_construct", "outcome_construct"
        )[:10],
    }
    return payload


def build_markdown(payload: dict[str, object], *, extraction_path: Path) -> str:
    lines: list[str] = []
    lines.append("# Review Descriptives")
    lines.append("")
    lines.append(f"Generated: {payload['generated_at']}")
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    lines.append(f"- Extraction source: `{extraction_path.as_posix()}`")
    lines.append(f"- Included studies: {payload['included_study_count']}")
    lines.append("")
    lines.append("## Missingness")
    lines.append("")
    for field_name, count in payload["missing_counts"].items():
        lines.append(f"- {field_name}: {count}")
    lines.append("")
    lines.append("## Distributions")
    lines.append("")

    distributions = payload["distributions"]
    for section_name in [
        "year",
        "country",
        "study_design",
        "predictor_construct",
        "outcome_construct",
    ]:
        lines.append(f"### {section_name.replace('_', ' ').title()}")
        lines.append("")
        section = distributions[section_name]
        if not section:
            lines.append("- No non-empty values available.")
        else:
            for key, count in section.items():
                lines.append(f"- {key}: {count}")
        lines.append("")

    sample = payload["sample_size_summary"]
    lines.append("## Sample Size Summary")
    lines.append("")
    lines.append(f"- Studies with sample size: {sample['studies_with_sample_size']}")
    lines.append(f"- Min: {sample['min']}")
    lines.append(f"- Median: {sample['median']}")
    lines.append(f"- Max: {sample['max']}")
    lines.append(f"- Total reported participants: {sample['total_reported_participants']}")
    lines.append("")

    lines.append("## Predictor x Outcome Pairs")
    lines.append("")
    pairs = payload["predictor_outcome_pairs"]
    if not pairs:
        lines.append("- No non-empty predictor/outcome pairs available.")
    else:
        for pair in pairs:
            lines.append(
                f"- {pair['predictor_construct']} x {pair['outcome_construct']}: {pair['count']}"
            )
    lines.append("")
    return "\n".join(lines)


def build_study_view(extraction_df: pd.DataFrame) -> pd.DataFrame:
    return included_study_table(extraction_df, STUDY_COLUMNS)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build descriptive analytics artifacts for the current review corpus."
    )
    parser.add_argument(
        "--extraction-input",
        default="../02_data/processed/extraction.csv",
        help="Path to extraction CSV.",
    )
    parser.add_argument(
        "--json-output",
        default="../outputs/review_descriptives.json",
        help="Path to machine-readable descriptives JSON output.",
    )
    parser.add_argument(
        "--markdown-output",
        default="../outputs/review_descriptives.md",
        help="Path to markdown descriptives summary output.",
    )
    args = parser.parse_args()

    extraction_path = Path(args.extraction_input)
    extraction_df = read_csv_or_empty(extraction_path)
    studies_df = build_study_view(extraction_df)
    payload = build_descriptives_payload(studies_df)
    markdown = build_markdown(payload, extraction_path=extraction_path)

    atomic_write_text(
        Path(args.json_output), json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    )
    atomic_write_text(Path(args.markdown_output), markdown)

    print(f"Wrote: {Path(args.json_output)}")
    print(f"Wrote: {Path(args.markdown_output)}")
    print(f"Included studies: {payload['included_study_count']}")


if __name__ == "__main__":
    main()

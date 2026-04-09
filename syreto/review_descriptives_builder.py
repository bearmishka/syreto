import argparse
import json
import os
import tempfile
from collections import Counter
from datetime import datetime
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
from matplotlib import pyplot as plt

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

DEFAULT_FIGURE_OUTPUTS = {
    "year": Path("../outputs/figures/year_distribution.png"),
    "study_design": Path("../outputs/figures/study_design_distribution.png"),
    "country": Path("../outputs/figures/country_distribution.png"),
    "quality_band": Path("../outputs/figures/quality_band_distribution.png"),
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
        "figure_outputs": {},
    }
    return payload


def quality_band_distribution(quality_df: pd.DataFrame) -> dict[str, int]:
    if quality_df.empty or "quality_band" not in quality_df.columns:
        return {}
    return normalized_counter(quality_df["quality_band"])


def render_distribution_figure(
    distribution: dict[str, int], *, title: str, xlabel: str, output_path: Path
) -> bool:
    if not distribution:
        return False

    output_path.parent.mkdir(parents=True, exist_ok=True)
    labels = list(distribution.keys())
    counts = list(distribution.values())

    fig_width = max(6.5, min(12.0, 1.1 * len(labels)))
    fig, ax = plt.subplots(figsize=(fig_width, 4.5))
    bars = ax.bar(range(len(labels)), counts, color="#355070", edgecolor="#1f2a44")
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Studies")
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=35, ha="right")
    ax.grid(axis="y", alpha=0.25, linestyle="--")
    ax.set_axisbelow(True)

    for bar, count in zip(bars, counts, strict=False):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            str(count),
            ha="center",
            va="bottom",
            fontsize=9,
        )

    fig.tight_layout()
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return True


def render_figures(
    payload: dict[str, object],
    *,
    year_output: Path,
    study_design_output: Path,
    country_output: Path,
    quality_band_output: Path,
) -> dict[str, str]:
    distributions = payload["distributions"]
    rendered: dict[str, str] = {}

    figure_specs = (
        ("year", "Included Studies by Publication Year", "Publication year", year_output),
        (
            "study_design",
            "Included Studies by Design",
            "Study design",
            study_design_output,
        ),
        ("country", "Included Studies by Country", "Country", country_output),
        (
            "quality_band",
            "Included Studies by Quality Band",
            "Quality band",
            quality_band_output,
        ),
    )

    for key, title, xlabel, output_path in figure_specs:
        distribution = distributions.get(key, {})
        if render_distribution_figure(
            distribution, title=title, xlabel=xlabel, output_path=output_path
        ):
            rendered[key] = output_path.as_posix()

    payload["figure_outputs"] = rendered
    return rendered


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

    lines.append("## Figures")
    lines.append("")
    figure_outputs = payload.get("figure_outputs", {})
    if not figure_outputs:
        lines.append("- No figures were generated from the current review state.")
    else:
        for figure_name, figure_path in figure_outputs.items():
            lines.append(f"- {figure_name}: `{figure_path}`")
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
    parser.add_argument(
        "--year-figure-output",
        default=str(DEFAULT_FIGURE_OUTPUTS["year"]),
        help="Path to year-distribution PNG output.",
    )
    parser.add_argument(
        "--study-design-figure-output",
        default=str(DEFAULT_FIGURE_OUTPUTS["study_design"]),
        help="Path to study-design distribution PNG output.",
    )
    parser.add_argument(
        "--country-figure-output",
        default=str(DEFAULT_FIGURE_OUTPUTS["country"]),
        help="Path to country-distribution PNG output.",
    )
    parser.add_argument(
        "--quality-input",
        default="../outputs/quality_appraisal_scored.csv",
        help="Path to scored quality appraisal CSV for optional quality-band descriptives.",
    )
    parser.add_argument(
        "--quality-band-figure-output",
        default=str(DEFAULT_FIGURE_OUTPUTS["quality_band"]),
        help="Path to quality-band distribution PNG output.",
    )
    args = parser.parse_args()

    extraction_path = Path(args.extraction_input)
    extraction_df = read_csv_or_empty(extraction_path)
    studies_df = build_study_view(extraction_df)
    payload = build_descriptives_payload(studies_df)
    quality_df = read_csv_or_empty(Path(args.quality_input))
    payload["distributions"]["quality_band"] = quality_band_distribution(quality_df)
    render_figures(
        payload,
        year_output=Path(args.year_figure_output),
        study_design_output=Path(args.study_design_figure_output),
        country_output=Path(args.country_figure_output),
        quality_band_output=Path(args.quality_band_figure_output),
    )
    markdown = build_markdown(payload, extraction_path=extraction_path)

    atomic_write_text(
        Path(args.json_output), json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    )
    atomic_write_text(Path(args.markdown_output), markdown)

    print(f"Wrote: {Path(args.json_output)}")
    print(f"Wrote: {Path(args.markdown_output)}")
    for figure_path in payload["figure_outputs"].values():
        print(f"Wrote: {figure_path}")
    print(f"Included studies: {payload['included_study_count']}")


if __name__ == "__main__":
    main()

import argparse
from datetime import datetime
import os
from pathlib import Path
import tempfile

import pandas as pd


ITEM_COLUMNS = [f"item_{index:02d}" for index in range(1, 12)]
APPRAISAL_COLUMNS = [
    "study_id",
    "study_design",
    "jbi_tool",
    *ITEM_COLUMNS,
    "appraiser_id",
    "checked_by",
    "appraisal_notes",
]
ALLOWED_RESPONSES = {"yes", "no", "unclear", "na"}

TOOL_CONFIG = {
    "cross_sectional": {
        "label": "jbi_analytical_cross_sectional",
        "items": 8,
        "aliases": {
            "cross_sectional",
            "cross-sectional",
            "analytical_cross_sectional",
            "jbi_cross_sectional",
            "jbi_analytical_cross_sectional",
        },
    },
    "case_control": {
        "label": "jbi_case_control",
        "items": 10,
        "aliases": {
            "case_control",
            "case-control",
            "jbi_case_control",
            "jbi_case-control",
        },
    },
    "cohort": {
        "label": "jbi_cohort",
        "items": 11,
        "aliases": {
            "cohort",
            "jbi_cohort",
        },
    },
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


def atomic_write_dataframe_csv(frame: pd.DataFrame, path: Path, *, index: bool = False) -> None:
    csv_text = frame.to_csv(index=index)
    atomic_write_text(path, csv_text)


def normalize(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() == "nan" else text


def normalize_lower(value: object) -> str:
    return normalize(value).lower()


def is_empty(value: object) -> bool:
    text = normalize_lower(value)
    return text in {"", "nan"}


def read_csv_or_empty(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)

    try:
        df = pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=columns)

    for column in columns:
        if column not in df.columns:
            df[column] = ""

    ordered = columns + [column for column in df.columns if column not in columns]
    return df[ordered]


def detect_tool_from_design(study_design: str) -> str | None:
    text = normalize_lower(study_design)
    if not text:
        return None

    if "case" in text and "control" in text:
        return "case_control"
    if "cross" in text:
        return "cross_sectional"
    if (
        "cohort" in text
        or "longitudinal" in text
        or "prospective" in text
        or "retrospective" in text
    ):
        return "cohort"

    return None


def parse_tool_key(raw_tool: str, study_design: str) -> str | None:
    text = normalize_lower(raw_tool).replace(" ", "_")

    if text:
        for key, config in TOOL_CONFIG.items():
            if text == key or text in config["aliases"]:
                return key

    return detect_tool_from_design(study_design)


def build_extraction_study_map(extraction_df: pd.DataFrame) -> dict[str, str]:
    study_map: dict[str, str] = {}
    if extraction_df.empty or "study_id" not in extraction_df.columns:
        return study_map

    for _, row in extraction_df.iterrows():
        study_id = normalize(row.get("study_id", ""))
        if not study_id:
            continue

        study_design = normalize(row.get("study_design", ""))
        if study_id not in study_map or (not study_map[study_id] and study_design):
            study_map[study_id] = study_design

    return study_map


def ensure_appraisal_rows(
    appraisal_df: pd.DataFrame, extraction_study_map: dict[str, str]
) -> pd.DataFrame:
    working = appraisal_df.copy()

    for study_id, study_design in extraction_study_map.items():
        mask = working["study_id"].fillna("").astype(str).str.strip().eq(study_id)
        if not mask.any():
            row_data = {column: "" for column in working.columns}
            row_data["study_id"] = study_id
            row_data["study_design"] = study_design
            detected = detect_tool_from_design(study_design)
            row_data["jbi_tool"] = TOOL_CONFIG[detected]["label"] if detected else ""
            working = pd.concat([working, pd.DataFrame([row_data])], ignore_index=True)
            continue

        row_index = int(working.index[mask][-1])
        if is_empty(working.loc[row_index, "study_design"]) and study_design:
            working.loc[row_index, "study_design"] = study_design

        detected = detect_tool_from_design(normalize(working.loc[row_index, "study_design"]))
        if is_empty(working.loc[row_index, "jbi_tool"]) and detected:
            working.loc[row_index, "jbi_tool"] = TOOL_CONFIG[detected]["label"]

    return working


def add_issue(
    issues: list[dict],
    *,
    level: str,
    study_id: str,
    row: int,
    column: str,
    message: str,
    value: str = "",
) -> None:
    issues.append(
        {
            "level": level,
            "study_id": study_id,
            "row": row,
            "column": column,
            "message": message,
            "value": value,
        }
    )


def score_row(row: pd.Series, row_number: int, issues: list[dict]) -> dict[str, object]:
    study_id = normalize(row.get("study_id", ""))
    study_design = normalize(row.get("study_design", ""))
    tool_key = parse_tool_key(normalize(row.get("jbi_tool", "")), study_design)

    if not study_id:
        add_issue(
            issues,
            level="warning",
            study_id="",
            row=row_number,
            column="study_id",
            message="Row has empty study_id and will be skipped.",
        )
        return {}

    if tool_key is None:
        add_issue(
            issues,
            level="warning",
            study_id=study_id,
            row=row_number,
            column="jbi_tool",
            message="Unsupported or ambiguous study_design for JBI mapping.",
            value=study_design,
        )
        expected_items = 0
        tool_label = "unsupported"
    else:
        expected_items = int(TOOL_CONFIG[tool_key]["items"])
        tool_label = str(TOOL_CONFIG[tool_key]["label"])

    yes_count = 0
    no_count = 0
    unclear_count = 0
    na_count = 0
    missing_count = 0
    normalized_items: dict[str, str] = {}

    for index, column in enumerate(ITEM_COLUMNS, start=1):
        raw_value = normalize(row.get(column, ""))
        value_norm = normalize_lower(raw_value)
        normalized_items[column] = value_norm

        if value_norm and value_norm not in ALLOWED_RESPONSES:
            add_issue(
                issues,
                level="error",
                study_id=study_id,
                row=row_number,
                column=column,
                message="Invalid checklist value. Use yes/no/unclear/na.",
                value=raw_value,
            )
            continue

        if expected_items == 0:
            if value_norm and value_norm != "na":
                add_issue(
                    issues,
                    level="warning",
                    study_id=study_id,
                    row=row_number,
                    column=column,
                    message="Checklist item provided for unsupported design mapping.",
                    value=raw_value,
                )
            continue

        if index > expected_items:
            if value_norm and value_norm != "na":
                add_issue(
                    issues,
                    level="warning",
                    study_id=study_id,
                    row=row_number,
                    column=column,
                    message=f"Item is outside expected range for {tool_label} (1-{expected_items}).",
                    value=raw_value,
                )
            continue

        if not value_norm:
            missing_count += 1
            add_issue(
                issues,
                level="warning",
                study_id=study_id,
                row=row_number,
                column=column,
                message="Checklist item is missing.",
            )
            continue

        if value_norm == "yes":
            yes_count += 1
        elif value_norm == "no":
            no_count += 1
        elif value_norm == "unclear":
            unclear_count += 1
        elif value_norm == "na":
            na_count += 1

    denominator = yes_count + no_count + unclear_count
    score_pct: float | None
    quality_band: str

    if denominator <= 0:
        score_pct = None
        quality_band = "not_scored"
    else:
        score_pct = (100.0 * yes_count) / denominator
        if score_pct >= 75.0:
            quality_band = "high"
        elif score_pct >= 50.0:
            quality_band = "moderate"
        else:
            quality_band = "low"

    completion_pct = None
    if expected_items > 0:
        answered = yes_count + no_count + unclear_count + na_count
        completion_pct = (100.0 * answered) / expected_items

    output = {
        "study_id": study_id,
        "study_design": study_design,
        "jbi_tool": tool_label,
        "item_count_expected": expected_items,
        **normalized_items,
        "yes_count": yes_count,
        "no_count": no_count,
        "unclear_count": unclear_count,
        "na_count": na_count,
        "missing_count": missing_count,
        "score_numerator": yes_count,
        "score_denominator": denominator,
        "score_pct": "" if score_pct is None else f"{score_pct:.1f}",
        "quality_band": quality_band,
        "completion_pct": "" if completion_pct is None else f"{completion_pct:.1f}",
        "appraiser_id": normalize(row.get("appraiser_id", "")),
        "checked_by": normalize(row.get("checked_by", "")),
        "appraisal_notes": normalize(row.get("appraisal_notes", "")),
    }
    return output


def format_quality_summary(row: pd.Series) -> str:
    tool = normalize(row.get("jbi_tool", ""))
    yes_count = normalize(row.get("yes_count", "0"))
    no_count = normalize(row.get("no_count", "0"))
    unclear_count = normalize(row.get("unclear_count", "0"))
    na_count = normalize(row.get("na_count", "0"))
    denominator = normalize(row.get("score_denominator", "0"))
    score_pct = normalize(row.get("score_pct", ""))
    quality_band = normalize(row.get("quality_band", "not_scored"))

    if tool == "unsupported":
        design = normalize(row.get("study_design", ""))
        return f"jbi=unsupported|study_design={design or 'NA'}"

    if score_pct:
        score_text = f"{score_pct}%"
    else:
        score_text = "NA"

    return (
        f"jbi={tool}|yes={yes_count}|no={no_count}|unclear={unclear_count}|na={na_count}|"
        f"score={score_text} ({yes_count}/{denominator})|band={quality_band}"
    )


def sync_extraction_quality(
    extraction_df: pd.DataFrame,
    scored_df: pd.DataFrame,
) -> pd.DataFrame:
    if (
        extraction_df.empty
        or "study_id" not in extraction_df.columns
        or "quality_appraisal" not in extraction_df.columns
    ):
        return extraction_df

    score_map: dict[str, str] = {}
    for _, scored_row in scored_df.iterrows():
        study_id = normalize(scored_row.get("study_id", ""))
        if not study_id:
            continue
        score_map[study_id] = format_quality_summary(scored_row)

    updated = extraction_df.copy()
    for index, row in updated.iterrows():
        study_id = normalize(row.get("study_id", ""))
        if not study_id:
            continue
        if study_id in score_map:
            updated.loc[index, "quality_appraisal"] = score_map[study_id]

    return updated


def aggregate_scores(scored_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "study_design",
        "jbi_tool",
        "studies_total",
        "scored_studies",
        "mean_score_pct",
        "median_score_pct",
        "min_score_pct",
        "max_score_pct",
        "high_n",
        "moderate_n",
        "low_n",
        "not_scored_n",
    ]
    if scored_df.empty:
        return pd.DataFrame(columns=columns)

    working = scored_df.copy()
    working["study_design"] = working["study_design"].fillna("").astype(str).str.strip()
    working["jbi_tool"] = working["jbi_tool"].fillna("").astype(str).str.strip()
    working["quality_band"] = working["quality_band"].fillna("").astype(str).str.strip().str.lower()
    working["score_pct_num"] = pd.to_numeric(working["score_pct"], errors="coerce")

    rows: list[dict[str, object]] = []
    grouped = working.groupby(["study_design", "jbi_tool"], dropna=False, sort=True)
    for (study_design, jbi_tool), frame in grouped:
        scores = frame["score_pct_num"].dropna()
        rows.append(
            {
                "study_design": study_design,
                "jbi_tool": jbi_tool,
                "studies_total": int(frame.shape[0]),
                "scored_studies": int(scores.shape[0]),
                "mean_score_pct": "" if scores.empty else f"{scores.mean():.1f}",
                "median_score_pct": "" if scores.empty else f"{scores.median():.1f}",
                "min_score_pct": "" if scores.empty else f"{scores.min():.1f}",
                "max_score_pct": "" if scores.empty else f"{scores.max():.1f}",
                "high_n": int(frame["quality_band"].eq("high").sum()),
                "moderate_n": int(frame["quality_band"].eq("moderate").sum()),
                "low_n": int(frame["quality_band"].eq("low").sum()),
                "not_scored_n": int(frame["quality_band"].eq("not_scored").sum()),
            }
        )

    aggregated = pd.DataFrame(rows, columns=columns)
    return aggregated.sort_values(by=["study_design", "jbi_tool"], kind="stable").reset_index(
        drop=True
    )


def build_summary(
    *,
    scored_df: pd.DataFrame,
    aggregate_df: pd.DataFrame,
    issues: list[dict],
    appraisal_input_path: Path,
    scored_output_path: Path,
    aggregate_output_path: Path,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    errors = [item for item in issues if item["level"] == "error"]
    warnings = [item for item in issues if item["level"] == "warning"]

    lines: list[str] = []
    lines.append("# Quality Appraisal Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Appraisal input: `{appraisal_input_path.as_posix()}`")
    lines.append(f"- Scored output: `{scored_output_path.as_posix()}`")
    lines.append(f"- Aggregate output: `{aggregate_output_path.as_posix()}`")
    lines.append(f"- Studies appraised: {int(scored_df.shape[0])}")

    lines.append("")
    lines.append("## Totals")
    lines.append("")
    lines.append(f"- Errors: {len(errors)}")
    lines.append(f"- Warnings: {len(warnings)}")

    lines.append("")
    lines.append("## Aggregated Table")
    lines.append("")
    if aggregate_df.empty:
        lines.append("- No appraisal rows available for aggregation.")
    else:
        lines.append(
            "| study_design | jbi_tool | studies | scored | mean | median | min | max | high/moderate/low/not_scored |"
        )
        lines.append("|---|---|---:|---:|---:|---:|---:|---:|---|")
        for _, row in aggregate_df.iterrows():
            distribution = (
                f"{normalize(row.get('high_n', 0))}/"
                f"{normalize(row.get('moderate_n', 0))}/"
                f"{normalize(row.get('low_n', 0))}/"
                f"{normalize(row.get('not_scored_n', 0))}"
            )
            lines.append(
                "| "
                f"{normalize(row.get('study_design', '')) or 'NA'} | "
                f"{normalize(row.get('jbi_tool', '')) or 'NA'} | "
                f"{normalize(row.get('studies_total', 0))} | "
                f"{normalize(row.get('scored_studies', 0))} | "
                f"{(normalize(row.get('mean_score_pct', '')) + '%') if normalize(row.get('mean_score_pct', '')) else 'NA'} | "
                f"{(normalize(row.get('median_score_pct', '')) + '%') if normalize(row.get('median_score_pct', '')) else 'NA'} | "
                f"{(normalize(row.get('min_score_pct', '')) + '%') if normalize(row.get('min_score_pct', '')) else 'NA'} | "
                f"{(normalize(row.get('max_score_pct', '')) + '%') if normalize(row.get('max_score_pct', '')) else 'NA'} | "
                f"{distribution} |"
            )

    if not scored_df.empty:
        lines.append("")
        lines.append("## Study Scores")
        lines.append("")
        lines.append("| study_id | study_design | jbi_tool | yes/no/unclear/na | score | band |")
        lines.append("|---|---|---|---|---:|---|")
        for _, row in scored_df.sort_values(by=["study_id"], kind="stable").iterrows():
            score_text = normalize(row.get("score_pct", ""))
            lines.append(
                "| "
                f"{normalize(row.get('study_id', ''))} | "
                f"{normalize(row.get('study_design', ''))} | "
                f"{normalize(row.get('jbi_tool', ''))} | "
                f"{normalize(row.get('yes_count', '0'))}/{normalize(row.get('no_count', '0'))}/{normalize(row.get('unclear_count', '0'))}/{normalize(row.get('na_count', '0'))} | "
                f"{(score_text + '%') if score_text else 'NA'} | "
                f"{normalize(row.get('quality_band', ''))} |"
            )

    lines.append("")
    lines.append("## Issues")
    lines.append("")
    if issues:
        lines.append("| study_id | row | level | column | message | value |")
        lines.append("|---|---:|---|---|---|---|")
        for issue in issues:
            value = normalize(issue.get("value", "")).replace("|", "\\|")
            lines.append(
                f"| {normalize(issue.get('study_id', ''))} | {issue['row']} | {issue['level']} | `{issue['column']}` | {issue['message']} | `{value}` |"
            )
    else:
        lines.append("- ✅ No issues found.")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append(
        "- JBI tool mapping is inferred from `study_design` (cross-sectional, case-control, cohort)."
    )
    lines.append("- Scoring rule: `yes=1`, `no=0`, `unclear=0`, `na` excluded from denominator.")
    lines.append("- Quality bands: high (>=75%), moderate (50-74.9%), low (<50%).")

    return "\n".join(lines) + "\n"


def should_fail(fail_on: str, error_count: int, warning_count: int) -> bool:
    mode = normalize_lower(fail_on)
    if mode == "none":
        return False
    if mode == "warning":
        return (error_count + warning_count) > 0
    return error_count > 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apply structured JBI quality appraisal by study design and compute final scores."
    )
    parser.add_argument(
        "--extraction",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction template CSV",
    )
    parser.add_argument(
        "--appraisal-input",
        default="../02_data/codebook/quality_appraisal_template.csv",
        help="Path to structured quality appraisal checklist CSV",
    )
    parser.add_argument(
        "--scored-output",
        default="outputs/quality_appraisal_scored.csv",
        help="Path to scored quality appraisal CSV output",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/quality_appraisal_summary.md",
        help="Path to markdown quality appraisal summary",
    )
    parser.add_argument(
        "--aggregate-output",
        default="outputs/quality_appraisal_aggregate.csv",
        help="Path to aggregated quality appraisal table",
    )
    parser.add_argument(
        "--sync-extraction",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Sync computed score summary back into extraction `quality_appraisal` column",
    )
    parser.add_argument(
        "--fail-on",
        default="error",
        choices=["none", "warning", "error"],
        help="Fail mode: error (default), warning, none",
    )
    args = parser.parse_args()

    extraction_path = Path(args.extraction)
    appraisal_input_path = Path(args.appraisal_input)
    scored_output_path = Path(args.scored_output)
    summary_output_path = Path(args.summary_output)
    aggregate_output_path = Path(args.aggregate_output)

    if not extraction_path.exists():
        raise FileNotFoundError(f"Extraction file not found: {extraction_path}")

    extraction_df = read_csv_or_empty(extraction_path, columns=[])
    if "study_id" not in extraction_df.columns:
        raise ValueError("Extraction file must include `study_id` column.")
    if "study_design" not in extraction_df.columns:
        raise ValueError("Extraction file must include `study_design` column.")
    if "quality_appraisal" not in extraction_df.columns:
        raise ValueError("Extraction file must include `quality_appraisal` column.")

    appraisal_df = read_csv_or_empty(appraisal_input_path, columns=APPRAISAL_COLUMNS)
    extraction_study_map = build_extraction_study_map(extraction_df)
    appraisal_df = ensure_appraisal_rows(appraisal_df, extraction_study_map)

    issues: list[dict] = []
    scored_rows: list[dict[str, object]] = []
    for index, row in appraisal_df.iterrows():
        row_number = int(index) + 2
        scored = score_row(row, row_number=row_number, issues=issues)
        if scored:
            scored_rows.append(scored)

    scored_df = pd.DataFrame(scored_rows)
    if scored_df.empty:
        scored_df = pd.DataFrame(
            columns=[
                "study_id",
                "study_design",
                "jbi_tool",
                "item_count_expected",
                *ITEM_COLUMNS,
                "yes_count",
                "no_count",
                "unclear_count",
                "na_count",
                "missing_count",
                "score_numerator",
                "score_denominator",
                "score_pct",
                "quality_band",
                "completion_pct",
                "appraiser_id",
                "checked_by",
                "appraisal_notes",
            ]
        )

    atomic_write_dataframe_csv(appraisal_df, appraisal_input_path, index=False)

    atomic_write_dataframe_csv(scored_df, scored_output_path, index=False)

    aggregate_df = aggregate_scores(scored_df)
    atomic_write_dataframe_csv(aggregate_df, aggregate_output_path, index=False)

    if args.sync_extraction:
        updated_extraction = sync_extraction_quality(extraction_df, scored_df)
        atomic_write_dataframe_csv(updated_extraction, extraction_path, index=False)

    summary_text = build_summary(
        scored_df=scored_df,
        aggregate_df=aggregate_df,
        issues=issues,
        appraisal_input_path=appraisal_input_path,
        scored_output_path=scored_output_path,
        aggregate_output_path=aggregate_output_path,
    )
    atomic_write_text(summary_output_path, summary_text)

    error_count = sum(1 for issue in issues if issue["level"] == "error")
    warning_count = sum(1 for issue in issues if issue["level"] == "warning")

    print(f"Updated: {appraisal_input_path}")
    print(f"Wrote: {scored_output_path}")
    print(f"Wrote: {aggregate_output_path}")
    print(f"Wrote: {summary_output_path}")
    if args.sync_extraction:
        print(f"Updated: {extraction_path}")
    print(f"Validation issues: errors={error_count}, warnings={warning_count}")

    if should_fail(args.fail_on, error_count=error_count, warning_count=warning_count):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

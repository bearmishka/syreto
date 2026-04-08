import argparse
import math
import os
import tempfile
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

REQUIRED_COLUMNS = [
    "study_id",
    "first_author",
    "year",
    "main_effect_metric",
    "main_effect_value",
    "effect_direction",
    "adjusted_unadjusted",
    "model_type",
]

OPTIONAL_CI_COLUMNS = [
    "ci_lower",
    "ci_upper",
]

METRIC_ALIASES = {
    "r": {
        "r",
        "pearson_r",
        "pearsonr",
        "corr",
        "correlation",
        "correlation_coefficient",
        "rho",
    },
    "d": {
        "d",
        "cohen_d",
        "cohens_d",
        "cohen_s_d",
        "hedges_g",
        "g",
        "smd",
        "standardized_mean_difference",
    },
    "or": {
        "or",
        "odds_ratio",
        "oddsratio",
    },
    "eta2": {
        "eta2",
        "eta_2",
        "eta^2",
        "eta_squared",
        "eta_sq",
        "eta²",
        "partial_eta2",
        "partial_eta_2",
        "partial_eta^2",
        "partial_eta_squared",
        "partial_eta_sq",
        "partial_eta²",
    },
}

LEGACY_TO_GENERIC_COLUMN_MAP = {
    "author": "first_author",
    "effect_measure": "main_effect_metric",
    "effect_metric": "main_effect_metric",
    "effect_value": "main_effect_value",
    "adjustment": "adjusted_unadjusted",
    "adjustment_status": "adjusted_unadjusted",
    "analysis_model": "model_type",
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


def harmonize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    working = df.copy()

    for legacy, generic in LEGACY_TO_GENERIC_COLUMN_MAP.items():
        if legacy not in working.columns:
            continue

        if generic not in working.columns:
            working[generic] = working[legacy]
            continue

        generic_values = working[generic].fillna("").astype(str).str.strip()
        legacy_values = working[legacy].fillna("").astype(str).str.strip()
        fill_mask = (generic_values == "") & (legacy_values != "")
        if fill_mask.any():
            working.loc[fill_mask, generic] = working.loc[fill_mask, legacy]

    return working


def is_missing(value: object) -> bool:
    return normalize_lower(value) in MISSING_CODES


def canonical_metric(raw_metric: object) -> str | None:
    text = normalize_lower(raw_metric)
    if not text:
        return None

    normalized = text.replace("-", "_").replace(" ", "_").replace("²", "2")

    for metric_key, aliases in METRIC_ALIASES.items():
        if text in aliases or normalized in aliases:
            return metric_key

    if "odds" in normalized or normalized == "or":
        return "or"
    if "eta" in normalized and "2" in normalized:
        return "eta2"
    if "cohen" in normalized or "hedges" in normalized or normalized in {"d", "g", "smd"}:
        return "d"
    if normalized in {"r", "corr", "correlation", "rho"}:
        return "r"

    return None


def numeric_or_none(value: object) -> float | None:
    text = normalize(value)
    if is_missing(text):
        return None
    parsed = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


def signed_direction(effect_direction: str) -> int | None:
    direction = normalize_lower(effect_direction)
    if direction == "positive":
        return 1
    if direction == "negative":
        return -1
    return None


def metric_scale_error(metric_key: str, value: float) -> str | None:
    if metric_key == "r":
        if value < -1.0 or value > 1.0:
            return "`r` is out of valid range [-1, 1]."
        return None

    if metric_key == "or":
        if value <= 0.0:
            return "`OR` must be > 0."
        return None

    if metric_key == "eta2":
        if value < 0.0 or value > 1.0:
            return "`eta²` is out of valid range [0, 1]."
        return None

    return None


def add_issue(
    issues: list[dict],
    *,
    level: str,
    row: int,
    study_id: str,
    message: str,
    value: str = "",
) -> None:
    issues.append(
        {
            "level": level,
            "row": row,
            "study_id": study_id,
            "message": message,
            "value": value,
        }
    )


def convert_from_r(value_r: float) -> dict[str, float | None]:
    result: dict[str, float | None] = {
        "converted_r": value_r,
        "converted_d": None,
        "converted_or": None,
        "converted_eta2": value_r * value_r,
        "fisher_z": None,
    }

    if abs(value_r) < 1.0:
        result["converted_d"] = (2.0 * value_r) / math.sqrt(1.0 - (value_r * value_r))
        result["converted_or"] = math.exp(result["converted_d"] * math.pi / math.sqrt(3.0))
        result["fisher_z"] = math.atanh(value_r)

    return result


def convert_from_d(value_d: float) -> dict[str, float | None]:
    converted_r = value_d / math.sqrt((value_d * value_d) + 4.0)
    converted_or = math.exp(value_d * math.pi / math.sqrt(3.0))
    converted_eta2 = converted_r * converted_r

    return {
        "converted_r": converted_r,
        "converted_d": value_d,
        "converted_or": converted_or,
        "converted_eta2": converted_eta2,
        "fisher_z": math.atanh(converted_r),
    }


def convert_from_or(value_or: float) -> dict[str, float | None]:
    converted_d = math.log(value_or) * math.sqrt(3.0) / math.pi
    converted_r = converted_d / math.sqrt((converted_d * converted_d) + 4.0)
    converted_eta2 = converted_r * converted_r

    return {
        "converted_r": converted_r,
        "converted_d": converted_d,
        "converted_or": value_or,
        "converted_eta2": converted_eta2,
        "fisher_z": math.atanh(converted_r),
    }


def convert_from_eta2(value_eta2: float, direction_sign: int | None) -> dict[str, float | None]:
    if value_eta2 == 0:
        return {
            "converted_r": 0.0,
            "converted_d": 0.0,
            "converted_or": 1.0,
            "converted_eta2": 0.0,
            "fisher_z": 0.0,
        }

    if direction_sign is None:
        return {
            "converted_r": None,
            "converted_d": None,
            "converted_or": None,
            "converted_eta2": value_eta2,
            "fisher_z": None,
        }

    converted_r = float(direction_sign) * math.sqrt(value_eta2)
    return convert_from_r(converted_r) | {"converted_eta2": value_eta2}


def should_fail(mode: str, errors: int, warnings: int) -> bool:
    norm_mode = normalize_lower(mode)
    if norm_mode == "none":
        return False
    if norm_mode == "warning":
        return (errors + warnings) > 0
    return errors > 0


def build_summary(
    *,
    input_path: Path,
    output_path: Path,
    converted_df: pd.DataFrame,
    raw_rows: int,
    rows_with_effect_fields: int,
    status_counts: Counter,
    metric_counts: Counter,
    issues: list[dict],
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    errors = [issue for issue in issues if issue["level"] == "error"]
    warnings = [issue for issue in issues if issue["level"] == "warning"]

    lines: list[str] = []
    lines.append("# Effect Size Conversion Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Extraction input: `{input_path.as_posix()}`")
    lines.append(f"- Converted output: `{output_path.as_posix()}`")
    lines.append("")
    lines.append("## Counts")
    lines.append("")
    lines.append(f"- Raw rows in extraction: {raw_rows}")
    lines.append(f"- Rows with metric/value content: {rows_with_effect_fields}")
    lines.append(f"- Converted rows exported: {int(converted_df.shape[0])}")
    lines.append(f"- Conversion status `converted`: {int(status_counts.get('converted', 0))}")
    lines.append(f"- Conversion status `partial`: {int(status_counts.get('partial', 0))}")
    lines.append(f"- Conversion status `skipped`: {int(status_counts.get('skipped', 0))}")
    lines.append(f"- Conversion status `error`: {int(status_counts.get('error', 0))}")
    lines.append(f"- Issues: errors={len(errors)}, warnings={len(warnings)}")

    lines.append("")
    lines.append("## Source Metrics")
    lines.append("")
    if metric_counts:
        for metric_key in sorted(metric_counts.keys()):
            lines.append(f"- `{metric_key}`: {int(metric_counts[metric_key])}")
    else:
        lines.append("- No source metrics were parsed.")

    lines.append("")
    lines.append("## Conversion Rules")
    lines.append("")
    lines.append("- `d = 2r / sqrt(1 - r^2)`")
    lines.append("- `r = d / sqrt(d^2 + 4)`")
    lines.append("- `d = ln(OR) * sqrt(3) / pi` and `OR = exp(d * pi / sqrt(3))`")
    lines.append("- `eta^2 = r^2`; `r = ±sqrt(eta^2)` requires effect direction sign")
    lines.append("- `fisher_z = atanh(r)` when `|r| < 1`")
    lines.append(
        "- If CI bounds are present (`ci_lower`, `ci_upper`), they must use the same metric scale as `main_effect_metric`."
    )

    lines.append("")
    lines.append("## Issues")
    lines.append("")
    if issues:
        lines.append("| row | study_id | level | message | value |")
        lines.append("|---:|---|---|---|---|")
        for issue in issues:
            value_text = normalize(issue.get("value", "")).replace("|", "\\|")
            lines.append(
                f"| {issue['row']} | {normalize(issue.get('study_id', ''))} | {issue['level']} | {issue['message']} | `{value_text}` |"
            )
    else:
        lines.append("- ✅ No issues found.")

    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert effect sizes between r, d, OR, eta² for meta-analytic harmonization."
    )
    parser.add_argument(
        "--input",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction template CSV",
    )
    parser.add_argument(
        "--output",
        default="outputs/effect_size_converted.csv",
        help="Path to converted effect-size CSV",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/effect_size_conversion_summary.md",
        help="Path to markdown conversion summary",
    )
    parser.add_argument(
        "--fail-on",
        default="none",
        choices=["none", "warning", "error"],
        help="Fail mode: none (default), warning, error",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    summary_output_path = Path(args.summary_output)

    if not input_path.exists():
        raise FileNotFoundError(f"Extraction file not found: {input_path}")

    try:
        extraction_df = pd.read_csv(input_path, dtype=str)
    except pd.errors.EmptyDataError:
        extraction_df = pd.DataFrame(columns=REQUIRED_COLUMNS)

    extraction_df = harmonize_columns(extraction_df)

    for column in REQUIRED_COLUMNS:
        if column not in extraction_df.columns:
            extraction_df[column] = ""

    for column in OPTIONAL_CI_COLUMNS:
        if column not in extraction_df.columns:
            extraction_df[column] = ""

    issues: list[dict] = []
    converted_rows: list[dict] = []
    status_counts: Counter = Counter()
    metric_counts: Counter = Counter()

    raw_rows = int(extraction_df.shape[0])
    rows_with_effect_fields = 0

    for index, row in extraction_df.iterrows():
        row_number = int(index) + 2

        metric_raw = normalize(row.get("main_effect_metric", ""))
        value_raw = normalize(row.get("main_effect_value", ""))
        direction_raw = normalize(row.get("effect_direction", ""))

        if is_missing(metric_raw) and is_missing(value_raw):
            continue

        rows_with_effect_fields += 1

        study_id = normalize(row.get("study_id", ""))
        metric_key = canonical_metric(metric_raw)
        value_numeric = numeric_or_none(value_raw)

        output = {
            "row": row_number,
            "study_id": study_id,
            "first_author": normalize(row.get("first_author", "")),
            "year": normalize(row.get("year", "")),
            "main_effect_metric": metric_raw,
            "main_effect_value": value_raw,
            "effect_direction": direction_raw,
            "adjusted_unadjusted": normalize(row.get("adjusted_unadjusted", "")),
            "model_type": normalize(row.get("model_type", "")),
            "source_metric_canonical": metric_key or "",
            "source_value_numeric": "" if value_numeric is None else value_numeric,
            "converted_r": "",
            "converted_d": "",
            "converted_or": "",
            "converted_eta2": "",
            "fisher_z": "",
            "conversion_status": "",
            "conversion_notes": "",
        }

        if metric_key is None:
            output["conversion_status"] = "error"
            output["conversion_notes"] = "Unsupported metric label"
            add_issue(
                issues,
                level="error",
                row=row_number,
                study_id=study_id,
                message="Unsupported `main_effect_metric` for conversion.",
                value=metric_raw,
            )
            status_counts["error"] += 1
            converted_rows.append(output)
            continue

        metric_counts[metric_key] += 1

        if value_numeric is None:
            output["conversion_status"] = "error"
            output["conversion_notes"] = "Metric value is missing/non-numeric"
            add_issue(
                issues,
                level="error",
                row=row_number,
                study_id=study_id,
                message="`main_effect_value` is missing or non-numeric.",
                value=value_raw,
            )
            status_counts["error"] += 1
            converted_rows.append(output)
            continue

        metric_value_scale_error = metric_scale_error(metric_key, value_numeric)
        if metric_value_scale_error is not None:
            output["conversion_status"] = "error"
            output["conversion_notes"] = metric_value_scale_error
            add_issue(
                issues,
                level="error",
                row=row_number,
                study_id=study_id,
                message=metric_value_scale_error,
                value=value_raw,
            )
            status_counts["error"] += 1
            converted_rows.append(output)
            continue

        ci_scale_issues: list[tuple[str, str, str]] = []
        for ci_column in OPTIONAL_CI_COLUMNS:
            ci_raw = normalize(row.get(ci_column, ""))
            ci_numeric = numeric_or_none(ci_raw)
            if ci_numeric is None:
                continue
            ci_scale_error = metric_scale_error(metric_key, ci_numeric)
            if ci_scale_error is not None:
                ci_scale_issues.append((ci_column, ci_raw, ci_scale_error))

        if ci_scale_issues:
            output["conversion_status"] = "error"
            output["conversion_notes"] = "CI bounds are inconsistent with main_effect_metric scale"
            for ci_column, ci_raw, ci_scale_error in ci_scale_issues:
                add_issue(
                    issues,
                    level="error",
                    row=row_number,
                    study_id=study_id,
                    message=f"{ci_scale_error} (column: `{ci_column}`)",
                    value=ci_raw,
                )
            status_counts["error"] += 1
            converted_rows.append(output)
            continue

        conversion_notes: list[str] = []
        converted: dict[str, float | None]

        if metric_key == "r":
            converted = convert_from_r(value_numeric)
            if abs(value_numeric) == 1.0:
                conversion_notes.append("|r| = 1: d/OR/Fisher z are undefined")
                add_issue(
                    issues,
                    level="warning",
                    row=row_number,
                    study_id=study_id,
                    message="|r| = 1: d/OR/Fisher z cannot be computed.",
                    value=value_raw,
                )

        elif metric_key == "d":
            converted = convert_from_d(value_numeric)

        elif metric_key == "or":
            converted = convert_from_or(value_numeric)

        else:
            sign = signed_direction(direction_raw)
            converted = convert_from_eta2(value_numeric, sign)
            if value_numeric > 0 and sign is None:
                conversion_notes.append("eta² provided without positive/negative direction")
                add_issue(
                    issues,
                    level="warning",
                    row=row_number,
                    study_id=study_id,
                    message="Cannot derive signed r/d/OR from eta² without direction sign.",
                    value=direction_raw,
                )

        for column in ["converted_r", "converted_d", "converted_or", "converted_eta2", "fisher_z"]:
            value = converted.get(column)
            output[column] = "" if value is None else value

        has_primary_conversions = (
            output["converted_r"] != ""
            or output["converted_d"] != ""
            or output["converted_or"] != ""
        )
        if has_primary_conversions:
            if conversion_notes:
                output["conversion_status"] = "partial"
                status_counts["partial"] += 1
            else:
                output["conversion_status"] = "converted"
                status_counts["converted"] += 1
        elif output["converted_eta2"] != "" and conversion_notes:
            output["conversion_status"] = "partial"
            status_counts["partial"] += 1
        else:
            output["conversion_status"] = "skipped"
            status_counts["skipped"] += 1

        output["conversion_notes"] = "; ".join(conversion_notes)
        converted_rows.append(output)

    converted_df = pd.DataFrame(converted_rows)
    if converted_df.empty:
        converted_df = pd.DataFrame(
            columns=[
                "row",
                "study_id",
                "first_author",
                "year",
                "main_effect_metric",
                "main_effect_value",
                "effect_direction",
                "adjusted_unadjusted",
                "model_type",
                "source_metric_canonical",
                "source_value_numeric",
                "converted_r",
                "converted_d",
                "converted_or",
                "converted_eta2",
                "fisher_z",
                "conversion_status",
                "conversion_notes",
            ]
        )

    atomic_write_dataframe_csv(converted_df, output_path, index=False)

    summary_text = build_summary(
        input_path=input_path,
        output_path=output_path,
        converted_df=converted_df,
        raw_rows=raw_rows,
        rows_with_effect_fields=rows_with_effect_fields,
        status_counts=status_counts,
        metric_counts=metric_counts,
        issues=issues,
    )
    atomic_write_text(summary_output_path, summary_text)

    error_count = sum(1 for issue in issues if issue["level"] == "error")
    warning_count = sum(1 for issue in issues if issue["level"] == "warning")

    print(f"Wrote: {output_path}")
    print(f"Wrote: {summary_output_path}")
    print(
        "Conversion status counts: "
        + ", ".join(
            f"{key}={int(status_counts.get(key, 0))}"
            for key in ["converted", "partial", "skipped", "error"]
        )
    )
    print(f"Issues: errors={error_count}, warnings={warning_count}")

    if should_fail(args.fail_on, errors=error_count, warnings=warning_count):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

import argparse
from pathlib import Path

import pandas as pd

EMPTY_VALUES = {"", "nan", "none"}
PLAN_COLUMNS = [
    "reviewer",
    "current_records",
    "load_share_percent",
    "equal_share_target",
    "delta_to_equal_share",
    "suggested_next_batch",
]


def canonicalize_summary_text(text: str) -> str:
    stripped = text.strip("\n")
    if not stripped:
        return ""

    lines = stripped.splitlines()
    while len(lines) >= 2 and len(lines) % 2 == 0:
        half = len(lines) // 2
        if lines[:half] != lines[half:]:
            break
        lines = lines[:half]

    return "\n".join(lines) + "\n"


def clean_text(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() in EMPTY_VALUES else text


def parse_non_negative_int(value: object) -> int:
    text = clean_text(value)
    if not text:
        return 0
    numeric = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return 0
    return max(int(float(numeric)), 0)


def read_csv_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def build_workload_plan(screening_df: pd.DataFrame, *, stage: str) -> pd.DataFrame:
    if screening_df.empty:
        return pd.DataFrame(columns=PLAN_COLUMNS)

    working = screening_df.copy()
    if "reviewer" not in working.columns:
        return pd.DataFrame(columns=PLAN_COLUMNS)

    if "stage" in working.columns and stage:
        stage_mask = (
            working["stage"].fillna("").astype(str).str.strip().str.lower().eq(stage.lower())
        )
        filtered = working.loc[stage_mask].copy()
    else:
        filtered = working.copy()

    if filtered.empty:
        return pd.DataFrame(columns=PLAN_COLUMNS)

    filtered["reviewer"] = filtered["reviewer"].fillna("").astype(str).str.strip()

    if "records_screened" in filtered.columns:
        filtered["current_records"] = filtered["records_screened"].apply(parse_non_negative_int)
    else:
        filtered["current_records"] = 0

    filtered = filtered.loc[filtered["reviewer"].ne("")].copy()
    if filtered.empty:
        return pd.DataFrame(columns=PLAN_COLUMNS)

    grouped = (
        filtered.groupby("reviewer", as_index=False)["current_records"]
        .sum()
        .sort_values("reviewer", kind="stable")
    )

    total_records = int(grouped["current_records"].sum())
    reviewer_count = int(grouped.shape[0])
    equal_share_target = (
        int((total_records + reviewer_count - 1) / reviewer_count) if reviewer_count > 0 else 0
    )

    grouped["equal_share_target"] = equal_share_target
    grouped["delta_to_equal_share"] = grouped["equal_share_target"] - grouped["current_records"]
    grouped["suggested_next_batch"] = grouped["delta_to_equal_share"].clip(lower=0)

    if total_records > 0:
        grouped["load_share_percent"] = (grouped["current_records"] / total_records * 100).round(1)
    else:
        grouped["load_share_percent"] = 0.0

    grouped["load_share_percent"] = grouped["load_share_percent"].map(
        lambda value: f"{float(value):.1f}"
    )
    grouped["current_records"] = grouped["current_records"].astype(int)
    grouped["equal_share_target"] = grouped["equal_share_target"].astype(int)
    grouped["delta_to_equal_share"] = grouped["delta_to_equal_share"].astype(int)
    grouped["suggested_next_batch"] = grouped["suggested_next_batch"].astype(int)

    return grouped.loc[:, PLAN_COLUMNS]


def build_summary(
    *,
    stage: str,
    screening_log_path: Path,
    plan_path: Path,
    reviewer_count: int,
    total_records: int,
    non_blocking_reason: str,
    fail_on_single_reviewer: bool,
) -> str:
    lines: list[str] = []
    lines.append("# Reviewer Workload Balancer Summary")
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    lines.append(f"- Screening log: `{screening_log_path.as_posix()}`")
    lines.append(f"- Stage filter: `{stage}`")
    lines.append(f"- Output plan: `{plan_path.as_posix()}`")
    lines.append("")
    lines.append("## Snapshot")
    lines.append("")
    lines.append(f"- Reviewers observed: {reviewer_count}")
    lines.append(f"- Total screened records in scope: {total_records}")
    lines.append(
        f"- Non-blocking fallback active: {'yes' if not fail_on_single_reviewer else 'no'}"
    )
    lines.append("")
    lines.append("## Status")
    lines.append("")
    lines.append(f"- {non_blocking_reason}")
    lines.append("")
    return canonicalize_summary_text("\n".join(lines))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build non-blocking reviewer workload balancing suggestions."
    )
    parser.add_argument(
        "--screening-log",
        default="../02_data/processed/screening_daily_log.csv",
        help="Path to screening daily log CSV",
    )
    parser.add_argument(
        "--stage",
        default="title_abstract",
        help="Stage value filter from screening log (default: title_abstract)",
    )
    parser.add_argument(
        "--plan-output",
        default="outputs/reviewer_workload_plan.csv",
        help="Path to workload plan CSV",
    )
    parser.add_argument(
        "--summary",
        default="outputs/reviewer_workload_balancer_summary.md",
        help="Path to markdown summary",
    )
    parser.add_argument(
        "--fail-on-single-reviewer",
        action="store_true",
        help="Optional strict mode: return exit code 1 if fewer than two reviewers are available.",
    )
    args = parser.parse_args(argv)

    screening_log_path = Path(args.screening_log)
    plan_output_path = Path(args.plan_output)
    summary_path = Path(args.summary)

    screening_df = read_csv_or_empty(screening_log_path)
    plan_df = build_workload_plan(screening_df, stage=args.stage)

    reviewer_count = int(plan_df.shape[0])
    total_records = int(plan_df["current_records"].sum()) if not plan_df.empty else 0

    if screening_df.empty:
        status_message = "Input log missing or empty; generated empty non-blocking plan."
    elif reviewer_count == 0:
        status_message = (
            "No reviewer rows found for selected stage; generated empty non-blocking plan."
        )
    elif reviewer_count == 1:
        reviewer_name = str(plan_df.iloc[0]["reviewer"])
        status_message = f"Single reviewer detected ({reviewer_name}); balancer remains advisory and non-blocking by default."
    else:
        status_message = f"Balanced plan generated for {reviewer_count} reviewers."

    plan_output_path.parent.mkdir(parents=True, exist_ok=True)
    plan_df.to_csv(plan_output_path, index=False)

    summary_text = build_summary(
        stage=args.stage,
        screening_log_path=screening_log_path,
        plan_path=plan_output_path,
        reviewer_count=reviewer_count,
        total_records=total_records,
        non_blocking_reason=status_message,
        fail_on_single_reviewer=bool(args.fail_on_single_reviewer),
    )
    summary_text = canonicalize_summary_text(summary_text)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(summary_text, encoding="utf-8")

    print(f"Wrote: {plan_output_path}")
    print(f"Wrote: {summary_path}")
    print(status_message)

    if args.fail_on_single_reviewer and reviewer_count < 2:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

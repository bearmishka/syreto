import argparse
from pathlib import Path
import subprocess
import sys


def run_step(*, name: str, command: list[str], cwd: Path) -> None:
    result = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        return

    details: list[str] = []
    stdout_text = result.stdout.strip()
    stderr_text = result.stderr.strip()
    if stdout_text:
        details.append(f"stdout:\n{stdout_text}")
    if stderr_text:
        details.append(f"stderr:\n{stderr_text}")

    details_text = "\n\n".join(details)
    raise RuntimeError(
        f"Step `{name}` failed with exit code {result.returncode}."
        + (f"\n\n{details_text}" if details_text else "")
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run NOS -> JBI -> quality_appraisal -> NOS-report roundtrip in one command."
    )
    parser.add_argument(
        "--nos-input",
        default="../02_data/codebook/quality_appraisal_template_nos.csv",
        help="Path to source NOS-oriented appraisal CSV.",
    )
    parser.add_argument(
        "--jbi-output",
        default="../02_data/codebook/quality_appraisal_template.csv",
        help="Path to intermediate JBI-compatible appraisal CSV.",
    )
    parser.add_argument(
        "--nos-to-jbi-summary-output",
        default="outputs/nos_to_jbi_conversion_summary.md",
        help="Path to NOS -> JBI conversion summary output.",
    )
    parser.add_argument(
        "--extraction",
        default="../02_data/codebook/extraction_template.csv",
        help="Path to extraction template CSV for quality appraisal sync/scoring.",
    )
    parser.add_argument(
        "--scored-output",
        default="outputs/quality_appraisal_scored.csv",
        help="Path to quality appraisal scored output CSV.",
    )
    parser.add_argument(
        "--quality-summary-output",
        default="outputs/quality_appraisal_summary.md",
        help="Path to quality appraisal summary markdown output.",
    )
    parser.add_argument(
        "--aggregate-output",
        default="outputs/quality_appraisal_aggregate.csv",
        help="Path to quality appraisal aggregate output CSV.",
    )
    parser.add_argument(
        "--quality-fail-on",
        default="none",
        choices=["none", "warning", "error"],
        help="Fail mode to pass through to quality_appraisal.py.",
    )
    parser.add_argument(
        "--sync-extraction",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Sync computed quality summaries back into extraction CSV.",
    )
    parser.add_argument(
        "--roundtrip-nos-output",
        default="outputs/quality_appraisal_template_roundtrip_nos.csv",
        help="Path to final roundtrip NOS-oriented CSV output.",
    )
    parser.add_argument(
        "--nos-report-output",
        default="outputs/quality_appraisal_roundtrip_nos_report.md",
        help="Path to final NOS report markdown output.",
    )
    args = parser.parse_args(argv)

    script_root = Path(__file__).resolve().parent

    nos_input_path = Path(args.nos_input)
    jbi_output_path = Path(args.jbi_output)
    nos_to_jbi_summary_output_path = Path(args.nos_to_jbi_summary_output)
    extraction_path = Path(args.extraction)
    scored_output_path = Path(args.scored_output)
    quality_summary_output_path = Path(args.quality_summary_output)
    aggregate_output_path = Path(args.aggregate_output)
    roundtrip_nos_output_path = Path(args.roundtrip_nos_output)
    nos_report_output_path = Path(args.nos_report_output)

    run_step(
        name="nos_to_jbi",
        command=[
            sys.executable,
            str(script_root / "nos_to_jbi_converter.py"),
            "--nos-input",
            str(nos_input_path),
            "--jbi-output",
            str(jbi_output_path),
            "--summary-output",
            str(nos_to_jbi_summary_output_path),
        ],
        cwd=script_root,
    )

    quality_command = [
        sys.executable,
        str(script_root / "quality_appraisal.py"),
        "--extraction",
        str(extraction_path),
        "--appraisal-input",
        str(jbi_output_path),
        "--scored-output",
        str(scored_output_path),
        "--summary-output",
        str(quality_summary_output_path),
        "--aggregate-output",
        str(aggregate_output_path),
        "--fail-on",
        str(args.quality_fail_on),
        "--sync-extraction" if args.sync_extraction else "--no-sync-extraction",
    ]
    run_step(name="quality_appraisal", command=quality_command, cwd=script_root)

    run_step(
        name="jbi_to_nos",
        command=[
            sys.executable,
            str(script_root / "jbi_to_nos_converter.py"),
            "--jbi-input",
            str(jbi_output_path),
            "--nos-output",
            str(roundtrip_nos_output_path),
            "--summary-output",
            str(nos_report_output_path),
        ],
        cwd=script_root,
    )

    print(f"Wrote: {jbi_output_path}")
    print(f"Wrote: {nos_to_jbi_summary_output_path}")
    print(f"Wrote: {scored_output_path}")
    print(f"Wrote: {quality_summary_output_path}")
    print(f"Wrote: {aggregate_output_path}")
    print(f"Wrote: {roundtrip_nos_output_path}")
    print(f"Wrote: {nos_report_output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


def parse_missing_required_field_ids(summary_text: str) -> list[int]:
    missing_field_ids: list[int] = []
    in_missing_required_section = False

    for raw_line in summary_text.splitlines():
        line = raw_line.strip()

        if line == "## Missing Required Fields":
            in_missing_required_section = True
            continue

        if in_missing_required_section and line.startswith("## "):
            break

        if not in_missing_required_section:
            continue

        matched = re.match(r"^-\s*(\d+)\.\s+", line)
        if matched:
            missing_field_ids.append(int(matched.group(1)))

    return missing_field_ids


def parser() -> argparse.ArgumentParser:
    cli_parser = argparse.ArgumentParser(
        description="Validate that only expected PROSPERO required fields remain manual."
    )
    cli_parser.add_argument(
        "--summary",
        default="outputs/prospero_submission_drafter_summary.md",
        help="Path to prospero_submission_drafter summary markdown",
    )
    cli_parser.add_argument(
        "--expected-field",
        type=int,
        action="append",
        default=None,
        help="Expected missing required field id; can be passed multiple times",
    )
    return cli_parser


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    summary_path = Path(args.summary)

    if not summary_path.exists():
        print(
            f"[prospero_manual_fields_check] Missing summary file: {summary_path}", file=sys.stderr
        )
        return 2

    summary_text = summary_path.read_text(encoding="utf-8")
    missing_field_ids = parse_missing_required_field_ids(summary_text)

    expected_fields_raw = args.expected_field if args.expected_field is not None else [42, 43]
    expected_field_ids = list(dict.fromkeys(expected_fields_raw))
    if missing_field_ids != expected_field_ids:
        print(
            "[prospero_manual_fields_check] Expected missing required fields "
            f"{expected_field_ids}, got {missing_field_ids}.",
            file=sys.stderr,
        )
        return 1

    print(
        "[prospero_manual_fields_check] OK: only expected required fields remain manual "
        f"{expected_field_ids}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

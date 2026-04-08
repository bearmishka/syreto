from __future__ import annotations

import argparse
import csv
from pathlib import Path

EXPECTED_HEADER = ["stable_key", "record_id", "first_seen_date"]
EXPECTED_HEADER_LINE = ",".join(EXPECTED_HEADER)


def validate_record_id_map(path: Path) -> list[str]:
    issues: list[str] = []

    if not path.exists():
        issues.append(f"Missing file: {path.as_posix()}")
        return issues

    raw_text = path.read_text(encoding="utf-8", errors="replace")
    header_count = raw_text.count(EXPECTED_HEADER_LINE)
    if header_count != 1:
        issues.append(
            f"Header occurrence mismatch: expected exactly 1 '{EXPECTED_HEADER_LINE}', found {header_count}."
        )

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != EXPECTED_HEADER:
            issues.append(
                f"CSV schema mismatch: expected {EXPECTED_HEADER}, got {reader.fieldnames}."
            )
            return issues

        for row_index, row in enumerate(reader, start=2):
            for column in EXPECTED_HEADER:
                value = str(row.get(column, ""))
                if EXPECTED_HEADER_LINE in value:
                    issues.append(
                        f"Detected concatenated header token in row {row_index}, column '{column}'."
                    )

    return issues


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate structural integrity of record_id_map.csv before pipeline execution."
    )
    parser.add_argument(
        "--path",
        default="../02_data/processed/record_id_map.csv",
        help="Path to record_id_map.csv (default: ../02_data/processed/record_id_map.csv).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    path = Path(args.path)
    issues = validate_record_id_map(path)

    if issues:
        print(f"record_id_map integrity check FAILED for: {path.as_posix()}")
        for issue in issues:
            print(f"- {issue}")
        return 1

    print(f"record_id_map integrity check passed: {path.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

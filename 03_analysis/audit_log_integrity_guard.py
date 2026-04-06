from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path


EXPECTED_HEADER_LINE = "timestamp,action,file,description"


@dataclass
class AuditLogIntegrityReport:
    path: Path
    exists: bool
    header_line: str
    normalized_rows: list[str]
    duplicate_rows: list[tuple[int, str]]
    issues: list[str]


def canonicalize_line(raw_line: str) -> str:
    return raw_line.rstrip("\r").rstrip()


def validate_audit_log(path: Path) -> AuditLogIntegrityReport:
    if not path.exists():
        return AuditLogIntegrityReport(
            path=path,
            exists=False,
            header_line="",
            normalized_rows=[],
            duplicate_rows=[],
            issues=[f"Missing file: {path.as_posix()}"],
        )

    raw_lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    header_line = canonicalize_line(raw_lines[0]) if raw_lines else ""

    issues: list[str] = []
    if header_line != EXPECTED_HEADER_LINE:
        issues.append(
            f"Header mismatch: expected '{EXPECTED_HEADER_LINE}', got '{header_line or '<empty>'}'."
        )

    normalized_rows: list[str] = []
    duplicate_rows: list[tuple[int, str]] = []
    seen_rows: set[str] = set()

    for row_index, raw_line in enumerate(raw_lines[1:], start=2):
        normalized = canonicalize_line(raw_line)
        if not normalized:
            continue

        if normalized in seen_rows:
            duplicate_rows.append((row_index, normalized))
            continue

        seen_rows.add(normalized)
        normalized_rows.append(normalized)

    if duplicate_rows:
        issues.append(f"Duplicate rows detected: {len(duplicate_rows)}")

    return AuditLogIntegrityReport(
        path=path,
        exists=True,
        header_line=header_line,
        normalized_rows=normalized_rows,
        duplicate_rows=duplicate_rows,
        issues=issues,
    )


def rewrite_audit_log(path: Path, report: AuditLogIntegrityReport) -> None:
    lines = [EXPECTED_HEADER_LINE]
    lines.extend(report.normalized_rows)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate/deduplicate audit_log.csv entries (header + duplicate rows)."
    )
    parser.add_argument(
        "--path",
        default="../02_data/processed/audit_log.csv",
        help="Path to audit_log.csv (default: ../02_data/processed/audit_log.csv).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Rewrite audit_log.csv with normalized header + deduplicated rows.",
    )
    parser.add_argument(
        "--max-examples",
        type=int,
        default=3,
        help="Maximum duplicate examples to print (default: 3).",
    )
    return parser.parse_args(argv)


def print_duplicate_examples(report: AuditLogIntegrityReport, *, max_examples: int) -> None:
    if not report.duplicate_rows:
        return

    examples = report.duplicate_rows[:max_examples]
    for row_index, row_text in examples:
        print(f"- duplicate row at line {row_index}: {row_text}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    path = Path(args.path)

    report = validate_audit_log(path)

    if args.apply and report.exists:
        rewrite_audit_log(path, report)
        report = validate_audit_log(path)

    if report.issues:
        print(f"audit_log integrity check FAILED: {path.as_posix()}")
        for issue in report.issues:
            print(f"- {issue}")
        print_duplicate_examples(report, max_examples=max(args.max_examples, 0))
        return 1

    if args.apply:
        print(f"audit_log normalized + deduplicated: {path.as_posix()}")

    print(
        "audit_log integrity check passed: "
        f"{path.as_posix()} (rows={len(report.normalized_rows)}, duplicates=0)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
import argparse
import re
from dataclasses import dataclass
from datetime import datetime
from fnmatch import fnmatch
from pathlib import Path


DEFAULT_SCAN_PATHS = [
    "../01_protocol",
    "../02_data/codebook",
    "../04_manuscript",
    "../README.md",
]
DEFAULT_EXTENSIONS = [".md", ".txt", ".csv", ".tex"]
DEFAULT_EXCLUDE_GLOBS = ["**/build/**"]
DEFAULT_BANNED_PATTERNS = [
    r"\bbulimia\b",
    r"\bbn\b(?!-)",
    r"object[\\s-]*relations?",
    r"\bpsychodynamic\b",
    r"\battachment\b",
    r"\bidentity disturbance\b",
    r"\bidentity diffusion\b",
    r"\bego identity\b",
]

DEFAULT_PLACEHOLDER_PATTERNS = [
    r"\[(?:[A-Z0-9_]+(?: [A-Z0-9_]+)*)\]",
]


@dataclass(frozen=True)
class MatchResult:
    path: Path
    line_number: int
    pattern: str
    line_text: str
    match_group: str


def normalize_extensions(extensions: list[str]) -> set[str]:
    normalized: set[str] = set()
    for extension in extensions:
        candidate = extension.strip().lower()
        if not candidate:
            continue
        if not candidate.startswith("."):
            candidate = f".{candidate}"
        normalized.add(candidate)
    return normalized


def should_skip(path: Path, exclude_globs: list[str]) -> bool:
    posix_path = path.as_posix()
    return any(fnmatch(posix_path, pattern) for pattern in exclude_globs)


def iter_target_files(
    scan_paths: list[Path], allowed_extensions: set[str], exclude_globs: list[str]
) -> list[Path]:
    files: set[Path] = set()
    for scan_path in scan_paths:
        if not scan_path.exists():
            continue
        if scan_path.is_file():
            if scan_path.suffix.lower() in allowed_extensions and not should_skip(
                scan_path, exclude_globs
            ):
                files.add(scan_path)
            continue

        for candidate in scan_path.rglob("*"):
            if not candidate.is_file():
                continue
            if candidate.suffix.lower() not in allowed_extensions:
                continue
            if should_skip(candidate, exclude_globs):
                continue
            files.add(candidate)

    return sorted(files)


def scan_file(
    path: Path, patterns: list[re.Pattern[str]], *, match_group: str = "banned_term"
) -> list[MatchResult]:
    text = path.read_text(encoding="utf-8", errors="replace")
    matches: list[MatchResult] = []

    for line_number, line in enumerate(text.splitlines(), start=1):
        for pattern in patterns:
            if pattern.search(line):
                snippet = line.strip()
                if len(snippet) > 180:
                    snippet = f"{snippet[:177]}..."
                matches.append(
                    MatchResult(
                        path=path,
                        line_number=line_number,
                        pattern=pattern.pattern,
                        line_text=snippet,
                        match_group=match_group,
                    )
                )

    return matches


def build_summary(
    *,
    scan_paths: list[Path],
    scanned_files: list[Path],
    missing_paths: list[Path],
    matches: list[MatchResult],
    enabled_checks: list[str],
    output_path: Path,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines: list[str] = []
    lines.append("# Template Term Guard Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    for scan_path in scan_paths:
        lines.append(f"- `{scan_path.as_posix()}`")
    lines.append("")
    lines.append("## Scan Stats")
    lines.append("")
    if enabled_checks:
        lines.append(f"- Checks enabled: {', '.join(enabled_checks)}")
    else:
        lines.append("- Checks enabled: none")
    lines.append(f"- Files scanned: {len(scanned_files)}")
    lines.append(f"- Matches found: {len(matches)}")
    if enabled_checks:
        counts_by_group: dict[str, int] = {}
        for group in enabled_checks:
            counts_by_group[group] = sum(1 for match in matches if match.match_group == group)
        for group in enabled_checks:
            lines.append(f"- {group} matches: {counts_by_group.get(group, 0)}")
    if missing_paths:
        lines.append(f"- Missing scan paths: {len(missing_paths)}")
    lines.append(f"- Summary output: `{output_path.as_posix()}`")
    lines.append("")

    if missing_paths:
        lines.append("## Missing Paths")
        lines.append("")
        for path in missing_paths:
            lines.append(f"- `{path.as_posix()}`")
        lines.append("")

    lines.append("## Matches")
    lines.append("")
    if matches:
        for match in matches:
            lines.append(
                f"- `[{match.match_group}] {match.path.as_posix()}:{match.line_number}` | `{match.pattern}` | `{match.line_text}`"
            )
    else:
        lines.append("- No matches found in scanned files.")

    lines.append("")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan template source files for banned topic-specific terms."
    )
    parser.add_argument(
        "--scan-path",
        action="append",
        default=[],
        help="File or directory to scan (repeatable). Defaults to template source scope.",
    )
    parser.add_argument(
        "--extension",
        action="append",
        default=[],
        help="Allowed extension (repeatable), e.g. --extension md. Defaults to md/txt/csv/tex.",
    )
    parser.add_argument(
        "--exclude-glob",
        action="append",
        default=[],
        help="Glob pattern to exclude (repeatable).",
    )
    parser.add_argument(
        "--banned-pattern",
        action="append",
        default=[],
        help="Regex pattern to ban (repeatable).",
    )
    parser.add_argument(
        "--placeholder-pattern",
        action="append",
        default=[],
        help="Regex pattern for unresolved placeholders (repeatable).",
    )
    parser.add_argument(
        "--check-banned-terms",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable banned-term scan (default: true).",
    )
    parser.add_argument(
        "--check-placeholders",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable unresolved-placeholder scan (default: false).",
    )
    parser.add_argument(
        "--summary-output",
        default=None,
        help="Path to markdown summary output (default: 03_analysis/outputs/template_term_guard_summary.md).",
    )
    parser.add_argument(
        "--fail-on-match",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Exit with code 1 if matches are found (default: true).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    script_dir = Path(__file__).resolve().parent

    if args.scan_path:
        scan_paths = [Path(path_str) for path_str in args.scan_path]
    else:
        scan_paths = [(script_dir / path_str).resolve() for path_str in DEFAULT_SCAN_PATHS]

    missing_paths = [path for path in scan_paths if not path.exists()]

    extension_inputs = args.extension if args.extension else DEFAULT_EXTENSIONS
    allowed_extensions = normalize_extensions(extension_inputs)

    exclude_globs = args.exclude_glob if args.exclude_glob else DEFAULT_EXCLUDE_GLOBS

    scanned_files = iter_target_files(scan_paths, allowed_extensions, exclude_globs)

    enabled_checks: list[str] = []
    all_matches: list[MatchResult] = []

    if args.check_banned_terms:
        enabled_checks.append("banned_terms")
        banned_patterns_raw = (
            args.banned_pattern if args.banned_pattern else DEFAULT_BANNED_PATTERNS
        )
        banned_patterns = [
            re.compile(pattern, flags=re.IGNORECASE) for pattern in banned_patterns_raw
        ]
        for file_path in scanned_files:
            all_matches.extend(scan_file(file_path, banned_patterns, match_group="banned_terms"))

    if args.check_placeholders:
        enabled_checks.append("placeholders")
        placeholder_patterns_raw = (
            args.placeholder_pattern if args.placeholder_pattern else DEFAULT_PLACEHOLDER_PATTERNS
        )
        placeholder_patterns = [re.compile(pattern) for pattern in placeholder_patterns_raw]
        for file_path in scanned_files:
            all_matches.extend(
                scan_file(file_path, placeholder_patterns, match_group="placeholders")
            )

    summary_output = (
        Path(args.summary_output)
        if args.summary_output
        else script_dir / "outputs/template_term_guard_summary.md"
    )
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    summary_text = build_summary(
        scan_paths=scan_paths,
        scanned_files=scanned_files,
        missing_paths=missing_paths,
        matches=all_matches,
        enabled_checks=enabled_checks,
        output_path=summary_output,
    )
    summary_output.write_text(summary_text, encoding="utf-8")

    if not enabled_checks:
        print(f"No checks enabled. Summary: {summary_output}")
    elif all_matches:
        print(f"Found {len(all_matches)} guard matches. See: {summary_output}")
        for match in all_matches:
            print(
                f"- [{match.match_group}] {match.path}:{match.line_number} | {match.pattern} | {match.line_text}"
            )
    else:
        print(f"No matches found across {len(scanned_files)} files. Summary: {summary_output}")

    if all_matches and args.fail_on_match:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

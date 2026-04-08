import argparse
import os
import re
from dataclasses import dataclass
from datetime import datetime
from glob import glob
from pathlib import Path


DEFAULT_PLACEHOLDER_TARGETS = [
    "../01_protocol/protocol.md",
    "../04_manuscript/main.tex",
]
DEFAULT_PROCESSED_DIR = "../02_data/processed"
DEFAULT_PROCESSED_EXTENSIONS = {".csv", ".md", ".txt", ".tex"}
DEFAULT_ARTIFACT_TARGETS = [
    "../04_manuscript/tables/*.tex",
    "outputs/*.tikz",
]

KNOWN_PLACEHOLDER_TOKENS = [
    "YOUR REVIEW TITLE",
    "EXPOSURE_OR_CONCEPT",
    "OUTCOME",
    "POPULATION",
    "START_YEAR",
    "ELIGIBLE_LANGUAGES",
    "AUTHOR NAME",
    "AFFILIATION",
    "FUNDER NAME",
    "GRANT NUMBER",
    "REGISTRY NAME",
    "IDENTIFIER",
    "DATE",
    "REPOSITORY LINK OR DOI",
]

DATA_MARKER_PATTERNS = [
    r"\bMR_DEMO_[A-Z0-9_-]+\b",
    r"\b[A-Z0-9]+_DEMO_[A-Z0-9_-]+\b",
    r"\btemplate_[A-Za-z0-9_.-]+\b",
    r"\bYYYY-MM-DD\b",
    r"\bbootstrap/demo\b",
]

EPISTEMIC_WARNING_MARKER = "% EPISTEMIC_STATUS: TEMPLATE_OR_DEMO_DATA_DETECTED"


@dataclass(frozen=True)
class GuardMatch:
    path: Path
    line_number: int
    pattern: str
    line_text: str
    match_group: str
    matched_text: str


@dataclass(frozen=True)
class MarkerUpdate:
    path: Path
    status: str


def resolve_path(script_dir: Path, path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (script_dir / path).resolve()


def path_for_report(path: Path, script_dir: Path) -> str:
    try:
        return path.relative_to(script_dir).as_posix()
    except ValueError:
        return path.as_posix()


def build_placeholder_patterns() -> list[re.Pattern[str]]:
    patterns: list[re.Pattern[str]] = []
    for token in KNOWN_PLACEHOLDER_TOKENS:
        token_pattern_parts: list[str] = []
        for character in token:
            if character == " ":
                token_pattern_parts.append(r"\s+")
            elif character == "_":
                token_pattern_parts.append(r"(?:_|\\_)")
            else:
                token_pattern_parts.append(re.escape(character))
        token_pattern = "".join(token_pattern_parts)
        patterns.append(re.compile(rf"\[{token_pattern}\]"))

    patterns.append(re.compile(r"\[(?:[A-Z][A-Z0-9]*(?:(?:\s+|_|\\_)[A-Z0-9]+)+)\]"))
    return patterns


def build_data_marker_patterns() -> list[re.Pattern[str]]:
    return [re.compile(pattern, flags=re.IGNORECASE) for pattern in DATA_MARKER_PATTERNS]


def iter_processed_files(processed_dir: Path) -> list[Path]:
    files: list[Path] = []
    for path in processed_dir.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in DEFAULT_PROCESSED_EXTENSIONS:
            continue
        files.append(path)
    return sorted(files)


def scan_file(path: Path, patterns: list[re.Pattern[str]], *, match_group: str) -> list[GuardMatch]:
    matches: list[GuardMatch] = []
    text = path.read_text(encoding="utf-8", errors="replace")
    seen: set[tuple[int, str, str]] = set()

    for line_number, line in enumerate(text.splitlines(), start=1):
        for pattern in patterns:
            found = pattern.search(line)
            if found is None:
                continue

            signature = (line_number, found.group(0).lower(), match_group)
            if signature in seen:
                continue
            seen.add(signature)

            snippet = line.strip()
            if len(snippet) > 180:
                snippet = f"{snippet[:177]}..."

            matches.append(
                GuardMatch(
                    path=path,
                    line_number=line_number,
                    pattern=pattern.pattern,
                    line_text=snippet,
                    match_group=match_group,
                    matched_text=found.group(0),
                )
            )

    return matches


def resolve_artifact_paths(
    script_dir: Path, artifact_targets: list[str]
) -> tuple[list[Path], list[str]]:
    files: set[Path] = set()
    missing_targets: list[str] = []

    for target in artifact_targets:
        if any(symbol in target for symbol in "*?[]"):
            glob_pattern = (
                target if Path(target).is_absolute() else (script_dir / target).as_posix()
            )
            matched = [Path(path).resolve() for path in glob(glob_pattern, recursive=True)]
            matched_files = [path for path in matched if path.is_file()]
            if matched_files:
                files.update(matched_files)
            else:
                missing_targets.append(target)
            continue

        target_path = resolve_path(script_dir, target)
        if target_path.is_file():
            files.add(target_path)
        else:
            missing_targets.append(target)

    return sorted(files), missing_targets


def apply_epistemic_marker(path: Path, *, should_mark: bool) -> str:
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines(keepends=True)

    has_marker = bool(lines) and lines[0].strip() == EPISTEMIC_WARNING_MARKER
    if should_mark and not has_marker:
        newline = "\n"
        if lines and lines[0].endswith("\r\n"):
            newline = "\r\n"
        path.write_text(f"{EPISTEMIC_WARNING_MARKER}{newline}{text}", encoding="utf-8")
        return "marked"

    if not should_mark and has_marker:
        path.write_text("".join(lines[1:]), encoding="utf-8")
        return "unmarked"

    return "unchanged"


def apply_artifact_markers(paths: list[Path], *, should_mark: bool) -> list[MarkerUpdate]:
    updates: list[MarkerUpdate] = []
    for path in paths:
        status = apply_epistemic_marker(path, should_mark=should_mark)
        updates.append(MarkerUpdate(path=path, status=status))
    return updates


def build_report(
    *,
    review_mode: str,
    fail_on_risk: bool,
    placeholder_targets: list[Path],
    placeholder_missing: list[Path],
    processed_dir: Path,
    processed_missing: bool,
    processed_files: list[Path],
    artifact_targets: list[str],
    artifact_missing: list[str],
    artifact_updates: list[MarkerUpdate],
    matches: list[GuardMatch],
    output_path: Path,
    script_dir: Path,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    placeholder_matches = [match for match in matches if match.match_group == "placeholder"]
    data_matches = [match for match in matches if match.match_group == "data_marker"]
    risk_detected = bool(matches)
    decision = "fail" if risk_detected and fail_on_risk else "pass"

    lines: list[str] = []
    lines.append("# Epistemic Consistency Guard Report")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append(f"Review mode: {review_mode}")
    lines.append(f"Fail on risk: {'yes' if fail_on_risk else 'no'}")
    lines.append(f"Risk detected: {'yes' if risk_detected else 'no'}")
    lines.append("")

    lines.append("## Scope")
    lines.append("")
    lines.append("- Placeholder targets:")
    for path in placeholder_targets:
        lines.append(f"  - `{path_for_report(path, script_dir)}`")
    lines.append(f"- Processed data directory: `{path_for_report(processed_dir, script_dir)}`")
    lines.append("- Artifact target specs:")
    for target in artifact_targets:
        lines.append(f"  - `{target}`")
    lines.append(f"- Summary output: `{path_for_report(output_path, script_dir)}`")
    lines.append("")

    lines.append("## Scan Stats")
    lines.append("")
    lines.append(f"- Placeholder matches: {len(placeholder_matches)}")
    lines.append(f"- Data marker matches: {len(data_matches)}")
    lines.append(f"- Total matches: {len(matches)}")
    lines.append(f"- Processed files scanned: {len(processed_files)}")
    lines.append(f"- Artifact files checked: {len(artifact_updates)}")
    lines.append(
        f"- Artifact files marked: {sum(1 for item in artifact_updates if item.status == 'marked')}"
    )
    lines.append(
        f"- Artifact files unmarked: {sum(1 for item in artifact_updates if item.status == 'unmarked')}"
    )
    lines.append(
        f"- Artifact files unchanged: {sum(1 for item in artifact_updates if item.status == 'unchanged')}"
    )
    lines.append("")

    missing_items: list[str] = []
    missing_items.extend(path_for_report(path, script_dir) for path in placeholder_missing)
    if processed_missing:
        missing_items.append(path_for_report(processed_dir, script_dir))
    missing_items.extend(artifact_missing)

    lines.append("## Missing Inputs")
    lines.append("")
    if missing_items:
        for item in missing_items:
            lines.append(f"- `{item}`")
    else:
        lines.append("- None.")
    lines.append("")

    lines.append("## Risk Matches")
    lines.append("")
    if matches:
        ordered = sorted(
            matches, key=lambda item: (item.path.as_posix(), item.line_number, item.pattern)
        )
        for match in ordered:
            lines.append(
                "- "
                f"`[{match.match_group}] {path_for_report(match.path, script_dir)}:{match.line_number}` "
                f"| `{match.matched_text}` | `{match.line_text}`"
            )
    else:
        lines.append("- No epistemic risk markers detected.")
    lines.append("")

    lines.append("## Artifact Marker Actions")
    lines.append("")
    if artifact_updates:
        for update in artifact_updates:
            lines.append(f"- `[{update.status}] {path_for_report(update.path, script_dir)}`")
    else:
        lines.append("- No artifact files resolved from target specs.")
    lines.append("")

    lines.append("## Gate Decision")
    lines.append("")
    if decision == "fail":
        lines.append("- Decision: fail (risk detected and strict mode enabled).")
    else:
        lines.append("- Decision: pass.")
    lines.append("")

    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Guard against epistemic mismatch between template/demo inputs and generated manuscript artifacts."
    )
    parser.add_argument(
        "--placeholder-target",
        action="append",
        default=[],
        help="File path to scan for unresolved placeholders (repeatable).",
    )
    parser.add_argument(
        "--processed-dir",
        default=DEFAULT_PROCESSED_DIR,
        help="Directory with processed data files to scan for demo/template markers.",
    )
    parser.add_argument(
        "--artifact-target",
        action="append",
        default=[],
        help="File path or glob for high-risk artifact marking (repeatable).",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/epistemic_consistency_report.md",
        help="Markdown report output path.",
    )
    parser.add_argument(
        "--review-mode",
        choices=["template", "production"],
        default=None,
        help="Operational mode used in reporting/gating (default: REVIEW_MODE env or template).",
    )
    parser.add_argument(
        "--fail-on-risk",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Exit with code 1 when risk is detected (default: enabled in production mode).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    script_dir = Path(__file__).resolve().parent

    review_mode = args.review_mode or os.getenv("REVIEW_MODE", "template")
    if review_mode not in {"template", "production"}:
        print(f"Invalid review mode: {review_mode}")
        return 2

    fail_on_risk = (
        args.fail_on_risk if args.fail_on_risk is not None else review_mode == "production"
    )

    placeholder_specs = (
        args.placeholder_target if args.placeholder_target else DEFAULT_PLACEHOLDER_TARGETS
    )
    placeholder_targets = [resolve_path(script_dir, spec) for spec in placeholder_specs]

    processed_dir = resolve_path(script_dir, args.processed_dir)

    artifact_targets = args.artifact_target if args.artifact_target else DEFAULT_ARTIFACT_TARGETS
    artifact_paths, artifact_missing = resolve_artifact_paths(script_dir, artifact_targets)

    placeholder_patterns = build_placeholder_patterns()
    data_marker_patterns = build_data_marker_patterns()

    placeholder_missing: list[Path] = []
    placeholder_matches: list[GuardMatch] = []
    for path in placeholder_targets:
        if not path.is_file():
            placeholder_missing.append(path)
            continue
        placeholder_matches.extend(scan_file(path, placeholder_patterns, match_group="placeholder"))

    processed_files: list[Path] = []
    data_matches: list[GuardMatch] = []
    processed_missing = not processed_dir.is_dir()
    if not processed_missing:
        processed_files = iter_processed_files(processed_dir)
        for path in processed_files:
            data_matches.extend(scan_file(path, data_marker_patterns, match_group="data_marker"))

    all_matches = placeholder_matches + data_matches
    risk_detected = bool(all_matches)

    artifact_updates = apply_artifact_markers(artifact_paths, should_mark=risk_detected)

    summary_output = resolve_path(script_dir, args.summary_output)
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    report_text = build_report(
        review_mode=review_mode,
        fail_on_risk=fail_on_risk,
        placeholder_targets=placeholder_targets,
        placeholder_missing=placeholder_missing,
        processed_dir=processed_dir,
        processed_missing=processed_missing,
        processed_files=processed_files,
        artifact_targets=artifact_targets,
        artifact_missing=artifact_missing,
        artifact_updates=artifact_updates,
        matches=all_matches,
        output_path=summary_output,
        script_dir=script_dir,
    )
    summary_output.write_text(report_text, encoding="utf-8")

    if risk_detected:
        print(
            f"Epistemic risk markers detected ({len(all_matches)} matches). Report: {summary_output}"
        )
    else:
        print(f"No epistemic risk markers detected. Report: {summary_output}")

    if risk_detected and fail_on_risk:
        print("Blocking run: strict epistemic guard is enabled.")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

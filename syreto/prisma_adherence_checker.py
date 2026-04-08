import argparse
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


DEFAULT_MANUSCRIPT_ROOT = Path("../04_manuscript")
DEFAULT_OUTPUT_PATH = Path("outputs/prisma_adherence_report.md")
DEFAULT_EXTENSIONS = [".tex"]

LATEX_COMMAND_PATTERN = re.compile(r"\\[a-zA-Z@]+\*?")
NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class PrismaChecklistItem:
    number: int
    title: str
    expectation: str
    signal_groups: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class SignalHit:
    signal: str
    path: Path
    line_number: int
    line_text: str


@dataclass
class ItemAssessment:
    item: PrismaChecklistItem
    group_hits: list[SignalHit | None]

    @property
    def covered(self) -> bool:
        return all(hit is not None for hit in self.group_hits)


PRISMA_2020_ITEMS: list[PrismaChecklistItem] = [
    PrismaChecklistItem(
        number=1,
        title="Title",
        expectation="Identify the report explicitly as a systematic review (or systematic review with meta-analysis).",
        signal_groups=(("systematic review", "meta-analysis", "meta analysis"),),
    ),
    PrismaChecklistItem(
        number=2,
        title="Abstract",
        expectation="Provide a structured abstract summarizing key review methods and findings.",
        signal_groups=(("abstract",),),
    ),
    PrismaChecklistItem(
        number=3,
        title="Rationale",
        expectation="Describe the rationale for the review in the context of existing knowledge.",
        signal_groups=(("rationale", "background", "why the topic matters"),),
    ),
    PrismaChecklistItem(
        number=4,
        title="Objectives",
        expectation="State explicit review objectives and/or research questions.",
        signal_groups=(
            ("objective", "research question", "review question", "aim of this review"),
        ),
    ),
    PrismaChecklistItem(
        number=5,
        title="Eligibility Criteria",
        expectation="Specify inclusion and exclusion criteria for studies.",
        signal_groups=(
            ("eligibility criteria", "inclusion criteria", "excluded studies", "eligible studies"),
        ),
    ),
    PrismaChecklistItem(
        number=6,
        title="Information Sources",
        expectation="List all information sources and dates searched.",
        signal_groups=(
            (
                "information sources",
                "databases include",
                "pubmed",
                "embase",
                "scopus",
                "web of science",
                "psycinfo",
            ),
        ),
    ),
    PrismaChecklistItem(
        number=7,
        title="Search Strategy",
        expectation="Present full search strategies for each source.",
        signal_groups=(
            ("search strategy", "search strings", "boolean queries", "full search strategy"),
        ),
    ),
    PrismaChecklistItem(
        number=8,
        title="Selection Process",
        expectation="Describe how records/studies were selected and by whom.",
        signal_groups=(
            (
                "selection process",
                "screened in two stages",
                "dual independent",
                "title abstract screening",
            ),
        ),
    ),
    PrismaChecklistItem(
        number=9,
        title="Data Collection Process",
        expectation="Describe how data were collected/extracted from reports.",
        signal_groups=(("data collection process", "data extraction", "extraction template"),),
    ),
    PrismaChecklistItem(
        number=10,
        title="Data Items",
        expectation="Define all outcomes and other variables collected.",
        signal_groups=(("data items", "outcome measures", "covariates", "variables extracted"),),
    ),
    PrismaChecklistItem(
        number=11,
        title="Study Risk of Bias Assessment",
        expectation="Specify methods used to assess risk of bias in included studies.",
        signal_groups=(("risk of bias assessment", "quality appraisal", "methodological quality"),),
    ),
    PrismaChecklistItem(
        number=12,
        title="Effect Measures",
        expectation="Specify effect measure(s) used for outcomes.",
        signal_groups=(
            (
                "effect measures",
                "effect size",
                "odds ratio",
                "risk ratio",
                "standardized mean difference",
            ),
        ),
    ),
    PrismaChecklistItem(
        number=13,
        title="Synthesis Methods",
        expectation="Describe synthesis methods, including data preparation and heterogeneity/sensitivity handling.",
        signal_groups=(
            (
                "synthesis methods",
                "narrative synthesis",
                "meta-analysis",
                "heterogeneity",
                "sensitivity analysis",
            ),
        ),
    ),
    PrismaChecklistItem(
        number=14,
        title="Reporting Bias Assessment",
        expectation="Describe methods used to assess risk of bias due to missing results/reporting biases.",
        signal_groups=(("reporting bias", "publication bias", "small-study effects"),),
    ),
    PrismaChecklistItem(
        number=15,
        title="Certainty Assessment",
        expectation="Describe methods used to assess certainty/confidence in the body of evidence.",
        signal_groups=(("certainty assessment", "certainty of evidence", "grade"),),
    ),
    PrismaChecklistItem(
        number=16,
        title="Study Selection",
        expectation="Report selection results from search to included studies (ideally with a flow diagram).",
        signal_groups=(
            ("study selection", "prisma flow", "records identified", "reports assessed full text"),
        ),
    ),
    PrismaChecklistItem(
        number=17,
        title="Study Characteristics",
        expectation="Cite and present characteristics of each included study.",
        signal_groups=(("study characteristics", "sample descriptors", "study design"),),
    ),
    PrismaChecklistItem(
        number=18,
        title="Risk of Bias in Studies",
        expectation="Present risk-of-bias assessments for included studies.",
        signal_groups=(("risk of bias in studies", "quality scores", "appraisal results"),),
    ),
    PrismaChecklistItem(
        number=19,
        title="Results of Individual Studies",
        expectation="Present summary statistics/effect estimates for each included study.",
        signal_groups=(("individual study results", "study-level results", "effect directions"),),
    ),
    PrismaChecklistItem(
        number=20,
        title="Results of Syntheses",
        expectation="Present results of each synthesis, including heterogeneity/sensitivity where relevant.",
        signal_groups=(
            (
                "results of syntheses",
                "pooled effect",
                "heterogeneity results",
                "sensitivity analysis results",
            ),
        ),
    ),
    PrismaChecklistItem(
        number=21,
        title="Reporting Biases",
        expectation="Present assessments of risk of bias due to missing results/reporting biases.",
        signal_groups=(("reporting biases", "publication bias results", "funnel plot"),),
    ),
    PrismaChecklistItem(
        number=22,
        title="Certainty of Evidence",
        expectation="Present certainty/confidence in the body of evidence for each outcome.",
        signal_groups=(("certainty of evidence", "confidence in the evidence", "grade certainty"),),
    ),
    PrismaChecklistItem(
        number=23,
        title="Discussion",
        expectation="Interpret results, discuss limitations of evidence/review, and implications.",
        signal_groups=(("discussion",), ("limitations", "implications", "interpret")),
    ),
    PrismaChecklistItem(
        number=24,
        title="Registration and Protocol",
        expectation="Provide registration/protocol details and note protocol amendments, if any.",
        signal_groups=(("registration", "preregistration", "prospero", "protocol"),),
    ),
    PrismaChecklistItem(
        number=25,
        title="Support",
        expectation="Describe sources of financial/non-financial support and role of funders.",
        signal_groups=(("funding", "support", "grant"),),
    ),
    PrismaChecklistItem(
        number=26,
        title="Competing Interests",
        expectation="Declare competing interests/conflicts of interest.",
        signal_groups=(("conflicts of interest", "competing interests"),),
    ),
    PrismaChecklistItem(
        number=27,
        title="Availability of Data, Code, and Materials",
        expectation="Report where data, code, and other materials are available.",
        signal_groups=(
            ("data availability", "code availability", "materials availability", "data and code"),
        ),
    ),
]


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


def strip_latex_comment(line: str) -> str:
    for index, char in enumerate(line):
        if char != "%":
            continue

        backslash_count = 0
        cursor = index - 1
        while cursor >= 0 and line[cursor] == "\\":
            backslash_count += 1
            cursor -= 1

        if backslash_count % 2 == 0:
            return line[:index]

    return line


def normalize_for_match(text: str) -> str:
    without_comments = strip_latex_comment(text)
    without_commands = LATEX_COMMAND_PATTERN.sub(" ", without_comments)
    lowered = without_commands.lower()
    alnum_only = NON_ALNUM_PATTERN.sub(" ", lowered)
    return " ".join(alnum_only.split())


def contains_normalized_phrase(normalized_text: str, normalized_phrase: str) -> bool:
    if not normalized_text or not normalized_phrase:
        return False
    return f" {normalized_phrase} " in f" {normalized_text} "


def iter_manuscript_files(manuscript_root: Path, allowed_extensions: set[str]) -> list[Path]:
    if not manuscript_root.exists():
        return []

    if manuscript_root.is_file():
        return [manuscript_root] if manuscript_root.suffix.lower() in allowed_extensions else []

    files: list[Path] = []
    for candidate in manuscript_root.rglob("*"):
        if candidate.is_file() and candidate.suffix.lower() in allowed_extensions:
            files.append(candidate)
    return sorted(files)


def scan_prisma_mentions(
    manuscript_root: Path,
    *,
    allowed_extensions: set[str],
) -> tuple[list[Path], list[ItemAssessment]]:
    manuscript_files = iter_manuscript_files(manuscript_root, allowed_extensions)

    normalized_signals: dict[int, list[list[tuple[str, str]]]] = {}
    assessments: list[ItemAssessment] = []

    for item in PRISMA_2020_ITEMS:
        per_group: list[list[tuple[str, str]]] = []
        for group in item.signal_groups:
            per_group.append([(signal, normalize_for_match(signal)) for signal in group])
        normalized_signals[item.number] = per_group
        assessments.append(ItemAssessment(item=item, group_hits=[None for _ in item.signal_groups]))

    assessments_by_number = {assessment.item.number: assessment for assessment in assessments}

    for path in manuscript_files:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        for line_number, raw_line in enumerate(lines, start=1):
            normalized_line = normalize_for_match(raw_line)
            if not normalized_line:
                continue

            for item in PRISMA_2020_ITEMS:
                assessment = assessments_by_number[item.number]
                per_group = normalized_signals[item.number]

                for group_index, group_signals in enumerate(per_group):
                    if assessment.group_hits[group_index] is not None:
                        continue

                    for original_signal, normalized_signal in group_signals:
                        if contains_normalized_phrase(normalized_line, normalized_signal):
                            snippet = raw_line.strip()
                            if len(snippet) > 180:
                                snippet = f"{snippet[:177]}..."
                            assessment.group_hits[group_index] = SignalHit(
                                signal=original_signal,
                                path=path,
                                line_number=line_number,
                                line_text=snippet,
                            )
                            break

    ordered_assessments = sorted(assessments, key=lambda assessment: assessment.item.number)
    return manuscript_files, ordered_assessments


def build_report(
    *,
    manuscript_root: Path,
    manuscript_files: list[Path],
    assessments: list[ItemAssessment],
    output_path: Path,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    covered = [assessment for assessment in assessments if assessment.covered]
    missing = [assessment for assessment in assessments if not assessment.covered]
    coverage_pct = (100.0 * len(covered) / len(assessments)) if assessments else 0.0

    lines: list[str] = []
    lines.append("# PRISMA 2020 Adherence Report")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    lines.append(f"- Manuscript root: `{manuscript_root.as_posix()}`")
    lines.append(f"- Files scanned: {len(manuscript_files)}")
    lines.append(f"- Report path: `{output_path.as_posix()}`")
    lines.append("")

    lines.append("## Coverage")
    lines.append("")
    lines.append(f"- Covered PRISMA items: {len(covered)}/{len(assessments)} ({coverage_pct:.1f}%)")
    lines.append(f"- Missing PRISMA items: {len(missing)}")
    lines.append("")

    lines.append("## Gap List")
    lines.append("")
    if missing:
        for assessment in missing:
            item = assessment.item
            missing_cues: list[str] = []
            for group_index, group in enumerate(item.signal_groups):
                if assessment.group_hits[group_index] is None:
                    missing_cues.append(group[0])

            cues_text = ", ".join(f"`{cue}`" for cue in missing_cues)
            lines.append(
                f"- [ ] **{item.number}. {item.title}** — {item.expectation} (cue: {cues_text})"
            )
    else:
        lines.append("- ✅ No checklist gaps detected (all 27 PRISMA items mentioned).")
    lines.append("")

    lines.append("## Item-by-Item Status")
    lines.append("")
    lines.append("| # | PRISMA item | Status | Evidence |")
    lines.append("|---:|---|---|---|")

    for assessment in assessments:
        item = assessment.item
        status = "✅ mentioned" if assessment.covered else "❌ gap"
        evidence_hits = [hit for hit in assessment.group_hits if hit is not None]

        if evidence_hits:
            evidence_bits = [
                f"`{hit.path.as_posix()}:{hit.line_number}` (`{hit.signal}`)"
                for hit in evidence_hits
            ]
            evidence = "<br>".join(evidence_bits)
        else:
            evidence = "—"

        lines.append(f"| {item.number} | {item.title} | {status} | {evidence} |")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- This is a heuristic keyword-based checker for PRISMA 2020 item mentions.")
    lines.append("- Use it as a fast pre-check before final manual PRISMA checklist verification.")
    lines.append("")

    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check whether a manuscript draft mentions all 27 PRISMA 2020 checklist items."
    )
    parser.add_argument(
        "--manuscript-root",
        default=DEFAULT_MANUSCRIPT_ROOT.as_posix(),
        help="Path to manuscript root directory (default: ../04_manuscript).",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_PATH.as_posix(),
        help="Markdown report output path (default: outputs/prisma_adherence_report.md).",
    )
    parser.add_argument(
        "--extension",
        action="append",
        default=[],
        help="File extension to scan (repeatable). Defaults to .tex.",
    )
    parser.add_argument(
        "--fail-on-gap",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Exit with code 1 when one or more checklist gaps are found (default: false).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    manuscript_root = Path(args.manuscript_root)
    output_path = Path(args.output)

    if args.extension:
        allowed_extensions = normalize_extensions(args.extension)
    else:
        allowed_extensions = set(DEFAULT_EXTENSIONS)

    manuscript_files, assessments = scan_prisma_mentions(
        manuscript_root,
        allowed_extensions=allowed_extensions,
    )

    report = build_report(
        manuscript_root=manuscript_root,
        manuscript_files=manuscript_files,
        assessments=assessments,
        output_path=output_path,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")

    missing_count = sum(1 for assessment in assessments if not assessment.covered)
    covered_count = len(assessments) - missing_count

    print(f"Wrote: {output_path}")
    print(f"PRISMA coverage: {covered_count}/{len(assessments)} items mentioned")
    print(f"Gaps: {missing_count}")
    if not manuscript_files:
        print("Warning: no manuscript files matched the scan scope.")

    if args.fail_on_gap and missing_count > 0:
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

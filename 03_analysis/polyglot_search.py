import argparse
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

FIELD_TERM_RE = re.compile(
    r'(?P<term>"(?:[^"\\]|\\.)*"|[^\s()]+)\s*\[\s*(?P<tag>[^\]]+)\s*\]',
    re.IGNORECASE,
)

QUOTED_TEXT_RE = re.compile(r'("(?:[^"\\]|\\.)*")')


def normalize_text(value: object) -> str:
    text = str(value if value is not None else "")
    return text.replace("\r\n", "\n").replace("\r", "\n").strip()


def normalize_field_tag(raw_tag: str) -> str:
    text = normalize_text(raw_tag).lower()
    text = re.sub(r"[^a-z0-9]+", "", text)

    if text in {"titleabstract", "tiab", "titleab"}:
        return "tiab"
    if text in {"title", "ti"}:
        return "ti"
    if text in {"abstract", "ab"}:
        return "ab"
    if text in {
        "meshterms",
        "meshterm",
        "mesh",
        "mh",
        "majr",
        "majrnoexp",
        "majorfocus",
        "meshmajortopic",
        "meshmajortopicnoexp",
        "majortopic",
        "majortopicnoexp",
    }:
        return "mesh"
    if text in {"allfields", "all", "textword", "tw"}:
        return "all"
    return "unknown"


def normalize_boolean_operators(text: str) -> str:
    parts = QUOTED_TEXT_RE.split(text)
    normalized_parts: list[str] = []

    for index, part in enumerate(parts):
        if index % 2 == 1:
            normalized_parts.append(part)
            continue
        outside_quotes = re.sub(
            r"\b(and|or|not)\b",
            lambda match: match.group(1).upper(),
            part,
            flags=re.IGNORECASE,
        )
        normalized_parts.append(outside_quotes)

    return "".join(normalized_parts)


def collapse_whitespace_per_line(text: str) -> str:
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.split("\n")]

    normalized_lines: list[str] = []
    previous_blank = False
    for line in lines:
        is_blank = line == ""
        if is_blank and previous_blank:
            continue
        normalized_lines.append(line)
        previous_blank = is_blank

    return "\n".join(normalized_lines).strip()


@dataclass(frozen=True)
class TargetConfig:
    display_name: str
    field_wrappers: dict[str, str]
    default_wrapper: str


def wrap_template(template: str, term: str) -> str:
    return template.format(term=term)


TARGETS: dict[str, TargetConfig] = {
    "scopus": TargetConfig(
        display_name="Scopus",
        field_wrappers={
            "tiab": "TITLE-ABS-KEY({term})",
            "ti": "TITLE({term})",
            "ab": "ABS({term})",
            "mesh": "TITLE-ABS-KEY({term})",
            "all": "TITLE-ABS-KEY({term})",
        },
        default_wrapper="TITLE-ABS-KEY({term})",
    ),
    "wos": TargetConfig(
        display_name="Web of Science Core Collection",
        field_wrappers={
            "tiab": "TS=({term})",
            "ti": "TI=({term})",
            "ab": "AB=({term})",
            "mesh": "TS=({term})",
            "all": "TS=({term})",
        },
        default_wrapper="TS=({term})",
    ),
    "psycinfo": TargetConfig(
        display_name="PsycINFO (EBSCO style)",
        field_wrappers={
            "tiab": "TI,AB,SU({term})",
            "ti": "TI({term})",
            "ab": "AB({term})",
            "mesh": "TI,AB,SU({term})",
            "all": "TI,AB,SU({term})",
        },
        default_wrapper="TI,AB,SU({term})",
    ),
}

TARGET_ALIASES = {
    "scopus": "scopus",
    "wos": "wos",
    "webofscience": "wos",
    "web_of_science": "wos",
    "web-of-science": "wos",
    "psycinfo": "psycinfo",
    "psyc": "psycinfo",
}


def parse_targets(raw_targets: str) -> list[str]:
    parsed: list[str] = []
    unknown: list[str] = []

    for part in raw_targets.split(","):
        token = normalize_text(part).lower().replace(" ", "")
        if not token:
            continue
        key = TARGET_ALIASES.get(token)
        if key is None:
            unknown.append(part.strip())
            continue
        if key not in parsed:
            parsed.append(key)

    if unknown:
        known = ", ".join(sorted(TARGETS.keys()))
        raise ValueError(f"Unsupported targets: {', '.join(unknown)}. Supported targets: {known}")

    if not parsed:
        raise ValueError(
            "No targets selected. Use --targets with at least one of: scopus,wos,psycinfo"
        )

    return parsed


def extract_pubmed_query_from_markdown(markdown_text: str) -> str:
    pattern = re.compile(r"##\s*PubMed.*?```(?:text)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)
    match = pattern.search(markdown_text)
    if not match:
        return ""
    return normalize_text(match.group(1))


def load_pubmed_query(query_path: Path, inline_query: str) -> tuple[str, str]:
    inline = normalize_text(inline_query)
    if inline:
        return inline, "--query"

    if not query_path.exists():
        raise FileNotFoundError(f"PubMed query source not found: {query_path}")

    raw_text = normalize_text(query_path.read_text(encoding="utf-8"))
    if not raw_text:
        raise ValueError(f"PubMed query source is empty: {query_path}")

    if query_path.suffix.lower() == ".md":
        extracted = extract_pubmed_query_from_markdown(raw_text)
        if extracted:
            return extracted, f"{query_path.as_posix()} (PubMed code block)"

    return raw_text, query_path.as_posix()


def deduplicate_messages(messages: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for message in messages:
        if message in seen:
            continue
        seen.add(message)
        ordered.append(message)
    return ordered


def find_matching_parenthesis(text: str, start_index: int) -> int:
    depth = 0
    in_quotes = False
    escaped = False

    for index in range(start_index, len(text)):
        char = text[index]

        if in_quotes:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_quotes = False
            continue

        if char == '"':
            in_quotes = True
            continue

        if char == "(":
            depth += 1
            continue

        if char == ")":
            depth -= 1
            if depth == 0:
                return index

    return -1


def deduplicate_top_level_boolean_clauses(text: str, operator: str) -> tuple[str, int]:
    op = operator.strip().upper()
    if op not in {"OR", "AND", "NOT"}:
        raise ValueError(f"Unsupported operator for clause deduplication: {operator}")

    parts: list[str] = []
    start = 0
    depth = 0
    in_quotes = False
    escaped = False
    index = 0

    while index < len(text):
        char = text[index]

        if in_quotes:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_quotes = False
            index += 1
            continue

        if char == '"':
            in_quotes = True
            index += 1
            continue

        if char == "(":
            depth += 1
            index += 1
            continue

        if char == ")":
            depth = max(depth - 1, 0)
            index += 1
            continue

        if depth == 0 and text.startswith(op, index):
            prev_char = text[index - 1] if index > 0 else " "
            next_char = text[index + len(op)] if index + len(op) < len(text) else " "
            before_ok = prev_char.isspace() or prev_char in "()"
            after_ok = next_char.isspace() or next_char in "()"

            if before_ok and after_ok:
                parts.append(text[start:index].strip())
                index += len(op)
                start = index
                continue

        index += 1

    parts.append(text[start:].strip())

    if len(parts) <= 1:
        return text, 0

    removed = 0

    if op == "NOT":
        first_part = re.sub(r"\s+", " ", parts[0]).strip()
        if not first_part:
            return text, 0

        deduplicated_parts: list[str] = [first_part]
        seen_exclusions: set[str] = set()

        for part in parts[1:]:
            normalized_part = re.sub(r"\s+", " ", part).strip()
            if not normalized_part:
                continue
            if normalized_part in seen_exclusions:
                removed += 1
                continue
            seen_exclusions.add(normalized_part)
            deduplicated_parts.append(normalized_part)

        if len(deduplicated_parts) <= 1:
            return first_part, removed

        return " NOT ".join(deduplicated_parts), removed

    seen: set[str] = set()
    deduplicated_parts: list[str] = []

    for part in parts:
        normalized_part = re.sub(r"\s+", " ", part).strip()
        if not normalized_part:
            continue
        if normalized_part in seen:
            removed += 1
            continue
        seen.add(normalized_part)
        deduplicated_parts.append(normalized_part)

    if not deduplicated_parts:
        return text, 0

    return f" {op} ".join(deduplicated_parts), removed


def deduplicate_boolean_clauses(text: str, operator: str) -> tuple[str, int]:
    op = operator.strip().upper()

    def process(segment: str) -> tuple[str, int]:
        rebuilt_parts: list[str] = []
        removed_total = 0
        index = 0

        while index < len(segment):
            char = segment[index]

            if char != "(":
                rebuilt_parts.append(char)
                index += 1
                continue

            end_index = find_matching_parenthesis(segment, index)
            if end_index < 0:
                rebuilt_parts.append(char)
                index += 1
                continue

            inner_text = segment[index + 1 : end_index]
            processed_inner, removed_inner = process(inner_text)
            removed_total += removed_inner
            rebuilt_parts.append(f"({processed_inner})")
            index = end_index + 1

        rebuilt = "".join(rebuilt_parts)
        deduped, removed_here = deduplicate_top_level_boolean_clauses(rebuilt, op)
        removed_total += removed_here
        return deduped, removed_total

    return process(text)


def translate_pubmed_query(
    query_text: str,
    target_key: str,
    *,
    dedup_and_clauses: bool = False,
    dedup_not_clauses: bool = False,
) -> tuple[str, list[str]]:
    config = TARGETS[target_key]
    warnings: list[str] = []

    def replace_field_term(match: re.Match[str]) -> str:
        term = normalize_text(match.group("term"))
        raw_tag = normalize_text(match.group("tag"))
        normalized_tag = normalize_field_tag(raw_tag)

        if normalized_tag not in config.field_wrappers:
            wrapper = config.default_wrapper
            warnings.append(
                f"{TARGETS[target_key].display_name}: unknown PubMed field tag `[{raw_tag}]` mapped to default wrapper."
            )
        else:
            wrapper = config.field_wrappers[normalized_tag]
            if normalized_tag == "mesh":
                warnings.append(
                    f"{TARGETS[target_key].display_name}: MeSH tag `[{raw_tag}]` approximated with free-text field wrapper."
                )
            elif normalized_tag == "all":
                warnings.append(
                    f"{TARGETS[target_key].display_name}: broad field tag `[{raw_tag}]` approximated with free-text field wrapper."
                )

        return wrap_template(wrapper, term)

    translated = FIELD_TERM_RE.sub(replace_field_term, query_text)
    translated = normalize_boolean_operators(translated)
    translated = collapse_whitespace_per_line(translated)
    translated, removed_or_duplicates = deduplicate_boolean_clauses(translated, "OR")

    removed_and_duplicates = 0
    if dedup_and_clauses:
        translated, removed_and_duplicates = deduplicate_boolean_clauses(translated, "AND")

    removed_not_duplicates = 0
    if dedup_not_clauses:
        translated, removed_not_duplicates = deduplicate_boolean_clauses(translated, "NOT")

    translated = collapse_whitespace_per_line(translated)

    if removed_or_duplicates > 0:
        warnings.append(
            f"{TARGETS[target_key].display_name}: removed {removed_or_duplicates} duplicate OR clause(s) after field mapping."
        )

    if removed_and_duplicates > 0:
        warnings.append(
            f"{TARGETS[target_key].display_name}: removed {removed_and_duplicates} duplicate AND clause(s) after field mapping (--dedup-and-clauses)."
        )

    if removed_not_duplicates > 0:
        warnings.append(
            f"{TARGETS[target_key].display_name}: removed {removed_not_duplicates} duplicate NOT clause(s) after field mapping (--dedup-not-clauses)."
        )

    return translated, deduplicate_messages(warnings)


def write_target_files(target_queries: dict[str, str], output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []

    for target_key, query in target_queries.items():
        filename = f"polyglot_{target_key}.txt"
        path = output_dir / filename
        path.write_text(query + "\n", encoding="utf-8")
        paths.append(path)

    return paths


def build_summary_markdown(
    *,
    source_label: str,
    input_query: str,
    translated_queries: dict[str, str],
    warnings: list[str],
    target_file_paths: list[Path],
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = []
    lines.append("# Polyglot Search Translation")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs")
    lines.append("")
    lines.append(f"- Source PubMed query: `{source_label}`")
    lines.append(
        f"- Targets generated: {', '.join(TARGETS[target].display_name for target in translated_queries)}"
    )

    if target_file_paths:
        lines.append("- Target query files:")
        for file_path in target_file_paths:
            lines.append(f"  - `{file_path.as_posix()}`")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append(
        "- This is a syntax translation helper; final search strings still require manual QA in each database UI."
    )
    lines.append(
        "- Controlled-vocabulary tags (e.g., MeSH) are approximated as free-text fields in non-PubMed targets."
    )

    if warnings:
        lines.append("- Translation warnings:")
        for warning in warnings:
            lines.append(f"  - {warning}")

    lines.append("")
    lines.append("## PubMed Source")
    lines.append("")
    lines.append("```text")
    lines.append(input_query)
    lines.append("```")

    for target_key, query in translated_queries.items():
        lines.append("")
        lines.append(f"## {TARGETS[target_key].display_name}")
        lines.append("")
        lines.append("```text")
        lines.append(query)
        lines.append("```")

    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Translate a PubMed query into Scopus/Web of Science/PsycINFO syntax "
            "(Polyglot Search Translator style)."
        )
    )
    parser.add_argument(
        "--input",
        default="../01_protocol/pubmed_query_v0.2.txt",
        help="Path to source PubMed query text or markdown with PubMed code block",
    )
    parser.add_argument(
        "--query",
        default="",
        help="Inline PubMed query text (overrides --input)",
    )
    parser.add_argument(
        "--targets",
        default="scopus,wos,psycinfo",
        help="Comma-separated targets: scopus,wos,psycinfo",
    )
    parser.add_argument(
        "--summary-output",
        default="outputs/polyglot_search_summary.md",
        help="Path to markdown summary output",
    )
    parser.add_argument(
        "--target-output-dir",
        default="outputs/polyglot_queries",
        help="Directory for per-target translated query txt files",
    )
    parser.add_argument(
        "--write-target-files",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write per-target query txt files",
    )
    parser.add_argument(
        "--dedup-and-clauses",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable optional removal of duplicate AND clauses after target field mapping.",
    )
    parser.add_argument(
        "--dedup-not-clauses",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable optional removal of duplicate NOT clauses after target field mapping.",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    targets = parse_targets(args.targets)

    input_query, source_label = load_pubmed_query(input_path, args.query)

    translated_queries: dict[str, str] = {}
    all_warnings: list[str] = []
    for target in targets:
        translated_query, warnings = translate_pubmed_query(
            input_query,
            target,
            dedup_and_clauses=args.dedup_and_clauses,
            dedup_not_clauses=args.dedup_not_clauses,
        )
        translated_queries[target] = translated_query
        all_warnings.extend(warnings)

    all_warnings = deduplicate_messages(all_warnings)

    target_file_paths: list[Path] = []
    if args.write_target_files:
        target_file_paths = write_target_files(translated_queries, Path(args.target_output_dir))

    summary_text = build_summary_markdown(
        source_label=source_label,
        input_query=input_query,
        translated_queries=translated_queries,
        warnings=all_warnings,
        target_file_paths=target_file_paths,
    )

    summary_output_path = Path(args.summary_output)
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(summary_text, encoding="utf-8")

    print(f"Wrote: {summary_output_path}")
    if target_file_paths:
        for path in target_file_paths:
            print(f"Wrote: {path}")
    print(f"Targets generated: {len(translated_queries)}")
    print(f"Warnings: {len(all_warnings)}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import re
import sys
import warnings
import xml.etree.ElementTree as ET
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from prospero_submission_drafter_layers.builder import (
    build_draft_artifacts,
    build_submission_context,
)
from prospero_submission_drafter_layers.builder import (
    build_prefill_fields as build_prefill_fields_layer,
)
from prospero_submission_drafter_layers.field_composition import (
    field_value_map,
    is_required_in_mode,
    prospero_field_templates,
)
from prospero_submission_drafter_layers.formatting import (
    extract_latex_macro_argument,
    latex_to_plain,
    strip_latex_macro_with_argument,
    strip_markdown_markup,
)
from prospero_submission_drafter_layers.io import (
    load_autofill_profile,
    read_protocol_text,
    resolve_cli_paths,
    resolve_preset_manuscript_path,
    write_text_artifact,
)
from prospero_submission_drafter_layers.models import (
    ManuscriptMetadata,
    MarkdownSection,
    PrefillField,
    ProtocolData,
)
from prospero_submission_drafter_layers.rules import (
    completion_counts,
    evaluate_exit_conditions,
    required_scope_label,
    unresolved_placeholders_in_fields,
)

HEADING_PATTERN = re.compile(r"^(#{1,6})\s+(.+?)\s*$")
BULLET_PATTERN = re.compile(r"^\s*[-*+]\s+(.*\S)\s*$")
NUMBERED_PATTERN = re.compile(r"^\s*\d+[.)]\s+(.*\S)\s*$")
LAST_UPDATED_PATTERN = re.compile(r"^Last updated:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
PROSPERO_ID_PATTERN = re.compile(r"\bCRD\d{8,14}\b", re.IGNORECASE)
PLACEHOLDER_PATTERN = re.compile(r"\[[A-Z][A-Z0-9_ ]{1,}\]")


PROSPERO_SCHEMA_NAME = "PROSPERO Registration Form (47 fields)"
PROSPERO_SCHEMA_SOURCE = (
    "https://pmc.ncbi.nlm.nih.gov/articles/PMC11617274/ "
    "(Appendix 1 reproducing PROSPERO registration form fields)"
)


def declaration_statement(tex_text: str, label: str) -> str:
    pattern = re.compile(rf"\\textbf\{{{re.escape(label)}\.\}}\s*(.+?)\n\n", re.DOTALL)
    match = pattern.search(tex_text)
    if not match:
        return ""
    return latex_to_plain(match.group(1))


def infer_country_from_affiliation(affiliation: str) -> str:
    if not affiliation:
        return ""
    parts = [part.strip() for part in affiliation.split(",") if part.strip()]
    if not parts:
        return ""
    return parts[-1]


def parse_bn_pilot_manuscript(manuscript_path: Path) -> ManuscriptMetadata:
    if not manuscript_path.exists():
        raise SystemExit(f"BN preset manuscript file not found: {manuscript_path}")

    tex_text = manuscript_path.read_text(encoding="utf-8")

    raw_title = extract_latex_macro_argument(tex_text, "title")
    raw_author = extract_latex_macro_argument(tex_text, "author")
    raw_date = extract_latex_macro_argument(tex_text, "date")

    author_without_thanks = strip_latex_macro_with_argument(raw_author, "thanks")
    author_lines = [line.strip() for line in author_without_thanks.split("\\\\") if line.strip()]
    contact_name = latex_to_plain(author_lines[0]) if author_lines else ""
    affiliation = latex_to_plain(author_lines[1]) if len(author_lines) > 1 else ""

    email_match = re.search(r"mailto:([^}\s]+)", raw_author)
    contact_email = email_match.group(1).strip() if email_match else ""

    keywords_match = re.search(r"\\textbf\{Keywords:\}\s*(.+?)\n", tex_text)
    keyword_text = latex_to_plain(keywords_match.group(1)) if keywords_match else ""
    keywords = [item.strip() for item in keyword_text.split(";") if item.strip()]

    return ManuscriptMetadata(
        title=latex_to_plain(raw_title),
        contact_name=contact_name,
        contact_email=contact_email,
        affiliation=affiliation,
        country=infer_country_from_affiliation(affiliation),
        date_label=latex_to_plain(raw_date),
        keywords=keywords,
        funding_statement=declaration_statement(tex_text, "Funding"),
        conflicts_statement=declaration_statement(tex_text, "Conflicts of interest"),
        data_availability_statement=declaration_statement(tex_text, "Data availability"),
    )


def default_manuscript_path_for_preset(preset: str) -> str:
    if preset == "bn-pilot":
        return "../../2026-bn-pilot/04_manuscript/main.tex"
    return ""


def load_preset_metadata(
    preset: str, manuscript_arg: str
) -> tuple[ManuscriptMetadata | None, Path | None]:
    if preset == "none":
        return None, None

    manuscript_path = resolve_preset_manuscript_path(
        preset=preset,
        manuscript_arg=manuscript_arg,
        default_path_for_preset=default_manuscript_path_for_preset,
    )

    if preset == "bn-pilot":
        return parse_bn_pilot_manuscript(manuscript_path), manuscript_path

    return None, manuscript_path


def canonical_heading(title: str) -> str:
    text = strip_markdown_markup(title).lower()
    text = re.sub(r"^\d+\)\s*", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_markdown_sections(markdown_text: str) -> list[MarkdownSection]:
    sections: list[MarkdownSection] = []
    current: MarkdownSection | None = None

    for raw_line in markdown_text.splitlines():
        heading_match = HEADING_PATTERN.match(raw_line.strip())
        if heading_match:
            if current is not None:
                sections.append(current)
            level = len(heading_match.group(1))
            title = heading_match.group(2).strip()
            current = MarkdownSection(level=level, title=title, content_lines=[])
            continue

        if current is not None:
            current.content_lines.append(raw_line.rstrip())

    if current is not None:
        sections.append(current)
    return sections


def get_section(sections: list[MarkdownSection], heading: str) -> MarkdownSection | None:
    target = canonical_heading(heading)
    for section in sections:
        if canonical_heading(section.title) == target:
            return section
    return None


def get_child_sections(
    sections: list[MarkdownSection], parent_heading: str
) -> dict[str, MarkdownSection]:
    parent = get_section(sections, parent_heading)
    if parent is None:
        return {}

    parent_index = sections.index(parent)
    child_sections: dict[str, MarkdownSection] = {}

    for section in sections[parent_index + 1 :]:
        if section.level <= parent.level:
            break
        if section.level == parent.level + 1:
            child_sections[canonical_heading(section.title)] = section

    return child_sections


def extract_list_items(lines: list[str]) -> list[str]:
    items: list[str] = []
    for line in lines:
        bullet_match = BULLET_PATTERN.match(line)
        numbered_match = NUMBERED_PATTERN.match(line)
        if bullet_match:
            items.append(strip_markdown_markup(bullet_match.group(1)))
        elif numbered_match:
            items.append(strip_markdown_markup(numbered_match.group(1)))
    return [item for item in items if item]


def extract_first_paragraph(lines: list[str]) -> str:
    paragraph_parts: list[str] = []
    started = False

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if started:
                break
            continue
        if BULLET_PATTERN.match(stripped) or NUMBERED_PATTERN.match(stripped):
            continue
        if HEADING_PATTERN.match(stripped):
            continue

        paragraph_parts.append(strip_markdown_markup(stripped))
        started = True

    if paragraph_parts:
        return strip_markdown_markup(" ".join(paragraph_parts))
    return ""


def merge_section_text(lines: list[str]) -> str:
    paragraph = extract_first_paragraph(lines)
    items = extract_list_items(lines)
    if paragraph and items:
        return f"{paragraph} {'; '.join(items)}"
    if items:
        return "; ".join(items)
    return paragraph


def split_information_sources(lines: list[str]) -> tuple[list[str], list[str]]:
    information_sources: list[str] = []
    gray_sources: list[str] = []
    collecting_gray = False

    for line in lines:
        stripped_lower = line.strip().lower()
        if "optional gray literature" in stripped_lower:
            collecting_gray = True
            continue

        bullet_match = BULLET_PATTERN.match(line)
        if not bullet_match:
            continue

        item = strip_markdown_markup(bullet_match.group(1))
        if collecting_gray:
            gray_sources.append(item)
        else:
            information_sources.append(item)

    return information_sources, gray_sources


def find_first_by_prefix(values: list[str], prefix: str) -> str:
    for value in values:
        if value.lower().startswith(prefix.lower()):
            return value.split(":", 1)[-1].strip() if ":" in value else value
    return ""


def extract_protocol_data(markdown_text: str) -> ProtocolData:
    sections = parse_markdown_sections(markdown_text)

    working_title_section = get_section(sections, "Working title")
    review_type_section = get_section(sections, "Review type")
    objective_section = get_section(sections, "Objective")
    core_questions_section = get_section(sections, "Core review questions")
    eligibility_section = get_child_sections(sections, "Eligibility criteria")
    information_sources_section = get_section(sections, "Information sources")
    screening_section = get_section(sections, "Screening workflow")
    extraction_section = get_section(sections, "Data extraction fields")
    quality_section = get_section(sections, "Quality appraisal")
    synthesis_section = get_section(sections, "Synthesis plan")
    reproducibility_section = get_section(sections, "Reproducibility notes")

    core_questions = (
        extract_list_items(core_questions_section.content_lines) if core_questions_section else []
    )
    publication_time_language = extract_list_items(
        eligibility_section.get("time and language", MarkdownSection(0, "", [])).content_lines
    )
    information_sources, gray_sources = split_information_sources(
        information_sources_section.content_lines if information_sources_section else []
    )

    last_updated_match = LAST_UPDATED_PATTERN.search(markdown_text)
    registration_match = PROSPERO_ID_PATTERN.search(markdown_text)
    unresolved_placeholders = sorted(
        set(match.group(0) for match in PLACEHOLDER_PATTERN.finditer(markdown_text))
    )

    return ProtocolData(
        last_updated=strip_markdown_markup(last_updated_match.group(1))
        if last_updated_match
        else "",
        registration_id=registration_match.group(0).upper() if registration_match else "",
        unresolved_placeholders=unresolved_placeholders,
        working_title=merge_section_text(working_title_section.content_lines)
        if working_title_section
        else "",
        review_type=merge_section_text(review_type_section.content_lines)
        if review_type_section
        else "",
        objective=merge_section_text(objective_section.content_lines) if objective_section else "",
        core_questions=core_questions,
        population=merge_section_text(
            eligibility_section.get("population", MarkdownSection(0, "", [])).content_lines
        ),
        concepts_exposures=merge_section_text(
            eligibility_section.get(
                "concepts / exposures", MarkdownSection(0, "", [])
            ).content_lines
        ),
        outcomes=merge_section_text(
            eligibility_section.get("outcomes", MarkdownSection(0, "", [])).content_lines
        ),
        study_types=merge_section_text(
            eligibility_section.get("study types", MarkdownSection(0, "", [])).content_lines
        ),
        publication_years=find_first_by_prefix(publication_time_language, "publication years"),
        languages=find_first_by_prefix(publication_time_language, "languages"),
        exclusion_criteria=merge_section_text(
            eligibility_section.get("exclusion criteria", MarkdownSection(0, "", [])).content_lines
        ),
        information_sources=information_sources,
        gray_sources=gray_sources,
        screening_workflow=extract_list_items(screening_section.content_lines)
        if screening_section
        else [],
        extraction_fields=extract_list_items(extraction_section.content_lines)
        if extraction_section
        else [],
        quality_appraisal=merge_section_text(quality_section.content_lines)
        if quality_section
        else "",
        synthesis_plan=extract_list_items(synthesis_section.content_lines)
        if synthesis_section
        else [],
        reproducibility_notes=extract_list_items(reproducibility_section.content_lines)
        if reproducibility_section
        else [],
    )


def resolve_registration_mode(*, requested_mode: str, protocol_data: ProtocolData) -> str:
    if requested_mode in {"new", "update"}:
        return requested_mode
    return "update" if protocol_data.registration_id else "new"


def _build_prefill_fields_canonical(
    protocol_data: ProtocolData,
    *,
    registration_mode: str,
    manuscript_metadata: ManuscriptMetadata | None = None,
    auto_complete: bool = False,
    profile_values: dict[str, str] | None = None,
) -> list[PrefillField]:
    return build_prefill_fields_layer(
        protocol_data,
        registration_mode=registration_mode,
        manuscript_metadata=manuscript_metadata,
        auto_complete=auto_complete,
        profile_values=profile_values,
        field_templates_fn=prospero_field_templates,
        field_value_map_fn=field_value_map,
        is_required_in_mode_fn=is_required_in_mode,
    )


def build_prefill_fields(
    protocol_data: ProtocolData,
    *,
    registration_mode: str,
    manuscript_metadata: ManuscriptMetadata | None = None,
    auto_complete: bool = False,
    profile_values: dict[str, str] | None = None,
) -> list[PrefillField]:
    warnings.warn(
        "Python-level access via `prospero_submission_drafter.build_prefill_fields` is deprecated; "
        "use `prospero_submission_drafter_layers.builder.build_prefill_fields` with "
        "`prospero_submission_drafter_layers.field_composition` callbacks.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _build_prefill_fields_canonical(
        protocol_data,
        registration_mode=registration_mode,
        manuscript_metadata=manuscript_metadata,
        auto_complete=auto_complete,
        profile_values=profile_values,
    )


def render_structured_draft(
    *,
    fields: list[PrefillField],
    protocol_path: Path,
    protocol_data: ProtocolData,
    registration_mode: str,
    preset: str,
    preset_manuscript_path: Path | None,
    generated_at: str,
) -> str:
    counts = completion_counts(fields)
    lines: list[str] = []

    lines.append("# PROSPERO Registration Draft (Pre-filled)")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append(f"Source protocol: `{protocol_path.as_posix()}`")
    lines.append(f"Registration mode: `{registration_mode}`")
    lines.append(f"Preset: `{preset}`")
    if preset_manuscript_path is not None:
        lines.append(f"Preset manuscript: `{preset_manuscript_path.as_posix()}`")
    lines.append(f"Field schema: {PROSPERO_SCHEMA_NAME}")
    lines.append(f"Schema source: {PROSPERO_SCHEMA_SOURCE}")
    if protocol_data.last_updated:
        lines.append(f"Protocol last updated: {protocol_data.last_updated}")
    lines.append("")
    lines.append("## Completion Snapshot")
    lines.append("")
    lines.append(f"- Fields mapped: {counts['total_fields']}")
    lines.append(f"- Completed fields: {counts['total_complete']}")
    lines.append(
        f"- Required fields completed: {counts['required_complete']}/{counts['required_total']}"
    )
    lines.append(f"- Required fields needing input: {counts['required_missing']}")

    if protocol_data.unresolved_placeholders:
        lines.append(
            "- Unresolved protocol template placeholders: "
            + ", ".join(f"`{placeholder}`" for placeholder in protocol_data.unresolved_placeholders)
        )

    lines.append("")
    lines.append("## Field Draft")
    lines.append("")

    for field in fields:
        lines.append(f"### {field.number}. {field.label} (`{field.field_id}`)")
        lines.append(f"- Status: `{field.status}`")
        lines.append(f"- Required: {'yes' if field.required else 'no'}")
        lines.append(f"- Required scope: `{required_scope_label(field.required_scope)}`")
        lines.append(f"- Source: `{field.source_section or 'manual_input'}`")
        if field.value.strip():
            lines.append("- Value:")
            lines.append("```text")
            lines.append(field.value.strip())
            lines.append("```")
        else:
            lines.append("- Value: `[TO FILL]`")

        if field.notes.strip():
            lines.append(f"- Notes: {field.notes.strip()}")
        lines.append("")

    return "\n".join(lines)


def render_xml_draft(
    *,
    fields: list[PrefillField],
    protocol_path: Path,
    protocol_data: ProtocolData,
    registration_mode: str,
    preset: str,
    preset_manuscript_path: Path | None,
    generated_at: str,
) -> str:
    counts = completion_counts(fields)
    root = ET.Element("prosperoRegistrationDraft")
    root.set("generatedAt", generated_at)
    root.set("sourceProtocol", protocol_path.as_posix())
    root.set("registrationMode", registration_mode)
    root.set("preset", preset)
    if preset_manuscript_path is not None:
        root.set("presetManuscript", preset_manuscript_path.as_posix())
    root.set("schemaName", PROSPERO_SCHEMA_NAME)
    root.set("schemaSource", PROSPERO_SCHEMA_SOURCE)
    root.set("schemaVersion", "1.0")

    if protocol_data.last_updated:
        root.set("protocolLastUpdated", protocol_data.last_updated)

    snapshot = ET.SubElement(root, "completionSnapshot")
    ET.SubElement(snapshot, "totalFields").text = str(counts["total_fields"])
    ET.SubElement(snapshot, "totalComplete").text = str(counts["total_complete"])
    ET.SubElement(snapshot, "requiredTotal").text = str(counts["required_total"])
    ET.SubElement(snapshot, "requiredComplete").text = str(counts["required_complete"])
    ET.SubElement(snapshot, "requiredMissing").text = str(counts["required_missing"])

    placeholders = ET.SubElement(root, "unresolvedPlaceholders")
    for placeholder in protocol_data.unresolved_placeholders:
        ET.SubElement(placeholders, "placeholder").text = placeholder

    field_list = ET.SubElement(root, "fields")
    for field in fields:
        field_node = ET.SubElement(field_list, "field")
        field_node.set("number", str(field.number))
        field_node.set("id", field.field_id)
        field_node.set("required", "true" if field.required else "false")
        field_node.set("requiredScope", field.required_scope)
        field_node.set("status", field.status)
        ET.SubElement(field_node, "label").text = field.label
        ET.SubElement(field_node, "source").text = field.source_section or "manual_input"
        ET.SubElement(field_node, "value").text = field.value.strip()
        ET.SubElement(field_node, "notes").text = field.notes.strip()

    ET.indent(root, space="  ")
    xml_body = ET.tostring(root, encoding="unicode")
    return f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_body}\n'


def render_summary(
    *,
    protocol_path: Path,
    structured_output_path: Path,
    xml_output_path: Path,
    fields: list[PrefillField],
    protocol_data: ProtocolData,
    registration_mode: str,
    preset: str,
    preset_manuscript_path: Path | None,
    generated_at: str,
) -> str:
    counts = completion_counts(fields)
    unresolved_in_draft = unresolved_placeholders_in_fields(fields)
    missing_required = [
        f"{field.number}. {field.label}"
        for field in fields
        if field.required and field.status != "complete"
    ]

    lines: list[str] = []
    lines.append("# PROSPERO Submission Drafter Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Schema")
    lines.append("")
    lines.append(f"- Registration mode: `{registration_mode}`")
    lines.append(f"- Preset: `{preset}`")
    if preset_manuscript_path is not None:
        lines.append(f"- Preset manuscript: `{preset_manuscript_path.as_posix()}`")
    lines.append(f"- Field schema: {PROSPERO_SCHEMA_NAME}")
    lines.append(f"- Schema source: {PROSPERO_SCHEMA_SOURCE}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Protocol input: `{protocol_path.as_posix()}`")
    lines.append(f"- Structured draft output: `{structured_output_path.as_posix()}`")
    lines.append(f"- XML draft output: `{xml_output_path.as_posix()}`")
    lines.append("")
    lines.append("## Completion")
    lines.append("")
    lines.append(f"- Total fields mapped: {counts['total_fields']}")
    lines.append(f"- Complete fields: {counts['total_complete']}")
    lines.append(
        f"- Required fields complete: {counts['required_complete']}/{counts['required_total']}"
    )
    lines.append(f"- Required fields needing input: {counts['required_missing']}")
    lines.append("")
    lines.append("## Missing Required Fields")
    lines.append("")
    if missing_required:
        for label in missing_required:
            lines.append(f"- {label}")
    else:
        lines.append("- None")
    lines.append("")
    lines.append("## Placeholder Check")
    lines.append("")
    if protocol_data.unresolved_placeholders:
        lines.append(
            "- Unresolved placeholders in protocol source: "
            + ", ".join(f"`{placeholder}`" for placeholder in protocol_data.unresolved_placeholders)
        )
    else:
        lines.append("- No unresolved placeholders detected in protocol source.")

    if unresolved_in_draft:
        lines.append(
            "- Unresolved placeholders in generated draft: "
            + ", ".join(f"`{placeholder}`" for placeholder in unresolved_in_draft)
        )
    else:
        lines.append("- No unresolved placeholders remain in generated draft fields.")
    lines.append("")
    return "\n".join(lines)


def parser() -> argparse.ArgumentParser:
    cli_parser = argparse.ArgumentParser(
        description=(
            "Draft a pre-filled PROSPERO registration package from 01_protocol/protocol.md "
            "(structured markdown + XML; aligned to the 47-field PROSPERO form schema)."
        )
    )
    cli_parser.add_argument(
        "--protocol", default="../01_protocol/protocol.md", help="Path to protocol markdown input"
    )
    cli_parser.add_argument(
        "--structured-output",
        default="outputs/prospero_registration_prefill.md",
        help="Path to structured markdown output",
    )
    cli_parser.add_argument(
        "--xml-output",
        default="outputs/prospero_registration_prefill.xml",
        help="Path to XML output",
    )
    cli_parser.add_argument(
        "--summary-output",
        default="outputs/prospero_submission_drafter_summary.md",
        help="Path to markdown summary output",
    )
    cli_parser.add_argument(
        "--format",
        choices=["structured", "xml", "both"],
        default="both",
        help="Which outputs to generate",
    )
    cli_parser.add_argument(
        "--registration-mode",
        choices=["auto", "new", "update"],
        default="auto",
        help=(
            "Registration mode used for required-field logic. "
            "`auto` infers `update` if a CRD ID is found in protocol, otherwise `new`."
        ),
    )
    cli_parser.add_argument(
        "--preset",
        choices=["none", "bn-pilot"],
        default="none",
        help="Optional metadata preset for autofill of contact/affiliation fields.",
    )
    cli_parser.add_argument(
        "--manuscript",
        default="",
        help=(
            "Optional manuscript path used by presets. "
            "For `--preset bn-pilot`, defaults to `../../2026-bn-pilot/04_manuscript/main.tex`."
        ),
    )
    cli_parser.add_argument(
        "--auto-complete",
        action="store_true",
        help=(
            "Auto-fill missing required fields and unresolved placeholders using heuristic defaults "
            "(plus optional profile overrides)."
        ),
    )
    cli_parser.add_argument(
        "--autofill-profile",
        default="",
        help=(
            'Optional JSON file with field overrides. Format: {"fields": {"field_id": "value"}} '
            "or direct field map."
        ),
    )
    cli_parser.add_argument(
        "--fail-on-missing-required",
        action="store_true",
        help="Exit with non-zero code if required fields are still incomplete",
    )
    cli_parser.add_argument(
        "--fail-on-placeholders",
        action="store_true",
        help="Exit with non-zero code if unresolved [PLACEHOLDER] tokens remain in protocol",
    )
    return cli_parser


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)

    (
        protocol_path,
        structured_output_path,
        xml_output_path,
        summary_output_path,
    ) = resolve_cli_paths(
        protocol_arg=args.protocol,
        structured_output_arg=args.structured_output,
        xml_output_arg=args.xml_output,
        summary_output_arg=args.summary_output,
    )

    protocol_text = read_protocol_text(protocol_path)
    protocol_data = extract_protocol_data(protocol_text)
    manuscript_metadata, preset_manuscript_path = load_preset_metadata(args.preset, args.manuscript)
    profile_values: dict[str, str] = {}
    if args.autofill_profile:
        profile_values = load_autofill_profile(Path(args.autofill_profile))

    context = build_submission_context(
        protocol_data=protocol_data,
        manuscript_metadata=manuscript_metadata,
        preset_manuscript_path=preset_manuscript_path,
        requested_registration_mode=args.registration_mode,
        auto_complete=args.auto_complete,
        profile_values=profile_values,
        resolve_registration_mode_fn=resolve_registration_mode,
        build_prefill_fields_fn=_build_prefill_fields_canonical,
    )

    artifacts = build_draft_artifacts(
        context=context,
        protocol_path=protocol_path,
        structured_output_path=structured_output_path,
        xml_output_path=xml_output_path,
        preset=args.preset,
        output_format=args.format,
        render_structured_draft_fn=render_structured_draft,
        render_xml_draft_fn=render_xml_draft,
        render_summary_fn=render_summary,
    )

    wrote_paths: list[Path] = []
    if artifacts.structured_text is not None:
        wrote_paths.append(write_text_artifact(structured_output_path, artifacts.structured_text))
    if artifacts.xml_text is not None:
        wrote_paths.append(write_text_artifact(xml_output_path, artifacts.xml_text))
    wrote_paths.append(write_text_artifact(summary_output_path, artifacts.summary_text))

    for wrote_path in wrote_paths:
        print(f"Wrote: {wrote_path}")

    counts = completion_counts(context.fields)
    unresolved_in_draft = unresolved_placeholders_in_fields(context.fields)
    exit_code, exit_messages = evaluate_exit_conditions(
        fail_on_missing_required=args.fail_on_missing_required,
        fail_on_placeholders=args.fail_on_placeholders,
        auto_complete=args.auto_complete,
        required_missing=counts["required_missing"],
        unresolved_in_draft=unresolved_in_draft,
        unresolved_protocol_placeholders=context.protocol_data.unresolved_placeholders,
    )
    for message in exit_messages:
        print(message)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET


HEADING_PATTERN = re.compile(r"^(#{1,6})\s+(.+?)\s*$")
BULLET_PATTERN = re.compile(r"^\s*[-*+]\s+(.*\S)\s*$")
NUMBERED_PATTERN = re.compile(r"^\s*\d+[.)]\s+(.*\S)\s*$")
LAST_UPDATED_PATTERN = re.compile(r"^Last updated:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
PROSPERO_ID_PATTERN = re.compile(r"\bCRD\d{8,14}\b", re.IGNORECASE)
PLACEHOLDER_PATTERN = re.compile(r"\[[A-Z][A-Z0-9_ ]{1,}\]")
ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@dataclass
class MarkdownSection:
    level: int
    title: str
    content_lines: list[str]


@dataclass
class PrefillField:
    number: int
    field_id: str
    label: str
    required: bool
    required_scope: str
    value: str
    source_section: str
    notes: str
    status: str = "needs_input"


@dataclass
class ProtocolData:
    last_updated: str
    registration_id: str
    unresolved_placeholders: list[str]
    working_title: str
    review_type: str
    objective: str
    core_questions: list[str]
    population: str
    concepts_exposures: str
    outcomes: str
    study_types: str
    publication_years: str
    languages: str
    exclusion_criteria: str
    information_sources: list[str]
    gray_sources: list[str]
    screening_workflow: list[str]
    extraction_fields: list[str]
    quality_appraisal: str
    synthesis_plan: list[str]
    reproducibility_notes: list[str]


@dataclass
class ManuscriptMetadata:
    title: str
    contact_name: str
    contact_email: str
    affiliation: str
    country: str
    date_label: str
    keywords: list[str]
    funding_statement: str
    conflicts_statement: str
    data_availability_statement: str


@dataclass(frozen=True)
class ProsperoFieldTemplate:
    number: int
    field_id: str
    label: str
    required_scope: str
    source_section: str
    notes: str


PROSPERO_SCHEMA_NAME = "PROSPERO Registration Form (47 fields)"
PROSPERO_SCHEMA_SOURCE = (
    "https://pmc.ncbi.nlm.nih.gov/articles/PMC11617274/ "
    "(Appendix 1 reproducing PROSPERO registration form fields)"
)


def strip_markdown_markup(value: str) -> str:
    text = value.strip()
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
    text = re.sub(r"\*([^*]+)\*", r"\1", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def extract_braced_content(text: str, brace_start_index: int) -> tuple[str, int] | None:
    if brace_start_index >= len(text) or text[brace_start_index] != "{":
        return None

    depth = 0
    chars: list[str] = []
    index = brace_start_index
    while index < len(text):
        char = text[index]

        if char == "\\":
            if depth > 0:
                chars.append(char)
            if index + 1 < len(text):
                if depth > 0:
                    chars.append(text[index + 1])
                index += 2
                continue

        if char == "{":
            depth += 1
            if depth > 1:
                chars.append(char)
            index += 1
            continue

        if char == "}":
            depth -= 1
            if depth == 0:
                return "".join(chars), index + 1
            chars.append(char)
            index += 1
            continue

        if depth > 0:
            chars.append(char)
        index += 1

    return None


def extract_latex_macro_argument(text: str, macro: str) -> str:
    pattern = re.compile(rf"\\{macro}\s*", re.DOTALL)
    match = pattern.search(text)
    if not match:
        return ""

    brace_start = text.find("{", match.end())
    if brace_start == -1:
        return ""

    extracted = extract_braced_content(text, brace_start)
    if extracted is None:
        return ""
    content, _ = extracted
    return content.strip()


def strip_latex_macro_with_argument(text: str, macro: str) -> str:
    output: list[str] = []
    index = 0
    needle = f"\\{macro}"

    while index < len(text):
        macro_index = text.find(needle, index)
        if macro_index == -1:
            output.append(text[index:])
            break

        output.append(text[index:macro_index])
        cursor = macro_index + len(needle)
        while cursor < len(text) and text[cursor].isspace():
            cursor += 1

        if cursor < len(text) and text[cursor] == "{":
            extracted = extract_braced_content(text, cursor)
            if extracted is None:
                index = cursor + 1
            else:
                _, cursor_after = extracted
                index = cursor_after
        else:
            index = cursor

    return "".join(output)


def latex_to_plain(text: str) -> str:
    value = text
    value = re.sub(r"\\href\{[^{}]*\}\{([^{}]*)\}", r"\1", value)
    value = re.sub(r"\\textbf\{([^{}]*)\}", r"\1", value)
    value = re.sub(r"\\textit\{([^{}]*)\}", r"\1", value)
    value = value.replace("\\\\", "\n")
    value = re.sub(r"\\[a-zA-Z]+", "", value)
    value = value.replace("{", "").replace("}", "")
    value = value.replace("~", " ")
    return collapse_whitespace(value)


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

    manuscript_path = (
        Path(manuscript_arg) if manuscript_arg else Path(default_manuscript_path_for_preset(preset))
    )

    if preset == "bn-pilot":
        return parse_bn_pilot_manuscript(manuscript_path), manuscript_path

    return None, manuscript_path


def load_autofill_profile(profile_path: Path) -> dict[str, str]:
    if not profile_path.exists():
        raise SystemExit(f"Autofill profile not found: {profile_path}")

    try:
        payload = json.loads(profile_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid autofill profile JSON: {profile_path} ({exc})") from exc

    if not isinstance(payload, dict):
        raise SystemExit("Autofill profile must be a JSON object.")

    fields = payload.get("fields", payload)
    if not isinstance(fields, dict):
        raise SystemExit("Autofill profile `fields` must be a JSON object (field_id -> value).")

    normalized: dict[str, str] = {}
    for key, value in fields.items():
        normalized[str(key).strip()] = str(value).strip()

    if "fields" in payload:
        for key, value in payload.items():
            if key == "fields":
                continue
            normalized[str(key).strip()] = str(value).strip()

    return normalized


def normalize_placeholder_token(token: str) -> str:
    raw = token.strip()
    if raw.startswith("[") and raw.endswith("]"):
        core = raw[1:-1]
    else:
        core = raw
    core = core.upper().replace(" ", "_")
    return f"[{core}]"


def placeholder_token_variants(token: str) -> set[str]:
    normalized = normalize_placeholder_token(token)
    core = normalized[1:-1]
    return {normalized, f"[{core.replace('_', ' ')}]"}


def replace_placeholders(text: str, replacements: dict[str, str]) -> str:
    updated = text
    for token, replacement in replacements.items():
        if not replacement:
            continue
        updated = updated.replace(token, replacement)
    return updated


def parse_date_label_to_iso(text: str) -> str:
    value = text.strip()
    if not value:
        return ""
    if ISO_DATE_PATTERN.match(value):
        return value

    parsed: datetime | None = None
    for fmt in ("%B %Y", "%b %Y", "%Y-%m", "%Y/%m", "%Y"):
        try:
            parsed = datetime.strptime(value, fmt)
            break
        except ValueError:
            continue

    if parsed is None:
        return ""

    if fmt == "%Y":
        return f"{parsed.year:04d}-01-01"

    return parsed.strftime("%Y-%m-01")


def infer_default_start_date(
    protocol_data: ProtocolData, manuscript_metadata: ManuscriptMetadata | None
) -> str:
    if manuscript_metadata is not None:
        manuscript_iso = parse_date_label_to_iso(manuscript_metadata.date_label)
        if manuscript_iso:
            return manuscript_iso

    protocol_iso = parse_date_label_to_iso(protocol_data.last_updated)
    if protocol_iso:
        return protocol_iso

    return datetime.now().strftime("%Y-%m-%d")


def infer_default_completion_date(start_date_iso: str) -> str:
    if ISO_DATE_PATTERN.match(start_date_iso):
        base = datetime.strptime(start_date_iso, "%Y-%m-%d")
        return (base + timedelta(days=180)).strftime("%Y-%m-%d")
    return (datetime.now() + timedelta(days=180)).strftime("%Y-%m-%d")


def default_placeholder_replacements(
    protocol_data: ProtocolData,
    manuscript_metadata: ManuscriptMetadata | None,
) -> dict[str, str]:
    title_fallback = (
        manuscript_metadata.title if manuscript_metadata else "Systematic review protocol"
    )

    replacements: dict[str, str] = {}
    canonical_defaults = {
        "[YOUR_REVIEW_TITLE]": title_fallback,
        "[POPULATION]": "target population",
        "[EXPOSURE_OR_CONCEPT]": "exposure/concept of interest",
        "[OUTCOME]": "outcome(s) of interest",
        "[ELIGIBLE_LANGUAGES]": "English",
        "[START_YEAR]": "2000",
    }
    for token, value in canonical_defaults.items():
        for variant in placeholder_token_variants(token):
            replacements[variant] = value

    if manuscript_metadata and manuscript_metadata.title:
        lower_title = manuscript_metadata.title.lower()
        if "bulimia" in lower_title and "women" in lower_title:
            replacements["[POPULATION]"] = "women with bulimia nervosa"
            replacements["[OUTCOME]"] = "identity-related indicators"
            replacements["[EXPOSURE_OR_CONCEPT]"] = "object-relations characteristics"

    if protocol_data.working_title and not PLACEHOLDER_PATTERN.search(protocol_data.working_title):
        replacements["[YOUR_REVIEW_TITLE]"] = protocol_data.working_title

    return replacements


def merge_placeholder_replacements(
    default_replacements: dict[str, str],
    profile_overrides: dict[str, str],
) -> dict[str, str]:
    merged = dict(default_replacements)
    for key, value in profile_overrides.items():
        for token in placeholder_token_variants(key):
            merged[token] = value
    return merged


def auto_complete_missing_required_fields(
    fields: list[PrefillField],
    *,
    protocol_data: ProtocolData,
    manuscript_metadata: ManuscriptMetadata | None,
    profile_values: dict[str, str],
) -> list[PrefillField]:
    defaults = {
        "review_start_date": infer_default_start_date(protocol_data, manuscript_metadata),
        "review_completion_date": "",
        "named_contact": manuscript_metadata.contact_name if manuscript_metadata else "Review lead",
        "named_contact_email": manuscript_metadata.contact_email
        if manuscript_metadata
        else "review-team@example.org",
        "organisational_affiliation": manuscript_metadata.affiliation
        if manuscript_metadata
        else "Independent research team",
        "review_team": (
            f"{manuscript_metadata.contact_name} ({manuscript_metadata.affiliation})"
            if manuscript_metadata
            and manuscript_metadata.contact_name
            and manuscript_metadata.affiliation
            else "Review team to be finalized"
        ),
        "funding_sources": (
            manuscript_metadata.funding_statement
            if manuscript_metadata and manuscript_metadata.funding_statement
            else "No external funding reported."
        ),
        "conflicts_of_interest": (
            manuscript_metadata.conflicts_statement
            if manuscript_metadata and manuscript_metadata.conflicts_statement
            else "No conflicts of interest reported."
        ),
        "country": manuscript_metadata.country
        if manuscript_metadata and manuscript_metadata.country
        else "Not specified",
        "language": "English",
        "subgroups": "None planned.",
        "review_status": "Ongoing review",
        "review_stage": "Protocol drafted; review not yet started.",
        "participants_population": "Target population as defined in protocol.",
        "interventions_exposures": "Exposure/intervention of interest as defined in protocol.",
        "main_outcomes": "Primary outcomes as defined in protocol.",
        "condition_or_domain": "Condition/domain specified in protocol objective.",
        "review_question": "Primary review question as defined in protocol.",
    }

    default_start = profile_values.get("review_start_date", defaults["review_start_date"])
    defaults["review_start_date"] = parse_date_label_to_iso(default_start) or default_start
    defaults["review_completion_date"] = profile_values.get(
        "review_completion_date",
        infer_default_completion_date(defaults["review_start_date"]),
    )

    placeholder_overrides = {
        key: value
        for key, value in profile_values.items()
        if key.startswith("[")
        or key
        in {
            "YOUR_REVIEW_TITLE",
            "POPULATION",
            "EXPOSURE_OR_CONCEPT",
            "OUTCOME",
            "ELIGIBLE_LANGUAGES",
            "START_YEAR",
        }
    }
    placeholder_replacements = merge_placeholder_replacements(
        default_placeholder_replacements(protocol_data, manuscript_metadata),
        placeholder_overrides,
    )

    completed_fields: list[PrefillField] = []
    for field in fields:
        value = field.value.strip()

        if value and PLACEHOLDER_PATTERN.search(value):
            value = replace_placeholders(value, placeholder_replacements).strip()

        if field.field_id in profile_values and profile_values[field.field_id].strip():
            value = profile_values[field.field_id].strip()
        elif (not value or value == "[TO FILL]") and defaults.get(field.field_id):
            value = defaults[field.field_id].strip()

        if not value and field.required:
            value = "Auto-filled placeholder: confirm before submission."

        completed_fields.append(
            PrefillField(
                number=field.number,
                field_id=field.field_id,
                label=field.label,
                required=field.required,
                required_scope=field.required_scope,
                value=value,
                source_section=field.source_section,
                notes=field.notes,
            )
        )

    return completed_fields


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


def join_values(values: list[str]) -> str:
    return "\n".join(f"- {value}" for value in values if value)


def is_required_in_mode(required_scope: str, registration_mode: str) -> bool:
    if required_scope == "always":
        return True
    if required_scope == "new_only":
        return registration_mode == "new"
    if required_scope == "update_only":
        return registration_mode == "update"
    return False


def resolve_registration_mode(*, requested_mode: str, protocol_data: ProtocolData) -> str:
    if requested_mode in {"new", "update"}:
        return requested_mode
    return "update" if protocol_data.registration_id else "new"


def review_question_value(protocol_data: ProtocolData) -> str:
    sections: list[str] = []
    if protocol_data.objective:
        sections.append(protocol_data.objective)
    if protocol_data.core_questions:
        sections.append("Core review questions:\n" + join_values(protocol_data.core_questions))
    return "\n\n".join(section for section in sections if section)


def searches_value(protocol_data: ProtocolData) -> str:
    chunks: list[str] = []
    if protocol_data.information_sources:
        chunks.append(
            "Databases and citation sources:\n" + join_values(protocol_data.information_sources)
        )
    if protocol_data.gray_sources:
        chunks.append(
            "Optional gray literature sources:\n" + join_values(protocol_data.gray_sources)
        )
    if protocol_data.publication_years:
        chunks.append(f"Publication years/time limits: {protocol_data.publication_years}")
    if protocol_data.languages:
        chunks.append(f"Language limits: {protocol_data.languages}")
    return "\n\n".join(chunk for chunk in chunks if chunk)


def data_extraction_value(protocol_data: ProtocolData) -> str:
    chunks: list[str] = []
    if protocol_data.screening_workflow:
        chunks.append("Selection workflow:\n" + join_values(protocol_data.screening_workflow))
    if protocol_data.extraction_fields:
        chunks.append("Data coding fields:\n" + join_values(protocol_data.extraction_fields))
    return "\n\n".join(chunk for chunk in chunks if chunk)


def subgroup_value(protocol_data: ProtocolData) -> str:
    subgroup_candidates = [
        question
        for question in protocol_data.core_questions
        if any(
            keyword in question.lower()
            for keyword in ("moderate", "subgroup", "interaction", "age", "setting")
        )
    ]
    return join_values(subgroup_candidates)


def prefer_non_placeholder(primary: str, fallback: str) -> str:
    primary_text = primary.strip()
    if primary_text and not PLACEHOLDER_PATTERN.search(primary_text):
        return primary_text
    return fallback.strip()


def prospero_field_templates() -> list[ProsperoFieldTemplate]:
    return [
        ProsperoFieldTemplate(1, "review_title", "Review title", "always", "Working title", ""),
        ProsperoFieldTemplate(
            2,
            "original_language_title",
            "Original language title",
            "optional",
            "Working title",
            "Provide only if non-English title exists.",
        ),
        ProsperoFieldTemplate(
            3,
            "review_start_date",
            "Anticipated or actual start date",
            "always",
            "manual_input",
            "Use ISO date format (YYYY-MM-DD).",
        ),
        ProsperoFieldTemplate(
            4,
            "review_completion_date",
            "Anticipated completion date",
            "always",
            "manual_input",
            "Use ISO date format (YYYY-MM-DD).",
        ),
        ProsperoFieldTemplate(
            5,
            "review_stage",
            "Stage of review at time of submission",
            "always",
            "Inferred from protocol",
            "PROSPERO expects explicit stage checkboxes/status values.",
        ),
        ProsperoFieldTemplate(6, "named_contact", "Named contact", "always", "manual_input", ""),
        ProsperoFieldTemplate(
            7, "named_contact_email", "Named contact email", "always", "manual_input", ""
        ),
        ProsperoFieldTemplate(
            8, "named_contact_address", "Named contact address", "optional", "manual_input", ""
        ),
        ProsperoFieldTemplate(
            9, "named_contact_phone", "Named contact phone number", "optional", "manual_input", ""
        ),
        ProsperoFieldTemplate(
            10,
            "organisational_affiliation",
            "Organisational affiliation",
            "always",
            "manual_input",
            "",
        ),
        ProsperoFieldTemplate(
            11,
            "review_team",
            "Review team members and their organisational affiliations",
            "always",
            "manual_input",
            "",
        ),
        ProsperoFieldTemplate(
            12,
            "funding_sources",
            "Funding sources/sponsors",
            "always",
            "manual_input",
            "Enter 'None' if no external funding.",
        ),
        ProsperoFieldTemplate(
            13,
            "conflicts_of_interest",
            "Conflicts of interest",
            "always",
            "manual_input",
            "Enter declarations for all reviewers.",
        ),
        ProsperoFieldTemplate(14, "collaborators", "Collaborators", "optional", "manual_input", ""),
        ProsperoFieldTemplate(
            15,
            "review_question",
            "Review question",
            "always",
            "Objective + core review questions",
            "",
        ),
        ProsperoFieldTemplate(16, "searches", "Searches", "always", "Information sources", ""),
        ProsperoFieldTemplate(
            17,
            "search_strategy_url",
            "URL to search strategy",
            "optional",
            "manual_input",
            "Provide a public URL/DOI if available.",
        ),
        ProsperoFieldTemplate(
            18,
            "condition_or_domain",
            "Condition or domain being studied",
            "always",
            "Eligibility criteria + objective",
            "",
        ),
        ProsperoFieldTemplate(
            19,
            "participants_population",
            "Participants/population",
            "always",
            "Eligibility criteria > Population",
            "",
        ),
        ProsperoFieldTemplate(
            20,
            "interventions_exposures",
            "Intervention(s), exposure(s)",
            "always",
            "Eligibility criteria > Concepts / exposures",
            "",
        ),
        ProsperoFieldTemplate(
            21,
            "comparators_controls",
            "Comparator(s)/control",
            "optional",
            "manual_input",
            "Fill if comparators are prespecified.",
        ),
        ProsperoFieldTemplate(
            22,
            "study_types",
            "Types of studies to be included",
            "always",
            "Eligibility criteria > Study types",
            "",
        ),
        ProsperoFieldTemplate(
            23,
            "context",
            "Context",
            "optional",
            "Eligibility criteria + sources",
            "Specify setting/healthcare context if relevant.",
        ),
        ProsperoFieldTemplate(
            24, "main_outcomes", "Main outcome(s)", "always", "Eligibility criteria > Outcomes", ""
        ),
        ProsperoFieldTemplate(
            25,
            "additional_outcomes",
            "Additional outcome(s)",
            "optional",
            "Core review questions",
            "",
        ),
        ProsperoFieldTemplate(
            26,
            "data_extraction",
            "Data extraction (selection and coding)",
            "always",
            "Screening workflow + data extraction fields",
            "",
        ),
        ProsperoFieldTemplate(
            27,
            "risk_of_bias",
            "Risk of bias (quality) assessment",
            "always",
            "Quality appraisal",
            "",
        ),
        ProsperoFieldTemplate(
            28, "data_synthesis", "Strategy for data synthesis", "always", "Synthesis plan", ""
        ),
        ProsperoFieldTemplate(
            29,
            "subgroups",
            "Analysis of subgroups or subsets",
            "always",
            "Core review questions",
            "Provide 'none planned' if not applicable.",
        ),
        ProsperoFieldTemplate(
            30, "review_type", "Type and method of review", "always", "Review type", ""
        ),
        ProsperoFieldTemplate(
            31, "language", "Language", "always", "Eligibility criteria > Time and language", ""
        ),
        ProsperoFieldTemplate(
            32,
            "country",
            "Country",
            "always",
            "manual_input",
            "Country of the review team/contact institution.",
        ),
        ProsperoFieldTemplate(
            33,
            "other_registration_details",
            "Other registration details",
            "optional",
            "Global protocol text",
            "List any other registry IDs or write 'None'.",
        ),
        ProsperoFieldTemplate(
            34,
            "published_protocol_reference",
            "Reference and/or URL for published protocol",
            "optional",
            "manual_input",
            "Add DOI/URL if protocol is public.",
        ),
        ProsperoFieldTemplate(
            35,
            "dissemination_plans",
            "Dissemination plans",
            "optional",
            "Reproducibility notes",
            "",
        ),
        ProsperoFieldTemplate(
            36, "keywords", "Keywords", "optional", "manual_input", "Provide 3-10 keywords."
        ),
        ProsperoFieldTemplate(
            37,
            "previous_versions",
            "Details of previous versions for this review",
            "update_only",
            "Global protocol text",
            "Required for review updates.",
        ),
        ProsperoFieldTemplate(
            38, "review_status", "Review status", "always", "Inferred from protocol", ""
        ),
        ProsperoFieldTemplate(
            39,
            "review_team_details",
            "Review team details",
            "update_only",
            "manual_input",
            "Required when submitting an update.",
        ),
        ProsperoFieldTemplate(
            40,
            "existing_review_same_authors",
            "Details of any existing review of same topic by same authors",
            "optional",
            "manual_input",
            "",
        ),
        ProsperoFieldTemplate(
            41,
            "update_status",
            "Current review update status",
            "update_only",
            "manual_input",
            "Required when submitting an update.",
        ),
        ProsperoFieldTemplate(
            42,
            "update_completion_date",
            "Date of completion of review update",
            "update_only",
            "manual_input",
            "Required when submitting an update.",
        ),
        ProsperoFieldTemplate(
            43,
            "update_publication_date",
            "Date review update published",
            "update_only",
            "manual_input",
            "Required when submitting an update.",
        ),
        ProsperoFieldTemplate(
            44,
            "review_publication_links",
            "Citations and links to publications of the review",
            "update_only",
            "manual_input",
            "Required when submitting an update.",
        ),
        ProsperoFieldTemplate(
            45,
            "review_amendments",
            "Current review amendments",
            "update_only",
            "manual_input",
            "Required when submitting an update.",
        ),
        ProsperoFieldTemplate(
            46,
            "amendment_details",
            "Details of amendments",
            "update_only",
            "manual_input",
            "Required when submitting an update.",
        ),
        ProsperoFieldTemplate(
            47,
            "achievements_knowledge_gaps",
            "Achievements and knowledge gaps identified during review",
            "optional",
            "manual_input",
            "Optional reflective field.",
        ),
    ]


def field_value_map(
    protocol_data: ProtocolData,
    registration_mode: str,
    manuscript_metadata: ManuscriptMetadata | None = None,
) -> dict[str, str]:
    context_text = ""
    if protocol_data.population or protocol_data.information_sources:
        context_text = (
            "Clinical/research context defined by protocol eligibility criteria and database sources; "
            "adapt to PROSPERO context wording as needed."
        )

    additional_outcomes_text = ""
    if protocol_data.core_questions:
        additional_outcomes_text = join_values(protocol_data.core_questions[1:])

    dissemination_text = ""
    if protocol_data.reproducibility_notes:
        dissemination_text = "Planned dissemination and reproducibility actions:\n" + join_values(
            protocol_data.reproducibility_notes
        )

    review_stage_text = "Protocol drafted; screening/extraction not started. Confirm actual review stage checkboxes in PROSPERO before submission."
    review_status_text = (
        "Ongoing protocol preparation and pre-screening stage"
        if registration_mode == "new"
        else "Update in progress"
    )

    preset_title = manuscript_metadata.title if manuscript_metadata else ""
    preset_name = manuscript_metadata.contact_name if manuscript_metadata else ""
    preset_email = manuscript_metadata.contact_email if manuscript_metadata else ""
    preset_affiliation = manuscript_metadata.affiliation if manuscript_metadata else ""
    preset_country = manuscript_metadata.country if manuscript_metadata else ""
    preset_keywords = "; ".join(manuscript_metadata.keywords) if manuscript_metadata else ""
    preset_date = manuscript_metadata.date_label if manuscript_metadata else ""
    preset_funding = manuscript_metadata.funding_statement if manuscript_metadata else ""
    preset_conflicts = manuscript_metadata.conflicts_statement if manuscript_metadata else ""
    preset_data_availability = (
        manuscript_metadata.data_availability_statement if manuscript_metadata else ""
    )

    review_team_text = ""
    if preset_name and preset_affiliation:
        review_team_text = f"{preset_name} ({preset_affiliation})"
    elif preset_name:
        review_team_text = preset_name

    dissemination_sections: list[str] = []
    if dissemination_text:
        dissemination_sections.append(dissemination_text)
    if preset_data_availability:
        dissemination_sections.append(f"Data availability statement: {preset_data_availability}")
    merged_dissemination = "\n\n".join(section for section in dissemination_sections if section)

    return {
        "review_title": prefer_non_placeholder(protocol_data.working_title, preset_title),
        "original_language_title": "",
        "review_start_date": preset_date,
        "review_completion_date": "",
        "review_stage": review_stage_text,
        "named_contact": preset_name,
        "named_contact_email": preset_email,
        "named_contact_address": "",
        "named_contact_phone": "",
        "organisational_affiliation": preset_affiliation,
        "review_team": review_team_text,
        "funding_sources": preset_funding,
        "conflicts_of_interest": preset_conflicts,
        "collaborators": "",
        "review_question": review_question_value(protocol_data),
        "searches": searches_value(protocol_data),
        "search_strategy_url": "",
        "condition_or_domain": protocol_data.outcomes or protocol_data.objective,
        "participants_population": protocol_data.population,
        "interventions_exposures": protocol_data.concepts_exposures,
        "comparators_controls": "",
        "study_types": protocol_data.study_types,
        "context": context_text,
        "main_outcomes": protocol_data.outcomes,
        "additional_outcomes": additional_outcomes_text,
        "data_extraction": data_extraction_value(protocol_data),
        "risk_of_bias": protocol_data.quality_appraisal,
        "data_synthesis": join_values(protocol_data.synthesis_plan),
        "subgroups": subgroup_value(protocol_data),
        "review_type": protocol_data.review_type,
        "language": protocol_data.languages,
        "country": preset_country,
        "other_registration_details": (
            f"Existing PROSPERO ID: {protocol_data.registration_id}"
            if protocol_data.registration_id
            else "None reported"
        ),
        "published_protocol_reference": "",
        "dissemination_plans": merged_dissemination,
        "keywords": preset_keywords,
        "previous_versions": protocol_data.registration_id,
        "review_status": review_status_text,
        "review_team_details": "",
        "existing_review_same_authors": "",
        "update_status": "",
        "update_completion_date": "",
        "update_publication_date": "",
        "review_publication_links": "",
        "review_amendments": "",
        "amendment_details": "",
        "achievements_knowledge_gaps": "",
    }


def field_notes_for_missing(field: PrefillField) -> str:
    if not field.required:
        return field.notes

    requirement_note = (
        "Required by PROSPERO for this registration mode; complete manually before submission."
    )
    if field.notes:
        return f"{requirement_note} {field.notes}"
    return requirement_note


def build_prefill_fields(
    protocol_data: ProtocolData,
    *,
    registration_mode: str,
    manuscript_metadata: ManuscriptMetadata | None = None,
    auto_complete: bool = False,
    profile_values: dict[str, str] | None = None,
) -> list[PrefillField]:
    value_by_id = field_value_map(
        protocol_data,
        registration_mode,
        manuscript_metadata,
    )
    fields: list[PrefillField] = []

    for template in prospero_field_templates():
        fields.append(
            PrefillField(
                number=template.number,
                field_id=template.field_id,
                label=template.label,
                required=is_required_in_mode(template.required_scope, registration_mode),
                required_scope=template.required_scope,
                value=value_by_id.get(template.field_id, "").strip(),
                source_section=template.source_section,
                notes=template.notes,
            )
        )

    if auto_complete:
        fields = auto_complete_missing_required_fields(
            fields,
            protocol_data=protocol_data,
            manuscript_metadata=manuscript_metadata,
            profile_values=profile_values or {},
        )

    return finalize_fields(fields)


def finalize_fields(fields: list[PrefillField]) -> list[PrefillField]:
    date_field_ids = {
        "review_start_date",
        "review_completion_date",
        "update_completion_date",
        "update_publication_date",
    }
    email_field_ids = {"named_contact_email"}

    for field in fields:
        raw_value = field.value.strip()
        has_placeholder = bool(PLACEHOLDER_PATTERN.search(raw_value))
        if raw_value and not has_placeholder:
            field.status = "complete"
        else:
            field.status = "needs_input"
            field.notes = field_notes_for_missing(field)

        if has_placeholder:
            unresolved_note = "Contains unresolved template placeholder(s)."
            if field.notes:
                field.notes = f"{field.notes} {unresolved_note}"
            else:
                field.notes = unresolved_note

        if raw_value and field.field_id in date_field_ids and not ISO_DATE_PATTERN.match(raw_value):
            field.status = "needs_input"
            date_note = "Date must be in `YYYY-MM-DD` format."
            if date_note not in field.notes:
                field.notes = f"{field.notes} {date_note}".strip()

        if raw_value and field.field_id in email_field_ids and not EMAIL_PATTERN.match(raw_value):
            field.status = "needs_input"
            email_note = "Email format looks invalid; verify before submission."
            if email_note not in field.notes:
                field.notes = f"{field.notes} {email_note}".strip()

    return fields


def completion_counts(fields: list[PrefillField]) -> dict[str, int]:
    required_total = sum(1 for field in fields if field.required)
    required_complete = sum(1 for field in fields if field.required and field.status == "complete")
    total_complete = sum(1 for field in fields if field.status == "complete")
    return {
        "total_fields": len(fields),
        "total_complete": total_complete,
        "required_total": required_total,
        "required_complete": required_complete,
        "required_missing": required_total - required_complete,
    }


def unresolved_placeholders_in_fields(fields: list[PrefillField]) -> list[str]:
    tokens: set[str] = set()
    for field in fields:
        for match in PLACEHOLDER_PATTERN.finditer(field.value):
            tokens.add(match.group(0))
    return sorted(tokens)


def required_scope_label(required_scope: str) -> str:
    labels = {
        "always": "always",
        "new_only": "new registration only",
        "update_only": "update registration only",
        "optional": "optional",
    }
    return labels.get(required_scope, required_scope)


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

    protocol_path = Path(args.protocol)
    structured_output_path = Path(args.structured_output)
    xml_output_path = Path(args.xml_output)
    summary_output_path = Path(args.summary_output)

    if not protocol_path.exists():
        raise SystemExit(f"Protocol file not found: {protocol_path}")

    protocol_text = protocol_path.read_text(encoding="utf-8")
    protocol_data = extract_protocol_data(protocol_text)
    manuscript_metadata, preset_manuscript_path = load_preset_metadata(args.preset, args.manuscript)
    profile_values: dict[str, str] = {}
    if args.autofill_profile:
        profile_values = load_autofill_profile(Path(args.autofill_profile))

    registration_mode = resolve_registration_mode(
        requested_mode=args.registration_mode,
        protocol_data=protocol_data,
    )
    fields = build_prefill_fields(
        protocol_data,
        registration_mode=registration_mode,
        manuscript_metadata=manuscript_metadata,
        auto_complete=args.auto_complete,
        profile_values=profile_values,
    )
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    wrote_paths: list[Path] = []

    if args.format in {"structured", "both"}:
        structured_text = render_structured_draft(
            fields=fields,
            protocol_path=protocol_path,
            protocol_data=protocol_data,
            registration_mode=registration_mode,
            preset=args.preset,
            preset_manuscript_path=preset_manuscript_path,
            generated_at=generated_at,
        )
        structured_output_path.parent.mkdir(parents=True, exist_ok=True)
        structured_output_path.write_text(structured_text, encoding="utf-8")
        wrote_paths.append(structured_output_path)

    if args.format in {"xml", "both"}:
        xml_text = render_xml_draft(
            fields=fields,
            protocol_path=protocol_path,
            protocol_data=protocol_data,
            registration_mode=registration_mode,
            preset=args.preset,
            preset_manuscript_path=preset_manuscript_path,
            generated_at=generated_at,
        )
        xml_output_path.parent.mkdir(parents=True, exist_ok=True)
        xml_output_path.write_text(xml_text, encoding="utf-8")
        wrote_paths.append(xml_output_path)

    summary_text = render_summary(
        protocol_path=protocol_path,
        structured_output_path=structured_output_path,
        xml_output_path=xml_output_path,
        fields=fields,
        protocol_data=protocol_data,
        registration_mode=registration_mode,
        preset=args.preset,
        preset_manuscript_path=preset_manuscript_path,
        generated_at=generated_at,
    )
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(summary_text, encoding="utf-8")
    wrote_paths.append(summary_output_path)

    for wrote_path in wrote_paths:
        print(f"Wrote: {wrote_path}")

    counts = completion_counts(fields)
    unresolved_in_draft = unresolved_placeholders_in_fields(fields)
    exit_code = 0

    if args.fail_on_missing_required and counts["required_missing"] > 0:
        print("Error: required PROSPERO fields are still missing input.")
        exit_code = 1

    if args.fail_on_placeholders:
        if args.auto_complete:
            if unresolved_in_draft:
                print("Error: unresolved placeholders remain in generated draft.")
                exit_code = 1
        elif protocol_data.unresolved_placeholders:
            print("Error: unresolved protocol placeholders detected.")
            exit_code = 1

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

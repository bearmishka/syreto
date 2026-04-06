from __future__ import annotations

import re

from .models import ManuscriptMetadata
from .models import ProsperoFieldTemplate
from .models import ProtocolData


PLACEHOLDER_PATTERN = re.compile(r"\[[A-Z][A-Z0-9_ ]{1,}\]")


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
        chunks.append("Databases and citation sources:\n" + join_values(protocol_data.information_sources))
    if protocol_data.gray_sources:
        chunks.append("Optional gray literature sources:\n" + join_values(protocol_data.gray_sources))
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
        if any(keyword in question.lower() for keyword in ("moderate", "subgroup", "interaction", "age", "setting"))
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
        ProsperoFieldTemplate(2, "original_language_title", "Original language title", "optional", "Working title", "Provide only if non-English title exists."),
        ProsperoFieldTemplate(3, "review_start_date", "Anticipated or actual start date", "always", "manual_input", "Use ISO date format (YYYY-MM-DD)."),
        ProsperoFieldTemplate(4, "review_completion_date", "Anticipated completion date", "always", "manual_input", "Use ISO date format (YYYY-MM-DD)."),
        ProsperoFieldTemplate(5, "review_stage", "Stage of review at time of submission", "always", "Inferred from protocol", "PROSPERO expects explicit stage checkboxes/status values."),
        ProsperoFieldTemplate(6, "named_contact", "Named contact", "always", "manual_input", ""),
        ProsperoFieldTemplate(7, "named_contact_email", "Named contact email", "always", "manual_input", ""),
        ProsperoFieldTemplate(8, "named_contact_address", "Named contact address", "optional", "manual_input", ""),
        ProsperoFieldTemplate(9, "named_contact_phone", "Named contact phone number", "optional", "manual_input", ""),
        ProsperoFieldTemplate(10, "organisational_affiliation", "Organisational affiliation", "always", "manual_input", ""),
        ProsperoFieldTemplate(11, "review_team", "Review team members and their organisational affiliations", "always", "manual_input", ""),
        ProsperoFieldTemplate(12, "funding_sources", "Funding sources/sponsors", "always", "manual_input", "Enter 'None' if no external funding."),
        ProsperoFieldTemplate(13, "conflicts_of_interest", "Conflicts of interest", "always", "manual_input", "Enter declarations for all reviewers."),
        ProsperoFieldTemplate(14, "collaborators", "Collaborators", "optional", "manual_input", ""),
        ProsperoFieldTemplate(15, "review_question", "Review question", "always", "Objective + core review questions", ""),
        ProsperoFieldTemplate(16, "searches", "Searches", "always", "Information sources", ""),
        ProsperoFieldTemplate(17, "search_strategy_url", "URL to search strategy", "optional", "manual_input", "Provide a public URL/DOI if available."),
        ProsperoFieldTemplate(18, "condition_or_domain", "Condition or domain being studied", "always", "Eligibility criteria + objective", ""),
        ProsperoFieldTemplate(19, "participants_population", "Participants/population", "always", "Eligibility criteria > Population", ""),
        ProsperoFieldTemplate(20, "interventions_exposures", "Intervention(s), exposure(s)", "always", "Eligibility criteria > Concepts / exposures", ""),
        ProsperoFieldTemplate(21, "comparators_controls", "Comparator(s)/control", "optional", "manual_input", "Fill if comparators are prespecified."),
        ProsperoFieldTemplate(22, "study_types", "Types of studies to be included", "always", "Eligibility criteria > Study types", ""),
        ProsperoFieldTemplate(23, "context", "Context", "optional", "Eligibility criteria + sources", "Specify setting/healthcare context if relevant."),
        ProsperoFieldTemplate(24, "main_outcomes", "Main outcome(s)", "always", "Eligibility criteria > Outcomes", ""),
        ProsperoFieldTemplate(25, "additional_outcomes", "Additional outcome(s)", "optional", "Core review questions", ""),
        ProsperoFieldTemplate(26, "data_extraction", "Data extraction (selection and coding)", "always", "Screening workflow + data extraction fields", ""),
        ProsperoFieldTemplate(27, "risk_of_bias", "Risk of bias (quality) assessment", "always", "Quality appraisal", ""),
        ProsperoFieldTemplate(28, "data_synthesis", "Strategy for data synthesis", "always", "Synthesis plan", ""),
        ProsperoFieldTemplate(29, "subgroups", "Analysis of subgroups or subsets", "always", "Core review questions", "Provide 'none planned' if not applicable."),
        ProsperoFieldTemplate(30, "review_type", "Type and method of review", "always", "Review type", ""),
        ProsperoFieldTemplate(31, "language", "Language", "always", "Eligibility criteria > Time and language", ""),
        ProsperoFieldTemplate(32, "country", "Country", "always", "manual_input", "Country of the review team/contact institution."),
        ProsperoFieldTemplate(33, "other_registration_details", "Other registration details", "optional", "Global protocol text", "List any other registry IDs or write 'None'."),
        ProsperoFieldTemplate(34, "published_protocol_reference", "Reference and/or URL for published protocol", "optional", "manual_input", "Add DOI/URL if protocol is public."),
        ProsperoFieldTemplate(35, "dissemination_plans", "Dissemination plans", "optional", "Reproducibility notes", ""),
        ProsperoFieldTemplate(36, "keywords", "Keywords", "optional", "manual_input", "Provide 3-10 keywords."),
        ProsperoFieldTemplate(37, "previous_versions", "Details of previous versions for this review", "update_only", "Global protocol text", "Required for review updates."),
        ProsperoFieldTemplate(38, "review_status", "Review status", "always", "Inferred from protocol", ""),
        ProsperoFieldTemplate(39, "review_team_details", "Review team details", "update_only", "manual_input", "Required when submitting an update."),
        ProsperoFieldTemplate(40, "existing_review_same_authors", "Details of any existing review of same topic by same authors", "optional", "manual_input", ""),
        ProsperoFieldTemplate(41, "update_status", "Current review update status", "update_only", "manual_input", "Required when submitting an update."),
        ProsperoFieldTemplate(42, "update_completion_date", "Date of completion of review update", "update_only", "manual_input", "Required when submitting an update."),
        ProsperoFieldTemplate(43, "update_publication_date", "Date review update published", "update_only", "manual_input", "Required when submitting an update."),
        ProsperoFieldTemplate(44, "review_publication_links", "Citations and links to publications of the review", "update_only", "manual_input", "Required when submitting an update."),
        ProsperoFieldTemplate(45, "review_amendments", "Current review amendments", "update_only", "manual_input", "Required when submitting an update."),
        ProsperoFieldTemplate(46, "amendment_details", "Details of amendments", "update_only", "manual_input", "Required when submitting an update."),
        ProsperoFieldTemplate(47, "achievements_knowledge_gaps", "Achievements and knowledge gaps identified during review", "optional", "manual_input", "Optional reflective field."),
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
        dissemination_text = (
            "Planned dissemination and reproducibility actions:\n"
            + join_values(protocol_data.reproducibility_notes)
        )

    review_stage_text = (
        "Protocol drafted; screening/extraction not started. Confirm actual review stage checkboxes in PROSPERO before submission."
    )
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
    preset_data_availability = manuscript_metadata.data_availability_statement if manuscript_metadata else ""

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

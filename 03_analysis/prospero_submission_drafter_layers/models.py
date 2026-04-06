from __future__ import annotations

from dataclasses import dataclass


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
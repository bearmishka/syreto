from __future__ import annotations

import re
from datetime import datetime, timedelta

from .models import ManuscriptMetadata, PrefillField, ProtocolData

PLACEHOLDER_PATTERN = re.compile(r"\[[A-Z][A-Z0-9_ ]{1,}\]")
ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


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


def field_notes_for_missing(field: PrefillField) -> str:
    if not field.required:
        return field.notes

    requirement_note = (
        "Required by PROSPERO for this registration mode; complete manually before submission."
    )
    if field.notes:
        return f"{requirement_note} {field.notes}"
    return requirement_note


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


def required_scope_label(required_scope: str) -> str:
    labels = {
        "always": "always",
        "new_only": "new registration only",
        "update_only": "update registration only",
        "optional": "optional",
    }
    return labels.get(required_scope, required_scope)


def completion_counts(fields: list[PrefillField]) -> dict[str, int]:
    required_fields = [field for field in fields if field.required]
    required_complete = sum(1 for field in required_fields if field.status == "complete")
    total_complete = sum(1 for field in fields if field.status == "complete")

    return {
        "total_fields": len(fields),
        "total_complete": total_complete,
        "required_total": len(required_fields),
        "required_complete": required_complete,
        "required_missing": len(required_fields) - required_complete,
    }


def unresolved_placeholders_in_fields(fields: list[PrefillField]) -> list[str]:
    unresolved: list[str] = []
    for field in fields:
        unresolved.extend(PLACEHOLDER_PATTERN.findall(field.value))
    return sorted(set(unresolved))


def evaluate_exit_conditions(
    *,
    fail_on_missing_required: bool,
    fail_on_placeholders: bool,
    auto_complete: bool,
    required_missing: int,
    unresolved_in_draft: list[str],
    unresolved_protocol_placeholders: list[str],
) -> tuple[int, list[str]]:
    exit_code = 0
    messages: list[str] = []

    if fail_on_missing_required and required_missing > 0:
        messages.append("Error: required PROSPERO fields are still missing input.")
        exit_code = 1

    if fail_on_placeholders:
        if auto_complete:
            if unresolved_in_draft:
                messages.append("Error: unresolved placeholders remain in generated draft.")
                exit_code = 1
        elif unresolved_protocol_placeholders:
            messages.append("Error: unresolved protocol placeholders detected.")
            exit_code = 1

    return exit_code, messages

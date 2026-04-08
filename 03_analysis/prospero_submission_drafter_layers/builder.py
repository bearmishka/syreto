from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from .models import ManuscriptMetadata, PrefillField, ProsperoFieldTemplate, ProtocolData
from .rules import (
    auto_complete_missing_required_fields,
    finalize_fields,
)


@dataclass
class DraftBuildContext:
    protocol_data: ProtocolData
    manuscript_metadata: ManuscriptMetadata | None
    preset_manuscript_path: Path | None
    registration_mode: str
    fields: list[PrefillField]
    generated_at: str


@dataclass
class DraftArtifacts:
    structured_text: str | None
    xml_text: str | None
    summary_text: str


def build_prefill_fields(
    protocol_data: ProtocolData,
    *,
    registration_mode: str,
    manuscript_metadata: ManuscriptMetadata | None = None,
    auto_complete: bool = False,
    profile_values: dict[str, str] | None = None,
    field_templates_fn: Callable[[], list[ProsperoFieldTemplate]],
    field_value_map_fn: Callable[..., dict[str, str]],
    is_required_in_mode_fn: Callable[[str, str], bool],
    auto_complete_missing_required_fields_fn: Callable[
        ..., list[PrefillField]
    ] = auto_complete_missing_required_fields,
    finalize_fields_fn: Callable[[list[PrefillField]], list[PrefillField]] = finalize_fields,
) -> list[PrefillField]:
    normalized_profile_values = profile_values or {}
    value_lookup = field_value_map_fn(
        protocol_data,
        registration_mode=registration_mode,
        manuscript_metadata=manuscript_metadata,
    )

    fields: list[PrefillField] = []
    for template in field_templates_fn():
        field_value = value_lookup.get(template.field_id, "").strip()
        if template.field_id in normalized_profile_values:
            field_value = normalized_profile_values[template.field_id].strip()

        fields.append(
            PrefillField(
                number=template.number,
                field_id=template.field_id,
                label=template.label,
                required=is_required_in_mode_fn(template.required_scope, registration_mode),
                required_scope=template.required_scope,
                value=field_value,
                source_section=template.source_section,
                notes=template.notes,
            )
        )

    if auto_complete:
        fields = auto_complete_missing_required_fields_fn(
            fields,
            protocol_data=protocol_data,
            manuscript_metadata=manuscript_metadata,
            profile_values=normalized_profile_values,
        )

    return finalize_fields_fn(fields)


def build_submission_context(
    *,
    protocol_data: ProtocolData,
    manuscript_metadata: ManuscriptMetadata | None,
    preset_manuscript_path: Path | None,
    requested_registration_mode: str,
    auto_complete: bool,
    profile_values: dict[str, str],
    resolve_registration_mode_fn: Callable[..., str],
    build_prefill_fields_fn: Callable[..., list[PrefillField]],
    now_fn: Callable[[], datetime] = datetime.now,
) -> DraftBuildContext:
    registration_mode = resolve_registration_mode_fn(
        requested_mode=requested_registration_mode,
        protocol_data=protocol_data,
    )
    fields = build_prefill_fields_fn(
        protocol_data,
        registration_mode=registration_mode,
        manuscript_metadata=manuscript_metadata,
        auto_complete=auto_complete,
        profile_values=profile_values,
    )
    generated_at = now_fn().strftime("%Y-%m-%d %H:%M")

    return DraftBuildContext(
        protocol_data=protocol_data,
        manuscript_metadata=manuscript_metadata,
        preset_manuscript_path=preset_manuscript_path,
        registration_mode=registration_mode,
        fields=fields,
        generated_at=generated_at,
    )


def build_draft_artifacts(
    *,
    context: DraftBuildContext,
    protocol_path: Path,
    structured_output_path: Path,
    xml_output_path: Path,
    preset: str,
    output_format: str,
    render_structured_draft_fn: Callable[..., str],
    render_xml_draft_fn: Callable[..., str],
    render_summary_fn: Callable[..., str],
) -> DraftArtifacts:
    structured_text: str | None = None
    xml_text: str | None = None

    if output_format in {"structured", "both"}:
        structured_text = render_structured_draft_fn(
            fields=context.fields,
            protocol_path=protocol_path,
            protocol_data=context.protocol_data,
            registration_mode=context.registration_mode,
            preset=preset,
            preset_manuscript_path=context.preset_manuscript_path,
            generated_at=context.generated_at,
        )

    if output_format in {"xml", "both"}:
        xml_text = render_xml_draft_fn(
            fields=context.fields,
            protocol_path=protocol_path,
            protocol_data=context.protocol_data,
            registration_mode=context.registration_mode,
            preset=preset,
            preset_manuscript_path=context.preset_manuscript_path,
            generated_at=context.generated_at,
        )

    summary_text = render_summary_fn(
        protocol_path=protocol_path,
        structured_output_path=structured_output_path,
        xml_output_path=xml_output_path,
        fields=context.fields,
        protocol_data=context.protocol_data,
        registration_mode=context.registration_mode,
        preset=preset,
        preset_manuscript_path=context.preset_manuscript_path,
        generated_at=context.generated_at,
    )

    return DraftArtifacts(
        structured_text=structured_text,
        xml_text=xml_text,
        summary_text=summary_text,
    )

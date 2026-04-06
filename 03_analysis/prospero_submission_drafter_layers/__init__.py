from __future__ import annotations

from .builder import DraftArtifacts
from .builder import DraftBuildContext
from .builder import auto_complete_missing_required_fields
from .builder import build_draft_artifacts
from .builder import build_prefill_fields
from .builder import build_submission_context
from .builder import completion_counts
from .builder import default_placeholder_replacements
from .builder import evaluate_exit_conditions
from .builder import infer_default_completion_date
from .builder import infer_default_start_date
from .builder import merge_placeholder_replacements
from .builder import normalize_placeholder_token
from .builder import parse_date_label_to_iso
from .builder import placeholder_token_variants
from .builder import replace_placeholders
from .builder import unresolved_placeholders_in_fields
from .field_composition import field_value_map
from .field_composition import is_required_in_mode
from .field_composition import prospero_field_templates
from .formatting import collapse_whitespace
from .formatting import extract_braced_content
from .formatting import extract_latex_macro_argument
from .formatting import latex_to_plain
from .formatting import strip_latex_macro_with_argument
from .formatting import strip_markdown_markup
from .io import load_autofill_profile
from .io import read_protocol_text
from .io import resolve_cli_paths
from .io import resolve_preset_manuscript_path
from .io import write_text_artifact
from .models import ManuscriptMetadata
from .models import MarkdownSection
from .models import PrefillField
from .models import ProsperoFieldTemplate
from .models import ProtocolData
from .rules import field_notes_for_missing
from .rules import finalize_fields
from .rules import required_scope_label


__all__ = [
    "DraftArtifacts",
    "DraftBuildContext",
    "ManuscriptMetadata",
    "MarkdownSection",
    "PrefillField",
    "ProsperoFieldTemplate",
    "ProtocolData",
    "auto_complete_missing_required_fields",
    "build_draft_artifacts",
    "build_prefill_fields",
    "build_submission_context",
    "collapse_whitespace",
    "completion_counts",
    "default_placeholder_replacements",
    "evaluate_exit_conditions",
    "field_value_map",
    "is_required_in_mode",
    "prospero_field_templates",
    "extract_braced_content",
    "extract_latex_macro_argument",
    "field_notes_for_missing",
    "finalize_fields",
    "infer_default_completion_date",
    "infer_default_start_date",
    "latex_to_plain",
    "load_autofill_profile",
    "merge_placeholder_replacements",
    "normalize_placeholder_token",
    "parse_date_label_to_iso",
    "placeholder_token_variants",
    "read_protocol_text",
    "replace_placeholders",
    "required_scope_label",
    "resolve_cli_paths",
    "resolve_preset_manuscript_path",
    "strip_latex_macro_with_argument",
    "strip_markdown_markup",
    "unresolved_placeholders_in_fields",
    "write_text_artifact",
]
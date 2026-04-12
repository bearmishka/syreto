"""Legacy entrypoint shim for the canonical packaged PROSPERO drafter module.

The source of truth now lives in ``syreto.prospero_submission_drafter``. This
wrapper preserves direct imports from ``03_analysis/`` and script-path
execution while keeping the historical Python-level compatibility shim for
``build_prefill_fields``.
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

ANALYSIS_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ANALYSIS_ROOT.parents[0]
if str(ANALYSIS_ROOT) not in sys.path:
    sys.path.insert(0, str(ANALYSIS_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from prospero_submission_drafter_layers.builder import (  # noqa: E402
    build_prefill_fields as build_prefill_fields_layer,
)
from prospero_submission_drafter_layers.field_composition import (  # noqa: E402
    field_value_map,
    is_required_in_mode,
    prospero_field_templates,
)

from syreto import (
    prospero_submission_drafter as _canonical_prospero_submission_drafter,  # noqa: E402
)
from syreto.prospero_submission_drafter import *  # noqa: F401,F403,E402


def build_prefill_fields(
    protocol_data: object,
    *,
    registration_mode: str,
    manuscript_metadata: object | None = None,
    auto_complete: bool = False,
    profile_values: dict[str, str] | None = None,
) -> list[object]:
    warnings.warn(
        "Python-level access via `prospero_submission_drafter.build_prefill_fields` is deprecated; "
        "use `prospero_submission_drafter_layers.builder.build_prefill_fields` with "
        "`prospero_submission_drafter_layers.field_composition` callbacks.",
        DeprecationWarning,
        stacklevel=2,
    )
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


main = _canonical_prospero_submission_drafter.main

if __name__ == "__main__":
    raise SystemExit(main())

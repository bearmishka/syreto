"""Legacy entrypoint shim for the canonical packaged provenance module.

The source of truth now lives in ``syreto.provenance``. This wrapper preserves
direct imports from ``03_analysis/`` while the repository spine is migrated
toward package-owned Python logic.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from syreto import provenance as _canonical_provenance
from syreto.provenance import *  # noqa: F401,F403

atomic_replace_bytes = _canonical_provenance.atomic_replace_bytes
atomic_write_text = _canonical_provenance.atomic_write_text
provenance_sidecar_path = _canonical_provenance.provenance_sidecar_path
write_provenance_sidecar = _canonical_provenance.write_provenance_sidecar

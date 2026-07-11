"""Legacy entrypoint shim for the canonical packaged latex_utils module.

The source of truth now lives in ``syreto.latex_utils``. This wrapper
preserves direct imports from ``03_analysis/`` while the repository spine is
migrated toward package-owned Python logic.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from syreto import latex_utils as _canonical_latex_utils
from syreto.latex_utils import *  # noqa: F401,F403

latex_escape = _canonical_latex_utils.latex_escape
render_table_block = _canonical_latex_utils.render_table_block

"""Legacy entrypoint shim for the canonical packaged study_table module.

The source of truth now lives in ``syreto.study_table``. This wrapper
preserves direct imports from ``03_analysis/`` while the repository spine is
migrated toward package-owned Python logic.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from syreto import study_table as _canonical_study_table
from syreto.study_table import *  # noqa: F401,F403

MISSING_CODES = _canonical_study_table.MISSING_CODES
INCLUDE_CODES = _canonical_study_table.INCLUDE_CODES
LEGACY_TO_GENERIC_COLUMN_MAP = _canonical_study_table.LEGACY_TO_GENERIC_COLUMN_MAP
normalize = _canonical_study_table.normalize
normalize_lower = _canonical_study_table.normalize_lower
is_missing = _canonical_study_table.is_missing
read_csv_or_empty = _canonical_study_table.read_csv_or_empty
harmonize_study_columns = _canonical_study_table.harmonize_study_columns
load_study_table = _canonical_study_table.load_study_table
included_study_table = _canonical_study_table.included_study_table
sort_study_table = _canonical_study_table.sort_study_table

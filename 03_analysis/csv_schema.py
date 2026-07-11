"""Legacy entrypoint shim for the canonical packaged CSV schema module.

The source of truth now lives in ``syreto.csv_schema``. This wrapper preserves
direct imports from ``03_analysis/`` while the repository spine is migrated
toward package-owned Python logic.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from syreto import csv_schema as _canonical_csv_schema
from syreto.csv_schema import *  # noqa: F401,F403

CsvSchemaContract = _canonical_csv_schema.CsvSchemaContract
CsvSchemaFinding = _canonical_csv_schema.CsvSchemaFinding
CsvSchemaPosture = _canonical_csv_schema.CsvSchemaPosture
schema_contract_paths = _canonical_csv_schema.schema_contract_paths
validate_csv_schema_contract = _canonical_csv_schema.validate_csv_schema_contract
summarize_csv_schema_posture = _canonical_csv_schema.summarize_csv_schema_posture

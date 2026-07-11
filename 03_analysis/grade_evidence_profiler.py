"""Legacy entrypoint shim for the canonical packaged GRADE profiler module.

The source of truth now lives in ``syreto.grade_evidence_profiler``. This
wrapper preserves direct imports from ``03_analysis/`` and script-path
execution while the repository spine is migrated toward package-owned Python
logic.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from syreto import grade_evidence_profiler as _canonical_grade_evidence_profiler
from syreto.grade_evidence_profiler import *  # noqa: F401,F403


def main() -> None:
    original_cwd = Path.cwd()
    try:
        os.chdir(SCRIPT_DIR)
        _canonical_grade_evidence_profiler.main()
    finally:
        os.chdir(original_cwd)


if __name__ == "__main__":
    main()

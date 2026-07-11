"""Legacy entrypoint shim for the canonical packaged study_flow_map_builder module.

The source of truth now lives in ``syreto.study_flow_map_builder``. This wrapper
preserves direct imports from ``03_analysis/`` and script-path execution while
the repository spine is migrated toward package-owned Python logic.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from syreto import study_flow_map_builder as _canonical_study_flow_map_builder
from syreto.study_flow_map_builder import *  # noqa: F401,F403

main = _canonical_study_flow_map_builder.main

if __name__ == "__main__":
    original_cwd = Path.cwd()
    try:
        os.chdir(SCRIPT_DIR)
        main()
    finally:
        os.chdir(original_cwd)

"""Legacy entrypoint shim for the canonical packaged results_interpretation_layer module.

The source of truth now lives in ``syreto.results_interpretation_layer``. This wrapper
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

from syreto import results_interpretation_layer as _canonical_results_interpretation_layer
from syreto.results_interpretation_layer import *  # noqa: F401,F403

main = _canonical_results_interpretation_layer.main

if __name__ == "__main__":
    original_cwd = Path.cwd()
    try:
        os.chdir(SCRIPT_DIR)
        main()
    finally:
        os.chdir(original_cwd)

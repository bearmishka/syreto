"""Legacy entrypoint shim for the canonical packaged epistemic_consistency_guard module.

The source of truth now lives in ``syreto.epistemic_consistency_guard``. This wrapper
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

from syreto import epistemic_consistency_guard as _canonical_epistemic_consistency_guard
from syreto.epistemic_consistency_guard import *  # noqa: F401,F403

main = _canonical_epistemic_consistency_guard.main

if __name__ == "__main__":
    original_cwd = Path.cwd()
    try:
        os.chdir(SCRIPT_DIR)
        main()
    finally:
        os.chdir(original_cwd)

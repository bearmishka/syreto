"""Legacy entrypoint shim for the canonical packaged publication_bias_assessment module.

The source of truth now lives in ``syreto.publication_bias_assessment``. This wrapper
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

from syreto import publication_bias_assessment as _canonical_publication_bias_assessment
from syreto.publication_bias_assessment import *  # noqa: F401,F403

main = _canonical_publication_bias_assessment.main

if __name__ == "__main__":
    original_cwd = Path.cwd()
    try:
        os.chdir(SCRIPT_DIR)
        main()
    finally:
        os.chdir(original_cwd)

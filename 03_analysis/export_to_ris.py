"""Legacy entrypoint shim for the canonical packaged RIS export module.

The source of truth now lives in ``syreto.export_to_ris``. This wrapper
preserves direct imports from ``03_analysis/`` and script-path execution while
the repository spine is migrated toward package-owned logic.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from syreto import export_to_ris as _canonical_export_to_ris
from syreto.export_to_ris import *  # noqa: F401,F403

main = _canonical_export_to_ris.main

if __name__ == "__main__":
    main()

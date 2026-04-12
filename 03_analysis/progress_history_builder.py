"""Legacy entrypoint shim for the canonical packaged progress-history module.

The source of truth now lives in ``syreto.progress_history_builder``. This
wrapper preserves direct imports from ``03_analysis/`` and script-path
execution while the repository spine is migrated toward package-owned logic.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from syreto import progress_history_builder as _canonical_progress_history_builder
from syreto.progress_history_builder import *  # noqa: F401,F403

main = _canonical_progress_history_builder.main

if __name__ == "__main__":
    main()

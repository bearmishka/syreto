"""Legacy entrypoint shim for the canonical packaged status report module.

The source of truth now lives in ``syreto.status_report``. This wrapper keeps
legacy script-path execution and direct module imports working while the
repository spine is migrated toward package-owned Python logic.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from syreto import status_report as _canonical_status_report
from syreto.status_report import *  # noqa: F401,F403

main = _canonical_status_report.main

if __name__ == "__main__":
    main()

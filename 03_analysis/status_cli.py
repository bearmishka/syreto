"""Legacy entrypoint shim for the canonical packaged status CLI module.

The source of truth now lives in ``syreto.status_cli``. This wrapper preserves
legacy script-path execution and direct imports from ``03_analysis/`` while the
repository spine is migrated toward package-owned Python logic.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from syreto import status_cli as _canonical_status_cli
from syreto.status_cli import *  # noqa: F401,F403

main = _canonical_status_cli.main

if __name__ == "__main__":
    main()

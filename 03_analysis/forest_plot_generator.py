"""Legacy entrypoint shim for the canonical packaged forest-plot module.

The source of truth now lives in ``syreto.forest_plot_generator``. This wrapper
preserves direct imports from ``03_analysis/`` and script-path execution while
the repository spine is migrated toward package-owned logic.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from syreto import forest_plot_generator as _canonical_forest_plot_generator
from syreto.forest_plot_generator import *  # noqa: F401,F403

main = _canonical_forest_plot_generator.main

if __name__ == "__main__":
    main()

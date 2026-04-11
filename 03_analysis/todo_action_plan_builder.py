"""Legacy entrypoint shim for the canonical packaged TODO action plan module.

The source of truth now lives in ``syreto.todo_action_plan_builder``. This
wrapper preserves script-path execution and direct imports from
``03_analysis/`` while package-owned logic becomes canonical.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from syreto import todo_action_plan_builder as _canonical_todo_action_plan_builder
from syreto.todo_action_plan_builder import *  # noqa: F401,F403

main = _canonical_todo_action_plan_builder.main

if __name__ == "__main__":
    main()

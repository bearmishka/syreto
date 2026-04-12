"""Legacy entrypoint shim for the canonical packaged PROSPERO manual-field check."""

from syreto.prospero_manual_fields_check import *  # noqa: F401,F403
from syreto.prospero_manual_fields_check import main

if __name__ == "__main__":
    raise SystemExit(main())

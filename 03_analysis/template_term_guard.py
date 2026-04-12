from pathlib import Path

from syreto.template_term_guard import *  # noqa: F403
from syreto.template_term_guard import main as _package_main


def main(argv: list[str] | None = None) -> int:
    return _package_main(argv, script_dir=Path(__file__).resolve().parent)


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import sys

from .scripts import AVAILABLE_SCRIPTS, run_script, script_path


def parser() -> argparse.ArgumentParser:
    cli_parser = argparse.ArgumentParser(
        prog="syreto",
        description="Run or inspect packaged SYRETO analysis scripts.",
    )
    subparsers = cli_parser.add_subparsers(dest="command")

    subparsers.add_parser(
        "list",
        help="List available analysis scripts.",
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Run an analysis script by name.",
    )
    run_parser.add_argument("script", help="Script name (with or without .py)")
    run_parser.add_argument(
        "script_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed through to the script; use `--` before script flags.",
    )

    path_parser = subparsers.add_parser(
        "path",
        help="Print resolved filesystem path for a script.",
    )
    path_parser.add_argument("script", help="Script name (with or without .py)")

    return cli_parser


def _normalize_passthrough_args(values: list[str]) -> list[str]:
    if values and values[0] == "--":
        return values[1:]
    return values


def _list_scripts() -> int:
    for script in AVAILABLE_SCRIPTS:
        print(script)
    return 0


def _script_path(name: str) -> int:
    try:
        resolved = script_path(name)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    print(str(resolved))
    return 0


def _run_script(name: str, script_args: list[str]) -> int:
    try:
        result = run_script(
            name,
            *_normalize_passthrough_args(script_args),
            check=False,
            capture_output=False,
            text=True,
        )
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    return int(result.returncode)


def _alias_argv(argv: list[str] | None) -> list[str]:
    if argv is not None:
        return list(argv)
    return list(sys.argv[1:])


def main_status(argv: list[str] | None = None) -> int:
    return _run_script("status_cli", _alias_argv(argv))


def main_draft(argv: list[str] | None = None) -> int:
    return _run_script("prospero_submission_drafter", _alias_argv(argv))


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    command = args.command or "list"

    if command == "list":
        return _list_scripts()
    if command == "path":
        return _script_path(args.script)
    if command == "run":
        return _run_script(args.script, args.script_args)

    raise SystemExit(f"Unknown command: {command}")


if __name__ == "__main__":
    raise SystemExit(main())

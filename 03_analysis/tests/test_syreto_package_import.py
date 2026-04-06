from __future__ import annotations

from contextlib import redirect_stderr
from contextlib import redirect_stdout
import importlib
from io import StringIO
from pathlib import Path
import sys
import tomllib
import unittest
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import syreto  # noqa: E402


class SyretoPackageImportTests(unittest.TestCase):
    def test_package_exposes_expected_script_lookup(self) -> None:
        self.assertIn("status_report", syreto.AVAILABLE_SCRIPTS)
        script = syreto.script_path("status_report")
        self.assertEqual(script.name, "status_report.py")
        self.assertEqual(script.parent, syreto.analysis_dir())

    def test_direct_submodule_import_path_works(self) -> None:
        module = importlib.import_module("syreto.analysis.status_report")
        self.assertTrue(hasattr(module, "main"))

    def test_submodule_import_handles_local_script_dependency(self) -> None:
        module = importlib.import_module("syreto.analysis.subgroup_analysis_builder")
        self.assertTrue(hasattr(module, "main"))

    def test_registry_exposes_script_spec_and_loader(self) -> None:
        from syreto.analysis import get_script_spec

        spec = get_script_spec("status_report")
        self.assertEqual(spec.name, "status_report")
        self.assertEqual(spec.path.name, "status_report.py")
        loaded = spec.load()
        self.assertTrue(hasattr(loaded, "main"))

    def test_cli_list_command_outputs_known_script(self) -> None:
        from syreto import cli

        stdout = StringIO()
        with redirect_stdout(stdout):
            exit_code = cli.main(["list"])

        self.assertEqual(exit_code, 0)
        self.assertIn("status_report", stdout.getvalue().splitlines())

    def test_cli_path_command_resolves_script(self) -> None:
        from syreto import cli

        stdout = StringIO()
        with redirect_stdout(stdout):
            exit_code = cli.main(["path", "status_report"])

        self.assertEqual(exit_code, 0)
        self.assertTrue(stdout.getvalue().strip().endswith("status_report.py"))

    def test_cli_path_command_returns_code_2_for_missing_script(self) -> None:
        from syreto import cli

        stderr = StringIO()
        with redirect_stderr(stderr):
            exit_code = cli.main(["path", "missing_script"])

        self.assertEqual(exit_code, 2)
        self.assertIn("missing_script", stderr.getvalue())

    def test_cli_status_alias_routes_to_status_cli(self) -> None:
        from syreto import cli

        with mock.patch.object(cli, "_run_script", return_value=0) as patched:
            exit_code = cli.main_status(["--fail-on", "major"])

        self.assertEqual(exit_code, 0)
        patched.assert_called_once_with("status_cli", ["--fail-on", "major"])

    def test_cli_draft_alias_routes_to_prospero_drafter(self) -> None:
        from syreto import cli

        with mock.patch.object(cli, "_run_script", return_value=0) as patched:
            exit_code = cli.main_draft(["--output", "outputs/prospero_submission_prefill.md"])

        self.assertEqual(exit_code, 0)
        patched.assert_called_once_with(
            "prospero_submission_drafter",
            ["--output", "outputs/prospero_submission_prefill.md"],
        )

    def test_cli_alias_entrypoints_follow_explicit_allowlist(self) -> None:
        pyproject_path = PROJECT_ROOT / "pyproject.toml"
        config = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
        scripts = config["project"]["scripts"]

        aliases = {name for name in scripts if name.startswith("syreto-")}
        allowed_aliases = {"syreto-status", "syreto-draft"}
        self.assertSetEqual(
            aliases,
            allowed_aliases,
            (
                "Allowed syreto aliases are fixed to syreto-status and syreto-draft; "
                "update policy and this test intentionally before adding more."
            ),
        )

        expected_targets = {
            "syreto-status": "syreto.cli:main_status",
            "syreto-draft": "syreto.cli:main_draft",
        }
        actual_targets = {name: scripts[name] for name in sorted(allowed_aliases)}
        self.assertEqual(actual_targets, expected_targets)
        self.assertIn("syreto", scripts)


if __name__ == "__main__":
    unittest.main()
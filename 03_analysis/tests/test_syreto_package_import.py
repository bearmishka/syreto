from __future__ import annotations

import importlib
import sys
import tomllib
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
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

    def test_package_version_matches_release(self) -> None:
        self.assertEqual(syreto.__version__, "0.2.0")

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

    def test_cli_status_subcommand_routes_to_status_cli(self) -> None:
        from syreto import cli

        with mock.patch.object(cli, "_run_script", return_value=0) as patched:
            exit_code = cli.main(["status", "--", "--fail-on", "major"])

        self.assertEqual(exit_code, 0)
        patched.assert_called_once_with("status_cli", ["--", "--fail-on", "major"])

    def test_cli_artifacts_lists_selected_group(self) -> None:
        from syreto import cli

        stdout = StringIO()
        with redirect_stdout(stdout):
            exit_code = cli.main(["artifacts", "--kind", "operational"])

        self.assertEqual(exit_code, 0)
        rendered = stdout.getvalue()
        self.assertIn("operational:", rendered)
        self.assertIn("outputs/status_summary.json", rendered)
        self.assertNotIn("manuscript:", rendered)

    def test_cli_artifacts_missing_only_filters_present_items(self) -> None:
        from syreto import cli

        existing = {
            (cli.PROJECT_ROOT / "outputs/status_summary.json").resolve(),
        }

        def fake_exists(self: Path) -> bool:
            return self.resolve() in existing

        stdout = StringIO()
        with mock.patch.object(Path, "exists", fake_exists):
            with redirect_stdout(stdout):
                exit_code = cli.main(["artifacts", "--kind", "operational", "--missing-only"])

        self.assertEqual(exit_code, 0)
        rendered = stdout.getvalue()
        self.assertIn("operational:", rendered)
        self.assertNotIn("outputs/status_summary.json", rendered)
        self.assertIn("outputs/status_report.md", rendered)

    def test_cli_validate_csv_routes_to_csv_validator(self) -> None:
        from syreto import cli

        with mock.patch.object(cli, "_run_script", return_value=0) as patched:
            exit_code = cli.main(["validate", "csv", "--", "--fail-on", "warning"])

        self.assertEqual(exit_code, 0)
        patched.assert_called_once_with("validate_csv_inputs", ["--fail-on", "warning"])

    def test_cli_validate_extraction_routes_to_extraction_validator(self) -> None:
        from syreto import cli

        with mock.patch.object(cli, "_run_script", return_value=0) as patched:
            exit_code = cli.main(["validate", "extraction", "--", "--fail-on", "error"])

        self.assertEqual(exit_code, 0)
        patched.assert_called_once_with("validate_extraction", ["--fail-on", "error"])

    def test_cli_validate_all_runs_both_validators_in_order(self) -> None:
        from syreto import cli

        with mock.patch.object(cli, "_run_script", return_value=0) as patched:
            exit_code = cli.main(["validate", "all", "--", "--fail-on", "error"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            patched.call_args_list,
            [
                mock.call("validate_csv_inputs", ["--fail-on", "error"]),
                mock.call("validate_extraction", ["--fail-on", "error"]),
            ],
        )

    def test_cli_doctor_reports_summary(self) -> None:
        from syreto import cli

        stdout = StringIO()
        with redirect_stdout(stdout):
            exit_code = cli.main(["doctor"])

        self.assertEqual(exit_code, 0)
        rendered = stdout.getvalue()
        self.assertIn("SyReTo doctor", rendered)
        self.assertIn("Version: 0.2.0", rendered)
        self.assertIn("Environment", rendered)
        self.assertIn("Summary:", rendered)

    def test_cli_doctor_strict_fails_on_warnings(self) -> None:
        from syreto import cli

        with mock.patch.object(
            cli,
            "DOCTOR_OPTIONAL_PATHS",
            (("status summary", cli.PROJECT_ROOT / "outputs/definitely_missing_status.json"),),
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                exit_code = cli.main(["doctor", "--strict"])

        self.assertEqual(exit_code, 1)
        self.assertIn("[warn] status summary", stdout.getvalue())

    def test_cli_doctor_suggests_daily_run_when_status_artifacts_missing(self) -> None:
        from syreto import cli

        with mock.patch.object(
            cli,
            "DOCTOR_OPTIONAL_PATHS",
            (("status summary", cli.PROJECT_ROOT / "outputs/definitely_missing_status.json"),),
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                exit_code = cli.main(["doctor"])

        self.assertEqual(exit_code, 0)
        self.assertIn("Run `cd 03_analysis && bash daily_run.sh`", stdout.getvalue())

    def test_cli_analytics_descriptives_routes_to_builder(self) -> None:
        from syreto import cli

        with mock.patch.object(cli, "_run_script", return_value=0) as patched:
            exit_code = cli.main(
                ["analytics", "descriptives", "--", "--json-output", "outputs/x.json"]
            )

        self.assertEqual(exit_code, 0)
        patched.assert_called_once_with(
            "review_descriptives_builder",
            ["--", "--json-output", "outputs/x.json"],
        )

    def test_cli_analytics_defaults_to_descriptives(self) -> None:
        from syreto import cli

        with mock.patch.object(cli, "_run_script", return_value=0) as patched:
            exit_code = cli.main(["analytics"])

        self.assertEqual(exit_code, 0)
        patched.assert_called_once_with("review_descriptives_builder", [])

    def test_cli_review_run_executes_daily_run(self) -> None:
        from syreto import cli

        completed = mock.Mock(returncode=0)
        with mock.patch.object(cli.subprocess, "run", return_value=completed) as patched:
            exit_code = cli.main(["review", "run", "--", "arg1"])

        self.assertEqual(exit_code, 0)
        patched.assert_called_once_with(
            ["bash", str(cli.DAILY_RUN_SCRIPT), "arg1"],
            cwd=str(cli.DAILY_RUN_SCRIPT.parent),
            check=False,
            text=True,
        )

    def test_cli_review_defaults_to_run(self) -> None:
        from syreto import cli

        completed = mock.Mock(returncode=0)
        with mock.patch.object(cli.subprocess, "run", return_value=completed) as patched:
            exit_code = cli.main(["review"])

        self.assertEqual(exit_code, 0)
        patched.assert_called_once_with(
            ["bash", str(cli.DAILY_RUN_SCRIPT)],
            cwd=str(cli.DAILY_RUN_SCRIPT.parent),
            check=False,
            text=True,
        )

    def test_cli_review_run_returns_code_2_when_daily_run_missing(self) -> None:
        from syreto import cli

        stderr = StringIO()
        missing_path = cli.PROJECT_ROOT / "03_analysis" / "missing_daily_run.sh"
        with mock.patch.object(cli, "DAILY_RUN_SCRIPT", missing_path):
            with redirect_stderr(stderr):
                exit_code = cli.main(["review", "run"])

        self.assertEqual(exit_code, 2)
        self.assertIn("Review runner not found", stderr.getvalue())

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

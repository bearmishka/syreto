import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "python_source_guard.py"
spec = importlib.util.spec_from_file_location("python_source_guard", MODULE_PATH)
python_source_guard = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = python_source_guard
assert spec.loader is not None
spec.loader.exec_module(python_source_guard)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = PROJECT_ROOT / "tests"
BROKEN_FIXTURES_ROOT = TESTS_ROOT / "fixtures" / "python_source_guard_broken"


class PythonSourceIntegrityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.report = python_source_guard.run_guard(
            project_root=PROJECT_ROOT,
            tests_root=TESTS_ROOT,
            strict_header_duplicates=True,
        )

    def test_all_python_files_compile(self) -> None:
        self.assertEqual(self.report.syntax_errors, [])

    def test_single_main_guard_per_file(self) -> None:
        self.assertEqual(self.report.multiple_main_guards, [])

    def test_no_run_on_tokens_after_main_exit(self) -> None:
        self.assertEqual(self.report.run_on_after_main_exit, [])

    def test_test_modules_are_not_fully_duplicated(self) -> None:
        self.assertEqual(self.report.duplicated_test_modules, [])

    def test_no_duplicate_header_lines(self) -> None:
        self.assertEqual(self.report.duplicate_header_lines, [])

    def test_no_duplicate_header_constants(self) -> None:
        self.assertEqual(self.report.duplicate_header_constants, [])

    def test_detects_duplicate_header_lines_and_constants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            sample = root / "sample_module.py"
            sample.write_text(
                "from pathlib import Path\n"
                "\n"
                "DUP = 1\n"
                "DUP = 1\n"
                "FLAG = 'x'\n"
                "FLAG = 'x'\n"
                "\n"
                "def main() -> int:\n"
                "    return 0\n",
                encoding="utf-8",
            )

            report = python_source_guard.run_guard(
                project_root=root,
                tests_root=root / "tests",
            )

        self.assertGreaterEqual(len(report.duplicate_header_lines), 1)
        self.assertGreaterEqual(len(report.duplicate_header_constants), 1)

    def test_strict_mode_detects_non_constant_duplicate_assignments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            sample = root / "sample_module.py"
            sample.write_text(
                "value = 1\nvalue = 2\n\ndef main() -> int:\n    return 0\n",
                encoding="utf-8",
            )

            non_strict_report = python_source_guard.run_guard(
                project_root=root,
                tests_root=root / "tests",
                strict_header_duplicates=False,
            )
            strict_report = python_source_guard.run_guard(
                project_root=root,
                tests_root=root / "tests",
                strict_header_duplicates=True,
            )

        self.assertEqual(non_strict_report.duplicate_header_constants, [])
        self.assertGreaterEqual(len(strict_report.duplicate_header_constants), 1)

    def test_broken_fixtures_cover_all_detectors(self) -> None:
        report = python_source_guard.run_guard(
            project_root=BROKEN_FIXTURES_ROOT,
            tests_root=BROKEN_FIXTURES_ROOT / "tests",
            strict_header_duplicates=True,
            include_fixtures=True,
        )

        self.assertTrue(report.has_issues())
        self.assertTrue(any("syntax_error_module.py" in item for item in report.syntax_errors))
        self.assertTrue(
            any("duplicate_main_guard_module.py" in item for item in report.multiple_main_guards)
        )
        self.assertTrue(
            any(
                "run_on_after_main_exit_module.py" in item for item in report.run_on_after_main_exit
            )
        )
        self.assertTrue(
            any(
                "duplicate_header_lines_module.py" in item for item in report.duplicate_header_lines
            )
        )
        self.assertTrue(
            any(
                "duplicate_header_constants_module.py" in item
                for item in report.duplicate_header_constants
            )
        )
        self.assertTrue(
            any(
                "strict_duplicate_assignment_module.py" in item
                for item in report.duplicate_header_constants
            )
        )
        self.assertTrue(
            any("test_duplicated_module.py" in item for item in report.duplicated_test_modules)
        )

    def test_cli_exit_code_is_success(self) -> None:
        exit_code = python_source_guard.main(
            [
                "--project-root",
                str(PROJECT_ROOT),
                "--tests-root",
                str(TESTS_ROOT),
                "--quiet",
                "--strict-header-duplicates",
            ]
        )
        self.assertEqual(exit_code, 0)

    def test_cli_json_output_is_valid(self) -> None:
        stdout_buffer = io.StringIO()
        with contextlib.redirect_stdout(stdout_buffer):
            exit_code = python_source_guard.main(
                [
                    "--project-root",
                    str(PROJECT_ROOT),
                    "--tests-root",
                    str(TESTS_ROOT),
                    "--json",
                    "--strict-header-duplicates",
                ]
            )

        self.assertEqual(exit_code, 0)

        payload = json.loads(stdout_buffer.getvalue())
        self.assertEqual(payload["has_issues"], False)
        self.assertEqual(payload["total_issues"], 0)
        self.assertIsInstance(payload["scanned_python_files"], int)
        self.assertIsInstance(payload["scanned_test_files"], int)
        self.assertIsInstance(payload["syntax_errors"], list)
        self.assertIsInstance(payload["multiple_main_guards"], list)
        self.assertIsInstance(payload["run_on_after_main_exit"], list)
        self.assertIsInstance(payload["duplicated_test_modules"], list)
        self.assertIsInstance(payload["duplicate_header_lines"], list)
        self.assertIsInstance(payload["duplicate_header_constants"], list)
        self.assertEqual(payload["strict_header_duplicates"], True)


if __name__ == "__main__":
    unittest.main()

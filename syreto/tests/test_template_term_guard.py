import importlib.util
import re
import sys
import tempfile
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).resolve().parents[1] / "template_term_guard.py"
spec = importlib.util.spec_from_file_location("template_term_guard", MODULE_PATH)
template_term_guard = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = template_term_guard
assert spec.loader is not None
spec.loader.exec_module(template_term_guard)


class TemplateTermGuardTests(unittest.TestCase):
    def test_default_scan_paths_match_template_scope(self) -> None:
        self.assertEqual(
            template_term_guard.DEFAULT_SCAN_PATHS,
            [
                "../01_protocol",
                "../02_data/codebook",
                "../04_manuscript",
                "../README.md",
            ],
        )

    def test_scan_file_detects_banned_pattern(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            sample_file = Path(tmp_dir) / "sample.md"
            sample_file.write_text("This sentence mentions bulimia nervosa.\n", encoding="utf-8")

            patterns = [re.compile(r"\bbulimia\b", flags=re.IGNORECASE)]
            matches = template_term_guard.scan_file(sample_file, patterns)

            self.assertEqual(len(matches), 1)
            self.assertEqual(matches[0].line_number, 1)

    def test_scan_file_ignores_hyphenated_bn_alias(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            sample_file = Path(tmp_dir) / "sample.md"
            sample_file.write_text(
                "Use --preset bn-pilot for profile autofill.\n", encoding="utf-8"
            )

            patterns = [re.compile(r"\bbn\b(?!-)", flags=re.IGNORECASE)]
            matches = template_term_guard.scan_file(sample_file, patterns)

            self.assertEqual(matches, [])

    def test_iter_target_files_applies_extension_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            (base / "keep.md").write_text("ok", encoding="utf-8")
            (base / "skip.py").write_text("print('x')", encoding="utf-8")

            files = template_term_guard.iter_target_files(
                scan_paths=[base],
                allowed_extensions={".md"},
                exclude_globs=[],
            )

            self.assertEqual([path.name for path in files], ["keep.md"])

    def test_scan_file_detects_placeholder_pattern_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            sample_file = Path(tmp_dir) / "protocol.md"
            sample_file.write_text("Working title: [YOUR REVIEW TITLE]\n", encoding="utf-8")

            patterns = [re.compile(r"\[(?:YOUR REVIEW TITLE)\]", flags=re.IGNORECASE)]
            matches = template_term_guard.scan_file(
                sample_file, patterns, match_group="placeholders"
            )

            self.assertEqual(len(matches), 1)
            self.assertEqual(matches[0].match_group, "placeholders")

    def test_main_placeholder_check_fails_on_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            sample_file = tmp_path / "search_strings.md"
            sample_file.write_text("([POPULATION TERM 1])\n", encoding="utf-8")
            summary_file = tmp_path / "summary.md"

            exit_code = template_term_guard.main(
                [
                    "--scan-path",
                    str(sample_file),
                    "--check-placeholders",
                    "--no-check-banned-terms",
                    "--summary-output",
                    str(summary_file),
                    "--fail-on-match",
                ]
            )

            self.assertEqual(exit_code, 1)
            self.assertTrue(summary_file.exists())
            summary_text = summary_file.read_text(encoding="utf-8")
            self.assertIn("Checks enabled: placeholders", summary_text)
            self.assertIn("placeholders matches: 1", summary_text)


if __name__ == "__main__":
    unittest.main()

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


MODULE_PATH = Path(__file__).resolve().parents[1] / "prisma_adherence_checker.py"
spec = importlib.util.spec_from_file_location("prisma_adherence_checker", MODULE_PATH)
prisma_adherence_checker = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = prisma_adherence_checker
assert spec.loader is not None
spec.loader.exec_module(prisma_adherence_checker)


class PrismaAdherenceCheckerTests(unittest.TestCase):
    def test_checklist_contains_27_items(self) -> None:
        self.assertEqual(len(prisma_adherence_checker.PRISMA_2020_ITEMS), 27)

    def test_scan_detects_missing_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manuscript_root = Path(tmp_dir)
            (manuscript_root / "main.tex").write_text(
                "\\title{Systematic review of X}\n"
                "\\begin{abstract}\n"
                "Background rationale and objective are described here.\n"
                "\\end{abstract}\n",
                encoding="utf-8",
            )

            _, assessments = prisma_adherence_checker.scan_prisma_mentions(
                manuscript_root,
                allowed_extensions={".tex"},
            )

        covered_numbers = {assessment.item.number for assessment in assessments if assessment.covered}
        missing_numbers = {assessment.item.number for assessment in assessments if not assessment.covered}

        self.assertIn(1, covered_numbers)
        self.assertIn(2, covered_numbers)
        self.assertIn(3, covered_numbers)
        self.assertIn(4, covered_numbers)
        self.assertIn(5, missing_numbers)
        self.assertGreater(len(missing_numbers), 0)

    def test_scan_can_cover_all_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manuscript_root = Path(tmp_dir)
            lines: list[str] = []

            for item in prisma_adherence_checker.PRISMA_2020_ITEMS:
                for group in item.signal_groups:
                    lines.append(f"This manuscript mentions {group[0]}.")

            (manuscript_root / "main.tex").write_text("\n".join(lines), encoding="utf-8")

            _, assessments = prisma_adherence_checker.scan_prisma_mentions(
                manuscript_root,
                allowed_extensions={".tex"},
            )

        self.assertTrue(all(assessment.covered for assessment in assessments))

    def test_latex_comment_signals_do_not_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manuscript_root = Path(tmp_dir)
            (manuscript_root / "main.tex").write_text(
                "% publication bias is only in comment\n",
                encoding="utf-8",
            )

            _, assessments = prisma_adherence_checker.scan_prisma_mentions(
                manuscript_root,
                allowed_extensions={".tex"},
            )

        item14 = next(assessment for assessment in assessments if assessment.item.number == 14)
        self.assertFalse(item14.covered)

    def test_build_report_includes_gap_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manuscript_root = Path(tmp_dir)
            file_path = manuscript_root / "main.tex"
            file_path.write_text(
                "\\title{Systematic review draft}\n"
                "\\begin{abstract}\n"
                "Background rationale and objective are noted.\n"
                "\\end{abstract}\n",
                encoding="utf-8",
            )

            files, assessments = prisma_adherence_checker.scan_prisma_mentions(
                manuscript_root,
                allowed_extensions={".tex"},
            )

            report_text = prisma_adherence_checker.build_report(
                manuscript_root=manuscript_root,
                manuscript_files=files,
                assessments=assessments,
                output_path=manuscript_root / "prisma_adherence_report.md",
            )

        self.assertIn("## Gap List", report_text)
        self.assertIn("**5. Eligibility Criteria**", report_text)


if __name__ == "__main__":
    unittest.main()
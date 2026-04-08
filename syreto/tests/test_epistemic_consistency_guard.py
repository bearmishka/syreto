import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


MODULE_PATH = Path(__file__).resolve().parents[1] / "epistemic_consistency_guard.py"
spec = importlib.util.spec_from_file_location("epistemic_consistency_guard", MODULE_PATH)
epistemic_consistency_guard = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = epistemic_consistency_guard
assert spec.loader is not None
spec.loader.exec_module(epistemic_consistency_guard)


class EpistemicConsistencyGuardTests(unittest.TestCase):
    def _build_fixture_workspace(self, base: Path) -> dict[str, Path]:
        protocol = base / "01_protocol/protocol.md"
        manuscript = base / "04_manuscript/main.tex"
        processed_dir = base / "02_data/processed"
        search_log = processed_dir / "search_log.csv"
        master_records = processed_dir / "master_records.csv"
        table = base / "04_manuscript/tables/prisma_counts_table.tex"
        tikz = base / "03_analysis/outputs/forest_plot.tikz"
        report = base / "03_analysis/outputs/epistemic_consistency_report.md"

        protocol.parent.mkdir(parents=True, exist_ok=True)
        manuscript.parent.mkdir(parents=True, exist_ok=True)
        processed_dir.mkdir(parents=True, exist_ok=True)
        table.parent.mkdir(parents=True, exist_ok=True)
        tikz.parent.mkdir(parents=True, exist_ok=True)

        protocol.write_text("# Protocol\n`[YOUR REVIEW TITLE]`\n", encoding="utf-8")
        manuscript.write_text("\\title{[YOUR REVIEW TITLE]}\n", encoding="utf-8")
        search_log.write_text(
            "database,date_searched,query_version,export_filename\n"
            "PubMed,,template_v1,pubmed_YYYY-MM-DD.ris\n",
            encoding="utf-8",
        )
        master_records.write_text(
            "record_id,title\nMR_DEMO_001,Demo row for smoke test\n",
            encoding="utf-8",
        )
        table.write_text("% table content\n", encoding="utf-8")
        tikz.write_text("% tikz content\n", encoding="utf-8")

        return {
            "protocol": protocol,
            "manuscript": manuscript,
            "processed_dir": processed_dir,
            "table": table,
            "tikz": tikz,
            "report": report,
            "search_log": search_log,
            "master_records": master_records,
        }

    def _run_guard(
        self,
        fixture: dict[str, Path],
        *,
        review_mode: str = "template",
        fail_on_risk_flag: str = "--no-fail-on-risk",
    ) -> int:
        return epistemic_consistency_guard.main(
            [
                "--placeholder-target",
                fixture["protocol"].as_posix(),
                "--placeholder-target",
                fixture["manuscript"].as_posix(),
                "--processed-dir",
                fixture["processed_dir"].as_posix(),
                "--artifact-target",
                fixture["table"].as_posix(),
                "--artifact-target",
                fixture["tikz"].as_posix(),
                "--summary-output",
                fixture["report"].as_posix(),
                "--review-mode",
                review_mode,
                fail_on_risk_flag,
            ]
        )

    def test_detects_risk_markers_and_marks_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            fixture = self._build_fixture_workspace(Path(tmp_dir))

            exit_code = self._run_guard(
                fixture, review_mode="template", fail_on_risk_flag="--no-fail-on-risk"
            )

            self.assertEqual(exit_code, 0)
            report_text = fixture["report"].read_text(encoding="utf-8")
            self.assertIn("Risk detected: yes", report_text)
            self.assertIn("Placeholder matches:", report_text)
            self.assertIn("Data marker matches:", report_text)

            table_first_line = fixture["table"].read_text(encoding="utf-8").splitlines()[0]
            tikz_first_line = fixture["tikz"].read_text(encoding="utf-8").splitlines()[0]
            self.assertEqual(table_first_line, epistemic_consistency_guard.EPISTEMIC_WARNING_MARKER)
            self.assertEqual(tikz_first_line, epistemic_consistency_guard.EPISTEMIC_WARNING_MARKER)

    def test_unmarks_artifacts_after_risk_is_resolved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            fixture = self._build_fixture_workspace(Path(tmp_dir))

            first_exit = self._run_guard(
                fixture, review_mode="template", fail_on_risk_flag="--no-fail-on-risk"
            )
            self.assertEqual(first_exit, 0)

            fixture["protocol"].write_text("# Protocol\nOperational title\n", encoding="utf-8")
            fixture["manuscript"].write_text("\\title{Operational Manuscript}\n", encoding="utf-8")
            fixture["search_log"].write_text(
                "database,date_searched,query_version,export_filename\n"
                "PubMed,2026-03-14,run_2026_03_14,pubmed_2026-03-14.ris\n",
                encoding="utf-8",
            )
            fixture["master_records"].write_text(
                "record_id,title\nMR0001,Operational row\n", encoding="utf-8"
            )

            second_exit = self._run_guard(
                fixture, review_mode="template", fail_on_risk_flag="--no-fail-on-risk"
            )

            self.assertEqual(second_exit, 0)
            table_lines = fixture["table"].read_text(encoding="utf-8").splitlines()
            tikz_lines = fixture["tikz"].read_text(encoding="utf-8").splitlines()
            self.assertNotEqual(
                table_lines[0], epistemic_consistency_guard.EPISTEMIC_WARNING_MARKER
            )
            self.assertNotEqual(tikz_lines[0], epistemic_consistency_guard.EPISTEMIC_WARNING_MARKER)

            report_text = fixture["report"].read_text(encoding="utf-8")
            self.assertIn("Risk detected: no", report_text)
            self.assertIn("Decision: pass", report_text)

    def test_strict_production_mode_fails_on_risk(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            fixture = self._build_fixture_workspace(Path(tmp_dir))

            exit_code = self._run_guard(
                fixture, review_mode="production", fail_on_risk_flag="--fail-on-risk"
            )

            self.assertEqual(exit_code, 1)
            report_text = fixture["report"].read_text(encoding="utf-8")
            self.assertIn("Review mode: production", report_text)
            self.assertIn("Fail on risk: yes", report_text)
            self.assertIn("Decision: fail", report_text)


if __name__ == "__main__":
    unittest.main()

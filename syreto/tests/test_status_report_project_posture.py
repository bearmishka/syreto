import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "status_report.py"
spec = importlib.util.spec_from_file_location("status_report", MODULE_PATH)
status_report = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = status_report
assert spec.loader is not None
spec.loader.exec_module(status_report)


class StatusReportProjectPostureTests(unittest.TestCase):
    def _full_master_df(self) -> pd.DataFrame:
        rows = [
            {
                "record_id": "MR_DEMO_001",
                "source_database": "pubmed",
                "source_record_id": "PMID_DEMO_001",
                "title": "Demo Study A",
                "abstract": "Demo abstract",
                "authors": "Smith, J.",
                "year": "2021",
                "journal": "Demo Journal",
                "doi": "10.1000/demo-a",
                "pmid": "",
                "normalized_title": "demo study a",
                "normalized_first_author": "smith",
                "is_duplicate": "no",
                "duplicate_of_record_id": "",
                "dedup_reason": "",
                "notes": "Demo record",
            }
        ]
        return pd.DataFrame(rows, columns=status_report.MASTER_RECORD_COLUMNS)

    def _prisma_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"stage": "records_identified_databases", "count": "1", "notes": ""},
                {"stage": "duplicates_removed", "count": "0", "notes": ""},
                {"stage": "records_screened_title_abstract", "count": "1", "notes": ""},
                {"stage": "records_excluded_title_abstract", "count": "", "notes": ""},
                {"stage": "reports_assessed_full_text", "count": "", "notes": ""},
                {"stage": "studies_included_qualitative_synthesis", "count": "", "notes": ""},
            ]
        )

    def _write_minimum_artifacts(self, base: Path) -> dict[str, Path]:
        screening_summary_path = base / "screening_metrics_summary.md"
        screening_summary_path.write_text("# Screening metrics\n", encoding="utf-8")

        csv_validation_path = base / "csv_input_validation_summary.md"
        csv_validation_path.write_text("- Errors: 0\n- Warnings: 0\n", encoding="utf-8")

        extraction_validation_path = base / "extraction_validation_summary.md"
        extraction_validation_path.write_text("- Errors: 0\n- Warnings: 0\n", encoding="utf-8")

        quality_summary_path = base / "quality_appraisal_summary.md"
        quality_summary_path.write_text("ok\n", encoding="utf-8")
        quality_scored_path = base / "quality_appraisal_scored.csv"
        quality_scored_path.write_text("study_id,score\nA,1\n", encoding="utf-8")

        effect_summary_path = base / "effect_size_conversion_summary.md"
        effect_summary_path.write_text("ok\n", encoding="utf-8")
        effect_converted_path = base / "effect_size_converted.csv"
        effect_converted_path.write_text("study_id,converted_d\nA,0.2\n", encoding="utf-8")

        dedup_summary_path = base / "dedup_stats_summary.md"
        dedup_summary_path.write_text("ok\n", encoding="utf-8")
        prisma_flow_path = base / "prisma_flow_diagram.tex"
        prisma_flow_path.write_text("% flow\n", encoding="utf-8")

        return {
            "screening_summary": screening_summary_path,
            "csv_validation": csv_validation_path,
            "extraction_validation": extraction_validation_path,
            "quality_summary": quality_summary_path,
            "quality_scored": quality_scored_path,
            "effect_summary": effect_summary_path,
            "effect_converted": effect_converted_path,
            "dedup_summary": dedup_summary_path,
            "prisma_flow": prisma_flow_path,
        }

    def test_project_posture_reports_semantic_blocker_when_placeholders_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            artifacts = self._write_minimum_artifacts(base)

            protocol_path = base / "protocol.md"
            protocol_path.write_text("# Protocol\n[YOUR REVIEW TITLE]\n", encoding="utf-8")

            manuscript_path = base / "main.tex"
            manuscript_path.write_text("\\title{[YOUR REVIEW TITLE]}\n", encoding="utf-8")

            screening_df = pd.DataFrame(columns=["date", "reviewer", "stage", "records_screened"])
            screening_records_df = pd.DataFrame(
                columns=["record_id", "reviewer", "title_abstract_decision"]
            )
            master_df = self._full_master_df()
            search_df = pd.DataFrame([{"database": "PubMed", "results_total": "1"}])
            prisma_df = self._prisma_df()

            report, summary = status_report.build_status_report(
                screening_df=screening_df,
                screening_records_df=screening_records_df,
                master_df=master_df,
                search_df=search_df,
                prisma_df=prisma_df,
                protocol_path=protocol_path,
                manuscript_path=manuscript_path,
                screening_summary_path=artifacts["screening_summary"],
                csv_input_validation_summary_path=artifacts["csv_validation"],
                extraction_validation_summary_path=artifacts["extraction_validation"],
                quality_appraisal_summary_path=artifacts["quality_summary"],
                quality_appraisal_scored_path=artifacts["quality_scored"],
                effect_size_conversion_summary_path=artifacts["effect_summary"],
                effect_size_converted_path=artifacts["effect_converted"],
                dedup_summary_path=artifacts["dedup_summary"],
                prisma_flow_path=artifacts["prisma_flow"],
            )

            self.assertIn("## Project Posture", report)
            self.assertIn("Primary blocker: semantic_completeness", report)
            self.assertEqual(summary["project_posture"]["primary_blocker"], "semantic_completeness")
            self.assertFalse(summary["project_posture"]["semantic_completeness"]["complete"])
            self.assertGreater(
                summary["project_posture"]["semantic_completeness"]["protocol_placeholder_count"], 0
            )
            placeholder_policy = next(
                item for item in summary["input_checklist"] if item["id"] == "semantic_placeholders"
            )
            self.assertTrue(placeholder_policy["done"])
            self.assertIn("REVIEW_MODE=template", placeholder_policy["details"])

    def test_project_posture_marks_semantic_complete_when_no_placeholders(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            artifacts = self._write_minimum_artifacts(base)

            protocol_path = base / "protocol.md"
            protocol_path.write_text("# Protocol\nFinalized review title\n", encoding="utf-8")

            manuscript_path = base / "main.tex"
            manuscript_path.write_text("\\title{Finalized review title}\n", encoding="utf-8")

            screening_df = pd.DataFrame(columns=["date", "reviewer", "stage", "records_screened"])
            screening_records_df = pd.DataFrame(
                columns=["record_id", "reviewer", "title_abstract_decision"]
            )
            master_df = self._full_master_df()
            search_df = pd.DataFrame([{"database": "PubMed", "results_total": "1"}])
            prisma_df = self._prisma_df()

            report, summary = status_report.build_status_report(
                screening_df=screening_df,
                screening_records_df=screening_records_df,
                master_df=master_df,
                search_df=search_df,
                prisma_df=prisma_df,
                protocol_path=protocol_path,
                manuscript_path=manuscript_path,
                screening_summary_path=artifacts["screening_summary"],
                csv_input_validation_summary_path=artifacts["csv_validation"],
                extraction_validation_summary_path=artifacts["extraction_validation"],
                quality_appraisal_summary_path=artifacts["quality_summary"],
                quality_appraisal_scored_path=artifacts["quality_scored"],
                effect_size_conversion_summary_path=artifacts["effect_summary"],
                effect_size_converted_path=artifacts["effect_converted"],
                dedup_summary_path=artifacts["dedup_summary"],
                prisma_flow_path=artifacts["prisma_flow"],
            )

            self.assertIn("## Project Posture", report)
            self.assertIn("Primary blocker: none", report)
            self.assertIsNone(summary["project_posture"]["primary_blocker"])
            self.assertTrue(summary["project_posture"]["semantic_completeness"]["complete"])

    def test_production_mode_escalates_unresolved_placeholders_to_blocking_checklist_item(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            artifacts = self._write_minimum_artifacts(base)

            protocol_path = base / "protocol.md"
            protocol_path.write_text("# Protocol\n[YOUR REVIEW TITLE]\n", encoding="utf-8")

            manuscript_path = base / "main.tex"
            manuscript_path.write_text("\\title{[YOUR REVIEW TITLE]}\n", encoding="utf-8")

            screening_df = pd.DataFrame(columns=["date", "reviewer", "stage", "records_screened"])
            screening_records_df = pd.DataFrame(
                columns=["record_id", "reviewer", "title_abstract_decision"]
            )
            master_df = self._full_master_df()
            search_df = pd.DataFrame([{"database": "PubMed", "results_total": "1"}])
            prisma_df = self._prisma_df()

            _, summary = status_report.build_status_report(
                screening_df=screening_df,
                screening_records_df=screening_records_df,
                master_df=master_df,
                search_df=search_df,
                prisma_df=prisma_df,
                protocol_path=protocol_path,
                manuscript_path=manuscript_path,
                screening_summary_path=artifacts["screening_summary"],
                csv_input_validation_summary_path=artifacts["csv_validation"],
                extraction_validation_summary_path=artifacts["extraction_validation"],
                quality_appraisal_summary_path=artifacts["quality_summary"],
                quality_appraisal_scored_path=artifacts["quality_scored"],
                effect_size_conversion_summary_path=artifacts["effect_summary"],
                effect_size_converted_path=artifacts["effect_converted"],
                dedup_summary_path=artifacts["dedup_summary"],
                prisma_flow_path=artifacts["prisma_flow"],
                review_mode="production",
            )

            self.assertEqual(summary["review_mode"], "production")
            placeholder_policy = next(
                item for item in summary["input_checklist"] if item["id"] == "semantic_placeholders"
            )
            self.assertFalse(placeholder_policy["done"])
            self.assertIn("REVIEW_MODE=production", placeholder_policy["details"])
            self.assertTrue(
                any(
                    check["level"] == "warning"
                    and "not allowed in REVIEW_MODE=production" in check["message"]
                    for check in summary["health_checks"]
                )
            )


if __name__ == "__main__":
    unittest.main()

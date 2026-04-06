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


class StatusReportStageAlignmentTests(unittest.TestCase):
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
            },
            {
                "record_id": "MR_DEMO_002",
                "source_database": "scopus",
                "source_record_id": "SCOPUS_DEMO_002",
                "title": "Demo Study B",
                "abstract": "Demo abstract",
                "authors": "Doe, A.",
                "year": "2020",
                "journal": "Demo Journal",
                "doi": "10.1000/demo-b",
                "pmid": "",
                "normalized_title": "demo study b",
                "normalized_first_author": "doe",
                "is_duplicate": "no",
                "duplicate_of_record_id": "",
                "dedup_reason": "",
                "notes": "Demo record",
            },
        ]
        return pd.DataFrame(rows, columns=status_report.MASTER_RECORD_COLUMNS)

    def _prisma_df(self, *, identified: int, duplicates: int, screened: int) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"stage": "records_identified_databases", "count": str(identified), "notes": ""},
                {"stage": "duplicates_removed", "count": str(duplicates), "notes": ""},
                {"stage": "records_screened_title_abstract", "count": str(screened), "notes": ""},
                {"stage": "records_excluded_title_abstract", "count": "", "notes": ""},
                {"stage": "reports_assessed_full_text", "count": "", "notes": ""},
                {"stage": "studies_included_qualitative_synthesis", "count": "", "notes": ""},
            ]
        )

    def _write_minimum_artifacts(self, base: Path) -> dict[str, Path]:
        protocol_path = base / "protocol.md"
        protocol_path.write_text("# Protocol\nNo registration yet.\n", encoding="utf-8")

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
            "protocol": protocol_path,
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

    def test_bootstrap_demo_stage_and_partial_prisma_sync(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            artifacts = self._write_minimum_artifacts(base)

            screening_df = pd.DataFrame(columns=["date", "reviewer", "stage", "records_screened"])
            screening_records_df = pd.DataFrame(columns=["record_id", "reviewer", "title_abstract_decision"])
            master_df = self._full_master_df()
            search_df = pd.DataFrame(
                [
                    {
                        "database": "PubMed",
                        "date_searched": "",
                        "results_total": "0",
                    }
                ]
            )
            prisma_df = self._prisma_df(identified=2, duplicates=0, screened=2)

            _, summary = status_report.build_status_report(
                screening_df=screening_df,
                screening_records_df=screening_records_df,
                master_df=master_df,
                search_df=search_df,
                prisma_df=prisma_df,
                protocol_path=artifacts["protocol"],
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

            self.assertEqual(summary["stage_assessment"]["id"], "bootstrap_demo")
            self.assertTrue(
                any(
                    "PRISMA identified count cannot be fully verified" in check["message"]
                    for check in summary["health_checks"]
                )
            )
            self.assertFalse(
                any(
                    check["level"] == "error" and "PRISMA counts are out of sync" in check["message"]
                    for check in summary["health_checks"]
                )
            )

            prisma_check = next(item for item in summary["input_checklist"] if item["id"] == "prisma_sync")
            self.assertTrue(prisma_check["done"])

    def test_prisma_mismatch_remains_error_when_search_totals_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            artifacts = self._write_minimum_artifacts(base)

            screening_df = pd.DataFrame(columns=["date", "reviewer", "stage", "records_screened"])
            screening_records_df = pd.DataFrame(columns=["record_id", "reviewer", "title_abstract_decision"])
            master_df = self._full_master_df()
            search_df = pd.DataFrame([{"database": "PubMed", "date_searched": "2026-03-14", "results_total": "5"}])
            prisma_df = self._prisma_df(identified=2, duplicates=0, screened=2)

            _, summary = status_report.build_status_report(
                screening_df=screening_df,
                screening_records_df=screening_records_df,
                master_df=master_df,
                search_df=search_df,
                prisma_df=prisma_df,
                protocol_path=artifacts["protocol"],
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

            self.assertTrue(
                any(
                    check["level"] == "error" and "PRISMA counts are out of sync" in check["message"]
                    for check in summary["health_checks"]
                )
            )

            prisma_check = next(item for item in summary["input_checklist"] if item["id"] == "prisma_sync")
            self.assertFalse(prisma_check["done"])

    def test_prisma_partial_screening_is_not_flagged_as_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            artifacts = self._write_minimum_artifacts(base)

            screening_df = pd.DataFrame(columns=["date", "reviewer", "stage", "records_screened"])
            screening_records_df = pd.DataFrame(columns=["record_id", "reviewer", "title_abstract_decision"])
            master_df = self._full_master_df()
            search_df = pd.DataFrame([{"database": "PubMed", "date_searched": "2026-03-14", "results_total": "2"}])
            prisma_df = self._prisma_df(identified=2, duplicates=0, screened=1)

            _, summary = status_report.build_status_report(
                screening_df=screening_df,
                screening_records_df=screening_records_df,
                master_df=master_df,
                search_df=search_df,
                prisma_df=prisma_df,
                protocol_path=artifacts["protocol"],
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

            self.assertFalse(
                any(
                    check["level"] == "error" and "PRISMA counts are out of sync" in check["message"]
                    for check in summary["health_checks"]
                )
            )
            self.assertTrue(
                any(
                    "Title/abstract screening is in progress" in check["message"]
                    for check in summary["health_checks"]
                )
            )
            self.assertFalse(any("computed unique records" in warning for warning in summary["warnings"]))

            prisma_check = next(item for item in summary["input_checklist"] if item["id"] == "prisma_sync")
            self.assertTrue(prisma_check["done"])

    def test_reviewer_workload_metrics_are_exposed_in_status_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            artifacts = self._write_minimum_artifacts(base)

            reviewer_workload_summary_path = base / "reviewer_workload_balancer_summary.md"
            reviewer_workload_summary_path.write_text(
                "\n".join(
                    [
                        "# Reviewer Workload Balancer Summary",
                        "",
                        "## Scope",
                        "",
                        "- Screening log: `screening_daily_log.csv`",
                        "- Stage filter: `title_abstract`",
                        "- Output plan: `reviewer_workload_plan.csv`",
                        "",
                        "## Snapshot",
                        "",
                        "- Reviewers observed: 2",
                        "- Total screened records in scope: 14",
                        "- Non-blocking fallback active: yes",
                        "",
                        "## Status",
                        "",
                        "- Balanced plan generated for 2 reviewers.",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            screening_df = pd.DataFrame(columns=["date", "reviewer", "stage", "records_screened"])
            screening_records_df = pd.DataFrame(columns=["record_id", "reviewer", "title_abstract_decision"])
            master_df = self._full_master_df()
            search_df = pd.DataFrame([{"database": "PubMed", "date_searched": "2026-03-14", "results_total": "5"}])
            prisma_df = self._prisma_df(identified=2, duplicates=0, screened=2)

            _, summary = status_report.build_status_report(
                screening_df=screening_df,
                screening_records_df=screening_records_df,
                master_df=master_df,
                search_df=search_df,
                prisma_df=prisma_df,
                protocol_path=artifacts["protocol"],
                screening_summary_path=artifacts["screening_summary"],
                csv_input_validation_summary_path=artifacts["csv_validation"],
                extraction_validation_summary_path=artifacts["extraction_validation"],
                quality_appraisal_summary_path=artifacts["quality_summary"],
                quality_appraisal_scored_path=artifacts["quality_scored"],
                effect_size_conversion_summary_path=artifacts["effect_summary"],
                effect_size_converted_path=artifacts["effect_converted"],
                dedup_summary_path=artifacts["dedup_summary"],
                prisma_flow_path=artifacts["prisma_flow"],
                reviewer_workload_summary_path=reviewer_workload_summary_path,
            )

            workload = summary["reviewer_workload_balancer"]
            self.assertTrue(workload["present"])
            self.assertTrue(workload["parsed"])
            self.assertEqual(workload["stage_filter"], "title_abstract")
            self.assertEqual(workload["reviewers_observed"], 2)
            self.assertEqual(workload["total_screened_records_in_scope"], 14)
            self.assertTrue(workload["non_blocking_fallback_active"])


if __name__ == "__main__":
    unittest.main()
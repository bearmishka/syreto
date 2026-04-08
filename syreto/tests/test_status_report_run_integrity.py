import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

MODULE_PATH = Path(__file__).resolve().parents[1] / "status_report.py"
spec = importlib.util.spec_from_file_location("status_report", MODULE_PATH)
status_report = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = status_report
assert spec.loader is not None
spec.loader.exec_module(status_report)


class StatusReportRunIntegrityTests(unittest.TestCase):
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

    def _build_base_frames(
        self,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        screening_df = pd.DataFrame(columns=["date", "reviewer", "stage", "records_screened"])
        screening_records_df = pd.DataFrame(
            columns=["record_id", "reviewer", "title_abstract_decision"]
        )
        master_df = pd.DataFrame(
            [
                {
                    "record_id": "MR0001",
                    "source_database": "pubmed",
                    "source_record_id": "PMID_1",
                    "title": "Study A",
                    "abstract": "Abstract",
                    "authors": "Doe, A.",
                    "year": "2024",
                    "journal": "Journal",
                    "doi": "10.1000/a",
                    "pmid": "",
                    "normalized_title": "study a",
                    "normalized_first_author": "doe",
                    "is_duplicate": "no",
                    "duplicate_of_record_id": "",
                    "dedup_reason": "",
                    "notes": "",
                }
            ],
            columns=status_report.MASTER_RECORD_COLUMNS,
        )
        search_df = pd.DataFrame(
            [{"database": "PubMed", "date_searched": "2026-03-15", "results_total": "1"}]
        )
        prisma_df = pd.DataFrame(
            [
                {"stage": "records_identified_databases", "count": "1", "notes": ""},
                {"stage": "duplicates_removed", "count": "0", "notes": ""},
                {"stage": "records_screened_title_abstract", "count": "1", "notes": ""},
                {"stage": "records_excluded_title_abstract", "count": "", "notes": ""},
                {"stage": "reports_assessed_full_text", "count": "", "notes": ""},
                {"stage": "studies_included_qualitative_synthesis", "count": "", "notes": ""},
            ]
        )
        return screening_df, screening_records_df, master_df, search_df, prisma_df

    def test_failed_marker_is_reported_as_integrity_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            artifacts = self._write_minimum_artifacts(base)
            screening_df, screening_records_df, master_df, search_df, prisma_df = (
                self._build_base_frames()
            )

            protocol_path = base / "protocol.md"
            protocol_path.write_text("# Protocol\nPROSPERO: CRD42026000001\n", encoding="utf-8")
            manuscript_path = base / "main.tex"
            manuscript_path.write_text("\\title{Review}\n", encoding="utf-8")

            manifest_path = base / "daily_run_manifest.json"
            manifest_path.write_text(
                '{"run_id":"run-1","state":"failed","started_at_utc":"2026-03-15T09:00:00Z","updated_at_utc":"2026-03-15T09:01:00Z","pipeline_exit_code":1,"status_checkpoint_exit_code":0,"final_exit_code":1,"failure_phase":"pipeline","rollback_applied":true,"transactional_mode":"enabled"}\n',
                encoding="utf-8",
            )
            failed_marker_path = base / "daily_run_failed.marker"
            failed_marker_path.write_text(
                '{"run_id":"run-1","failed_at_utc":"2026-03-15T09:01:00Z","pipeline_exit_code":1,"status_checkpoint_exit_code":0,"final_exit_code":1,"failure_phase":"pipeline"}\n',
                encoding="utf-8",
            )

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
                daily_run_manifest_path=manifest_path,
                daily_run_failed_marker_path=failed_marker_path,
            )

            self.assertIn("## Daily Run Integrity", report)
            self.assertFalse(summary["daily_run_integrity"]["ok"])
            self.assertTrue(
                any(
                    check["level"] == "error" and "marked failed" in check["message"]
                    for check in summary["health_checks"]
                )
            )
            self.assertTrue(
                any(
                    check["level"] == "warning" and "rollback was applied" in check["message"]
                    for check in summary["health_checks"]
                )
            )

    def test_success_manifest_without_marker_is_reported_clean(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            artifacts = self._write_minimum_artifacts(base)
            screening_df, screening_records_df, master_df, search_df, prisma_df = (
                self._build_base_frames()
            )

            protocol_path = base / "protocol.md"
            protocol_path.write_text("# Protocol\nPROSPERO: CRD42026000001\n", encoding="utf-8")
            manuscript_path = base / "main.tex"
            manuscript_path.write_text("\\title{Review}\n", encoding="utf-8")

            manifest_path = base / "daily_run_manifest.json"
            manifest_path.write_text(
                '{"run_id":"run-2","state":"success","started_at_utc":"2026-03-15T09:00:00Z","updated_at_utc":"2026-03-15T09:02:00Z","pipeline_exit_code":0,"status_checkpoint_exit_code":0,"final_exit_code":0,"failure_phase":"","rollback_applied":false,"transactional_mode":"enabled"}\n',
                encoding="utf-8",
            )
            failed_marker_path = base / "daily_run_failed.marker"

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
                daily_run_manifest_path=manifest_path,
                daily_run_failed_marker_path=failed_marker_path,
            )

            self.assertTrue(summary["daily_run_integrity"]["ok"])
            self.assertTrue(
                any("clean completed run" in check["message"] for check in summary["health_checks"])
            )
            self.assertFalse(
                any("partially updated" in check["message"] for check in summary["health_checks"])
            )

    def test_concatenated_manifest_stream_recovers_last_object_with_warning(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            artifacts = self._write_minimum_artifacts(base)
            screening_df, screening_records_df, master_df, search_df, prisma_df = (
                self._build_base_frames()
            )

            protocol_path = base / "protocol.md"
            protocol_path.write_text("# Protocol\nPROSPERO: CRD42026000001\n", encoding="utf-8")
            manuscript_path = base / "main.tex"
            manuscript_path.write_text("\\title{Review}\n", encoding="utf-8")

            manifest_path = base / "daily_run_manifest.json"
            manifest_path.write_text(
                '{"run_id":"run-3","state":"success","started_at_utc":"2026-03-15T09:00:00Z","updated_at_utc":"2026-03-15T09:02:00Z","pipeline_exit_code":0,"status_checkpoint_exit_code":0,"final_exit_code":0,"failure_phase":"","rollback_applied":false,"transactional_mode":"enabled"}'
                '{"run_id":"run-3","state":"success","started_at_utc":"2026-03-15T09:00:00Z","updated_at_utc":"2026-03-15T09:02:00Z","pipeline_exit_code":0,"status_checkpoint_exit_code":0,"final_exit_code":0,"failure_phase":"","rollback_applied":false,"transactional_mode":"enabled"}\n',
                encoding="utf-8",
            )
            failed_marker_path = base / "daily_run_failed.marker"

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
                daily_run_manifest_path=manifest_path,
                daily_run_failed_marker_path=failed_marker_path,
            )

            self.assertIn("## Daily Run Integrity", report)
            self.assertEqual(
                summary["daily_run_integrity"]["manifest"]["stream_object_count"],
                2,
            )
            self.assertTrue(
                any(
                    check["level"] == "warning"
                    and "contains concatenated JSON objects" in check["message"]
                    for check in summary["health_checks"]
                )
            )

    def test_running_manifest_for_current_daily_run_is_informational(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            artifacts = self._write_minimum_artifacts(base)
            screening_df, screening_records_df, master_df, search_df, prisma_df = (
                self._build_base_frames()
            )

            protocol_path = base / "protocol.md"
            protocol_path.write_text("# Protocol\nPROSPERO: CRD42026000001\n", encoding="utf-8")
            manuscript_path = base / "main.tex"
            manuscript_path.write_text("\\title{Review}\n", encoding="utf-8")

            manifest_path = base / "daily_run_manifest.json"
            manifest_path.write_text(
                '{"run_id":"run-current","state":"running","started_at_utc":"2026-03-15T09:00:00Z","updated_at_utc":"2026-03-15T09:02:00Z","pipeline_exit_code":-1,"status_checkpoint_exit_code":-1,"final_exit_code":-1,"failure_phase":"","rollback_applied":false,"transactional_mode":"enabled"}\n',
                encoding="utf-8",
            )
            failed_marker_path = base / "daily_run_failed.marker"

            previous_run_id = os.environ.get("DAILY_RUN_ID")
            os.environ["DAILY_RUN_ID"] = "run-current"
            try:
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
                    daily_run_manifest_path=manifest_path,
                    daily_run_failed_marker_path=failed_marker_path,
                )
            finally:
                if previous_run_id is None:
                    os.environ.pop("DAILY_RUN_ID", None)
                else:
                    os.environ["DAILY_RUN_ID"] = previous_run_id

            self.assertTrue(summary["daily_run_integrity"]["ok"])
            self.assertTrue(
                any(
                    check["level"] == "info" and "current in-progress run" in check["message"]
                    for check in summary["health_checks"]
                )
            )
            self.assertFalse(
                any(
                    check["level"] == "warning" and "unfinished run" in check["message"]
                    for check in summary["health_checks"]
                )
            )


if __name__ == "__main__":
    unittest.main()

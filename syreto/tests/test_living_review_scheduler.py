import importlib.util
from datetime import date
from pathlib import Path
import sys
import tempfile
import unittest

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "living_review_scheduler.py"
spec = importlib.util.spec_from_file_location("living_review_scheduler", MODULE_PATH)
living_review_scheduler = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = living_review_scheduler
assert spec.loader is not None
spec.loader.exec_module(living_review_scheduler)


class LivingReviewSchedulerTests(unittest.TestCase):
    def test_build_search_diffs_detects_query_and_result_changes(self) -> None:
        search_log_df = pd.DataFrame(
            [
                {
                    "database": "PubMed",
                    "date_searched": "2026-01-01",
                    "query_version": "v1",
                    "filters_applied": "humans",
                    "results_total": "100",
                    "results_exported": "95",
                },
                {
                    "database": "PubMed",
                    "date_searched": "2026-02-10",
                    "query_version": "v2",
                    "filters_applied": "humans+adults",
                    "results_total": "125",
                    "results_exported": "120",
                },
            ]
        )

        sessions_df = living_review_scheduler.prepare_search_sessions(search_log_df)
        diffs_df = living_review_scheduler.build_search_diffs(sessions_df)

        self.assertEqual(int(diffs_df.shape[0]), 1)
        row = diffs_df.iloc[0]
        self.assertEqual(row["database"], "PubMed")
        self.assertEqual(row["days_between_searches"], "40")
        self.assertEqual(row["query_version_changed"], "yes")
        self.assertEqual(row["filters_changed"], "yes")
        self.assertEqual(row["drift_severity"], "high")
        self.assertEqual(row["delta_results_total"], "25")
        self.assertEqual(row["delta_results_exported"], "25")

    def test_build_search_diffs_high_severity_on_large_absolute_delta_fallback(self) -> None:
        search_log_df = pd.DataFrame(
            [
                {
                    "database": "Scopus",
                    "date_searched": "2026-01-01",
                    "query_version": "v1",
                    "filters_applied": "humans",
                    "results_total": "1000",
                    "results_exported": "900",
                },
                {
                    "database": "Scopus",
                    "date_searched": "2026-02-01",
                    "query_version": "v1",
                    "filters_applied": "humans",
                    "results_total": "1205",
                    "results_exported": "905",
                },
            ]
        )

        sessions_df = living_review_scheduler.prepare_search_sessions(search_log_df)
        diffs_df = living_review_scheduler.build_search_diffs(sessions_df)

        self.assertEqual(int(diffs_df.shape[0]), 1)
        row = diffs_df.iloc[0]
        self.assertEqual(row["query_version_changed"], "no")
        self.assertEqual(row["filters_changed"], "no")
        self.assertEqual(row["delta_results_total"], "205")
        self.assertEqual(row["drift_severity"], "high")

    def test_build_living_schedule_includes_overdue_and_no_prior_rows(self) -> None:
        search_log_df = pd.DataFrame(
            [
                {
                    "database": "PubMed",
                    "date_searched": "2026-01-01",
                    "query_version": "v1",
                    "results_total": "100",
                    "results_exported": "95",
                },
                {
                    "database": "Embase",
                    "date_searched": "",
                    "query_version": "template_v1",
                    "results_total": "0",
                    "results_exported": "0",
                },
            ]
        )
        sessions_df = living_review_scheduler.prepare_search_sessions(search_log_df)

        schedule_df = living_review_scheduler.build_living_schedule(
            sessions_df,
            include_databases=["PubMed", "Embase"],
            cadence_days=30,
            horizon_cycles=2,
            today=date(2026, 2, 10),
            review_mode="living",
        )

        self.assertEqual(int(schedule_df.shape[0]), 3)

        pubmed_cycle_1 = schedule_df.loc[
            (schedule_df["database"] == "PubMed") & (schedule_df["cycle_index"] == "1")
        ].iloc[0]
        self.assertEqual(pubmed_cycle_1["scheduled_search_date"], "2026-01-31")
        self.assertEqual(pubmed_cycle_1["schedule_status"], "overdue")
        self.assertEqual(pubmed_cycle_1["days_until_due"], "-10")

        embase_row = schedule_df.loc[schedule_df["database"] == "Embase"].iloc[0]
        self.assertEqual(embase_row["schedule_status"], "no_prior_completed_search")

    def test_main_living_mode_generates_schedule_and_diffs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            search_log_path = tmp_path / "search_log.csv"
            search_log_path.write_text(
                "database,date_searched,query_version,start_year,end_date,filters_applied,results_total,results_exported,export_filename,notes\n"
                "PubMed,2026-01-01,v1,1980,2026-01-01,humans,100,95,pubmed_2026-01-01.ris,\n"
                "PubMed,2026-02-10,v2,1980,2026-02-10,humans+adults,125,120,pubmed_2026-02-10.ris,\n"
                "Scopus,2026-02-01,v1,1980,2026-02-01,,200,190,scopus_2026-02-01.ris,\n",
                encoding="utf-8",
            )

            schedule_output = tmp_path / "schedule.csv"
            diffs_output = tmp_path / "diffs.csv"
            summary_output = tmp_path / "summary.md"

            exit_code = living_review_scheduler.main(
                [
                    "--search-log",
                    str(search_log_path),
                    "--review-mode",
                    "living",
                    "--cadence-days",
                    "30",
                    "--horizon-cycles",
                    "2",
                    "--today",
                    "2026-03-01",
                    "--schedule-output",
                    str(schedule_output),
                    "--diffs-output",
                    str(diffs_output),
                    "--summary-output",
                    str(summary_output),
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue(schedule_output.exists())
            self.assertTrue(diffs_output.exists())
            self.assertTrue(summary_output.exists())

            schedule_df = pd.read_csv(schedule_output, dtype=str)
            diffs_df = pd.read_csv(diffs_output, dtype=str)
            summary_text = summary_output.read_text(encoding="utf-8")

            self.assertEqual(int(diffs_df.shape[0]), 1)
            self.assertGreater(int(schedule_df.shape[0]), 0)
            self.assertIn("Requested review mode: `living`", summary_text)
            self.assertIn("Resolved review mode: `living`", summary_text)
            self.assertIn("Review-mode source: `cli`", summary_text)
            self.assertIn("Drift severity `high`:", summary_text)

    def test_main_standard_mode_writes_empty_schedule(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            search_log_path = tmp_path / "search_log.csv"
            search_log_path.write_text(
                "database,date_searched,query_version,start_year,end_date,filters_applied,results_total,results_exported,export_filename,notes\n"
                "PubMed,2026-01-01,v1,1980,2026-01-01,humans,100,95,pubmed_2026-01-01.ris,\n",
                encoding="utf-8",
            )

            schedule_output = tmp_path / "schedule.csv"
            diffs_output = tmp_path / "diffs.csv"
            summary_output = tmp_path / "summary.md"

            exit_code = living_review_scheduler.main(
                [
                    "--search-log",
                    str(search_log_path),
                    "--review-mode",
                    "standard",
                    "--schedule-output",
                    str(schedule_output),
                    "--diffs-output",
                    str(diffs_output),
                    "--summary-output",
                    str(summary_output),
                ]
            )

            self.assertEqual(exit_code, 0)
            schedule_df = pd.read_csv(schedule_output, dtype=str)
            summary_text = summary_output.read_text(encoding="utf-8")

            self.assertEqual(int(schedule_df.shape[0]), 0)
            self.assertIn("Requested review mode: `standard`", summary_text)
            self.assertIn("Resolved review mode: `standard`", summary_text)
            self.assertIn("Review-mode source: `cli`", summary_text)

    def test_main_auto_mode_resolves_living_from_protocol_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            protocol_path = tmp_path / "protocol.md"
            protocol_path.write_text(
                "# Protocol\n\n"
                "This project is a living review with continual updates to the search cadence.\n",
                encoding="utf-8",
            )

            search_log_path = tmp_path / "search_log.csv"
            search_log_path.write_text(
                "database,date_searched,query_version,start_year,end_date,filters_applied,results_total,results_exported,export_filename,notes\n"
                "PubMed,2026-01-01,v1,1980,2026-01-01,humans,100,95,pubmed_2026-01-01.ris,\n",
                encoding="utf-8",
            )

            schedule_output = tmp_path / "schedule.csv"
            diffs_output = tmp_path / "diffs.csv"
            summary_output = tmp_path / "summary.md"

            exit_code = living_review_scheduler.main(
                [
                    "--search-log",
                    str(search_log_path),
                    "--review-mode",
                    "auto",
                    "--protocol",
                    str(protocol_path),
                    "--cadence-days",
                    "30",
                    "--today",
                    "2026-02-15",
                    "--schedule-output",
                    str(schedule_output),
                    "--diffs-output",
                    str(diffs_output),
                    "--summary-output",
                    str(summary_output),
                ]
            )

            self.assertEqual(exit_code, 0)
            schedule_df = pd.read_csv(schedule_output, dtype=str)
            summary_text = summary_output.read_text(encoding="utf-8")

            self.assertGreater(int(schedule_df.shape[0]), 0)
            self.assertIn("Requested review mode: `auto`", summary_text)
            self.assertIn("Resolved review mode: `living`", summary_text)
            self.assertIn("Review-mode source: `protocol`", summary_text)
            self.assertIn("Protocol mode signal:", summary_text)

    def test_main_auto_mode_defaults_to_standard_without_protocol_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            protocol_path = tmp_path / "protocol.md"
            protocol_path.write_text("# Protocol\n\nStandard one-pass review.\n", encoding="utf-8")

            search_log_path = tmp_path / "search_log.csv"
            search_log_path.write_text(
                "database,date_searched,query_version,start_year,end_date,filters_applied,results_total,results_exported,export_filename,notes\n"
                "PubMed,2026-01-01,v1,1980,2026-01-01,humans,100,95,pubmed_2026-01-01.ris,\n",
                encoding="utf-8",
            )

            schedule_output = tmp_path / "schedule.csv"
            diffs_output = tmp_path / "diffs.csv"
            summary_output = tmp_path / "summary.md"

            exit_code = living_review_scheduler.main(
                [
                    "--search-log",
                    str(search_log_path),
                    "--review-mode",
                    "auto",
                    "--protocol",
                    str(protocol_path),
                    "--schedule-output",
                    str(schedule_output),
                    "--diffs-output",
                    str(diffs_output),
                    "--summary-output",
                    str(summary_output),
                ]
            )

            self.assertEqual(exit_code, 0)
            schedule_df = pd.read_csv(schedule_output, dtype=str)
            summary_text = summary_output.read_text(encoding="utf-8")

            self.assertEqual(int(schedule_df.shape[0]), 0)
            self.assertIn("Requested review mode: `auto`", summary_text)
            self.assertIn("Resolved review mode: `standard`", summary_text)
            self.assertIn("Review-mode source: `default`", summary_text)

    def test_check_cadence_reports_overdue_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            search_log_path = tmp_path / "search_log.csv"
            search_log_path.write_text(
                "database,date_searched,query_version,start_year,end_date,filters_applied,results_total,results_exported,export_filename,notes\n"
                "PubMed,2026-01-01,v1,1980,2026-01-01,humans,100,95,pubmed_2026-01-01.ris,\n",
                encoding="utf-8",
            )

            schedule_output = tmp_path / "schedule.csv"
            diffs_output = tmp_path / "diffs.csv"
            summary_output = tmp_path / "summary.md"

            exit_code = living_review_scheduler.main(
                [
                    "--search-log",
                    str(search_log_path),
                    "--review-mode",
                    "standard",
                    "--check-cadence",
                    "--cadence-days",
                    "30",
                    "--today",
                    "2026-02-20",
                    "--schedule-output",
                    str(schedule_output),
                    "--diffs-output",
                    str(diffs_output),
                    "--summary-output",
                    str(summary_output),
                ]
            )

            self.assertEqual(exit_code, 0)
            summary_text = summary_output.read_text(encoding="utf-8")

            self.assertIn("Cadence check enabled: yes", summary_text)
            self.assertIn("## Cadence Check", summary_text)
            self.assertIn("`overdue`: 1", summary_text)
            self.assertIn("`PubMed`: next due 2026-01-31", summary_text)


if __name__ == "__main__":
    unittest.main()

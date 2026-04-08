import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

MODULE_PATH = Path(__file__).resolve().parents[1] / "screening_metrics.py"
spec = importlib.util.spec_from_file_location("screening_metrics", MODULE_PATH)
screening_metrics = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = screening_metrics
assert spec.loader is not None
spec.loader.exec_module(screening_metrics)


class ScreeningMetricsTests(unittest.TestCase):
    def test_consensus_stats_from_results_computes_requested_metrics(self) -> None:
        consensus_df = pd.DataFrame(
            [
                {"record_id": "R001", "final_decision": "include", "conflict": "no"},
                {"record_id": "R002", "final_decision": "exclude", "conflict": "yes"},
                {"record_id": "R003", "final_decision": "exclude", "conflict": "no"},
            ]
        )

        stats = screening_metrics.consensus_stats_from_results(consensus_df)

        self.assertTrue(stats["available"])
        self.assertEqual(stats["records_screened"], 3)
        self.assertEqual(stats["records_excluded"], 2)
        self.assertEqual(stats["conflicts"], 1)
        self.assertEqual(stats["conflict_rate"], "33.3%")

    def test_main_writes_summary_with_screening_statistics_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            daily_log_path = tmp_path / "screening_daily_log.csv"
            agreement_path = tmp_path / "screening_title_abstract_dual_log.csv"
            consensus_path = tmp_path / "screening_title_abstract_results.csv"
            output_path = tmp_path / "screening_metrics_summary.md"
            stats_output_path = tmp_path / "screening_statistics.csv"

            pd.DataFrame(
                [
                    {
                        "date": "2026-03-15",
                        "reviewer": "EP",
                        "stage": "title_abstract",
                        "records_screened": "3",
                        "include_n": "1",
                        "exclude_n": "2",
                        "maybe_n": "0",
                        "pending_n": "0",
                        "time_spent_minutes": "30",
                    }
                ]
            ).to_csv(daily_log_path, index=False)

            pd.DataFrame(
                [
                    {
                        "record_id": "R001",
                        "reviewer": "EP",
                        "title_abstract_decision": "include",
                    }
                ]
            ).to_csv(agreement_path, index=False)

            pd.DataFrame(
                [
                    {"record_id": "R001", "final_decision": "include", "conflict": "no"},
                    {"record_id": "R002", "final_decision": "exclude", "conflict": "yes"},
                ]
            ).to_csv(consensus_path, index=False)

            original_argv = sys.argv[:]
            try:
                sys.argv = [
                    "screening_metrics.py",
                    "--input",
                    str(daily_log_path),
                    "--agreement-input",
                    str(agreement_path),
                    "--consensus-input",
                    str(consensus_path),
                    "--output",
                    str(output_path),
                    "--stats-output",
                    str(stats_output_path),
                ]
                screening_metrics.main()
            finally:
                sys.argv = original_argv

            summary = output_path.read_text(encoding="utf-8")
            self.assertIn("## Screening Statistics (Title/Abstract Consensus)", summary)
            self.assertIn("- records_screened: 2", summary)
            self.assertIn("- records_excluded: 1", summary)
            self.assertIn("- conflicts: 1", summary)
            self.assertIn("- conflict_rate: 50.0%", summary)

            stats_df = pd.read_csv(stats_output_path, dtype=str).fillna("")
            self.assertEqual(stats_df.iloc[0]["records_screened"], "2")
            self.assertEqual(stats_df.iloc[0]["records_excluded"], "1")
            self.assertEqual(stats_df.iloc[0]["conflicts"], "1")
            self.assertEqual(stats_df.iloc[0]["conflict_rate"], "50.0%")


if __name__ == "__main__":
    unittest.main()

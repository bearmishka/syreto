import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "consolidate_title_abstract_consensus.py"
spec = importlib.util.spec_from_file_location("consolidate_title_abstract_consensus", MODULE_PATH)
consolidate_title_abstract_consensus = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = consolidate_title_abstract_consensus
assert spec.loader is not None
spec.loader.exec_module(consolidate_title_abstract_consensus)


class ConsolidateTitleAbstractConsensusTests(unittest.TestCase):
    def test_build_consensus_sets_no_conflict_when_reviewers_agree(self) -> None:
        prepared = pd.DataFrame(
            [
                {
                    "record_id": "R001",
                    "reviewer": "EP",
                    "decision": "include",
                    "decision_date_parsed": pd.Timestamp("2026-03-15"),
                },
                {
                    "record_id": "R001",
                    "reviewer": "IR",
                    "decision": "include",
                    "decision_date_parsed": pd.Timestamp("2026-03-15"),
                },
            ]
        )

        results = consolidate_title_abstract_consensus.build_consensus_results(
            prepared,
            pd.DataFrame(),
            default_conflict_resolver="consensus_pending",
            default_resolution_decision="uncertain",
        )

        self.assertEqual(int(results.shape[0]), 1)
        row = results.iloc[0]
        self.assertEqual(row["conflict"], "no")
        self.assertEqual(row["conflict_resolver"], "")
        self.assertEqual(row["resolution_decision"], "")
        self.assertEqual(row["final_decision"], "include")

    def test_build_consensus_uses_default_resolution_for_new_conflicts(self) -> None:
        prepared = pd.DataFrame(
            [
                {
                    "record_id": "R002",
                    "reviewer": "EP",
                    "decision": "include",
                    "decision_date_parsed": pd.Timestamp("2026-03-15"),
                },
                {
                    "record_id": "R002",
                    "reviewer": "IR",
                    "decision": "exclude",
                    "decision_date_parsed": pd.Timestamp("2026-03-15"),
                },
            ]
        )

        results = consolidate_title_abstract_consensus.build_consensus_results(
            prepared,
            pd.DataFrame(),
            default_conflict_resolver="consensus_pending",
            default_resolution_decision="uncertain",
        )

        row = results.iloc[0]
        self.assertEqual(row["conflict"], "yes")
        self.assertEqual(row["conflict_resolver"], "consensus_pending")
        self.assertEqual(row["resolution_decision"], "uncertain")
        self.assertEqual(row["final_decision"], "uncertain")

    def test_build_consensus_preserves_existing_adjudication_fields(self) -> None:
        prepared = pd.DataFrame(
            [
                {
                    "record_id": "R003",
                    "reviewer": "EP",
                    "decision": "include",
                    "decision_date_parsed": pd.Timestamp("2026-03-15"),
                },
                {
                    "record_id": "R003",
                    "reviewer": "IR",
                    "decision": "exclude",
                    "decision_date_parsed": pd.Timestamp("2026-03-15"),
                },
            ]
        )
        existing = pd.DataFrame(
            [
                {
                    "record_id": "R003",
                    "reviewer1_decision": "include",
                    "reviewer2_decision": "exclude",
                    "conflict": "yes",
                    "conflict_resolver": "ADJ1",
                    "resolution_decision": "exclude",
                    "final_decision": "exclude",
                    "exclusion_reason": "No eligible outcome",
                }
            ]
        )

        results = consolidate_title_abstract_consensus.build_consensus_results(
            prepared,
            existing,
            default_conflict_resolver="consensus_pending",
            default_resolution_decision="uncertain",
        )

        row = results.iloc[0]
        self.assertEqual(row["conflict"], "yes")
        self.assertEqual(row["conflict_resolver"], "ADJ1")
        self.assertEqual(row["resolution_decision"], "exclude")
        self.assertEqual(row["final_decision"], "exclude")
        self.assertEqual(row["exclusion_reason"], "No eligible outcome")

    def test_main_writes_results_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            dual_log_path = tmp_path / "screening_title_abstract_dual_log.csv"
            results_output_path = tmp_path / "screening_title_abstract_results.csv"
            summary_path = tmp_path / "summary.md"

            pd.DataFrame(
                [
                    {
                        "record_id": "R100",
                        "reviewer": "EP",
                        "title_abstract_decision": "include",
                        "decision_date": "2026-03-15",
                    },
                    {
                        "record_id": "R100",
                        "reviewer": "IR",
                        "title_abstract_decision": "exclude",
                        "decision_date": "2026-03-15",
                    },
                ]
            ).to_csv(dual_log_path, index=False)

            exit_code = consolidate_title_abstract_consensus.main(
                [
                    "--dual-log",
                    str(dual_log_path),
                    "--results-output",
                    str(results_output_path),
                    "--summary",
                    str(summary_path),
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue(results_output_path.exists())
            self.assertTrue(summary_path.exists())

            results_df = pd.read_csv(results_output_path, dtype=str).fillna("")
            self.assertEqual(int(results_df.shape[0]), 1)
            self.assertEqual(results_df.iloc[0]["conflict_resolver"], "consensus_pending")
            self.assertIn("Conflicts detected", summary_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()

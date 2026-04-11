import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

MODULE_PATH = Path(__file__).resolve().parents[1] / "reviewer_workload_balancer.py"
spec = importlib.util.spec_from_file_location("reviewer_workload_balancer", MODULE_PATH)
reviewer_workload_balancer = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = reviewer_workload_balancer
assert spec.loader is not None
spec.loader.exec_module(reviewer_workload_balancer)


class ReviewerWorkloadBalancerTests(unittest.TestCase):
    def test_canonicalize_summary_text_collapses_exact_duplicate_block(self) -> None:
        block = (
            "# Reviewer Workload Balancer Summary\n\n"
            "## Scope\n\n"
            "- Screening log: `tmp.csv`\n"
            "- Stage filter: `title_abstract`\n"
            "- Output plan: `plan.csv`\n"
        )
        duplicated = block + block

        canonical = reviewer_workload_balancer.canonicalize_summary_text(duplicated)

        self.assertEqual(canonical, block)

    def test_single_reviewer_is_non_blocking_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            screening_log = tmp_path / "screening_daily_log.csv"
            plan_path = tmp_path / "reviewer_workload_plan.csv"
            summary_path = tmp_path / "reviewer_workload_balancer_summary.md"

            pd.DataFrame(
                [
                    {
                        "date": "2026-03-14",
                        "reviewer": "R1",
                        "stage": "title_abstract",
                        "records_screened": "12",
                    }
                ]
            ).to_csv(screening_log, index=False)

            exit_code = reviewer_workload_balancer.main(
                [
                    "--screening-log",
                    str(screening_log),
                    "--plan-output",
                    str(plan_path),
                    "--summary",
                    str(summary_path),
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue(plan_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertIn("non-blocking", summary_path.read_text(encoding="utf-8").lower())
            plan_provenance = json.loads(
                plan_path.with_name(f"{plan_path.name}.provenance.json").read_text(encoding="utf-8")
            )
            summary_provenance = json.loads(
                summary_path.with_name(f"{summary_path.name}.provenance.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(plan_provenance["generated_by"], "reviewer_workload_balancer.py")
            self.assertEqual(plan_provenance["upstream_inputs"], [str(screening_log)])
            self.assertEqual(summary_provenance["artifact_path"], str(summary_path))

    def test_strict_mode_fails_when_only_one_reviewer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            screening_log = tmp_path / "screening_daily_log.csv"

            pd.DataFrame(
                [
                    {
                        "date": "2026-03-14",
                        "reviewer": "R1",
                        "stage": "title_abstract",
                        "records_screened": "5",
                    }
                ]
            ).to_csv(screening_log, index=False)

            exit_code = reviewer_workload_balancer.main(
                [
                    "--screening-log",
                    str(screening_log),
                    "--fail-on-single-reviewer",
                ]
            )

            self.assertEqual(exit_code, 1)

    def test_two_reviewers_generate_rebalancing_suggestions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            screening_log = tmp_path / "screening_daily_log.csv"
            plan_path = tmp_path / "reviewer_workload_plan.csv"

            pd.DataFrame(
                [
                    {
                        "date": "2026-03-14",
                        "reviewer": "R1",
                        "stage": "title_abstract",
                        "records_screened": "10",
                    },
                    {
                        "date": "2026-03-14",
                        "reviewer": "R2",
                        "stage": "title_abstract",
                        "records_screened": "4",
                    },
                ]
            ).to_csv(screening_log, index=False)

            exit_code = reviewer_workload_balancer.main(
                [
                    "--screening-log",
                    str(screening_log),
                    "--plan-output",
                    str(plan_path),
                ]
            )

            self.assertEqual(exit_code, 0)
            plan_df = pd.read_csv(plan_path, dtype=str)
            by_reviewer = {row["reviewer"]: row for _, row in plan_df.iterrows()}
            self.assertEqual(by_reviewer["R1"]["suggested_next_batch"], "0")
            self.assertEqual(by_reviewer["R2"]["suggested_next_batch"], "3")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "quality_appraisal.py"


class QualityAppraisalAtomicOutputsTests(unittest.TestCase):
    def test_writes_expected_outputs_and_syncs_extraction(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            extraction_path = tmp_path / "extraction.csv"
            appraisal_input_path = tmp_path / "quality_appraisal_template.csv"
            scored_output_path = tmp_path / "quality_appraisal_scored.csv"
            summary_output_path = tmp_path / "quality_appraisal_summary.md"
            aggregate_output_path = tmp_path / "quality_appraisal_aggregate.csv"

            pd.DataFrame(
                [
                    {
                        "study_id": "S1",
                        "study_design": "cross_sectional",
                        "quality_appraisal": "",
                    }
                ]
            ).to_csv(extraction_path, index=False)

            scored_output_path.write_text("stale_scored\n", encoding="utf-8")
            summary_output_path.write_text("stale_summary\n", encoding="utf-8")

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--extraction",
                    str(extraction_path),
                    "--appraisal-input",
                    str(appraisal_input_path),
                    "--scored-output",
                    str(scored_output_path),
                    "--summary-output",
                    str(summary_output_path),
                    "--aggregate-output",
                    str(aggregate_output_path),
                    "--fail-on",
                    "none",
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(appraisal_input_path.exists())
            self.assertTrue(scored_output_path.exists())
            self.assertTrue(summary_output_path.exists())
            self.assertTrue(aggregate_output_path.exists())

            scored_df = pd.read_csv(scored_output_path, dtype=str)
            self.assertEqual(int(scored_df.shape[0]), 1)
            self.assertEqual(scored_df.iloc[0]["study_id"], "S1")
            self.assertIn(
                "study_id", scored_output_path.read_text(encoding="utf-8").splitlines()[0]
            )

            summary_text = summary_output_path.read_text(encoding="utf-8")
            self.assertIn("Quality Appraisal Summary", summary_text)
            self.assertNotIn("stale_summary", summary_text)

            synced_extraction_df = pd.read_csv(extraction_path, dtype=str)
            synced_value = str(synced_extraction_df.iloc[0]["quality_appraisal"])
            self.assertIn("jbi=", synced_value)


if __name__ == "__main__":
    unittest.main()

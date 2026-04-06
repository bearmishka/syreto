from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "quality_appraisal_roundtrip.py"


class QualityAppraisalRoundtripTests(unittest.TestCase):
    def test_roundtrip_pipeline_writes_expected_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            nos_input_path = tmp_path / "quality_appraisal_template_nos.csv"
            extraction_path = tmp_path / "extraction.csv"
            jbi_output_path = tmp_path / "quality_appraisal_template.csv"
            nos_to_jbi_summary_output_path = tmp_path / "nos_to_jbi_conversion_summary.md"
            scored_output_path = tmp_path / "quality_appraisal_scored.csv"
            quality_summary_output_path = tmp_path / "quality_appraisal_summary.md"
            aggregate_output_path = tmp_path / "quality_appraisal_aggregate.csv"
            roundtrip_nos_output_path = tmp_path / "quality_appraisal_template_roundtrip_nos.csv"
            nos_report_output_path = tmp_path / "quality_appraisal_roundtrip_nos_report.md"

            pd.DataFrame(
                [
                    {
                        "study_id": "S1",
                        "study_design": "cross_sectional",
                        "selection_bias": "serious",
                        "performance_bias": "moderate",
                        "detection_bias": "low",
                        "attrition_bias": "low",
                        "reporting_bias": "moderate",
                        "overall_risk": "serious",
                        "appraiser_id": "a1",
                        "checked_by": "a2",
                        "appraisal_notes": "seed note",
                    }
                ]
            ).to_csv(nos_input_path, index=False)

            pd.DataFrame(
                [
                    {
                        "study_id": "S1",
                        "study_design": "cross_sectional",
                        "quality_appraisal": "",
                    }
                ]
            ).to_csv(extraction_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--nos-input",
                    str(nos_input_path),
                    "--jbi-output",
                    str(jbi_output_path),
                    "--nos-to-jbi-summary-output",
                    str(nos_to_jbi_summary_output_path),
                    "--extraction",
                    str(extraction_path),
                    "--scored-output",
                    str(scored_output_path),
                    "--quality-summary-output",
                    str(quality_summary_output_path),
                    "--aggregate-output",
                    str(aggregate_output_path),
                    "--quality-fail-on",
                    "none",
                    "--roundtrip-nos-output",
                    str(roundtrip_nos_output_path),
                    "--nos-report-output",
                    str(nos_report_output_path),
                    "--no-sync-extraction",
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(jbi_output_path.exists())
            self.assertTrue(nos_to_jbi_summary_output_path.exists())
            self.assertTrue(scored_output_path.exists())
            self.assertTrue(quality_summary_output_path.exists())
            self.assertTrue(aggregate_output_path.exists())
            self.assertTrue(roundtrip_nos_output_path.exists())
            self.assertTrue(nos_report_output_path.exists())

            jbi_df = pd.read_csv(jbi_output_path, dtype=str)
            self.assertEqual(int(jbi_df.shape[0]), 1)
            self.assertEqual(jbi_df.iloc[0]["study_id"], "S1")
            self.assertEqual(jbi_df.iloc[0]["jbi_tool"], "jbi_analytical_cross_sectional")

            scored_df = pd.read_csv(scored_output_path, dtype=str)
            self.assertEqual(int(scored_df.shape[0]), 1)
            self.assertEqual(scored_df.iloc[0]["study_id"], "S1")

            roundtrip_nos_df = pd.read_csv(roundtrip_nos_output_path, dtype=str)
            self.assertEqual(int(roundtrip_nos_df.shape[0]), 1)
            self.assertEqual(roundtrip_nos_df.iloc[0]["study_id"], "S1")
            self.assertEqual(
                roundtrip_nos_df.iloc[0]["appraisal_framework"],
                "newcastle_ottawa_scale_adapted",
            )

            nos_to_jbi_summary_text = nos_to_jbi_summary_output_path.read_text(encoding="utf-8")
            self.assertIn("NOS to JBI Conversion Summary", nos_to_jbi_summary_text)

            quality_summary_text = quality_summary_output_path.read_text(encoding="utf-8")
            self.assertIn("Quality Appraisal Summary", quality_summary_text)

            nos_report_text = nos_report_output_path.read_text(encoding="utf-8")
            self.assertIn("JBI to NOS Conversion Summary", nos_report_text)


if __name__ == "__main__":
    unittest.main()
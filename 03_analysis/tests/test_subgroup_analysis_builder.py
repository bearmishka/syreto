from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "subgroup_analysis_builder.py"


class SubgroupAnalysisBuilderTests(unittest.TestCase):
    def test_generates_subgroup_rows_for_available_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            data_input_path = tmp_path / "publication_bias_data.csv"
            extraction_path = tmp_path / "extraction_template.csv"
            output_path = tmp_path / "subgroup_analysis.csv"
            summary_path = tmp_path / "subgroup_analysis_summary.md"

            pd.DataFrame(
                [
                    {"study_id": "S1", "effect_analysis": "0.20", "se": "0.10"},
                    {"study_id": "S2", "effect_analysis": "0.30", "se": "0.10"},
                    {"study_id": "S3", "effect_analysis": "0.40", "se": "0.12"},
                ]
            ).to_csv(data_input_path, index=False)

            pd.DataFrame(
                [
                    {
                        "study_id": "S1",
                        "country": "USA",
                        "study_design": "cohort",
                        "setting": "outpatient",
                        "follow_up": "6_months",
                    },
                    {
                        "study_id": "S2",
                        "country": "USA",
                        "study_design": "cohort",
                        "setting": "inpatient",
                        "follow_up": "12_months",
                    },
                    {
                        "study_id": "S3",
                        "country": "UK",
                        "study_design": "case-control",
                        "setting": "outpatient",
                        "follow_up": "12_months",
                    },
                ]
            ).to_csv(extraction_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--data-input",
                    str(data_input_path),
                    "--extraction-input",
                    str(extraction_path),
                    "--metric",
                    "converted_d",
                    "--output",
                    str(output_path),
                    "--summary-output",
                    str(summary_path),
                    "--fail-on",
                    "none",
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(output_path.exists())
            self.assertTrue(summary_path.exists())

            output_df = pd.read_csv(output_path, dtype=str)
            self.assertEqual(
                list(output_df.columns), ["subgroup", "k", "effect", "ci_low", "ci_high", "i2"]
            )
            self.assertGreaterEqual(int(output_df.shape[0]), 6)

            subgroup_labels = output_df["subgroup"].tolist()
            self.assertIn("region: USA", subgroup_labels)
            self.assertIn("study_design: cohort", subgroup_labels)
            self.assertIn("population_type: outpatient", subgroup_labels)
            self.assertIn("followup_duration: 12_months", subgroup_labels)

    def test_marks_missing_subgroup_columns_as_not_available(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            data_input_path = tmp_path / "publication_bias_data.csv"
            extraction_path = tmp_path / "extraction_template.csv"
            output_path = tmp_path / "subgroup_analysis.csv"
            summary_path = tmp_path / "subgroup_analysis_summary.md"

            pd.DataFrame(
                [
                    {"study_id": "S1", "effect_analysis": "0.20", "se": "0.10"},
                    {"study_id": "S2", "effect_analysis": "0.30", "se": "0.10"},
                ]
            ).to_csv(data_input_path, index=False)

            pd.DataFrame(
                [
                    {"study_id": "S1", "country": "USA", "study_design": "cohort"},
                    {"study_id": "S2", "country": "UK", "study_design": "case-control"},
                ]
            ).to_csv(extraction_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--data-input",
                    str(data_input_path),
                    "--extraction-input",
                    str(extraction_path),
                    "--output",
                    str(output_path),
                    "--summary-output",
                    str(summary_path),
                    "--fail-on",
                    "none",
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)

            output_df = pd.read_csv(output_path, dtype=str)
            self.assertIn("population_type: not_available", output_df["subgroup"].tolist())
            self.assertIn("followup_duration: not_available", output_df["subgroup"].tolist())

            summary_text = summary_path.read_text(encoding="utf-8")
            self.assertIn("Warnings", summary_text)


if __name__ == "__main__":
    unittest.main()

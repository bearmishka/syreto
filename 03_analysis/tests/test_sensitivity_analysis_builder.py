from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "sensitivity_analysis_builder.py"


class SensitivityAnalysisBuilderTests(unittest.TestCase):
    def test_generates_expected_analysis_rows_and_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            converted_path = tmp_path / "effect_size_converted.csv"
            extraction_path = tmp_path / "extraction_template.csv"
            quality_path = tmp_path / "quality_appraisal_scored.csv"
            output_path = tmp_path / "sensitivity_analysis.csv"
            summary_path = tmp_path / "sensitivity_analysis_summary.md"

            pd.DataFrame(
                [
                    {
                        "row": "2",
                        "study_id": "S1",
                        "first_author": "A",
                        "year": "2020",
                        "source_metric_canonical": "d",
                        "effect_direction": "positive",
                        "conversion_status": "converted",
                        "converted_d": "0.20",
                    },
                    {
                        "row": "3",
                        "study_id": "S2",
                        "first_author": "B",
                        "year": "2021",
                        "source_metric_canonical": "d",
                        "effect_direction": "positive",
                        "conversion_status": "converted",
                        "converted_d": "0.30",
                    },
                    {
                        "row": "4",
                        "study_id": "S3",
                        "first_author": "C",
                        "year": "2022",
                        "source_metric_canonical": "d",
                        "effect_direction": "positive",
                        "conversion_status": "converted",
                        "converted_d": "0.40",
                    },
                ]
            ).to_csv(converted_path, index=False)

            pd.DataFrame(
                [
                    {
                        "study_id": "S1",
                        "outcome_construct": "confidence",
                        "ci_lower": "0.00",
                        "ci_upper": "0.40",
                        "sample_size": "120",
                        "effect_direction": "positive",
                    },
                    {
                        "study_id": "S2",
                        "outcome_construct": "confidence",
                        "ci_lower": "0.10",
                        "ci_upper": "0.50",
                        "sample_size": "120",
                        "effect_direction": "positive",
                    },
                    {
                        "study_id": "S3",
                        "outcome_construct": "confidence",
                        "ci_lower": "0.20",
                        "ci_upper": "0.60",
                        "sample_size": "120",
                        "effect_direction": "positive",
                    },
                ]
            ).to_csv(extraction_path, index=False)

            pd.DataFrame(
                [
                    {"study_id": "S1", "quality_band": "high"},
                    {"study_id": "S2", "quality_band": "high"},
                    {"study_id": "S3", "quality_band": "low"},
                ]
            ).to_csv(quality_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--converted-input",
                    str(converted_path),
                    "--extraction-input",
                    str(extraction_path),
                    "--quality-input",
                    str(quality_path),
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
                list(output_df.columns),
                ["analysis", "included_studies", "effect", "ci_low", "ci_high", "notes"],
            )
            self.assertEqual(int(output_df.shape[0]), 3)
            self.assertEqual(
                sorted(output_df["analysis"].tolist()),
                ["high_quality_only", "leave-one-out", "random_effects_only"],
            )

            random_row = output_df[output_df["analysis"] == "random_effects_only"].iloc[0]
            self.assertEqual(random_row["included_studies"], "3")
            self.assertNotEqual(random_row["effect"], "")

            high_quality_row = output_df[output_df["analysis"] == "high_quality_only"].iloc[0]
            self.assertEqual(high_quality_row["included_studies"], "2")
            self.assertNotEqual(high_quality_row["effect"], "")

            leave_one_out_row = output_df[output_df["analysis"] == "leave-one-out"].iloc[0]
            self.assertEqual(leave_one_out_row["included_studies"], "3")
            self.assertIn("leave-one-out", leave_one_out_row["notes"].lower())

            summary_text = summary_path.read_text(encoding="utf-8")
            self.assertIn("Sensitivity Analysis Summary", summary_text)

    def test_high_quality_only_handles_empty_quality_subset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            converted_path = tmp_path / "effect_size_converted.csv"
            extraction_path = tmp_path / "extraction_template.csv"
            quality_path = tmp_path / "quality_appraisal_scored.csv"
            output_path = tmp_path / "sensitivity_analysis.csv"
            summary_path = tmp_path / "sensitivity_analysis_summary.md"

            pd.DataFrame(
                [
                    {
                        "row": "2",
                        "study_id": "S1",
                        "source_metric_canonical": "d",
                        "effect_direction": "positive",
                        "conversion_status": "converted",
                        "converted_d": "0.20",
                    },
                    {
                        "row": "3",
                        "study_id": "S2",
                        "source_metric_canonical": "d",
                        "effect_direction": "positive",
                        "conversion_status": "converted",
                        "converted_d": "0.30",
                    },
                ]
            ).to_csv(converted_path, index=False)

            pd.DataFrame(
                [
                    {
                        "study_id": "S1",
                        "ci_lower": "0.00",
                        "ci_upper": "0.40",
                        "sample_size": "120",
                        "effect_direction": "positive",
                    },
                    {
                        "study_id": "S2",
                        "ci_lower": "0.10",
                        "ci_upper": "0.50",
                        "sample_size": "120",
                        "effect_direction": "positive",
                    },
                ]
            ).to_csv(extraction_path, index=False)

            pd.DataFrame(
                [
                    {"study_id": "S1", "quality_band": "low"},
                    {"study_id": "S2", "quality_band": "low"},
                ]
            ).to_csv(quality_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--converted-input",
                    str(converted_path),
                    "--extraction-input",
                    str(extraction_path),
                    "--quality-input",
                    str(quality_path),
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
            high_quality_row = output_df[output_df["analysis"] == "high_quality_only"].iloc[0]
            self.assertEqual(high_quality_row["included_studies"], "0")
            self.assertTrue(pd.isna(high_quality_row["effect"]) or high_quality_row["effect"] == "")
            self.assertIn("high-quality", high_quality_row["notes"].lower())


if __name__ == "__main__":
    unittest.main()

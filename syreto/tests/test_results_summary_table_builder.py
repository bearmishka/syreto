from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "results_summary_table_builder.py"


class ResultsSummaryTableBuilderTests(unittest.TestCase):
    def test_builds_deduped_rows_with_certainty_and_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            meta_path = tmp_path / "meta_analysis_results.csv"
            extraction_path = tmp_path / "extraction_template.csv"
            grade_path = tmp_path / "grade_evidence_profile.csv"
            output_path = tmp_path / "results_summary_table.csv"
            latex_path = tmp_path / "results_summary_table.tex"
            summary_path = tmp_path / "results_summary_table_summary.md"

            pd.DataFrame(
                [
                    {
                        "outcome": "confidence",
                        "k_studies": "2",
                        "pooled_effect": "0.301",
                        "ci_low": "0.10",
                        "ci_high": "0.50",
                    },
                    {
                        "outcome": "confidence",
                        "k_studies": "99",
                        "pooled_effect": "9.99",
                        "ci_low": "9.0",
                        "ci_high": "10.0",
                    },
                    {
                        "outcome": "identity",
                        "k_studies": "1",
                        "pooled_effect": "-0.42",
                        "ci_low": "-0.70",
                        "ci_high": "-0.14",
                    },
                ]
            ).to_csv(meta_path, index=False)

            pd.DataFrame(
                [
                    {"study_id": "S1", "outcome_construct": "confidence", "sample_size": "120"},
                    {"study_id": "S2", "outcome_construct": "confidence", "sample_size": "80"},
                    {"study_id": "S3", "outcome_construct": "identity", "sample_size": "55"},
                ]
            ).to_csv(extraction_path, index=False)

            pd.DataFrame(
                [
                    {"outcome_construct": "confidence", "overall_certainty": "moderate"},
                    {"outcome_construct": "confidence", "overall_certainty": "low"},
                    {"outcome_construct": "identity", "overall_certainty": "very low"},
                ]
            ).to_csv(grade_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--meta-input",
                    str(meta_path),
                    "--extraction-input",
                    str(extraction_path),
                    "--grade-input",
                    str(grade_path),
                    "--output",
                    str(output_path),
                    "--latex-output",
                    str(latex_path),
                    "--summary-output",
                    str(summary_path),
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(output_path.exists())
            self.assertTrue(latex_path.exists())
            self.assertTrue(summary_path.exists())

            output_df = pd.read_csv(output_path, dtype=str)
            self.assertEqual(
                list(output_df.columns),
                ["outcome", "studies", "participants", "effect", "ci", "certainty_grade"],
            )
            self.assertEqual(int(output_df.shape[0]), 2)

            confidence_row = output_df[output_df["outcome"] == "confidence"].iloc[0]
            self.assertEqual(confidence_row["studies"], "2")
            self.assertEqual(confidence_row["participants"], "200")
            self.assertEqual(confidence_row["effect"], "d=0.30")
            self.assertEqual(confidence_row["ci"], "[0.10, 0.50]")
            self.assertEqual(confidence_row["certainty_grade"], "low")

            identity_row = output_df[output_df["outcome"] == "identity"].iloc[0]
            self.assertEqual(identity_row["certainty_grade"], "very low")

            summary_text = summary_path.read_text(encoding="utf-8")
            self.assertIn("Duplicate outcome in meta results ignored", summary_text)

            latex_text = latex_path.read_text(encoding="utf-8")
            self.assertIn(r"\label{tab:final_results_summary}", latex_text)
            self.assertIn("confidence", latex_text)
            self.assertIn("very low", latex_text)

    def test_fallbacks_when_meta_input_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            missing_meta_path = tmp_path / "missing_meta_analysis_results.csv"
            extraction_path = tmp_path / "extraction_template.csv"
            grade_path = tmp_path / "grade_evidence_profile.csv"
            output_path = tmp_path / "results_summary_table.csv"
            latex_path = tmp_path / "results_summary_table.tex"
            summary_path = tmp_path / "results_summary_table_summary.md"

            pd.DataFrame(
                [
                    {"study_id": "S1", "outcome_construct": "confidence", "sample_size": "40"},
                    {"study_id": "S2", "outcome_construct": "confidence", "sample_size": "60"},
                ]
            ).to_csv(extraction_path, index=False)

            pd.DataFrame(
                [
                    {"outcome_construct": "confidence", "overall_certainty": "moderate"},
                ]
            ).to_csv(grade_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--meta-input",
                    str(missing_meta_path),
                    "--extraction-input",
                    str(extraction_path),
                    "--grade-input",
                    str(grade_path),
                    "--output",
                    str(output_path),
                    "--latex-output",
                    str(latex_path),
                    "--summary-output",
                    str(summary_path),
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(latex_path.exists())

            output_df = pd.read_csv(output_path, dtype=str)
            self.assertEqual(int(output_df.shape[0]), 1)
            row = output_df.iloc[0]
            self.assertEqual(row["outcome"], "confidence")
            self.assertEqual(row["studies"], "2")
            self.assertEqual(row["participants"], "100")
            self.assertEqual(row["effect"], "NR")
            self.assertEqual(row["ci"], "NR")
            self.assertEqual(row["certainty_grade"], "moderate")

            summary_text = summary_path.read_text(encoding="utf-8")
            self.assertIn("Meta-analysis input is missing or empty", summary_text)
            self.assertIn("Manuscript table output", summary_text)


if __name__ == "__main__":
    unittest.main()

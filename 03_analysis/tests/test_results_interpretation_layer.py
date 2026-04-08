from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "results_interpretation_layer.py"


class ResultsInterpretationLayerTests(unittest.TestCase):
    def test_generates_interpretive_narrative_with_heterogeneity_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            meta_path = tmp_path / "meta_analysis_results.csv"
            results_path = tmp_path / "results_summary_table.csv"
            publication_bias_path = tmp_path / "publication_bias_results.csv"
            markdown_output = tmp_path / "results_interpretation_layer.md"
            tex_output = tmp_path / "03c_interpretation_auto.tex"

            pd.DataFrame(
                [
                    {
                        "outcome": "Outcome A",
                        "k_studies": "4",
                        "pooled_effect": "0.55",
                        "ci_low": "0.20",
                        "ci_high": "0.90",
                        "p_value": "0.01",
                        "i2": "67.0",
                        "tau2": "0.05",
                        "model": "random_effects",
                    },
                    {
                        "outcome": "Outcome B",
                        "k_studies": "1",
                        "pooled_effect": "-0.18",
                        "ci_low": "-0.30",
                        "ci_high": "-0.05",
                        "p_value": "0.04",
                        "i2": "0.0",
                        "tau2": "0.0",
                        "model": "random_effects",
                    },
                ]
            ).to_csv(meta_path, index=False)

            pd.DataFrame(
                [
                    {
                        "outcome": "Outcome A",
                        "studies": "4",
                        "participants": "320",
                        "effect": "d=0.55",
                        "ci": "[0.20, 0.90]",
                        "certainty_grade": "low",
                    },
                    {
                        "outcome": "Outcome B",
                        "studies": "1",
                        "participants": "70",
                        "effect": "d=-0.18",
                        "ci": "[-0.30, -0.05]",
                        "certainty_grade": "very low",
                    },
                ]
            ).to_csv(results_path, index=False)

            pd.DataFrame(
                [
                    {
                        "outcome": "Outcome A",
                        "k_studies": "4",
                        "n_with_se": "4",
                        "egger_test_p": "0.20",
                        "begg_test_p": "0.30",
                        "funnel_asymmetry": "no_significant_asymmetry",
                    },
                    {
                        "outcome": "Outcome B",
                        "k_studies": "1",
                        "n_with_se": "1",
                        "egger_test_p": "",
                        "begg_test_p": "",
                        "funnel_asymmetry": "not_assessed",
                    },
                ]
            ).to_csv(publication_bias_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--meta-input",
                    str(meta_path),
                    "--results-summary-input",
                    str(results_path),
                    "--publication-bias-input",
                    str(publication_bias_path),
                    "--markdown-output",
                    str(markdown_output),
                    "--tex-output",
                    str(tex_output),
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(markdown_output.exists())
            self.assertTrue(tex_output.exists())

            markdown_text = markdown_output.read_text(encoding="utf-8")
            self.assertIn("moderate increase", markdown_text)
            self.assertIn("Heterogeneity remained substantial (I² = 67.0%)", markdown_text)
            self.assertIn("single study", markdown_text)
            self.assertIn("Certainty of evidence was low", markdown_text)

            tex_text = tex_output.read_text(encoding="utf-8")
            self.assertIn("\\paragraph{Interpretation layer}", tex_text)
            self.assertRegex(tex_text, r"I\$\^2\$ = 67\.0\\?%")

    def test_falls_back_to_results_summary_when_meta_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            meta_path = tmp_path / "meta_analysis_results.csv"
            results_path = tmp_path / "results_summary_table.csv"
            publication_bias_path = tmp_path / "publication_bias_results.csv"
            markdown_output = tmp_path / "results_interpretation_layer.md"
            tex_output = tmp_path / "03c_interpretation_auto.tex"

            pd.DataFrame(
                columns=["outcome", "k_studies", "pooled_effect", "ci_low", "ci_high", "i2"]
            ).to_csv(
                meta_path,
                index=False,
            )
            pd.DataFrame(
                [
                    {
                        "outcome": "Outcome C",
                        "studies": "2",
                        "participants": "120",
                        "effect": "d=0.34",
                        "ci": "[0.10, 0.58]",
                        "certainty_grade": "moderate",
                    }
                ]
            ).to_csv(results_path, index=False)
            pd.DataFrame(columns=["outcome", "funnel_asymmetry"]).to_csv(
                publication_bias_path, index=False
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--meta-input",
                    str(meta_path),
                    "--results-summary-input",
                    str(results_path),
                    "--publication-bias-input",
                    str(publication_bias_path),
                    "--markdown-output",
                    str(markdown_output),
                    "--tex-output",
                    str(tex_output),
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)

            markdown_text = markdown_output.read_text(encoding="utf-8")
            self.assertIn("Outcome C", markdown_text)
            self.assertIn("small-to-moderate increase", markdown_text)


if __name__ == "__main__":
    unittest.main()

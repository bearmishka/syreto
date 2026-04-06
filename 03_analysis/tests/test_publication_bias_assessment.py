import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest

import numpy as np
import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "publication_bias_assessment.py"
spec = importlib.util.spec_from_file_location("publication_bias_assessment", MODULE_PATH)
publication_bias_assessment = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = publication_bias_assessment
assert spec.loader is not None
spec.loader.exec_module(publication_bias_assessment)


class PublicationBiasAssessmentTests(unittest.TestCase):
    def test_prepare_bias_data_builds_standard_errors_from_ci_and_sample(self) -> None:
        converted_df = pd.DataFrame(
            [
                {
                    "row": "2",
                    "study_id": "S1",
                    "first_author": "A",
                    "year": "2020",
                    "source_metric_canonical": "d",
                    "effect_direction": "positive",
                    "conversion_status": "converted",
                    "converted_d": "0.40",
                },
                {
                    "row": "3",
                    "study_id": "S2",
                    "first_author": "B",
                    "year": "2021",
                    "source_metric_canonical": "r",
                    "effect_direction": "negative",
                    "conversion_status": "converted",
                    "converted_d": "-0.22",
                },
            ]
        )

        extraction_df = pd.DataFrame(
            [
                {
                    "ci_lower": "0.10",
                    "ci_upper": "0.70",
                    "sample_size": "120",
                    "effect_direction": "positive",
                },
                {
                    "ci_lower": "",
                    "ci_upper": "",
                    "sample_size": "90",
                    "effect_direction": "negative",
                },
            ]
        )

        data_df, stats_dict = publication_bias_assessment.prepare_bias_data(
            converted_df,
            extraction_df,
            metric="converted_d",
            max_studies=0,
        )

        self.assertEqual(data_df.shape[0], 2)
        self.assertEqual(stats_dict["with_raw_ci"], 1)
        self.assertEqual(stats_dict["with_approx_ci"], 1)
        self.assertEqual(stats_dict["with_se"], 2)
        self.assertTrue(data_df["se"].notna().all())

    def test_egger_regression_requires_at_least_three_studies(self) -> None:
        data_df = pd.DataFrame(
            [
                {"effect_analysis": 0.20, "se": 0.10},
                {"effect_analysis": 0.10, "se": 0.12},
            ]
        )
        result = publication_bias_assessment.egger_regression(data_df, min_studies=10)

        self.assertEqual(result["status"], "not_computed")
        self.assertIn("requires_at_least_3", str(result["reason"]))

    def test_egger_regression_computes_regression_outputs(self) -> None:
        data_df = pd.DataFrame(
            [
                {"effect_analysis": 0.35, "se": 0.08},
                {"effect_analysis": 0.26, "se": 0.09},
                {"effect_analysis": 0.18, "se": 0.10},
                {"effect_analysis": 0.12, "se": 0.11},
                {"effect_analysis": 0.05, "se": 0.12},
            ]
        )
        result = publication_bias_assessment.egger_regression(data_df, min_studies=10)

        self.assertEqual(result["status"], "computed")
        self.assertEqual(result["n_studies"], 5)
        self.assertTrue(np.isfinite(float(result["intercept"])))
        self.assertTrue(np.isfinite(float(result["p_value"])))

    def test_begg_test_computes_rank_correlation_outputs(self) -> None:
        data_df = pd.DataFrame(
            [
                {"effect_analysis": 0.35, "se": 0.08},
                {"effect_analysis": 0.26, "se": 0.09},
                {"effect_analysis": 0.18, "se": 0.10},
                {"effect_analysis": 0.12, "se": 0.11},
                {"effect_analysis": 0.05, "se": 0.12},
            ]
        )
        result = publication_bias_assessment.begg_test(data_df, min_studies=10)

        self.assertEqual(result["status"], "computed")
        self.assertEqual(result["n_studies"], 5)
        self.assertTrue(np.isfinite(float(result["tau"])))
        self.assertTrue(np.isfinite(float(result["p_value"])))

    def test_publication_bias_results_by_outcome_contains_required_columns(self) -> None:
        converted_df = pd.DataFrame(
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
                    "converted_d": "0.25",
                },
                {
                    "row": "4",
                    "study_id": "S3",
                    "first_author": "C",
                    "year": "2022",
                    "source_metric_canonical": "d",
                    "effect_direction": "positive",
                    "conversion_status": "converted",
                    "converted_d": "0.30",
                },
            ]
        )

        extraction_df = pd.DataFrame(
            [
                {
                    "outcome_construct": "confidence",
                    "ci_lower": "0.00",
                    "ci_upper": "0.40",
                    "sample_size": "120",
                    "effect_direction": "positive",
                },
                {
                    "outcome_construct": "confidence",
                    "ci_lower": "0.05",
                    "ci_upper": "0.45",
                    "sample_size": "120",
                    "effect_direction": "positive",
                },
                {
                    "outcome_construct": "confidence",
                    "ci_lower": "0.10",
                    "ci_upper": "0.50",
                    "sample_size": "120",
                    "effect_direction": "positive",
                },
            ]
        )

        data_df, _ = publication_bias_assessment.prepare_bias_data(
            converted_df,
            extraction_df,
            metric="converted_d",
            max_studies=0,
        )

        results_df = publication_bias_assessment.publication_bias_results_by_outcome(
            data_df,
            min_studies_egger=3,
            min_studies_begg=3,
        )

        self.assertEqual(list(results_df.columns), publication_bias_assessment.PUBLICATION_BIAS_RESULTS_COLUMNS)
        self.assertEqual(int(results_df.shape[0]), 1)
        self.assertEqual(results_df.iloc[0]["outcome"], "confidence")
        self.assertEqual(int(results_df.iloc[0]["k_studies"]), 3)
        self.assertEqual(int(results_df.iloc[0]["n_with_se"]), 3)
        self.assertIn(
            results_df.iloc[0]["funnel_asymmetry"],
            {
                "possible_asymmetry",
                "possible_asymmetry_low_power",
                "no_significant_asymmetry",
                "no_significant_asymmetry_low_power",
                "not_assessed",
            },
        )

    def test_render_latex_table_writes_expected_label(self) -> None:
        egger_result = {
            "status": "computed",
            "n_studies": 12,
            "min_studies": 10,
            "intercept": 0.18,
            "slope": 0.90,
            "se_intercept": 0.07,
            "t_stat": 2.50,
            "p_value": 0.031,
            "df": 10,
            "ci_lower": 0.02,
            "ci_upper": 0.34,
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "publication_bias_assessment_table.tex"
            publication_bias_assessment.render_latex_table(
                metric="converted_d",
                egger_result=egger_result,
                output_path=output_path,
            )
            text = output_path.read_text(encoding="utf-8")

        self.assertIn("tab:publication_bias_assessment", text)
        self.assertIn("Egger intercept", text)


if __name__ == "__main__":
    unittest.main()
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

MODULE_PATH = Path(__file__).resolve().parents[1] / "forest_plot_generator.py"
spec = importlib.util.spec_from_file_location("forest_plot_generator", MODULE_PATH)
forest_plot_generator = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = forest_plot_generator
assert spec.loader is not None
spec.loader.exec_module(forest_plot_generator)


class ForestPlotGeneratorTests(unittest.TestCase):
    def test_extraction_template_contains_required_metadata_columns(self) -> None:
        extraction_template_path = (
            Path(__file__).resolve().parents[2] / "02_data" / "codebook" / "extraction_template.csv"
        )
        extraction_header = pd.read_csv(extraction_template_path, nrows=0).columns.tolist()

        missing = [
            column
            for column in forest_plot_generator.EXTRACTION_METADATA_COLUMNS
            if column not in extraction_header
        ]
        self.assertEqual(missing, [])

    def test_convert_source_value_for_or_to_d(self) -> None:
        converted = forest_plot_generator.convert_source_value(
            source_metric="or",
            source_value=2.0,
            direction_sign=None,
            target_metric="converted_d",
        )
        self.assertIsNotNone(converted)
        self.assertGreater(float(converted), 0.0)

    def test_missing_required_columns_reports_schema_gap(self) -> None:
        dataframe = pd.DataFrame(columns=["ci_lower", "sample_size"])
        missing = forest_plot_generator.missing_required_columns(
            dataframe,
            forest_plot_generator.EXTRACTION_METADATA_COLUMNS,
        )
        self.assertEqual(missing, ["ci_upper", "effect_direction"])

    def test_approximate_ci_from_sample_size(self) -> None:
        ci = forest_plot_generator.approximate_ci("converted_d", effect_value=0.3, sample_size=100)
        self.assertIsNotNone(ci)
        assert ci is not None
        self.assertLess(ci[0], 0.3)
        self.assertGreater(ci[1], 0.3)

    def test_prepare_plot_data_builds_rows_and_ci_sources(self) -> None:
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
                    "converted_d": "0.45",
                },
                {
                    "row": "3",
                    "study_id": "S2",
                    "first_author": "B",
                    "year": "2021",
                    "source_metric_canonical": "r",
                    "effect_direction": "negative",
                    "conversion_status": "partial",
                    "converted_d": "-0.20",
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
                    "sample_size": "80",
                    "effect_direction": "negative",
                },
            ]
        )

        plot_df, stats = forest_plot_generator.prepare_plot_data(
            converted_df,
            extraction_df,
            metric="converted_d",
            max_studies=25,
        )

        self.assertEqual(plot_df.shape[0], 2)
        self.assertEqual(stats["eligible_rows"], 2)
        self.assertEqual(stats["with_raw_ci"], 1)
        self.assertEqual(stats["with_approx_ci"], 1)

    def test_harmonize_extraction_metadata_from_filled_schema(self) -> None:
        extraction_df = pd.DataFrame(
            [
                {
                    "study_id": "S1",
                    "author": "Ng",
                    "sample_size": "100",
                    "effect_value": "0.33",
                    "confidence_interval": "[0.10, 0.50]",
                },
                {
                    "study_id": "S2",
                    "author": "Lopez",
                    "sample_size": "80",
                    "effect_value": "-0.20",
                    "confidence_interval": "[-0.40, -0.05]",
                },
            ]
        )

        harmonized = forest_plot_generator.harmonize_extraction_metadata(extraction_df)

        self.assertEqual(harmonized.loc[0, "first_author"], "Ng")
        self.assertEqual(harmonized.loc[0, "main_effect_value"], "0.33")
        self.assertEqual(harmonized.loc[0, "ci_lower"], "0.1")
        self.assertEqual(harmonized.loc[0, "ci_upper"], "0.5")
        self.assertEqual(harmonized.loc[0, "effect_direction"], "positive")
        self.assertEqual(harmonized.loc[1, "effect_direction"], "negative")

    def test_build_converted_from_extraction_with_canonical_effect_columns(self) -> None:
        extraction_df = pd.DataFrame(
            [
                {
                    "study_id": "S2",
                    "first_author": "Kim",
                    "year": "2021",
                    "main_effect_metric": "r",
                    "main_effect_value": "0.40",
                }
            ]
        )

        harmonized = forest_plot_generator.harmonize_extraction_metadata(extraction_df)
        converted_df = forest_plot_generator.build_converted_from_extraction(
            harmonized,
            metric="converted_d",
        )

        self.assertEqual(converted_df.shape[0], 1)
        self.assertEqual(converted_df.loc[0, "study_id"], "S2")
        self.assertEqual(converted_df.loc[0, "first_author"], "Kim")
        self.assertGreater(float(converted_df.loc[0, "converted_d"]), 0.0)

    def test_extraction_unit_mismatch_issues_detect_invalid_r_scale(self) -> None:
        extraction_df = pd.DataFrame(
            [
                {
                    "study_id": "S3",
                    "main_effect_metric": "r",
                    "main_effect_value": "31",
                    "ci_lower": "-2",
                    "ci_upper": "0.5",
                }
            ]
        )

        issues = forest_plot_generator.extraction_unit_mismatch_issues(extraction_df)

        self.assertGreaterEqual(len(issues), 2)
        self.assertTrue(any("outside [-1, 1]" in issue for issue in issues))

    def test_prepare_plot_data_uses_study_id_metadata_fallback(self) -> None:
        converted_df = pd.DataFrame(
            [
                {
                    "row": "",
                    "study_id": "S1",
                    "first_author": "Ng",
                    "year": "2022",
                    "source_metric_canonical": "d",
                    "effect_direction": "",
                    "conversion_status": "converted",
                    "converted_d": "0.33",
                }
            ]
        )
        extraction_df = pd.DataFrame(
            [
                {
                    "study_id": "S1",
                    "author": "Ng",
                    "sample_size": "100",
                    "effect_value": "0.33",
                    "confidence_interval": "[0.10, 0.50]",
                }
            ]
        )

        harmonized = forest_plot_generator.harmonize_extraction_metadata(extraction_df)
        plot_df, stats = forest_plot_generator.prepare_plot_data(
            converted_df,
            harmonized,
            metric="converted_d",
            max_studies=25,
        )

        self.assertEqual(plot_df.shape[0], 1)
        self.assertEqual(stats["with_raw_ci"], 1)
        self.assertEqual(plot_df.loc[0, "ci_source"], "converted_raw_ci")
        self.assertAlmostEqual(float(plot_df.loc[0, "ci_lower"]), 0.10, places=6)
        self.assertAlmostEqual(float(plot_df.loc[0, "ci_upper"]), 0.50, places=6)

    def test_build_converted_from_extraction_generates_rows(self) -> None:
        extraction_df = pd.DataFrame(
            [
                {
                    "study_id": "S1",
                    "author": "Ng",
                    "year": "2022",
                    "effect_measure": "r",
                    "effect_value": "-0.50",
                }
            ]
        )
        harmonized = forest_plot_generator.harmonize_extraction_metadata(extraction_df)
        converted_df = forest_plot_generator.build_converted_from_extraction(
            harmonized,
            metric="converted_d",
        )

        self.assertEqual(converted_df.shape[0], 1)
        self.assertEqual(converted_df.loc[0, "study_id"], "S1")
        self.assertEqual(converted_df.loc[0, "first_author"], "Ng")
        self.assertEqual(converted_df.loc[0, "source_metric_canonical"], "r")
        self.assertEqual(converted_df.loc[0, "conversion_status"], "converted")
        self.assertLess(float(converted_df.loc[0, "converted_d"]), 0.0)

    def test_render_tikz_creates_tikzpicture(self) -> None:
        plot_df = pd.DataFrame(
            [
                {
                    "study_label": "A (2020) [S1]",
                    "effect": 0.2,
                    "ci_lower": -0.1,
                    "ci_upper": 0.5,
                    "ci_source": "converted_raw_ci",
                    "effect_text": "0.200 [-0.100, 0.500]",
                }
            ]
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "forest_plot.tikz"
            forest_plot_generator.render_tikz(
                plot_df=plot_df,
                metric="converted_d",
                output_path=output_path,
            )
            text = output_path.read_text(encoding="utf-8")

        self.assertIn("\\begin{tikzpicture}", text)
        self.assertIn("Forest plot", text)


if __name__ == "__main__":
    unittest.main()

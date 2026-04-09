import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

MODULE_PATH = Path(__file__).resolve().parents[1] / "review_descriptives_builder.py"
spec = importlib.util.spec_from_file_location("review_descriptives_builder", MODULE_PATH)
review_descriptives_builder = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = review_descriptives_builder
assert spec.loader is not None
spec.loader.exec_module(review_descriptives_builder)


class ReviewDescriptivesBuilderTests(unittest.TestCase):
    def test_build_descriptives_payload_counts_distributions(self) -> None:
        studies_df = pd.DataFrame(
            [
                {
                    "study_id": "S1",
                    "year": "2020",
                    "country": "USA",
                    "study_design": "cohort",
                    "sample_size": "120",
                    "predictor_construct": "attachment",
                    "outcome_construct": "distress",
                },
                {
                    "study_id": "S2",
                    "year": "2020",
                    "country": "USA",
                    "study_design": "cohort",
                    "sample_size": "80",
                    "predictor_construct": "attachment",
                    "outcome_construct": "functioning",
                },
                {
                    "study_id": "S3",
                    "year": "",
                    "country": "Canada",
                    "study_design": "trial",
                    "sample_size": "",
                    "predictor_construct": "identity",
                    "outcome_construct": "distress",
                },
            ]
        )

        payload = review_descriptives_builder.build_descriptives_payload(studies_df)

        self.assertEqual(payload["included_study_count"], 3)
        self.assertEqual(payload["missing_counts"]["year"], 1)
        self.assertEqual(payload["missing_counts"]["sample_size"], 1)
        self.assertEqual(payload["distributions"]["country"]["USA"], 2)
        self.assertEqual(payload["distributions"]["study_design"]["cohort"], 2)
        self.assertEqual(payload["sample_size_summary"]["studies_with_sample_size"], 2)
        self.assertEqual(payload["sample_size_summary"]["total_reported_participants"], 200)
        self.assertEqual(payload["predictor_outcome_pairs"][0]["count"], 1)
        self.assertEqual(payload["figure_outputs"], {})

    def test_render_figures_writes_png_outputs(self) -> None:
        studies_df = pd.DataFrame(
            [
                {"predictor_construct": "attachment", "outcome_construct": "distress"},
                {"predictor_construct": "attachment", "outcome_construct": "distress"},
                {"predictor_construct": "identity", "outcome_construct": "functioning"},
            ]
        )
        payload = {
            "distributions": {
                "year": {"2020": 2, "2021": 1},
                "study_design": {"cohort": 2, "trial": 1},
                "country": {"USA": 2, "Canada": 1},
                "quality_band": {"high": 1, "moderate": 2},
            },
            "figure_outputs": {},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            rendered = review_descriptives_builder.render_figures(
                payload,
                year_output=base / "year.png",
                study_design_output=base / "design.png",
                country_output=base / "country.png",
                quality_band_output=base / "quality.png",
                predictor_outcome_heatmap_output=base / "heatmap.png",
                studies_df=studies_df,
            )

            self.assertSetEqual(
                set(rendered.keys()),
                {"year", "study_design", "country", "quality_band", "predictor_outcome_heatmap"},
            )
            for figure_path in rendered.values():
                self.assertTrue(Path(figure_path).exists())

    def test_predictor_outcome_matrix_builds_cooccurrence_grid(self) -> None:
        studies_df = pd.DataFrame(
            [
                {"predictor_construct": "attachment", "outcome_construct": "distress"},
                {"predictor_construct": "attachment", "outcome_construct": "distress"},
                {"predictor_construct": "attachment", "outcome_construct": "functioning"},
                {"predictor_construct": "identity", "outcome_construct": "distress"},
            ]
        )

        predictors, outcomes, matrix = review_descriptives_builder.predictor_outcome_matrix(
            studies_df
        )

        self.assertEqual(predictors[:2], ["attachment", "identity"])
        self.assertEqual(outcomes[:2], ["distress", "functioning"])
        self.assertEqual(matrix[0][0], 2)
        self.assertEqual(matrix[0][1], 1)

    def test_quality_band_distribution_reads_scored_appraisal(self) -> None:
        quality_df = pd.DataFrame(
            [
                {"study_id": "S1", "quality_band": "high"},
                {"study_id": "S2", "quality_band": "moderate"},
                {"study_id": "S3", "quality_band": "moderate"},
            ]
        )

        distribution = review_descriptives_builder.quality_band_distribution(quality_df)
        self.assertEqual(distribution["moderate"], 2)
        self.assertEqual(distribution["high"], 1)

    def test_main_writes_json_markdown_and_figure_outputs(self) -> None:
        extraction_df = pd.DataFrame(
            [
                {
                    "study_id": "S1",
                    "author": "Smith",
                    "year": "2021",
                    "country": "USA",
                    "study_design": "cohort",
                    "sample_size": "150",
                    "object_relation_construct": "attachment",
                    "identity_construct": "distress",
                    "consensus_status": "include",
                }
            ]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            extraction_path = base / "extraction.csv"
            json_output = base / "review_descriptives.json"
            markdown_output = base / "review_descriptives.md"
            year_output = base / "figures" / "year.png"
            design_output = base / "figures" / "design.png"
            country_output = base / "figures" / "country.png"
            quality_output = base / "figures" / "quality.png"
            heatmap_output = base / "figures" / "heatmap.png"
            quality_input = base / "quality.csv"
            extraction_df.to_csv(extraction_path, index=False)
            pd.DataFrame([{"study_id": "S1", "quality_band": "high", "score_pct": "80.0"}]).to_csv(
                quality_input, index=False
            )

            argv = sys.argv[:]
            try:
                sys.argv = [
                    "review_descriptives_builder.py",
                    "--extraction-input",
                    str(extraction_path),
                    "--json-output",
                    str(json_output),
                    "--markdown-output",
                    str(markdown_output),
                    "--year-figure-output",
                    str(year_output),
                    "--study-design-figure-output",
                    str(design_output),
                    "--country-figure-output",
                    str(country_output),
                    "--quality-input",
                    str(quality_input),
                    "--quality-band-figure-output",
                    str(quality_output),
                    "--predictor-outcome-heatmap-output",
                    str(heatmap_output),
                ]
                review_descriptives_builder.main()
            finally:
                sys.argv = argv

            payload = json.loads(json_output.read_text(encoding="utf-8"))
            markdown = markdown_output.read_text(encoding="utf-8")

        self.assertEqual(payload["included_study_count"], 1)
        self.assertEqual(payload["distributions"]["predictor_construct"]["attachment"], 1)
        self.assertSetEqual(
            set(payload["figure_outputs"].keys()),
            {"year", "study_design", "country", "quality_band", "predictor_outcome_heatmap"},
        )
        self.assertIn("Included studies: 1", markdown)
        self.assertIn("attachment x distress: 1", markdown)
        self.assertIn("year:", markdown)


if __name__ == "__main__":
    unittest.main()

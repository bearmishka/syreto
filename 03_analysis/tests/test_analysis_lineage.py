from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "analysis_lineage.py"


class AnalysisLineageTests(unittest.TestCase):
    def test_builds_per_outcome_study_lineage_with_source_priority(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            forest_path = tmp_path / "forest_plot_data.csv"
            meta_path = tmp_path / "meta_analysis_results.csv"
            publication_bias_path = tmp_path / "publication_bias_data.csv"
            grade_path = tmp_path / "grade_evidence_profile.csv"
            extraction_path = tmp_path / "extraction_template.csv"
            output_path = tmp_path / "analysis_lineage.json"

            pd.DataFrame(
                [
                    {"study_id": "S1"},
                    {"study_id": "S3"},
                ]
            ).to_csv(forest_path, index=False)

            pd.DataFrame(
                [
                    {"outcome": "body_image"},
                    {"outcome": "self_esteem"},
                    {"outcome": "quality_of_life"},
                    {"outcome": "anxiety"},
                ]
            ).to_csv(meta_path, index=False)

            pd.DataFrame(
                [
                    {"study_id": "S1", "outcome": "body_image"},
                    {"study_id": "S2", "outcome": "body_image"},
                ]
            ).to_csv(publication_bias_path, index=False)

            pd.DataFrame(
                [
                    {"study_id": "S5", "outcome_construct": "quality_of_life"},
                ]
            ).to_csv(grade_path, index=False)

            pd.DataFrame(
                [
                    {"study_id": "S1", "outcome_construct": "body_image"},
                    {"study_id": "S2", "outcome_construct": "body_image"},
                    {"study_id": "S3", "outcome_construct": "self_esteem"},
                    {"study_id": "S4", "outcome_construct": "anxiety"},
                ]
            ).to_csv(extraction_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--forest-input",
                    str(forest_path),
                    "--meta-input",
                    str(meta_path),
                    "--publication-bias-input",
                    str(publication_bias_path),
                    "--grade-input",
                    str(grade_path),
                    "--extraction-input",
                    str(extraction_path),
                    "--output",
                    str(output_path),
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(output_path.exists())

            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertIn("generated_at_utc", payload)

            forest_records = {
                record["outcome"]: record["studies_used"]
                for record in payload["forest_plot"]["records"]
            }
            self.assertEqual(forest_records["body_image"], ["S1"])
            self.assertEqual(forest_records["self_esteem"], ["S3"])

            publication_bias_records = {
                record["outcome"]: record["studies_used"]
                for record in payload["publication_bias"]["records"]
            }
            self.assertEqual(publication_bias_records["body_image"], ["S1", "S2"])

            grade_records = {
                record["outcome"]: record["studies_used"]
                for record in payload["grade_profile"]["records"]
            }
            self.assertEqual(grade_records["quality_of_life"], ["S5"])

            meta_records = {
                record["outcome"]: record for record in payload["meta_analysis"]["records"]
            }
            self.assertEqual(meta_records["body_image"]["studies_used"], ["S1", "S2"])
            self.assertIn("publication_bias_data", meta_records["body_image"]["evidence_sources"])

            self.assertEqual(meta_records["self_esteem"]["studies_used"], ["S3"])
            self.assertIn("forest_plot_data", meta_records["self_esteem"]["evidence_sources"])

            self.assertEqual(meta_records["quality_of_life"]["studies_used"], ["S5"])
            self.assertIn(
                "grade_evidence_profile", meta_records["quality_of_life"]["evidence_sources"]
            )

            self.assertEqual(meta_records["anxiety"]["studies_used"], ["S4"])
            self.assertIn("extraction_template", meta_records["anxiety"]["evidence_sources"])


if __name__ == "__main__":
    unittest.main()

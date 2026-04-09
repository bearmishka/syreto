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

    def test_main_writes_json_and_markdown_outputs(self) -> None:
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
            extraction_df.to_csv(extraction_path, index=False)

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
                ]
                review_descriptives_builder.main()
            finally:
                sys.argv = argv

            payload = json.loads(json_output.read_text(encoding="utf-8"))
            markdown = markdown_output.read_text(encoding="utf-8")

        self.assertEqual(payload["included_study_count"], 1)
        self.assertEqual(payload["distributions"]["predictor_construct"]["attachment"], 1)
        self.assertIn("Included studies: 1", markdown)
        self.assertIn("attachment x distress: 1", markdown)


if __name__ == "__main__":
    unittest.main()

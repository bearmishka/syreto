import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

MODULE_PATH = Path(__file__).resolve().parents[1] / "study_table.py"
spec = importlib.util.spec_from_file_location("study_table", MODULE_PATH)
study_table = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = study_table
assert spec.loader is not None
spec.loader.exec_module(study_table)


class StudyTableTests(unittest.TestCase):
    def test_harmonize_study_columns_promotes_legacy_aliases(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "study_id": "S1",
                    "author": "Smith",
                    "object_relation_construct": "attachment",
                    "identity_construct": "distress",
                    "effect_metric": "r",
                    "effect_value": "0.41",
                }
            ]
        )

        harmonized = study_table.harmonize_study_columns(
            frame,
            [
                "study_id",
                "first_author",
                "predictor_construct",
                "outcome_construct",
                "main_effect_metric",
                "main_effect_value",
            ],
        )

        row = harmonized.iloc[0]
        self.assertEqual(row["first_author"], "Smith")
        self.assertEqual(row["predictor_construct"], "attachment")
        self.assertEqual(row["outcome_construct"], "distress")
        self.assertEqual(row["main_effect_metric"], "r")
        self.assertEqual(row["main_effect_value"], "0.41")

    def test_included_study_table_filters_and_deduplicates(self) -> None:
        frame = pd.DataFrame(
            [
                {"study_id": "S1", "consensus_status": "exclude", "study_design": "cohort"},
                {"study_id": "S1", "consensus_status": "include", "study_design": "trial"},
                {"study_id": " ", "consensus_status": "include", "study_design": "case report"},
                {"study_id": "S2", "consensus_status": "include", "study_design": "cohort"},
            ]
        )

        included = study_table.included_study_table(
            frame, ["study_id", "consensus_status", "study_design"]
        )

        self.assertEqual(list(included["study_id"]), ["S1", "S2"])
        self.assertEqual(
            included.loc[included["study_id"] == "S1", "study_design"].iloc[0], "trial"
        )

    def test_load_study_table_reads_and_sorts_canonical_columns(self) -> None:
        frame = pd.DataFrame(
            [
                {"study_id": "B", "author": "Brown", "year": "2021"},
                {"study_id": "A", "author": "Adams", "year": "2019"},
            ]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "extraction.csv"
            frame.to_csv(path, index=False)
            loaded = study_table.load_study_table(path, ["study_id", "first_author", "year"])
            sorted_df = study_table.sort_study_table(loaded)

        self.assertEqual(list(sorted_df["study_id"]), ["A", "B"])
        self.assertEqual(list(sorted_df["first_author"]), ["Adams", "Brown"])


if __name__ == "__main__":
    unittest.main()

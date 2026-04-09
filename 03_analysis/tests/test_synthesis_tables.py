import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "synthesis_tables.py"
spec = importlib.util.spec_from_file_location("synthesis_tables", MODULE_PATH)
synthesis_tables = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = synthesis_tables
assert spec.loader is not None
spec.loader.exec_module(synthesis_tables)


class SynthesisTablesTests(unittest.TestCase):
    def test_read_extraction_harmonizes_legacy_columns(self) -> None:
        csv_text = (
            "study_id,author,year,object_relation_construct,identity_measure\n"
            "S1,Smith,2020,attachment,Distress Scale\n"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "extraction.csv"
            path.write_text(csv_text, encoding="utf-8")
            extraction_df = synthesis_tables.read_extraction(path)

        row = extraction_df.iloc[0]
        self.assertEqual(row["first_author"], "Smith")
        self.assertEqual(row["predictor_construct"], "attachment")
        self.assertEqual(row["outcome_measure"], "Distress Scale")

    def test_sort_extraction_rows_uses_canonical_study_identity_order(self) -> None:
        extraction_df = synthesis_tables.pd.DataFrame(
            [
                {"study_id": "B", "first_author": "Brown", "year": "2021"},
                {"study_id": "A", "first_author": "Adams", "year": "2019"},
            ]
        )

        sorted_df = synthesis_tables.sort_extraction_rows(extraction_df)
        self.assertEqual(list(sorted_df["study_id"]), ["A", "B"])


if __name__ == "__main__":
    unittest.main()

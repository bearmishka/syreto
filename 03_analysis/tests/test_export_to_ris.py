from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

import pandas as pd

MODULE_PATH = Path(__file__).resolve().parents[1] / "export_to_ris.py"
spec = importlib.util.spec_from_file_location("export_to_ris", MODULE_PATH)
export_to_ris = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = export_to_ris
assert spec.loader is not None
spec.loader.exec_module(export_to_ris)


class ExportToRisTests(unittest.TestCase):
    def test_select_included_extraction_rows_uses_study_table_contract(self) -> None:
        extraction_df = pd.DataFrame(
            [
                {
                    "study_id": "S2",
                    "author": "Lopez",
                    "year": "2022",
                    "consensus_status": "exclude",
                },
                {
                    "study_id": "S1",
                    "author": "Ng",
                    "year": "2021",
                    "consensus_status": "include",
                },
            ]
        )

        included = export_to_ris.select_included_extraction_rows(extraction_df)
        sorted_rows = export_to_ris.sort_included_rows(included)

        self.assertEqual(list(sorted_rows["study_id"]), ["S1"])
        self.assertEqual(sorted_rows.iloc[0]["first_author"], "Ng")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from syreto.csv_schema import summarize_csv_schema_posture


class CsvSchemaPostureTests(unittest.TestCase):
    def test_posture_counts_missing_files_and_present_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            data_root = Path(tmp_dir)
            processed = data_root / "processed"
            processed.mkdir(parents=True, exist_ok=True)

            (processed / "search_log.csv").write_text(
                "\n".join(
                    [
                        "database,date_searched,query_version,start_year,end_date,filters_applied,results_total,results_exported,export_filename,notes",
                        "pubmed,2026-01-01,v1,2020,2026-01-01,,12,12,export.ris,",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            posture = summarize_csv_schema_posture(data_root)

            self.assertEqual(posture.checked_files, 1)
            self.assertGreaterEqual(posture.missing_files, 1)
            self.assertEqual(posture.error_count, 0)
            self.assertEqual(posture.warning_count, 0)
            self.assertTrue(
                any(detail["level"] == "missing" for detail in posture.details),
                msg=posture.details,
            )


if __name__ == "__main__":
    unittest.main()

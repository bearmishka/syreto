import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "retraction_checker.py"
spec = importlib.util.spec_from_file_location("retraction_checker", MODULE_PATH)
retraction_checker = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = retraction_checker
assert spec.loader is not None
spec.loader.exec_module(retraction_checker)


class RetractionCheckerTests(unittest.TestCase):
    def test_normalize_doi_handles_prefixes_and_trailing_punctuation(self) -> None:
        self.assertEqual(
            retraction_checker.normalize_doi("https://doi.org/10.1234/ABC.DEF."),
            "10.1234/abc.def",
        )
        self.assertEqual(
            retraction_checker.normalize_doi("doi:10.1000/xyz);"),
            "10.1000/xyz",
        )

    def test_parse_retraction_database_uses_original_paper_doi(self) -> None:
        payload = (
            "Record ID,Title,OriginalPaperDOI,RetractionDate,RetractionNature,URLS\n"
            "101,Study A,10.1111/alpha,2024-01-02,Fake data,https://example.org/a\n"
            "102,Study B,doi:10.2222/BETA,2025-03-04,Error,https://example.org/b\n"
        )

        index, metadata = retraction_checker.parse_retraction_database(payload)

        self.assertIn("10.1111/alpha", index)
        self.assertIn("10.2222/beta", index)
        self.assertEqual(int(metadata["rows_scanned"]), 2)
        self.assertEqual(int(metadata["rows_with_doi"]), 2)
        self.assertIn("OriginalPaperDOI", metadata["doi_columns"])

    def test_resolve_source_studies_uses_master_when_extraction_doi_missing(self) -> None:
        included_df = pd.DataFrame(
            [
                {
                    "study_id": "S001",
                    "first_author": "Smith",
                    "year": "2021",
                    "title": "Interpersonal predictors and outcomes",
                    "doi": "",
                    "record_id": "",
                    "source_record_id": "",
                }
            ]
        )
        master_df = pd.DataFrame(
            [
                {
                    "record_id": "MR_001",
                    "source_record_id": "SRC_001",
                    "title": "Interpersonal predictors and outcomes",
                    "authors": "Smith, J.; Doe, A.",
                    "year": "2021",
                    "doi": "10.5555/demo",
                    "is_duplicate": "no",
                }
            ]
        )

        indexes = retraction_checker.build_master_indexes(master_df)
        source_rows, counts = retraction_checker.resolve_source_studies(included_df, master_df, indexes)

        self.assertEqual(len(source_rows), 1)
        self.assertEqual(source_rows[0]["source_doi"], "10.5555/demo")
        self.assertEqual(source_rows[0]["doi_source"], "title_author_year")
        self.assertEqual(int(counts["title_author_year"]), 1)

    def test_build_result_rows_assigns_retracted_and_missing_statuses(self) -> None:
        source_rows = [
            {
                "study_id": "S001",
                "first_author": "Smith",
                "year": "2021",
                "source_doi": "10.5555/demo",
                "doi_source": "extraction_doi",
                "match_method": "extraction_doi",
                "master_record_id": "",
                "master_source_record_id": "",
            },
            {
                "study_id": "S002",
                "first_author": "Garcia",
                "year": "2020",
                "source_doi": "",
                "doi_source": "unresolved",
                "match_method": "no_match",
                "master_record_id": "",
                "master_source_record_id": "",
            },
        ]
        retraction_index = {
            "10.5555/demo": [
                {
                    "record_id": "RW-1",
                    "title": "Study demo",
                    "retraction_date": "2024-01-02",
                    "retraction_reason": "Fake data",
                    "url": "https://example.org/rw1",
                }
            ]
        }

        rows, counts = retraction_checker.build_result_rows(source_rows, retraction_index, api_error="")

        self.assertEqual(rows[0]["retraction_status"], "retracted")
        self.assertEqual(rows[1]["retraction_status"], "missing_doi")
        self.assertEqual(int(counts["retracted"]), 1)
        self.assertEqual(int(counts["missing_doi"]), 1)

    def test_main_with_local_snapshot_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            extraction_path = tmp_path / "extraction.csv"
            extraction_path.write_text(
                "study_id,first_author,year,title,doi,record_id,source_record_id\n"
                "S001,Smith,2021,Demo title,10.5555/demo,,\n",
                encoding="utf-8",
            )

            master_path = tmp_path / "master.csv"
            master_path.write_text(
                "record_id,source_record_id,title,authors,year,doi,is_duplicate\n",
                encoding="utf-8",
            )

            snapshot_path = tmp_path / "retraction_watch.csv"
            snapshot_path.write_text(
                "Record ID,Title,OriginalPaperDOI,RetractionDate,RetractionNature,URLS\n"
                "101,Demo title,10.5555/demo,2024-01-02,Fake data,https://example.org/rw1\n",
                encoding="utf-8",
            )

            results_output = tmp_path / "results.csv"
            summary_output = tmp_path / "summary.md"

            exit_code = retraction_checker.main(
                [
                    "--extraction",
                    str(extraction_path),
                    "--master",
                    str(master_path),
                    "--database-snapshot",
                    str(snapshot_path),
                    "--results-output",
                    str(results_output),
                    "--summary-output",
                    str(summary_output),
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue(results_output.exists())
            self.assertTrue(summary_output.exists())

            results_df = pd.read_csv(results_output, dtype=str)
            self.assertEqual(int(results_df.shape[0]), 1)
            self.assertEqual(results_df.loc[0, "retraction_status"], "retracted")


if __name__ == "__main__":
    unittest.main()
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

MODULE_PATH = Path(__file__).resolve().parents[1] / "dedup_merge.py"
spec = importlib.util.spec_from_file_location("dedup_merge", MODULE_PATH)
dedup_merge = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = dedup_merge
assert spec.loader is not None
spec.loader.exec_module(dedup_merge)


class DedupMergeRecordIdStabilityTests(unittest.TestCase):
    def _write_raw_csv(self, path: Path, rows: list[dict[str, str]]) -> None:
        pd.DataFrame(rows).to_csv(path, index=False)

    def test_read_record_id_map_recovers_concatenated_header_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            map_path = Path(tmp_dir) / "record_id_map.csv"
            map_path.write_text(
                "stable_key,record_id,first_seen_date\n"
                "doi:10.1000/a,R00001,2026-03-14\n"
                "source:pubmed|pmid-1,R00001,2026-03-14stable_key,record_id,first_seen_date\n"
                "doi:10.1000/b,R00002,2026-03-14\n",
                encoding="utf-8",
            )

            map_df = dedup_merge.read_record_id_map(map_path)

            self.assertEqual(int(map_df.shape[0]), 3)
            self.assertIn("source:pubmed|pmid-1", map_df["stable_key"].tolist())

    def test_append_record_id_map_entries_adds_missing_newline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            map_path = Path(tmp_dir) / "record_id_map.csv"
            map_path.write_text(
                "stable_key,record_id,first_seen_date\ndoi:10.1000/a,R00001,2026-03-14",
                encoding="utf-8",
            )

            appended = dedup_merge.append_record_id_map_entries(
                map_path,
                [
                    {
                        "stable_key": "doi:10.1000/b",
                        "record_id": "R00002",
                        "first_seen_date": "2026-03-14",
                    }
                ],
            )

            self.assertEqual(appended, 1)
            map_text = map_path.read_text(encoding="utf-8")
            self.assertNotIn("2026-03-14doi:10.1000/b", map_text)

            map_df = dedup_merge.read_record_id_map(map_path)
            self.assertEqual(int(map_df.shape[0]), 2)

    def test_canonical_record_id_remains_stable_when_new_higher_priority_source_appears(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            raw_dir = tmp_path / "raw"
            processed_dir = tmp_path / "processed"
            outputs_dir = tmp_path / "outputs"
            raw_dir.mkdir(parents=True, exist_ok=True)
            processed_dir.mkdir(parents=True, exist_ok=True)
            outputs_dir.mkdir(parents=True, exist_ok=True)

            search_log_path = processed_dir / "search_log.csv"
            master_path = processed_dir / "master_records.csv"
            record_id_map_path = processed_dir / "record_id_map.csv"
            summary_path = outputs_dir / "dedup_merge_summary.md"
            triage_path = outputs_dir / "new_record_triage.csv"

            scopus_file = raw_dir / "scopus_export.csv"
            self._write_raw_csv(
                scopus_file,
                [
                    {
                        "source_record_id": "SC-001",
                        "title": "Mindfulness and sleep quality in adults",
                        "authors": "Smith, Jane; Doe, John",
                        "year": "2024",
                        "journal": "Sleep Journal",
                        "doi": "10.1234/example-001",
                        "pmid": "",
                    }
                ],
            )
            pd.DataFrame(
                [
                    {
                        "database": "Scopus",
                        "date_searched": "2026-01-01",
                        "query_version": "v1",
                        "start_year": "2000",
                        "end_date": "2026-01-01",
                        "filters_applied": "",
                        "results_total": "1",
                        "results_exported": "1",
                        "export_filename": scopus_file.name,
                        "notes": "",
                    }
                ]
            ).to_csv(search_log_path, index=False)

            exit_code_first = dedup_merge.main(
                [
                    "--raw-dir",
                    str(raw_dir),
                    "--search-log",
                    str(search_log_path),
                    "--master",
                    str(master_path),
                    "--record-id-map",
                    str(record_id_map_path),
                    "--summary",
                    str(summary_path),
                    "--triage-output",
                    str(triage_path),
                    "--title-fuzzy-threshold",
                    "0",
                ]
            )
            self.assertEqual(exit_code_first, 0)

            first_master_df = pd.read_csv(master_path, dtype=str)
            first_canonical_id = first_master_df.loc[
                first_master_df["is_duplicate"] == "no", "record_id"
            ].iloc[0]

            pubmed_file = raw_dir / "pubmed_export.csv"
            self._write_raw_csv(
                pubmed_file,
                [
                    {
                        "source_record_id": "PM-9001",
                        "title": "Mindfulness and sleep quality in adults",
                        "authors": "Smith, Jane; Doe, John",
                        "year": "2024",
                        "journal": "Sleep Journal",
                        "doi": "10.1234/example-001",
                        "pmid": "12345678",
                    }
                ],
            )
            pd.DataFrame(
                [
                    {
                        "database": "PubMed",
                        "date_searched": "2026-02-01",
                        "query_version": "v1",
                        "start_year": "2000",
                        "end_date": "2026-02-01",
                        "filters_applied": "",
                        "results_total": "1",
                        "results_exported": "1",
                        "export_filename": pubmed_file.name,
                        "notes": "",
                    },
                    {
                        "database": "Scopus",
                        "date_searched": "2026-01-01",
                        "query_version": "v1",
                        "start_year": "2000",
                        "end_date": "2026-01-01",
                        "filters_applied": "",
                        "results_total": "1",
                        "results_exported": "1",
                        "export_filename": scopus_file.name,
                        "notes": "",
                    },
                ]
            ).to_csv(search_log_path, index=False)

            exit_code_second = dedup_merge.main(
                [
                    "--raw-dir",
                    str(raw_dir),
                    "--search-log",
                    str(search_log_path),
                    "--master",
                    str(master_path),
                    "--record-id-map",
                    str(record_id_map_path),
                    "--summary",
                    str(summary_path),
                    "--triage-output",
                    str(triage_path),
                    "--title-fuzzy-threshold",
                    "0",
                ]
            )
            self.assertEqual(exit_code_second, 0)

            second_master_df = pd.read_csv(master_path, dtype=str)
            canonical_rows = second_master_df.loc[
                (second_master_df["doi"] == "10.1234/example-001")
                & (second_master_df["is_duplicate"] == "no")
            ]
            self.assertEqual(int(canonical_rows.shape[0]), 1)
            self.assertEqual(canonical_rows.iloc[0]["record_id"], first_canonical_id)

            map_df = pd.read_csv(record_id_map_path, dtype=str)
            self.assertTrue(
                map_df["stable_key"].astype(str).str.startswith("doi:10.1234/example-001").any()
            )

            triage_df = pd.read_csv(triage_path, dtype=str)
            self.assertEqual(int(triage_df.shape[0]), 0)

    def test_record_id_map_is_append_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            raw_dir = tmp_path / "raw"
            processed_dir = tmp_path / "processed"
            outputs_dir = tmp_path / "outputs"
            raw_dir.mkdir(parents=True, exist_ok=True)
            processed_dir.mkdir(parents=True, exist_ok=True)
            outputs_dir.mkdir(parents=True, exist_ok=True)

            search_log_path = processed_dir / "search_log.csv"
            master_path = processed_dir / "master_records.csv"
            record_id_map_path = processed_dir / "record_id_map.csv"
            summary_path = outputs_dir / "dedup_merge_summary.md"
            triage_path = outputs_dir / "new_record_triage.csv"

            first_file = raw_dir / "first.csv"
            self._write_raw_csv(
                first_file,
                [
                    {
                        "source_record_id": "A-1",
                        "title": "Study A",
                        "authors": "Lee, Ann",
                        "year": "2023",
                        "doi": "10.5555/a",
                    }
                ],
            )
            pd.DataFrame(
                [
                    {
                        "database": "Scopus",
                        "date_searched": "2026-01-01",
                        "query_version": "v1",
                        "start_year": "2000",
                        "end_date": "2026-01-01",
                        "filters_applied": "",
                        "results_total": "1",
                        "results_exported": "1",
                        "export_filename": first_file.name,
                        "notes": "",
                    }
                ]
            ).to_csv(search_log_path, index=False)

            self.assertEqual(
                dedup_merge.main(
                    [
                        "--raw-dir",
                        str(raw_dir),
                        "--search-log",
                        str(search_log_path),
                        "--master",
                        str(master_path),
                        "--record-id-map",
                        str(record_id_map_path),
                        "--summary",
                        str(summary_path),
                        "--triage-output",
                        str(triage_path),
                        "--title-fuzzy-threshold",
                        "0",
                    ]
                ),
                0,
            )

            initial_map_text = record_id_map_path.read_text(encoding="utf-8")

            second_file = raw_dir / "second.csv"
            self._write_raw_csv(
                second_file,
                [
                    {
                        "source_record_id": "B-2",
                        "title": "Study B",
                        "authors": "Doe, Max",
                        "year": "2024",
                        "doi": "10.5555/b",
                    }
                ],
            )
            pd.DataFrame(
                [
                    {
                        "database": "Scopus",
                        "date_searched": "2026-01-01",
                        "query_version": "v1",
                        "start_year": "2000",
                        "end_date": "2026-01-01",
                        "filters_applied": "",
                        "results_total": "1",
                        "results_exported": "1",
                        "export_filename": first_file.name,
                        "notes": "",
                    },
                    {
                        "database": "PubMed",
                        "date_searched": "2026-02-01",
                        "query_version": "v1",
                        "start_year": "2000",
                        "end_date": "2026-02-01",
                        "filters_applied": "",
                        "results_total": "1",
                        "results_exported": "1",
                        "export_filename": second_file.name,
                        "notes": "",
                    },
                ]
            ).to_csv(search_log_path, index=False)

            self.assertEqual(
                dedup_merge.main(
                    [
                        "--raw-dir",
                        str(raw_dir),
                        "--search-log",
                        str(search_log_path),
                        "--master",
                        str(master_path),
                        "--record-id-map",
                        str(record_id_map_path),
                        "--summary",
                        str(summary_path),
                        "--triage-output",
                        str(triage_path),
                        "--title-fuzzy-threshold",
                        "0",
                    ]
                ),
                0,
            )

            updated_map_text = record_id_map_path.read_text(encoding="utf-8")
            self.assertTrue(updated_map_text.startswith(initial_map_text))
            self.assertGreater(len(updated_map_text), len(initial_map_text))

    def test_bootstrap_existing_master_ids_when_map_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            raw_dir = tmp_path / "raw"
            processed_dir = tmp_path / "processed"
            outputs_dir = tmp_path / "outputs"
            raw_dir.mkdir(parents=True, exist_ok=True)
            processed_dir.mkdir(parents=True, exist_ok=True)
            outputs_dir.mkdir(parents=True, exist_ok=True)

            search_log_path = processed_dir / "search_log.csv"
            master_path = processed_dir / "master_records.csv"
            record_id_map_path = processed_dir / "record_id_map.csv"
            summary_path = outputs_dir / "dedup_merge_summary.md"
            triage_path = outputs_dir / "new_record_triage.csv"

            pd.DataFrame(
                [
                    {
                        "record_id": "R00420",
                        "source_database": "Scopus",
                        "source_record_id": "LEG-1",
                        "title": "Legacy Study",
                        "abstract": "",
                        "authors": "Miller, A",
                        "year": "2021",
                        "journal": "Legacy Journal",
                        "doi": "10.7777/legacy",
                        "pmid": "",
                        "normalized_title": "legacy study",
                        "normalized_first_author": "miller",
                        "is_duplicate": "no",
                        "duplicate_of_record_id": "",
                        "dedup_reason": "",
                        "notes": "",
                    }
                ],
                columns=dedup_merge.MASTER_COLUMNS,
            ).to_csv(master_path, index=False)

            raw_file = raw_dir / "legacy.csv"
            self._write_raw_csv(
                raw_file,
                [
                    {
                        "source_record_id": "LEG-1",
                        "title": "Legacy Study",
                        "authors": "Miller, A",
                        "year": "2021",
                        "doi": "10.7777/legacy",
                    }
                ],
            )
            pd.DataFrame(
                [
                    {
                        "database": "Scopus",
                        "date_searched": "2026-01-01",
                        "query_version": "v1",
                        "start_year": "2000",
                        "end_date": "2026-01-01",
                        "filters_applied": "",
                        "results_total": "1",
                        "results_exported": "1",
                        "export_filename": raw_file.name,
                        "notes": "",
                    }
                ]
            ).to_csv(search_log_path, index=False)

            self.assertEqual(
                dedup_merge.main(
                    [
                        "--raw-dir",
                        str(raw_dir),
                        "--search-log",
                        str(search_log_path),
                        "--master",
                        str(master_path),
                        "--record-id-map",
                        str(record_id_map_path),
                        "--summary",
                        str(summary_path),
                        "--triage-output",
                        str(triage_path),
                        "--title-fuzzy-threshold",
                        "0",
                    ]
                ),
                0,
            )

            merged_df = pd.read_csv(master_path, dtype=str)
            canonical_id = merged_df.loc[merged_df["is_duplicate"] == "no", "record_id"].iloc[0]
            self.assertEqual(canonical_id, "R00420")
            self.assertTrue(record_id_map_path.exists())


if __name__ == "__main__":
    unittest.main()

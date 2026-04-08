from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "study_flow_map_builder.py"


class StudyFlowMapBuilderTests(unittest.TestCase):
    def test_builds_study_flow_with_explicit_record_links(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            extraction_path = tmp_path / "extraction_template.csv"
            master_records_path = tmp_path / "master_records.csv"
            record_id_map_path = tmp_path / "record_id_map.csv"
            screening_path = tmp_path / "screening_title_abstract_results.csv"
            search_log_path = tmp_path / "search_log.csv"
            output_path = tmp_path / "study_flow_map.csv"
            summary_path = tmp_path / "study_flow_map_summary.md"

            pd.DataFrame(
                [
                    {
                        "study_id": "study_01",
                        "source_id": "SRC_A",
                        "included_in_meta": "yes",
                        "included_in_bias": "yes",
                        "included_in_grade": "yes",
                        "exclusion_reason": "included_primary",
                    },
                    {
                        "study_id": "study_02",
                        "source_id": "SRC_B",
                        "included_in_meta": "no",
                        "included_in_bias": "no",
                        "included_in_grade": "no",
                        "exclusion_reason": "wrong_outcome",
                    },
                ]
            ).to_csv(extraction_path, index=False)

            pd.DataFrame(
                [
                    {"record_id": "R001", "source_record_id": "SRC_A", "source_database": "PubMed"},
                    {"record_id": "R002", "source_record_id": "SRC_B", "source_database": "Scopus"},
                ]
            ).to_csv(master_records_path, index=False)

            pd.DataFrame(columns=["stable_key", "record_id", "first_seen_date"]).to_csv(
                record_id_map_path, index=False
            )

            pd.DataFrame(
                [
                    {"record_id": "R001", "final_decision": "include"},
                    {"record_id": "R002", "final_decision": "exclude"},
                ]
            ).to_csv(screening_path, index=False)

            pd.DataFrame(
                [
                    {"database": "PubMed", "results_total": "10"},
                    {"database": "Scopus", "results_total": "2"},
                ]
            ).to_csv(search_log_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--extraction-input",
                    str(extraction_path),
                    "--master-records-input",
                    str(master_records_path),
                    "--record-id-map-input",
                    str(record_id_map_path),
                    "--screening-input",
                    str(screening_path),
                    "--search-log-input",
                    str(search_log_path),
                    "--output",
                    str(output_path),
                    "--summary-output",
                    str(summary_path),
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(output_path.exists())
            self.assertTrue(summary_path.exists())

            flow_df = pd.read_csv(output_path, dtype=str)
            rows = {row["study_id"]: row for _, row in flow_df.iterrows()}

            self.assertEqual(rows["study_01"]["found_in_search"], "yes")
            self.assertEqual(rows["study_01"]["passed_screening"], "yes")
            self.assertEqual(rows["study_01"]["included_in_review"], "yes")
            self.assertEqual(rows["study_01"]["included_in_meta"], "yes")
            self.assertEqual(rows["study_01"]["included_in_bias"], "yes")

            self.assertEqual(rows["study_02"]["found_in_search"], "yes")
            self.assertEqual(rows["study_02"]["passed_screening"], "no")
            self.assertEqual(rows["study_02"]["included_in_review"], "no")
            self.assertEqual(rows["study_02"]["included_in_meta"], "no")
            self.assertEqual(rows["study_02"]["included_in_bias"], "no")

    def test_uses_heuristic_screening_path_when_links_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            extraction_path = tmp_path / "extraction_template.csv"
            master_records_path = tmp_path / "master_records.csv"
            record_id_map_path = tmp_path / "record_id_map.csv"
            screening_path = tmp_path / "screening_title_abstract_results.csv"
            search_log_path = tmp_path / "search_log.csv"
            output_path = tmp_path / "study_flow_map.csv"
            summary_path = tmp_path / "study_flow_map_summary.md"

            pd.DataFrame(
                [
                    {
                        "study_id": "study_heuristic",
                        "source_id": "UNMAPPED_SOURCE",
                        "included_in_meta": "no",
                        "included_in_bias": "yes",
                        "included_in_grade": "yes",
                        "exclusion_reason": "included_contextual",
                    }
                ]
            ).to_csv(extraction_path, index=False)

            pd.DataFrame(columns=["record_id", "source_record_id", "source_database"]).to_csv(
                master_records_path, index=False
            )
            pd.DataFrame(columns=["stable_key", "record_id", "first_seen_date"]).to_csv(
                record_id_map_path, index=False
            )

            pd.DataFrame(
                [
                    {"record_id": "R100", "final_decision": "include"},
                ]
            ).to_csv(screening_path, index=False)

            pd.DataFrame(
                [
                    {"database": "PubMed", "results_total": "3"},
                ]
            ).to_csv(search_log_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--extraction-input",
                    str(extraction_path),
                    "--master-records-input",
                    str(master_records_path),
                    "--record-id-map-input",
                    str(record_id_map_path),
                    "--screening-input",
                    str(screening_path),
                    "--search-log-input",
                    str(search_log_path),
                    "--output",
                    str(output_path),
                    "--summary-output",
                    str(summary_path),
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)

            flow_df = pd.read_csv(output_path, dtype=str)
            row = flow_df.iloc[0]
            self.assertEqual(row["study_id"], "study_heuristic")
            self.assertEqual(row["found_in_search"], "yes")
            self.assertEqual(row["passed_screening"], "yes")
            self.assertEqual(row["included_in_review"], "yes")
            self.assertEqual(row["included_in_meta"], "no")
            self.assertEqual(row["included_in_bias"], "yes")

            summary_text = summary_path.read_text(encoding="utf-8")
            self.assertIn("Heuristic screening assumptions: 1", summary_text)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


RECORD_ID_MAP_HEADER = "stable_key,record_id,first_seen_date"
RECORD_ID_MAP_PATH = Path(__file__).resolve().parents[2] / "02_data/processed/record_id_map.csv"
MODULE_PATH = Path(__file__).resolve().parents[1] / "record_id_map_integrity_guard.py"
spec = importlib.util.spec_from_file_location("record_id_map_integrity_guard", MODULE_PATH)
record_id_map_integrity_guard = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = record_id_map_integrity_guard
assert spec.loader is not None
spec.loader.exec_module(record_id_map_integrity_guard)


class RecordIdMapIntegrityTests(unittest.TestCase):
    def test_record_id_map_guard_passes_for_repository_file(self) -> None:
        issues = record_id_map_integrity_guard.validate_record_id_map(RECORD_ID_MAP_PATH)
        self.assertEqual(issues, [])

    def test_record_id_map_guard_detects_concatenated_header_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir) / "record_id_map.csv"
            tmp_path.write_text(
                "stable_key,record_id,first_seen_date\n"
                "doi:10.1000/a,R00001,2026-03-14\n"
                "source:pubmed|pmid-1,R00001,2026-03-14stable_key,record_id,first_seen_date\n",
                encoding="utf-8",
            )

            issues = record_id_map_integrity_guard.validate_record_id_map(tmp_path)
            joined_issues = "\n".join(issues)

            self.assertIn(RECORD_ID_MAP_HEADER, joined_issues)
            self.assertIn("Header occurrence mismatch", joined_issues)


if __name__ == "__main__":
    unittest.main()

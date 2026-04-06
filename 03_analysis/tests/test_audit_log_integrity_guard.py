from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


AUDIT_LOG_PATH = Path(__file__).resolve().parents[2] / "02_data/processed/audit_log.csv"
MODULE_PATH = Path(__file__).resolve().parents[1] / "audit_log_integrity_guard.py"
spec = importlib.util.spec_from_file_location("audit_log_integrity_guard", MODULE_PATH)
audit_log_integrity_guard = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = audit_log_integrity_guard
assert spec.loader is not None
spec.loader.exec_module(audit_log_integrity_guard)


class AuditLogIntegrityGuardTests(unittest.TestCase):
    def test_guard_passes_for_repository_audit_log(self) -> None:
        report = audit_log_integrity_guard.validate_audit_log(AUDIT_LOG_PATH)
        self.assertEqual(report.issues, [])

    def test_guard_detects_duplicate_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir) / "audit_log.csv"
            tmp_path.write_text(
                "timestamp,action,file,description\n"
                "2026-03-18T21:33:41Z,run_success,03_analysis/daily_run.sh,daily_run success (run_id=20260318T213325Z-42937; review_mode=production; status_checkpoint_exit_code=0)\n"
                "2026-03-18T21:33:41Z,run_success,03_analysis/daily_run.sh,daily_run success (run_id=20260318T213325Z-42937; review_mode=production; status_checkpoint_exit_code=0)  \n",
                encoding="utf-8",
            )

            report = audit_log_integrity_guard.validate_audit_log(tmp_path)

            self.assertTrue(any("Duplicate rows detected" in issue for issue in report.issues))
            self.assertEqual(len(report.duplicate_rows), 1)

    def test_apply_rewrites_header_and_deduplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir) / "audit_log.csv"
            tmp_path.write_text(
                "bad,header\n"
                "2026-03-18T21:33:41Z,run_success,03_analysis/daily_run.sh,daily_run success (run_id=20260318T213325Z-42937; review_mode=production; status_checkpoint_exit_code=0)\n"
                "2026-03-18T21:33:41Z,run_success,03_analysis/daily_run.sh,daily_run success (run_id=20260318T213325Z-42937; review_mode=production; status_checkpoint_exit_code=0)\n",
                encoding="utf-8",
            )

            exit_code = audit_log_integrity_guard.main(["--path", tmp_path.as_posix(), "--apply"])
            self.assertEqual(exit_code, 0)

            lines = tmp_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(lines[0], audit_log_integrity_guard.EXPECTED_HEADER_LINE)
            self.assertEqual(len(lines), 2)


if __name__ == "__main__":
    unittest.main()
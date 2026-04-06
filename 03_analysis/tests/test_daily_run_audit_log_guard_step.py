from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "daily_run.sh"


def make_fake_python(binary_path: Path, log_path: Path) -> None:
    binary_path.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"printf '%s\\n' \"$*\" >> '{log_path.as_posix()}'\n"
        "if [[ \"$*\" == *\"status_cli.py\"* ]]; then\n"
        "  echo 'status_cli_checkpoint_ok'\n"
        "  exit 0\n"
        "fi\n"
        "if [[ \"$*\" == *\"record_id_map_integrity_guard.py\"* ]]; then\n"
        "  exit 77\n"
        "fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    binary_path.chmod(0o755)


class DailyRunAuditLogGuardStepTests(unittest.TestCase):
    def test_audit_log_guard_runs_between_csv_and_record_id_guards(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir(parents=True, exist_ok=True)

            calls_log = tmp_path / "python_calls.log"
            fake_python = fake_bin / "python"
            make_fake_python(fake_python, calls_log)

            status_cli_snapshot = tmp_path / "status_cli_snapshot.txt"
            run_manifest = tmp_path / "daily_run_manifest.json"
            run_failed_marker = tmp_path / "daily_run_failed.marker"
            audit_log = tmp_path / "audit_log.csv"

            env = os.environ.copy()
            env["PATH"] = f"{fake_bin.as_posix()}:{env.get('PATH', '')}"
            env["RUN_PREFLIGHT_PLACEHOLDER_GUARD"] = "skip"
            env["RUN_TEMPLATE_TERM_GUARD"] = "skip"
            env["RUN_KEYWORD_ANALYSIS"] = "0"
            env["RUN_POLYGLOT_TRANSLATION"] = "0"
            env["RUN_CITATION_TRACKING"] = "0"
            env["RUN_PUBLICATION_BIAS"] = "0"
            env["RUN_PROSPERO_DRAFTER"] = "0"
            env["RUN_MULTILANG_ABSTRACT_SCREENER"] = "0"
            env["RUN_RETRACTION_CHECKER"] = "0"
            env["RUN_LIVING_REVIEW_SCHEDULER"] = "0"
            env["RUN_REVIEWER_WORKLOAD_BALANCER"] = "0"
            env["RUN_WEEKLY_RISK_DIGEST"] = "0"
            env["STATUS_CLI_SNAPSHOT"] = status_cli_snapshot.as_posix()
            env["AUDIT_LOG_PATH"] = audit_log.as_posix()
            env["DAILY_RUN_MANIFEST"] = run_manifest.as_posix()
            env["DAILY_RUN_FAILED_MARKER"] = run_failed_marker.as_posix()
            env["DAILY_RUN_TRANSACTION_PATHS"] = (tmp_path / "tracked_outputs").as_posix()
            env["DAILY_RUN_TRANSACTION_ROOT"] = (tmp_path / ".daily_run_transaction").as_posix()

            result = subprocess.run(
                ["bash", str(SCRIPT_PATH)],
                cwd=SCRIPT_PATH.parent,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 77)
            self.assertTrue(calls_log.exists())
            calls = [line.strip() for line in calls_log.read_text(encoding="utf-8").splitlines() if line.strip()]

            validate_idx = next((idx for idx, call in enumerate(calls) if "validate_csv_inputs.py" in call), None)
            audit_guard_idx = next((idx for idx, call in enumerate(calls) if "audit_log_integrity_guard.py" in call), None)
            record_map_idx = next((idx for idx, call in enumerate(calls) if "record_id_map_integrity_guard.py" in call), None)

            self.assertIsNotNone(validate_idx)
            self.assertIsNotNone(audit_guard_idx)
            self.assertIsNotNone(record_map_idx)
            self.assertLess(validate_idx, audit_guard_idx)
            self.assertLess(audit_guard_idx, record_map_idx)

            audit_guard_call = calls[audit_guard_idx]
            self.assertIn("audit_log_integrity_guard.py", audit_guard_call)
            self.assertIn("--path", audit_guard_call)
            self.assertIn(audit_log.as_posix(), audit_guard_call)


if __name__ == "__main__":
    unittest.main()
from __future__ import annotations

import json
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
        "if [[ \"$*\" == *\"validate_csv_inputs.py\"* ]]; then\n"
        "  exit 42\n"
        "fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    binary_path.chmod(0o755)


class DailyRunStatusCheckpointTests(unittest.TestCase):
    def test_status_checkpoint_runs_even_if_pipeline_fails_early(self) -> None:
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

            env = os.environ.copy()
            env["PATH"] = f"{fake_bin.as_posix()}:{env.get('PATH', '')}"
            env["DAILY_RUN_TRANSACTION_PATHS"] = (tmp_path / "transaction_scope").as_posix()
            env["DAILY_RUN_TRANSACTION_ROOT"] = (tmp_path / ".daily_run_transaction").as_posix()
            env["RUN_PREFLIGHT_PLACEHOLDER_GUARD"] = "skip"
            env["RUN_WEEKLY_RISK_DIGEST"] = "0"
            env["STATUS_CLI_SNAPSHOT"] = status_cli_snapshot.as_posix()
            env["AUDIT_LOG_PATH"] = (tmp_path / "audit_log.csv").as_posix()
            env["DAILY_RUN_MANIFEST"] = run_manifest.as_posix()
            env["DAILY_RUN_FAILED_MARKER"] = run_failed_marker.as_posix()

            result = subprocess.run(
                ["bash", str(SCRIPT_PATH)],
                cwd=SCRIPT_PATH.parent,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 42)
            self.assertTrue(calls_log.exists())
            calls = [line.strip() for line in calls_log.read_text(encoding="utf-8").splitlines() if line.strip()]

            validate_idx = next((idx for idx, call in enumerate(calls) if "validate_csv_inputs.py" in call), None)
            status_report_idx = next((idx for idx, call in enumerate(calls) if "status_report.py" in call), None)
            status_cli_idx = next((idx for idx, call in enumerate(calls) if "status_cli.py" in call), None)

            self.assertIsNotNone(validate_idx)
            self.assertIsNotNone(status_report_idx)
            self.assertIsNotNone(status_cli_idx)
            self.assertGreater(status_report_idx, validate_idx)
            self.assertGreater(status_cli_idx, validate_idx)

            self.assertTrue(status_cli_snapshot.exists())
            self.assertIn("status_cli_checkpoint_ok", status_cli_snapshot.read_text(encoding="utf-8"))

            self.assertTrue(run_manifest.exists())
            manifest_payload = json.loads(run_manifest.read_text(encoding="utf-8"))
            self.assertEqual(manifest_payload["state"], "failed")
            self.assertEqual(manifest_payload["final_exit_code"], 42)
            self.assertEqual(manifest_payload["failure_phase"], "pipeline")

            self.assertTrue(run_failed_marker.exists())
            failed_marker_payload = json.loads(run_failed_marker.read_text(encoding="utf-8"))
            self.assertEqual(failed_marker_payload["final_exit_code"], 42)
            self.assertEqual(failed_marker_payload["failure_phase"], "pipeline")


if __name__ == "__main__":
    unittest.main()

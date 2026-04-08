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
        'if [[ "$*" == *"status_cli.py"* ]]; then\n'
        "  echo 'status_cli_checkpoint_ok'\n"
        "  exit 0\n"
        "fi\n"
        'if [[ "$*" == *"validate_csv_inputs.py"* ]]; then\n'
        "  exit 42\n"
        "fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    binary_path.chmod(0o755)


class DailyRunPriorityPolicyDefaultTests(unittest.TestCase):
    def test_uses_policy_default_fail_threshold_when_status_fail_on_is_unset(self) -> None:
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
            priority_policy = tmp_path / "custom_priority_policy.json"
            priority_policy.write_text(
                json.dumps(
                    {
                        "fail_thresholds": {"default": "critical"},
                        "health_level_severity": {"warning": "major", "error": "critical"},
                        "checklist_priority": {"default": "major"},
                    }
                ),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["PATH"] = f"{fake_bin.as_posix()}:{env.get('PATH', '')}"
            env["DAILY_RUN_TRANSACTION_PATHS"] = (tmp_path / "transaction_scope").as_posix()
            env["DAILY_RUN_TRANSACTION_ROOT"] = (tmp_path / ".daily_run_transaction").as_posix()
            env["RUN_PREFLIGHT_PLACEHOLDER_GUARD"] = "skip"
            env["RUN_WEEKLY_RISK_DIGEST"] = "0"
            env["REVIEW_MODE"] = "production"
            env["STATUS_PRIORITY_POLICY"] = priority_policy.as_posix()
            env.pop("STATUS_FAIL_ON", None)
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
            self.assertIn("STATUS_FAIL_ON not set; using policy default 'critical'", result.stdout)
            self.assertTrue(calls_log.exists())

            calls = [
                line.strip()
                for line in calls_log.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            status_cli_calls = [call for call in calls if "status_cli.py" in call]
            self.assertGreaterEqual(len(status_cli_calls), 2)

            policy_arg = f"--priority-policy {priority_policy.as_posix()}"
            self.assertTrue(any(policy_arg in call for call in status_cli_calls))

            production_gate_call = next(call for call in status_cli_calls if "--todo-only" in call)
            self.assertIn("--fail-on critical", production_gate_call)
            self.assertIn(policy_arg, production_gate_call)


if __name__ == "__main__":
    unittest.main()

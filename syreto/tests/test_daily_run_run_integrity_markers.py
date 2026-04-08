from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "daily_run.sh"


def make_fake_python(
    binary_path: Path,
    log_path: Path,
    *,
    fail_validate_csv: bool,
    transactional_target_path: Path,
) -> None:
    target_literal = transactional_target_path.as_posix()
    fail_clause = ""
    if fail_validate_csv:
        fail_clause = (
            'if [[ "$*" == *"validate_csv_inputs.py"* ]]; then\n'
            f"  printf '%s\\n' 'partial_update' > '{target_literal}'\n"
            "  exit 42\n"
            "fi\n"
        )

    binary_path.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"printf '%s\\n' \"$*\" >> '{log_path.as_posix()}'\n"
        'if [[ "$*" == *"status_cli.py"* ]]; then\n'
        "  echo 'status_cli_checkpoint_ok'\n"
        "fi\n"
        f"{fail_clause}"
        "exit 0\n",
        encoding="utf-8",
    )
    binary_path.chmod(0o755)


class DailyRunRunIntegrityMarkersTests(unittest.TestCase):
    def _run_daily(
        self, *, fail_validate_csv: bool
    ) -> tuple[subprocess.CompletedProcess[str], dict, bool, dict, list[str], str]:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir(parents=True, exist_ok=True)

            transactional_target = tmp_path / "transactional_scope.txt"
            transactional_target.write_text("baseline_snapshot\n", encoding="utf-8")

            calls_log = tmp_path / "python_calls.log"
            fake_python = fake_bin / "python"
            make_fake_python(
                fake_python,
                calls_log,
                fail_validate_csv=fail_validate_csv,
                transactional_target_path=transactional_target,
            )

            status_cli_snapshot = tmp_path / "status_cli_snapshot.txt"
            run_manifest = tmp_path / "daily_run_manifest.json"
            run_failed_marker = tmp_path / "daily_run_failed.marker"

            env = os.environ.copy()
            env["PATH"] = f"{fake_bin.as_posix()}:{env.get('PATH', '')}"
            env["DAILY_RUN_TRANSACTION_PATHS"] = transactional_target.as_posix()
            env["DAILY_RUN_TRANSACTION_ROOT"] = (tmp_path / ".daily_run_transaction").as_posix()
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

            calls: list[str] = []
            if calls_log.exists():
                calls = [
                    line.strip()
                    for line in calls_log.read_text(encoding="utf-8").splitlines()
                    if line.strip()
                ]

            manifest_payload: dict = {}
            if run_manifest.exists():
                manifest_payload = json.loads(run_manifest.read_text(encoding="utf-8"))

            failed_marker_exists = run_failed_marker.exists()
            failed_marker_payload: dict = {}
            if failed_marker_exists:
                failed_marker_payload = json.loads(run_failed_marker.read_text(encoding="utf-8"))

            transactional_target_text = transactional_target.read_text(encoding="utf-8")

            return (
                result,
                manifest_payload,
                failed_marker_exists,
                failed_marker_payload,
                calls,
                transactional_target_text,
            )

    def test_successful_run_marks_manifest_success_and_clears_failed_marker(self) -> None:
        result, manifest_payload, failed_marker_exists, _, _, transactional_target_text = (
            self._run_daily(fail_validate_csv=False)
        )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(manifest_payload.get("state"), "success")
        self.assertEqual(manifest_payload.get("final_exit_code"), 0)
        self.assertFalse(manifest_payload.get("rollback_applied"))
        self.assertFalse(failed_marker_exists)
        self.assertEqual(transactional_target_text, "baseline_snapshot\n")

    def test_failed_run_marks_manifest_failed_and_writes_failed_marker(self) -> None:
        (
            result,
            manifest_payload,
            failed_marker_exists,
            failed_marker_payload,
            calls,
            transactional_target_text,
        ) = self._run_daily(fail_validate_csv=True)

        self.assertEqual(result.returncode, 42)
        self.assertEqual(manifest_payload.get("state"), "failed")
        self.assertEqual(manifest_payload.get("final_exit_code"), 42)
        self.assertTrue(manifest_payload.get("rollback_applied"))
        self.assertTrue(failed_marker_exists)

        self.assertEqual(failed_marker_payload.get("final_exit_code"), 42)
        self.assertEqual(failed_marker_payload.get("failure_phase"), "pipeline")
        self.assertEqual(transactional_target_text, "baseline_snapshot\n")

        status_report_calls = [call for call in calls if "status_report.py" in call]
        self.assertGreaterEqual(len(status_report_calls), 2)


if __name__ == "__main__":
    unittest.main()

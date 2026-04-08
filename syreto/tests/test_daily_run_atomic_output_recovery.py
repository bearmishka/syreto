from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "daily_run.sh"


def make_fake_python(binary_path: Path, log_path: Path, tracked_output_path: Path) -> None:
    tracked_output_literal = tracked_output_path.as_posix()
    binary_path.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"printf '%s\\n' \"$*\" >> '{log_path.as_posix()}'\n"
        'if [[ "$*" == *"validate_csv_inputs.py"* ]]; then\n'
        f"  printf '%s\\n' 'partial_corrupted_output' > '{tracked_output_literal}'\n"
        "  exit 42\n"
        "fi\n"
        'if [[ "$*" == *"status_report.py"* ]]; then\n'
        '  if [[ -f "${DAILY_RUN_FAILED_MARKER:-}" ]]; then\n'
        f"    printf '%s\\n' 'status_report_after_failed_marker' >> '{log_path.as_posix()}'\n"
        "  else\n"
        f"    printf '%s\\n' 'status_report_before_failed_marker' >> '{log_path.as_posix()}'\n"
        "  fi\n"
        "  exit 0\n"
        "fi\n"
        'if [[ "$*" == *"status_cli.py"* ]]; then\n'
        "  echo 'status_cli_checkpoint_ok'\n"
        "  exit 0\n"
        "fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    binary_path.chmod(0o755)


class DailyRunAtomicOutputRecoveryTests(unittest.TestCase):
    def test_failed_run_rolls_back_partial_output_and_refreshes_status_after_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir(parents=True, exist_ok=True)

            tracked_outputs_dir = tmp_path / "tracked_outputs"
            tracked_outputs_dir.mkdir(parents=True, exist_ok=True)
            tracked_output_path = tracked_outputs_dir / "quality_appraisal_summary.md"
            tracked_output_path.write_text("baseline_valid_output\n", encoding="utf-8")

            calls_log = tmp_path / "python_calls.log"
            fake_python = fake_bin / "python"
            make_fake_python(fake_python, calls_log, tracked_output_path)

            status_cli_snapshot = tmp_path / "status_cli_snapshot.txt"
            run_manifest = tmp_path / "daily_run_manifest.json"
            run_failed_marker = tmp_path / "daily_run_failed.marker"

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
            env["AUDIT_LOG_PATH"] = (tmp_path / "audit_log.csv").as_posix()
            env["DAILY_RUN_MANIFEST"] = run_manifest.as_posix()
            env["DAILY_RUN_FAILED_MARKER"] = run_failed_marker.as_posix()
            env["DAILY_RUN_TRANSACTION_PATHS"] = tracked_outputs_dir.as_posix()
            env["DAILY_RUN_TRANSACTION_ROOT"] = (tmp_path / ".daily_run_transaction").as_posix()

            result = subprocess.run(
                ["bash", str(SCRIPT_PATH)],
                cwd=SCRIPT_PATH.parent,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 42)
            self.assertTrue(run_manifest.exists())
            self.assertTrue(run_failed_marker.exists())

            manifest_payload = json.loads(run_manifest.read_text(encoding="utf-8"))
            self.assertEqual(manifest_payload.get("state"), "failed")
            self.assertTrue(manifest_payload.get("rollback_applied"))
            self.assertEqual(manifest_payload.get("failure_phase"), "pipeline")

            failed_marker_payload = json.loads(run_failed_marker.read_text(encoding="utf-8"))
            self.assertEqual(failed_marker_payload.get("failure_phase"), "pipeline")

            restored_output_text = tracked_output_path.read_text(encoding="utf-8")
            self.assertEqual(restored_output_text, "baseline_valid_output\n")

            calls = [
                line.strip()
                for line in calls_log.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertIn("status_report_after_failed_marker", calls)


if __name__ == "__main__":
    unittest.main()

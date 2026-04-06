from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "daily_run.sh"


def make_fake_python(binary_path: Path) -> None:
    binary_path.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "if [[ \"$*\" == *\"status_cli.py\"* ]]; then\n"
        "  echo 'status_cli_checkpoint_ok'\n"
        "fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    binary_path.chmod(0o755)


class DailyRunManifestSingleObjectSmokeTests(unittest.TestCase):
    def test_manifest_stays_single_json_object_for_repeated_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir(parents=True, exist_ok=True)

            fake_python = fake_bin / "python"
            make_fake_python(fake_python)

            manifest_path = tmp_path / "daily_run_manifest.json"
            failed_marker_path = tmp_path / "daily_run_failed.marker"
            audit_log_path = tmp_path / "audit_log.csv"

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
            env["STATUS_CLI_SNAPSHOT"] = (tmp_path / "status_cli_snapshot.txt").as_posix()
            env["DAILY_RUN_TRANSACTION_PATHS"] = (tmp_path / "transaction_scope").as_posix()
            env["DAILY_RUN_TRANSACTION_ROOT"] = (tmp_path / ".daily_run_transaction").as_posix()
            env["DAILY_RUN_MANIFEST"] = manifest_path.as_posix()
            env["DAILY_RUN_FAILED_MARKER"] = failed_marker_path.as_posix()
            env["AUDIT_LOG_PATH"] = audit_log_path.as_posix()
            env["DAILY_RUN_ID"] = "smoke-fixed-run-id"

            audit_log_path.write_text(
                "timestamp,action,file,description\n"
                "2026-03-17T00:00:00Z,run_success,03_analysis/daily_run.sh,daily_run success (run_id=seed; review_mode=template; status_checkpoint_exit_code=0)\n"
                "2026-03-17T00:00:00Z,run_success,03_analysis/daily_run.sh,daily_run success (run_id=seed; review_mode=template; status_checkpoint_exit_code=0)\n",
                encoding="utf-8",
            )

            result_first = subprocess.run(
                ["bash", str(SCRIPT_PATH)],
                cwd=SCRIPT_PATH.parent,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            result_second = subprocess.run(
                ["bash", str(SCRIPT_PATH)],
                cwd=SCRIPT_PATH.parent,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result_first.returncode, 0)
            self.assertEqual(result_second.returncode, 0)
            self.assertTrue(manifest_path.exists())

            raw_text = manifest_path.read_text(encoding="utf-8")
            self.assertNotIn("}{", raw_text)
            self.assertEqual(raw_text.count("{"), 1)
            self.assertEqual(raw_text.count("}"), 1)

            parsed = json.loads(raw_text)
            self.assertEqual(parsed.get("run_id"), "smoke-fixed-run-id")
            self.assertEqual(parsed.get("state"), "success")
            self.assertFalse(parsed.get("rollback_applied"))

            self.assertTrue(audit_log_path.exists())
            audit_lines = [
                line.strip()
                for line in audit_log_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertGreaterEqual(len(audit_lines), 2)
            self.assertEqual(audit_lines[0], "timestamp,action,file,description")
            self.assertTrue(all(",run_success,03_analysis/daily_run.sh," in line for line in audit_lines[1:]))
            self.assertEqual(len(audit_lines[1:]), len(set(audit_lines[1:])))


if __name__ == "__main__":
    unittest.main()

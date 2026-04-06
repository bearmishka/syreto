from __future__ import annotations

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


class DailyRunAuditLogNewlineTests(unittest.TestCase):
    def test_run_success_entry_starts_new_line_when_audit_log_lacks_trailing_newline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir(parents=True, exist_ok=True)

            fake_python = fake_bin / "python"
            make_fake_python(fake_python)

            audit_log = tmp_path / "audit_log.csv"
            audit_log.write_text(
                "timestamp,action,file,description\n"
                "2026-03-17,update,README.md,documented audit-log deduplication behavior in daily_run section",
                encoding="utf-8",
            )

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
            env["AUDIT_LOG_PATH"] = audit_log.as_posix()
            env["DAILY_RUN_MANIFEST"] = (tmp_path / "daily_run_manifest.json").as_posix()
            env["DAILY_RUN_FAILED_MARKER"] = (tmp_path / "daily_run_failed.marker").as_posix()
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

            self.assertEqual(result.returncode, 0)

            lines = [line for line in audit_log.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertIn(
                "2026-03-17,update,README.md,documented audit-log deduplication behavior in daily_run section",
                lines,
            )
            self.assertTrue(any(",run_success,03_analysis/daily_run.sh," in line for line in lines))
            self.assertFalse(
                any("daily_run section" in line and ",run_success," in line for line in lines),
                msg="run_success entry should be on a separate line",
            )


if __name__ == "__main__":
    unittest.main()
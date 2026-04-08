from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "daily_run.sh"


def make_fake_python(binary_path: Path, log_path: Path) -> None:
    binary_path.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"printf '%s\\n' \"$*\" >> '{log_path.as_posix()}'\n"
        "exit 0\n",
        encoding="utf-8",
    )
    binary_path.chmod(0o755)


def run_daily_run_with_review_mode(mode: str) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        fake_bin = tmp_path / "bin"
        fake_bin.mkdir(parents=True, exist_ok=True)

        calls_log = tmp_path / "python_calls.log"
        fake_python = fake_bin / "python"
        make_fake_python(fake_python, calls_log)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin.as_posix()}:{env.get('PATH', '')}"
        env["DAILY_RUN_TRANSACTION_PATHS"] = (tmp_path / "transaction_scope").as_posix()
        env["DAILY_RUN_TRANSACTION_ROOT"] = (tmp_path / ".daily_run_transaction").as_posix()
        env["REVIEW_MODE"] = mode
        env["RUN_PREFLIGHT_PLACEHOLDER_GUARD"] = "skip"
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
        env["AUDIT_LOG_PATH"] = (tmp_path / "audit_log.csv").as_posix()

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

        return result, calls


class DailyRunEpistemicGuardModesTests(unittest.TestCase):
    def _epistemic_guard_calls(self, calls: list[str]) -> list[str]:
        return [call for call in calls if "epistemic_consistency_guard.py" in call]

    def test_template_mode_runs_informational_epistemic_guard(self) -> None:
        result, calls = run_daily_run_with_review_mode("template")

        self.assertEqual(result.returncode, 0)
        guard_calls = self._epistemic_guard_calls(calls)
        self.assertEqual(len(guard_calls), 1)
        call = guard_calls[0]
        self.assertIn("--review-mode template", call)
        self.assertIn("--no-fail-on-risk", call)
        self.assertNotIn("--fail-on-risk", call)

    def test_production_mode_runs_strict_epistemic_guard(self) -> None:
        result, calls = run_daily_run_with_review_mode("production")

        self.assertEqual(result.returncode, 0)
        guard_calls = self._epistemic_guard_calls(calls)
        self.assertEqual(len(guard_calls), 1)
        call = guard_calls[0]
        self.assertIn("--review-mode production", call)
        self.assertIn("--fail-on-risk", call)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "daily_run.sh"


def make_fake_python(binary_path: Path, log_path: Path, *, exit_code: int = 99) -> None:
    binary_path.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"printf '%s\\n' \"$*\" >> '{log_path.as_posix()}'\n"
        f"exit {exit_code}\n",
        encoding="utf-8",
    )
    binary_path.chmod(0o755)


def run_daily_run_with_mode(
    mode: str | None,
    *,
    status_fail_on: str | None = None,
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
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
        env["AUDIT_LOG_PATH"] = (tmp_path / "audit_log.csv").as_posix()
        if mode is None:
            env.pop("RUN_PREFLIGHT_PLACEHOLDER_GUARD", None)
        else:
            env["RUN_PREFLIGHT_PLACEHOLDER_GUARD"] = mode
        if status_fail_on is None:
            env.pop("STATUS_FAIL_ON", None)
        else:
            env["STATUS_FAIL_ON"] = status_fail_on

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


class DailyRunPreflightModesTests(unittest.TestCase):
    def test_default_mode_is_warn(self) -> None:
        result, calls = run_daily_run_with_mode(None)

        self.assertEqual(result.returncode, 99)
        self.assertGreaterEqual(len(calls), 1)
        self.assertIn("template_term_guard.py", calls[0])
        self.assertIn("--no-fail-on-match", calls[0])

    def test_fail_mode_calls_placeholder_guard_with_fail_on_match(self) -> None:
        result, calls = run_daily_run_with_mode("fail")

        self.assertEqual(result.returncode, 99)
        self.assertGreaterEqual(len(calls), 1)
        self.assertIn("template_term_guard.py", calls[0])
        self.assertIn("--fail-on-match", calls[0])
        self.assertNotIn("--no-fail-on-match", calls[0])

    def test_warn_mode_calls_placeholder_guard_without_fail_on_match(self) -> None:
        result, calls = run_daily_run_with_mode("warn")

        self.assertEqual(result.returncode, 99)
        self.assertGreaterEqual(len(calls), 1)
        self.assertIn("template_term_guard.py", calls[0])
        self.assertIn("--no-fail-on-match", calls[0])

    def test_skip_mode_skips_placeholder_guard(self) -> None:
        result, calls = run_daily_run_with_mode("skip")

        self.assertEqual(result.returncode, 99)
        self.assertGreaterEqual(len(calls), 1)
        self.assertIn("consolidate_title_abstract_consensus.py", calls[0])
        self.assertNotIn("template_term_guard.py", calls[0])

    def test_alias_one_maps_to_fail(self) -> None:
        result, calls = run_daily_run_with_mode("1")

        self.assertEqual(result.returncode, 99)
        self.assertGreaterEqual(len(calls), 1)
        self.assertIn(
            "Compatibility alias detected: RUN_PREFLIGHT_PLACEHOLDER_GUARD=1 -> fail", result.stdout
        )
        self.assertIn("template_term_guard.py", calls[0])
        self.assertIn("--fail-on-match", calls[0])

    def test_alias_zero_maps_to_skip(self) -> None:
        result, calls = run_daily_run_with_mode("0")

        self.assertEqual(result.returncode, 99)
        self.assertGreaterEqual(len(calls), 1)
        self.assertIn(
            "Compatibility alias detected: RUN_PREFLIGHT_PLACEHOLDER_GUARD=0 -> skip", result.stdout
        )
        self.assertIn("consolidate_title_abstract_consensus.py", calls[0])

    def test_invalid_mode_exits_with_code_2_before_python_calls(self) -> None:
        result, calls = run_daily_run_with_mode("invalid")

        self.assertEqual(result.returncode, 2)
        self.assertEqual(calls, [])
        self.assertIn("Invalid RUN_PREFLIGHT_PLACEHOLDER_GUARD", result.stdout)

    def test_invalid_status_fail_on_exits_with_code_2_before_python_calls(self) -> None:
        result, calls = run_daily_run_with_mode("skip", status_fail_on="blocker")

        self.assertEqual(result.returncode, 2)
        self.assertEqual(calls, [])
        self.assertIn("Invalid STATUS_FAIL_ON", result.stdout)


if __name__ == "__main__":
    unittest.main()

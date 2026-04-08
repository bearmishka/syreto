from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "daily_run.sh"


def make_fake_python(
    binary_path: Path, log_path: Path, *, fail_on_template_guard: bool = False
) -> None:
    fail_clause = ""
    if fail_on_template_guard:
        fail_clause = (
            'if [[ "$*" == *"template_term_guard.py"* ]] && [[ "$*" == *"--fail-on-match"* ]]; then\n'
            "  exit 1\n"
            "fi\n"
        )

    binary_path.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"printf '%s\\n' \"$*\" >> '{log_path.as_posix()}'\n"
        f"{fail_clause}"
        "exit 0\n",
        encoding="utf-8",
    )
    binary_path.chmod(0o755)


def run_daily_run_with_mode(
    mode: str | None,
    *,
    fail_on_template_guard: bool = False,
    review_mode: str = "template",
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        fake_bin = tmp_path / "bin"
        fake_bin.mkdir(parents=True, exist_ok=True)

        calls_log = tmp_path / "python_calls.log"
        fake_python = fake_bin / "python"
        make_fake_python(fake_python, calls_log, fail_on_template_guard=fail_on_template_guard)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin.as_posix()}:{env.get('PATH', '')}"
        env["DAILY_RUN_TRANSACTION_PATHS"] = (tmp_path / "transaction_scope").as_posix()
        env["DAILY_RUN_TRANSACTION_ROOT"] = (tmp_path / ".daily_run_transaction").as_posix()
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
        env["REVIEW_MODE"] = review_mode

        if mode is None:
            env.pop("RUN_TEMPLATE_TERM_GUARD", None)
        else:
            env["RUN_TEMPLATE_TERM_GUARD"] = mode

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


class DailyRunTemplateTermGuardModesTests(unittest.TestCase):
    def _template_guard_calls(self, calls: list[str]) -> list[str]:
        return [call for call in calls if "template_term_guard.py" in call]

    def test_default_mode_is_warn(self) -> None:
        result, calls = run_daily_run_with_mode(None)

        self.assertEqual(result.returncode, 0)
        guard_calls = self._template_guard_calls(calls)
        self.assertEqual(len(guard_calls), 1)
        self.assertIn("--no-fail-on-match", guard_calls[0])
        self.assertNotIn("--fail-on-match", guard_calls[0])

    def test_fail_mode_uses_fail_on_match(self) -> None:
        result, calls = run_daily_run_with_mode("fail")

        self.assertEqual(result.returncode, 0)
        guard_calls = self._template_guard_calls(calls)
        self.assertEqual(len(guard_calls), 1)
        self.assertIn("--fail-on-match", guard_calls[0])
        self.assertNotIn("--no-fail-on-match", guard_calls[0])

    def test_fail_mode_does_not_block_when_term_guard_reports_matches(self) -> None:
        result, calls = run_daily_run_with_mode("fail", fail_on_template_guard=True)

        self.assertEqual(result.returncode, 0)
        self.assertIn("[daily_run] Done. Updated files:", result.stdout)
        self.assertNotIn("Pipeline finished with failures", result.stdout)
        guard_calls = self._template_guard_calls(calls)
        self.assertEqual(len(guard_calls), 1)
        self.assertIn("--fail-on-match", guard_calls[0])

    def test_production_mode_enforces_strict_manuscript_guard_even_when_skip_requested(
        self,
    ) -> None:
        result, calls = run_daily_run_with_mode("skip", review_mode="production")

        self.assertEqual(result.returncode, 0)
        guard_calls = self._template_guard_calls(calls)
        self.assertEqual(len(guard_calls), 1)
        guard_call = guard_calls[0]
        self.assertIn("--scan-path ../04_manuscript", guard_call)
        self.assertIn("--check-placeholders", guard_call)
        self.assertIn("--no-check-banned-terms", guard_call)
        self.assertIn("--placeholder-pattern \\[(?:[A-Z][A-Z0-9_\\\\ ]{3,})\\]", guard_call)
        self.assertIn("--fail-on-match", guard_call)

    def test_production_mode_blocks_when_manuscript_guard_reports_matches(self) -> None:
        result, calls = run_daily_run_with_mode(
            "warn",
            fail_on_template_guard=True,
            review_mode="production",
        )

        self.assertEqual(result.returncode, 1)
        self.assertIn("Pipeline finished with failures", result.stdout)
        guard_calls = self._template_guard_calls(calls)
        self.assertEqual(len(guard_calls), 1)
        self.assertIn("--scan-path ../04_manuscript", guard_calls[0])

    def test_skip_mode_disables_template_term_guard(self) -> None:
        result, calls = run_daily_run_with_mode("skip")

        self.assertEqual(result.returncode, 0)
        guard_calls = self._template_guard_calls(calls)
        self.assertEqual(guard_calls, [])

    def test_alias_one_maps_to_fail(self) -> None:
        result, calls = run_daily_run_with_mode("1")

        self.assertEqual(result.returncode, 0)
        self.assertIn(
            "Compatibility alias detected: RUN_TEMPLATE_TERM_GUARD=1 -> fail", result.stdout
        )
        guard_calls = self._template_guard_calls(calls)
        self.assertEqual(len(guard_calls), 1)
        self.assertIn("--fail-on-match", guard_calls[0])

    def test_alias_zero_maps_to_skip(self) -> None:
        result, calls = run_daily_run_with_mode("0")

        self.assertEqual(result.returncode, 0)
        self.assertIn(
            "Compatibility alias detected: RUN_TEMPLATE_TERM_GUARD=0 -> skip", result.stdout
        )
        guard_calls = self._template_guard_calls(calls)
        self.assertEqual(guard_calls, [])

    def test_invalid_mode_exits_with_code_2_before_python_calls(self) -> None:
        result, calls = run_daily_run_with_mode("invalid")

        self.assertEqual(result.returncode, 2)
        self.assertEqual(calls, [])
        self.assertIn("Invalid RUN_TEMPLATE_TERM_GUARD", result.stdout)


if __name__ == "__main__":
    unittest.main()

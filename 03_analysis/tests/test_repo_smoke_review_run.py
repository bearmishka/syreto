from __future__ import annotations

import json
import os
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in os.sys.path:
    os.sys.path.insert(0, str(PROJECT_ROOT))

from syreto import cli  # noqa: E402


class _PathBackup:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.existed = path.exists()
        self.content = path.read_bytes() if self.existed and path.is_file() else None

    def restore(self) -> None:
        if self.existed:
            if self.content is not None:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                self.path.write_bytes(self.content)
        elif self.path.exists():
            if self.path.is_file():
                self.path.unlink()


def make_fake_python(binary_path: Path, log_path: Path) -> None:
    binary_path.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"printf '%s\\n' \"$*\" >> '{log_path.as_posix()}'\n"
        "mkdir -p outputs\n"
        'if [[ "$*" == *"status_report.py"* ]]; then\n'
        "  cat > outputs/status_summary.json <<'EOF'\n"
        '  {"project_status":"ok","findings":[],"generated_by":"repo-smoke"}\n'
        "EOF\n"
        "  cat > outputs/status_report.md <<'EOF'\n"
        "  # Status Report\n"
        "EOF\n"
        "fi\n"
        'if [[ "$*" == *"todo_action_plan_builder.py"* ]]; then\n'
        "  cat > outputs/todo_action_plan.md <<'EOF'\n"
        "  # Todo Action Plan\n"
        "EOF\n"
        "fi\n"
        'if [[ "$*" == *"review_descriptives_builder.py"* ]]; then\n'
        "  cat > outputs/review_descriptives.json <<'EOF'\n"
        '  {"included_study_count":1}\n'
        "EOF\n"
        "  cat > outputs/review_descriptives.md <<'EOF'\n"
        "  # Review Descriptives\n"
        "EOF\n"
        "fi\n"
        'if [[ "$*" == *"epistemic_consistency_guard.py"* ]]; then\n'
        "  cat > outputs/epistemic_consistency_report.md <<'EOF'\n"
        "  # Epistemic Consistency Report\n"
        "EOF\n"
        "fi\n"
        'if [[ "$*" == *"template_term_guard.py"* ]]; then\n'
        "  cat > outputs/template_term_guard_summary.md <<'EOF'\n"
        "  # Template Term Guard Summary\n"
        "EOF\n"
        "fi\n"
        'if [[ "$*" == *"status_cli.py"* ]]; then\n'
        "  echo 'status_cli_checkpoint_ok'\n"
        "fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    binary_path.chmod(0o755)


class RepoSmokeReviewRunTests(unittest.TestCase):
    def test_cli_review_run_with_repo_smoke_fixture_produces_core_run_artifacts(self) -> None:
        fixture_config = "reviews/fixtures/repo-smoke/review.toml"
        outputs_root = PROJECT_ROOT / "03_analysis" / "outputs"
        backed_up_paths = [
            outputs_root / "status_summary.json",
            outputs_root / "status_report.md",
            outputs_root / "todo_action_plan.md",
            outputs_root / "review_descriptives.json",
            outputs_root / "review_descriptives.md",
            outputs_root / "template_term_guard_summary.md",
            outputs_root / "epistemic_consistency_report.md",
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir(parents=True, exist_ok=True)
            calls_log = tmp_path / "python_calls.log"
            fake_python = fake_bin / "python"
            make_fake_python(fake_python, calls_log)

            manifest_path = tmp_path / "daily_run_manifest.json"
            failed_marker_path = tmp_path / "daily_run_failed.marker"
            run_events_path = tmp_path / "run_events.jsonl"
            status_cli_snapshot = tmp_path / "status_cli_snapshot.txt"
            audit_log_path = tmp_path / "audit_log.csv"
            transaction_target = tmp_path / "transaction_scope.txt"
            transaction_target.write_text("baseline\n", encoding="utf-8")

            env_updates = {
                "PATH": f"{fake_bin.as_posix()}:{os.environ.get('PATH', '')}",
                "RUN_PREFLIGHT_PLACEHOLDER_GUARD": "skip",
                "RUN_TEMPLATE_TERM_GUARD": "skip",
                "RUN_KEYWORD_ANALYSIS": "0",
                "RUN_POLYGLOT_TRANSLATION": "0",
                "RUN_CITATION_TRACKING": "0",
                "RUN_PUBLICATION_BIAS": "0",
                "RUN_PROSPERO_DRAFTER": "0",
                "RUN_MULTILANG_ABSTRACT_SCREENER": "0",
                "RUN_RETRACTION_CHECKER": "0",
                "RUN_LIVING_REVIEW_SCHEDULER": "0",
                "RUN_REVIEWER_WORKLOAD_BALANCER": "0",
                "RUN_WEEKLY_RISK_DIGEST": "0",
                "STATUS_CLI_SNAPSHOT": status_cli_snapshot.as_posix(),
                "DAILY_RUN_TRANSACTION_PATHS": transaction_target.as_posix(),
                "DAILY_RUN_TRANSACTION_ROOT": (tmp_path / ".daily_run_transaction").as_posix(),
                "DAILY_RUN_MANIFEST": manifest_path.as_posix(),
                "DAILY_RUN_FAILED_MARKER": failed_marker_path.as_posix(),
                "RUN_EVENTS_PATH": run_events_path.as_posix(),
                "AUDIT_LOG_PATH": audit_log_path.as_posix(),
            }

            with ExitStack() as stack:
                backups = [_PathBackup(path) for path in backed_up_paths]
                stack.callback(lambda: [backup.restore() for backup in reversed(backups)])
                stack.enter_context(mock.patch.dict(os.environ, env_updates, clear=False))

                exit_code = cli.main(["review", "run", "--config", fixture_config])

                self.assertEqual(exit_code, 0)
                self.assertTrue(manifest_path.exists())
                self.assertTrue(run_events_path.exists())
                self.assertFalse(failed_marker_path.exists())
                self.assertTrue(status_cli_snapshot.exists())
                self.assertTrue(audit_log_path.exists())
                self.assertTrue((outputs_root / "status_summary.json").exists())

                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                self.assertEqual(manifest.get("state"), "success")
                self.assertEqual(manifest.get("final_exit_code"), 0)
                self.assertFalse(manifest.get("rollback_applied"))

                run_events = [
                    json.loads(line)
                    for line in run_events_path.read_text(encoding="utf-8").splitlines()
                    if line.strip()
                ]
                self.assertTrue(
                    any(
                        event.get("step") == "status_report" and event.get("status") == "success"
                        for event in run_events
                    )
                )

                calls = calls_log.read_text(encoding="utf-8")
                self.assertIn("status_report.py", calls)
                self.assertIn("todo_action_plan_builder.py", calls)
                self.assertIn("status_cli.py", calls)


if __name__ == "__main__":
    unittest.main()

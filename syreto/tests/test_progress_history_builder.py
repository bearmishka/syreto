from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "progress_history_builder.py"


def run_builder(
    *,
    manifest_payload: dict,
    status_summary_payload: dict,
    history_path: Path,
    summary_path: Path,
    review_mode: str = "template",
) -> subprocess.CompletedProcess[str]:
    manifest_path = history_path.parent / "daily_run_manifest.json"
    status_summary_path = history_path.parent / "status_summary.json"

    manifest_path.write_text(
        json.dumps(manifest_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    status_summary_path.write_text(
        json.dumps(status_summary_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--manifest",
            str(manifest_path),
            "--status-summary",
            str(status_summary_path),
            "--history-output",
            str(history_path),
            "--summary-output",
            str(summary_path),
            "--review-mode",
            review_mode,
        ],
        cwd=SCRIPT_PATH.parent,
        capture_output=True,
        text=True,
        check=False,
    )


class ProgressHistoryBuilderTests(unittest.TestCase):
    def test_appends_runs_and_computes_latest_deltas(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            history_path = tmp_path / "progress_history.csv"
            summary_path = tmp_path / "progress_history_summary.md"

            run1_manifest = {
                "run_id": "run_001",
                "state": "success",
                "started_at_utc": "2026-03-17T23:00:00Z",
                "updated_at_utc": "2026-03-17T23:01:00Z",
                "pipeline_exit_code": 0,
                "status_checkpoint_exit_code": 0,
                "final_exit_code": 0,
                "failure_phase": "",
                "rollback_applied": False,
                "transactional_mode": "enabled",
            }
            run1_status = {
                "data_snapshot": {
                    "search_results_total": 10,
                    "unique_records_after_dedup": 8,
                    "records_screened": 7,
                    "includes": 2,
                    "excludes": 4,
                    "maybe": 1,
                    "pending": 0,
                },
                "stage_assessment": {"id": "screening_active"},
                "health_checks": [
                    {"level": "ok", "message": "ok"},
                    {"level": "warning", "message": "warn"},
                ],
                "warnings": ["w1"],
                "input_checklist": [
                    {"id": "search_totals", "done": False},
                ],
            }

            result1 = run_builder(
                manifest_payload=run1_manifest,
                status_summary_payload=run1_status,
                history_path=history_path,
                summary_path=summary_path,
                review_mode="template",
            )
            self.assertEqual(result1.returncode, 0, msg=result1.stderr)

            history_df = pd.read_csv(history_path, dtype=str).fillna("")
            self.assertEqual(history_df.shape[0], 1)
            self.assertEqual(history_df.iloc[0]["run_id"], "run_001")
            self.assertEqual(history_df.iloc[0]["delta_search_results_total"], "")

            run2_manifest = {
                "run_id": "run_002",
                "state": "success",
                "started_at_utc": "2026-03-18T00:00:00Z",
                "updated_at_utc": "2026-03-18T00:02:00Z",
                "pipeline_exit_code": 0,
                "status_checkpoint_exit_code": 0,
                "final_exit_code": 0,
                "failure_phase": "",
                "rollback_applied": False,
                "transactional_mode": "enabled",
            }
            run2_status = {
                "data_snapshot": {
                    "search_results_total": 13,
                    "unique_records_after_dedup": 10,
                    "records_screened": 9,
                    "includes": 3,
                    "excludes": 5,
                    "maybe": 1,
                    "pending": 0,
                },
                "stage_assessment": {"id": "screening_active"},
                "health_checks": [
                    {"level": "ok", "message": "ok"},
                ],
                "warnings": [],
                "input_checklist": [
                    {"id": "search_totals", "done": True},
                ],
            }

            result2 = run_builder(
                manifest_payload=run2_manifest,
                status_summary_payload=run2_status,
                history_path=history_path,
                summary_path=summary_path,
                review_mode="production",
            )
            self.assertEqual(result2.returncode, 0, msg=result2.stderr)

            history_df = pd.read_csv(history_path, dtype=str).fillna("")
            self.assertEqual(history_df.shape[0], 2)
            latest = history_df.iloc[-1]
            self.assertEqual(latest["run_id"], "run_002")
            self.assertEqual(latest["review_mode"], "production")
            self.assertEqual(latest["delta_search_results_total"], "3")
            self.assertEqual(latest["delta_unique_records_after_dedup"], "2")
            self.assertEqual(latest["delta_records_screened"], "2")
            self.assertEqual(latest["delta_todo_open_count"], "-1")

            summary_text = summary_path.read_text(encoding="utf-8")
            self.assertIn("Runs tracked: 2", summary_text)
            self.assertIn("Search results total: +3", summary_text)

    def test_upserts_same_run_id_without_duplicate_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            history_path = tmp_path / "progress_history.csv"
            summary_path = tmp_path / "progress_history_summary.md"

            run_manifest = {
                "run_id": "run_same",
                "state": "failed",
                "started_at_utc": "2026-03-17T23:00:00Z",
                "updated_at_utc": "2026-03-17T23:01:00Z",
                "pipeline_exit_code": 1,
                "status_checkpoint_exit_code": 1,
                "final_exit_code": 1,
                "failure_phase": "pipeline",
                "rollback_applied": True,
                "transactional_mode": "enabled",
            }
            status_payload = {
                "data_snapshot": {
                    "search_results_total": 5,
                    "unique_records_after_dedup": 5,
                    "records_screened": 4,
                    "includes": 1,
                    "excludes": 3,
                    "maybe": 0,
                    "pending": 0,
                },
                "stage_assessment": {"id": "bootstrap_demo"},
                "health_checks": [{"level": "error", "message": "err"}],
                "warnings": ["warn"],
                "input_checklist": [{"id": "x", "done": False}],
            }

            result1 = run_builder(
                manifest_payload=run_manifest,
                status_summary_payload=status_payload,
                history_path=history_path,
                summary_path=summary_path,
            )
            self.assertEqual(result1.returncode, 0, msg=result1.stderr)

            run_manifest_updated = {
                **run_manifest,
                "state": "success",
                "updated_at_utc": "2026-03-17T23:05:00Z",
                "pipeline_exit_code": 0,
                "status_checkpoint_exit_code": 0,
                "final_exit_code": 0,
                "failure_phase": "",
                "rollback_applied": False,
            }
            status_payload_updated = {
                **status_payload,
                "data_snapshot": {
                    **status_payload["data_snapshot"],
                    "search_results_total": 6,
                },
            }

            result2 = run_builder(
                manifest_payload=run_manifest_updated,
                status_summary_payload=status_payload_updated,
                history_path=history_path,
                summary_path=summary_path,
            )
            self.assertEqual(result2.returncode, 0, msg=result2.stderr)

            history_df = pd.read_csv(history_path, dtype=str).fillna("")
            self.assertEqual(history_df.shape[0], 1)
            row = history_df.iloc[0]
            self.assertEqual(row["run_id"], "run_same")
            self.assertEqual(row["state"], "success")
            self.assertEqual(row["pipeline_exit_code"], "0")
            self.assertEqual(row["search_results_total"], "6")


if __name__ == "__main__":
    unittest.main()

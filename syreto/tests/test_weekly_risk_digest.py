import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest


MODULE_PATH = Path(__file__).resolve().parents[1] / "weekly_risk_digest.py"
spec = importlib.util.spec_from_file_location("weekly_risk_digest", MODULE_PATH)
weekly_risk_digest = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = weekly_risk_digest
assert spec.loader is not None
spec.loader.exec_module(weekly_risk_digest)


class WeeklyRiskDigestTests(unittest.TestCase):
    def test_generates_digest_from_status_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            status_path = tmp_path / "status_summary.json"
            output_path = tmp_path / "weekly_risk_digest.md"

            payload = {
                "data_snapshot": {
                    "search_results_total": 120,
                    "unique_records_after_dedup": 90,
                    "records_screened": 40,
                    "includes": 6,
                },
                "project_posture": {
                    "summary_en": "Production-capable review scaffold with enforced status integrity, but not yet domain-instantiated.",
                    "primary_blocker": "semantic_completeness",
                    "semantic_completeness": {
                        "complete": False,
                        "protocol_placeholder_count": 6,
                        "manuscript_placeholder_count": 11,
                        "placeholder_examples": ["[YOUR REVIEW TITLE]", "[POPULATION]"],
                    },
                },
                "registration": {"registered": False},
                "reviewer_agreement": {"cohen_kappa": {"available": False}},
                "health_checks": [
                    {
                        "level": "error",
                        "message": "PRISMA counts are out of sync with source data.",
                    },
                    {"level": "warning", "message": "Only one title/abstract reviewer logged."},
                ],
                "warnings": ["Fill PRISMA counts for required core keys."],
                "suggested_next_step": ["Sync PRISMA counts from source trackers."],
                "input_checklist": [
                    {
                        "done": False,
                        "title": "Keep PRISMA core counts in sync",
                        "details": "mismatch detected",
                        "file": "02_data/processed/prisma_counts_template.csv",
                    }
                ],
            }
            status_path.write_text(json.dumps(payload), encoding="utf-8")

            exit_code = weekly_risk_digest.main(
                [
                    "--status-summary",
                    str(status_path),
                    "--output",
                    str(output_path),
                    "--today",
                    "2026-03-14",
                ]
            )

            self.assertEqual(exit_code, 0)
            text = output_path.read_text(encoding="utf-8")
            self.assertIn("Weekly Risk Digest", text)
            self.assertIn("PRISMA counts are out of sync", text)
            self.assertIn("Sync PRISMA counts from source trackers.", text)
            self.assertIn("## Project Posture", text)
            self.assertIn("Primary blocker: semantic_completeness", text)
            self.assertIn("Unresolved placeholders: protocol=6, manuscript=11", text)

    def test_digest_handles_missing_project_posture(self) -> None:
        digest = weekly_risk_digest.build_digest(
            {
                "data_snapshot": {
                    "search_results_total": 0,
                    "unique_records_after_dedup": 0,
                    "records_screened": 0,
                    "includes": 0,
                },
                "stage_assessment": {},
                "registration": {"registered": False},
                "reviewer_agreement": {"cohen_kappa": {"available": False}},
                "health_checks": [],
                "warnings": [],
                "suggested_next_step": [],
                "input_checklist": [],
            },
            today_value=weekly_risk_digest.parse_today("2026-03-14"),
            max_risks=5,
            max_actions=5,
        )

        self.assertIn("## Project Posture", digest)
        self.assertIn("not available in current status summary", digest)

    def test_missing_status_summary_writes_fallback_digest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            missing_status = tmp_path / "missing_status_summary.json"
            output_path = tmp_path / "weekly_risk_digest.md"

            exit_code = weekly_risk_digest.main(
                [
                    "--status-summary",
                    str(missing_status),
                    "--output",
                    str(output_path),
                ]
            )

            self.assertEqual(exit_code, 0)
            text = output_path.read_text(encoding="utf-8")
            self.assertIn("Digest fallback", text)
            self.assertIn("status summary file not found", text)


if __name__ == "__main__":
    unittest.main()

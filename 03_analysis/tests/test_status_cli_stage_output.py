import importlib.util
from pathlib import Path
import sys
import unittest


MODULE_PATH = Path(__file__).resolve().parents[1] / "status_cli.py"
spec = importlib.util.spec_from_file_location("status_cli", MODULE_PATH)
status_cli = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = status_cli
assert spec.loader is not None
spec.loader.exec_module(status_cli)


class StatusCliStageOutputTests(unittest.TestCase):
    def test_cli_output_contains_stage_line(self) -> None:
        summary = {
            "generated_at": "2026-03-14 21:00",
            "data_snapshot": {
                "search_results_total": 0,
                "unique_records_after_dedup": 2,
                "records_screened": 0,
                "includes": 0,
            },
            "stage_assessment": {
                "id": "bootstrap_demo",
                "label": "Bootstrap / Demo Calibration",
                "reasons": ["Demo markers detected."],
            },
            "registration": {"registered": False, "registration_id": None},
            "reviewer_agreement": {
                "title_abstract_reviewers": [],
                "cohen_kappa": {"available": False},
            },
            "csv_input_validation": {"present": True, "parsed": True, "errors": 0, "warnings": 0},
            "extraction_validation": {"present": True, "parsed": True, "errors": 0, "warnings": 0},
            "effect_size_conversion": {
                "summary_present": True,
                "converted_present": True,
                "details": "summary: present; converted CSV: present",
            },
            "warnings": [],
            "suggested_next_step": [],
            "health_checks": [],
            "input_checklist": [],
        }

        rendered = status_cli.build_cli_output(summary)
        self.assertIn("- Stage: Bootstrap / Demo Calibration (bootstrap_demo)", rendered)

    def test_cli_output_groups_todos_by_file_with_quick_fix_hint(self) -> None:
        summary = {
            "generated_at": "2026-03-18 00:00",
            "data_snapshot": {},
            "stage_assessment": {"id": "screening_active", "label": "Screening Active"},
            "registration": {"registered": False},
            "reviewer_agreement": {"title_abstract_reviewers": [], "cohen_kappa": {"available": False}},
            "csv_input_validation": {},
            "extraction_validation": {},
            "effect_size_conversion": {},
            "warnings": [],
            "suggested_next_step": [],
            "health_checks": [],
            "input_checklist": [
                {
                    "id": "csv_input_validation",
                    "title": "Keep CSV input validation clean",
                    "file": "03_analysis/outputs/csv_input_validation_summary.md",
                    "details": "errors=1",
                    "hint": "Run validator",
                    "done": False,
                },
                {
                    "id": "search_totals",
                    "title": "Fill search totals",
                    "file": "02_data/processed/search_log.csv",
                    "details": "sum=0",
                    "hint": "Fill results_total",
                    "done": False,
                },
            ],
        }

        rendered = status_cli.build_cli_output(summary, todo_only=True)
        self.assertIn("TODO by file", rendered)
        self.assertIn("03_analysis/outputs/csv_input_validation_summary.md", rendered)
        self.assertIn("02_data/processed/search_log.csv", rendered)
        self.assertIn("quick-fix: python validate_csv_inputs.py", rendered)


if __name__ == "__main__":
    unittest.main()
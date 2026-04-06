from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "todo_action_plan_builder.py"


class TodoActionPlanBuilderTests(unittest.TestCase):
    def test_builds_grouped_todo_plan_with_quick_fix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            status_summary_path = tmp_path / "status_summary.json"
            output_path = tmp_path / "todo_action_plan.md"

            status_summary_path.write_text(
                json.dumps(
                    {
                        "input_checklist": [
                            {
                                "id": "csv_input_validation",
                                "title": "Keep CSV input validation clean",
                                "file": "03_analysis/outputs/csv_input_validation_summary.md",
                                "details": "errors=2",
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
                            {
                                "id": "quality_appraisal",
                                "title": "Quality appraisal",
                                "file": "03_analysis/outputs/quality_appraisal_summary.md",
                                "details": "missing",
                                "hint": "Run appraisal",
                                "done": True,
                            },
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--input",
                    str(status_summary_path),
                    "--output",
                    str(output_path),
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(output_path.exists())

            text = output_path.read_text(encoding="utf-8")
            self.assertIn("Pending checklist items: 2", text)
            self.assertIn("`02_data/processed/search_log.csv`", text)
            self.assertIn("`03_analysis/outputs/csv_input_validation_summary.md`", text)
            self.assertIn("Quick fix: `python validate_csv_inputs.py`", text)
            self.assertIn("Quick fix: `python validate_csv_inputs.py`", text)
            self.assertNotIn("Quality appraisal", text)

    def test_writes_empty_plan_when_no_pending_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            status_summary_path = tmp_path / "status_summary.json"
            output_path = tmp_path / "todo_action_plan.md"

            status_summary_path.write_text(
                json.dumps({"input_checklist": [{"id": "x", "done": True}]}, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--input",
                    str(status_summary_path),
                    "--output",
                    str(output_path),
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            text = output_path.read_text(encoding="utf-8")
            self.assertIn("Pending checklist items: 0", text)
            self.assertIn("✅ No open TODO items.", text)


if __name__ == "__main__":
    unittest.main()
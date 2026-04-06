import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "nos_to_jbi_converter.py"
spec = importlib.util.spec_from_file_location("nos_to_jbi_converter", MODULE_PATH)
nos_to_jbi_converter = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = nos_to_jbi_converter
assert spec.loader is not None
spec.loader.exec_module(nos_to_jbi_converter)


class NosToJbiConverterTests(unittest.TestCase):
    def test_risk_to_response_mapping(self) -> None:
        self.assertEqual(nos_to_jbi_converter.risk_to_response("low"), "yes")
        self.assertEqual(nos_to_jbi_converter.risk_to_response("moderate"), "unclear")
        self.assertEqual(nos_to_jbi_converter.risk_to_response("serious"), "no")
        self.assertEqual(nos_to_jbi_converter.risk_to_response(""), "unclear")

    def test_row_to_jbi_for_cross_sectional(self) -> None:
        row = pd.Series(
            {
                "study_id": "S1",
                "study_design": "cross_sectional",
                "selection_bias": "serious",
                "performance_bias": "moderate",
                "detection_bias": "low",
                "attrition_bias": "low",
                "reporting_bias": "moderate",
                "overall_risk": "serious",
                "appraiser_id": "a1",
                "checked_by": "a2",
                "appraisal_notes": "base note",
            }
        )
        converted = nos_to_jbi_converter.row_to_jbi(row)

        self.assertEqual(converted["study_id"], "S1")
        self.assertEqual(converted["jbi_tool"], "jbi_analytical_cross_sectional")
        self.assertEqual(converted["study_design"], "cross_sectional")
        self.assertEqual(converted["item_01"], "no")
        self.assertEqual(converted["item_04"], "unclear")
        self.assertEqual(converted["item_05"], "yes")
        self.assertEqual(converted["item_09"], "na")
        self.assertIn("Converted from NOS template", converted["appraisal_notes"])

    def test_cli_conversion_writes_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            nos_input_path = tmp_path / "quality_appraisal_template_nos.csv"
            jbi_output_path = tmp_path / "quality_appraisal_template.csv"
            summary_output_path = tmp_path / "nos_to_jbi_conversion_summary.md"

            pd.DataFrame(
                [
                    {
                        "study_id": "S1",
                        "study_design": "cross_sectional",
                        "selection_bias": "serious",
                        "performance_bias": "moderate",
                        "detection_bias": "moderate",
                        "attrition_bias": "low",
                        "reporting_bias": "moderate",
                        "overall_risk": "serious",
                        "appraiser_id": "appraiser_a",
                        "checked_by": "reviewer_a",
                        "appraisal_notes": "note",
                    }
                ]
            ).to_csv(nos_input_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(MODULE_PATH),
                    "--nos-input",
                    str(nos_input_path),
                    "--jbi-output",
                    str(jbi_output_path),
                    "--summary-output",
                    str(summary_output_path),
                ],
                cwd=MODULE_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(jbi_output_path.exists())
            self.assertTrue(summary_output_path.exists())

            jbi_df = pd.read_csv(jbi_output_path, dtype=str)
            self.assertEqual(int(jbi_df.shape[0]), 1)
            self.assertEqual(jbi_df.iloc[0]["study_id"], "S1")
            self.assertEqual(jbi_df.iloc[0]["jbi_tool"], "jbi_analytical_cross_sectional")

            summary_text = summary_output_path.read_text(encoding="utf-8")
            self.assertIn("NOS to JBI Conversion Summary", summary_text)


if __name__ == "__main__":
    unittest.main()
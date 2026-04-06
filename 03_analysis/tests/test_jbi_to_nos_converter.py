import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "jbi_to_nos_converter.py"
spec = importlib.util.spec_from_file_location("jbi_to_nos_converter", MODULE_PATH)
jbi_to_nos_converter = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = jbi_to_nos_converter
assert spec.loader is not None
spec.loader.exec_module(jbi_to_nos_converter)


class JbiToNosConverterTests(unittest.TestCase):
    def test_response_to_risk_mapping(self) -> None:
        self.assertEqual(jbi_to_nos_converter.response_to_risk("yes"), "low")
        self.assertEqual(jbi_to_nos_converter.response_to_risk("unclear"), "moderate")
        self.assertEqual(jbi_to_nos_converter.response_to_risk("no"), "serious")
        self.assertEqual(jbi_to_nos_converter.response_to_risk(""), "moderate")

    def test_row_to_nos_maps_bias_and_stars(self) -> None:
        row = pd.Series(
            {
                "study_id": "S1",
                "study_design": "cross_sectional",
                "jbi_tool": "jbi_analytical_cross_sectional",
                "item_01": "yes",
                "item_02": "no",
                "item_03": "unclear",
                "item_04": "yes",
                "item_05": "unclear",
                "item_06": "no",
                "item_07": "yes",
                "item_08": "yes",
                "item_09": "na",
                "item_10": "na",
                "item_11": "na",
                "appraiser_id": "a1",
                "checked_by": "a2",
                "appraisal_notes": "base",
            }
        )
        converted = jbi_to_nos_converter.row_to_nos(row)

        self.assertEqual(converted["study_id"], "S1")
        self.assertEqual(converted["study_design"], "cross_sectional")
        self.assertEqual(converted["appraisal_framework"], "newcastle_ottawa_scale_adapted")
        self.assertEqual(converted["selection_bias"], "serious")
        self.assertEqual(converted["performance_bias"], "low")
        self.assertEqual(converted["detection_bias"], "moderate")
        self.assertEqual(converted["attrition_bias"], "serious")
        self.assertEqual(converted["reporting_bias"], "low")
        self.assertEqual(converted["overall_risk"], "low")
        self.assertEqual(converted["nos_selection_stars"], "1")
        self.assertEqual(converted["nos_comparability_stars"], "1")
        self.assertEqual(converted["nos_outcome_exposure_stars"], "2")
        self.assertEqual(converted["nos_total_stars"], "4")
        self.assertIn("Converted from JBI template", converted["appraisal_notes"])

    def test_cli_conversion_writes_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            jbi_input_path = tmp_path / "quality_appraisal_template.csv"
            nos_output_path = tmp_path / "quality_appraisal_template_nos.csv"
            summary_output_path = tmp_path / "jbi_to_nos_conversion_summary.md"

            pd.DataFrame(
                [
                    {
                        "study_id": "S1",
                        "study_design": "cross_sectional",
                        "jbi_tool": "jbi_analytical_cross_sectional",
                        "item_01": "yes",
                        "item_02": "yes",
                        "item_03": "yes",
                        "item_04": "yes",
                        "item_05": "yes",
                        "item_06": "yes",
                        "item_07": "yes",
                        "item_08": "yes",
                        "item_09": "na",
                        "item_10": "na",
                        "item_11": "na",
                        "appraiser_id": "a1",
                        "checked_by": "a2",
                        "appraisal_notes": "note",
                    }
                ]
            ).to_csv(jbi_input_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(MODULE_PATH),
                    "--jbi-input",
                    str(jbi_input_path),
                    "--nos-output",
                    str(nos_output_path),
                    "--summary-output",
                    str(summary_output_path),
                ],
                cwd=MODULE_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(nos_output_path.exists())
            self.assertTrue(summary_output_path.exists())

            nos_df = pd.read_csv(nos_output_path, dtype=str)
            self.assertEqual(int(nos_df.shape[0]), 1)
            self.assertEqual(nos_df.iloc[0]["study_id"], "S1")
            self.assertEqual(nos_df.iloc[0]["overall_risk"], "low")

            summary_text = summary_output_path.read_text(encoding="utf-8")
            self.assertIn("JBI to NOS Conversion Summary", summary_text)


if __name__ == "__main__":
    unittest.main()
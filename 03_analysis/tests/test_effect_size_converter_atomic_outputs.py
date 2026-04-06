from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "effect_size_converter.py"


class EffectSizeConverterAtomicOutputsTests(unittest.TestCase):
    def test_writes_converted_and_summary_outputs_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            extraction_path = tmp_path / "extraction.csv"
            converted_output_path = tmp_path / "effect_size_converted.csv"
            summary_output_path = tmp_path / "effect_size_conversion_summary.md"

            pd.DataFrame(
                [
                    {
                        "study_id": "S1",
                        "first_author": "Doe",
                        "year": "2024",
                        "main_effect_metric": "d",
                        "main_effect_value": "0.5",
                        "effect_direction": "positive",
                        "adjusted_unadjusted": "adjusted",
                        "model_type": "linear",
                    }
                ]
            ).to_csv(extraction_path, index=False)

            converted_output_path.write_text("stale_output\n", encoding="utf-8")
            summary_output_path.write_text("stale_summary\n", encoding="utf-8")

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--input",
                    str(extraction_path),
                    "--output",
                    str(converted_output_path),
                    "--summary-output",
                    str(summary_output_path),
                    "--fail-on",
                    "none",
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(converted_output_path.exists())
            self.assertTrue(summary_output_path.exists())

            converted_df = pd.read_csv(converted_output_path, dtype=str)
            self.assertEqual(int(converted_df.shape[0]), 1)
            self.assertEqual(converted_df.iloc[0]["study_id"], "S1")
            self.assertEqual(converted_df.iloc[0]["converted_d"], "0.5")
            self.assertIn("study_id", converted_output_path.read_text(encoding="utf-8").splitlines()[0])

            summary_text = summary_output_path.read_text(encoding="utf-8")
            self.assertIn("Effect Size Conversion Summary", summary_text)
            self.assertNotIn("stale_summary", summary_text)

    def test_supports_legacy_effect_field_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            extraction_path = tmp_path / "extraction_legacy.csv"
            converted_output_path = tmp_path / "effect_size_converted.csv"
            summary_output_path = tmp_path / "effect_size_conversion_summary.md"

            pd.DataFrame(
                [
                    {
                        "study_id": "S2",
                        "author": "Lopez",
                        "year": "2023",
                        "effect_measure": "r",
                        "effect_value": "-0.40",
                        "effect_direction": "negative",
                        "adjustment_status": "unadjusted",
                        "analysis_model": "correlation",
                    }
                ]
            ).to_csv(extraction_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--input",
                    str(extraction_path),
                    "--output",
                    str(converted_output_path),
                    "--summary-output",
                    str(summary_output_path),
                    "--fail-on",
                    "none",
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            converted_df = pd.read_csv(converted_output_path, dtype=str)
            self.assertEqual(int(converted_df.shape[0]), 1)
            self.assertEqual(converted_df.iloc[0]["study_id"], "S2")
            self.assertEqual(converted_df.iloc[0]["first_author"], "Lopez")
            self.assertEqual(converted_df.iloc[0]["main_effect_metric"], "r")
            self.assertEqual(converted_df.iloc[0]["main_effect_value"], "-0.40")
            self.assertEqual(converted_df.iloc[0]["adjusted_unadjusted"], "unadjusted")
            self.assertEqual(converted_df.iloc[0]["model_type"], "correlation")
            self.assertEqual(converted_df.iloc[0]["conversion_status"], "converted")

    def test_flags_ci_unit_mismatch_against_main_effect_metric(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            extraction_path = tmp_path / "extraction_ci_mismatch.csv"
            converted_output_path = tmp_path / "effect_size_converted.csv"
            summary_output_path = tmp_path / "effect_size_conversion_summary.md"

            pd.DataFrame(
                [
                    {
                        "study_id": "S3",
                        "first_author": "Kim",
                        "year": "2024",
                        "main_effect_metric": "r",
                        "main_effect_value": "0.40",
                        "ci_lower": "-2.0",
                        "ci_upper": "0.70",
                        "effect_direction": "positive",
                        "adjusted_unadjusted": "adjusted",
                        "model_type": "linear",
                    }
                ]
            ).to_csv(extraction_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--input",
                    str(extraction_path),
                    "--output",
                    str(converted_output_path),
                    "--summary-output",
                    str(summary_output_path),
                    "--fail-on",
                    "error",
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 1)

            converted_df = pd.read_csv(converted_output_path, dtype=str)
            self.assertEqual(int(converted_df.shape[0]), 1)
            self.assertEqual(converted_df.iloc[0]["conversion_status"], "error")
            self.assertIn(
                "CI bounds are inconsistent with main_effect_metric scale",
                converted_df.iloc[0]["conversion_notes"],
            )

            summary_text = summary_output_path.read_text(encoding="utf-8")
            self.assertIn("column: `ci_lower`", summary_text)
            self.assertIn("`r` is out of valid range [-1, 1].", summary_text)


if __name__ == "__main__":
    unittest.main()
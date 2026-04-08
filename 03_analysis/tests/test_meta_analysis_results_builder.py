from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "meta_analysis_results_builder.py"


class MetaAnalysisResultsBuilderTests(unittest.TestCase):
    def test_builds_group_level_results_for_outcome_construct(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            converted_path = tmp_path / "effect_size_converted.csv"
            extraction_path = tmp_path / "extraction_template.csv"
            output_path = tmp_path / "meta_analysis_results.csv"
            summary_path = tmp_path / "meta_analysis_results_summary.md"
            trace_path = tmp_path / "analysis_trace.json"

            pd.DataFrame(
                [
                    {
                        "row": "2",
                        "study_id": "S1",
                        "source_metric_canonical": "d",
                        "effect_direction": "positive",
                        "conversion_status": "converted",
                        "converted_d": "0.20",
                    },
                    {
                        "row": "3",
                        "study_id": "S2",
                        "source_metric_canonical": "d",
                        "effect_direction": "positive",
                        "conversion_status": "converted",
                        "converted_d": "0.40",
                    },
                ]
            ).to_csv(converted_path, index=False)

            pd.DataFrame(
                [
                    {
                        "study_id": "S1",
                        "outcome_construct": "confidence",
                        "ci_lower": "0.00",
                        "ci_upper": "0.40",
                        "sample_size": "120",
                        "effect_direction": "positive",
                    },
                    {
                        "study_id": "S2",
                        "outcome_construct": "confidence",
                        "ci_lower": "0.20",
                        "ci_upper": "0.60",
                        "sample_size": "120",
                        "effect_direction": "positive",
                    },
                ]
            ).to_csv(extraction_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--converted-input",
                    str(converted_path),
                    "--extraction-input",
                    str(extraction_path),
                    "--metric",
                    "converted_d",
                    "--model",
                    "random_effects",
                    "--output",
                    str(output_path),
                    "--summary-output",
                    str(summary_path),
                    "--trace-output",
                    str(trace_path),
                    "--fail-on",
                    "none",
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(output_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertTrue(trace_path.exists())

            output_df = pd.read_csv(output_path, dtype=str)
            self.assertEqual(int(output_df.shape[0]), 1)
            self.assertEqual(output_df.iloc[0]["outcome"], "confidence")
            self.assertEqual(output_df.iloc[0]["k_studies"], "2")
            self.assertEqual(output_df.iloc[0]["model"], "random_effects")

            pooled_effect = float(output_df.iloc[0]["pooled_effect"])
            self.assertGreater(pooled_effect, 0.25)
            self.assertLess(pooled_effect, 0.35)

            summary_text = summary_path.read_text(encoding="utf-8")
            self.assertIn("Meta-Analysis Results Summary", summary_text)
            self.assertIn("confidence", summary_text)

            trace_payload = json.loads(trace_path.read_text(encoding="utf-8"))
            self.assertIn("outcome_1", trace_payload)
            self.assertEqual(trace_payload["outcome_1"]["outcome"], "confidence")
            self.assertEqual(trace_payload["outcome_1"]["studies"], ["S1", "S2"])
            self.assertEqual(trace_payload["outcome_1"]["excluded"], [])

    def test_uses_or_scale_in_output_and_approximates_se_from_sample_size(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            converted_path = tmp_path / "effect_size_converted.csv"
            extraction_path = tmp_path / "extraction_template.csv"
            output_path = tmp_path / "meta_analysis_results.csv"
            summary_path = tmp_path / "meta_analysis_results_summary.md"
            trace_path = tmp_path / "analysis_trace.json"

            pd.DataFrame(
                [
                    {
                        "row": "2",
                        "study_id": "S3",
                        "source_metric_canonical": "or",
                        "effect_direction": "positive",
                        "conversion_status": "converted",
                        "converted_or": "2.00",
                    }
                ]
            ).to_csv(converted_path, index=False)

            pd.DataFrame(
                [
                    {
                        "study_id": "S3",
                        "outcome_construct": "body_image",
                        "ci_lower": "",
                        "ci_upper": "",
                        "sample_size": "100",
                        "effect_direction": "positive",
                    }
                ]
            ).to_csv(extraction_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--converted-input",
                    str(converted_path),
                    "--extraction-input",
                    str(extraction_path),
                    "--metric",
                    "converted_or",
                    "--model",
                    "fixed_effects",
                    "--output",
                    str(output_path),
                    "--summary-output",
                    str(summary_path),
                    "--trace-output",
                    str(trace_path),
                    "--fail-on",
                    "none",
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)

            output_df = pd.read_csv(output_path, dtype=str)
            self.assertEqual(int(output_df.shape[0]), 1)
            self.assertEqual(output_df.iloc[0]["outcome"], "body_image")
            self.assertEqual(output_df.iloc[0]["k_studies"], "1")
            self.assertEqual(output_df.iloc[0]["model"], "fixed_effects")

            pooled_effect = float(output_df.iloc[0]["pooled_effect"])
            self.assertGreater(pooled_effect, 1.9)
            self.assertLess(pooled_effect, 2.1)

            trace_payload = json.loads(trace_path.read_text(encoding="utf-8"))
            self.assertEqual(trace_payload["outcome_1"]["studies"], ["S3"])

    def test_analysis_trace_reports_excluded_study_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            converted_path = tmp_path / "effect_size_converted.csv"
            extraction_path = tmp_path / "extraction_template.csv"
            output_path = tmp_path / "meta_analysis_results.csv"
            summary_path = tmp_path / "meta_analysis_results_summary.md"
            trace_path = tmp_path / "analysis_trace.json"

            pd.DataFrame(
                [
                    {
                        "row": "2",
                        "study_id": "S1",
                        "source_metric_canonical": "d",
                        "effect_direction": "positive",
                        "conversion_status": "converted",
                        "converted_d": "0.20",
                    },
                    {
                        "row": "3",
                        "study_id": "S2",
                        "source_metric_canonical": "d",
                        "effect_direction": "positive",
                        "conversion_status": "converted",
                        "converted_d": "0.40",
                    },
                ]
            ).to_csv(converted_path, index=False)

            pd.DataFrame(
                [
                    {
                        "study_id": "S1",
                        "outcome_construct": "confidence",
                        "ci_lower": "0.00",
                        "ci_upper": "0.40",
                        "sample_size": "120",
                        "effect_direction": "positive",
                        "included_in_meta": "yes",
                        "exclusion_reason": "included_primary",
                    },
                    {
                        "study_id": "S2",
                        "outcome_construct": "confidence",
                        "ci_lower": "",
                        "ci_upper": "",
                        "sample_size": "",
                        "effect_direction": "positive",
                        "included_in_meta": "no",
                        "exclusion_reason": "missing variance",
                    },
                ]
            ).to_csv(extraction_path, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--converted-input",
                    str(converted_path),
                    "--extraction-input",
                    str(extraction_path),
                    "--metric",
                    "converted_d",
                    "--model",
                    "random_effects",
                    "--output",
                    str(output_path),
                    "--summary-output",
                    str(summary_path),
                    "--trace-output",
                    str(trace_path),
                    "--fail-on",
                    "none",
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)

            trace_payload = json.loads(trace_path.read_text(encoding="utf-8"))
            outcome_trace = trace_payload["outcome_1"]
            self.assertEqual(outcome_trace["outcome"], "confidence")
            self.assertEqual(outcome_trace["studies"], ["S1"])
            self.assertEqual(outcome_trace["excluded"], ["S2"])
            self.assertEqual(outcome_trace["reason_excluded"], "missing_variance")
            self.assertEqual(outcome_trace["reason_excluded_by_study"]["S2"], "missing_variance")


if __name__ == "__main__":
    unittest.main()

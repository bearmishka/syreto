import importlib.util
import sys
import unittest
from pathlib import Path

import pandas as pd

MODULE_PATH = Path(__file__).resolve().parents[1] / "validate_extraction.py"
spec = importlib.util.spec_from_file_location("validate_extraction", MODULE_PATH)
validate_extraction = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = validate_extraction
assert spec.loader is not None
spec.loader.exec_module(validate_extraction)


class ValidateExtractionHarmonizationTests(unittest.TestCase):
    def test_harmonize_columns_does_not_add_legacy_mirror_columns(self) -> None:
        row = {column: "" for column in validate_extraction.REQUIRED_COLUMNS}
        row["study_id"] = "S1"
        row["first_author"] = "Smith"
        row["main_effect_metric"] = "d"
        row["main_effect_value"] = "0.25"

        harmonized = validate_extraction.harmonize_columns(pd.DataFrame([row]))

        self.assertNotIn("author", harmonized.columns)
        self.assertNotIn("effect_measure", harmonized.columns)

        missing, extra = validate_extraction.validate_schema(harmonized)
        self.assertEqual(missing, [])
        self.assertEqual(extra, [])

    def test_harmonize_columns_maps_legacy_aliases_and_parses_ci(self) -> None:
        legacy_df = pd.DataFrame(
            [
                {
                    "study_id": "S2",
                    "author": "Garcia",
                    "effect_measure": "r",
                    "effect_value": "-0.20",
                    "confidence_interval": "[-0.40, -0.05]",
                    "risk_of_bias": "high concern",
                }
            ]
        )

        harmonized = validate_extraction.harmonize_columns(legacy_df)

        self.assertEqual(harmonized.loc[0, "first_author"], "Garcia")
        self.assertEqual(harmonized.loc[0, "main_effect_metric"], "r")
        self.assertEqual(harmonized.loc[0, "main_effect_value"], "-0.20")
        self.assertEqual(harmonized.loc[0, "quality_appraisal"], "high concern")
        self.assertEqual(harmonized.loc[0, "ci_lower"], "-0.4")
        self.assertEqual(harmonized.loc[0, "ci_upper"], "-0.05")

    def test_validate_schema_ignores_known_compatibility_columns(self) -> None:
        row = {column: "" for column in validate_extraction.REQUIRED_COLUMNS}
        row["study_id"] = "S3"
        row["source_id"] = "SRC3"
        row["included_in_meta"] = "yes"
        row["included_in_bias"] = "yes"
        row["included_in_grade"] = "yes"
        row["exclusion_reason"] = "included_primary"
        row["decision_justification"] = (
            "Included: complete effect-size data available for synthesis workflows."
        )
        row["effect_measure"] = "d"
        row["effect_value"] = "0.33"
        row["confidence_interval"] = "[0.11, 0.55]"

        dataframe = pd.DataFrame([row])
        missing, extra = validate_extraction.validate_schema(dataframe)

        self.assertEqual(missing, [])
        self.assertEqual(extra, [])

    def test_validate_rows_requires_binary_inclusion_flags(self) -> None:
        row = {column: "" for column in validate_extraction.REQUIRED_COLUMNS}
        row["study_id"] = "S4"
        row["source_id"] = "SRC4"
        row["included_in_meta"] = "maybe"
        row["included_in_bias"] = "yes"
        row["included_in_grade"] = ""
        row["exclusion_reason"] = "included_primary"
        row["decision_justification"] = "Included rationale recorded for reviewer traceability."

        issues = validate_extraction.validate_rows(pd.DataFrame([row]))

        self.assertTrue(
            any(
                issue["column"] == "included_in_meta" and issue["level"] == "error"
                for issue in issues
            )
        )
        self.assertTrue(
            any(
                issue["column"] == "included_in_grade" and issue["level"] == "error"
                for issue in issues
            )
        )

    def test_validate_rows_requires_transparency_reason_and_justification(self) -> None:
        row = {column: "" for column in validate_extraction.REQUIRED_COLUMNS}
        row["study_id"] = "S5"
        row["source_id"] = "SRC5"
        row["included_in_meta"] = "yes"
        row["included_in_bias"] = "yes"
        row["included_in_grade"] = "yes"
        row["exclusion_reason"] = "unknown_reason"
        row["decision_justification"] = "short"

        issues = validate_extraction.validate_rows(pd.DataFrame([row]))

        self.assertTrue(
            any(
                issue["column"] == "exclusion_reason" and issue["level"] == "error"
                for issue in issues
            )
        )
        self.assertTrue(
            any(
                issue["column"] == "decision_justification" and issue["level"] == "warning"
                for issue in issues
            )
        )

    def test_validate_rows_flags_ci_metric_scale_mismatch(self) -> None:
        row = {column: "" for column in validate_extraction.REQUIRED_COLUMNS}
        row["study_id"] = "S6"
        row["source_id"] = "SRC6"
        row["included_in_meta"] = "yes"
        row["included_in_bias"] = "yes"
        row["included_in_grade"] = "yes"
        row["exclusion_reason"] = "included_primary"
        row["decision_justification"] = (
            "Included: effect inputs available and scoped for synthesis."
        )
        row["main_effect_metric"] = "r"
        row["main_effect_value"] = "0.40"
        row["ci_lower"] = "-1.20"
        row["ci_upper"] = "0.70"
        row["effect_direction"] = "positive"
        row["adjusted_unadjusted"] = "adjusted"
        row["model_type"] = "linear"

        issues = validate_extraction.validate_rows(pd.DataFrame([row]))

        self.assertTrue(
            any(
                issue["column"] == "ci_lower"
                and issue["level"] == "error"
                and "[-1, 1]" in issue["message"]
                for issue in issues
            )
        )

    def test_validate_rows_flags_unsupported_main_effect_metric(self) -> None:
        row = {column: "" for column in validate_extraction.REQUIRED_COLUMNS}
        row["study_id"] = "S7"
        row["source_id"] = "SRC7"
        row["included_in_meta"] = "yes"
        row["included_in_bias"] = "yes"
        row["included_in_grade"] = "yes"
        row["exclusion_reason"] = "included_primary"
        row["decision_justification"] = (
            "Included: effect inputs available and scoped for synthesis."
        )
        row["main_effect_metric"] = "hazard_ratio"
        row["main_effect_value"] = "1.20"
        row["effect_direction"] = "positive"
        row["adjusted_unadjusted"] = "adjusted"
        row["model_type"] = "cox"

        issues = validate_extraction.validate_rows(pd.DataFrame([row]))

        self.assertTrue(
            any(
                issue["column"] == "main_effect_metric"
                and issue["level"] == "error"
                and "Unsupported main_effect_metric" in issue["message"]
                for issue in issues
            )
        )


if __name__ == "__main__":
    unittest.main()

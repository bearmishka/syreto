import importlib.util
from pathlib import Path
import sys
import unittest

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "grade_evidence_profiler.py"
spec = importlib.util.spec_from_file_location("grade_evidence_profiler", MODULE_PATH)
grade_evidence_profiler = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = grade_evidence_profiler
assert spec.loader is not None
spec.loader.exec_module(grade_evidence_profiler)


class GradeEvidenceProfilerTests(unittest.TestCase):
    def test_baseline_points_from_design(self) -> None:
        self.assertEqual(
            grade_evidence_profiler.baseline_points_from_design("randomized clinical trial"), 4
        )
        self.assertEqual(
            grade_evidence_profiler.baseline_points_from_design("prospective cohort"), 2
        )
        self.assertEqual(grade_evidence_profiler.baseline_points_from_design("case report"), 1)

    def test_inconsistency_downgrade_for_direction_divergence(self) -> None:
        group_stats = {
            "p||o": {
                "n_valid": 4,
                "dominant_direction": "positive",
                "dominant_share": 0.75,
            }
        }
        row = pd.Series(
            {
                "predictor_construct": "p",
                "outcome_construct": "o",
                "effect_direction": "negative",
            }
        )
        downgrade, label, note = grade_evidence_profiler.inconsistency_downgrade(row, group_stats)
        self.assertEqual(downgrade, 1)
        self.assertEqual(label, "serious")
        self.assertIn("diverges", note)

    def test_build_profile_uses_quality_band_and_domains(self) -> None:
        extraction_df = pd.DataFrame(
            [
                {
                    "study_id": "S1",
                    "study_design": "randomized trial",
                    "predictor_construct": "predictor_a",
                    "outcome_construct": "outcome_x",
                    "sample_size": "220",
                    "effect_direction": "positive",
                    "main_effect_value": "0.5",
                    "main_effect_metric": "SMD",
                    "ci_lower": "0.1",
                    "ci_upper": "0.9",
                    "p_value": "0.01",
                    "condition_diagnostic_method": "clinical interview",
                    "condition_diagnostic_system": "DSM-5",
                    "condition_definition": "diagnosed",
                    "predictor_instrument_type": "questionnaire",
                    "predictor_instrument_name": "Scale A",
                    "outcome_measure": "Scale B",
                    "consensus_status": "include",
                    "quality_appraisal": "",
                },
                {
                    "study_id": "S2",
                    "study_design": "cohort",
                    "predictor_construct": "predictor_a",
                    "outcome_construct": "outcome_x",
                    "sample_size": "40",
                    "effect_direction": "negative",
                    "main_effect_value": "",
                    "main_effect_metric": "",
                    "ci_lower": "",
                    "ci_upper": "",
                    "p_value": "",
                    "condition_diagnostic_method": "",
                    "condition_diagnostic_system": "",
                    "condition_definition": "",
                    "predictor_instrument_type": "",
                    "predictor_instrument_name": "",
                    "outcome_measure": "",
                    "consensus_status": "include",
                    "quality_appraisal": "",
                },
            ]
        )
        quality_df = pd.DataFrame(
            [
                {"study_id": "S1", "quality_band": "high", "score_pct": "82.0"},
                {"study_id": "S2", "quality_band": "low", "score_pct": "44.0"},
            ]
        )

        profile = grade_evidence_profiler.build_profile(extraction_df, quality_df)
        self.assertEqual(profile.shape[0], 2)

        row_s1 = profile.loc[profile["study_id"] == "S1"].iloc[0]
        row_s2 = profile.loc[profile["study_id"] == "S2"].iloc[0]

        self.assertEqual(row_s1["risk_of_bias"], "not serious")
        self.assertEqual(row_s1["quality_band"], "high")

        self.assertEqual(row_s2["risk_of_bias"], "very serious")
        self.assertEqual(row_s2["quality_band"], "low")
        self.assertIn(row_s2["overall_certainty"], {"very low", "low"})

    def test_render_latex_table_has_expected_caption(self) -> None:
        profile_df = pd.DataFrame(
            [
                {
                    "study_id": "S1",
                    "study_design": "cohort",
                    "risk_of_bias": "serious",
                    "inconsistency": "not serious",
                    "indirectness": "serious",
                    "imprecision": "serious",
                    "overall_certainty": "low",
                }
            ]
        )
        latex = grade_evidence_profiler.render_latex_table(
            profile_df,
            extraction_path=Path("../02_data/codebook/extraction_template.csv"),
        )
        self.assertIn("GRADE evidence profile by included study", latex)
        self.assertIn("tab:grade_evidence_profile", latex)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import filecmp
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = PROJECT_ROOT / "syreto"
LEGACY_ROOT = PROJECT_ROOT / "03_analysis"

EXPECTED_CODE_DIFFS = {
    "csv_schema.py",
    "effect_size_converter.py",
    "export_to_ris.py",
    "forest_plot_generator.py",
    "grade_evidence_profiler.py",
    "progress_history_builder.py",
    "prospero_manual_fields_check.py",
    "prospero_submission_drafter.py",
    "provenance.py",
    "quality_appraisal.py",
    "results_summary_table_builder.py",
    "status_cli.py",
    "status_report.py",
    "template_term_guard.py",
    "todo_action_plan_builder.py",
    "validate_csv_inputs.py",
}

EXPECTED_LAYER_DIFFS = {
    "prospero_submission_drafter_layers/__init__.py",
    "prospero_submission_drafter_layers/builder.py",
    "prospero_submission_drafter_layers/field_composition.py",
    "prospero_submission_drafter_layers/formatting.py",
    "prospero_submission_drafter_layers/io.py",
    "prospero_submission_drafter_layers/models.py",
    "prospero_submission_drafter_layers/rules.py",
}

EXPECTED_TEST_DIFFS = {
    "test_prospero_submission_drafter.py",
    "test_python_source_integrity.py",
    "test_validate_csv_inputs_none_handling.py",
}


def differing_python_files(
    left_root: Path,
    right_root: Path,
    *,
    recursive: bool = True,
) -> set[str]:
    left_iter = left_root.rglob("*.py") if recursive else left_root.glob("*.py")
    right_iter = right_root.rglob("*.py") if recursive else right_root.glob("*.py")
    left = {path.relative_to(left_root).as_posix(): path for path in left_iter}
    right = {path.relative_to(right_root).as_posix(): path for path in right_iter}
    return {
        name
        for name in sorted(set(left) & set(right))
        if not filecmp.cmp(left[name], right[name], shallow=False)
    }


class RepositoryBoundaryIntegrityTests(unittest.TestCase):
    def test_only_expected_code_diffs_remain_between_package_and_legacy_trees(self) -> None:
        differing = differing_python_files(LEGACY_ROOT, PACKAGE_ROOT, recursive=False)
        self.assertEqual(differing, EXPECTED_CODE_DIFFS)

    def test_only_expected_prospero_layer_diffs_remain(self) -> None:
        differing = differing_python_files(
            LEGACY_ROOT / "prospero_submission_drafter_layers",
            PACKAGE_ROOT / "prospero_submission_drafter_layers",
        )
        normalized = {f"prospero_submission_drafter_layers/{name}" for name in differing}
        self.assertEqual(normalized, EXPECTED_LAYER_DIFFS)

    def test_only_expected_test_diffs_remain_between_package_and_legacy_tests(self) -> None:
        differing = differing_python_files(LEGACY_ROOT / "tests", PACKAGE_ROOT / "tests")
        self.assertEqual(differing, EXPECTED_TEST_DIFFS)

    def test_legacy_top_level_diffs_are_explicit_compatibility_surfaces(self) -> None:
        for relative_path in sorted(EXPECTED_CODE_DIFFS):
            if "/" in relative_path:
                continue

            legacy_text = (LEGACY_ROOT / relative_path).read_text(encoding="utf-8")
            self.assertTrue(
                "Legacy entrypoint shim" in legacy_text
                or "Compatibility wrapper" in legacy_text
                or "from syreto." in legacy_text,
                msg=f"{relative_path} no longer looks like an explicit compatibility surface",
            )

    def test_legacy_prospero_layer_diffs_are_explicit_compatibility_wrappers(self) -> None:
        for relative_path in sorted(EXPECTED_LAYER_DIFFS):
            if not relative_path.startswith("prospero_submission_drafter_layers/"):
                continue

            legacy_text = (LEGACY_ROOT / relative_path).read_text(encoding="utf-8")
            self.assertTrue(
                "Compatibility wrapper" in legacy_text or "Compatibility exports" in legacy_text
            )
            self.assertIn("syreto.prospero_submission_drafter_layers", legacy_text)


if __name__ == "__main__":
    unittest.main()

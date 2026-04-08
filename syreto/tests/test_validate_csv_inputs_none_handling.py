import importlib.util
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd

MODULE_PATH = Path(__file__).resolve().parents[1] / "validate_csv_inputs.py"
spec = importlib.util.spec_from_file_location("validate_csv_inputs", MODULE_PATH)
validate_csv_inputs = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(validate_csv_inputs)


class ValidateCsvInputsNoneHandlingTests(unittest.TestCase):
    def _collect_export_filename_issues(
        self,
        *,
        export_filename: str,
        results_exported: str,
        template_info: bool,
        create_raw_dir: bool,
    ) -> list[dict]:
        with tempfile.TemporaryDirectory() as tmp_dir:
            raw_dir = Path(tmp_dir) / "raw"
            if create_raw_dir:
                raw_dir.mkdir(parents=True, exist_ok=True)

            dataframe = pd.DataFrame(
                [
                    {
                        "export_filename": export_filename,
                        "results_exported": results_exported,
                    }
                ]
            )
            issues: list[dict] = []
            validate_csv_inputs.validate_search_log_export_files(
                dataframe,
                file_name="search_log",
                raw_exports_dir=raw_dir,
                issues=issues,
                template_filename_as_info=template_info,
            )
        return issues

    def test_normalize_none_returns_empty_string(self) -> None:
        self.assertEqual(validate_csv_inputs.normalize(None), "")

    def test_is_empty_treats_none_variants_as_empty(self) -> None:
        self.assertTrue(validate_csv_inputs.is_empty(None))
        self.assertTrue(validate_csv_inputs.is_empty("None"))
        self.assertTrue(validate_csv_inputs.is_empty(" none "))

    def test_is_empty_keeps_non_empty_text_non_empty(self) -> None:
        self.assertFalse(validate_csv_inputs.is_empty("bulimia"))

    def test_placeholder_filename_is_info_when_not_exported(self) -> None:
        issues = self._collect_export_filename_issues(
            export_filename="pubmed_YYYY-MM-DD.ris",
            results_exported="",
            template_info=True,
            create_raw_dir=True,
        )

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["level"], "info")

    def test_placeholder_filename_is_warning_when_info_mode_disabled(self) -> None:
        issues = self._collect_export_filename_issues(
            export_filename="pubmed_YYYY-MM-DD.ris",
            results_exported="",
            template_info=False,
            create_raw_dir=True,
        )

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["level"], "warning")

    def test_non_placeholder_missing_file_stays_warning(self) -> None:
        issues = self._collect_export_filename_issues(
            export_filename="pubmed_2026-03-13.ris",
            results_exported="",
            template_info=True,
            create_raw_dir=True,
        )

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["level"], "warning")

    def test_placeholder_filename_is_error_when_exported_count_positive(self) -> None:
        issues = self._collect_export_filename_issues(
            export_filename="pubmed_YYYY-MM-DD.ris",
            results_exported="5",
            template_info=True,
            create_raw_dir=True,
        )

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["level"], "error")

    def test_missing_raw_dir_with_only_placeholder_filename_is_info(self) -> None:
        issues = self._collect_export_filename_issues(
            export_filename="pubmed_YYYY-MM-DD.ris",
            results_exported="",
            template_info=True,
            create_raw_dir=False,
        )

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["level"], "info")

    def test_prisma_cross_file_consistency_accepts_aligned_counts(self) -> None:
        search_df = pd.DataFrame(
            [
                {"results_total": "6"},
                {"results_total": "4"},
            ]
        )
        screening_df = pd.DataFrame(
            [
                {"stage": "title_abstract", "records_screened": "8", "include_n": "3"},
                {"stage": "full_text", "records_screened": "3", "include_n": "2"},
            ]
        )
        prisma_df = pd.DataFrame(
            [
                {"stage": "records_identified_databases", "count": "10"},
                {"stage": "duplicates_removed", "count": "2"},
                {"stage": "records_screened_title_abstract", "count": "8"},
                {"stage": "studies_included_qualitative_synthesis", "count": "2"},
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_prisma_cross_file_consistency(
            search_df=search_df,
            screening_df=screening_df,
            prisma_df=prisma_df,
            issues=issues,
        )

        self.assertEqual(issues, [])

    def test_prisma_cross_file_consistency_flags_screened_mismatch(self) -> None:
        search_df = pd.DataFrame([{"results_total": "5"}])
        screening_df = pd.DataFrame(
            [
                {"stage": "title_abstract", "records_screened": "4", "include_n": "1"},
                {"stage": "full_text", "records_screened": "1", "include_n": "1"},
            ]
        )
        prisma_df = pd.DataFrame(
            [
                {"stage": "records_identified_databases", "count": "5"},
                {"stage": "duplicates_removed", "count": "0"},
                {"stage": "records_screened_title_abstract", "count": "5"},
                {"stage": "studies_included_qualitative_synthesis", "count": "1"},
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_prisma_cross_file_consistency(
            search_df=search_df,
            screening_df=screening_df,
            prisma_df=prisma_df,
            issues=issues,
        )

        screened_issues = [
            issue
            for issue in issues
            if issue["file"] == "screening_daily_log"
            and issue["column"] == "records_screened"
            and issue["level"] == "error"
        ]
        self.assertEqual(len(screened_issues), 1)

    def test_prisma_cross_file_consistency_flags_included_mismatch(self) -> None:
        search_df = pd.DataFrame([{"results_total": "5"}])
        screening_df = pd.DataFrame(
            [
                {"stage": "title_abstract", "records_screened": "5", "include_n": "2"},
                {"stage": "full_text", "records_screened": "2", "include_n": "1"},
            ]
        )
        prisma_df = pd.DataFrame(
            [
                {"stage": "records_identified_databases", "count": "5"},
                {"stage": "duplicates_removed", "count": "0"},
                {"stage": "records_screened_title_abstract", "count": "5"},
                {"stage": "studies_included_qualitative_synthesis", "count": "2"},
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_prisma_cross_file_consistency(
            search_df=search_df,
            screening_df=screening_df,
            prisma_df=prisma_df,
            issues=issues,
        )

        included_issues = [
            issue
            for issue in issues
            if issue["file"] == "screening_daily_log"
            and issue["column"] == "include_n"
            and issue["level"] == "error"
        ]
        self.assertEqual(len(included_issues), 1)

    def test_prisma_cross_file_consistency_accepts_aligned_screening_fulltext_log(self) -> None:
        search_df = pd.DataFrame([{"results_total": "5"}])
        screening_df = pd.DataFrame(
            [
                {"stage": "title_abstract", "records_screened": "5", "include_n": "2"},
                {"stage": "full_text", "records_screened": "2", "include_n": "1"},
            ]
        )
        fulltext_df = pd.DataFrame(
            [
                {
                    "record_id": "R1",
                    "fulltext_available": "yes",
                    "include": "include",
                    "exclusion_reason": "",
                },
                {
                    "record_id": "R2",
                    "fulltext_available": "yes",
                    "include": "exclude",
                    "exclusion_reason": "wrong outcome",
                },
                {
                    "record_id": "R3",
                    "fulltext_available": "no",
                    "include": "",
                    "exclusion_reason": "full text unavailable",
                },
            ]
        )
        prisma_df = pd.DataFrame(
            [
                {"stage": "records_identified_databases", "count": "5"},
                {"stage": "duplicates_removed", "count": "0"},
                {"stage": "records_screened_title_abstract", "count": "5"},
                {"stage": "reports_sought_for_retrieval", "count": "3"},
                {"stage": "reports_not_retrieved", "count": "1"},
                {"stage": "reports_assessed_full_text", "count": "2"},
                {"stage": "reports_excluded_full_text", "count": "1"},
                {"stage": "studies_included_qualitative_synthesis", "count": "1"},
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_prisma_cross_file_consistency(
            search_df=search_df,
            screening_df=screening_df,
            prisma_df=prisma_df,
            fulltext_df=fulltext_df,
            issues=issues,
        )

        self.assertEqual(issues, [])

    def test_prisma_cross_file_consistency_flags_fulltext_log_prisma_mismatch(self) -> None:
        search_df = pd.DataFrame([{"results_total": "5"}])
        screening_df = pd.DataFrame(
            [
                {"stage": "title_abstract", "records_screened": "5", "include_n": "2"},
                {"stage": "full_text", "records_screened": "2", "include_n": "1"},
            ]
        )
        fulltext_df = pd.DataFrame(
            [
                {
                    "record_id": "R1",
                    "fulltext_available": "yes",
                    "include": "include",
                    "exclusion_reason": "",
                },
                {
                    "record_id": "R2",
                    "fulltext_available": "yes",
                    "include": "exclude",
                    "exclusion_reason": "wrong outcome",
                },
            ]
        )
        prisma_df = pd.DataFrame(
            [
                {"stage": "records_identified_databases", "count": "5"},
                {"stage": "duplicates_removed", "count": "0"},
                {"stage": "records_screened_title_abstract", "count": "5"},
                {"stage": "reports_assessed_full_text", "count": "2"},
                {"stage": "reports_excluded_full_text", "count": "0"},
                {"stage": "studies_included_qualitative_synthesis", "count": "1"},
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_prisma_cross_file_consistency(
            search_df=search_df,
            screening_df=screening_df,
            prisma_df=prisma_df,
            fulltext_df=fulltext_df,
            issues=issues,
        )

        mismatch_issues = [
            issue
            for issue in issues
            if issue["file"] == "screening_fulltext_log"
            and issue["level"] == "error"
            and "reports_excluded_full_text" in issue["message"]
        ]
        self.assertEqual(len(mismatch_issues), 1)

    def test_title_abstract_results_rules_accept_consistent_row(self) -> None:
        dataframe = pd.DataFrame(
            [
                {
                    "reviewer1_decision": "include",
                    "reviewer2_decision": "include",
                    "conflict": "no",
                    "conflict_resolver": "",
                    "resolution_decision": "",
                    "final_decision": "include",
                    "exclusion_reason": "",
                }
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_title_abstract_results_rules(
            dataframe,
            file_name="screening_title_abstract_results",
            issues=issues,
        )

        self.assertEqual(issues, [])

    def test_title_abstract_results_rules_flag_incorrect_conflict(self) -> None:
        dataframe = pd.DataFrame(
            [
                {
                    "reviewer1_decision": "include",
                    "reviewer2_decision": "exclude",
                    "conflict": "no",
                    "conflict_resolver": "",
                    "resolution_decision": "",
                    "final_decision": "include",
                    "exclusion_reason": "",
                }
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_title_abstract_results_rules(
            dataframe,
            file_name="screening_title_abstract_results",
            issues=issues,
        )

        conflict_issues = [
            issue for issue in issues if issue["column"] == "conflict" and issue["level"] == "error"
        ]
        self.assertEqual(len(conflict_issues), 1)

    def test_title_abstract_results_rules_require_resolution_fields_on_conflict(self) -> None:
        dataframe = pd.DataFrame(
            [
                {
                    "reviewer1_decision": "include",
                    "reviewer2_decision": "exclude",
                    "conflict": "yes",
                    "conflict_resolver": "",
                    "resolution_decision": "",
                    "final_decision": "uncertain",
                    "exclusion_reason": "",
                }
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_title_abstract_results_rules(
            dataframe,
            file_name="screening_title_abstract_results",
            issues=issues,
        )

        missing_resolver = [
            issue
            for issue in issues
            if issue["column"] == "conflict_resolver" and issue["level"] == "error"
        ]
        missing_resolution = [
            issue
            for issue in issues
            if issue["column"] == "resolution_decision" and issue["level"] == "error"
        ]
        self.assertEqual(len(missing_resolver), 1)
        self.assertEqual(len(missing_resolution), 1)

    def test_title_abstract_results_rules_warn_when_final_mismatches_resolution(self) -> None:
        dataframe = pd.DataFrame(
            [
                {
                    "reviewer1_decision": "include",
                    "reviewer2_decision": "exclude",
                    "conflict": "yes",
                    "conflict_resolver": "ADJ1",
                    "resolution_decision": "include",
                    "final_decision": "exclude",
                    "exclusion_reason": "",
                }
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_title_abstract_results_rules(
            dataframe,
            file_name="screening_title_abstract_results",
            issues=issues,
        )

        mismatch_issues = [
            issue
            for issue in issues
            if issue["column"] == "final_decision"
            and issue["level"] == "warning"
            and "resolution_decision" in issue["message"]
        ]
        self.assertEqual(len(mismatch_issues), 1)

    def test_screening_fulltext_rules_warn_when_exclusion_reason_missing(self) -> None:
        dataframe = pd.DataFrame(
            [
                {
                    "record_id": "R1",
                    "fulltext_available": "yes",
                    "include": "exclude",
                    "exclusion_reason": "",
                    "reviewer": "EP",
                    "notes": "",
                }
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_screening_fulltext_rules(
            dataframe,
            file_name="screening_fulltext_log",
            issues=issues,
        )

        reason_issues = [
            issue
            for issue in issues
            if issue["column"] == "exclusion_reason" and issue["level"] == "warning"
        ]
        self.assertEqual(len(reason_issues), 1)

    def test_screening_fulltext_rules_flag_impossible_include_when_unavailable(self) -> None:
        dataframe = pd.DataFrame(
            [
                {
                    "record_id": "R2",
                    "fulltext_available": "no",
                    "include": "include",
                    "exclusion_reason": "",
                    "reviewer": "EP",
                    "notes": "",
                }
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_screening_fulltext_rules(
            dataframe,
            file_name="screening_fulltext_log",
            issues=issues,
        )

        impossible_issues = [
            issue for issue in issues if issue["column"] == "include" and issue["level"] == "error"
        ]
        self.assertEqual(len(impossible_issues), 1)

    def test_validate_date_columns_flags_future_and_unrealistic_past_dates(self) -> None:
        dataframe = pd.DataFrame(
            [
                {"date_searched": "1899-12-31"},
                {"date_searched": "2999-01-01"},
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_date_columns(
            dataframe,
            file_name="search_log",
            columns=["date_searched"],
            issues=issues,
        )

        past_issues = [issue for issue in issues if issue["level"] == "error" and issue["row"] == 2]
        future_issues = [
            issue for issue in issues if issue["level"] == "warning" and issue["row"] == 3
        ]
        self.assertEqual(len(past_issues), 1)
        self.assertEqual(len(future_issues), 1)

    def test_search_log_ranges_flag_temporal_and_export_count_mismatches(self) -> None:
        current_year = datetime.now().year
        dataframe = pd.DataFrame(
            [
                {
                    "start_year": str(current_year + 1),
                    "date_searched": "2026-03-01",
                    "end_date": "2026-03-15",
                    "results_total": "10",
                    "results_exported": "12",
                }
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_search_log_ranges(
            dataframe,
            file_name="search_log",
            issues=issues,
        )

        self.assertTrue(
            any(issue["column"] == "start_year" and issue["level"] == "error" for issue in issues)
        )
        self.assertTrue(
            any(issue["column"] == "end_date" and issue["level"] == "error" for issue in issues)
        )
        self.assertTrue(
            any(
                issue["column"] == "results_exported" and issue["level"] == "error"
                for issue in issues
            )
        )

    def test_screening_daily_ranges_flag_decision_math_mismatch(self) -> None:
        dataframe = pd.DataFrame(
            [
                {
                    "records_screened": "5",
                    "include_n": "2",
                    "exclude_n": "1",
                    "maybe_n": "1",
                    "pending_n": "0",
                    "time_spent_minutes": "0",
                }
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_screening_daily_ranges(
            dataframe,
            file_name="screening_daily_log",
            issues=issues,
        )

        self.assertTrue(
            any(
                issue["column"] == "records_screened" and issue["level"] == "error"
                for issue in issues
            )
        )
        self.assertTrue(
            any(
                issue["column"] == "time_spent_minutes" and issue["level"] == "warning"
                for issue in issues
            )
        )

    def test_master_records_rules_flag_duplicate_reference_errors(self) -> None:
        dataframe = pd.DataFrame(
            [
                {
                    "record_id": "R001",
                    "year": "2020",
                    "is_duplicate": "yes",
                    "duplicate_of_record_id": "",
                },
                {
                    "record_id": "R002",
                    "year": "2020",
                    "is_duplicate": "yes",
                    "duplicate_of_record_id": "R999",
                },
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_master_records_rules(
            dataframe,
            file_name="master_records",
            issues=issues,
        )

        missing_ref = [issue for issue in issues if issue["column"] == "duplicate_of_record_id"]
        self.assertGreaterEqual(len(missing_ref), 2)

    def test_master_records_rules_treat_nan_like_empty_duplicate_reference(self) -> None:
        dataframe = pd.DataFrame(
            [
                {
                    "record_id": "R001",
                    "year": "2020",
                    "is_duplicate": "no",
                    "duplicate_of_record_id": float("nan"),
                },
                {
                    "record_id": "R002",
                    "year": "2020",
                    "is_duplicate": "no",
                    "duplicate_of_record_id": "none",
                },
                {
                    "record_id": "R003",
                    "year": "2020",
                    "is_duplicate": "no",
                    "duplicate_of_record_id": "nan",
                },
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_master_records_rules(
            dataframe,
            file_name="master_records",
            issues=issues,
        )

        duplicate_ref_issues = [
            issue for issue in issues if issue["column"] == "duplicate_of_record_id"
        ]
        self.assertEqual(duplicate_ref_issues, [])

    def test_decision_log_rules_require_reason_for_exclusions(self) -> None:
        dataframe = pd.DataFrame(
            [
                {
                    "record_id": "MR00002",
                    "stage": "screening",
                    "decision": "exclude",
                    "reason": "",
                    "reviewer": "R1",
                }
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_decision_log_rules(
            dataframe,
            file_name="decision_log",
            issues=issues,
        )

        self.assertTrue(
            any(issue["column"] == "reason" and issue["level"] == "error" for issue in issues)
        )

    def test_decision_log_rules_warn_on_fulltext_maybe(self) -> None:
        dataframe = pd.DataFrame(
            [
                {
                    "record_id": "MR00003",
                    "stage": "full_text",
                    "decision": "maybe",
                    "reason": "insufficient details in abstract",
                    "reviewer": "R2",
                }
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_decision_log_rules(
            dataframe,
            file_name="decision_log",
            issues=issues,
        )

        self.assertTrue(
            any(issue["column"] == "decision" and issue["level"] == "warning" for issue in issues)
        )

    def test_decision_log_master_alignment_flags_unknown_record_ids(self) -> None:
        decision_log_df = pd.DataFrame(
            [
                {
                    "record_id": "MR99999",
                    "stage": "screening",
                    "decision": "exclude",
                    "reason": "wrong population",
                    "reviewer": "R1",
                }
            ]
        )
        master_df = pd.DataFrame(
            [
                {"record_id": "MR00001"},
                {"record_id": "MR00002"},
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_decision_log_master_alignment(
            decision_log_df=decision_log_df,
            master_df=master_df,
            issues=issues,
        )

        self.assertTrue(
            any(
                issue["file"] == "decision_log"
                and issue["column"] == "record_id"
                and issue["level"] == "warning"
                for issue in issues
            )
        )

    def test_prisma_cross_file_consistency_flags_title_abstract_and_reason_mismatches(self) -> None:
        search_df = pd.DataFrame([{"results_total": "5"}])
        screening_df = pd.DataFrame(
            [
                {"stage": "title_abstract", "records_screened": "5", "include_n": "2"},
                {"stage": "full_text", "records_screened": "2", "include_n": "1"},
            ]
        )
        title_abstract_results_df = pd.DataFrame(
            [
                {"record_id": "R1", "final_decision": "include"},
                {"record_id": "R2", "final_decision": "exclude"},
                {"record_id": "R3", "final_decision": "exclude"},
                {"record_id": "R4", "final_decision": "include"},
                {"record_id": "R5", "final_decision": "include"},
            ]
        )
        fulltext_df = pd.DataFrame(
            [
                {
                    "record_id": "R1",
                    "fulltext_available": "yes",
                    "include": "include",
                    "exclusion_reason": "",
                },
                {
                    "record_id": "R4",
                    "fulltext_available": "yes",
                    "include": "exclude",
                    "exclusion_reason": "wrong outcome",
                },
            ]
        )
        fulltext_reasons_df = pd.DataFrame(
            [
                {"reason": "No eligible outcome", "count": "3"},
            ]
        )
        master_df = pd.DataFrame(
            [
                {"record_id": "R1"},
                {"record_id": "R2"},
                {"record_id": "R3"},
            ]
        )
        prisma_df = pd.DataFrame(
            [
                {"stage": "records_identified_databases", "count": "5"},
                {"stage": "duplicates_removed", "count": "0"},
                {"stage": "records_screened_title_abstract", "count": "5"},
                {"stage": "records_excluded_title_abstract", "count": "1"},
                {"stage": "reports_sought_for_retrieval", "count": "2"},
                {"stage": "reports_not_retrieved", "count": "0"},
                {"stage": "reports_assessed_full_text", "count": "2"},
                {"stage": "reports_excluded_full_text", "count": "1"},
                {"stage": "studies_included_qualitative_synthesis", "count": "1"},
            ]
        )

        issues: list[dict] = []
        validate_csv_inputs.validate_prisma_cross_file_consistency(
            search_df=search_df,
            screening_df=screening_df,
            prisma_df=prisma_df,
            fulltext_df=fulltext_df,
            title_abstract_results_df=title_abstract_results_df,
            fulltext_reasons_df=fulltext_reasons_df,
            master_df=master_df,
            issues=issues,
        )

        self.assertTrue(
            any(
                issue["file"] == "screening_title_abstract_results"
                and issue["column"] == "final_decision"
                and issue["level"] == "error"
                for issue in issues
            )
        )
        self.assertTrue(
            any(
                issue["file"] == "full_text_exclusion_reasons"
                and issue["column"] == "count"
                and issue["level"] == "error"
                for issue in issues
            )
        )
        self.assertTrue(
            any(
                issue["file"] == "screening_title_abstract_results"
                and issue["column"] == "record_id"
                and issue["level"] == "warning"
                for issue in issues
            )
        )


if __name__ == "__main__":
    unittest.main()

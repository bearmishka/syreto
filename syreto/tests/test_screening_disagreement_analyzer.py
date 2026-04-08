import importlib.util
from pathlib import Path
import sys
import unittest

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "screening_disagreement_analyzer.py"
spec = importlib.util.spec_from_file_location("screening_disagreement_analyzer", MODULE_PATH)
screening_disagreement_analyzer = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = screening_disagreement_analyzer
assert spec.loader is not None
spec.loader.exec_module(screening_disagreement_analyzer)


class ScreeningDisagreementAnalyzerTests(unittest.TestCase):
    def test_classify_article_type_matches_expected_labels(self) -> None:
        label_review = screening_disagreement_analyzer.classify_article_type(
            title="Systematic review of eating outcomes",
            abstract="",
            journal="",
        )
        label_case = screening_disagreement_analyzer.classify_article_type(
            title="Case report: severe symptoms",
            abstract="",
            journal="",
        )
        label_unknown = screening_disagreement_analyzer.classify_article_type(
            title="Unstructured title",
            abstract="No recognizable publication-type cues.",
            journal="",
        )

        self.assertEqual(label_review, "review/meta-analysis")
        self.assertEqual(label_case, "case report/series")
        self.assertEqual(label_unknown, "other/unclear")

    def test_best_reviewer_pair_uses_maximum_overlap(self) -> None:
        dual_df = pd.DataFrame(
            [
                {"record_id": "R1", "reviewer": "A", "title_abstract_decision": "include"},
                {"record_id": "R1", "reviewer": "B", "title_abstract_decision": "include"},
                {"record_id": "R2", "reviewer": "A", "title_abstract_decision": "exclude"},
                {"record_id": "R2", "reviewer": "B", "title_abstract_decision": "exclude"},
                {"record_id": "R2", "reviewer": "C", "title_abstract_decision": "include"},
                {"record_id": "R3", "reviewer": "A", "title_abstract_decision": "maybe"},
                {"record_id": "R3", "reviewer": "C", "title_abstract_decision": "maybe"},
                {"record_id": "R4", "reviewer": "A", "title_abstract_decision": "exclude"},
                {"record_id": "R4", "reviewer": "C", "title_abstract_decision": "include"},
            ]
        )

        prepared = screening_disagreement_analyzer.prepare_dual_log(dual_df)
        pair = screening_disagreement_analyzer.best_reviewer_pair(prepared)
        self.assertEqual(pair, ("A", "C"))

    def test_disagreement_analysis_builds_type_pattern_table(self) -> None:
        dual_df = pd.DataFrame(
            [
                {"record_id": "R1", "reviewer": "EP", "title_abstract_decision": "include"},
                {"record_id": "R1", "reviewer": "IR", "title_abstract_decision": "include"},
                {"record_id": "R2", "reviewer": "EP", "title_abstract_decision": "include"},
                {"record_id": "R2", "reviewer": "IR", "title_abstract_decision": "exclude"},
                {"record_id": "R3", "reviewer": "EP", "title_abstract_decision": "maybe"},
                {"record_id": "R3", "reviewer": "IR", "title_abstract_decision": "exclude"},
            ]
        )
        master_df = pd.DataFrame(
            [
                {
                    "record_id": "R1",
                    "title": "Systematic review of outcomes",
                    "abstract": "",
                    "journal": "Journal A",
                    "source_database": "pubmed",
                    "year": "2021",
                },
                {
                    "record_id": "R2",
                    "title": "Case report of clinical course",
                    "abstract": "",
                    "journal": "Journal B",
                    "source_database": "embase",
                    "year": "2019",
                },
                {
                    "record_id": "R3",
                    "title": "Randomized clinical trial",
                    "abstract": "",
                    "journal": "Journal C",
                    "source_database": "scopus",
                    "year": "2020",
                },
            ]
        )

        analysis = screening_disagreement_analyzer.disagreement_analysis(
            dual_df,
            master_df,
            top_records=20,
        )

        self.assertTrue(analysis["available"])
        self.assertEqual(analysis["reviewer_pair"], "EP vs IR")
        self.assertEqual(analysis["paired_records"], 3)
        self.assertEqual(analysis["disagreements"], 2)

        type_patterns = analysis["type_patterns"]
        case_row = type_patterns.loc[type_patterns["article_type"] == "case report/series"].iloc[0]
        self.assertEqual(int(case_row["disagreements"]), 1)
        self.assertEqual(case_row["disagreement_rate"], "100.0%")

    def test_markdown_report_contains_calibration_table_heading(self) -> None:
        dual_df = pd.DataFrame(
            [
                {"record_id": "R1", "reviewer": "A", "title_abstract_decision": "include"},
                {"record_id": "R1", "reviewer": "B", "title_abstract_decision": "exclude"},
            ]
        )
        master_df = pd.DataFrame(
            [
                {
                    "record_id": "R1",
                    "title": "Case report: sample",
                    "abstract": "",
                    "journal": "",
                    "source_database": "pubmed",
                    "year": "2022",
                }
            ]
        )

        analysis = screening_disagreement_analyzer.disagreement_analysis(
            dual_df,
            master_df,
            top_records=10,
        )
        report = screening_disagreement_analyzer.build_markdown_report(
            analysis=analysis,
            dual_log_path=Path("../02_data/processed/screening_title_abstract_dual_log.csv"),
            master_records_path=Path("../02_data/processed/master_records.csv"),
            output_path=Path("outputs/screening_disagreement_report.md"),
            patterns_output_path=Path("outputs/screening_disagreement_patterns.csv"),
            records_output_path=Path("outputs/screening_disagreement_records.csv"),
        )

        self.assertIn("## Calibration Table — Disagreement by Article Type", report)
        self.assertIn("| Article type | Paired records | Disagreements |", report)


if __name__ == "__main__":
    unittest.main()

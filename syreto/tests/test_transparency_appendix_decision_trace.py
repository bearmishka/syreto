from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "transparency_appendix_decision_trace.py"


class TransparencyAppendixDecisionTraceTests(unittest.TestCase):
    def test_inserts_auto_decision_trace_block_before_extraction_heading(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            appendix_path = tmp_path / "appendix_transparency.md"
            decision_log_path = tmp_path / "decision_log.csv"
            analysis_trace_path = tmp_path / "analysis_trace.json"
            summary_path = tmp_path / "sync_summary.md"
            latex_output_path = tmp_path / "decision_trace_table.tex"
            analysis_latex_output_path = tmp_path / "analysis_trace_table.tex"

            appendix_path.write_text(
                "# Transparency Appendix\n\n"
                "## 2. Screening examples\n\n"
                "Placeholder screening content.\n\n"
                "## 3. Extraction sample (3 studies)\n",
                encoding="utf-8",
            )
            decision_log_path.write_text(
                "record_id,stage,decision,reason,reviewer\n"
                "MR00001,screening,include,meets criteria,R1\n"
                "MR00002,fulltext,exclude,wrong outcome,R2\n",
                encoding="utf-8",
            )
            analysis_trace_path.write_text(
                json.dumps(
                    {
                        "generated_at_utc": "2026-03-18T20:00:00Z",
                        "metric": "converted_d",
                        "source_files": {},
                        "outcome_1": {
                            "outcome": "body_image",
                            "studies": ["S1", "S2"],
                            "model": "random_effects",
                            "excluded": ["S7"],
                            "reason_excluded": "missing variance",
                            "reason_excluded_by_study": {"S7": "missing variance"},
                        },
                    }
                ),
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    "python",
                    str(SCRIPT_PATH),
                    "--appendix",
                    str(appendix_path),
                    "--decision-log",
                    str(decision_log_path),
                    "--analysis-trace",
                    str(analysis_trace_path),
                    "--summary-output",
                    str(summary_path),
                    "--latex-output",
                    str(latex_output_path),
                    "--analysis-latex-output",
                    str(analysis_latex_output_path),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)

            appendix_text = appendix_path.read_text(encoding="utf-8")
            self.assertIn("<!-- AUTO_DECISION_TRACE_START -->", appendix_text)
            self.assertIn("### Decision trace log (auto-export)", appendix_text)
            self.assertIn("| MR00002 | fulltext | exclude | wrong outcome | R2 |", appendix_text)
            self.assertIn("<!-- AUTO_ANALYSIS_TRACE_START -->", appendix_text)
            self.assertIn("### Analysis traceability (auto-export)", appendix_text)
            self.assertIn(
                "| body_image | random_effects | S1, S2 | S7 | missing variance |", appendix_text
            )
            self.assertLess(
                appendix_text.find("<!-- AUTO_DECISION_TRACE_START -->"),
                appendix_text.find("## 3. Extraction sample (3 studies)"),
            )

            summary_text = summary_path.read_text(encoding="utf-8")
            self.assertIn("Decision rows exported: 2", summary_text)
            self.assertIn("Analysis outcomes exported: 1", summary_text)
            self.assertIn(str(latex_output_path), summary_text)

            latex_text = latex_output_path.read_text(encoding="utf-8")
            self.assertIn(r"\label{tab:decision_trace_log}", latex_text)
            self.assertIn("MR00002 & fulltext & exclude & R2 & wrong outcome", latex_text)

            analysis_latex_text = analysis_latex_output_path.read_text(encoding="utf-8")
            self.assertIn(r"\label{tab:analysis_trace_log}", analysis_latex_text)
            self.assertIn(
                "body\\_image & random\\_effects & S1, S2 & S7 & missing variance",
                analysis_latex_text,
            )

    def test_replaces_existing_auto_block_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            appendix_path = tmp_path / "appendix_transparency.md"
            decision_log_path = tmp_path / "decision_log.csv"
            analysis_trace_path = tmp_path / "analysis_trace.json"
            summary_path = tmp_path / "sync_summary.md"
            latex_output_path = tmp_path / "decision_trace_table.tex"
            analysis_latex_output_path = tmp_path / "analysis_trace_table.tex"

            appendix_path.write_text(
                "# Transparency Appendix\n\n"
                "## 2. Screening examples\n\n"
                "<!-- AUTO_DECISION_TRACE_START -->\n"
                "stale block\n"
                "<!-- AUTO_DECISION_TRACE_END -->\n\n"
                "## 3. Extraction sample (3 studies)\n",
                encoding="utf-8",
            )
            decision_log_path.write_text(
                "record_id,stage,decision,reason,reviewer\n"
                "MR00003,screening,exclude,wrong population,R3\n",
                encoding="utf-8",
            )
            analysis_trace_path.write_text(
                json.dumps(
                    {
                        "generated_at_utc": "2026-03-18T20:00:00Z",
                        "metric": "converted_d",
                        "source_files": {},
                        "outcome_1": {
                            "outcome": "confidence",
                            "studies": ["S1"],
                            "model": "random_effects",
                            "excluded": ["S9"],
                            "reason_excluded": "missing variance",
                            "reason_excluded_by_study": {"S9": "missing variance"},
                        },
                    }
                ),
                encoding="utf-8",
            )

            for _ in range(2):
                result = subprocess.run(
                    [
                        "python",
                        str(SCRIPT_PATH),
                        "--appendix",
                        str(appendix_path),
                        "--decision-log",
                        str(decision_log_path),
                        "--analysis-trace",
                        str(analysis_trace_path),
                        "--summary-output",
                        str(summary_path),
                        "--latex-output",
                        str(latex_output_path),
                        "--analysis-latex-output",
                        str(analysis_latex_output_path),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)

            appendix_text = appendix_path.read_text(encoding="utf-8")
            self.assertEqual(appendix_text.count("<!-- AUTO_DECISION_TRACE_START -->"), 1)
            self.assertEqual(appendix_text.count("<!-- AUTO_DECISION_TRACE_END -->"), 1)
            self.assertEqual(appendix_text.count("<!-- AUTO_ANALYSIS_TRACE_START -->"), 1)
            self.assertEqual(appendix_text.count("<!-- AUTO_ANALYSIS_TRACE_END -->"), 1)
            self.assertIn(
                "| MR00003 | screening | exclude | wrong population | R3 |", appendix_text
            )
            self.assertIn(
                "| confidence | random_effects | S1 | S9 | missing variance |", appendix_text
            )

            latex_text = latex_output_path.read_text(encoding="utf-8")
            self.assertIn("MR00003 & screening & exclude & R3 & wrong population", latex_text)

    def test_respects_latex_max_rows_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            appendix_path = tmp_path / "appendix_transparency.md"
            decision_log_path = tmp_path / "decision_log.csv"
            summary_path = tmp_path / "sync_summary.md"
            latex_output_path = tmp_path / "decision_trace_table.tex"

            appendix_path.write_text("# Transparency Appendix\n", encoding="utf-8")
            decision_log_path.write_text(
                "record_id,stage,decision,reason,reviewer\n"
                "MR10001,screening,include,criterion a,R1\n"
                "MR10002,screening,exclude,criterion b,R2\n"
                "MR10003,fulltext,exclude,criterion c,R3\n",
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    "python",
                    str(SCRIPT_PATH),
                    "--appendix",
                    str(appendix_path),
                    "--decision-log",
                    str(decision_log_path),
                    "--summary-output",
                    str(summary_path),
                    "--latex-output",
                    str(latex_output_path),
                    "--latex-max-rows",
                    "2",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)

            latex_text = latex_output_path.read_text(encoding="utf-8")
            self.assertIn("MR10001 & screening & include & R1 & criterion a", latex_text)
            self.assertIn("MR10002 & screening & exclude & R2 & criterion b", latex_text)
            self.assertNotIn("MR10003 & fulltext & exclude & R3 & criterion c", latex_text)
            self.assertIn("Table truncated to first 2 rows", latex_text)

    def test_respects_markdown_max_rows_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            appendix_path = tmp_path / "appendix_transparency.md"
            decision_log_path = tmp_path / "decision_log.csv"
            summary_path = tmp_path / "sync_summary.md"
            latex_output_path = tmp_path / "decision_trace_table.tex"

            appendix_path.write_text("# Transparency Appendix\n", encoding="utf-8")
            decision_log_path.write_text(
                "record_id,stage,decision,reason,reviewer\n"
                "MR20001,screening,include,criterion a,R1\n"
                "MR20002,screening,exclude,criterion b,R2\n"
                "MR20003,fulltext,exclude,criterion c,R3\n",
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    "python",
                    str(SCRIPT_PATH),
                    "--appendix",
                    str(appendix_path),
                    "--decision-log",
                    str(decision_log_path),
                    "--summary-output",
                    str(summary_path),
                    "--latex-output",
                    str(latex_output_path),
                    "--max-rows",
                    "2",
                    "--latex-max-rows",
                    "3",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)

            appendix_text = appendix_path.read_text(encoding="utf-8")
            self.assertIn("| MR20001 | screening | include | criterion a | R1 |", appendix_text)
            self.assertIn("| MR20002 | screening | exclude | criterion b | R2 |", appendix_text)
            self.assertNotIn("| MR20003 | fulltext | exclude | criterion c | R3 |", appendix_text)
            self.assertIn("Table truncated to first 2 rows", appendix_text)

    def test_respects_analysis_markdown_max_rows_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            appendix_path = tmp_path / "appendix_transparency.md"
            decision_log_path = tmp_path / "decision_log.csv"
            analysis_trace_path = tmp_path / "analysis_trace.json"
            summary_path = tmp_path / "sync_summary.md"
            latex_output_path = tmp_path / "decision_trace_table.tex"
            analysis_latex_output_path = tmp_path / "analysis_trace_table.tex"

            appendix_path.write_text("# Transparency Appendix\n", encoding="utf-8")
            decision_log_path.write_text(
                "record_id,stage,decision,reason,reviewer\n"
                "MR30001,screening,include,criterion a,R1\n",
                encoding="utf-8",
            )
            analysis_trace_path.write_text(
                json.dumps(
                    {
                        "generated_at_utc": "2026-03-18T20:00:00Z",
                        "metric": "converted_d",
                        "source_files": {},
                        "outcome_1": {
                            "outcome": "body_image",
                            "studies": ["S1"],
                            "model": "random_effects",
                            "excluded": ["S5"],
                            "reason_excluded": "missing variance",
                            "reason_excluded_by_study": {"S5": "missing variance"},
                        },
                        "outcome_2": {
                            "outcome": "self_esteem",
                            "studies": ["S2"],
                            "model": "random_effects",
                            "excluded": [],
                            "reason_excluded": "",
                            "reason_excluded_by_study": {},
                        },
                    }
                ),
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    "python",
                    str(SCRIPT_PATH),
                    "--appendix",
                    str(appendix_path),
                    "--decision-log",
                    str(decision_log_path),
                    "--analysis-trace",
                    str(analysis_trace_path),
                    "--summary-output",
                    str(summary_path),
                    "--latex-output",
                    str(latex_output_path),
                    "--analysis-latex-output",
                    str(analysis_latex_output_path),
                    "--analysis-max-rows",
                    "1",
                    "--analysis-latex-max-rows",
                    "1",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)

            appendix_text = appendix_path.read_text(encoding="utf-8")
            self.assertIn(
                "| body_image | random_effects | S1 | S5 | missing variance |", appendix_text
            )
            self.assertNotIn("| self_esteem | random_effects | S2 | — | — |", appendix_text)
            self.assertIn("Table truncated to first 1 rows", appendix_text)

            analysis_latex_text = analysis_latex_output_path.read_text(encoding="utf-8")
            self.assertIn(
                "body\\_image & random\\_effects & S1 & S5 & missing variance", analysis_latex_text
            )
            self.assertNotIn("self\\_esteem & random\\_effects & S2", analysis_latex_text)
            self.assertIn("Table truncated to first 1 rows", analysis_latex_text)


if __name__ == "__main__":
    unittest.main()

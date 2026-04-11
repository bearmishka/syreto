from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "status_report.py"


class StatusReportCliOutputsTests(unittest.TestCase):
    def test_main_writes_status_outputs_with_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            screening_log = base / "screening_log.csv"
            screening_record_log = base / "screening_record_log.csv"
            master = base / "master_records.csv"
            search_log = base / "search_log.csv"
            prisma = base / "prisma_counts.csv"
            protocol = base / "protocol.md"
            manuscript = base / "main.tex"
            screening_summary = base / "screening_metrics_summary.md"
            csv_validation = base / "csv_input_validation_summary.md"
            extraction_validation = base / "extraction_validation_summary.md"
            quality_summary = base / "quality_appraisal_summary.md"
            quality_scored = base / "quality_appraisal_scored.csv"
            effect_summary = base / "effect_size_conversion_summary.md"
            effect_converted = base / "effect_size_converted.csv"
            reviewer_workload_summary = base / "reviewer_workload_balancer_summary.md"
            dedup_summary = base / "dedup_stats_summary.md"
            prisma_flow = base / "prisma_flow_diagram.tex"
            daily_run_manifest = base / "daily_run_manifest.json"
            daily_run_failed_marker = base / "daily_run_failed.marker"
            output = base / "status_report.md"
            json_output = base / "status_summary.json"

            pd.DataFrame(columns=["date", "reviewer", "stage", "records_screened"]).to_csv(
                screening_log, index=False
            )
            pd.DataFrame(columns=["record_id", "reviewer", "title_abstract_decision"]).to_csv(
                screening_record_log, index=False
            )
            pd.DataFrame(
                [
                    {
                        "record_id": "MR0001",
                        "source_database": "pubmed",
                        "source_record_id": "PMID_1",
                        "title": "Study A",
                        "abstract": "Abstract",
                        "authors": "Doe, A.",
                        "year": "2024",
                        "journal": "Journal",
                        "doi": "10.1000/a",
                        "pmid": "",
                        "normalized_title": "study a",
                        "normalized_first_author": "doe",
                        "is_duplicate": "no",
                        "duplicate_of_record_id": "",
                        "dedup_reason": "",
                        "notes": "",
                    }
                ]
            ).to_csv(master, index=False)
            pd.DataFrame(
                [{"database": "PubMed", "date_searched": "2026-03-15", "results_total": "1"}]
            ).to_csv(search_log, index=False)
            pd.DataFrame(
                [
                    {"stage": "records_identified_databases", "count": "1", "notes": ""},
                    {"stage": "duplicates_removed", "count": "0", "notes": ""},
                ]
            ).to_csv(prisma, index=False)
            protocol.write_text("# Protocol\nPROSPERO: CRD42026000001\n", encoding="utf-8")
            manuscript.write_text("\\title{Review}\n", encoding="utf-8")
            screening_summary.write_text("# Screening metrics\n", encoding="utf-8")
            csv_validation.write_text("- Errors: 0\n- Warnings: 0\n", encoding="utf-8")
            extraction_validation.write_text("- Errors: 0\n- Warnings: 0\n", encoding="utf-8")
            quality_summary.write_text("ok\n", encoding="utf-8")
            quality_scored.write_text("study_id,score\nA,1\n", encoding="utf-8")
            effect_summary.write_text("ok\n", encoding="utf-8")
            effect_converted.write_text("study_id,converted_d\nA,0.2\n", encoding="utf-8")
            reviewer_workload_summary.write_text("ok\n", encoding="utf-8")
            dedup_summary.write_text("ok\n", encoding="utf-8")
            prisma_flow.write_text("% flow\n", encoding="utf-8")
            daily_run_manifest.write_text(
                '{"run_id":"run-2","state":"success","started_at_utc":"2026-03-15T09:00:00Z","updated_at_utc":"2026-03-15T09:02:00Z","pipeline_exit_code":0,"status_checkpoint_exit_code":0,"final_exit_code":0,"failure_phase":"","rollback_applied":false,"transactional_mode":"enabled"}\n',
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--screening-log",
                    str(screening_log),
                    "--screening-record-log",
                    str(screening_record_log),
                    "--master",
                    str(master),
                    "--search-log",
                    str(search_log),
                    "--prisma",
                    str(prisma),
                    "--protocol",
                    str(protocol),
                    "--manuscript",
                    str(manuscript),
                    "--screening-summary",
                    str(screening_summary),
                    "--csv-input-validation-summary",
                    str(csv_validation),
                    "--extraction-validation-summary",
                    str(extraction_validation),
                    "--quality-appraisal-summary",
                    str(quality_summary),
                    "--quality-appraisal-scored",
                    str(quality_scored),
                    "--effect-size-summary",
                    str(effect_summary),
                    "--effect-size-converted",
                    str(effect_converted),
                    "--reviewer-workload-summary",
                    str(reviewer_workload_summary),
                    "--dedup-summary",
                    str(dedup_summary),
                    "--prisma-flow",
                    str(prisma_flow),
                    "--daily-run-manifest",
                    str(daily_run_manifest),
                    "--daily-run-failed-marker",
                    str(daily_run_failed_marker),
                    "--output",
                    str(output),
                    "--json-output",
                    str(json_output),
                    "--review-mode",
                    "template",
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(output.exists())
            self.assertTrue(json_output.exists())

            report_provenance = json.loads(
                output.with_name(f"{output.name}.provenance.json").read_text(encoding="utf-8")
            )
            summary_provenance = json.loads(
                json_output.with_name(f"{json_output.name}.provenance.json").read_text(
                    encoding="utf-8"
                )
            )

            self.assertEqual(report_provenance["generated_by"], "status_report.py")
            self.assertEqual(summary_provenance["generated_by"], "status_report.py")
            self.assertEqual(summary_provenance["review_mode"], "template")
            self.assertIn(str(search_log), summary_provenance["upstream_inputs"])
            self.assertEqual(report_provenance["artifact_path"], str(output))


if __name__ == "__main__":
    unittest.main()

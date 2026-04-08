import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

MODULE_PATH = Path(__file__).resolve().parents[1] / "dedup_stats.py"
spec = importlib.util.spec_from_file_location("dedup_stats", MODULE_PATH)
dedup_stats = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = dedup_stats
assert spec.loader is not None
spec.loader.exec_module(dedup_stats)


class DedupStatsPrismaSyncTests(unittest.TestCase):
    def _prepare_inputs(self, tmp_path: Path) -> tuple[Path, Path, Path, Path, Path]:
        master_path = tmp_path / "master_records.csv"
        search_log_path = tmp_path / "search_log.csv"
        screening_fulltext_log_path = tmp_path / "screening_fulltext_log.csv"
        screening_title_abstract_results_path = tmp_path / "screening_title_abstract_results.csv"
        prisma_path = tmp_path / "prisma_counts_template.csv"

        pd.DataFrame(
            [
                {"record_id": "R00001", "is_duplicate": "no", "title": "Study A"},
                {"record_id": "R00002", "is_duplicate": "yes", "title": "Study B"},
            ]
        ).to_csv(master_path, index=False)

        pd.DataFrame([{"results_total": "5"}]).to_csv(search_log_path, index=False)

        pd.DataFrame(
            [
                {
                    "record_id": "R00001",
                    "fulltext_available": "yes",
                    "include": "include",
                    "exclusion_reason": "",
                    "reviewer": "EP",
                    "notes": "",
                },
                {
                    "record_id": "R00003",
                    "fulltext_available": "yes",
                    "include": "exclude",
                    "exclusion_reason": "wrong outcome",
                    "reviewer": "EP",
                    "notes": "",
                },
                {
                    "record_id": "R00004",
                    "fulltext_available": "no",
                    "include": "",
                    "exclusion_reason": "full text unavailable",
                    "reviewer": "EP",
                    "notes": "",
                },
            ]
        ).to_csv(screening_fulltext_log_path, index=False)

        pd.DataFrame(
            [
                {
                    "record_id": "R00001",
                    "reviewer1_decision": "include",
                    "reviewer2_decision": "include",
                    "conflict": "no",
                    "conflict_resolver": "",
                    "resolution_decision": "",
                    "final_decision": "include",
                    "exclusion_reason": "",
                },
                {
                    "record_id": "R00002",
                    "reviewer1_decision": "exclude",
                    "reviewer2_decision": "exclude",
                    "conflict": "no",
                    "conflict_resolver": "",
                    "resolution_decision": "",
                    "final_decision": "exclude",
                    "exclusion_reason": "wrong population",
                },
            ]
        ).to_csv(screening_title_abstract_results_path, index=False)

        pd.DataFrame(
            [
                {"stage": "records_identified_databases", "count": "0", "notes": ""},
                {"stage": "duplicates_removed", "count": "0", "notes": ""},
                {"stage": "records_screened_title_abstract", "count": "0", "notes": ""},
                {"stage": "records_excluded_title_abstract", "count": "0", "notes": ""},
                {"stage": "reports_sought_for_retrieval", "count": "0", "notes": ""},
                {"stage": "reports_not_retrieved", "count": "0", "notes": ""},
                {"stage": "reports_assessed_full_text", "count": "0", "notes": ""},
                {"stage": "reports_excluded_full_text", "count": "0", "notes": ""},
                {"stage": "studies_included_qualitative_synthesis", "count": "0", "notes": ""},
            ]
        ).to_csv(prisma_path, index=False)

        return (
            master_path,
            search_log_path,
            screening_fulltext_log_path,
            screening_title_abstract_results_path,
            prisma_path,
        )

    def test_apply_requires_backup_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            (
                master_path,
                search_log_path,
                screening_fulltext_log_path,
                screening_title_abstract_results_path,
                prisma_path,
            ) = self._prepare_inputs(tmp_path)

            with self.assertRaises(SystemExit) as ctx:
                dedup_stats.main(
                    [
                        "--master",
                        str(master_path),
                        "--search-log",
                        str(search_log_path),
                        "--screening-fulltext-log",
                        str(screening_fulltext_log_path),
                        "--screening-title-abstract-results",
                        str(screening_title_abstract_results_path),
                        "--prisma",
                        str(prisma_path),
                        "--apply",
                    ]
                )

            self.assertEqual(ctx.exception.code, 2)

    def test_dry_run_does_not_modify_prisma_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            (
                master_path,
                search_log_path,
                screening_fulltext_log_path,
                screening_title_abstract_results_path,
                prisma_path,
            ) = self._prepare_inputs(tmp_path)
            summary_path = tmp_path / "dedup_stats_summary.md"
            flow_tex_path = tmp_path / "prisma_flow_diagram.tex"

            original = prisma_path.read_text(encoding="utf-8")

            exit_code = dedup_stats.main(
                [
                    "--master",
                    str(master_path),
                    "--search-log",
                    str(search_log_path),
                    "--screening-fulltext-log",
                    str(screening_fulltext_log_path),
                    "--screening-title-abstract-results",
                    str(screening_title_abstract_results_path),
                    "--prisma",
                    str(prisma_path),
                    "--summary",
                    str(summary_path),
                    "--flow-backend",
                    "tikz",
                    "--flow-tex-output",
                    str(flow_tex_path),
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertEqual(prisma_path.read_text(encoding="utf-8"), original)
            self.assertTrue(summary_path.exists())
            self.assertTrue(flow_tex_path.exists())

            summary_text = summary_path.read_text(encoding="utf-8")
            self.assertIn("Mode: `dry-run`", summary_text)

    def test_apply_with_backup_updates_prisma_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            (
                master_path,
                search_log_path,
                screening_fulltext_log_path,
                screening_title_abstract_results_path,
                prisma_path,
            ) = self._prepare_inputs(tmp_path)
            summary_path = tmp_path / "dedup_stats_summary.md"
            flow_tex_path = tmp_path / "prisma_flow_diagram.tex"
            backup_path = tmp_path / "prisma_counts_template.backup.csv"

            original = prisma_path.read_text(encoding="utf-8")

            exit_code = dedup_stats.main(
                [
                    "--master",
                    str(master_path),
                    "--search-log",
                    str(search_log_path),
                    "--screening-fulltext-log",
                    str(screening_fulltext_log_path),
                    "--screening-title-abstract-results",
                    str(screening_title_abstract_results_path),
                    "--prisma",
                    str(prisma_path),
                    "--summary",
                    str(summary_path),
                    "--flow-backend",
                    "tikz",
                    "--flow-tex-output",
                    str(flow_tex_path),
                    "--apply",
                    "--backup",
                    "--backup-path",
                    str(backup_path),
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue(backup_path.exists())
            self.assertEqual(backup_path.read_text(encoding="utf-8"), original)

            updated_df = pd.read_csv(prisma_path, dtype=str)
            counts = dict(zip(updated_df["stage"], updated_df["count"]))
            self.assertEqual(counts["records_identified_databases"], "5")
            self.assertEqual(counts["duplicates_removed"], "1")
            self.assertEqual(counts["records_screened_title_abstract"], "2")
            self.assertEqual(counts["records_excluded_title_abstract"], "1")
            self.assertEqual(counts["reports_sought_for_retrieval"], "3")
            self.assertEqual(counts["reports_not_retrieved"], "1")
            self.assertEqual(counts["reports_assessed_full_text"], "2")
            self.assertEqual(counts["reports_excluded_full_text"], "1")
            self.assertEqual(counts["studies_included_qualitative_synthesis"], "1")

    def test_apply_repairs_corrupted_backup_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            (
                master_path,
                search_log_path,
                screening_fulltext_log_path,
                screening_title_abstract_results_path,
                prisma_path,
            ) = self._prepare_inputs(tmp_path)
            summary_path = tmp_path / "dedup_stats_summary.md"
            flow_tex_path = tmp_path / "prisma_flow_diagram.tex"
            backup_path = tmp_path / "prisma_counts_template.backup.csv"

            original = prisma_path.read_text(encoding="utf-8")
            original_copy2 = dedup_stats.shutil.copy2

            def corrupting_copy2(
                src: str | Path, dst: str | Path, *args: object, **kwargs: object
            ) -> str | Path:
                copied_path = original_copy2(src, dst, *args, **kwargs)
                backup_target = Path(dst)
                payload = backup_target.read_text(encoding="utf-8")
                backup_target.write_text(payload + payload, encoding="utf-8")
                return copied_path

            with mock.patch.object(dedup_stats.shutil, "copy2", side_effect=corrupting_copy2):
                exit_code = dedup_stats.main(
                    [
                        "--master",
                        str(master_path),
                        "--search-log",
                        str(search_log_path),
                        "--screening-fulltext-log",
                        str(screening_fulltext_log_path),
                        "--screening-title-abstract-results",
                        str(screening_title_abstract_results_path),
                        "--prisma",
                        str(prisma_path),
                        "--summary",
                        str(summary_path),
                        "--flow-backend",
                        "tikz",
                        "--flow-tex-output",
                        str(flow_tex_path),
                        "--apply",
                        "--backup",
                        "--backup-path",
                        str(backup_path),
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(backup_path.read_text(encoding="utf-8"), original)

    def test_fulltext_counts_are_unchanged_when_fulltext_log_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            (
                master_path,
                search_log_path,
                screening_fulltext_log_path,
                screening_title_abstract_results_path,
                prisma_path,
            ) = self._prepare_inputs(tmp_path)
            summary_path = tmp_path / "dedup_stats_summary.md"
            flow_tex_path = tmp_path / "prisma_flow_diagram.tex"
            backup_path = tmp_path / "prisma_counts_template.backup.csv"

            pd.DataFrame(
                columns=[
                    "record_id",
                    "fulltext_available",
                    "include",
                    "exclusion_reason",
                    "reviewer",
                    "notes",
                ]
            ).to_csv(screening_fulltext_log_path, index=False)

            existing_df = pd.read_csv(prisma_path, dtype=str)
            existing_df.loc[existing_df["stage"] == "reports_sought_for_retrieval", "count"] = "7"
            existing_df.loc[existing_df["stage"] == "reports_assessed_full_text", "count"] = "6"
            existing_df.loc[existing_df["stage"] == "reports_excluded_full_text", "count"] = "5"
            existing_df.loc[
                existing_df["stage"] == "studies_included_qualitative_synthesis", "count"
            ] = "4"
            existing_df.to_csv(prisma_path, index=False)

            exit_code = dedup_stats.main(
                [
                    "--master",
                    str(master_path),
                    "--search-log",
                    str(search_log_path),
                    "--screening-fulltext-log",
                    str(screening_fulltext_log_path),
                    "--screening-title-abstract-results",
                    str(screening_title_abstract_results_path),
                    "--prisma",
                    str(prisma_path),
                    "--summary",
                    str(summary_path),
                    "--flow-backend",
                    "tikz",
                    "--flow-tex-output",
                    str(flow_tex_path),
                    "--apply",
                    "--backup",
                    "--backup-path",
                    str(backup_path),
                ]
            )

            self.assertEqual(exit_code, 0)

            updated_df = pd.read_csv(prisma_path, dtype=str)
            counts = dict(zip(updated_df["stage"], updated_df["count"]))
            self.assertEqual(counts["reports_sought_for_retrieval"], "1")
            self.assertEqual(counts["reports_assessed_full_text"], "6")
            self.assertEqual(counts["reports_excluded_full_text"], "5")
            self.assertEqual(counts["studies_included_qualitative_synthesis"], "4")

    def test_matplotlib_backend_writes_svg_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            (
                master_path,
                search_log_path,
                screening_fulltext_log_path,
                screening_title_abstract_results_path,
                prisma_path,
            ) = self._prepare_inputs(tmp_path)
            summary_path = tmp_path / "dedup_stats_summary.md"
            flow_svg_path = tmp_path / "prisma_flow_diagram.svg"

            exit_code = dedup_stats.main(
                [
                    "--master",
                    str(master_path),
                    "--search-log",
                    str(search_log_path),
                    "--screening-fulltext-log",
                    str(screening_fulltext_log_path),
                    "--screening-title-abstract-results",
                    str(screening_title_abstract_results_path),
                    "--prisma",
                    str(prisma_path),
                    "--summary",
                    str(summary_path),
                    "--flow-backend",
                    "matplotlib",
                    "--flow-output",
                    str(flow_svg_path),
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue(flow_svg_path.exists())
            self.assertIn("<svg", flow_svg_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()

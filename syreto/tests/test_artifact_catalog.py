import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from syreto import cli
from syreto.artifact_catalog import (
    artifact_catalog_entries,
    sync_artifact_catalog_doc,
    sync_artifact_catalog_surfaces,
    write_machine_readable_catalog,
)


class ArtifactCatalogTests(unittest.TestCase):
    def test_catalog_entries_expose_contract_fields(self) -> None:
        entries = artifact_catalog_entries(include_inputs=True)
        self.assertTrue(entries)

        master_records = next(
            entry for entry in entries if entry.path == "02_data/processed/master_records.csv"
        )
        self.assertTrue(master_records.canonical)
        self.assertFalse(master_records.reproducible)
        self.assertEqual(master_records.schema_ref, "syreto/csv_schema.py")
        self.assertIn("doctor", master_records.consumed_by)

    def test_machine_readable_catalog_is_written_with_expected_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "catalog.json"
            write_machine_readable_catalog(output_path)

            payload = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["schema_version"], "0.1")
        self.assertTrue(payload["entries"])
        status_summary = next(
            entry for entry in payload["entries"] if entry["path"] == "outputs/status_summary.json"
        )
        self.assertEqual(status_summary["canonical"], False)
        self.assertEqual(status_summary["reproducible"], True)
        self.assertIn("doctor", status_summary["consumed_by"])

    def test_cli_artifacts_json_surfaces_new_contract_fields(self) -> None:
        stdout = StringIO()
        with redirect_stdout(stdout):
            exit_code = cli.main(["artifacts", "--kind", "input", "--json"])

        self.assertEqual(exit_code, 0)
        rows = json.loads(stdout.getvalue())
        self.assertTrue(rows)
        search_log = next(row for row in rows if row["path"] == "02_data/processed/search_log.csv")
        self.assertEqual(search_log["kind"], "input")
        self.assertEqual(search_log["canonical"], True)
        self.assertEqual(search_log["reproducible"], False)
        self.assertEqual(search_log["schema_ref"], "syreto/csv_schema.py")
        self.assertIn("doctor", search_log["consumed_by"])

    def test_sync_artifact_catalog_doc_rebuilds_markdown_table_between_markers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            doc_path = Path(tmp_dir) / "artifact-catalog.md"
            doc_path.write_text(
                "# Test\n\n"
                "<!-- ARTIFACT_CATALOG_TABLE:START -->\n"
                "old table\n"
                "<!-- ARTIFACT_CATALOG_TABLE:END -->\n",
                encoding="utf-8",
            )

            sync_artifact_catalog_doc(doc_path)
            rendered = doc_path.read_text(encoding="utf-8")

        self.assertIn("| Artifact | Producer | Consumed by |", rendered)
        self.assertIn("02_data/processed/master_records.csv", rendered)
        self.assertIn("outputs/figures/predictor_outcome_heatmap.png", rendered)

    def test_sync_artifact_catalog_surfaces_updates_doc_and_json_together(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            doc_path = Path(tmp_dir) / "artifact-catalog.md"
            json_path = Path(tmp_dir) / "catalog.json"
            doc_path.write_text(
                "# Test\n\n"
                "<!-- ARTIFACT_CATALOG_TABLE:START -->\n"
                "stale\n"
                "<!-- ARTIFACT_CATALOG_TABLE:END -->\n",
                encoding="utf-8",
            )

            sync_artifact_catalog_surfaces(doc_path=doc_path, json_path=json_path)
            doc_rendered = doc_path.read_text(encoding="utf-8")
            json_payload = json.loads(json_path.read_text(encoding="utf-8"))

        self.assertIn("outputs/status_summary.json", doc_rendered)
        self.assertEqual(json_payload["schema_version"], "0.1")
        self.assertTrue(json_payload["entries"])

    def test_cli_artifacts_sync_catalog_updates_configured_targets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            doc_path = Path(tmp_dir) / "artifact-catalog.md"
            json_path = Path(tmp_dir) / "catalog.json"
            doc_path.write_text(
                "# Test\n\n"
                "<!-- ARTIFACT_CATALOG_TABLE:START -->\n"
                "stale\n"
                "<!-- ARTIFACT_CATALOG_TABLE:END -->\n",
                encoding="utf-8",
            )

            stdout = StringIO()
            original_doc = cli.ARTIFACT_CATALOG_DOC_PATH
            original_json = cli.ARTIFACT_CATALOG_JSON_PATH
            cli.ARTIFACT_CATALOG_DOC_PATH = doc_path
            cli.ARTIFACT_CATALOG_JSON_PATH = json_path
            try:
                with redirect_stdout(stdout):
                    exit_code = cli.main(
                        ["artifacts", "--kind", "input", "--json", "--sync-catalog"]
                    )
            finally:
                cli.ARTIFACT_CATALOG_DOC_PATH = original_doc
                cli.ARTIFACT_CATALOG_JSON_PATH = original_json

            payload = json.loads(json_path.read_text(encoding="utf-8"))
            doc_rendered = doc_path.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 0)
        self.assertTrue(payload["entries"])
        self.assertIn("02_data/processed/search_log.csv", doc_rendered)


if __name__ == "__main__":
    unittest.main()

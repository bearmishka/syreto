import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "prospero_submission_drafter_layers" / "io.py"
spec = importlib.util.spec_from_file_location("prospero_submission_drafter_layers.io", MODULE_PATH)
io_layer = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = io_layer
assert spec.loader is not None
spec.loader.exec_module(io_layer)


class ProsperoSubmissionDrafterIOLayerTests(unittest.TestCase):
    def test_resolve_cli_paths_returns_path_objects(self) -> None:
        protocol, structured, xml_output, summary = io_layer.resolve_cli_paths(
            "../01_protocol/protocol.md",
            "outputs/prospero_registration_prefill.md",
            "outputs/prospero_registration_prefill.xml",
            "outputs/prospero_submission_drafter_summary.md",
        )

        self.assertIsInstance(protocol, Path)
        self.assertIsInstance(structured, Path)
        self.assertIsInstance(xml_output, Path)
        self.assertIsInstance(summary, Path)

    def test_resolve_preset_manuscript_path_prefers_explicit_path(self) -> None:
        resolved = io_layer.resolve_preset_manuscript_path(
            preset="bn-pilot",
            manuscript_arg="custom/main.tex",
            default_path_for_preset=lambda _: "default/main.tex",
        )

        self.assertEqual(resolved, Path("custom/main.tex"))

    def test_read_protocol_text_raises_for_missing_file(self) -> None:
        missing_path = Path("nonexistent_protocol.md")

        with self.assertRaises(SystemExit) as exc:
            io_layer.read_protocol_text(missing_path)

        self.assertIn("Protocol file not found", str(exc.exception))

    def test_write_text_artifact_creates_parent_and_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "nested" / "out.md"
            written = io_layer.write_text_artifact(target, "content")

            self.assertEqual(written, target)
            self.assertEqual(target.read_text(encoding="utf-8"), "content")


if __name__ == "__main__":
    unittest.main()

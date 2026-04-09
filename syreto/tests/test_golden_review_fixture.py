import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "review_descriptives_builder.py"
spec = importlib.util.spec_from_file_location("review_descriptives_builder", MODULE_PATH)
review_descriptives_builder = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = review_descriptives_builder
assert spec.loader is not None
spec.loader.exec_module(review_descriptives_builder)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = PROJECT_ROOT / "reviews/fixtures/minimal-golden"
EXTRACTION_PATH = FIXTURE_ROOT / "data/codebook/extraction_template.csv"
QUALITY_PATH = FIXTURE_ROOT / "data/processed/quality_appraisal_scored.csv"
EXPECTED_JSON_PATH = FIXTURE_ROOT / "expected/review_descriptives_expected.json"
EXPECTED_MARKDOWN_PATH = FIXTURE_ROOT / "expected/review_descriptives_expected.md"


def _sanitize_payload(payload: dict[str, object]) -> dict[str, object]:
    sanitized = dict(payload)
    sanitized.pop("generated_at", None)
    figure_outputs = sanitized.pop("figure_outputs", {})
    sanitized["figure_output_keys"] = sorted(figure_outputs.keys())
    return sanitized


def _sanitize_markdown(markdown: str) -> str:
    sanitized_lines: list[str] = []
    for line in markdown.splitlines():
        if line.startswith("Generated: "):
            sanitized_lines.append("Generated: <dynamic>")
        elif line.startswith("- Extraction source: `"):
            sanitized_lines.append("- Extraction source: <fixture-extraction>")
        elif line.startswith("- ") and ": `" in line and line.endswith("`"):
            name, _sep, _value = line.partition(": `")
            sanitized_lines.append(f"{name}: <generated-figure>")
        else:
            sanitized_lines.append(line)
    return "\n".join(sanitized_lines).strip()


class GoldenReviewFixtureTests(unittest.TestCase):
    def test_minimal_golden_fixture_matches_expected_descriptives(self) -> None:
        extraction_df = review_descriptives_builder.read_csv_or_empty(EXTRACTION_PATH)
        studies_df = review_descriptives_builder.build_study_view(extraction_df)
        payload = review_descriptives_builder.build_descriptives_payload(studies_df)
        quality_df = review_descriptives_builder.read_csv_or_empty(QUALITY_PATH)
        payload["distributions"]["quality_band"] = (
            review_descriptives_builder.quality_band_distribution(quality_df)
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            review_descriptives_builder.render_figures(
                payload,
                year_output=base / "figures/year.png",
                study_design_output=base / "figures/study_design.png",
                country_output=base / "figures/country.png",
                quality_band_output=base / "figures/quality_band.png",
                predictor_outcome_heatmap_output=base / "figures/predictor_outcome_heatmap.png",
                studies_df=studies_df,
            )
            markdown = review_descriptives_builder.build_markdown(
                payload,
                extraction_path=EXTRACTION_PATH,
            )

        expected_payload = json.loads(EXPECTED_JSON_PATH.read_text(encoding="utf-8"))
        expected_markdown = EXPECTED_MARKDOWN_PATH.read_text(encoding="utf-8").strip()

        self.assertEqual(_sanitize_payload(payload), expected_payload)
        self.assertEqual(_sanitize_markdown(markdown), expected_markdown)


if __name__ == "__main__":
    unittest.main()

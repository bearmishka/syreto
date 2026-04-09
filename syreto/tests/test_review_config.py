from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from syreto.review_config import ReviewConfigError, load_review_config


class ReviewConfigTests(unittest.TestCase):
    def test_load_review_config_parses_example_review(self) -> None:
        config = load_review_config(
            Path(__file__).resolve().parents[2] / "reviews/example/review.toml"
        )

        self.assertEqual(config.review_id, "example-review")
        self.assertEqual(config.review_mode, "template")
        self.assertEqual(config.fail_on, "major")
        self.assertTrue(config.stages["synthesis"])
        self.assertTrue(config.data_root.as_posix().endswith("/reviews/example/data"))

    def test_load_review_config_rejects_absolute_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            config_path = base / "review.toml"
            config_path.write_text(
                "\n".join(
                    [
                        "[review]",
                        'id = "bad-review"',
                        'title = "Bad Review"',
                        "",
                        "[paths]",
                        'data_root = "/tmp/data"',
                        'protocol_root = "protocol/"',
                        'outputs_root = "outputs/"',
                        'manuscript_root = "manuscript/"',
                        "",
                        "[mode]",
                        'review_mode = "template"',
                        "",
                        "[stages]",
                        "search = true",
                        "deduplication = true",
                        "screening = true",
                        "extraction = true",
                        "synthesis = true",
                        "reporting = true",
                        "",
                        "[status]",
                        'fail_on = "major"',
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaises(ReviewConfigError):
                load_review_config(config_path)

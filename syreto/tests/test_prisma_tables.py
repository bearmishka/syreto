from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "prisma_tables.py"


class PrismaTablesTests(unittest.TestCase):
    def test_main_writes_latex_summary_and_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            prisma_input = tmp_path / "prisma_counts_template.csv"
            fulltext_input = tmp_path / "full_text_exclusion_reasons.csv"
            prisma_output = tmp_path / "prisma_counts_table.tex"
            fulltext_output = tmp_path / "fulltext_exclusion_table.tex"
            summary_output = tmp_path / "prisma_tables_summary.md"

            pd.DataFrame(
                [
                    {"stage": "records_identified_databases", "count": "120"},
                    {"stage": "duplicates_removed", "count": "20"},
                    {"stage": "studies_included_qualitative_synthesis", "count": "12"},
                ]
            ).to_csv(prisma_input, index=False)
            pd.DataFrame(
                [
                    {"reason": "Wrong population", "count": "5"},
                    {"reason": "Wrong outcome", "count": "3"},
                ]
            ).to_csv(fulltext_input, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--prisma-input",
                    str(prisma_input),
                    "--fulltext-input",
                    str(fulltext_input),
                    "--prisma-output",
                    str(prisma_output),
                    "--fulltext-output",
                    str(fulltext_output),
                    "--summary-output",
                    str(summary_output),
                ],
                cwd=SCRIPT_PATH.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(prisma_output.exists())
            self.assertTrue(fulltext_output.exists())
            self.assertTrue(summary_output.exists())

            prisma_text = prisma_output.read_text(encoding="utf-8")
            fulltext_text = fulltext_output.read_text(encoding="utf-8")
            summary_text = summary_output.read_text(encoding="utf-8")

            self.assertIn(r"\label{tab:prisma_counts}", prisma_text)
            self.assertIn("Records identified from databases", prisma_text)
            self.assertIn("Wrong population", fulltext_text)
            self.assertIn("PRISMA rows read: 3", summary_text)
            self.assertIn("Full-text reason rows read: 2", summary_text)

            prisma_provenance = json.loads(
                prisma_output.with_name(f"{prisma_output.name}.provenance.json").read_text(
                    encoding="utf-8"
                )
            )
            fulltext_provenance = json.loads(
                fulltext_output.with_name(f"{fulltext_output.name}.provenance.json").read_text(
                    encoding="utf-8"
                )
            )
            summary_provenance = json.loads(
                summary_output.with_name(f"{summary_output.name}.provenance.json").read_text(
                    encoding="utf-8"
                )
            )

            self.assertEqual(prisma_provenance["generated_by"], "prisma_tables.py")
            self.assertEqual(
                prisma_provenance["upstream_inputs"],
                [str(prisma_input), str(fulltext_input)],
            )
            self.assertEqual(fulltext_provenance["artifact_path"], str(fulltext_output))
            self.assertEqual(summary_provenance["artifact_path"], str(summary_output))


if __name__ == "__main__":
    unittest.main()

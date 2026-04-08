import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET


MODULE_PATH = Path(__file__).resolve().parents[1] / "prospero_submission_drafter.py"
spec = importlib.util.spec_from_file_location("prospero_submission_drafter", MODULE_PATH)
prospero_submission_drafter = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = prospero_submission_drafter
assert spec.loader is not None
spec.loader.exec_module(prospero_submission_drafter)


FILLED_PROTOCOL = """# Protocol Draft (SYRETO Template) v1.0

Last updated: 2026-03-14

## Working title

`Sleep quality and anxiety symptoms in university students: a systematic review`

## Review type

Systematic review (PRISMA 2020) with planned narrative synthesis and optional meta-analysis where feasible.

## 1) Objective

To synthesize empirical evidence on the association between sleep quality and anxiety symptoms in university students.

## 2) Core review questions

1. Which sleep quality measures are most frequently studied in university students?
2. How are these measures associated with anxiety symptoms?
3. Which methodological factors may moderate findings (age group, setting, design, instruments)?

## 3) Eligibility criteria

### Population

- Human participants in university students.

### Concepts / exposures

At least one eligible indicator of sleep quality must be reported.

### Outcomes

At least one eligible indicator of anxiety symptoms must be reported.

### Study types

- Observational quantitative studies (cross-sectional, case-control, cohort).
- Mixed-method studies with extractable empirical findings.

### Time and language

- Publication years: 2000 onward.
- Languages: English, Spanish.

### Exclusion criteria

- No target subgroup data.
- Pure case reports (`n < 5`) or purely theoretical papers without empirical data.

## 4) Information sources

- PubMed/MEDLINE
- Embase
- PsycINFO
- Scopus
- Web of Science Core Collection
- Backward/forward citation tracking of included studies

Optional gray literature pass:

- SSRN, dissertations, preprint repositories (clearly labeled as non-peer-reviewed evidence).

## 5) Screening workflow

1. Export records to `02_data/raw/`.
2. Deduplicate in `02_data/processed/`.
3. Title/abstract screening by two independent reviewers where feasible.

## 6) Data extraction fields

- Bibliographic details (author, year, country)
- Design and setting

## 7) Quality appraisal

Use design-appropriate appraisal criteria and report scoring logic transparently.

## 8) Synthesis plan

- Start with structured narrative synthesis across design/context/measurement strata.
- If effect metrics are sufficiently comparable, run exploratory harmonization and quantitative synthesis.

## 9) Reproducibility notes

- Keep all source exports in `02_data/raw/`.
"""


class ProsperoSubmissionDrafterTests(unittest.TestCase):
    def test_extract_protocol_data_parses_core_fields(self) -> None:
        data = prospero_submission_drafter.extract_protocol_data(FILLED_PROTOCOL)

        self.assertEqual(
            data.working_title,
            "Sleep quality and anxiety symptoms in university students: a systematic review",
        )
        self.assertEqual(data.last_updated, "2026-03-14")
        self.assertEqual(data.languages, "English, Spanish.")
        self.assertIn("PubMed/MEDLINE", data.information_sources)
        self.assertIn(
            "SSRN, dissertations, preprint repositories (clearly labeled as non-peer-reviewed evidence).",
            data.gray_sources,
        )

    def test_build_prefill_fields_marks_placeholder_values_as_incomplete(self) -> None:
        protocol_with_placeholder = FILLED_PROTOCOL.replace("university students", "[POPULATION]")
        data = prospero_submission_drafter.extract_protocol_data(protocol_with_placeholder)
        fields = prospero_submission_drafter.build_prefill_fields(data, registration_mode="new")

        population_field = next(
            field for field in fields if field.field_id == "participants_population"
        )
        self.assertEqual(population_field.status, "needs_input")
        self.assertIn("placeholder", population_field.notes.lower())
        self.assertIn("[POPULATION]", data.unresolved_placeholders)

    def test_render_xml_draft_contains_completion_snapshot(self) -> None:
        data = prospero_submission_drafter.extract_protocol_data(FILLED_PROTOCOL)
        fields = prospero_submission_drafter.build_prefill_fields(data, registration_mode="new")
        self.assertEqual(len(fields), 47)

        xml_text = prospero_submission_drafter.render_xml_draft(
            fields=fields,
            protocol_path=Path("../01_protocol/protocol.md"),
            protocol_data=data,
            registration_mode="new",
            preset="none",
            preset_manuscript_path=None,
            generated_at="2026-03-14 12:00",
        )

        root = ET.fromstring(xml_text)
        self.assertEqual(root.tag, "prosperoRegistrationDraft")
        self.assertEqual(root.attrib.get("registrationMode"), "new")
        required_total = sum(1 for field in fields if field.required)
        self.assertEqual(root.findtext("completionSnapshot/requiredTotal"), str(required_total))
        review_title_value = root.find("fields/field[@id='review_title']/value")
        self.assertIsNotNone(review_title_value)
        assert review_title_value is not None
        self.assertIn("Sleep quality", review_title_value.text or "")

    def test_update_only_fields_become_required_in_update_mode(self) -> None:
        protocol_with_id = FILLED_PROTOCOL + "\n\nRegistered as CRD42012345678\n"
        data = prospero_submission_drafter.extract_protocol_data(protocol_with_id)
        fields = prospero_submission_drafter.build_prefill_fields(data, registration_mode="update")

        update_status_field = next(field for field in fields if field.field_id == "update_status")
        self.assertTrue(update_status_field.required)
        self.assertEqual(update_status_field.required_scope, "update_only")

    def test_auto_complete_fills_all_required_fields_for_new_mode(self) -> None:
        data = prospero_submission_drafter.extract_protocol_data(FILLED_PROTOCOL)
        fields = prospero_submission_drafter.build_prefill_fields(
            data,
            registration_mode="new",
            auto_complete=True,
        )

        counts = prospero_submission_drafter.completion_counts(fields)
        self.assertEqual(counts["required_missing"], 0)

    def test_auto_complete_applies_profile_and_placeholder_replacements(self) -> None:
        protocol_with_placeholders = FILLED_PROTOCOL.replace(
            "Sleep quality and anxiety symptoms in university students: a systematic review",
            "[YOUR REVIEW TITLE]",
        ).replace("university students", "[POPULATION]")

        data = prospero_submission_drafter.extract_protocol_data(protocol_with_placeholders)
        fields = prospero_submission_drafter.build_prefill_fields(
            data,
            registration_mode="new",
            auto_complete=True,
            profile_values={
                "review_title": "Custom Automated Title",
                "country": "Cyprus",
                "POPULATION": "adolescent outpatients",
            },
        )

        title_field = next(field for field in fields if field.field_id == "review_title")
        population_field = next(
            field for field in fields if field.field_id == "participants_population"
        )
        country_field = next(field for field in fields if field.field_id == "country")

        self.assertEqual(title_field.value, "Custom Automated Title")
        self.assertIn("adolescent outpatients", population_field.value)
        self.assertEqual(population_field.status, "complete")
        self.assertEqual(country_field.value, "Cyprus")

    def test_load_autofill_profile_merges_fields_and_placeholder_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            profile_path = Path(tmp_dir) / "profile.json"
            profile_path.write_text(
                json.dumps(
                    {
                        "fields": {
                            "country": "Cyprus",
                            "named_contact": "Jane Doe",
                        },
                        "POPULATION": "adult women",
                    }
                ),
                encoding="utf-8",
            )

            profile = prospero_submission_drafter.load_autofill_profile(profile_path)

        self.assertEqual(profile["country"], "Cyprus")
        self.assertEqual(profile["named_contact"], "Jane Doe")
        self.assertEqual(profile["POPULATION"], "adult women")

    def test_parse_bn_pilot_manuscript_extracts_contact_metadata(self) -> None:
        tex_content = (
            "\\title{Pilot Title}\n"
            "\\author{Jane Doe\\thanks{Correspondence: \\href{mailto:jane@example.org}{jane@example.org}}\\\\\n"
            "Institute of Clinical Research, Limassol, Cyprus}\n"
            "\\date{March 2026}\n"
            "\\noindent\\textbf{Keywords:} one; two; three.\n"
            "\\textbf{Funding.} No external funding was reported.\n\n"
            "\\textbf{Conflicts of interest.} No conflicts of interest were reported.\n\n"
            "\\textbf{Data availability.} Dataset available on request.\n\n"
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            manuscript_path = Path(tmp_dir) / "main.tex"
            manuscript_path.write_text(tex_content, encoding="utf-8")

            metadata = prospero_submission_drafter.parse_bn_pilot_manuscript(manuscript_path)

        self.assertEqual(metadata.title, "Pilot Title")
        self.assertEqual(metadata.contact_name, "Jane Doe")
        self.assertEqual(metadata.contact_email, "jane@example.org")
        self.assertEqual(metadata.affiliation, "Institute of Clinical Research, Limassol, Cyprus")
        self.assertEqual(metadata.country, "Cyprus")
        self.assertEqual(metadata.date_label, "March 2026")
        self.assertEqual(metadata.keywords, ["one", "two", "three."])

    def test_main_with_bn_pilot_preset_fills_contact_field(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            protocol_path = tmp_path / "protocol.md"
            protocol_path.write_text(FILLED_PROTOCOL, encoding="utf-8")

            manuscript_path = tmp_path / "main.tex"
            manuscript_path.write_text(
                "\\title{BN Pilot Review}\n"
                "\\author{Jane Doe\\thanks{Correspondence: \\href{mailto:jane@example.org}{jane@example.org}}\\\\\n"
                "Independent Researcher, Limassol, Cyprus}\n"
                "\\date{2026-03-14}\n"
                "\\noindent\\textbf{Keywords:} bulimia nervosa; object relations.\n"
                "\\textbf{Funding.} No external funding was reported.\n\n"
                "\\textbf{Conflicts of interest.} No conflicts of interest were reported.\n\n"
                "\\textbf{Data availability.} Data not public.\n\n",
                encoding="utf-8",
            )

            structured_output = tmp_path / "prospero_prefill.md"
            xml_output = tmp_path / "prospero_prefill.xml"
            summary_output = tmp_path / "prospero_summary.md"

            exit_code = prospero_submission_drafter.main(
                [
                    "--protocol",
                    str(protocol_path),
                    "--preset",
                    "bn-pilot",
                    "--manuscript",
                    str(manuscript_path),
                    "--structured-output",
                    str(structured_output),
                    "--xml-output",
                    str(xml_output),
                    "--summary-output",
                    str(summary_output),
                    "--format",
                    "both",
                ]
            )

            self.assertEqual(exit_code, 0)
            text = structured_output.read_text(encoding="utf-8")
            self.assertIn("Jane Doe", text)
            self.assertIn("jane@example.org", text)
            self.assertIn("Cyprus", text)

    def test_main_auto_complete_allows_fail_on_placeholders(self) -> None:
        protocol_with_placeholders = (
            FILLED_PROTOCOL.replace(
                "Sleep quality and anxiety symptoms in university students: a systematic review",
                "[YOUR REVIEW TITLE]",
            )
            .replace("sleep quality", "[EXPOSURE_OR_CONCEPT]")
            .replace("anxiety symptoms", "[OUTCOME]")
            .replace("university students", "[POPULATION]")
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            protocol_path = tmp_path / "protocol.md"
            protocol_path.write_text(protocol_with_placeholders, encoding="utf-8")

            exit_code = prospero_submission_drafter.main(
                [
                    "--protocol",
                    str(protocol_path),
                    "--auto-complete",
                    "--fail-on-missing-required",
                    "--fail-on-placeholders",
                    "--format",
                    "structured",
                    "--structured-output",
                    str(tmp_path / "out.md"),
                    "--summary-output",
                    str(tmp_path / "summary.md"),
                ]
            )

            self.assertEqual(exit_code, 0)

    def test_main_generates_requested_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            protocol_path = tmp_path / "protocol.md"
            protocol_path.write_text(FILLED_PROTOCOL, encoding="utf-8")

            structured_output = tmp_path / "prospero_prefill.md"
            xml_output = tmp_path / "prospero_prefill.xml"
            summary_output = tmp_path / "prospero_summary.md"

            exit_code = prospero_submission_drafter.main(
                [
                    "--protocol",
                    str(protocol_path),
                    "--structured-output",
                    str(structured_output),
                    "--xml-output",
                    str(xml_output),
                    "--summary-output",
                    str(summary_output),
                    "--format",
                    "both",
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue(structured_output.exists())
            self.assertTrue(xml_output.exists())
            self.assertTrue(summary_output.exists())


if __name__ == "__main__":
    unittest.main()

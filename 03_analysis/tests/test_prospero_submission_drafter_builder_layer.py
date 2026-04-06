from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys
import unittest


ANALYSIS_ROOT = Path(__file__).resolve().parents[1]
if str(ANALYSIS_ROOT) not in sys.path:
    sys.path.insert(0, str(ANALYSIS_ROOT))

from prospero_submission_drafter_layers import builder
from prospero_submission_drafter_layers.models import ManuscriptMetadata
from prospero_submission_drafter_layers.models import PrefillField
from prospero_submission_drafter_layers.models import ProtocolData


def sample_protocol_data() -> ProtocolData:
    return ProtocolData(
        last_updated="2026-03-14",
        registration_id="",
        unresolved_placeholders=[],
        working_title="Sample Review Title",
        review_type="Systematic review",
        objective="Synthesize evidence.",
        core_questions=["Q1"],
        population="Adults",
        concepts_exposures="Sleep quality",
        outcomes="Anxiety",
        study_types="Observational",
        publication_years="2000 onward",
        languages="English",
        exclusion_criteria="None",
        information_sources=["PubMed"],
        gray_sources=[],
        screening_workflow=["Dual screen"],
        extraction_fields=["author", "year"],
        quality_appraisal="JBI",
        synthesis_plan=["Narrative synthesis"],
        reproducibility_notes=["Keep source exports"],
    )


def sample_field() -> PrefillField:
    return PrefillField(
        number=1,
        field_id="review_title",
        label="Review title",
        required=True,
        required_scope="always",
        value="Sample Review Title",
        source_section="Working title",
        notes="",
        status="complete",
    )


class ProsperoSubmissionDrafterBuilderLayerTests(unittest.TestCase):
    def test_parse_date_label_to_iso_supports_month_and_year_inputs(self) -> None:
        self.assertEqual(builder.parse_date_label_to_iso("March 2026"), "2026-03-01")
        self.assertEqual(builder.parse_date_label_to_iso("2026"), "2026-01-01")

    def test_build_submission_context_composes_mode_fields_and_timestamp(self) -> None:
        protocol_data = sample_protocol_data()
        manuscript_metadata = ManuscriptMetadata(
            title="Sample Review Title",
            contact_name="Jane Doe",
            contact_email="jane@example.org",
            affiliation="Institute, Cyprus",
            country="Cyprus",
            date_label="2026-03",
            keywords=["sleep", "anxiety"],
            funding_statement="None",
            conflicts_statement="None",
            data_availability_statement="On request",
        )

        captured: dict[str, object] = {}

        def fake_resolve_registration_mode(*, requested_mode: str, protocol_data: ProtocolData) -> str:
            captured["requested_mode"] = requested_mode
            captured["protocol_data"] = protocol_data
            return "new"

        def fake_build_prefill_fields(
            protocol_data: ProtocolData,
            *,
            registration_mode: str,
            manuscript_metadata: ManuscriptMetadata | None,
            auto_complete: bool,
            profile_values: dict[str, str],
        ) -> list[PrefillField]:
            captured["registration_mode"] = registration_mode
            captured["manuscript_metadata"] = manuscript_metadata
            captured["auto_complete"] = auto_complete
            captured["profile_values"] = profile_values
            return [sample_field()]

        context = builder.build_submission_context(
            protocol_data=protocol_data,
            manuscript_metadata=manuscript_metadata,
            preset_manuscript_path=Path("../04_manuscript/main.tex"),
            requested_registration_mode="auto",
            auto_complete=True,
            profile_values={"country": "Cyprus"},
            resolve_registration_mode_fn=fake_resolve_registration_mode,
            build_prefill_fields_fn=fake_build_prefill_fields,
            now_fn=lambda: datetime(2026, 3, 14, 12, 0),
        )

        self.assertEqual(context.registration_mode, "new")
        self.assertEqual(context.generated_at, "2026-03-14 12:00")
        self.assertEqual(len(context.fields), 1)
        self.assertEqual(captured["requested_mode"], "auto")
        self.assertEqual(captured["registration_mode"], "new")
        self.assertEqual(captured["auto_complete"], True)

    def test_build_draft_artifacts_respects_output_format_and_summary(self) -> None:
        context = builder.DraftBuildContext(
            protocol_data=sample_protocol_data(),
            manuscript_metadata=None,
            preset_manuscript_path=None,
            registration_mode="new",
            fields=[sample_field()],
            generated_at="2026-03-14 12:00",
        )

        calls = {"structured": 0, "xml": 0, "summary": 0}

        def fake_render_structured(**_: object) -> str:
            calls["structured"] += 1
            return "structured"

        def fake_render_xml(**_: object) -> str:
            calls["xml"] += 1
            return "xml"

        def fake_render_summary(**_: object) -> str:
            calls["summary"] += 1
            return "summary"

        artifacts = builder.build_draft_artifacts(
            context=context,
            protocol_path=Path("../01_protocol/protocol.md"),
            structured_output_path=Path("outputs/structured.md"),
            xml_output_path=Path("outputs/structured.xml"),
            preset="none",
            output_format="both",
            render_structured_draft_fn=fake_render_structured,
            render_xml_draft_fn=fake_render_xml,
            render_summary_fn=fake_render_summary,
        )

        self.assertEqual(artifacts.structured_text, "structured")
        self.assertEqual(artifacts.xml_text, "xml")
        self.assertEqual(artifacts.summary_text, "summary")
        self.assertEqual(calls, {"structured": 1, "xml": 1, "summary": 1})

    def test_auto_complete_missing_required_fields_applies_defaults_and_overrides(self) -> None:
        field = PrefillField(
            number=3,
            field_id="participants_population",
            label="Participants/population",
            required=True,
            required_scope="always",
            value="[POPULATION]",
            source_section="Eligibility criteria",
            notes="",
            status="needs_input",
        )
        protocol_data = sample_protocol_data()
        completed = builder.auto_complete_missing_required_fields(
            [field],
            protocol_data=protocol_data,
            manuscript_metadata=None,
            profile_values={"POPULATION": "adolescent outpatients"},
        )

        self.assertEqual(completed[0].value, "adolescent outpatients")
        self.assertEqual(completed[0].field_id, "participants_population")

    def test_completion_counts_and_placeholder_scan_match_expected(self) -> None:
        complete_field = sample_field()
        incomplete_field = PrefillField(
            number=2,
            field_id="participants_population",
            label="Population",
            required=True,
            required_scope="always",
            value="[POPULATION]",
            source_section="Population",
            notes="Needs details",
            status="needs_input",
        )

        counts = builder.completion_counts([complete_field, incomplete_field])
        placeholders = builder.unresolved_placeholders_in_fields([complete_field, incomplete_field])

        self.assertEqual(counts["total_fields"], 2)
        self.assertEqual(counts["required_missing"], 1)
        self.assertEqual(placeholders, ["[POPULATION]"])

    def test_evaluate_exit_conditions_returns_messages_and_code(self) -> None:
        exit_code, messages = builder.evaluate_exit_conditions(
            fail_on_missing_required=True,
            fail_on_placeholders=True,
            auto_complete=True,
            required_missing=1,
            unresolved_in_draft=["[POPULATION]"],
            unresolved_protocol_placeholders=[],
        )

        self.assertEqual(exit_code, 1)
        self.assertIn("required PROSPERO fields", messages[0])
        self.assertIn("unresolved placeholders remain", messages[1])


if __name__ == "__main__":
    unittest.main()
from __future__ import annotations

from pathlib import Path
import sys
import unittest


ANALYSIS_ROOT = Path(__file__).resolve().parents[1]
if str(ANALYSIS_ROOT) not in sys.path:
    sys.path.insert(0, str(ANALYSIS_ROOT))

from prospero_submission_drafter_layers import rules
from prospero_submission_drafter_layers.models import PrefillField
from prospero_submission_drafter_layers.models import ProtocolData


def sample_protocol_data() -> ProtocolData:
    return ProtocolData(
        last_updated="2026-03-14",
        registration_id="",
        unresolved_placeholders=[],
        working_title="Sample review title",
        review_type="Systematic review",
        objective="Objective text",
        core_questions=["Question"],
        population="Adults",
        concepts_exposures="Exposure",
        outcomes="Outcome",
        study_types="Observational",
        publication_years="2000 onward",
        languages="English",
        exclusion_criteria="None",
        information_sources=["PubMed"],
        gray_sources=[],
        screening_workflow=["Dual screening"],
        extraction_fields=["author", "year"],
        quality_appraisal="JBI",
        synthesis_plan=["Narrative synthesis"],
        reproducibility_notes=["Keep exports"],
    )


class ProsperoSubmissionDrafterRulesLayerTests(unittest.TestCase):
    def test_parse_date_label_to_iso_supports_expected_formats(self) -> None:
        self.assertEqual(rules.parse_date_label_to_iso("2026-03-14"), "2026-03-14")
        self.assertEqual(rules.parse_date_label_to_iso("March 2026"), "2026-03-01")
        self.assertEqual(rules.parse_date_label_to_iso("2026"), "2026-01-01")

    def test_auto_complete_applies_placeholder_profile_override(self) -> None:
        fields = [
            PrefillField(
                number=1,
                field_id="participants_population",
                label="Participants",
                required=True,
                required_scope="always",
                value="[POPULATION]",
                source_section="Eligibility",
                notes="",
                status="needs_input",
            )
        ]

        completed = rules.auto_complete_missing_required_fields(
            fields,
            protocol_data=sample_protocol_data(),
            manuscript_metadata=None,
            profile_values={"POPULATION": "adolescent outpatients"},
        )

        self.assertEqual(completed[0].value, "adolescent outpatients")

    def test_finalize_fields_enforces_placeholder_date_and_email_rules(self) -> None:
        fields = [
            PrefillField(
                number=1,
                field_id="participants_population",
                label="Participants",
                required=True,
                required_scope="always",
                value="[POPULATION]",
                source_section="Eligibility",
                notes="",
            ),
            PrefillField(
                number=2,
                field_id="review_start_date",
                label="Start date",
                required=True,
                required_scope="always",
                value="March 2026",
                source_section="Timeline",
                notes="",
            ),
            PrefillField(
                number=3,
                field_id="named_contact_email",
                label="Contact email",
                required=True,
                required_scope="always",
                value="invalid-email",
                source_section="Contact",
                notes="",
            ),
        ]

        finalized = rules.finalize_fields(fields)

        self.assertEqual(finalized[0].status, "needs_input")
        self.assertIn("placeholder", finalized[0].notes.lower())
        self.assertIn("YYYY-MM-DD", finalized[1].notes)
        self.assertIn("invalid", finalized[2].notes.lower())

    def test_completion_and_exit_helpers_return_expected_results(self) -> None:
        fields = [
            PrefillField(
                number=1,
                field_id="review_title",
                label="Title",
                required=True,
                required_scope="always",
                value="Complete",
                source_section="Working title",
                notes="",
                status="complete",
            ),
            PrefillField(
                number=2,
                field_id="participants_population",
                label="Participants",
                required=True,
                required_scope="always",
                value="[POPULATION]",
                source_section="Eligibility",
                notes="",
                status="needs_input",
            ),
        ]

        counts = rules.completion_counts(fields)
        unresolved = rules.unresolved_placeholders_in_fields(fields)
        exit_code, messages = rules.evaluate_exit_conditions(
            fail_on_missing_required=True,
            fail_on_placeholders=True,
            auto_complete=True,
            required_missing=counts["required_missing"],
            unresolved_in_draft=unresolved,
            unresolved_protocol_placeholders=[],
        )

        self.assertEqual(counts["required_missing"], 1)
        self.assertEqual(unresolved, ["[POPULATION]"])
        self.assertEqual(exit_code, 1)
        self.assertEqual(len(messages), 2)


if __name__ == "__main__":
    unittest.main()
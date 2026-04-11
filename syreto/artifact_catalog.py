from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ArtifactCatalogEntry:
    path: str
    producer: str
    consumed_by: tuple[str, ...]
    required: str
    canonical: bool
    schema_ref: str | None
    reproducible: bool
    human_readable: str
    machine_readable: str
    regenerable: bool
    kind: str
    provenance_tracked: bool = False


def _entry(
    *,
    path: str,
    producer: str,
    consumed_by: tuple[str, ...],
    required: str,
    canonical: bool,
    schema_ref: str | None,
    reproducible: bool,
    human_readable: str,
    machine_readable: str,
    regenerable: bool,
    kind: str,
    provenance_tracked: bool = False,
) -> ArtifactCatalogEntry:
    return ArtifactCatalogEntry(
        path=path,
        producer=producer,
        consumed_by=consumed_by,
        required=required,
        canonical=canonical,
        schema_ref=schema_ref,
        reproducible=reproducible,
        human_readable=human_readable,
        machine_readable=machine_readable,
        regenerable=regenerable,
        kind=kind,
        provenance_tracked=provenance_tracked,
    )


ARTIFACT_CATALOG: tuple[ArtifactCatalogEntry, ...] = (
    _entry(
        path="02_data/processed/search_log.csv",
        producer="review team / canonical inputs",
        consumed_by=("validation", "pipeline", "status", "doctor"),
        required="yes",
        canonical=True,
        schema_ref="syreto/csv_schema.py",
        reproducible=False,
        human_readable="yes",
        machine_readable="yes",
        regenerable=False,
        kind="input",
    ),
    _entry(
        path="02_data/processed/master_records.csv",
        producer="review team / dedup workflow",
        consumed_by=("status", "RIS export", "screening logic", "doctor"),
        required="yes",
        canonical=True,
        schema_ref="syreto/csv_schema.py",
        reproducible=False,
        human_readable="yes",
        machine_readable="yes",
        regenerable=False,
        kind="input",
    ),
    _entry(
        path="02_data/codebook/extraction_template.csv",
        producer="review team / extraction workflow",
        consumed_by=("appraisal", "synthesis", "analytics", "export", "doctor"),
        required="yes",
        canonical=True,
        schema_ref="syreto/csv_schema.py",
        reproducible=False,
        human_readable="yes",
        machine_readable="yes",
        regenerable=False,
        kind="input",
    ),
    _entry(
        path="outputs/status_summary.json",
        producer="status_report.py",
        consumed_by=("status_cli.py", "doctor", "todo_action_plan_builder.py", "users"),
        required="yes",
        canonical=False,
        schema_ref="docs/execution-contract.md",
        reproducible=True,
        human_readable="limited",
        machine_readable="yes",
        regenerable=True,
        kind="operational",
        provenance_tracked=True,
    ),
    _entry(
        path="outputs/status_report.md",
        producer="status_report.py",
        consumed_by=("users", "postmortem", "review workflow"),
        required="yes",
        canonical=False,
        schema_ref="docs/execution-contract.md",
        reproducible=True,
        human_readable="yes",
        machine_readable="no",
        regenerable=True,
        kind="operational",
        provenance_tracked=True,
    ),
    _entry(
        path="outputs/todo_action_plan.md",
        producer="todo_action_plan_builder.py",
        consumed_by=("users", "remediation workflow"),
        required="yes",
        canonical=False,
        schema_ref="docs/execution-contract.md",
        reproducible=True,
        human_readable="yes",
        machine_readable="no",
        regenerable=True,
        kind="operational",
        provenance_tracked=True,
    ),
    _entry(
        path="outputs/run_events.jsonl",
        producer="daily_run.sh observability layer",
        consumed_by=("syreto observability", "postmortem", "future UI/metrics"),
        required="expected for full run",
        canonical=False,
        schema_ref="docs/observability-model.md",
        reproducible=True,
        human_readable="partial",
        machine_readable="yes",
        regenerable=True,
        kind="operational",
    ),
    _entry(
        path="outputs/daily_run_manifest.json",
        producer="daily_run.sh",
        consumed_by=("run integrity checks", "postmortem", "doctor"),
        required="expected for full run",
        canonical=False,
        schema_ref="docs/execution-contract.md",
        reproducible=True,
        human_readable="limited",
        machine_readable="yes",
        regenerable=True,
        kind="operational",
        provenance_tracked=True,
    ),
    _entry(
        path="outputs/review_descriptives.json",
        producer="review_descriptives_builder.py",
        consumed_by=("analytics inspection", "future programmatic analytics consumers"),
        required="no",
        canonical=False,
        schema_ref="docs/review-analytics-model.md",
        reproducible=True,
        human_readable="limited",
        machine_readable="yes",
        regenerable=True,
        kind="operational",
        provenance_tracked=True,
    ),
    _entry(
        path="outputs/review_descriptives.md",
        producer="review_descriptives_builder.py",
        consumed_by=("users", "review inspection"),
        required="no",
        canonical=False,
        schema_ref="docs/review-analytics-model.md",
        reproducible=True,
        human_readable="yes",
        machine_readable="no",
        regenerable=True,
        kind="operational",
        provenance_tracked=True,
    ),
    _entry(
        path="outputs/figures/year_distribution.png",
        producer="analytics builders",
        consumed_by=("users", "reporting", "sanity checks"),
        required="no",
        canonical=False,
        schema_ref="docs/review-analytics-model.md",
        reproducible=True,
        human_readable="yes",
        machine_readable="no",
        regenerable=True,
        kind="operational",
    ),
    _entry(
        path="outputs/figures/study_design_distribution.png",
        producer="analytics builders",
        consumed_by=("users", "reporting", "sanity checks"),
        required="no",
        canonical=False,
        schema_ref="docs/review-analytics-model.md",
        reproducible=True,
        human_readable="yes",
        machine_readable="no",
        regenerable=True,
        kind="operational",
    ),
    _entry(
        path="outputs/figures/country_distribution.png",
        producer="analytics builders",
        consumed_by=("users", "reporting", "sanity checks"),
        required="no",
        canonical=False,
        schema_ref="docs/review-analytics-model.md",
        reproducible=True,
        human_readable="yes",
        machine_readable="no",
        regenerable=True,
        kind="operational",
    ),
    _entry(
        path="outputs/figures/quality_band_distribution.png",
        producer="analytics builders",
        consumed_by=("users", "reporting", "sanity checks"),
        required="no",
        canonical=False,
        schema_ref="docs/review-analytics-model.md",
        reproducible=True,
        human_readable="yes",
        machine_readable="no",
        regenerable=True,
        kind="operational",
    ),
    _entry(
        path="outputs/figures/predictor_outcome_heatmap.png",
        producer="analytics builders",
        consumed_by=("users", "reporting", "sanity checks"),
        required="no",
        canonical=False,
        schema_ref="docs/review-analytics-model.md",
        reproducible=True,
        human_readable="yes",
        machine_readable="no",
        regenerable=True,
        kind="operational",
    ),
    _entry(
        path="outputs/prisma_flow_diagram.svg",
        producer="dedup_stats.py",
        consumed_by=("users", "manuscript prep"),
        required="expected in normal run",
        canonical=False,
        schema_ref="docs/execution-contract.md",
        reproducible=True,
        human_readable="yes",
        machine_readable="no",
        regenerable=True,
        kind="operational",
    ),
    _entry(
        path="outputs/prisma_flow_diagram.tex",
        producer="dedup_stats.py",
        consumed_by=("manuscript workflows",),
        required="expected in normal run",
        canonical=False,
        schema_ref="docs/execution-contract.md",
        reproducible=True,
        human_readable="yes",
        machine_readable="no",
        regenerable=True,
        kind="operational",
    ),
    _entry(
        path="outputs/forest_plot_data.csv",
        producer="forest_plot_generator.py",
        consumed_by=("forest plot output layer", "downstream inspection"),
        required="optional/depends on stage",
        canonical=False,
        schema_ref="docs/study-table-model.md",
        reproducible=True,
        human_readable="yes",
        machine_readable="yes",
        regenerable=True,
        kind="operational",
    ),
    _entry(
        path="outputs/forest_plot.png",
        producer="forest_plot_generator.py",
        consumed_by=("users", "manuscript/reporting"),
        required="optional/depends on stage",
        canonical=False,
        schema_ref=None,
        reproducible=True,
        human_readable="yes",
        machine_readable="no",
        regenerable=True,
        kind="operational",
    ),
    _entry(
        path="outputs/included_studies_export.ris",
        producer="export_to_ris.py",
        consumed_by=("Zotero", "EndNote", "external reference workflows"),
        required="optional",
        canonical=False,
        schema_ref="docs/study-table-model.md",
        reproducible=True,
        human_readable="yes",
        machine_readable="semi-structured",
        regenerable=True,
        kind="operational",
    ),
    _entry(
        path="04_manuscript/tables/grade_evidence_profile_table.tex",
        producer="grade_evidence_profiler.py",
        consumed_by=("manuscript layer",),
        required="expected when manuscript layer is active",
        canonical=False,
        schema_ref="docs/execution-contract.md",
        reproducible=True,
        human_readable="yes",
        machine_readable="no",
        regenerable=True,
        kind="manuscript",
    ),
    _entry(
        path="04_manuscript/tables/results_summary_table.tex",
        producer="results_summary_table_builder.py",
        consumed_by=("manuscript layer",),
        required="expected when manuscript layer is active",
        canonical=False,
        schema_ref="docs/study-table-model.md",
        reproducible=True,
        human_readable="yes",
        machine_readable="no",
        regenerable=True,
        kind="manuscript",
        provenance_tracked=True,
    ),
    _entry(
        path="04_manuscript/sections/03c_interpretation_auto.tex",
        producer="results_interpretation_layer.py",
        consumed_by=("manuscript layer",),
        required="expected when manuscript layer is active",
        canonical=False,
        schema_ref="docs/execution-contract.md",
        reproducible=True,
        human_readable="yes",
        machine_readable="no",
        regenerable=True,
        kind="manuscript",
    ),
)


def artifact_catalog_entries(*, include_inputs: bool = False) -> tuple[ArtifactCatalogEntry, ...]:
    if include_inputs:
        return ARTIFACT_CATALOG
    return tuple(entry for entry in ARTIFACT_CATALOG if entry.kind != "input")


def artifact_groups() -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {"operational": [], "manuscript": []}
    for entry in ARTIFACT_CATALOG:
        if entry.kind in grouped:
            grouped[entry.kind].append(entry.path)
    return {name: tuple(paths) for name, paths in grouped.items()}


def provenance_tracked_artifacts() -> set[str]:
    return {entry.path for entry in ARTIFACT_CATALOG if entry.provenance_tracked}


def required_artifact_entries(*, include_expected: bool = True) -> tuple[ArtifactCatalogEntry, ...]:
    required_labels = {"yes"}
    if include_expected:
        required_labels.add("expected for full run")
        required_labels.add("expected in normal run")
        required_labels.add("expected when manuscript layer is active")
    return tuple(entry for entry in ARTIFACT_CATALOG if entry.required in required_labels)


def write_machine_readable_catalog(output_path: Path) -> None:
    payload = {
        "schema_version": "0.1",
        "entries": [
            {
                "path": entry.path,
                "producer": entry.producer,
                "consumed_by": list(entry.consumed_by),
                "required": entry.required,
                "canonical": entry.canonical,
                "schema_ref": entry.schema_ref,
                "reproducible": entry.reproducible,
                "human_readable": entry.human_readable,
                "machine_readable": entry.machine_readable,
                "regenerable": entry.regenerable,
                "kind": entry.kind,
                "provenance_tracked": entry.provenance_tracked,
            }
            for entry in ARTIFACT_CATALOG
        ],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def artifact_catalog_markdown_table() -> str:
    header = (
        "| Artifact | Producer | Consumed by | Required | Canonical | "
        "Schema ref | Reproducible | Human-readable | Machine-readable | Regenerable |"
    )
    divider = "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
    lines = [header, divider]
    for entry in ARTIFACT_CATALOG:
        schema_ref = "none"
        if entry.schema_ref:
            schema_path = entry.schema_ref
            absolute_path = Path(__file__).resolve().parents[1] / schema_path
            schema_ref = f"[{schema_path}]({absolute_path.as_posix()})"
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{entry.path}`",
                    entry.producer,
                    ", ".join(entry.consumed_by),
                    entry.required,
                    f"`{str(entry.canonical).lower()}`",
                    schema_ref,
                    f"`{str(entry.reproducible).lower()}`",
                    entry.human_readable,
                    entry.machine_readable,
                    "yes" if entry.regenerable else "no",
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def sync_artifact_catalog_doc(doc_path: Path) -> None:
    start_marker = "<!-- ARTIFACT_CATALOG_TABLE:START -->"
    end_marker = "<!-- ARTIFACT_CATALOG_TABLE:END -->"
    rendered_table = artifact_catalog_markdown_table()
    replacement = f"{start_marker}\n{rendered_table}\n{end_marker}"

    text = doc_path.read_text(encoding="utf-8")
    if start_marker not in text or end_marker not in text:
        raise ValueError("Artifact catalog doc is missing table sync markers.")

    before, remainder = text.split(start_marker, maxsplit=1)
    _, after = remainder.split(end_marker, maxsplit=1)
    doc_path.write_text(before + replacement + after, encoding="utf-8")


def sync_artifact_catalog_surfaces(
    *,
    doc_path: Path,
    json_path: Path,
) -> tuple[Path, Path]:
    sync_artifact_catalog_doc(doc_path)
    write_machine_readable_catalog(json_path)
    return doc_path, json_path

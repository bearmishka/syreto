# Provenance Layer

## Purpose

SyReTo uses a minimal provenance layer for selected generated artifacts.

The goal is not to introduce a separate provenance framework or a hidden runtime object model. The goal is simply to make a generated artifact legible at the file boundary:

- what artifact was produced
- when it was produced
- which script produced it
- which upstream inputs it depended on
- which review mode was active
- which review config defined the review instance, when available

## Representation

The current provenance layer uses sidecar JSON files stored next to generated artifacts:

```text
outputs/review_descriptives.json
outputs/review_descriptives.json.provenance.json
```

This keeps provenance:

- append-free and inspectable
- easy to diff in Git
- easy to parse in tests or future tooling
- separate from artifact payloads that already have their own schemas

## Minimal Sidecar Fields

Current sidecars may contain:

- `artifact_path`
- `generated_at_utc`
- `generated_by`
- `upstream_inputs`
- `review_mode`
- `review_config`
- `review_id`
- `output_profile`

Only the first five are part of the minimal provenance contract. The review-config-derived fields are best-effort enrichments when a valid `review.toml` is available.

`syreto doctor` validates this minimal contract for tracked artifacts. In the current posture, malformed or incomplete provenance sidecars are treated as warning-level provenance/schema problems rather than execution-controlling failures.

The CLI also exposes provenance triage directly:

- `syreto artifacts --provenance-missing-only`
- `syreto artifacts --provenance-invalid-only`

`syreto observability` also surfaces a provenance snapshot for outputs touched by recent run events, along with a small present/missing/invalid summary. `syreto doctor` reports the same kind of compact summary for tracked trust-bearing artifacts. This connects execution history to artifact-level provenance without changing execution semantics.

## Scope

The provenance layer is intentionally selective.

It is most useful for trust-bearing generated artifacts such as:

- machine-readable operational summaries
- human-facing review summaries
- stable tabular outputs used by downstream steps

It does not need to cover every transient or low-value file before it proves useful.

## Design Constraints

The provenance layer MUST remain a presentation and audit layer.

It MUST NOT:

- define execution logic
- alter pipeline control flow
- become a hidden source of truth
- replace canonical review inputs

Files remain canonical. Provenance sidecars only explain how generated artifacts came to exist.

## Current First Slice

The first implemented slice writes provenance sidecars for:

- `outputs/review_descriptives.json`
- `outputs/review_descriptives.md`
- `outputs/reviewer_workload_plan.csv`
- `outputs/reviewer_workload_balancer_summary.md`
- `outputs/results_summary_table.csv`
- `04_manuscript/tables/results_summary_table.tex`
- `outputs/results_summary_table_summary.md`
- `04_manuscript/tables/prisma_counts_table.tex`
- `04_manuscript/tables/fulltext_exclusion_table.tex`
- `outputs/prisma_tables_summary.md`
- `outputs/status_summary.json`
- `outputs/status_report.md`
- `outputs/todo_action_plan.md`
- `outputs/daily_run_manifest.json`
- `outputs/progress_history.csv`
- `outputs/progress_history_summary.md`

This is enough to establish a testable contract without turning provenance into a new subsystem.

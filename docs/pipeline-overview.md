# Pipeline Overview

## Purpose

SyReTo treats a systematic review as a deterministic production pipeline with explicit state transitions, operational checks, and manuscript-facing outputs.

The goal is not just to run analysis scripts, but to maintain a review in a state that is:

- reproducible
- auditable
- status-checkable
- manuscript-synchronized

## Core Model

The pipeline is built around a simple contract:

1. canonical review state lives in CSV and Markdown files under version control
2. analysis scripts transform that state into validated intermediate artifacts
3. status and integrity layers summarize whether the review is operationally healthy
4. manuscript-facing `.tex` and narrative outputs are regenerated from the review state

In practice, this means SyReTo is closer to a build system for systematic reviews than to a notebook collection.

## Main Stages

The end-to-end orchestration entrypoint is [`03_analysis/daily_run.sh`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/daily_run.sh).

At a high level, the pipeline moves through these stages:

### 1. Screening State Consolidation

This stage consolidates title/abstract consensus and validates input consistency.

Representative scripts:

- `consolidate_title_abstract_consensus.py`
- `validate_csv_inputs.py`
- `screening_metrics.py`
- `screening_disagreement_analyzer.py`

### 2. Integrity Guards

These steps check whether the review process itself remains trustworthy.

Representative scripts:

- `audit_log_integrity_guard.py`
- `record_id_map_integrity_guard.py`
- `template_term_guard.py`
- `epistemic_consistency_guard.py`

These guards are part of SyReTo’s operational contract, not optional polish.

### 3. Extraction and Appraisal

This stage validates structured extraction data and computes appraisal-derived artifacts.

Representative scripts:

- `validate_extraction.py`
- `quality_appraisal.py`
- `grade_evidence_profiler.py`

### 4. Quantitative Synthesis and Reporting

This stage harmonizes effect sizes, builds results summaries, and generates figures and narrative layers.

Representative scripts:

- `effect_size_converter.py`
- `meta_analysis_results_builder.py`
- `results_summary_table_builder.py`
- `forest_plot_generator.py`
- `results_interpretation_layer.py`

### 5. Study and Review Traceability

This stage creates lineage and transparency artifacts that help connect upstream decisions to downstream outputs.

Representative scripts:

- `analysis_lineage.py`
- `study_flow_map_builder.py`
- `transparency_appendix_decision_trace.py`
- `progress_history_builder.py`

### 6. Review Operations and Status

This stage summarizes project health, blockers, TODOs, and generated artifacts.

Representative scripts:

- `status_report.py`
- `status_cli.py`
- `todo_action_plan_builder.py`
- `weekly_risk_digest.py`

### 7. Manuscript Synchronization

This stage regenerates manuscript-facing tables and sections rather than treating them as manually maintained documents.

Representative scripts:

- `synthesis_tables.py`
- `prisma_tables.py`
- `grade_evidence_profiler.py`
- `results_summary_table_builder.py`
- `results_interpretation_layer.py`

### 8. Optional Operational Extensions

These stages can be enabled when needed without changing the core contract of the pipeline.

- citation tracking
- PROSPERO draft generation
- retraction checking
- living-review scheduling
- keyword analysis
- multilingual query translation

## Main Entry Points

There are two practical ways to interact with the system.

### Packaged CLI

The packaged CLI is defined in [`pyproject.toml`](/Users/pigra/Documents/New%20project/syreto_clean/pyproject.toml) and implemented in [`syreto/cli.py`](/Users/pigra/Documents/New%20project/syreto_clean/syreto/cli.py).

Current public entry points:

- `syreto`
- `syreto-status`
- `syreto-draft`

Important subcommands on `syreto` now include:

- `syreto status`
- `syreto artifacts`
- `syreto validate`
- `syreto doctor`
- `syreto analytics`
- `syreto review run`

These are useful for discovery, status checks, review-state analytics, and running packaged scripts.

### Full Orchestration

For a full pipeline run, the operational entrypoint is:

```bash
syreto review run
```

This invokes [`03_analysis/daily_run.sh`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/daily_run.sh) and is the primary “system run” interface.

## Source of Truth

SyReTo depends on keeping source-of-truth boundaries clear.

- `02_data/` contains canonical review data inputs and processed review-state CSVs
- `03_analysis/` contains orchestration and analysis scripts
- `outputs/` contains operational summaries and generated intermediate artifacts
- `04_manuscript/` is the canonical target location for manuscript-facing generated outputs when present in the working repository

Downstream outputs should be regenerated from source inputs rather than treated as primary editable state.

## Operational Success Condition

A pipeline run is considered successful when:

- the orchestrator exits cleanly
- status artifacts are generated
- integrity guards do not produce blocker failures for the configured review mode
- expected manuscript-facing outputs are regenerated
- the status layer reports an acceptable health state for the configured fail threshold

The best quick operational check is:

```bash
syreto doctor
syreto status
syreto analytics
```

For deeper inspection:

```bash
cd 03_analysis
python status_cli.py --input outputs/status_summary.json
```

For the future review-aware configuration layer, see [review-instance-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/review-instance-model.md).

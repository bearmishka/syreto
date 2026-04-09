# Artifact Catalog

## Purpose

This page describes the main artifact classes produced and consumed by SyReTo, with an emphasis on which files act as canonical inputs, which files are generated outputs, and which files are used to assess operational health.

It is both:

- a human-facing inventory of important artifacts
- a lightweight artifact contract for the most important operational and manuscript-facing files

## Artifact Contract Fields

For the highest-value artifacts, this page tracks:

- `artifact`: the path or artifact family
- `producer`: which script or layer creates it
- `consumer`: which script, layer, or user-facing surface reads it
- `required`: whether the artifact is expected for a trustworthy run in the current contract
- `canonical`: whether the artifact is source-of-truth or merely derived
- `human-readable`: whether a person can inspect it directly
- `machine-readable`: whether downstream code can rely on structured parsing
- `regenerable`: whether it should be recreated from upstream state rather than hand-maintained

## Core Artifact Contract

| Artifact | Producer | Consumer | Required | Canonical | Human-readable | Machine-readable | Regenerable |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `02_data/processed/search_log.csv` | review team / canonical inputs | validation, pipeline, status | yes | yes | yes | yes | no |
| `02_data/processed/master_records.csv` | review team / dedup workflow | status, RIS export, screening logic | yes | yes | yes | yes | no |
| `02_data/codebook/extraction_template.csv` | review team / extraction workflow | appraisal, synthesis, analytics, export | yes | yes | yes | yes | no |
| `outputs/status_summary.json` | `status_report.py` | `status_cli.py`, `doctor`, `todo_action_plan_builder.py`, users | yes | no | limited | yes | yes |
| `outputs/status_report.md` | `status_report.py` | users, postmortem, review workflow | yes | no | yes | no | yes |
| `outputs/todo_action_plan.md` | `todo_action_plan_builder.py` | users, remediation workflow | yes | no | yes | no | yes |
| `outputs/run_events.jsonl` | `daily_run.sh` observability layer | `syreto observability`, postmortem, future UI/metrics | expected for full run | no | partial | yes | yes |
| `outputs/daily_run_manifest.json` | `daily_run.sh` | run integrity checks, postmortem | expected for full run | no | limited | yes | yes |
| `outputs/review_descriptives.json` | `review_descriptives_builder.py` | analytics inspection, future programmatic analytics consumers | no | no | limited | yes | yes |
| `outputs/review_descriptives.md` | `review_descriptives_builder.py` | users, review inspection | no | no | yes | no | yes |
| `outputs/figures/*.png` | analytics builders | users, reporting, sanity checks | no | no | yes | no | yes |
| `outputs/prisma_flow_diagram.svg` | `dedup_stats.py` | users, manuscript prep | expected in normal run | no | yes | no | yes |
| `outputs/prisma_flow_diagram.tex` | `dedup_stats.py` | manuscript workflows | expected in normal run | no | yes | no | yes |
| `outputs/forest_plot_data.csv` | `forest_plot_generator.py` | forest plot output layer, downstream inspection | optional/depends on stage | no | yes | yes | yes |
| `outputs/forest_plot.png` | `forest_plot_generator.py` | users, manuscript/reporting | optional/depends on stage | no | yes | no | yes |
| `outputs/included_studies_export.ris` | `export_to_ris.py` | Zotero, EndNote, external reference workflows | optional | no | yes | semi-structured | yes |
| `04_manuscript/tables/grade_evidence_profile_table.tex` | `grade_evidence_profiler.py` | manuscript layer | expected when manuscript layer is active | no | yes | no | yes |
| `04_manuscript/tables/results_summary_table.tex` | `results_summary_table_builder.py` | manuscript layer | expected when manuscript layer is active | no | yes | no | yes |
| `04_manuscript/sections/03c_interpretation_auto.tex` | `results_interpretation_layer.py` | manuscript layer | expected when manuscript layer is active | no | yes | no | yes |

## Artifact Classes

SyReTo uses four main artifact classes:

1. canonical review-state inputs
2. generated operational outputs
3. manuscript-facing generated outputs
4. optional extension artifacts

## 1. Canonical Review-State Inputs

These files represent the review state that the pipeline reads and validates.

Primary examples in [`02_data/`](/Users/pigra/Documents/New%20project/syreto_clean/02_data):

- [`02_data/processed/search_log.csv`](/Users/pigra/Documents/New%20project/syreto_clean/02_data/processed/search_log.csv)
- [`02_data/processed/master_records.csv`](/Users/pigra/Documents/New%20project/syreto_clean/02_data/processed/master_records.csv)
- [`02_data/processed/screening_title_abstract_dual_log.csv`](/Users/pigra/Documents/New%20project/syreto_clean/02_data/processed/screening_title_abstract_dual_log.csv)
- [`02_data/processed/screening_title_abstract_results.csv`](/Users/pigra/Documents/New%20project/syreto_clean/02_data/processed/screening_title_abstract_results.csv)
- [`02_data/processed/screening_fulltext_log.csv`](/Users/pigra/Documents/New%20project/syreto_clean/02_data/processed/screening_fulltext_log.csv)
- [`02_data/processed/decision_log.csv`](/Users/pigra/Documents/New%20project/syreto_clean/02_data/processed/decision_log.csv)
- [`02_data/processed/prisma_counts_template.csv`](/Users/pigra/Documents/New%20project/syreto_clean/02_data/processed/prisma_counts_template.csv)
- [`02_data/processed/full_text_exclusion_reasons.csv`](/Users/pigra/Documents/New%20project/syreto_clean/02_data/processed/full_text_exclusion_reasons.csv)
- [`02_data/processed/audit_log.csv`](/Users/pigra/Documents/New%20project/syreto_clean/02_data/processed/audit_log.csv)
- [`02_data/processed/record_id_map.csv`](/Users/pigra/Documents/New%20project/syreto_clean/02_data/processed/record_id_map.csv)
- [`02_data/codebook/extraction_template.csv`](/Users/pigra/Documents/New%20project/syreto_clean/02_data/codebook/extraction_template.csv)

These files should be treated as operational inputs, not disposable intermediates.

## 2. Generated Operational Outputs

These artifacts summarize health, progress, consistency, and run outcomes.

Common outputs under [`outputs/`](/Users/pigra/Documents/New%20project/syreto_clean/outputs):

- [`outputs/status_summary.json`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/status_summary.json)
- [`outputs/status_report.md`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/status_report.md)
- [`outputs/todo_action_plan.md`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/todo_action_plan.md)
- [`outputs/review_descriptives.json`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/review_descriptives.json)
- [`outputs/review_descriptives.md`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/review_descriptives.md)
- [`outputs/progress_history.csv`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/progress_history.csv)
- [`outputs/progress_history_summary.md`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/progress_history_summary.md)
- [`outputs/dedup_merge_summary.md`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/dedup_merge_summary.md)
- [`outputs/dedup_stats_summary.md`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/dedup_stats_summary.md)
- [`outputs/epistemic_consistency_report.md`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/epistemic_consistency_report.md)
- [`outputs/prisma_flow_diagram.svg`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/prisma_flow_diagram.svg)
- [`outputs/prisma_flow_diagram.tex`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/prisma_flow_diagram.tex)
- [`outputs/figures/year_distribution.png`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/figures/year_distribution.png)
- [`outputs/figures/study_design_distribution.png`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/figures/study_design_distribution.png)
- [`outputs/figures/country_distribution.png`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/figures/country_distribution.png)
- [`outputs/figures/quality_band_distribution.png`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/figures/quality_band_distribution.png)
- [`outputs/figures/predictor_outcome_heatmap.png`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/figures/predictor_outcome_heatmap.png)

These are the first files to inspect when asking “did the run work?”

The `review_descriptives.*` and `outputs/figures/` artifacts are specifically intended to answer a different question:

- what does the current included-study corpus look like as a dataset?

## 3. Manuscript-Facing Generated Outputs

These artifacts are generated from review-state inputs and are intended to be consumed by manuscript workflows.

Canonical target outputs include:

- `04_manuscript/tables/prisma_counts_table.tex`
- `04_manuscript/tables/fulltext_exclusion_table.tex`
- `04_manuscript/tables/study_characteristics_table.tex`
- `04_manuscript/tables/grade_evidence_profile_table.tex`
- `04_manuscript/tables/results_summary_table.tex`
- `04_manuscript/tables/decision_trace_table.tex`
- `04_manuscript/tables/analysis_trace_table.tex`
- `04_manuscript/sections/03c_interpretation_auto.tex`

In the current checkout, `04_manuscript/` may be absent, but these paths remain the intended output targets in the pipeline contract.

## 4. Optional Extension Artifacts

Depending on enabled stages, SyReTo may also generate:

- PROSPERO draft outputs
- citation tracking outputs
- living-review scheduler outputs
- retraction check outputs
- keyword analysis outputs

Examples from the orchestration layer:

- `outputs/prospero_registration_prefill.md`
- `outputs/prospero_registration_prefill.xml`
- `outputs/citation_forward.csv`
- `outputs/citation_backward.csv`
- `outputs/living_review_schedule.csv`
- `outputs/retraction_check_results.csv`
- `outputs/keyword_candidates.csv`

## Artifact Roles

It helps to think of each artifact in one of these roles:

- source-of-truth input
- generated operational summary
- generated manuscript asset
- optional extension output

The key discipline is that generated artifacts should be reproducible from canonical inputs and scripted execution.

## How To Read This Catalog

This catalog is intentionally not just a file list.

Use it to answer questions such as:

- which artifacts are canonical inputs and must not be hand-edited downstream
- which artifacts are generated evidence of a run rather than source-of-truth
- which outputs are primarily for humans and which are meant for structured machine consumption
- which missing files are contract problems versus optional omissions
- which artifacts can be safely regenerated from upstream state

This makes the catalog a companion to:

- [execution-contract.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/execution-contract.md)
- [failure-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/failure-model.md)
- [observability-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/observability-model.md)

## Fast Inspection Checklist

If you need to assess the state of a run quickly, inspect these first:

1. [`outputs/status_summary.json`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/status_summary.json)
2. [`outputs/status_report.md`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/status_report.md)
3. [`outputs/todo_action_plan.md`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/todo_action_plan.md)
4. [`outputs/prisma_flow_diagram.svg`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/prisma_flow_diagram.svg)
5. the expected `.tex` outputs for the manuscript layer

If you want a quick inspection of corpus shape rather than operational health, inspect:

1. [`outputs/review_descriptives.json`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/review_descriptives.json)
2. [`outputs/review_descriptives.md`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/review_descriptives.md)
3. [`outputs/figures/year_distribution.png`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/figures/year_distribution.png)
4. [`outputs/figures/study_design_distribution.png`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/figures/study_design_distribution.png)
5. [`outputs/figures/country_distribution.png`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/figures/country_distribution.png)
6. [`outputs/figures/quality_band_distribution.png`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/figures/quality_band_distribution.png)
7. [`outputs/figures/predictor_outcome_heatmap.png`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/figures/predictor_outcome_heatmap.png)

## Related Entry Points

Operational summaries are surfaced through:

- [`syreto/cli.py`](/Users/pigra/Documents/New%20project/syreto_clean/syreto/cli.py)
- [`03_analysis/status_cli.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/status_cli.py)
- [`03_analysis/status_report.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/status_report.py)
- [`03_analysis/daily_run.sh`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/daily_run.sh)
- [`03_analysis/review_descriptives_builder.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/review_descriptives_builder.py)

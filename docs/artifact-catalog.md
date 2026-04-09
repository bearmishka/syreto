# Artifact Catalog

## Purpose

This page describes the main artifact classes produced and consumed by SyReTo, with an emphasis on which files act as canonical inputs, which files are generated outputs, and which files are used to assess operational health.

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

## Related Entry Points

Operational summaries are surfaced through:

- [`syreto/cli.py`](/Users/pigra/Documents/New%20project/syreto_clean/syreto/cli.py)
- [`03_analysis/status_cli.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/status_cli.py)
- [`03_analysis/status_report.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/status_report.py)
- [`03_analysis/daily_run.sh`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/daily_run.sh)
- [`03_analysis/review_descriptives_builder.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/review_descriptives_builder.py)

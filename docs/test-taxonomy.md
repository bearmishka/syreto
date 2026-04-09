# Test Taxonomy

## Purpose

This page defines the testing taxonomy of SyReTo.

It answers a more useful question than “does the project have tests?”:

`which kinds of operational risk are tested, and by which classes of tests?`

This matters because SyReTo is not just a reusable library. It is an operational scientific system whose trust depends on validation, status, guards, artifacts, rerun semantics, and pipeline behavior.

## Why A Test Taxonomy Matters

For a general-purpose library, test coverage is often discussed in terms of units and integration.

For SyReTo, that is not enough.

The system also needs tests that answer:

- do canonical schemas hold
- do contract-bearing artifacts stay stable
- do CLI surfaces remain thin and correct
- do integrity guards fail when trust boundaries are violated
- do pipeline checkpoints behave correctly across modes
- do failed or partial runs leave the right markers
- can rerun and rollback semantics be interpreted safely

That is why the testing surface needs a taxonomy, not just a count.

## Main Test Classes

The current SyReTo testing surface can be understood through the following main classes.

### 1. Unit Tests

These test bounded functions or modules in isolation.

Typical examples:

- effect conversion logic
- study-table harmonization helpers
- reporting-layer transformations
- analytics builders
- appraisal conversion utilities

Representative files:

- [test_study_table.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_study_table.py)
- [test_review_descriptives_builder.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_review_descriptives_builder.py)
- [test_grade_evidence_profiler.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_grade_evidence_profiler.py)
- [test_results_summary_table_builder.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_results_summary_table_builder.py)

### 2. Schema Tests

These test whether canonical inputs, expected columns, and backward-compatible harmonization rules still hold.

Typical examples:

- CSV validation assumptions
- extraction harmonization
- schema-gap handling
- legacy-column compatibility

Representative files:

- [test_validate_csv_inputs_none_handling.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_validate_csv_inputs_none_handling.py)
- [test_validate_extraction_harmonization.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_validate_extraction_harmonization.py)
- [test_forest_plot_generator.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_forest_plot_generator.py)

### 3. Artifact Contract Tests

These test whether contract-bearing outputs are generated, interpreted, or classified correctly.

Typical examples:

- status summary semantics
- status report posture and run-integrity findings
- review descriptives outputs
- manuscript-facing derived artifacts

Representative files:

- [test_status_cli_operational_integrity.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_status_cli_operational_integrity.py)
- [test_status_report_run_integrity.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_status_report_run_integrity.py)
- [test_status_report_project_posture.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_status_report_project_posture.py)
- [test_review_descriptives_builder.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_review_descriptives_builder.py)

### 4. CLI Smoke Tests

These test that the public CLI surface stays usable and thin.

They do not try to re-implement the whole pipeline through mocks; they check that the CLI routes correctly, loads config, exposes the right commands, and reports the right semantics.

Representative files:

- [test_syreto_package_import.py](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/tests/test_syreto_package_import.py)
- [test_status_cli_stage_output.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_status_cli_stage_output.py)

### 5. Pipeline Smoke Tests

These test the orchestration spine and mandatory checkpoints at a run level.

Typical examples:

- mode-dependent behavior
- final status checkpoint behavior
- manifest generation
- run-integrity markers
- single-object smoke around the daily-run manifest

Representative files:

- [test_daily_run_manifest_single_object_smoke.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_daily_run_manifest_single_object_smoke.py)
- [test_daily_run_status_checkpoint.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_daily_run_status_checkpoint.py)
- [test_daily_run_preflight_modes.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_daily_run_preflight_modes.py)
- [test_daily_run_priority_policy_defaults.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_daily_run_priority_policy_defaults.py)

### 6. Integrity Guard Tests

These test the guard layer directly.

Typical examples:

- audit log integrity
- record-id map integrity
- epistemic consistency guard behavior
- template leakage detection
- Python source integrity

Representative files:

- [test_audit_log_integrity_guard.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_audit_log_integrity_guard.py)
- [test_record_id_map_integrity.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_record_id_map_integrity.py)
- [test_epistemic_consistency_guard.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_epistemic_consistency_guard.py)
- [test_template_term_guard.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_template_term_guard.py)
- [test_python_source_integrity.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_python_source_integrity.py)

### 7. Golden Or Stable Output Tests

SyReTo does not rely heavily on large frozen snapshot suites, but it already contains tests that act like golden/stable-output checks by asserting expected rendered content, expected markers, or stable generated structure.

Typical examples:

- stage-aligned status rendering
- stable decision-trace outputs
- expected manuscript-facing content shape
- stable summary/report posture outputs

Representative files:

- [test_status_report_stage_alignment.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_status_report_stage_alignment.py)
- [test_transparency_appendix_decision_trace.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_transparency_appendix_decision_trace.py)
- [test_results_interpretation_layer.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_results_interpretation_layer.py)

### 8. Rerun, Recovery, And Failure-State Tests

These test whether failed or partial runs leave the right recoverability signals.

Typical examples:

- atomic output recovery
- failed-run markers
- rollback-related behavior
- stale or partial run semantics

Representative files:

- [test_daily_run_atomic_output_recovery.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_daily_run_atomic_output_recovery.py)
- [test_daily_run_run_integrity_markers.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_daily_run_run_integrity_markers.py)
- [test_status_report_run_integrity.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_status_report_run_integrity.py)

### 9. Configuration And Review-Instance Tests

These test that the explicit review-instance surface behaves correctly.

Typical examples:

- config parsing and validation
- config-aware `doctor`
- config-aware `status`
- config-aware `review run`

Representative files:

- [test_review_config.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests/test_review_config.py)
- [test_syreto_package_import.py](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/tests/test_syreto_package_import.py)

## Mirrored Test Layout

Many tests exist in both:

- [`syreto/tests/`](/Users/pigra/Documents/New%20project/syreto_clean/syreto/tests)
- [`03_analysis/tests/`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/tests)

This is intentional in the current repository posture.

The mirror helps test both:

- the installable package surface
- the legacy/original analysis-script surface

That is slightly unusual, but it fits SyReTo’s transition from script collection to packaged operational system.

## What This Taxonomy Says About The Product

The taxonomy reflects the actual nature of SyReTo.

SyReTo is tested not only as:

- a collection of Python modules

but as:

- a schema-sensitive review pipeline
- an artifact-producing operational system
- a guard-enforced trust surface
- a CLI-mediated execution interface
- a rerun/recovery-aware scientific workflow

This is important because the product claim is operational, not merely algorithmic.

## Relationship To Other Contracts

- [execution-contract.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/execution-contract.md)
- [artifact-catalog.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/artifact-catalog.md)
- [integrity-guards.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/integrity-guards.md)
- [recovery-rerun-semantics.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/recovery-rerun-semantics.md)
- [reproducibility-contract.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/reproducibility-contract.md)

Together, these documents explain not only how SyReTo is intended to behave, but how that behavior is continuously checked.

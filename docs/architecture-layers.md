# Architecture Layers

## Purpose

This page names the main architectural layers of SyReTo so that code, CLI surface, and documentation can be discussed against the same system model.

The goal is not to invent a framework taxonomy for its own sake. The goal is to preserve separation of concerns:

- execution logic should stay in pipeline code
- review truth should stay in versioned files and explicit config
- interfaces should remain thin
- observability should explain execution without controlling it

## Layer Summary

SyReTo is structured around four cooperating layers:

1. core
2. interfaces
3. tasks
4. observability

These layers are conceptually distinct even when some repository files touch more than one operational concern.

## 1. Core

The core layer is responsible for:

- pipeline execution
- review-state transitions
- config resolution
- artifact production
- execution and validity contracts

Representative surfaces:

- [`03_analysis/daily_run.sh`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/daily_run.sh)
- [`syreto/review_config.py`](/Users/pigra/Documents/New%20project/syreto_clean/syreto/review_config.py)
- validation and builder scripts under [`syreto/`](/Users/pigra/Documents/New%20project/syreto_clean/syreto)
- mirrored operational scripts under [`03_analysis/`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis)

The core decides what happens during a run.

It is where methodological and execution logic belong.

## 2. Interfaces

The interfaces layer is responsible for:

- selecting an entrypoint
- invoking existing pipeline behavior
- summarizing repository state
- exposing stable commands for users and future tooling

Representative surfaces:

- [`syreto/cli.py`](/Users/pigra/Documents/New%20project/syreto_clean/syreto/cli.py)
- packaged entrypoints declared in [`pyproject.toml`](/Users/pigra/Documents/New%20project/syreto_clean/pyproject.toml)
- current commands such as `syreto doctor`, `syreto status`, `syreto artifacts`, `syreto validate`, `syreto observability`, `syreto analytics`, and `syreto review run`

The interface layer should stay thin.

It may select a review instance and expose diagnostics, but it must not become the hidden place where epistemic structure is defined.

## 3. Tasks

The tasks layer is responsible for concrete review operations.

Examples include:

- search-state consolidation
- deduplication
- screening metrics
- extraction validation
- appraisal
- synthesis
- manuscript table generation
- analytics and export

Representative scripts include:

- [`03_analysis/consolidate_title_abstract_consensus.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/consolidate_title_abstract_consensus.py)
- [`03_analysis/validate_csv_inputs.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/validate_csv_inputs.py)
- [`03_analysis/grade_evidence_profiler.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/grade_evidence_profiler.py)
- [`03_analysis/results_summary_table_builder.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/results_summary_table_builder.py)
- [`03_analysis/review_descriptives_builder.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/review_descriptives_builder.py)

Task scripts are where local transformations happen.

They should consume canonical files and documented internal contracts such as [study-table-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/study-table-model.md), rather than inventing hidden runtime truth.

## 4. Observability

The observability layer is responsible for:

- recording what happened during execution
- preserving temporal evidence of step outcomes
- surfacing postmortem context
- exposing provenance for selected trust-bearing artifacts

Representative surfaces:

- [`outputs/run_events.jsonl`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/run_events.jsonl)
- [`outputs/daily_run_manifest.json`](/Users/pigra/Documents/New%20project/syreto_clean/outputs/daily_run_manifest.json)
- `*.provenance.json` sidecars for selected generated artifacts
- `syreto observability`
- provenance views in `syreto artifacts` and `syreto doctor`

This layer may explain execution, but it must not alter execution.

## Why This Separation Matters

This architectural split protects several core SyReTo claims:

- reproducibility: execution remains anchored to explicit files, config, and scripts
- inspectability: users can see which layer produced which signal
- auditability: failures can be traced without guessing where truth lives
- CLI discipline: the public surface stays useful without becoming a second hidden pipeline

Without this separation, the project would drift toward a tool that feels convenient in the moment but becomes harder to trust over time.

## Practical Mapping

In day-to-day usage, the layers line up like this:

- update canonical inputs and config: review-state layer of the core
- run `syreto review run`: interface invoking the core orchestration spine
- inspect `syreto doctor` and `syreto status`: interface summaries over core outputs
- inspect `syreto observability`: observability layer over run events and touched outputs
- inspect manuscript outputs and operational artifacts: task outputs produced by the core

## Related Docs

- [pipeline-overview.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/pipeline-overview.md)
- [execution-contract.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/execution-contract.md)
- [observability-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/observability-model.md)
- [review-instance-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/review-instance-model.md)
- [study-table-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/study-table-model.md)

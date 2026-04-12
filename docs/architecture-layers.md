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

- [`03_analysis/daily_run.sh`](../03_analysis/daily_run.sh)
- [`syreto/review_config.py`](../syreto/review_config.py)
- validation and builder scripts under [`syreto/`](../syreto)
- compatibility wrappers and shell-facing entrypoints under [`03_analysis/`](../03_analysis)

The core decides what happens during a run.

It is where methodological and execution logic belong.

### Repository Rule

At the repository boundary, SyReTo follows a strict source-of-truth split:

- [`syreto/`](../syreto) is the canonical Python package
- [`03_analysis/`](../03_analysis) is the shell spine and compatibility-entrypoint layer

This means:

- reusable Python logic **SHOULD** live in `syreto/`
- `03_analysis/` **MAY** keep orchestration scripts such as [`daily_run.sh`](../03_analysis/daily_run.sh)
- `03_analysis/` **MAY** keep thin wrappers needed for legacy imports or script-path execution
- `03_analysis/` **MUST NOT** grow a second independent implementation of package-owned module logic

When a top-level module exists in both places, the intended steady state is:

- `syreto/<module>.py` contains the implementation
- `03_analysis/<module>.py` is either absent or a thin compatibility shim

## 2. Interfaces

The interfaces layer is responsible for:

- selecting an entrypoint
- invoking existing pipeline behavior
- summarizing repository state
- exposing stable commands for users and future tooling

Representative surfaces:

- [`syreto/cli.py`](../syreto/cli.py)
- packaged entrypoints declared in [`pyproject.toml`](../pyproject.toml)
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

- [`03_analysis/consolidate_title_abstract_consensus.py`](../03_analysis/consolidate_title_abstract_consensus.py)
- [`syreto/validate_csv_inputs.py`](../syreto/validate_csv_inputs.py)
- [`syreto/grade_evidence_profiler.py`](../syreto/grade_evidence_profiler.py)
- [`syreto/results_summary_table_builder.py`](../syreto/results_summary_table_builder.py)
- [`syreto/review_descriptives_builder.py`](../syreto/review_descriptives_builder.py)

Task scripts are where local transformations happen.

They should consume canonical files and documented internal contracts such as [study-table-model.md](study-table-model.md), rather than inventing hidden runtime truth.

## 4. Observability

The observability layer is responsible for:

- recording what happened during execution
- preserving temporal evidence of step outcomes
- surfacing postmortem context
- exposing provenance for selected trust-bearing artifacts

Representative surfaces:

- [`outputs/run_events.jsonl`](../outputs/run_events.jsonl)
- [`outputs/daily_run_manifest.json`](../outputs/daily_run_manifest.json)
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

- [pipeline-overview.md](pipeline-overview.md)
- [execution-contract.md](execution-contract.md)
- [observability-model.md](observability-model.md)
- [review-instance-model.md](review-instance-model.md)
- [study-table-model.md](study-table-model.md)

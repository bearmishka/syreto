# Execution Contract

## Purpose

This page defines the execution contract of SyReTo.

It answers the operational question:

`what exactly does the system promise when a review run is executed?`

The execution contract is the layer that makes pipeline behavior explicit:

- which inputs are canonical
- which outputs are canonical
- which stages are required
- what counts as a successful run
- what counts as an invalid run
- which surfaces are expected to be deterministic
- which surfaces may remain environment- or configuration-dependent

This document is not a replacement for implementation details in code. It is the outward-facing statement of the review pipeline contract.

## Why An Execution Contract Matters

Without an explicit execution contract, a user may know:

- how to invoke the pipeline
- which files often appear after a run
- which checks sometimes fail

but still not know:

- which inputs are authoritative
- which outputs must exist for trust to be established
- which stages are mandatory
- whether a partial run is acceptable

SyReTo is meant to operate as a reproducible review system, not just as a script launcher. That requires a visible execution contract.

## Canonical Inputs

The canonical inputs of SyReTo are versioned files that define review state and feed deterministic processing steps.

In the current repository shape, canonical inputs primarily live under:

- `02_data/`
- `03_analysis/` for execution scripts and rule-based orchestration
- review-facing configuration in environment variables and, in the planned model, `review.toml`

In practice, the most important canonical input classes are:

- screening and study-state CSV files
- extraction tables
- appraisal tables
- processed record-identity files
- protocol- and review-state supporting tables used by downstream generators

Generated outputs are not canonical inputs and should not be hand-edited as a substitute for upstream state changes.

## Canonical Outputs

Canonical outputs are generated artifacts that downstream users and checks are allowed to rely on.

The most important current canonical outputs are:

- `outputs/status_summary.json`
- `outputs/status_report.md`
- `outputs/todo_action_plan.md`
- `outputs/daily_run_manifest.json`
- `outputs/run_events.jsonl`
- `outputs/review_descriptives.json`
- `outputs/review_descriptives.md`
- `outputs/figures/*.png`
- manuscript-facing tables and sections under `04_manuscript/`

Not every generated file has equal contract weight.

At the highest operational level, the contract depends most strongly on:

- status artifacts
- failure markers
- run manifest and observability stream
- manuscript-facing outputs expected from enabled mandatory stages

For the broader inventory, see [artifact-catalog.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/artifact-catalog.md).

## Required Stages

SyReTo may expose optional extension stages, but a trustworthy review run still depends on a required execution spine.

At a high level, the required spine includes:

1. mode and preflight validation
2. core input validation
3. integrity-guard execution
4. synthesis and status generation
5. final checkpoint and status evaluation

Depending on repository state and enabled outputs, manuscript-facing generation may also be part of the required successful path.

Optional stages such as citation tracking, retraction checking, keyword analysis, and living-review scheduling do not redefine the core contract by themselves.

## Successful Run

A run is successful when all of the following are true:

1. the primary execution entrypoint exits with code `0`
2. required validation stages complete successfully
3. mandatory checkpoint stages complete successfully
4. the run does not leave a failed-run marker
5. required status artifacts are current and readable
6. the configured status policy considers the resulting state acceptable
7. required downstream outputs for the active run mode are regenerated

In operational terms, a successful run is not just “the shell returned.” It is a run whose status-bearing artifacts agree that the review state is acceptable.

## Invalid Run

A run is invalid when the execution contract is not satisfied, even if some outputs were produced.

Typical invalid-run conditions include:

- non-zero exit from the main entrypoint
- failed required validation
- failed required integrity checks
- failed production status gate
- presence of `outputs/daily_run_failed.marker`
- missing or stale status artifacts after execution
- partial output generation with blocking checkpoint failure

An invalid run must not be rehabilitated by manually editing downstream outputs.

## Deterministic Guarantees

SyReTo is designed to provide deterministic guarantees for the core review pipeline when:

- canonical inputs are unchanged
- execution mode and thresholds are unchanged
- the same enabled stages are used
- the runtime environment is materially equivalent

Under those conditions, the system aims to reproduce:

- the same status results
- the same review descriptives
- the same manuscript-facing derived tables and sections
- the same integrity and checkpoint outcomes

This is the core reproducibility claim of the system.

## Configuration-Dependent And Environment-Dependent Surfaces

Not every surface is guaranteed in the same way.

The following may vary depending on environment, enabled stages, or repository posture:

- optional extension-stage outputs
- outputs gated on the presence of optional input files
- transactional rollback side effects
- timing, ordering details in run-event logging, and wall-clock metadata
- future review-instance-specific configuration once `review.toml` becomes active

This does not invalidate the deterministic core. It means the contract must distinguish between mandatory deterministic artifacts and optional or environment-sensitive ones.

## What SyReTo Guarantees

SyReTo guarantees, within its stated contract, that:

- canonical inputs remain file-based and inspectable
- execution proceeds through explicit scripted entrypoints
- status artifacts express operational acceptability
- failures are surfaced through explicit markers, reports, and exit codes
- observability records execution without controlling execution
- generated outputs are meant to be regenerated from upstream state, not manually maintained in parallel

## What SyReTo Does Not Guarantee

SyReTo does not guarantee that:

- every optional stage is always enabled or present
- every generated artifact is meaningful if the run is invalid
- local environment misconfiguration will be silently corrected
- downstream trust can be established from file presence alone
- ad hoc manual edits to generated artifacts preserve contract validity

These non-guarantees are important. They prevent a false sense of certainty around partial or manually altered runs.

## Relationship To Other Contracts

- [review-instance-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/review-instance-model.md) defines what review-specific configuration should look like
- [failure-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/failure-model.md) defines how the system behaves when the contract is violated
- [observability-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/observability-model.md) defines how execution is recorded without controlling it
- [artifact-catalog.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/artifact-catalog.md) inventories the most important generated outputs
- [recovery-rerun-semantics.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/recovery-rerun-semantics.md) defines when rerun, reuse, rollback, and stale-state handling preserve trust
- [reproducibility-contract.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/reproducibility-contract.md) defines the envelope within which “same config + same inputs = same canonical outputs” is expected to hold

Together, these documents define the external operational promises of SyReTo.

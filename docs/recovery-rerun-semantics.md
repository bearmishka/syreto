# Recovery And Rerun Semantics

## Purpose

This page defines how SyReTo treats reruns, recovery, stale state, and rollback.

It answers the operational question:

`when a run fails or leaves ambiguous outputs, what exactly is safe to rerun, reuse, or trust?`

This document exists to reduce gray-zone behavior after partial execution.

It makes explicit:

- what counts as a rerun
- what counts as a clean rerun
- which artifacts may be reused
- when reuse is forbidden
- how stale state is recognized
- when rollback markers invalidate trust in a run

## Why This Layer Matters

Without explicit rerun semantics, teams often fall into unsafe habits such as:

- trusting a downstream file because it exists
- rerunning only a convenient late stage after upstream state changed
- mixing fresh and stale outputs from different execution attempts
- treating rollback as if it had restored full trust automatically

SyReTo should not leave these decisions implicit.

## Core Definitions

### Rerun Step

A **rerun step** means re-executing a bounded pipeline step or script against the current canonical inputs and current repository state.

A rerun step may be appropriate when:

- the step is observational or descriptive
- the step is purely downstream of unchanged canonical inputs
- the step does not cross an integrity boundary on its own

Examples:

- rebuilding review descriptives after no upstream review-state change
- regenerating a manuscript-facing table from already validated current inputs

A rerun step does **not** by itself revalidate the whole run.

### Clean Rerun

A **clean rerun** means re-executing the required review spine from a state where previous failed-run ambiguity has been cleared or explicitly superseded.

In practice, a clean rerun means:

- canonical inputs are treated as the authoritative source again
- stale or failed-run markers are absent or superseded by a new valid run
- required checkpoints execute again
- required status artifacts are regenerated again
- the resulting run can be judged on its own contract, not on leftover files

Operationally, `syreto review run` is the default path for a clean rerun.

## Reuse Semantics

### Artifacts That May Be Reused

Reuse is acceptable when the artifact is:

- derived from unchanged canonical inputs
- not contradicted by a failed-run marker or rollback state
- not older than the inputs it depends on
- not bypassing a required validation or integrity boundary

Typical reusable artifacts:

- review descriptives and figures when no upstream study-state inputs changed
- manuscript-facing tables rebuilt from unchanged validated inputs
- optional convenience outputs that are clearly downstream of unchanged state

### Artifacts That Must Not Be Reused Blindly

Reuse is forbidden, or at least not trustworthy, when:

- canonical inputs changed after the artifact was produced
- a failed run left ambiguous downstream outputs
- the artifact depends on a stage that did not complete cleanly
- the artifact sits downstream of a failed validation, failed integrity check, or failed status gate
- rollback occurred and the artifact tree was not revalidated afterward

Typical unsafe reuse cases:

- reusing `status_summary.json` after canonical inputs changed
- trusting manuscript outputs created during a failed run
- reusing outputs after `daily_run_failed.marker` was present
- treating old run events as evidence for a newer downstream file

## Stale State

SyReTo treats state as **stale** when at least one of the following is true:

- a required generated artifact is older than the canonical input it depends on
- a required run-state artifact is missing after a supposed successful run
- run-history artifacts and status artifacts disagree about the most recent trustworthy execution
- a failed-run marker or rollback evidence remains present after partial output generation

Stale state does not always mean corruption.

It does mean the repository surface is not clean enough to trust without rerun or investigation.

## Rollback Semantics

Rollback is not equivalent to restored trust.

If rollback markers or rollback evidence exist, the correct interpretation is:

- the system attempted recovery
- prior execution was not clean
- downstream trust must be re-established by a new valid run or explicit investigation

In other words:

- rollback may preserve recoverability
- rollback does not itself certify correctness

This is why rollback state is treated as a distinct failure class in the broader failure model.

## Invalidating Markers

The following signals should be treated as invalidating or trust-reducing until cleared by a clean rerun or explicit review:

- `outputs/daily_run_failed.marker`
- rollback-state evidence
- stale `status_summary.json`
- stale or incomplete `run_events.jsonl`
- status artifacts that exist but describe a blocked or inconsistent repository posture

If these signals are present, downstream files may still exist, but their presence alone is not enough.

## Operational Rules

The safest operational reading is:

1. if a run failed, assume downstream outputs are untrustworthy until proven otherwise
2. if required inputs changed, assume dependent derived outputs are stale
3. if integrity or status gates failed, do not rehabilitate the run by rerendering late-stage outputs
4. if rollback occurred, require either investigation or clean rerun before trusting the repository surface again
5. use `syreto doctor`, `syreto status`, and `syreto observability` together before deciding whether reuse is acceptable

## Relationship To Existing Surfaces

These surfaces should be read together:

- `syreto doctor`
  - asks whether the repository is ready for an honest run
- `syreto status`
  - asks whether current status findings are acceptable
- `syreto observability`
  - asks what happened over time during execution
- `outputs/daily_run_failed.marker`
  - signals failed or incomplete run state
- `outputs/run_events.jsonl`
  - records step-level execution history
- `outputs/status_summary.json`
  - records consolidated project status

No single one of these is enough on its own to justify blind reuse after a failure.

## What This Means In Practice

If you want to know whether you may trust the current surface:

- start with `syreto doctor`
- inspect `syreto status`
- inspect `syreto observability`
- prefer `syreto review run` over ad hoc late-stage reruns when state is ambiguous

If you want to know whether you may trust a specific downstream artifact:

- ask whether its upstream canonical inputs changed
- ask whether a required checkpoint failed after it was generated
- ask whether a stale or rollback signal remains present

If the answer is unclear, prefer a clean rerun.

## Relationship To Other Contracts

- [execution-contract.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/execution-contract.md)
- [failure-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/failure-model.md)
- [observability-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/observability-model.md)
- [artifact-catalog.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/artifact-catalog.md)

Together, these documents define not just how SyReTo runs, but how trust is recovered after a run goes wrong.

# Failure Model

## Purpose

This page explains how SyReTo behaves when something goes wrong.

It is not a list of every possible exception. It is the operational contract around failure:

- which failure classes exist
- which ones are hard failures
- which ones are warnings only
- when the pipeline must stop
- how a user decides whether a run is trustworthy

This is the layer that connects validation, integrity guards, status reporting, and orchestration behavior into one understandable system.

## Operational Failure Taxonomy

For practical use, SyReTo treats failures as belonging to a small number of operational classes.

These classes are not just labels. They imply:

- which surface should detect the problem first
- whether the run is still usable
- whether the issue is recoverable by rerunning
- whether manual intervention is required

The current primary classes are:

- `config error`
- `missing artifact`
- `schema violation`
- `environment problem`
- `integrity guard failure`
- `partial run or stale outputs`
- `rollback state`

## Failure Semantics

Each failure class should be interpreted through four questions:

1. is this a hard fail or a warning
2. is it recoverable by rerun alone
3. does it require upstream correction
4. which operational surface should the user trust first

In shorthand:

- `hard fail` means the run is not acceptable for downstream trust
- `warning` means the system is signaling risk or incompleteness without declaring the whole run unusable by itself
- `recoverable` means a clean rerun may resolve the issue once prerequisites are satisfied
- `manual intervention` means a user must repair state, inputs, configuration, or environment before trust can be restored

## Why A Failure Model Matters

A reproducible system is not just one that can succeed repeatedly. It is also one that fails in legible, disciplined ways.

Without an explicit failure model, a user can see:

- a failed script
- a generated artifact
- a warning in a report
- a partially updated output tree

and still not know whether the run is usable.

SyReTo’s failure model is meant to reduce that ambiguity.

## Main Failure Classes

### 1. Environment and Setup Failures

Examples:

- missing Python tooling
- missing `uv`/development environment
- invalid runtime mode values
- missing required directories or scripts

Typical effect:

- the run should not be trusted
- setup must be repaired before proceeding

Primary detection surfaces:

- `syreto doctor`
- startup validation in `daily_run.sh`

Default operational posture:

- usually `hard fail`
- often recoverable after environment repair
- usually requires manual intervention before rerun

Canonical examples:

- `environment problem`
- `config error` when mode or runtime settings are invalid

### 2. Input and Schema Failures

Examples:

- missing canonical CSV files
- malformed columns
- invalid enumerated values
- inconsistent cross-file counts

Typical effect:

- validation should fail
- downstream outputs are not trustworthy until inputs are corrected

Primary detection surfaces:

- `syreto validate csv`
- `syreto validate extraction`
- the validation steps inside `daily_run.sh`

Default operational posture:

- usually `hard fail`
- not recoverable by rerun alone
- requires manual correction of canonical inputs

Canonical examples:

- `schema violation`
- `missing artifact` when required CSV inputs are absent or incomplete

### 3. Integrity Failures

Examples:

- audit log integrity violations
- record identity instability
- epistemic inconsistency between review state and downstream outputs
- unresolved template leakage in manuscript-facing outputs
- source integrity issues detected by the Python source guard

Typical effect:

- these are not cosmetic issues
- they indicate that operational trust has been compromised or cannot yet be established

Primary detection surfaces:

- integrity guard scripts
- mandatory final checkpoint behavior in `daily_run.sh`
- status artifacts

Default operational posture:

- usually `hard fail` in `production`
- may degrade to warning in some `template` scenarios
- usually requires manual investigation, not just rerun

Canonical examples:

- `integrity guard failure`
- template leakage with blocking semantics
- epistemic inconsistency detected at final checkpoint

### 4. Status Gate Failures

Examples:

- unresolved findings at or above `STATUS_FAIL_ON`
- production mode gate failure from `status_cli.py`

Typical effect:

- in `production`, the run must be treated as failed
- in `template`, the same signals may be informational rather than blocking

Primary detection surfaces:

- `syreto status`
- `status_cli.py --fail-on ...`
- final status checkpoint in `daily_run.sh`

Default operational posture:

- `hard fail` when findings meet or exceed the configured blocking threshold
- otherwise warning-only
- may require either manual correction or policy adjustment, depending on the finding and mode

### 5. Optional Stage Failures

Examples:

- citation tracking failure
- retraction checker failure
- living-review scheduler failure
- keyword analysis failure

Typical effect:

- depends on whether the stage is enabled and where it sits in the pipeline contract
- optional does not always mean irrelevant, but it does mean the user must interpret impact carefully

Default operational posture:

- often warning-only
- recoverability depends on whether the stage is enabled and whether its outputs are contractually required for the current run
- may require manual intervention if the stage is part of the intended publication or reporting posture

### 6. Partial Output or Stale Artifact Failures

Examples:

- some artifacts regenerated, others not
- failure during checkpoint after partial pipeline success
- failure followed by transactional rollback

Typical effect:

- outputs may exist but still be untrustworthy
- users must rely on failure markers and status artifacts, not only file presence

Default operational posture:

- usually `hard fail` for the run as a whole
- sometimes recoverable by a clean rerun
- may require manual cleanup if stale artifacts remain ambiguous

Canonical examples:

- `partial run or stale outputs`
- `missing artifact` when a required generated output is absent after supposed success
- `rollback state` when recovery leaves the tree in an explicitly incomplete state

## Failure Class Matrix

The table below gives the intended default interpretation of the most important failure classes.

| Failure class | Typical severity | Recoverable by rerun alone | Manual intervention | Primary surface |
| --- | --- | --- | --- | --- |
| `environment problem` | hard fail | sometimes | usually yes | `syreto doctor` |
| `config error` | hard fail | no | yes | `syreto doctor`, startup checks |
| `schema violation` | hard fail | no | yes | `syreto validate` |
| `missing artifact` | hard fail for required inputs/outputs; warning for optional outputs | sometimes | often yes | `syreto validate`, `syreto artifacts`, status artifacts |
| `integrity guard failure` | hard fail in production | rarely | yes | guards, `syreto status` |
| `partial run or stale outputs` | hard fail | sometimes | sometimes yes | failure marker, status artifacts |
| `rollback state` | hard fail until clarified | sometimes | often yes | manifest, failure marker, observability outputs |

This matrix is a default operational model, not a claim that every single failure instance has identical semantics.

## Hard Fail vs Warning Only

The most useful distinction in practice is this:

### Hard Fail

A condition is a hard fail when the system says the run is not acceptable for downstream trust.

Typical hard-fail cases:

- non-zero final exit from `daily_run.sh`
- failure of required validation steps
- failure of mandatory final checkpoint stages
- failed production status gate
- integrity failures that make checkpoint return non-zero
- presence of `outputs/daily_run_failed.marker`

### Warning Only

A condition is warning-only when the system is signaling risk or incompleteness without declaring the whole run unusable by itself.

Typical warning-only cases:

- informational findings in `template` mode
- optional artifacts not yet present
- missing convenience summaries where source state is otherwise intact
- non-blocking helper stage failures
- warning-level status findings below the configured fail threshold

Warning-only does not mean “ignore.” It means “do not automatically treat this as a terminal pipeline failure.”

## Recoverable vs Manual Intervention

Another useful distinction is whether the system expects rerun alone to be sufficient.

### Recoverable

A condition is recoverable when the main issue is transient or procedural and a clean rerun may restore trust once the immediate problem is removed.

Typical recoverable cases:

- environment repaired and rerun
- optional helper stage retried
- transient partial run followed by successful clean rerun
- rollback completed, followed by successful rerun

Recoverable does not mean “safe to ignore.” It means the recovery path is operationally straightforward.

### Requires Manual Intervention

A condition requires manual intervention when the review state, configuration, or canonical inputs must be repaired before a rerun can be meaningful.

Typical manual-intervention cases:

- malformed extraction schema
- missing required canonical CSV
- integrity failure caused by contradictory review state
- blocking template leakage in production outputs
- status findings that reflect real unresolved review-state problems

These are the cases where rerun without correction mostly reproduces the failure.

## Template vs Production

The same issue may be treated differently depending on `REVIEW_MODE`.

### In `template`

- the status gate is informational
- template leakage checks may warn instead of fail
- the system is more permissive about scaffolding and incomplete setup

### In `production`

- the status gate becomes enforcing
- template leakage in manuscript-facing outputs is treated strictly
- the run is expected to behave like a trustworthy operational pipeline

This distinction is central to understanding why some findings block one run but not another.

## When The Pipeline Must Stop

The pipeline must be treated as failed when any of the following is true:

- `daily_run.sh` exits non-zero
- the mandatory final checkpoint returns non-zero
- the production status gate fails
- the epistemic consistency checkpoint fails in a way that makes checkpoint return non-zero
- required validation steps fail
- required setup or source-of-truth files are missing

In those cases, downstream outputs should not be treated as reliable just because some files were generated.

## How To Recognize An Unusable Run

A run should be treated as unusable or not yet trustworthy when one or more of these signals is present:

- non-zero exit status
- `outputs/daily_run_failed.marker`
- missing or stale `outputs/status_summary.json`
- failed production status gate
- failed integrity guard with blocking semantics
- explicit rollback failure message
- status artifacts reporting unresolved blocker findings

This is especially important because file presence alone is not enough to establish trust.

## What The System Does On Failure

At a high level, the orchestration layer tries to behave deterministically on failure too.

- it still runs the consolidated final checkpoint
- it can write a failed-run marker
- it updates the daily run manifest
- it can attempt transactional rollback
- it refreshes status reporting after failure marker write

That means a failed run still leaves behind operational evidence about what happened.

## How `doctor` And `status` Should Be Interpreted

The failure model is also what makes `doctor` and `status` legible.

Use them differently:

- `syreto doctor` is strongest for `environment problem`, `config error`, and some `missing artifact` cases
- `syreto status` is strongest for `integrity guard failure`, blocking review-state findings, and final run acceptability
- `syreto validate` is strongest for `schema violation` and malformed canonical inputs
- `outputs/daily_run_failed.marker`, `outputs/status_summary.json`, and `outputs/run_events.jsonl` are strongest for `partial run or stale outputs` and `rollback state`

This division matters because not every failure class should be debugged from the same surface.

## Recommended User Interpretation

When something goes wrong, use this order:

1. check exit status
2. check `outputs/daily_run_failed.marker`
3. inspect `syreto doctor`
4. inspect `syreto status`
5. inspect `outputs/status_report.md`
6. inspect `outputs/todo_action_plan.md`
7. classify the issue as `config error`, `missing artifact`, `schema violation`, `environment problem`, `integrity guard failure`, `partial run or stale outputs`, or `rollback state`
8. determine whether it is hard-fail, warning-only, recoverable, or requires manual intervention

This order helps separate “environment not ready” from “review state invalid” from “run completed with warnings.”

## Relationship To Other Layers

- `validate` checks whether inputs and state are acceptable to process
- `doctor` checks whether the environment and repository surface are ready
- `integrity-guards` check whether operational trust has been compromised
- `failure model` explains what the system does when any of those layers reports trouble

Together, these define the usable trust contract of the pipeline.

## Related Docs

- [execution-contract.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/execution-contract.md)
- [daily-operations.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/daily-operations.md)
- [integrity-guards.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/integrity-guards.md)
- [pipeline-overview.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/pipeline-overview.md)
- [review-instance-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/review-instance-model.md)

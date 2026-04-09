# Integrity Guards

## Purpose

SyReTo does not treat integrity checks as optional lint around the edges of the workflow. Integrity guards are part of the system contract: they are what let the project claim that its outputs remain operationally trustworthy.

This page defines the integrity-guard layer as a specification, not just a feature list.

It answers:

- which guards exist
- where they run
- what they protect
- which artifacts they read or write
- what happens when they fail

## Why Guards Exist

A review pipeline can appear to “work” while drifting into an invalid state.

Examples:

- audit history is silently damaged
- record identities drift after deduplication
- manuscript outputs still contain unresolved placeholders
- source integrity degrades in ways tests do not immediately expose
- screening logic and manuscript-facing outputs become epistemically inconsistent

SyReTo addresses these risks with dedicated guards rather than relying on team vigilance alone.

## Guard Layer Principles

The integrity-guard layer exists to protect trust boundaries, not just file formatting.

In practice, guards are expected to:

- check whether the review remains operationally trustworthy
- run at explicit points in the execution spine
- emit legible failures rather than silent drift
- feed the broader failure and status model

Guards are therefore different from:

- generic lint
- optional convenience diagnostics
- downstream reporting scripts

## Guard Inventory

The main guard contract can be summarized like this.

| Guard | Primary script | Typical execution point | Protects | Failure effect |
| --- | --- | --- | --- | --- |
| Audit Log Integrity Guard | `audit_log_integrity_guard.py` | integrity phase in `daily_run.sh` | audit trail continuity and audit-log structure | invalidates trust in review history; typically hard fail |
| Record ID Map Integrity Guard | `record_id_map_integrity_guard.py` | integrity phase in `daily_run.sh` | record identity stability across deduplication and downstream linkage | invalidates record-link trust; typically hard fail |
| Template Term Guard | `template_term_guard.py` | preflight and manuscript-output checks | unresolved placeholders and template leakage | warn or fail depending on mode and invocation posture |
| Epistemic Consistency Guard | `epistemic_consistency_guard.py` | mandatory final checkpoint | consistency between decision state and downstream outputs | strongest downstream trust gate; warn in template, fail in production |
| Python Source Guard | `python_source_guard.py` | source-integrity/testing surface | implementation integrity of the pipeline code itself | blocks trust in pipeline implementation; usually hard fail in CI/test posture |

## Main Guard Types

### Audit Log Integrity Guard

Script:

- [`03_analysis/audit_log_integrity_guard.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/audit_log_integrity_guard.py)

Purpose:

- verifies that the audit log exists in the expected structure
- checks whether the log header and basic integrity expectations hold
- protects the continuity of the review’s explicit audit history

Primary input:

- [`02_data/processed/audit_log.csv`](/Users/pigra/Documents/New%20project/syreto_clean/02_data/processed/audit_log.csv)

Typical execution point:

- integrity phase in [`03_analysis/daily_run.sh`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/daily_run.sh)

Failure effect:

- usually treated as an integrity-compromising condition
- may still leave other artifacts on disk, but those artifacts should not be trusted as a clean run surface

### Record ID Map Integrity Guard

Script:

- [`03_analysis/record_id_map_integrity_guard.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/record_id_map_integrity_guard.py)

Purpose:

- checks whether record identity mapping remains stable across deduplication-related transformations
- protects the ability to connect records, screening state, and downstream reporting without silent identity drift

Primary input:

- [`02_data/processed/record_id_map.csv`](/Users/pigra/Documents/New%20project/syreto_clean/02_data/processed/record_id_map.csv)

Typical execution point:

- integrity phase in [`03_analysis/daily_run.sh`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/daily_run.sh)

Failure effect:

- usually treated as a hard integrity failure
- downstream artifacts that depend on stable record linkage should be treated as suspect until rerun or repair

### Epistemic Consistency Guard

Script:

- [`03_analysis/epistemic_consistency_guard.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/epistemic_consistency_guard.py)

Purpose:

- checks whether downstream synthesis and reporting artifacts remain consistent with the review’s actual decision state
- acts as a final checkpoint in `daily_run.sh`
- protects the epistemic trust boundary between review decisions and manuscript/reporting outputs

This is one of the strongest signals that SyReTo is trying to guard the reasoning surface of the review, not just file syntax.

Primary output:

- `outputs/epistemic_consistency_report.md`

Typical execution point:

- mandatory final checkpoint in [`03_analysis/daily_run.sh`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/daily_run.sh)

Failure effect:

- in `template` mode, may be informational or non-blocking
- in `production` mode, is intended to function as a real trust gate
- failure means downstream reporting artifacts may exist but are not epistemically trustworthy

### Template Term Guard

Script:

- [`03_analysis/template_term_guard.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/template_term_guard.py)

Purpose:

- scans protocol and manuscript-facing content for unresolved placeholders or banned template leakage
- protects against shipping scaffold text, template residue, or unresolved placeholder semantics into operational outputs

Typical use cases:

- preflight scan of protocol/search files
- post-generation scan of manuscript-facing outputs

Primary output:

- `outputs/template_term_guard_summary.md`

Typical execution points:

- preflight placeholder scan
- manuscript-facing output scan later in `daily_run.sh`

Failure effect:

- may warn or fail depending on mode and explicit invocation posture
- in `production`, manuscript-facing leakage is expected to be treated strictly

### Python Source Guard

Script:

- [`03_analysis/python_source_guard.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/python_source_guard.py)

Purpose:

- detects structural integrity issues in source files that should not silently enter the codebase
- protects the implementation trustworthiness of the pipeline itself

This is less about the review state and more about keeping the pipeline implementation itself trustworthy.

Typical execution point:

- test and source-integrity surface, rather than normal review-state orchestration

Failure effect:

- should block confidence in the implementation layer
- generally belongs to CI/test posture rather than end-user review execution

## How Guards Behave In Practice

Guards are not all used in exactly the same place.

- some run early as preflight checks
- some run during the main pipeline body
- some run as part of the final operational checkpoint
- some are stricter in `production` than in `template` mode

This layered placement is important: SyReTo is trying to catch problems at the most useful moment, not only at the very end.

## Guard Placement In The Spine

At a high level, the current guard placement is:

1. preflight placeholder checks
2. integrity-phase audit and record-identity checks
3. manuscript leakage checks during output generation
4. epistemic consistency as a mandatory final checkpoint
5. source-integrity checks in the development/test surface

This placement matters because each guard is intended to protect a different trust boundary:

- preflight trust
- provenance trust
- record-link trust
- manuscript-surface trust
- epistemic trust
- implementation trust

## What Happens When A Guard Fails

Guard failure should not be interpreted as “a script noticed something odd.”

It should be interpreted as:

- a named trust boundary was violated, or could not yet be established
- the run may no longer satisfy the execution contract
- downstream artifacts may need to be treated as stale, invalid, or requiring manual review

Operationally:

- some guard failures are warning-only in `template` mode
- some guard failures are hard-blocking in `production`
- final interpretation belongs with the broader failure model and status layer

Typical consequences include:

- non-zero run exit
- blocked status posture
- explicit guard summary artifacts
- failed-run markers or invalid-run semantics

## Guard Outputs And Surfaces

The guard layer is visible through multiple surfaces:

- dedicated guard scripts
- `daily_run.sh` execution logs
- `outputs/template_term_guard_summary.md`
- `outputs/epistemic_consistency_report.md`
- `outputs/status_summary.json`
- `outputs/status_report.md`
- `syreto doctor`
- `syreto status`

This is important: guard outcomes are not supposed to remain buried in script stdout.

## Relationship To Status Artifacts

Guard results feed into the operational status layer rather than floating around as isolated script outputs.

The most useful summary surfaces remain:

- `outputs/status_summary.json`
- `outputs/status_report.md`
- `outputs/todo_action_plan.md`

This means guards are part of the same operational story as the rest of the pipeline.

## Why This Matters For Code, Docs, And Paper

This layer matters simultaneously in three places:

- code
  - because guard placement and failure semantics shape the execution spine
- docs
  - because users need to know what trust boundary each guard protects
- paper
  - because guards are part of what justifies SyReTo’s claim to operational honesty and reproducibility

Without a clear guard specification, the project can still execute, but it is harder to explain why its outputs should be trusted.

## Why This Matters

The guards are one of the clearest things that differentiate SyReTo from a generic collection of analysis scripts.

They turn the project from:

- “a set of transforms that can produce tables”

into:

- “a review system that can also say when its own outputs should not be trusted yet”

That distinction is operationally important.

## Related Tests

The guard layer is covered by dedicated test modules in both `syreto/tests` and `03_analysis/tests`, including:

- audit log integrity tests
- record-id map integrity tests
- epistemic consistency tests
- template term guard tests
- python source integrity tests
- daily-run mode tests covering guard behavior

This makes the guard contract explicit rather than purely documentary.

## Related Docs

- [pipeline-overview.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/pipeline-overview.md)
- [artifact-catalog.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/artifact-catalog.md)
- [daily-operations.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/daily-operations.md)
- [failure-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/failure-model.md)
- [execution-contract.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/execution-contract.md)

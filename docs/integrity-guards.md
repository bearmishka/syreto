# Integrity Guards

## Purpose

SyReTo does not treat integrity checks as optional lint around the edges of the workflow. Integrity guards are part of the system contract: they are what let the project claim that its outputs remain operationally trustworthy.

## Why Guards Exist

A review pipeline can appear to “work” while drifting into an invalid state.

Examples:

- audit history is silently damaged
- record identities drift after deduplication
- manuscript outputs still contain unresolved placeholders
- source integrity degrades in ways tests do not immediately expose
- screening logic and manuscript-facing outputs become epistemically inconsistent

SyReTo addresses these risks with dedicated guards rather than relying on team vigilance alone.

## Main Guard Types

### Audit Log Integrity Guard

Script:

- [`03_analysis/audit_log_integrity_guard.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/audit_log_integrity_guard.py)

Purpose:

- verifies that the audit log exists in the expected structure
- checks whether the log header and basic integrity expectations hold

Primary input:

- [`02_data/processed/audit_log.csv`](/Users/pigra/Documents/New%20project/syreto_clean/02_data/processed/audit_log.csv)

### Record ID Map Integrity Guard

Script:

- [`03_analysis/record_id_map_integrity_guard.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/record_id_map_integrity_guard.py)

Purpose:

- checks whether record identity mapping remains stable across deduplication-related transformations

Primary input:

- [`02_data/processed/record_id_map.csv`](/Users/pigra/Documents/New%20project/syreto_clean/02_data/processed/record_id_map.csv)

### Epistemic Consistency Guard

Script:

- [`03_analysis/epistemic_consistency_guard.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/epistemic_consistency_guard.py)

Purpose:

- checks whether downstream synthesis and reporting artifacts remain consistent with the review’s actual decision state
- acts as a final checkpoint in `daily_run.sh`

This is one of the strongest signals that SyReTo is trying to guard the reasoning surface of the review, not just file syntax.

### Template Term Guard

Script:

- [`03_analysis/template_term_guard.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/template_term_guard.py)

Purpose:

- scans protocol and manuscript-facing content for unresolved placeholders or banned template leakage

Typical use cases:

- preflight scan of protocol/search files
- post-generation scan of manuscript-facing outputs

Primary output:

- `outputs/template_term_guard_summary.md`

### Python Source Guard

Script:

- [`03_analysis/python_source_guard.py`](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/python_source_guard.py)

Purpose:

- detects structural integrity issues in source files that should not silently enter the codebase

This is less about the review state and more about keeping the pipeline implementation itself trustworthy.

## How Guards Behave In Practice

Guards are not all used in exactly the same place.

- some run early as preflight checks
- some run during the main pipeline body
- some run as part of the final operational checkpoint
- some are stricter in `production` than in `template` mode

This layered placement is important: SyReTo is trying to catch problems at the most useful moment, not only at the very end.

## Relationship To Status Artifacts

Guard results feed into the operational status layer rather than floating around as isolated script outputs.

The most useful summary surfaces remain:

- `outputs/status_summary.json`
- `outputs/status_report.md`
- `outputs/todo_action_plan.md`

This means guards are part of the same operational story as the rest of the pipeline.

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

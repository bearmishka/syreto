# Daily Operations

## Purpose

This page describes how SyReTo is intended to be operated day to day: how to run the pipeline, how to interpret success and failure, and which outputs to check first.

## Primary Operational Entry Point

The main operational entry point is:

```bash
cd 03_analysis
bash daily_run.sh
```

This script is the closest thing SyReTo has to a production runbook. It is not just a convenience wrapper; it is the main orchestration layer for the review system.

## What `daily_run.sh` Does

At a high level, `daily_run.sh`:

- validates mode and environment settings
- runs preflight placeholder checks
- consolidates screening state
- validates core CSV inputs
- runs integrity guards
- generates synthesis, status, and manuscript-facing outputs
- runs optional extension stages when enabled
- executes a mandatory final status checkpoint
- prints the set of generated outputs at the end

## Default Operating Modes

The most important operational setting is `REVIEW_MODE`.

### `REVIEW_MODE=template`

This is the safer mode for scaffolding, development, and partial review setup.

- the status gate is informational
- template leakage checks can warn instead of fail
- epistemic guard runs in a non-blocking posture

### `REVIEW_MODE=production`

This is the stricter mode for a review that is expected to behave like a real operational system.

- status findings at or above the configured threshold can block the run
- template leakage in manuscript-facing outputs is enforced strictly
- the final epistemic consistency checkpoint is enforced in fail mode

## High-Value Environment Knobs

The orchestration layer exposes many toggles, but these are the ones most worth understanding early:

- `REVIEW_MODE`
- `STATUS_FAIL_ON`
- `STATUS_PRIORITY_POLICY`
- `RUN_TEMPLATE_TERM_GUARD`
- `RUN_TRANSPARENCY_APPENDIX_SYNC`
- `RUN_PROSPERO_DRAFTER`
- `RUN_CITATION_TRACKING`
- `RUN_RETRACTION_CHECKER`
- `RUN_PUBLICATION_BIAS`
- `RUN_LIVING_REVIEW_SCHEDULER`
- `RUN_KEYWORD_ANALYSIS`

Examples:

```bash
REVIEW_MODE=template bash daily_run.sh
REVIEW_MODE=production STATUS_FAIL_ON=major bash daily_run.sh
RUN_PROSPERO_DRAFTER=0 RUN_CITATION_TRACKING=0 bash daily_run.sh
```

## What To Check After A Run

A successful run is more than “the shell returned.”

Check these first:

1. `daily_run.sh` exited with code `0`
2. `outputs/status_summary.json` was generated or updated
3. `outputs/status_report.md` was generated or updated
4. `outputs/todo_action_plan.md` was generated or updated
5. expected manuscript-facing outputs were regenerated

Fast checks:

```bash
syreto doctor
syreto status
```

```bash
syreto artifacts --kind operational
```

```bash
cd 03_analysis
python status_cli.py --input outputs/status_summary.json
```

## How To Interpret Failures

When `daily_run.sh` fails, the important question is not just “which line failed?” but “which operational layer failed?”

Typical failure classes:

- invalid runtime mode or environment configuration
- missing or malformed canonical CSV inputs
- integrity guard failures
- production status gate failure
- placeholder leakage into manuscript-facing outputs
- optional stage failures when an enabled extension is not ready

## Transactional Behavior

The daily run layer includes transactional settings and snapshot support.

Relevant environment controls include:

- `DAILY_RUN_TRANSACTIONAL`
- `DAILY_RUN_KEEP_TRANSACTION_SNAPSHOT`
- `DAILY_RUN_TRANSACTION_ROOT`
- `DAILY_RUN_TRANSACTION_PATHS`

This makes the pipeline more recoverable than a simple fire-and-forget shell script.

## Key Operational Outputs

The most useful operational outputs are:

- `outputs/status_summary.json`
- `outputs/status_report.md`
- `outputs/todo_action_plan.md`
- `outputs/daily_run_manifest.json`
- `outputs/status_cli_snapshot.txt`
- `outputs/template_term_guard_summary.md`
- `outputs/epistemic_consistency_report.md`

If `daily_run.sh` creates `outputs/daily_run_failed.marker`, treat the run as incomplete even if some downstream artifacts were produced.

## Recommended Daily Rhythm

For normal usage, the pattern should be:

1. update canonical inputs under `02_data/`
2. run `daily_run.sh`
3. inspect `syreto doctor` and `syreto status`
4. inspect `status_report.md` and `todo_action_plan.md`
5. confirm expected manuscript-facing artifacts were regenerated
6. commit the new review state and generated outputs together when appropriate

## Related Docs

- [pipeline-overview.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/pipeline-overview.md)
- [artifact-catalog.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/artifact-catalog.md)
- [integrity-guards.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/integrity-guards.md)
- [failure-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/failure-model.md)

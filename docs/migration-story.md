# Migration Story

**Document ID:** SYRETO-MIG-v0.1
**Status:** Draft
**Scope:** Describes how existing review projects move from the legacy SyReTo posture to the newer config-aware, schema-aware, and observability-aware model

---

## 1. Purpose

This document explains how an existing review project should migrate toward the
newer SyReTo architecture without creating a split between:

- the old shell-and-artifact surface
- the new config-aware review-instance surface
- the new `StudyTable` internal contract
- the new observability layer

Without an explicit migration story, the new model would compete with the old
one instead of gradually replacing it.

The purpose of this document is to make the transition path explicit.

---

## 2. Core Principle

Migration in SyReTo is intended to be **layered**, not revolutionary.

The migration rule is:

- preserve canonical files
- preserve the current execution spine while it remains authoritative
- add explicit contracts around that spine
- move consumers to new internal contracts gradually
- avoid introducing a second hidden source of truth

In other words:

- the new model should first wrap and clarify the old system
- only later should it replace old assumptions where necessary

---

## 3. The Four Migration Axes

Existing review projects may transition along four partially independent axes:

1. legacy repository without explicit review config
2. config-aware review instance (`review.toml`)
3. `StudyTable` as downstream internal contract
4. observability event stream and run postmortem surface

These axes should not be treated as all-or-nothing.

---

## 4. Legacy Project Without Config

### 4.1 Current Meaning

A legacy project is one that still runs primarily through:

- `03_analysis/daily_run.sh`
- repository-aligned `02_data/`, `03_analysis/outputs/`, and `04_manuscript/`
- implicit repository shape rather than explicit review-instance config

This remains a supported posture.

### 4.2 What Does Not Need To Change Immediately

Legacy projects do **NOT** need to immediately:

- move data into `reviews/<id>/data/`
- restructure protocol files
- adopt path-decoupled execution
- rewrite artifact locations

### 4.3 Recommended First Step

The first migration step for a legacy project is:

```bash
syreto doctor
syreto status
syreto observability
```

This establishes operational visibility before any structural migration.

---

## 5. Migration To Config-Aware Review Instances

### 5.1 Transitional Model

The first config migration step is **not** full path decoupling.

It is the introduction of an explicit, versioned review-instance file that still
matches the current repository-aligned spine.

That is what [reviews/repo-default/review.toml](/Users/pigra/Documents/New%20project/syreto_clean/reviews/repo-default/review.toml) is for.

### 5.2 Practical Migration Path

For an existing legacy review:

1. start with a repository-aligned `review.toml`
2. keep paths aligned to the current repository layout
3. use:
   - `syreto doctor --config ...`
   - `syreto status --config ...`
   - `syreto review run --config ...`
4. treat this as explicit configuration over the old spine, not a second system

### 5.3 What Counts As Success

Config migration is successful when:

- the review instance has a versioned `review.toml`
- CLI entrypoints use that config
- the config does not conflict with current repository layout
- the team no longer depends on implicit path knowledge alone

### 5.4 What Is Not Yet Required

At this stage, migration does **NOT** require:

- review-local `data/`, `protocol/`, `outputs/`, and `manuscript/`
- full stage-toggle enforcement
- abandoning the current repository-aligned layout

This is an explicit transitional posture, not a half-finished failure.

---

## 6. Migration To StudyTable

### 6.1 Scope

`StudyTable` migration is **internal**.

It affects downstream consumers, not canonical upstream CSV truth.

### 6.2 Rule

Migration to `StudyTable` means:

- files remain canonical
- study-level consumers stop reinventing harmonization
- repeated downstream transitions use a shared internal contract

### 6.3 What Existing Projects Do Not Need To Do

Existing review repositories do **NOT** need to:

- rewrite extraction CSVs into a new master schema
- create a giant review object
- abandon current file boundaries

### 6.4 What Developers Should Do Instead

Migration should happen consumer by consumer.

Good order:

1. modules with repeated study-level alias handling
2. modules with repeated inclusion semantics
3. modules with repeated sorting/filtering behavior

In practice, this means migrating:

- synthesis
- appraisal/profile layers
- summary/reporting layers
- plotting
- export

before attempting any wider object model.

### 6.5 What Counts As Success

`StudyTable` migration is successful when:

- downstream consumers rely on canonicalized study-level columns
- legacy alias logic is centralized
- included-study semantics are stable across consumers
- canonical files remain the source of truth

---

## 7. Migration To Observability Events

### 7.1 Scope

Observability migration is also additive.

It does not replace execution or status.

It adds a temporal explanation layer.

### 7.2 Migration Rule

Existing review runs should first gain:

- `outputs/run_events.jsonl`
- `syreto observability`

without changing:

- exit-code semantics
- status semantics
- execution decision logic

### 7.3 What Existing Projects Do Not Need To Do

Projects do **NOT** need to:

- adopt dashboards
- adopt tracing infrastructure
- reinterpret status from observability alone

### 7.4 What Counts As Success

Observability migration is successful when:

- run events exist
- postmortem is possible without guessing
- observability remains explanatory, not controlling
- `status_summary.json` and `run_events.jsonl` complement each other rather than compete

---

## 8. Recommended Migration Sequence

For a real existing review project, the recommended order is:

1. stabilize legacy run behavior
2. make status/failure/observability visible
3. introduce repository-aligned `review.toml`
4. switch daily operations to config-aware CLI entrypoints
5. migrate downstream consumers to `StudyTable`
6. only later consider deeper path decoupling or richer review-local layout

This order matters.

It ensures that:

- visibility improves before structure changes
- explicit config appears before layout migration
- internal contracts improve before broad architectural replacement

---

## 9. Anti-Pattern To Avoid

The main migration anti-pattern is:

- leave the old path in place
- add a new path next to it
- never define which one is authoritative

Examples of this anti-pattern:

- running both implicit and config-aware review entrypoints without declaring the config path authoritative
- keeping ad hoc study-level harmonization while also claiming `StudyTable` is the contract
- writing observability data but still depending on anecdotal postmortem

This creates architectural competition instead of migration.

SyReTo should migrate by making the new layer explicit and authoritative step by step.

---

## 10. Operational Checklist

### 10.1 Legacy To Config-Aware

- add `review.toml`
- validate with `syreto doctor --config`
- use `syreto status --config`
- use `syreto review run --config`

### 10.2 Legacy Consumers To StudyTable

- identify repeated alias handling
- centralize harmonization
- switch one downstream consumer at a time
- keep file truth unchanged

### 10.3 Legacy Runs To Observability-Aware Runs

- confirm `outputs/run_events.jsonl` exists
- use `syreto observability`
- verify failure/postmortem semantics stay aligned with status artifacts

---

## 11. Relationship To Other Contracts

- [review-instance-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/review-instance-model.md)
- [review-config-schema.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/review-config-schema.md)
- [study-table-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/study-table-model.md)
- [observability-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/observability-model.md)
- [execution-contract.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/execution-contract.md)
- [versioning-policy.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/versioning-policy.md)

Together, these documents explain not only what the new architecture is, but
how existing projects move into it without losing operational clarity.

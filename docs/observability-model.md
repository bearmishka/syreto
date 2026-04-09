# Observability Model

**Document ID:** `SYRETO-OBS-v0.1`
**Status:** Draft
**Scope:** Defines the minimal observability contract for pipeline execution in SyReTo

---

## 1. Purpose

This document defines the **Observability Model** for SyReTo.

Its purpose is to make pipeline execution legible in time without changing the execution logic itself.

The model exists to answer questions such as:

- which step ran
- when it started
- when it finished
- whether it succeeded or failed
- why it failed
- which outputs it touched

This is not a workflow engine, a tracing platform, or a control layer. It is a structured record of execution.

---

## 2. Core Principle

SyReTo observability follows a strict non-interference rule:

> Observability may record execution, explain execution, and surface execution, but it must not control execution.

In operational terms, observability:

- **MAY** record step events
- **MAY** summarize execution history
- **MAY** support postmortem analysis

but it **MUST NOT**:

- decide which steps run
- alter pipeline behavior
- suppress failures
- repair outputs
- become a hidden orchestration layer

---

## 3. Minimal Event Model

The minimal event unit is a **run event**.

Each event describes a single pipeline step in time.

Example:

```json
{
  "run_id": "20260409T101500Z-12345",
  "review_mode": "production",
  "step_order": 12,
  "step": "deduplication",
  "started_at": "2026-04-09T10:15:00Z",
  "finished_at": "2026-04-09T10:15:12Z",
  "duration": 12.0,
  "status": "success",
  "failure_reason": null,
  "outputs_touched": [
    "outputs/dedup_merge_summary.md",
    "../02_data/processed/record_id_map.csv"
  ]
}
```

---

## 4. File Contract

The canonical event stream file is:

```text
outputs/run_events.jsonl
```

This file **MUST** be:

- append-only
- chronological
- machine-readable
- line-delimited JSON

Each line represents exactly one event object.

The file is intended to complement, not replace, higher-level status artifacts.

---

## 5. Required Event Fields

The minimal event schema should include:

- `run_id`
- `review_mode`
- `step_order`
- `step`
- `started_at`
- `finished_at`
- `duration`
- `status`
- `failure_reason`
- `outputs_touched`

Where:

- `status` is expected to be one of `success`, `failed`, or `warning`
- `failure_reason` is `null` on success and populated on failure
- `outputs_touched` is a list of paths that the step generated, modified, or attempted to update

---

## 6. Relationship To Existing SyReTo Artifacts

The observability stream does **NOT** replace existing operational artifacts.

It sits beside them.

Current relationship:

- `outputs/run_events.jsonl` records step-level temporal execution
- `outputs/status_summary.json` records consolidated project status
- `outputs/status_report.md` explains review state in a human-facing summary
- `outputs/daily_run_manifest.json` records run-level metadata
- `outputs/daily_run_failed.marker` records terminal failed-run state

In other words:

- run events explain **what happened over time**
- status artifacts explain **what state the project is now in**

---

## 7. Postmortem Use

The observability model exists in part to support postmortem analysis without guesswork.

Instead of:

- “something broke”

the system should be able to show:

- which step failed
- at what time
- after which prior steps
- with which failure reason
- after touching which outputs

Example postmortem interpretation:

```json
{
  "step": "screening",
  "status": "failed",
  "failure_reason": "missing inclusion criteria"
}
```

This makes failures auditable rather than anecdotal.

---

## 8. Append-Only Semantics

The event stream should be treated as append-only for a given run.

This means:

- earlier events are not rewritten to hide later failures
- success/failure progression remains visible
- temporal order is preserved

If a run fails and rollback occurs, the event stream should still explain that failure and the rollback attempt rather than pretending the failure never happened.

---

## 9. Deterministic Scope

The observability layer is not required to be globally identical across every machine down to formatting details, but it **MUST** preserve semantically stable execution facts.

Given the same run structure, it should communicate the same:

- step identities
- ordering
- statuses
- failure reasons
- output touch patterns

This is observability in service of temporal honesty, not decorative logging.

---

## 10. Non-Goals (v0.1)

The minimal observability model does **NOT** include:

- distributed tracing
- metrics backends
- OpenTelemetry integration
- runtime decision-making
- retries or self-healing logic
- dashboard requirements

Those may exist in the future, but they are not part of the minimal contract.

---

## 11. Relationship To Architecture

The observability model belongs to the **observability layer**, not to the execution layer.

This aligns with the intended architectural separation:

- core: pipeline execution and state
- interfaces: CLI and future APIs
- tasks: concrete review steps
- observability: temporal record, status visibility, postmortem evidence

This separation is important because SyReTo’s claims depend not just on producing outputs, but on producing outputs whose execution history is inspectable.

---

## 12. Recommended Implementation Path

The minimal implementation path is:

1. define the event schema
2. write append-only events from `daily_run.sh`
3. store them in `outputs/run_events.jsonl`
4. leave `status_summary.json` and `status_report.md` unchanged
5. optionally teach `doctor` and `status` to read the event stream later

This preserves architectural discipline:

- execution remains in the pipeline
- observability remains explanatory
- interfaces remain thin

---

## 13. Summary

The SyReTo Observability Model establishes:

- a minimal machine-readable event record for pipeline execution
- a postmortem-friendly history of step outcomes
- a strict non-interference rule
- a foundation for future observability improvements without platform drift

It is a formalization of temporal honesty in execution.

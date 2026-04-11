# Reproducibility Contract

**Document ID:** `SYRETO-REP-v0.1`
**Status:** Draft
**Scope:** Defines what SyReTo means by reproducibility, under which conditions that claim holds, and which surfaces remain intentionally non-deterministic

---

## 1. Purpose

This document defines the **Reproducibility Contract** of SyReTo.

It answers the operational question:

`when can two runs legitimately be expected to produce the same trustworthy outputs?`

The contract exists to make SyReTo’s reproducibility claim explicit rather than implied.

It defines:

- what is meant by “same run”
- which inputs and parameters must match
- which outputs are expected to match
- which environment constraints matter
- which elements are intentionally non-deterministic
- how non-deterministic surfaces are controlled or interpreted

---

## 2. Core Claim

The core SyReTo reproducibility claim is:

> identical review configuration plus identical canonical inputs should yield identical canonical outputs, provided the execution environment and enabled required stages are materially equivalent.

This is the center of the product’s reproducibility promise.

It does **not** mean that every file produced by every run is byte-for-byte identical under all circumstances.

It does mean that the canonical state-bearing outputs of a trustworthy run should agree.

---

## 3. What Must Be Identical

For the reproducibility claim to hold, the following must remain identical:

### 3.1 Review Configuration

This includes:

- review identity and selected `review.toml`
- `review_mode`
- status threshold and policy
- active required stages
- output profile when applicable

If configuration changes, the system should treat the run as a different execution posture rather than the “same run again.”

### 3.2 Canonical Inputs

This includes versioned source-of-truth artifacts such as:

- canonical review-state CSV files
- extraction and appraisal tables
- processed identity and audit files
- protocol-facing files when they are part of the execution posture

Generated outputs are not substitutes for canonical inputs.

### 3.3 Required Stage Set

The reproducibility claim assumes the same required execution spine is used.

If a required stage is skipped, disabled, or newly introduced, the run is not equivalent for contract purposes.

---

## 4. What Outputs Are Expected To Match

When the reproducibility conditions hold, the following canonical outputs are expected to match in meaning and operational acceptability:

- `outputs/status_summary.json`
- `outputs/status_report.md`
- `outputs/todo_action_plan.md`
- `outputs/daily_run_manifest.json` except for explicitly non-deterministic metadata
- `outputs/review_descriptives.json`
- `outputs/review_descriptives.md`
- stable analytics figures and manuscript-facing generated tables/sections, when their upstream required inputs are unchanged
- integrity and checkpoint outcomes

At a minimum, reproducible runs should agree on:

- pass/fail posture
- blocking findings
- generated canonical summary content
- review-state and manuscript-facing derived content

Where provenance sidecars are present, they should also agree on upstream artifact linkage, producing script identity, review mode, and review-config reference, while timestamps remain intentionally time-variant metadata.

For the broader artifact inventory, see [artifact-catalog.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/artifact-catalog.md).

---

## 5. Environment Constraints

SyReTo is not claiming reproducibility in a vacuum.

The contract assumes a materially compatible environment.

In the current repository posture, that means:

- Python `>=3.11`
- the dependency set described by [`pyproject.toml`](/Users/pigra/Documents/New%20project/syreto_clean/pyproject.toml)
- the locked environment represented by `uv.lock` when using the repository’s intended setup path
- the same relevant script and package versions in the repository checkout
- no hidden local modifications to pipeline logic or canonical inputs

The strongest practical posture is:

1. same git revision
2. same `uv.lock`
3. same `review.toml`
4. same canonical inputs

This is the closest thing SyReTo has to a reproducible execution envelope.

---

## 6. Material Equivalence vs Exact Identity

The contract is primarily about **material reproducibility**, not decorative sameness.

Material reproducibility means:

- the same trust-bearing conclusions about run validity
- the same canonical downstream review state
- the same blocking or non-blocking status interpretation
- the same study-level and manuscript-facing derived substance

Byte-for-byte identity may still be expected for many outputs, but the contract is centered on operational and semantic equivalence first.

---

## 7. Controlled Non-Determinism

Some surfaces are intentionally or unavoidably non-deterministic in limited ways.

These include:

- wall-clock timestamps
- run identifiers
- duration measurements
- event timing details in `run_events.jsonl`
- file modification times at the filesystem level
- optional stage outputs whose presence depends on explicitly enabled extensions or optional inputs

These are not reproducibility failures by themselves.

They are acceptable when they do **not** change:

- canonical review-state meaning
- status posture
- integrity outcomes
- required manuscript-facing content

---

## 8. How Non-Determinism Is Controlled

SyReTo controls non-deterministic surfaces by keeping them outside the core truth claim where possible.

In practice:

- canonical inputs remain explicit and file-based
- run events record timing without controlling execution
- observability is append-only and explanatory
- generated outputs are expected to be rebuilt from upstream state
- optional extensions do not redefine the core reproducibility claim

This lets the system remain honest about time and execution history without weakening its deterministic core.

---

## 9. What Breaks The Reproducibility Claim

The reproducibility contract does not hold when any of the following change materially:

- canonical inputs
- selected review configuration
- required execution stages
- status thresholds or execution mode
- repository code or dependency posture
- optional inputs that become operationally required for the selected run

The contract also fails when:

- a run is invalid
- stale outputs are reused across changed inputs
- rollback state remains unresolved
- downstream artifacts are manually edited in place

For those cases, see [recovery-rerun-semantics.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/recovery-rerun-semantics.md).

---

## 10. What SyReTo Guarantees

Within the stated envelope, SyReTo guarantees that:

- reproducibility is anchored in explicit config and explicit canonical inputs
- the core execution spine is intended to be deterministic
- status and integrity outcomes are part of the reproducibility promise, not separate from it
- non-deterministic execution metadata is treated as observational rather than epistemic

This means reproducibility is not just about regenerating files.

It is about regenerating a trustworthy review posture.

---

## 11. What SyReTo Does Not Guarantee

SyReTo does not guarantee:

- identical outputs across materially different environments
- identical outputs when optional-stage posture changes
- trustworthiness of outputs from invalid or partially failed runs
- that observability timestamps or run IDs remain stable across reruns
- that manual edits to derived artifacts preserve reproducibility

These non-guarantees are part of the contract, not exceptions to it.

---

## 12. Relationship To Other Contracts

- [execution-contract.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/execution-contract.md)
- [review-instance-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/review-instance-model.md)
- [review-config-schema.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/review-config-schema.md)
- [observability-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/observability-model.md)
- [recovery-rerun-semantics.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/recovery-rerun-semantics.md)
- [artifact-catalog.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/artifact-catalog.md)

Together, these documents define not only how SyReTo runs, but how it justifies the claim that its runs can be reproduced and trusted.

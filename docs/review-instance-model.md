# Review Instance Model

**Document ID:** `SYRETO-RIM-v0.1`
**Status:** Draft
**Scope:** Defines the configuration model for a single reproducible review instance in SyReTo

---

## 1. Purpose

This document defines the **Review Instance Model (RIM)**: a structured, version-controlled configuration that represents a single systematic review within SyReTo.

The goal of the RIM is to:

- make review execution explicit and reproducible
- separate configuration from execution logic
- provide a stable interface for CLI tools such as `status`, `doctor`, and `review run`
- ensure that all epistemically relevant parameters are externally visible and versioned

For the concrete field-level schema and a practical example config, see [review-config-schema.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/review-config-schema.md).

For the separate statement of when identical config and identical inputs are expected to reproduce identical canonical outputs, see [reproducibility-contract.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/reproducibility-contract.md).

---

## 2. Conceptual Definition

A **review instance** is a fully specified unit of execution that includes:

- identity: what review is this
- structure: where inputs and outputs live
- protocol: what methodological constraints apply
- execution mode: how the pipeline behaves
- policy: what constitutes a valid result

The review instance is defined by a single configuration file:

```text
reviews/<review-id>/review.toml
```

---

## 3. Design Principles

### 3.1 Explicitness Over Convenience

All parameters that affect:

- data selection
- pipeline behavior
- output generation
- validation criteria

**MUST** be declared in the config file.

The CLI **MUST NOT** introduce hidden defaults that alter epistemic behavior.

---

### 3.2 Versioned State

The review config:

- **MUST** be stored in version control
- **MUST** be diffable
- **MUST** represent the authoritative definition of the review instance

---

### 3.3 Separation of Concerns

| Layer | Responsibility |
|---|---|
| Config (`review.toml`) | declares parameters |
| Pipeline (Python/bash) | executes logic |
| CLI | selects and invokes |

This yields the intended SyReTo architecture:

- logic in Python/bash
- state in versioned files
- parameters in explicit config
- invocation through a thin CLI

---

### 3.4 Determinism

Given:

- identical config
- identical inputs

the system **MUST** produce:

- identical outputs
- identical status results

---

## 4. File Structure

The conceptual target structure is:

```text
reviews/
  ai-ethics/
    review.toml
    protocol/
    data/
    outputs/
    manuscript/
```

This does not require immediate migration of the current repository layout. It defines the model that future CLI and review-instance selection should target.

---

## 5. Configuration Schema (v0.1)

### 5.1 Identity

```toml
[review]
id = "ai-ethics"
title = "AI Ethics and Decision Systems"
```

---

### 5.2 Paths

```toml
[paths]
protocol = "protocol/"
data_root = "data/"
manuscript_root = "manuscript/"
outputs = "outputs/"
```

All paths are **relative to the review root directory**.

---

### 5.3 Execution Mode

```toml
[mode]
review_mode = "production"  # or "template"
```

This maps directly onto the existing operational distinction already used by `daily_run.sh`.

---

### 5.4 Stage Control

```toml
[stages]
search = true
deduplication = true
screening = true
extraction = true
synthesis = true
```

Stages define which parts of the pipeline are active.

In early adoption, this section may first act as a declared contract before it becomes a fully enforced execution switchboard.

---

### 5.5 Status Policy

```toml
[status]
fail_on = "major"   # options: none, minor, major, critical
```

This defines the threshold at which a run is considered invalid.

---

### 5.6 Output Profile (Optional)

```toml
[output]
profile = "default"
```

This is a future extension point for:

- journal-specific formats
- export variants
- manuscript packaging profiles

---

## 6. CLI Interaction Model

### 6.1 Selection

```bash
syreto review run --config reviews/ai-ethics/review.toml
```

The CLI:

- **MUST** load config
- **MUST NOT** override epistemic parameters

---

### 6.2 Status

```bash
syreto status --config ...
```

Status should:

- read config
- resolve review-specific paths
- evaluate pipeline outputs in the selected review instance

---

### 6.3 Doctor

```bash
syreto doctor --config ...
```

Doctor should:

- validate environment
- validate config completeness
- detect missing required inputs
- explain whether the selected review instance is operationally ready

---

## 7. Epistemic Boundary

The config defines the **epistemic boundary of the system**.

The following **MUST NOT** be defined implicitly:

- dataset location
- protocol
- validation thresholds
- stage activation

All such parameters **MUST** reside in `review.toml`.

This yields the core discipline:

> CLI may select a review instance, but must not define its epistemic structure.

---

## 8. Non-Goals (v0.1)

The Review Instance Model does **NOT** include:

- a workflow DSL
- an orchestration engine
- dynamic pipeline generation
- AI-assisted reasoning

This is a configuration model, not a platform rewrite.

---

## 9. Future Extensions

Potential additions include:

- multiple datasets per review
- versioned protocol definitions
- cross-review linking
- audit trails at config level
- review-instance-aware status dashboards

---

## 10. Relationship To Current SyReTo

The RIM is intentionally designed to avoid architectural rupture.

It does **NOT** require:

- replacing the shell spine
- removing Python orchestration logic
- introducing a workflow engine
- moving epistemic logic into the CLI

Instead, it provides a disciplined next layer over the current system:

- existing pipeline logic remains in Python/bash
- existing source-of-truth files remain versioned
- review-specific parameters become explicit and diffable
- CLI becomes a selector of review instances rather than a hidden source of meaning

---

## 11. Summary

The Review Instance Model establishes:

- a single source of truth for review configuration
- a deterministic execution contract
- a foundation for future review-aware CLI tooling
- a path to treating systematic reviews as formal, reproducible computational objects

It is the next step in fixing SyReTo not merely as a collection of scripts, but as a coherent, reproducible review system.

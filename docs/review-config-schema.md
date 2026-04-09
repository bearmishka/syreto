# Review Config Schema

## Purpose

This page defines the practical schema for `review.toml`.

It complements [review-instance-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/review-instance-model.md) by turning the conceptual model into a concrete configuration contract that future loaders, validators, and CLI entrypoints can implement.

The goal is not to create a workflow DSL. The goal is to make a single review instance explicit, versioned, and inspectable.

## Scope

`review.toml` is the canonical configuration file for a single review instance.

It is intended to define:

- review identity
- review-specific paths
- execution posture
- stage toggles
- status policy
- optional output profile selection

It is **not** intended to store live review state, extracted evidence, screening decisions, or manuscript content.

## File Location

The intended file location is:

```text
reviews/<review-id>/review.toml
```

The review root is the directory that contains this file.

All relative paths in the config are resolved relative to that review root.

## Minimal Schema

### `[review]`

Required fields:

- `id`
- `title`

Example:

```toml
[review]
id = "ai-ethics"
title = "AI Ethics and Decision Systems"
```

Semantics:

- `id` is the stable machine-readable identifier for the review instance
- `title` is the human-readable display title

### `[paths]`

Required fields:

- `data_root`
- `protocol_root`
- `outputs_root`
- `manuscript_root`

Example:

```toml
[paths]
data_root = "data/"
protocol_root = "protocol/"
outputs_root = "outputs/"
manuscript_root = "manuscript/"
```

Semantics:

- `data_root` points to canonical review inputs and processed data
- `protocol_root` points to protocol-facing files for the selected review
- `outputs_root` points to generated operational artifacts
- `manuscript_root` points to manuscript-facing generated artifacts

These paths define execution posture, not review evidence itself.

### `[mode]`

Required fields:

- `review_mode`

Example:

```toml
[mode]
review_mode = "production"
```

Allowed values in v0.1:

- `template`
- `production`

Semantics:

- `template` is a permissive posture for scaffolding and incomplete setup
- `production` is the strict posture for trustworthy review runs

### `[stages]`

Required fields:

- `search`
- `deduplication`
- `screening`
- `extraction`
- `synthesis`
- `reporting`

Example:

```toml
[stages]
search = true
deduplication = true
screening = true
extraction = true
synthesis = true
reporting = true
```

Semantics:

- stage flags declare which parts of the review pipeline are intended to be active
- in early adoption, these fields may act as declared contract before they become fully enforced switches

### `[status]`

Required fields:

- `fail_on`

Optional fields:

- `priority_policy`

Example:

```toml
[status]
fail_on = "major"
priority_policy = "default"
```

Allowed `fail_on` values in v0.1:

- `none`
- `minor`
- `major`
- `critical`

Semantics:

- `fail_on` defines the threshold at which the run is considered invalid
- `priority_policy` is reserved for repository-specific status evaluation policy where relevant

### `[output]`

Optional fields:

- `profile`

Example:

```toml
[output]
profile = "default"
```

Semantics:

- `profile` is a future extension point for output packaging and formatting variants
- it does not redefine the epistemic structure of the review

## Full Example

```toml
[review]
id = "ai-ethics"
title = "AI Ethics and Decision Systems"

[paths]
data_root = "data/"
protocol_root = "protocol/"
outputs_root = "outputs/"
manuscript_root = "manuscript/"

[mode]
review_mode = "production"

[stages]
search = true
deduplication = true
screening = true
extraction = true
synthesis = true
reporting = true

[status]
fail_on = "major"
priority_policy = "default"

[output]
profile = "default"
```

## What Must Not Live In `review.toml`

The config must not become a hidden domain model or workflow engine.

The following do **not** belong in `review.toml`:

- extracted study rows
- screening decisions
- study-level evidence
- generated manuscript text
- ad hoc per-run mutable state
- execution logic implemented as a DSL

In short:

- config defines execution posture
- artifacts define review state

## Validation Expectations

A future config loader or validator should check at least:

- required sections exist
- required fields exist
- path values are relative and resolvable from review root
- `review_mode` is valid
- `fail_on` is valid
- stage toggles are booleans

It should also report configuration failures in the same operational language used elsewhere in SyReTo, especially:

- `config error`
- `missing artifact`
- `environment problem`

## Relationship To Other Docs

- [review-instance-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/review-instance-model.md)
- [execution-contract.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/execution-contract.md)
- [failure-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/failure-model.md)

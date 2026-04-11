# Versioning Policy

**Document ID:** SYRETO-VP-v0.1
**Status:** Draft
**Scope:** Defines how SyReTo versions releases, schemas, and migration-relevant contracts

---

## 1. Purpose

This document defines how SyReTo interprets version changes.

The goal is not only release hygiene. The goal is to make it explicit when a
change is operationally safe, when it changes repository contracts, and when
existing review instances may need migration.

This matters especially for:

- artifact contracts
- `review.toml` and review-instance configuration
- `StudyTable` as an internal downstream contract
- generated outputs that are consumed by status, analytics, or manuscript layers

---

## 2. Baseline Rule

SyReTo follows a semantic-versioning-style policy:

```text
MAJOR.MINOR.PATCH
```

Interpretation:

- **PATCH**: fixes, clarifications, or safe internal changes that do not break declared contracts
- **MINOR**: additive or behavior-extending changes that preserve existing declared contracts
- **MAJOR**: breaking changes to public, operational, or schema-level contracts

SyReTo is not only a Python package.

Its version therefore applies to multiple contract layers at once:

- CLI surface
- artifact contracts
- review-config schema
- internal-but-documented schemas such as `StudyTable`
- migration expectations for versioned review instances

---

## 3. What Counts As a Breaking Change

A change is considered **breaking** if an existing, correctly versioned review
repository or downstream consumer can no longer rely on a previously documented
contract without adjustment.

This includes breakage to:

- command-line semantics
- generated artifact shape
- schema requirements
- config interpretation
- migration assumptions for review instances

Breakage is defined semantically, not only syntactically.

---

## 4. PATCH Version

PATCH releases are for changes that do **not** require users to reinterpret or
migrate declared contracts.

Examples:

- bug fixes that restore intended behavior
- stricter tests without changing the contract itself
- implementation refactors that preserve artifacts and semantics
- docs clarifications
- non-breaking pre-commit, CI, or packaging fixes
- additive smoke fixtures that do not change runtime contracts

PATCH releases **MUST NOT**:

- remove expected artifact fields
- reinterpret `review.toml` fields
- rename `StudyTable` columns that downstream consumers rely on
- change meaning of status or failure classes

---

## 5. MINOR Version

MINOR releases are for **additive** changes that expand the system while
preserving existing documented behavior.

Examples:

- a new optional CLI command
- a new optional artifact
- a new optional field in `review.toml`
- a new optional `StudyTable` column
- a new fixture or new CI scenario
- a new analytics artifact that does not change prior outputs

MINOR releases may add capabilities, but they **MUST** preserve:

- previously valid configs
- previously valid artifact schemas
- existing CLI meanings
- existing review-instance migration assumptions

Additive changes should remain backward-compatible by default.

---

## 6. MAJOR Version

MAJOR releases are required when SyReTo changes a declared contract in a way
that requires user intervention, repository updates, or consumer migration.

Examples:

- a required artifact field is removed or renamed
- a canonical output path changes
- an existing CLI command changes meaning
- `review.toml` required fields change incompatibly
- a previously optional config field becomes required
- a `StudyTable` required column changes name or semantics
- a review instance requires migration to remain runnable or trustworthy

If a user must update a versioned review instance, a workflow, or a downstream
consumer for the release to remain valid, the change is MAJOR unless a clear
backward-compatibility layer is preserved.

---

## 7. Artifact Contract Policy

Artifact contracts are version-relevant because SyReTo is artifact-driven.

### 7.1 PATCH-safe Artifact Changes

- bug fix in artifact generation that preserves declared schema
- formatting cleanup in human-facing reports without semantic contract change
- additive metadata that existing consumers can ignore

### 7.2 MINOR Artifact Changes

- new optional artifact
- new optional field in machine-readable artifact
- new optional section in markdown output

### 7.3 MAJOR Artifact Changes

- removal of a declared artifact
- incompatible rename of artifact path
- incompatible rename or removal of required fields
- changing artifact semantics so that old consumers misinterpret the output

For artifact details, see [artifact-catalog.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/artifact-catalog.md).

---

## 8. Review Config Schema Policy

`review.toml` is version-sensitive because it defines review-instance posture.

### 8.1 PATCH-safe Config Changes

- loader bug fix
- better validation diagnostics
- docs clarification for existing fields

### 8.2 MINOR Config Changes

- new optional section
- new optional field
- broader support for existing values, if previous values remain valid

### 8.3 MAJOR Config Changes

- rename or removal of required fields
- incompatible path interpretation changes
- incompatible meaning change for existing fields
- invalidating previously valid `review.toml` files without compatibility shim

When config schema changes affect existing review instances, SyReTo **MUST**
either:

- preserve backward compatibility, or
- publish explicit migration guidance

For field-level schema details, see [review-config-schema.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/review-config-schema.md).

---

## 9. StudyTable Schema Policy

`StudyTable` is an internal contract, but it is a documented internal contract.

That means it still needs version discipline.

### 9.1 PATCH-safe StudyTable Changes

- implementation cleanup in harmonization logic with unchanged canonical columns
- bug fixes that restore intended alias mapping

### 9.2 MINOR StudyTable Changes

- new optional canonical columns
- new helper functions around the same schema
- stronger tests for existing invariants

### 9.3 MAJOR StudyTable Changes

- rename or removal of required columns such as `study_id`
- semantic reinterpretation of existing canonical columns
- changing inclusion semantics in a way that breaks downstream expectations

If downstream consumers need code changes to remain correct, the schema change is
MAJOR unless compatibility aliases remain in place.

For the model itself, see [study-table-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/study-table-model.md).

---

## 10. Review Instance Migration Policy

When version changes affect review instances, migration expectations must be explicit.

### 10.1 No Migration Needed

This corresponds to PATCH releases and many MINOR releases.

Existing review instances remain valid without changes.

### 10.2 Soft Migration

This typically corresponds to additive MINOR releases.

Examples:

- optional new `review.toml` field
- optional new artifact
- optional new smoke fixture or analytics output

Existing review instances remain runnable, but maintainers may choose to adopt
the new capability.

### 10.3 Required Migration

This corresponds to MAJOR releases.

Examples:

- config schema changes that invalidate old review instances
- artifact contract changes that require downstream updates
- `StudyTable` contract changes that require migration of consumers

Required migrations **MUST** be accompanied by:

- explicit changelog notes
- upgrade instructions
- if feasible, a compatibility shim or migration script

---

## 11. Changelog Expectations

Release notes should make versioning consequences explicit.

At minimum, a release note should say whether the release contains:

- no contract changes
- additive contract changes
- breaking contract changes

For breaking changes, notes should explicitly name the affected layer:

- artifact contract
- config schema
- `StudyTable` schema
- CLI surface
- review-instance migration

---

## 12. Practical Rule Of Thumb

Use this decision sequence:

1. Did a documented contract change?
2. If no, release may be PATCH.
3. If yes, was the change purely additive and backward-compatible?
4. If yes, release may be MINOR.
5. If users must change existing configs, consumers, or review instances, release is MAJOR.

---

## 13. Summary

SyReTo versions more than Python code.

It versions:

- operational behavior
- artifact contracts
- review configuration
- study-level data contracts
- migration expectations for review instances

The key principle is simple:

- PATCH preserves contracts
- MINOR extends contracts
- MAJOR breaks contracts and requires migration planning

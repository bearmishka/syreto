# Study Table Model

**Document ID:** SYRETO-STM-v0.1
**Status:** Draft
**Scope:** Defines the minimal study-level internal contract for downstream synthesis, appraisal, plotting, and export in SyReTo

---

## 1. Purpose

This document defines the **Study Table Model (STM)**: a minimal, documented, and testable study-level data contract for downstream processing in SyReTo.

The STM exists to reduce repeated glue code across:

* synthesis tables
* GRADE-style appraisal
* results summaries
* forest-plot preparation
* bibliographic export

The STM is **not** a universal model of the entire review. It is a targeted internal contract for repeated study-level transitions.

---

## 2. Design Position

The Study Table Model follows the architectural rule:

* files remain canonical artifacts
* typed tables become internal contracts

The STM therefore:

* **MUST NOT** replace CSV artifacts as the source of truth
* **MUST NOT** become a hidden in-memory review object
* **MUST NOT** absorb record-level, screening-level, or workflow-level state
* **MUST** provide a stable schema that downstream code can rely on

---

## 3. Why This Model Exists

SyReTo already contains a de facto study-level contract, but it is currently repeated in multiple places.

Examples include:

* extraction schema validation in [validate_extraction.py](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/validate_extraction.py#L8)
* study characteristics rendering in [synthesis_tables.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/synthesis_tables.py#L39)
* effect harmonization and CI fallback in [forest_plot_generator.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/forest_plot_generator.py#L368)
* study-level certainty profiling in [grade_evidence_profiler.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/grade_evidence_profiler.py#L73)
* study export fallback rules in [export_to_ris.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/export_to_ris.py#L134)

The STM makes that implicit contract explicit.

---

## 4. Unit of Observation

The STM is a **study-level table**.

The intended unit is:

* **one row = one study-level observation**

The STM **MUST NOT** mix into the same row:

* screening events
* record-level bibliographic entries
* arm-level data
* effect-size-level data
* workflow execution state

If such structures are needed, they should live in separate artifacts.

---

## 5. Primary Key

The primary key is:

```text
study_id
```

Rules:

* `study_id` **MUST** be present
* `study_id` **MUST** be non-empty for included studies
* downstream joins **SHOULD** use `study_id` as the first join strategy
* other identifiers may support fallback matching, but do not replace the key

---

## 6. Minimal Schema (v0.1)

The following fields define the minimal STM contract.

### 6.1 Identity

Required:

* `study_id`

Recommended:

* `first_author`
* `year`
* `country`

### 6.2 Design and Context

Recommended:

* `study_design`
* `setting`
* `framework`

### 6.3 Sample and Population

Recommended:

* `sample_size`
* `age_mean`
* `age_range`
* `sex_distribution`

### 6.4 Condition Framing

Recommended:

* `condition_diagnostic_method`
* `condition_diagnostic_system`
* `diagnostic_frame_detail`
* `condition_definition`

### 6.5 Predictor and Outcome

Recommended:

* `predictor_construct`
* `predictor_instrument_type`
* `predictor_instrument_name`
* `predictor_subscale`
* `predictor_respondent_type`
* `outcome_construct`
* `outcome_measure`

### 6.6 Effect Core

Recommended:

* `main_effect_metric`
* `main_effect_value`
* `effect_direction`
* `ci_lower`
* `ci_upper`
* `p_value`

### 6.7 Inclusion and Quality

Recommended:

* `consensus_status`
* `quality_appraisal`

---

## 7. Harmonization Rules

The STM assumes a canonicalized view over legacy extraction columns.

Examples of expected harmonization include:

* `author` -> `first_author`
* `object_relation_construct` -> `predictor_construct`
* `identity_construct` -> `outcome_construct`
* `identity_measure` -> `outcome_measure`
* `effect_measure` / `effect_metric` -> `main_effect_metric`
* `effect_value` -> `main_effect_value`
* `risk_of_bias` -> `quality_appraisal`

The harmonization layer **MUST** live in Python.

The STM **MUST NOT** rely on downstream modules individually reinventing alias logic.

---

## 8. Invariants

The STM is valid only if the following invariants hold.

### 8.1 Identity Invariants

* `study_id` is present as a column
* included rows do not use an empty `study_id`

### 8.2 Shape Invariants

* each row represents one study-level observation
* duplicate `study_id` rows require an explicit policy before downstream use

### 8.3 Effect Invariants

* if `main_effect_value` is present, `main_effect_metric` should also be present
* if confidence intervals are present, they should be aligned with the effect metric
* `effect_direction` should be normalized to a bounded vocabulary where possible

### 8.4 Downstream Legibility

* missing data may be explicit
* silent schema drift is not allowed
* consumers may assume canonical column names once STM has been constructed

---

## 9. Non-Goals

The STM does not attempt to represent:

* the full review as a single object
* bibliographic master-record truth
* screening history
* workflow orchestration state
* arm-level or effect-size-level statistical structures

Those concerns should remain in separate artifacts and contracts.

---

## 10. Current Adoption Surface

The STM is no longer only a design target. It is already used in several downstream modules.

Current adoption includes:

1. [synthesis_tables.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/synthesis_tables.py)
2. [grade_evidence_profiler.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/grade_evidence_profiler.py)
3. [results_summary_table_builder.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/results_summary_table_builder.py)
4. [forest_plot_generator.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/forest_plot_generator.py)
5. [export_to_ris.py](/Users/pigra/Documents/New%20project/syreto_clean/syreto/export_to_ris.py)

These modules now rely on a shared internal study-level contract instead of each maintaining its own selection, harmonization, and sorting logic.

## 11. Remaining Adoption Targets

The next likely adoption targets are:

1. [validate_extraction.py](/Users/pigra/Documents/New%20project/syreto_clean/03_analysis/validate_extraction.py)
2. modules that still duplicate study-level alias handling or inclusion semantics
3. future analytics builders that need stable study-level inputs

---

## 12. Relationship to Other Models

The STM sits below the review-level architecture documents.

* [review-instance-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/review-instance-model.md) defines how a review is configured
* [failure-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/failure-model.md) defines how invalid runs are interpreted
* [observability-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/observability-model.md) defines how execution is recorded

The STM answers a narrower question:

* what study-level shape downstream code may safely assume

---

## 13. Summary

The Study Table Model is a minimal internal contract for repeated study-level transitions in SyReTo.

It is valuable because it:

* reduces repeated harmonization logic
* makes downstream assumptions explicit
* improves testability
* preserves file-based truth
* strengthens operational legibility without introducing a universal runtime object

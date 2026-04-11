# Review Analytics Model

**Document ID:** SYRETO-RAM-v0.1
**Status:** Draft
**Scope:** Defines the descriptive analytics and visualization layer for review-state inspection in SyReTo

---

## 1. Purpose

This document defines the **Review Analytics Model (RAM)** for SyReTo.

The RAM exists to make a review legible as a dataset, not only executable as a pipeline.

Its goals are to:

* provide structured descriptive analytics over the review corpus
* surface distributions, coverage, and imbalance in included studies
* support fast sanity checks during review operations
* generate reproducible visual summaries as standard artifacts

The RAM extends SyReTo from a review execution system into a review inspection system.

---

## 2. Positioning

SyReTo should not be understood only as a pipeline runner.

It should be understood as:

* an operating system for systematic reviews
* a reproducible execution environment
* an inspectable analytics surface over review state

The RAM therefore answers a distinct question:

* what does the current review corpus look like as structured evidence?

---

## 3. Design Principles

### 3.1 Descriptive First

The first version of RAM prioritizes:

* descriptive statistics
* distribution summaries
* simple review-state plots

It does **not** begin with advanced exploratory modeling.

This means the initial analytics layer should focus on:

* counts
* frequencies
* coverage summaries
* simple cross-tabulations
* deterministic plots

---

### 3.2 Analytics Must Follow Canonical Artifacts

The RAM **MUST** operate on versioned review artifacts.

Analytics outputs should be derived from:

* extraction tables
* synthesis tables
* quality appraisal tables
* status and manifest outputs

Analytics **MUST NOT** become a new hidden source of truth.

---

### 3.3 Visualization Is an Output Layer

Visualizations:

* explain the review corpus
* summarize evidence structure
* support post-run inspection

Visualizations **MUST NOT** control execution, validation, or eligibility decisions.

---

### 3.4 Deterministic Outputs

Given the same canonical inputs, RAM outputs **MUST** be reproducible.

This includes:

* identical summary counts
* identical cross-tabulations
* identical figures

---

## 4. Scope of the First Analytics Layer

The initial RAM scope should be limited to review-state descriptives.

### 4.1 Study-Level Descriptives

Examples:

* number of included studies
* publication year distribution
* country distribution
* study design distribution
* sample-size distribution

### 4.2 Measurement and Construct Coverage

Examples:

* predictor construct frequencies
* outcome construct frequencies
* instrument-type frequencies
* predictor × outcome co-occurrence summaries

### 4.3 Appraisal and Synthesis Coverage

Examples:

* quality-band distribution
* certainty-grade distribution
* number of studies contributing to each outcome
* participant coverage by outcome

---

## 5. Standard Analytics Artifacts

The first RAM implementation should produce a small, stable artifact set.

Suggested outputs:

```text
outputs/review_descriptives.json
outputs/review_descriptives.json.provenance.json
outputs/review_descriptives.md
outputs/review_descriptives.md.provenance.json
outputs/figures/year_distribution.png
outputs/figures/study_design_distribution.png
outputs/figures/country_distribution.png
outputs/figures/quality_band_distribution.png
outputs/figures/predictor_outcome_heatmap.png
```

Not every figure is mandatory in every run, but the artifact family should be predictable.
Where present, provenance sidecars make the analytics layer easier to audit without changing the analytics payload itself.

---

## 6. Relationship to Existing Outputs

The RAM complements existing SyReTo outputs.

It does not replace:

* [status_summary.json](/Users/pigra/Documents/New%20project/syreto_clean/outputs/status_summary.json)
* manuscript tables
* forest plots
* PRISMA outputs
* observability logs

Instead, it adds a review-state inspection layer between raw artifacts and narrative reporting.

---

## 7. Suggested First Visual Surface

The first visual surface should stay simple and reproducible.

### 7.1 Core Figures

Recommended first figures:

* PRISMA flow diagram
* year distribution
* study design distribution
* country distribution
* quality-band distribution

### 7.2 Optional Figures

Later additions may include:

* predictor × outcome heatmap
* sample-size histogram
* certainty-grade bar plot
* outcome coverage plot

---

## 8. Non-Goals (v0.1)

The first RAM version does not require:

* topic clustering
* PCA
* spectral analysis
* embeddings
* interactive dashboards
* exploratory modeling that changes review conclusions

These may become later optional layers, but are not required for the first analytics contract.

---

## 9. Why Advanced Analytics Is Deferred

Advanced analytics can be useful, but it is not the first source of value.

For SyReTo, the first value comes from making the review corpus:

* countable
* inspectable
* auditable
* visually legible

Exploratory analytics should only be added after the descriptive layer is stable.

---

## 10. Integration with Study Table Model

The RAM should build on the [study-table-model.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/study-table-model.md) contract whenever possible.

That means:

* study-level descriptives should derive from canonical study-level fields
* repeated analytics should consume harmonized columns
* file-based truth remains intact

The Study Table Model supplies stable study-level inputs.
The Review Analytics Model supplies stable descriptive outputs.

---

## 11. CLI Surface

The RAM suggests a future CLI layer such as:

```bash
syreto analytics descriptives
syreto analytics figures
syreto analytics all
```

The CLI should:

* invoke analytics generation
* select outputs
* report where artifacts were written

The CLI should not embed analytics logic itself.

---

## 12. Summary

The Review Analytics Model defines a minimal analytics and visualization layer for SyReTo.

Its role is to make review state:

* visible
* measurable
* reproducible
* easier to interpret

This is the layer that moves SyReTo beyond execution alone and toward a full operating system for systematic reviews.

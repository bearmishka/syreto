# Golden Review Fixtures

## Purpose

This page defines the role of small golden review fixtures in SyReTo.

Golden review fixtures are intentionally tiny, versioned review samples that can be used for:

- CI smoke
- regression detection
- deterministic-output checks
- documentation and demonstration
- future end-to-end golden runs

## Why They Matter

SyReTo is not only a library of reusable functions.

It is an operational scientific system.

That means small review-sized fixtures are valuable because they let the project test and demonstrate:

- input contracts
- builder determinism
- artifact generation
- CLI and review-surface smoke behavior
- future full-run reproducibility

## Current Fixture Surface

The current first golden fixture is:

- [reviews/fixtures/minimal-golden](/Users/pigra/Documents/New%20project/syreto_clean/reviews/fixtures/minimal-golden)
- [reviews/fixtures/repo-smoke](/Users/pigra/Documents/New%20project/syreto_clean/reviews/fixtures/repo-smoke)

It currently covers:

- minimal review config
- extraction input
- quality-band input
- expected analytics outputs

The `repo-smoke` fixture currently covers:

- repository-aligned `review.toml`
- config-aware `syreto review run --config ...`
- manifest creation
- run-event stream creation
- failed-marker absence on successful smoke runs
- core status artifact smoke generation through the current orchestration spine

Its current focus is the `StudyTable` and review-analytics surface rather than the full repository-aligned `daily_run.sh` orchestration spine.

## Why Start Small

The first fixture should be small enough to stay:

- readable
- inspectable
- easy to diff
- stable in CI

That is more useful than trying to create a huge synthetic review that becomes hard to maintain.

## Intended Uses

Golden fixtures are especially well suited for:

- analytics regression tests
- stable-output tests
- artifact contract checks
- future config-aware smoke runs

Over time, SyReTo can grow toward:

- one tiny golden review fixture
- one slightly richer fixture with more outputs
- one repository-aligned end-to-end smoke fixture
- and later one stricter end-to-end golden run with stronger output expectations

## Relationship To Other Contracts

- [test-taxonomy.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/test-taxonomy.md)
- [reproducibility-contract.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/reproducibility-contract.md)
- [artifact-catalog.md](/Users/pigra/Documents/New%20project/syreto_clean/docs/artifact-catalog.md)

Together, these documents explain not only what SyReTo promises, but how those promises are checked on stable review-sized samples.

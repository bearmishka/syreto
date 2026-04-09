# Contributing to SyReTo

SyReTo is maintained as a deterministic, artifact-driven system for systematic reviews. This document is intended to keep development disciplined, reproducible, and easy to review.

## Development Setup

Requires:

- Python `>=3.11`
- `uv`

Initial setup:

```bash
git clone https://github.com/bearmishka/syreto.git
cd syreto
uv sync --all-groups
uv run pre-commit install
```

## Core Commands

Run the main checks before opening a pull request:

```bash
uv run pytest -q
uv run pre-commit run --all-files
```

Useful targeted commands:

```bash
syreto doctor
syreto status
syreto analytics
syreto review run
```

## Development Rules

SyReTo is not maintained as a loose script collection. When contributing, preserve these constraints:

- keep pipeline logic in Python or shell, not in documentation or ad hoc manual steps
- keep canonical truth in versioned files and generated artifacts, not hidden runtime objects
- keep CLI thin: it may select or invoke, but should not become the hidden decision layer
- keep LaTeX as a presentation layer, not a logic layer
- keep observability explanatory, not controlling

## Artifact Discipline

Before committing, check whether your change affects:

- operational artifacts under `outputs/`
- manuscript-facing artifacts under `04_manuscript/`
- CLI behavior
- status/failure/observability contracts
- study-level data contracts

If any of those change, update the relevant docs in `README.md` or `docs/`.

## What To Commit

Commit:

- code
- tests
- docs
- configuration
- small canonical fixture/data files that are required for tests or system contracts

Do not commit:

- accidental local caches
- transient local outputs that are not part of the repository contract
- editor files
- environment-specific noise

If a generated artifact is part of the intended contract, make that explicit in the PR description.

## Testing Expectations

At minimum, contributors should run the checks relevant to the change.

Examples:

- CLI changes: run targeted CLI tests and `pre-commit`
- pipeline or shell changes: run affected tests plus syntax checks
- data-contract changes: run tests for impacted downstream consumers
- docs-only changes: run the relevant docs-only `pre-commit` checks

When in doubt, run:

```bash
uv run pytest -q
uv run pre-commit run --all-files
```

## Pull Request Expectations

A good PR should make it easy to answer:

- what changed
- why it changed
- what contracts were affected
- how it was validated
- whether docs were updated

PRs that change CLI behavior, artifact contracts, observability, failure semantics, or review-state models should explicitly say so.

## Change Categories That Need Extra Care

Please call out these categories clearly in the PR:

- `daily_run.sh` or orchestration changes
- output artifact contract changes
- review-state schema changes
- `StudyTable` or analytics changes
- manuscript-generation changes
- status/failure/observability changes

These areas have downstream effects and should not be presented as “small refactors” if behavior changed.

## Recommended Workflow

1. Make the smallest coherent change you can.
2. Add or update tests close to the changed contract.
3. Update docs if the user-facing or contributor-facing behavior changed.
4. Run checks.
5. Open a PR with a clear validation summary.

## Questions and Discussion

If you are unsure whether a change affects a contract, assume that it does and mention it explicitly in the PR. In SyReTo, clarity about operational consequences is more valuable than minimizing apparent scope.

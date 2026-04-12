# Changelog

## Unreleased

### Architecture

- established `syreto/` as the canonical Python implementation layer for reusable module logic
- converted legacy top-level modules in `03_analysis/` into compatibility wrappers around package-owned implementations, including status, CSV validation, template-term guard, provenance, progress history, reporting/export, and PROSPERO surfaces
- moved `prospero_submission_drafter_layers` into `syreto/` as canonical layered package code while preserving `03_analysis/` compatibility imports
- made the packaged script registry package-owned so script discovery resolves canonical modules from `syreto/` while still executing legacy shell-facing entrypoints where required

### Contracts, Validation, And Observability

- wired the failure model through status-facing outputs and summaries
- added reusable CSV schema contracts shared by `doctor` and `status`
- enriched `run_events.jsonl` with step kinds and input context
- expanded provenance coverage and visibility across operational CLI surfaces
- strengthened repo-smoke golden data contracts for trust-bearing outputs

### Documentation

- replaced repository-local absolute links with repo-relative documentation links
- aligned the README and architecture docs with the package-ownership model and shell-spine split
- clarified execution, pipeline, and test-taxonomy docs so `03_analysis/` is described as orchestration plus compatibility rather than a second implementation tree

### Testing And Fixtures

- reduced mirrored test drift by sharing broken `python_source_guard` fixtures across package and legacy test surfaces
- kept only compatibility-specific assertions in the remaining intentionally divergent `03_analysis/tests` modules
- reran and stabilized the full repository test suite after the architectural cleanup; current full-suite status is `578 passed`

## 0.2.0 - 2026-04-09

- migrated project tooling to `uv`, `pyproject.toml`, and `pre-commit`
- restored and stabilized the full pytest suite across `syreto/tests` and `03_analysis/tests`
- added missing data fixtures required by the analysis and reporting workflows
- cleaned up Ruff/pre-commit integration so repository-wide checks pass
- introduced a thin shared LaTeX table rendering helper for safer, more uniform `.tex` output

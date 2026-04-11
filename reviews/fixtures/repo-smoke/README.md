# Repo Smoke Review Fixture

This fixture is a repository-aligned smoke review used for end-to-end orchestration checks.

Its role is different from `minimal-golden`:

- `minimal-golden` checks stable analytics outputs on a tiny review-sized sample
- `repo-smoke` checks that the current `daily_run.sh` spine can be launched through `syreto review run --config ...`

This fixture intentionally uses repository-aligned paths so that it stays compatible with the current hardcoded orchestration spine.

The repository-aligned sample is intentionally tiny but coherent: a few canonical rows are present in the core processed CSVs so the smoke surface has not only schema truth, but also a small amount of content-level review truth.

It is meant for:

- config-aware run smoke tests
- execution-surface regression checks
- manifest and observability checks
- failed-marker and success-marker semantics

The fixture also stores a small normalized golden subset under `expected/` for:

- manifest semantics
- status summary semantics
- required successful run-event steps
- one trust-bearing canonical data artifact (`master_records.csv`)
- one trust-bearing screening-state artifact (`screening_title_abstract_results.csv`)
- one trust-bearing PRISMA input artifact (`prisma_counts_template.csv`)
- one canonical tabular artifact (`reviewer_workload_plan.csv`)
- one human-facing report (`reviewer_workload_balancer_summary.md`)
- one stable manuscript-facing artifact (`prisma_counts_table.tex`)

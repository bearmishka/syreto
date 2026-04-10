# Repo Smoke Production Review Fixture

This fixture is the production-mode counterpart to `repo-smoke`.

It exists to check that the same repository-aligned smoke review behaves differently when the run posture switches from `template` to `production`.

Its current purpose is mode-matrix regression, not full golden output comparison.

It also reuses the same workload-plan CSV and markdown summary expectations as
the template-mode `repo-smoke` fixture so that production smoke still covers
execution truth, data truth, and reporting truth.

It also keeps one tiny manuscript-facing `.tex` artifact in the golden subset
so the production-mode smoke still checks a minimal reporting-to-manuscript contract.

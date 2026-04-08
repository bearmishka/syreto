# Changelog

## 0.2.0 - 2026-04-09

- migrated project tooling to `uv`, `pyproject.toml`, and `pre-commit`
- restored and stabilized the full pytest suite across `syreto/tests` and `03_analysis/tests`
- added missing data fixtures required by the analysis and reporting workflows
- cleaned up Ruff/pre-commit integration so repository-wide checks pass
- introduced a thin shared LaTeX table rendering helper for safer, more uniform `.tex` output

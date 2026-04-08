# SyReTo — Systematic Review Toolkit

A deterministic, git-native toolkit for fully reproducible PRISMA-compliant systematic reviews — from search to manuscript.

## What is SyReTo?

Systematic reviews are often conducted using ad hoc spreadsheets, opaque commercial tools, and partially reproducible workflows. This makes auditability, replication, and protocol adherence difficult in practice.

SyReTo is an open-source pipeline for conducting reproducible systematic reviews with a full audit trail. It covers the complete review lifecycle: literature search, deduplication, screening, quality appraisal, data extraction, meta-analysis, and manuscript generation.

Every analytical decision is logged. Every state is replayable through git history. The pipeline avoids probabilistic inference and machine-learning classifiers, relying instead on explicit, rule-based logic with transparent thresholds.

## Key Features

- **Full pipeline coverage.** 48 scripts spanning PubMed fetch -> deduplication -> title/abstract screening -> full-text screening -> quality appraisal (JBI & NOS with bidirectional conversion) -> data extraction validation -> effect size conversion -> meta-analysis -> forest plots -> sensitivity analysis -> subgroup analysis -> publication bias assessment -> PRISMA flow diagram -> PRISMA adherence check (27/27 items) -> PROSPERO submission drafting -> LaTeX manuscript generation.
- **Deterministic by design.** Deduplication uses RapidFuzz (Levenshtein distance) with explicit thresholds. Screening decisions are recorded with reviewer initials and timestamps. No step depends on non-reproducible inference.
- **Integrity guards.** Five dedicated guard scripts validate not just data but the research process itself:
  - `audit_log_integrity_guard` verifies the audit log has not been tampered with
  - `record_id_map_integrity_guard` ensures record identity stability across deduplication
  - `epistemic_consistency_guard` checks that screening decisions are internally consistent
  - `python_source_guard` validates source code structural integrity
  - `template_term_guard` detects placeholder terms that should have been replaced
- **Daily orchestration.** `daily_run.sh` runs the entire pipeline with atomic transactions, preflight checks, and manifest generation.
- **Git-native audit trail.** CSV files are the single data format. `master_records.csv` is the current state; git history is the immutable ledger.
- **Installable package.** CLI commands include `syreto`, `syreto-status`, and `syreto-draft`.

## Installation

Requires Python >= 3.11.

```bash
git clone https://github.com/bearmishka/syreto.git
cd syreto
uv sync --all-groups
```

If you prefer `pip`:

```bash
pip install .
pip install pytest ruff pre-commit
```

### Development setup

```bash
uv run pytest
uv run pre-commit install
uv run pre-commit run --all-files
```

## Quick Start

```bash
syreto list
syreto-status
syreto run prisma_tables
cd 03_analysis
bash daily_run.sh
```

## Project Structure

```text
syreto/                  # Installable Python package
03_analysis/             # Analysis scripts
pyproject.toml           # Build config, dependencies, tooling
uv.lock                  # Locked dependency set for uv
```

## Running Tests

```bash
uv run pytest
```

At the moment, the repository test suite is mostly green, with a few failures tied to missing repository data files under `02_data/`.

## Dependencies

- Python >= 3.11
- pandas >= 2.0
- matplotlib >= 3.7
- scipy >= 1.10
- rapidfuzz >= 3.9

## Contributing

Contributions are welcome. Please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Run the tests (`uv run pytest`)
4. Run the linter (`uv run ruff check .`)
5. Open a pull request

## Citation

If you use SyReTo in your research, please cite:

```bibtex
@software{profatilova2026syreto,
  author       = {Profatilova, Evgeniya and Konstantinidis, Ilias},
  title        = {{SyReTo}: A Deterministic Toolkit for Reproducible Systematic Reviews},
  year         = {2026},
  url          = {https://github.com/bearmishka/syreto},
  version      = {0.1.0}
}
```

## License

MIT — see [LICENSE](LICENSE) for details.

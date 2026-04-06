# SyReTo — Systematic Review Toolkit

A deterministic, git-native toolkit for fully reproducible PRISMA-compliant systematic reviews — from search to manuscript.

## What is SyReTo?

Systematic reviews are often conducted using ad hoc spreadsheets, opaque commercial tools, and partially reproducible workflows. This makes auditability, replication, and protocol adherence difficult in practice.

SyReTo is an open-source pipeline for conducting reproducible systematic reviews with a full audit trail. It covers the complete review lifecycle: literature search, deduplication, screening, quality appraisal, data extraction, meta-analysis, and manuscript generation.

Every analytical decision is logged. Every state is replayable through git history. The pipeline avoids probabilistic inference and machine-learning classifiers, relying instead on explicit, rule-based logic with transparent thresholds.

## Key Features

- **Full pipeline coverage.** 48 scripts spanning PubMed fetch → deduplication → title/abstract screening → full-text screening → quality appraisal (JBI & NOS with bidirectional conversion) → data extraction validation → effect size conversion → meta-analysis → forest plots → sensitivity analysis → subgroup analysis → publication bias assessment → PRISMA flow diagram → PRISMA adherence check (27/27 items) → PROSPERO submission drafting → LaTeX manuscript generation.

- **Deterministic by design.** Deduplication uses RapidFuzz (Levenshtein distance) with explicit thresholds. Screening decisions are recorded with reviewer initials and timestamps. No step depends on non-reproducible inference.

- **Integrity guards.** Five dedicated guard scripts validate not just data but the research process itself:
  - `audit_log_integrity_guard` — verifies the audit log has not been tampered with
  - `record_id_map_integrity_guard` — ensures record identity stability across deduplication
  - `epistemic_consistency_guard` — checks that screening decisions are internally consistent
  - `python_source_guard` — validates source code structural integrity
  - `template_term_guard` — detects placeholder terms that should have been replaced

- **Daily orchestration.** `daily_run.sh` (1,070 lines) runs the entire pipeline with atomic transactions, preflight checks, and manifest generation. It functions as a CI/CD system for your systematic review.

- **Git-native audit trail.** CSV files are the single data format. `master_records.csv` is the current state; git history is the immutable ledger. Every change is a commit, every commit is traceable.

- **Installable package.** `pip install .` provides CLI commands: `syreto list`, `syreto run <script>`, `syreto-status`, `syreto-draft`.

## Installation

Requires Python ≥ 3.11.

```bash
git clone https://github.com/bearmishka/syreto.git
cd syreto
pip install .
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv sync
```

### Development setup

```bash
pip install -r requirements-dev.txt
# or
uv sync --group dev
```

## Quick Start

```bash
# List all available scripts
syreto list

# Check project status
syreto-status

# Run a specific script
syreto run prisma_tables

# Run the daily orchestration pipeline
cd 03_analysis
bash daily_run.sh
```

### Typical workflow

1. **Define your protocol** — populate `01_protocol/` with your PICO, search strings, and screening rules.
2. **Fetch records** — `syreto run pubmed_fetch` retrieves records and logs the search in `search_log.csv`.
3. **Deduplicate** — `syreto run dedup_merge` identifies and merges duplicates using fuzzy matching with explicit thresholds.
4. **Screen** — record title/abstract and full-text screening decisions in the structured CSV templates. `syreto run screening_metrics` tracks progress and inter-rater agreement.
5. **Appraise quality** — `syreto run quality_appraisal` supports JBI and NOS checklists with bidirectional conversion (`jbi_to_nos_converter`, `nos_to_jbi_converter`).
6. **Extract and validate** — `syreto run validate_extraction` checks extracted data against the codebook schema and harmonizes legacy column formats.
7. **Analyse** — `syreto run meta_analysis_results_builder` computes pooled effects; `syreto run forest_plot_generator` produces forest plots; `syreto run sensitivity_analysis_builder` and `syreto run subgroup_analysis_builder` generate robustness checks.
8. **Check PRISMA compliance** — `syreto run prisma_adherence_checker` audits your manuscript sections against all 27 PRISMA 2020 items.
9. **Draft PROSPERO registration** — `syreto run prospero_submission_drafter` pre-fills registration fields from your protocol and data.
10. **Run the daily pipeline** — `bash daily_run.sh` executes all guards, generates outputs, and writes a manifest to confirm integrity.

## Project Structure

```
syreto/                  # Installable Python package
  __init__.py            # Package API (AVAILABLE_SCRIPTS, run_script, etc.)
  cli.py                 # CLI entrypoints (syreto, syreto-status, syreto-draft)
  scripts.py             # Script discovery and execution
  analysis/
    __init__.py
    registry.py          # Script registry with ScriptSpec dataclass

03_analysis/             # Analysis scripts (48 modules)
  daily_run.sh           # Daily orchestration pipeline
  priority_policy.json   # Configurable output priority policy
  tests/                 # 57 test files, 258 passing tests
    fixtures/            # Test fixtures

pyproject.toml           # Build config, dependencies, CLI entrypoints, mypy config
requirements.txt         # Runtime dependencies
requirements-dev.txt     # Development dependencies (pytest, ruff, pre-commit)
```

## Running Tests

```bash
cd 03_analysis
python -m pytest tests/ -v
```

All 258 tests should pass. Tests cover edge cases, PRISMA cross-file consistency, atomic output recovery, epistemic guard modes, and CSV validation logic.

## Comparison with Existing Tools

| Feature | SyReTo | Covidence | Rayyan | RevMan | BibDedupe |
|---------|--------|-----------|--------|--------|-----------|
| Full pipeline (search → manuscript) | ✓ | — | — | — | — |
| Deduplication | ✓ | ✓ | ✓ | — | ✓ |
| Screening | ✓ | ✓ | ✓ | — | — |
| Quality appraisal (JBI + NOS) | ✓ | ✓ | — | — | — |
| Meta-analysis + forest plots | ✓ | — | — | ✓ | — |
| PRISMA adherence check | ✓ | — | — | — | — |
| PROSPERO prefill | ✓ | — | — | — | — |
| Publication bias assessment | ✓ | — | — | ✓ | — |
| Git-native audit trail | ✓ | — | — | — | — |
| Integrity guards | ✓ | — | — | — | — |
| Deterministic (no ML) | ✓ | — | — | ✓ | — |
| Open source | ✓ | — | — | ✓ | ✓ |
| Free | ✓ | $198/review | $119/yr | Free | Free |

## Design Principles

SyReTo is built on three principles aligned with recent work on epistemic governance in institutional reasoning systems (Konstantinidis, 2026):

1. **Non-erasure memory.** Past decisions are never overwritten. The CSV + git architecture ensures that every screening decision, quality rating, and analytical choice is preserved in the form in which it was made.

2. **Deterministic auditability.** Every output can be reproduced from the same inputs and the same version of the code. There are no stochastic components, no model weights, no API calls that might return different results.

3. **Separation of fact and interpretation.** Raw data (`02_data/raw/`) is kept strictly separate from processed data (`02_data/processed/`) and analytical outputs (`03_analysis/outputs/`). The pipeline never modifies source records.

## Dependencies

- Python ≥ 3.11
- pandas ≥ 2.0
- matplotlib ≥ 3.7
- scipy ≥ 1.10
- rapidfuzz ≥ 3.9

## Contributing

Contributions are welcome. Please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Run the tests (`python -m pytest tests/ -v`)
4. Run the linter (`ruff check .`)
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

A JOSS paper describing SyReTo is in preparation.

## License

MIT — see [LICENSE](LICENSE) for details.

## Acknowledgements

This project was developed as part of a systematic review on psychodynamic approaches to bulimia nervosa. The pipeline architecture is informed by principles of epistemic governance and non-erasure memory in institutional reasoning (Konstantinidis, 2026).


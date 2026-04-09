# SyReTo

SyReTo is a deterministic, git-native toolkit for running PRISMA-aligned systematic reviews as a reproducible pipeline rather than a loose collection of spreadsheets and one-off scripts.

## What This Is

SyReTo packages a full review workflow around explicit CSV inputs, rule-based analysis scripts, integrity guards, operational status checks, and manuscript-ready outputs.

It is designed to make a systematic review run like an auditable system:

- inputs live in versioned files
- transformations are scripted and replayable
- outputs are regenerated rather than edited by hand
- process failures are surfaced as status findings instead of being silently absorbed

## Who It Is For

SyReTo is for teams who want a systematic review workflow that is:

- reproducible across reruns and collaborators
- inspectable in plain files
- compatible with Git-based review and audit trails
- manuscript-oriented rather than notebook-oriented

It is especially suited to review teams who are comfortable working with CSV, Markdown, LaTeX, and command-line tooling.

## What Problem It Solves

Many systematic reviews are operationally fragile:

- search exports, screening decisions, appraisal sheets, and extraction tables drift apart
- manuscript tables are updated manually and become inconsistent with underlying data
- it is hard to tell whether a pipeline run succeeded cleanly or only partially
- auditability depends on team memory rather than explicit artifacts

SyReTo addresses that by treating the review as a deterministic production pipeline with explicit checkpoints, generated artifacts, and integrity guards.

## What It Produces

SyReTo produces both operational artifacts and manuscript-facing artifacts.

Operational outputs include:

- `outputs/status_summary.json`
- `outputs/status_report.md`
- `outputs/todo_action_plan.md`
- `outputs/prisma_flow_diagram.svg`
- `outputs/prisma_flow_diagram.tex`
- `outputs/dedup_merge_summary.md`
- `outputs/dedup_stats_summary.md`
- `outputs/epistemic_consistency_report.md`
- `outputs/progress_history.csv`
- `outputs/progress_history_summary.md`

Manuscript-facing outputs include:

- `04_manuscript/tables/prisma_counts_table.tex`
- `04_manuscript/tables/fulltext_exclusion_table.tex`
- `04_manuscript/tables/study_characteristics_table.tex`
- `04_manuscript/tables/grade_evidence_profile_table.tex`
- `04_manuscript/tables/results_summary_table.tex`
- `04_manuscript/tables/decision_trace_table.tex`
- `04_manuscript/tables/analysis_trace_table.tex`
- `04_manuscript/sections/03c_interpretation_auto.tex`

Optional workflow outputs also include PROSPERO draft artifacts, citation tracking outputs, retraction checks, living-review scheduling artifacts, and keyword-network suggestions when those stages are enabled.

## What Guarantees It Tries To Give

SyReTo is built around operational guarantees rather than black-box convenience.

- Deterministic processing: core steps rely on explicit rules and thresholds rather than opaque ML classifiers.
- Replayability: generated artifacts can be rebuilt from repository state and scripted inputs.
- Auditability: key review decisions are persisted in versioned files and summarized in status artifacts.
- Integrity checks: dedicated guards validate the audit log, record identity stability, epistemic consistency, source integrity, and template leakage.
- Manuscript synchronization: review-state summaries are turned into manuscript-ready tables and sections instead of being maintained separately by hand.

These guarantees depend on keeping the repository inputs canonical and running the pipeline through the scripted entrypoints instead of ad hoc edits to downstream outputs.

## Install In 5 Minutes

Requires Python `>=3.11`.

```bash
git clone https://github.com/bearmishka/syreto.git
cd syreto
uv sync --all-groups
```

Install git hooks:

```bash
uv run pre-commit install
```

Sanity-check the environment:

```bash
uv run pytest -q
```

Current release status: the full repository test suite passes at `485 passed`.

## Fastest Way To Run It

If you want the packaged entrypoints first:

```bash
syreto list
syreto doctor
syreto status
syreto artifacts --kind operational
syreto validate all -- --fail-on error
syreto-draft --help
```

If you want the full operational pipeline:

```bash
cd 03_analysis
bash daily_run.sh
```

## How To Know A Run Succeeded

A successful run is not just “the script finished.” You should expect the following signals:

1. `daily_run.sh` exits with code `0`.
2. `outputs/status_summary.json` is present and current.
3. `outputs/status_report.md` is generated.
4. `outputs/todo_action_plan.md` is generated.
5. `syreto status` reports a clean or acceptable state for your configured fail threshold.
6. Expected manuscript tables are regenerated under `04_manuscript/tables/`.

The most useful quick checks are:

```bash
syreto doctor
syreto status
syreto artifacts --kind operational
cd 03_analysis && python status_cli.py --input outputs/status_summary.json
```

If you are using production mode, the status gate and template-term guard should also pass without blocker findings.

## Main Entry Points

- `syreto list`: list packaged analysis scripts
- `syreto run <script>`: run a packaged script by name
- `syreto path <script>`: resolve the filesystem path for a packaged script
- `syreto status`: run the packaged status CLI
- `syreto artifacts`: inspect key operational and manuscript-facing artifacts
- `syreto validate`: run packaged validation checks
- `syreto doctor`: run a quick repository readiness diagnostic
- `syreto-status`: print a concise operational summary from `status_summary.json`
- `syreto-draft`: run the PROSPERO draft generation entrypoint
- `03_analysis/daily_run.sh`: orchestrate the end-to-end review pipeline

## Project Shape

```text
syreto/                  installable Python package and packaged entrypoints
03_analysis/             analysis and orchestration scripts
02_data/                 canonical review inputs and processed CSVs
04_manuscript/           manuscript-facing generated artifacts
pyproject.toml           packaging, dependencies, tooling, test config
uv.lock                  locked dependency set
```

## Development

Run checks locally with:

```bash
uv run pytest -q
uv run pre-commit run --all-files
```

## Citation

If you use SyReTo in research, cite the software and pin the release you used.

```bibtex
@software{profatilova2026syreto,
  author       = {Profatilova, Evgeniya and Konstantinidis, Ilias},
  title        = {{SyReTo}: A Deterministic Toolkit for Reproducible Systematic Reviews},
  year         = {2026},
  url          = {https://github.com/bearmishka/syreto},
  version      = {0.2.0}
}
```

## License

MIT. See [LICENSE](LICENSE).

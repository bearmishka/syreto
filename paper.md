---
title: "SyReTo: an operating system for systematic reviews"
tags:
  - systematic review
  - evidence synthesis
  - PRISMA
  - reproducibility
  - Python
  - audit trail
authors:
  - name: Evgeniya Profatilova
    orcid: 0009-0007-6618-3662
    affiliation: 1
  - name: Ilias Konstantinidis
    orcid: 0009-0009-8628-641X
    affiliation: 2
affiliations:
  - name: "[Affiliation to be added before submission]"
    index: 1
  - name: "[Affiliation to be added before submission]"
    index: 2
date: 2026-04-09
bibliography: paper.bib
---

# Summary

SyReTo is a deterministic, git-native framework for conducting systematic reviews as reproducible computational processes rather than as loosely coordinated spreadsheets, reference-manager exports, and ad hoc scripts. It is designed as an operating system for systematic reviews: a single environment in which review state, validation, execution, status assessment, and manuscript-facing outputs are managed as one auditable process.

The software operationalizes the full review lifecycle around explicit files and scripted transformations. Canonical review state is held in version-controlled CSV and Markdown artifacts. Analysis and reporting steps are executed through Python and shell entrypoints built around the scientific Python stack, especially pandas [@reback2020pandas], and interoperable with the broader NumPy ecosystem [@harris2020array]. Operational validity is surfaced through validation steps, integrity guards, status gates, failure markers, and machine-readable run artifacts. Manuscript-facing outputs such as PRISMA tables, GRADE tables, result summaries, and narrative layers are generated from the same underlying review state rather than maintained separately by hand.

This design makes SyReTo suitable for teams that need transparent review operations, deterministic reruns, and a direct audit trail between inputs, analytical decisions, and manuscript artifacts. SyReTo also integrates with common evidence-synthesis exchange formats, particularly CSV and RIS, and can fit into reference-manager workflows such as Zotero or EndNote through standard RIS exports rather than bespoke platform bindings.

# Statement of need

Many systematic reviews are still conducted through a fragile mixture of spreadsheet files, reference-management tools, and one-off scripts. Search logs, screening decisions, extraction sheets, appraisal tables, and manuscript tables often drift apart operationally. The result is a process that is difficult to reproduce, difficult to audit, and difficult to trust after multiple rounds of revision.

This problem is not only computational but procedural. A review can appear complete while still being operationally inconsistent: counts may no longer match, placeholder language may remain in manuscript outputs, record identities may drift after deduplication, and downstream outputs may no longer reflect current review state. In such settings, reproducibility is weakened not only by missing code, but by a lack of explicit execution discipline.

SyReTo addresses this gap by treating the systematic review as a managed computational object. Instead of offering isolated utilities, it provides a coherent execution environment for PRISMA-aligned review workflows [@page2021prisma]. Its design goal is not merely to automate tasks, but to make the review process itself inspectable, reproducible, and operationally honest.

# What the software does

SyReTo manages a systematic review as a single process with explicit state transitions and operational checkpoints. In practical terms, the software:

- validates canonical review-state inputs
- consolidates screening state and review logs
- performs deduplication and state harmonization
- validates extraction and appraisal layers
- generates synthesis and reporting artifacts
- produces status summaries and action-oriented operational reports
- regenerates manuscript-facing outputs from the current review state
- exports and consumes review-relevant artifacts through plain-text and tabular formats that interoperate with common research toolchains

This makes SyReTo different from a script bundle or a notebook collection. The software is intended to manage process integrity as much as analysis. The primary output is not only a set of tables or figures, but a review state whose current validity can be assessed.

# Architecture

SyReTo is structured around a layered architecture with explicit separation between state, workflows, interfaces, and observability.

## Data sources and review state

Canonical review state is stored in version-controlled files, primarily under `02_data/`. These include search logs, screening logs, decision logs, PRISMA count templates, audit logs, record identity maps, and extraction templates. These files act as the source of truth for execution and are intended to be diffable and auditable in Git. This format choice is deliberate: SyReTo uses explicit, inspectable file contracts such as CSV, Markdown, LaTeX, and RIS rather than opaque project databases.

## Workflows

The review workflow is implemented as a PRISMA-aligned pipeline. Major stages include screening-state consolidation, input validation, integrity checks, extraction validation, appraisal, synthesis, reporting, and manuscript synchronization. The orchestration spine is currently centered on `daily_run.sh`, which executes these stages in a controlled order and enforces final checkpoint behavior.

## Managers and interfaces

Execution and operational control are exposed through thin interfaces rather than hidden control logic. The CLI provides commands such as `status`, `artifacts`, `validate`, and `doctor`, while the pipeline logic remains in Python and shell scripts. This preserves a strict separation of concerns: configuration and state remain external, execution logic remains scripted, and the interface layer selects and invokes rather than deciding epistemic behavior.

## Integration surface

SyReTo is designed to interoperate with the surrounding evidence-synthesis ecosystem rather than replace it wholesale. In its current form, this includes:

- `pandas`-centered tabular processing for review-state management [@reback2020pandas]
- compatibility with the broader NumPy/scientific-Python ecosystem through array-oriented numerical tooling [@harris2020array]
- CSV as the primary exchange format for review-state inputs and outputs
- RIS export pathways for bibliographic interoperability, including Zotero- and EndNote-compatible exports
- LaTeX- and BibTeX-adjacent manuscript workflows through generated `.tex` outputs and standard scholarly writing environments

This integration strategy favors explicit file exchange and inspectable transformations over opaque application coupling.

## Observability

SyReTo includes an explicit observability layer. Run state is surfaced through machine-readable artifacts such as `status_summary.json`, a markdown status report, failure markers, run manifests, and a step-level `run_events.jsonl` event stream. These artifacts are intended to explain what happened over time without controlling execution. In other words, observability records and explains execution; it does not govern it.

# Reproducibility and operational integrity

Reproducibility in SyReTo is defined more strictly than “the code can be rerun.” The system is designed so that, given identical inputs and configuration, it should produce identical outputs and identical status conclusions.

Several features support this claim:

- deterministic pipeline execution over explicit review-state files
- version-controlled canonical inputs
- validation of schema, values, and cross-file consistency
- integrity guards for audit logs, record identity stability, template leakage, source integrity, and epistemic consistency
- status gates that distinguish informational findings from blocking operational failures
- explicit failure markers and manifests for failed runs
- manuscript artifacts generated from review state rather than curated separately

Together these mechanisms allow SyReTo to express not just whether a review ran, but whether the run should be trusted. This makes operational integrity a first-class output of the system.

# Reuse potential

SyReTo is built to support review teams that need transparent, inspectable systematic review infrastructure rather than opaque workflow tooling. Its layered design also provides a basis for future review-instance parameterization through explicit configuration rather than hidden CLI defaults. This makes it suitable both as immediately usable software and as a reproducible foundation for domain-specific review pipelines.

# Acknowledgements

The authors acknowledge the broader open-source ecosystem that supports reproducible scientific software, including Python packaging, testing, and linting tools used in the SyReTo development workflow.

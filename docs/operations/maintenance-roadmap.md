# Maintenance and research-use roadmap

This roadmap separates repository maintenance from claims that require research
evidence. It does not change a protected review execution or declare the frozen
Arm A r1 boundary terminal. The existing passive-adoption boundary remains in force.

## 1. Restore trustworthy continuous integration

The first maintenance change addresses:

- Ruff import ordering and formatting in the Git handoff tooling;
- Git handoff fixtures that can deliberately track local-only files despite user ignore rules;
- full collection and execution of `syreto/tests` and `03_analysis/tests`;
- removal of obsolete exclusions for repository-data integration tests;
- locked dependency installation, including the correct SyReTo package version;
- consistent Ruff versions in the lockfile and pre-commit configuration;
- preservation of the vendor standard's pinned hash during whitespace checks;
- documentation that points to executable checks instead of stale test counts.

Acceptance requires the full pytest suite, repository pre-commit checks, a
reviewed diff, and successful GitHub CI for the proposed revision. Local checks
alone do not establish a successful GitHub run.

## 2. Establish the first real research use

Current status (2026-10-28): the maintainer reports that SyReTo is not yet used
in any research project. No research adoption or impact is claimed here.

The next research milestone is a prospective pilot on a maintainer-selected
question, with a written protocol and preserved researcher decisions. Agree on
the question, scope, reviewers, data access, and sharing permissions before
starting. Then document the actual workflow and its evidence:

- research question, responsible researchers, and current use of the software;
- code revision, configuration, environment, and input provenance;
- saved screening, extraction, and appraisal decisions;
- outputs and the research steps they support;
- observed benefits and limitations, with evidence for any comparative claims;
- what can be shared publicly and what requires restricted editorial access.

The existing synthetic golden and smoke fixtures establish regression behavior;
they do not by themselves establish adoption in research. A public reference
analysis should document the pilot once it has actually been conducted.
Reproduction must use preserved researcher decisions rather than imply that a RIS import alone
performs scientific screening or evidence appraisal.

## 3. Update the software paper

Revise `paper.md` around Summary, Statement of need, State of the field,
Software design, Research impact statement, and AI usage disclosure, with
acknowledgements and references. Reuse the existing architecture and
reproducibility contracts, including their limitations.

Author confirmation is needed for affiliations, research-use evidence, AI tools
and the scope of their use, human verification, funding, and conflicts of
interest. Do not replace missing facts with invented affiliations, adoption,
validation, or disclosure statements.

Check the current JOSS paper and submission requirements when preparing the
submission. A first commit's date does not establish when a repository became
public, sustained development, or editorial eligibility.

## 4. Prepare the next release

Review the accumulated Unreleased changes against the versioning policy before
choosing a version. Verify package metadata, citation metadata, changelog,
installation, tests, CI, and reproducible examples for that revision. Create the
versioned release and archival deposit only after those checks; record an actual
DOI once the archive exists.

## 5. Develop through use and feedback

Track concrete bugs, user difficulties, and proposed improvements in issues.
Use scoped branches and pull requests with reproducible validation. Prioritize
changes demonstrated by research use; there is no target issue or PR count.

Do not promise a JOSS submission date from this checklist alone. Research use,
sustained open development, and the current editorial requirements need their
own evidence.

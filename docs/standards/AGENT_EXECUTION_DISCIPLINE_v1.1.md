# Agent Execution Discipline v1.1

**Status:** Canonical internal standard  
**Date:** 2026-09-26  
**Scope:** All long-running or repeated agent-assisted engineering, research, analysis, and document workflows.

**Supersedes:** v1.0 (2026-09-20). Sections 14–19 specify the new normative requirements. Existing frozen protocols retain authority over their registered execution; adoption must not silently amend them.

**Change basis:** Four additions requested following the ScientistTwo discussion: Promotion Gates, Ablation Discipline, Experimental Rebuttal, Integrity Audit. These are our operational requirements; this document does not claim to reproduce or independently verify the external system.

## 1. Purpose

This standard defines how to structure work performed with coding/research agents so that model context is treated as scarce working memory, not durable storage; already established facts are not repeatedly rediscovered; deterministic work is executed by deterministic tools; long-running tasks survive context resets, quota limits, crashes, and agent replacement; evidence, failures, decisions, and provenance remain inspectable; and agents receive the smallest sufficient context for the current task.

This is not primarily a token-saving trick. It is an execution discipline for reliable long-horizon agent work.

## 2. Core architectural principle

> **Durable state outside the model; ephemeral reasoning inside the model.**

The model context is working memory. It MUST NOT be the only location containing current task state, completed work, architectural decisions, source bindings, test results, failure history, next actions, provenance, or approval state.

Anything required after a context reset must exist as a durable artefact.

## 3. Universal rules

### Rule 1 — Give the agent a map, not an encyclopedia

`AGENTS.md`, `CLAUDE.md`, or equivalent top-level agent instructions should be short and stable. Their primary job is to point to authoritative sources.

Preferred structure:

```text
AGENTS.md
ARCHITECTURE.md
docs/
  index.md
  design/
  product/
  operations/
  exec-plans/
  generated/
```

### Rule 2 — Repository/project knowledge is the system of record

Important knowledge discovered during agent work must be promoted out of chat into versioned artefacts: architecture decisions, interfaces, domain invariants, known limitations, workflow contracts, runbooks, reproducibility requirements, accepted research decisions, and current implementation plans.

Do not rely on old chat history, a particular agent session, human memory, or hidden model memory.

### Rule 3 — Separate semantic work from mechanical work

Use an LLM only where interpretation, synthesis, scientific judgement, design, or non-trivial code generation is needed.

Mechanical work belongs in scripts/tools, including SHA-256, manifests, git identity checks, replay, subprocess execution, file comparison, schema validation, test execution, environment creation, fixture copying, package integrity, status aggregation, CI gates, and static checks.

A reasoning model must not be used as an expensive replacement for `make`, `pytest`, `jq`, `git`, `sha256sum`, or a state machine.

### Rule 4 — Work source-centrically where possible

Prefer:

```text
inspect source cluster A once
  -> task A1
  -> task A2
  -> task A3
```

over repeatedly rediscovering the same repository for each task.

### Rule 5 — Generated context packs are content-addressed caches

For repeated work on a stable subsystem, generate compact context artefacts such as:

```text
SOURCE_CONTEXT_<id>.json
DOMAIN_CONTEXT_<id>.json
DATASET_CONTEXT_<id>.json
```

They should contain only reusable high-signal facts: relevant files, symbols/interfaces, commands, fixtures, dependencies, known constraints, source hashes, configuration identity, and known limitations.

### Rule 6 — Never trust a context cache without invalidation rules

Invalidate cached context when relevant identity changes: git HEAD/tree, relevant file hashes, dependency lockfile, schema/version, configuration, policy/version, input corpus, experiment protocol, environment image, or external snapshot identity.

If guards match, the agent should not reread the entire source “for reassurance”.

If additional evidence is genuinely needed, record:

```text
MISSING_EVIDENCE_REASON
EXTRA_SOURCE_PATHS_READ
```

### Rule 7 — Resume from state, not conversation history

Every long-running workflow should have a compact canonical state artefact such as `PIPELINE_STATE.json`.

Useful fields include workflow/version identity, current phase, completed/pending/failed tasks, active attempts, context hashes, artefact/handoff paths, next deterministic action, and budget/stop status.

Normal resume should be:

```text
read state
verify identity/hashes
continue next action
```

not repository-wide reconstruction.

### Rule 8 — Make handoffs small and structured

Detailed evidence belongs in files. A handoff should normally contain only task ID, status, key result, report/artefact path, hashes where relevant, warnings/limitations, and next action.

### Rule 9 — Keep stable prompt prefixes stable

Recommended order:

1. stable role/invariants;
2. stable tool/execution contract;
3. stable output schema;
4. dynamic task-specific suffix.

Do not rewrite the same role differently for every task.

### Rule 10 — Persistent agents are scoped caches, not immortal employees

Reuse an agent context when sequential tasks share a stable subsystem and no independence requirement exists.

Start fresh when the domain changes materially, context becomes noisy, independence/blinding is required, security/authority boundaries change, or source identity invalidates previous understanding.

### Rule 11 — Prefer reset + structured handoff over endless context growth

For long-running work:

```text
bounded agent session
 -> durable checkpoint
 -> compact handoff
 -> fresh/resumed context
```

is preferred over allowing an indefinitely growing conversation to become the project database.

### Rule 12 — Failed attempts are append-only evidence

Never rewrite history to make an agent run look cleaner. Preserve failed attempts, retries, reasons, terminal reports, environment problems, and protocol deviations.

### Rule 13 — Isolate parallel work

Parallel agents should have distinct worktrees/output namespaces, non-overlapping ownership, separate logs/reports, and an explicit merge/integration step.

### Rule 14 — Agents reason; deterministic gates decide mechanical facts

Tests, hashes, schemas, CI, and other deterministic checks should be evaluated by tools, not by model opinion.

### Rule 15 — Observability is part of the workflow

Where available, record agent/session ID, task ID, model, reasoning configuration, input/cached/output/reasoning tokens, tool calls, wall-clock time, source reads, retries, context resets/compactions, and terminal status.

Do not fabricate unavailable metrics.

## 4. Standard project layout

Reference layout:

```text
AGENTS.md
ARCHITECTURE.md

docs/
  index.md
  design/
  decisions/
  exec-plans/
    active/
    completed/
  runbooks/
  generated/

.agent/
  state/
    PIPELINE_STATE.json
  contexts/
    SOURCE_CONTEXT_*.json
  runs/
  handoffs/
  manifests/
  checkpoints/
  failures/
```

Normally commit durable project knowledge, design decisions, reusable runbooks, historically relevant execution plans, and schemas/contracts. Keep temporary stdout/stderr, runtime scratch files, disposable environments, and large intermediate outputs run-local unless evidence requires preservation. Never commit secrets.

## 5. Three execution levels

### Level 0 — Short task

Use for one bounded task touching one/few files with little recovery need. Use normal agent, git, and test discipline. Do not build a state machine for a tiny change.

### Level 1 — Multi-step task

Use when work spans several related steps/sessions or repeated source access. Add an execution plan, compact task state, structured handoff, deterministic validation, and source-centric batching where useful.

### Level 2 — Long-running / research / production agent workflow

Use for many tasks/candidates, hours/days of execution, multiple agents, experiments, expensive context, retries/recovery, or provenance-sensitive work.

Require durable state, content-addressed context packs, explicit invalidation, append-only history, deterministic execution substrate, source/domain clustering, isolated workers, budget/stop rules, observability, explicit authority boundaries, and recovery tests.

## 6. Anti-patterns

Avoid giant `AGENTS.md`, chat as database, repository reread on every task, LLM as workflow engine, infinite persistent sessions, blind cache reuse, excessive precomputed context, and premature framework construction.

## 7. Known trade-offs and failure modes

There is no completely free implementation.

### Stale context risk
Mitigate with hashes/version guards and automatic invalidation.

### Context entrenchment
A context pack can preserve a mistaken interpretation. Separate factual extracted context from interpretation; permit explicit missing-evidence escalation; periodically validate generated contexts.

### Persistent-session context rot
Reuse only within coherent clusters; bound sessions; reset with handoff.

### Lossy compaction
Treat summaries as navigation state, not evidence. Durable artefacts remain authoritative.

### Reduced independence
Use fresh agents for independent verification, blinded evaluation, or adversarial review.

### Instrumentation overhead
Use Level 0/1/2 proportionality and automate shared infrastructure.

### Over-optimization for token cost
Optimize for the smallest **sufficient** context, not the smallest possible context.

## 8. Default policy for our projects

This discipline becomes the default for any task expected to span multiple agent sessions, use multiple subagent calls, repeat over a candidate/item set, incur material context-reconstruction cost, or require scientific/legal provenance.

Small one-off work remains lightweight.

## 9. Project-specific rollout

### SyReTo
Passive adoption only while frozen Arm A r1 remains non-terminal. Record current `PIPELINE_STATE`, `SOURCE_CONTEXT`, runner and guards as prototype/reference implementation. Add the context-policy pointer and an explicit deferred migration record outside the frozen execution boundary. Do not move, refactor, regenerate or alter its execution substrate, prompts, dependencies, inputs, scheduling, guards or evaluation semantics. The deferred record must identify the protected baseline, scope, owner/next action, and activation condition: a recorded terminal checkpoint followed by a separate migration change. Inspect actual terminal evidence; a pause or this document is not a terminal checkpoint. Even documentation is not passive if consumed by the frozen run or covered by its identity: place it outside that boundary. Preserve all historical records and denominators.

### MNEMOSYNE / CONDE
Use Level 2 for ingestion/enrichment/analysis pipelines and Level 1 for ordinary feature work. Reuse the existing event-oriented history and Temporal rather than creating a second orchestration universe. Use deterministic workers for mechanical transformations and LLM workers only for semantic classification/synthesis. Version prompts, policies, sources, and model configuration.

### CPW
Use Level 2 with the research extension (informally “Level 2+”; not a fourth execution level) for experiments. Apply Sections 14–17. Every experiment should have immutable RunSpec, input/scenario identity, model/prompt identity, run record, output/evidence manifest, deterministic aggregation, bounded semantic adjudication, and checkpoint/resume.

### ASKA
Use Level 2 for corpus/audit/remediation batches and Level 1 for ordinary code changes. Version corpus/law snapshots, keep compact legal context, make schema/law_refs validation deterministic, use LLMs for legal-semantic judgement, and keep append-only remediation history.

### Kafene
Use Level 0/1 for bounded development and Level 2 for the living knowledge engine: monitor → diff → relevance → evidence → proposed patch → review → publish. Preserve source snapshots, effective/retrieval dates, claim-to-source bindings and freshness/invalidation rules. A changed or unavailable source triggers re-evaluation, not automatic publication. Publication requires the recorded review and authority required by the project.

### Leadgen / intelligence realization
Use Level 2 for batches: inexpensive deterministic filtering → semantic triage → promotion → expensive enrichment. Fix promotion criteria and budget before the batch. Measure total batch cost per promoted lead, yield and downstream eligibility; record zero-promoted batches without dividing by zero. Measure each enrichment layer with controlled comparisons. Promotion is qualification under the stated criteria, not evidence of conversion or revenue.

### Agent Experiment / research harnesses
Use Level 2 with the research extension. Compare persistent/fresh contexts, context packs, batching, compaction, promotion and reasoning configurations under versioned protocols and comparable budgets. Preserve all attempted runs; do not claim savings from token counts alone or only successful runs.

### Website / small bots / isolated maintenance
Usually Level 0 or Level 1. Do not add Level 2 machinery unless the work becomes genuinely long-running or repetitive.

## 10. Agent instruction baseline

Projects adopting this standard should include a short persistent instruction such as:

```text
Context discipline:
- Treat model context as working memory, not durable project state.
- Use repository/project artefacts as the source of truth.
- Do not broadly reread unchanged source when a valid hashed context artefact exists.
- Read additional source only for a specific missing-evidence reason.
- Delegate deterministic validation/execution to scripts and tests.
- Keep handoffs compact; detailed evidence belongs in files.
- Preserve failed attempts and retries.
- Resume from durable state/checkpoints.
- Reset context when it becomes noisy or crosses domain boundaries.
```

## 11. Adoption checklist

- classify normal work as Level 0 / 1 / 2;
- keep `AGENTS.md` / equivalent short and navigational;
- identify authoritative project knowledge;
- establish execution-plan location;
- add durable state only where multi-session work needs it;
- identify deterministic work currently performed by agents;
- move it into scripts/tests;
- define context-pack schema where repeated rereads occur;
- add hash/version invalidation;
- define handoff schema;
- define checkpoint/resume procedure;
- define when persistent agent reuse is allowed;
- define when fresh independent context is mandatory;
- add token/tool/time observability where exposed;
- test recovery from an interrupted run;
- periodically remove stale agent documentation;
- record promotion criteria, identity bindings and failure behavior (§14);
- for research claims, register ablations and falsifiable rebuttal tests (§§15–16);
- audit evidence integrity before accepting outcomes (§17);
- record applicability, actual validation evidence and deferred work (§§18–19).

## 12. Success criteria

The standard is working when a new agent session can resume from small durable state; agents do not reread large unchanged code/doc sets; deterministic work consumes negligible reasoning tokens; quota/crash resets do not lose work; failed attempts remain inspectable; independent reviewers can remain fresh where needed; project knowledge does not depend on chat history; and context size grows with the current task rather than total project age.

## 13. Governing maxim

> **Store knowledge durably, retrieve progressively, reason narrowly, execute deterministically, checkpoint explicitly.**

## 14. Promotion Gates

Expensive work, acceptance of evidence and publication are distinct transitions. A passed cheap filter does not authorize publication or establish a scientific claim.

For each material Level 2 transition, record before execution:

- gate ID/version, source and destination states, and candidate/run identity;
- required evidence and deterministic checks, plus explicit semantic-review criteria where needed;
- decision authority, budget ceiling and permitted next action;
- outcome: PASS, REJECT, HOLD (insufficient evidence), or ERROR (execution failure);
- evidence references/hashes, reviewer identity when applicable, timestamp and reason.

Only PASS permits the specified transition. Missing evidence, stale identity, unavailable review, tool failure or exhausted budget cannot default to PASS. REJECT is a substantive outcome; ERROR is not negative scientific evidence. A retry is a new attempt, linked to the previous one. A changed gate requires a new version; never silently reclassify historical decisions.

Gate records bind to the relevant input, corpus/scenario/source snapshot, code, configuration, policy, prompt and model identities. Identity changes invalidate affected eligibility until checks are rerun. Use relevant content hashes to avoid invalidating everything merely because an unrelated documentation commit changes HEAD; preserve the full originating commit for traceability.

Implement transitions in the existing deterministic substrate. Semantic workers produce structured proposals and reasons; the substrate validates record completeness and transition legality. Mechanical validation does not establish semantic truth. Human or delegated review authority remains explicit. Exceptions require a separate authorized deviation record; a worker cannot lower its own threshold.

## 15. Ablation Discipline

Required for research claims attributing benefits to a component; optional for ordinary bounded maintenance. Before confirmatory execution register the claim, baseline, component removed/replaced, expected diagnostic pattern, metrics, dataset/scenario split, seeds/replicates, budget and stopping rule, aggregation and uncertainty method, and permitted exclusions. Explain sample size and limits; a smoke test is not statistical validation.

Compare the full system against a meaningful baseline and a version differing in the intended factor. Hold other conditions fixed where feasible; record unavoidable confounders. Keep development/tuning evidence separate from confirmatory evidence. Inspecting a holdout and then tuning on it makes it development evidence; retain the original result and require new held-out evidence for confirmation.

Measure outcome quality alongside total cost, latency, retries and failures. Include orchestration, context construction and amortization assumptions. Report differences and uncertainty, not just the best run. Record missing telemetry as unavailable, not zero. Retain negative, inconclusive and failed runs with distinct statuses.

Ablation failure can show fragility or necessity within the tested system; it does not by itself identify a unique mechanism, prove sufficiency, or establish generality. Narrow claims accordingly. An exploratory ablation after seeing results is labelled exploratory and cannot retroactively become preregistered.

## 16. Experimental Rebuttal

For material empirical claims, turn the strongest credible objection into a discriminating check rather than a prose defence.

Maintain a compact claim/objection register with:

- claim ID and current scope, supporting evidence and competing explanation;
- a test capable of separating the explanations, expected outcomes and decision rule;
- protocol and artifact identities, budget/stop rule and execution status;
- observed result, evidence references, disposition and resulting claim boundary.

Use a fresh reviewer context where independence or blinding matters. Do not expose hidden labels or author conclusions to a blinded reviewer. A fresh context is a procedural safeguard, not proof of independence; disclose shared model, tooling or source limitations. Never describe self-review as independent review.

Run the smallest adequate deterministic or controlled empirical check. Possible dispositions include supported within tested scope, narrowed, refuted, inconclusive, and blocked. If no affordable or valid test exists, retain the objection as unresolved and constrain the claim. Do not invent an experiment or interpret a persuasive model response as experimental evidence. Do not iterate until a preferred outcome while hiding earlier attempts.

## 17. Integrity Audit

Required before Level 2 result acceptance/release and at the terminal checkpoint of a research run; scope it to the actual artifacts and claims. Separate integrity of the record from scientific validity of its conclusions.

Audit at least:

1. Identity: baseline commit/tree, actual executed code (including any dirty diff), dependency/environment identity, RunSpec, input/source snapshots, prompt/policy/model configuration and relevant hashes.
2. Completeness: reconcile selected items with pending, active and terminal states; reconcile retries through attempt IDs. Not executed, rejected, infrastructure failure and substantive negative result remain distinct. Record denominator changes explicitly.
3. Provenance: every reported metric/claim has a traceable raw artifact and aggregation path; generated narrative or cached interpretation is not a substitute for evidence.
4. Reproduction: rerun deterministic checks/aggregation against preserved inputs, or state precisely why blocked. Replay of recorded model outputs is distinct from a new stochastic model call. A configured seed does not promise deterministic provider behavior.
5. Leakage/deviations: inspect split boundaries, hidden-label access, post-hoc tuning, exclusions, changed thresholds, protocol deviations and review authority.
6. Recovery: verify checkpoint consistency, incomplete attempt handling and safe continuation, including external side effects where relevant.

Report PASS, FAIL or BLOCKED with scope, commands/check versions, exit statuses, evidence paths and unresolved findings. A hash match establishes identity, not correctness. A documentation-only adoption cannot claim runtime audit PASS. Critical integrity failures block acceptance; remediation appends new evidence and a new audit result, preserving the original failure.

Bind audit evidence to the substantive revision and artifact manifest. A subsequent checkpoint-only commit does not automatically require rerunning all checks: inspect its diff and prove that it changes no audited inputs or execution semantics. Changes to audited material require the affected checks again. Never weaken a check just to eliminate a stale-status warning.

## 18. Recovery and adoption acceptance

Reuse existing state stores, event logs, Temporal workflows, manifests and runners. The reference paths in §4 are illustrative; do not create a competing source of truth or a second scheduler. A schema or runbook alone is not an implemented gate or recovery mechanism.

Use immutable completed artifacts with atomic state/checkpoint updates or an equivalent transactional mechanism. Track a logical task separately from its attempts. Define handling of abandoned in-progress attempts, concurrent writers, partial outputs and stale checkpoints. External writes require idempotency keys or reconciliation; a local state file cannot guarantee exactly-once remote effects. Never replay a real publication, notification, payment or paid model batch as a smoke test.

A recovery smoke test must use a disposable fixture, interrupt at a meaningful boundary, start a fresh process from durable state, verify identity, resume the next legal action and compare with an uninterrupted reference. Check preservation of failed/partial attempts and absence of duplicate accepted outputs/side effects. Also verify that changed relevant identity blocks stale resume and that missing evidence blocks promotion. Use mocks/fakes for remote and paid effects; report this limitation.

Per repository, record:

- normal-task level and workflow-specific exceptions;
- authoritative standard path/version/content hash and existing substrate reused;
- implemented capabilities versus documented, deferred, inapplicable or blocked ones;
- validation commands/results and artifact paths;
- protected experiment boundaries and explicit deferred migration triggers;
- actual commit identity and next action.

Adoption must not create an unrequested research campaign, consume paid inference budgets or promote a prototype to production. Where execution is not yet implemented, provide the minimal coherent contract/scaffold and state that runtime acceptance remains pending. Never fill gaps with fabricated results.

## 19. Rollout order and completion discipline

Canonical specification v1.1 first, then:

1. CPW — reference adoption of Level 2 plus research extension.
2. ASKA / GOLD — corpus/law-snapshot identity, batch validation and semantic remediation review.
3. MNEMOSYNE / CONDE — discipline on top of existing Temporal/event/provenance infrastructure.
4. Kafene — lightweight development; full update discipline for the living knowledge workflow.
5. Leadgen / intelligence realization — staged cost control and enrichment evaluation.
6. Agent Experiment / other research harnesses — controlled evaluation of the discipline itself.
7. SyReTo — passive adoption only under the frozen-boundary rule in §9.
8. Remaining small repositories — proportional Level 0/1 adoption.

For each: audit → classify → implement minimum sufficient discipline → applicable recovery/validation checks → inspect diff → commit → next repository. Preserve unrelated user work. Do not move to the next repository while presenting unfinished adoption as complete. A real access, scientific or execution blocker must be recorded and reported, not disguised as a passed gate.

Keep one authoritative standard. Repository copies are pinned vendor copies with upstream identity and content hash; local applicability belongs in a separate short adoption record. Do not silently fork the normative text. Update short agent entrypoints to the pinned version and remove conflicting active pointers while retaining historical documents as history.

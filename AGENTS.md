# Agent entrypoint

Read `README.md` for SyReTo development and reproducibility rules.

Agent execution discipline:
- pinned standard: `docs/standards/AGENT_EXECUTION_DISCIPLINE_v1.1.md`;
- SyReTo adoption boundary: `docs/operations/agent_execution_discipline_adoption.md`.

The current execution-discipline posture is passive. Do not modify review
execution, prompts, dependencies, inputs, scheduling, guards, evaluation
semantics, or artifact contracts merely to satisfy the shared standard.

For authorized coding or documentation work, run
`python3 scripts/task_sync.py start` before substantive edits. After required
checks and an explicit scoped commit, run `python3 scripts/task_sync.py finish`.
Explicit user no-commit/no-push instructions take precedence.

Use one writing agent per worktree; parallel writing tasks require separate
worktrees/branches. Git handoff details: `docs/operations/git-handoff.md`.

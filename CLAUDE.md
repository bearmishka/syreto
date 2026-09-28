# Claude entrypoint

Follow `AGENTS.md` as the repository agent entrypoint.

Pinned execution standard:
`docs/standards/AGENT_EXECUTION_DISCIPLINE_v1.1.md`.
SyReTo applicability:
`docs/operations/agent_execution_discipline_adoption.md`.

The current execution-discipline posture is passive; do not retrofit runtime
machinery into protected review execution merely because the standard exists.

For authorized edits, use `python3 scripts/task_sync.py start` before work and
`python3 scripts/task_sync.py finish` after required checks and an explicit
scoped commit. See `README.md` and `docs/operations/git-handoff.md`.

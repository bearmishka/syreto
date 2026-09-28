# Agent entrypoint

Read `README.md` for SyReTo development and reproducibility rules.

For authorized coding or documentation work, run
`python3 scripts/task_sync.py start` before substantive edits.

After the repository's required checks and an explicit scoped commit, run
`python3 scripts/task_sync.py finish`. Explicit user no-commit/no-push
instructions take precedence; report LOCAL_ONLY / HANDOFF_INCOMPLETE instead.

Use one writing agent per worktree; parallel writing tasks require separate
worktrees/branches. The Git handoff procedure does not replace SyReTo validation,
review execution, canonical inputs, generated-artifact contracts, release pins,
or reproducibility requirements.

Detailed Git handoff behavior and recovery:
`docs/operations/git-handoff.md`.

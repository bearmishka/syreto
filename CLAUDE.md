# Claude entrypoint

Follow `AGENTS.md` as the repository agent entrypoint.

Before substantive authorized coding or documentation edits, run
`python3 scripts/task_sync.py start`. After required project checks and an
explicit scoped commit, run `python3 scripts/task_sync.py finish`.

Do not use Git handoff to bypass SyReTo validation or reproducibility rules.
See `README.md` and `docs/operations/git-handoff.md`.

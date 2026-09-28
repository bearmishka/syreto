# Git task handoff

Version 1.0 — 2026-09-28. Applies to normal contributor development sessions.
This procedure governs Git synchronization and publication only. It does not
change SyReTo review execution, canonical review inputs, generated-artifact
contracts, release pins, or reproducibility semantics.

## Commands

From the repository root (Python 3.9+ and Git required, no extra dependencies):

- `python3 scripts/task_sync.py start` — task-start: check identity, upstream,
  operation state and worktree; fetch; fast-forward only if solely behind.
- `python3 scripts/task_sync.py finish` — task-finish: after review, required
  project tests and an explicit scoped commit, publish to the configured
  same-name upstream branch and verify its SHA remotely.
- `python3 scripts/task_sync.py finish --no-push` — honor an explicit user
  no-publication instruction; exit 2 and report LOCAL_ONLY, never COMPLETE.
- `python3 tests/test_task_sync.py` — integration tests using disposable local
  repositories and a bare remote; no GitHub/API/model calls.

Read AGENTS.md/CLAUDE.md and run required project preflight checks too. Commit only
reviewed task files explicitly; neither command stages, commits or runs product
tests. Keep the final handoff outside the committed tree to avoid an infinite
"commit the SHA, then commit the new SHA" cycle.

## States and recovery

READY means start synchronized to the fetched upstream commit. COMPLETE means
finish verified identical local and remote heads and no unexpected worktree
changes at verification time. This is a Git handoff result, not a test result,
scientific acceptance or merge approval. Another writer can change the remote
later; begin every session with start.

Ahead-only start is LOCAL_ONLY/BLOCKED: finish the previous handoff first.
Divergence blocks both commands; inspect both histories and reconcile as a
separate explicit action. Finish does not silently integrate remote changes.
Network/push failure leaves local commits available, reports HANDOFF_INCOMPLETE
and exits nonzero. Retry finish after resolving the concrete cause. If start
fast-forwarded but a later check failed, inspect HEAD; never assume rollback.

Configure the intended upstream once, explicitly. Missing upstream, detached HEAD,
cross-name upstreams and unapproved remote URLs block. A new branch needs a reviewed
initial push/upstream setup before these commands can manage it. Do not point a
feature branch at main as its publication destination.

Policy is `scripts/task_sync.json`: approved remote name/URLs, optional working
branch restriction and exact untracked-file exceptions. Exceptions never authorize
reading, staging or deleting those files; if an exception becomes tracked locally
or remotely, review is required. Ordinary ignored files retain Git's normal
behavior; these tools are not a backup of ignored/untracked data.

One writing agent per worktree. Parallel tasks require separate worktrees/branches.
The per-worktree command lock only serializes these short commands, not whole agent
sessions or all Git clients. After a crash, inspect active processes and Git state
before manually removing a stale task-sync.lock. No automatic lock deletion.

## Authority and limitations

Default for authorized coding tasks: checks → scoped commit → finish/push to the
working branch. An explicit instruction not to commit/publish overrides this default;
report incomplete publication honestly. Protected branches use the repository's
existing review/PR process. Never bypass protection, force-push, reset, stash,
clean, auto-rebase, or silently merge another branch. Do not weaken frozen protocols.

A handoff lists repository, branch, commit SHA, checks, publication result and next
action. Never say "done" while hiding unpublished commits. These are executable
checks plus agent instructions, not an unbypassable security boundary. Local hooks
are deliberately not installed or replaced; a repository update cannot install
hooks in another user's clone. Git hooks/configuration can themselves have effects;
normal Git hooks still run on fetch/merge/push.

## Initial validation

The stdlib integration suite covers synchronized start/finish, fast-forward,
ahead-only publication, divergence preservation, dirty state, exact local exceptions,
remote collisions, tracked exceptions, rejected push, unavailable remote, wrong
push URL, missing upstream, detached HEAD, unfinished operations, explicit no-push,
linked worktrees and command locking. Product/runtime sources and pinned standards
are outside this change.

#!/usr/bin/env python3
"""Git handoff guard; stdlib only. See docs/operations/git-handoff.md."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


class Stop(Exception):
    pass


def git(*args: str, optional: bool = False) -> str:
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0")
    try:
        result = subprocess.run(["git", "-C", str(ROOT), *args], capture_output=True,
                                text=True, env=env, timeout=120)
    except (OSError, subprocess.TimeoutExpired):
        raise Stop("Git unavailable or timed out; inspect locally. No success recorded.") from None
    if result.returncode and not optional:
        # Raw stderr/command arguments may contain credentials in configured URLs.
        raise Stop(f"Git operation {args[0]} failed (exit {result.returncode}); inspect locally.")
    return result.stdout.rstrip("\n") if result.returncode == 0 else ""


def config_value(key: str) -> str:
    return git("config", "--get", key, optional=True)


def clean(policy: dict) -> None:
    allowed = set(policy.get("allowed_untracked", []))
    for path in allowed:
        if git("ls-files", "--", path):
            raise Stop("A local-only exception is now tracked; review policy.")
    entries = git("status", "--porcelain=v1", "-z", "--untracked-files=all").split("\0")
    for entry in entries:
        if not entry:
            continue
        if entry[:3] == "?? " and entry[3:] in allowed:
            continue
        raise Stop("Uncommitted changes or unexpected untracked files; preserve and review locally.")


def no_operation() -> None:
    for name in ("MERGE_HEAD", "CHERRY_PICK_HEAD", "REVERT_HEAD", "rebase-merge", "rebase-apply", "sequencer", "BISECT_START", "index.lock"):
        path = Path(git("rev-parse", "--git-path", name))
        if not path.is_absolute():
            path = ROOT / path
        if path.exists():
            raise Stop("An unfinished Git operation or index lock exists.")


def run(action: str, no_push: bool = False) -> dict:
    if Path(git("rev-parse", "--show-toplevel")).resolve() != ROOT:
        raise Stop("Script directory is not this repository root.")
    policy = json.loads((ROOT / "scripts/task_sync.json").read_text())
    branch = git("symbolic-ref", "--quiet", "--short", "HEAD", optional=True)
    if not branch:
        raise Stop("Detached HEAD is not a task branch.")
    if policy.get("allowed_branches") and branch not in policy["allowed_branches"]:
        raise Stop("This branch is outside the configured working boundary.")
    remote = config_value(f"branch.{branch}.remote")
    target = config_value(f"branch.{branch}.merge")
    if not remote or remote == "." or target != f"refs/heads/{branch}":
        raise Stop("Configure an explicit same-name remote upstream before proceeding.")
    if remote != policy["remote"]:
        raise Stop("Upstream remote differs from the approved remote.")
    expected = policy["remote_urls"]
    fetch_urls = git("remote", "get-url", "--all", remote).splitlines()
    push_urls = git("remote", "get-url", "--push", "--all", remote).splitlines()
    if len(fetch_urls) != 1 or len(push_urls) != 1 or any(u not in expected for u in fetch_urls + push_urls):
        raise Stop("Remote fetch/push URL does not match the approved repository.")
    no_operation()
    clean(policy)
    # A local per-worktree command lock. It is not a whole-session writer lock.
    lock = Path(git("rev-parse", "--git-path", "task-sync.lock"))
    if not lock.is_absolute():
        lock = ROOT / lock
    try:
        lock.mkdir()
    except FileExistsError:
        raise Stop("Another handoff command is active, or its lock requires manual recovery.") from None
    try:
        head = git("rev-parse", "HEAD")
        if no_push:
            return {"status": "LOCAL_ONLY", "branch": branch, "sha": head,
                    "reason": "Explicit no-publish instruction; handoff is incomplete."}
        git("fetch", "--no-tags", remote, target)
        fetched = git("rev-parse", "FETCH_HEAD")
        for path in policy.get("allowed_untracked", []):
            if git("ls-tree", "-r", "--name-only", fetched, "--", path):
                raise Stop("Remote tracks a local-only exception; review before updating.")
        ahead, behind = map(int, git("rev-list", "--left-right", "--count", f"{head}...{fetched}").split())
        if git("rev-parse", "HEAD") != head:
            raise Stop("HEAD changed during synchronization; another writer may be active.")
        clean(policy)
        if ahead and behind:
            raise Stop(f"DIVERGED: ahead {ahead}, behind {behind}; explicit reconciliation required.")
        if action == "start":
            if ahead:
                raise Stop(f"LOCAL_ONLY: {ahead} unpublished commits; finish previous handoff first.")
            if behind:
                git("merge", "--ff-only", fetched)
            clean(policy)
            if git("rev-parse", "HEAD") != fetched:
                raise Stop("HEAD changed unexpectedly after start synchronization.")
            return {"status": "READY", "branch": branch, "sha": fetched, "advanced": behind}
        if behind:
            raise Stop(f"REMOTE_AHEAD: behind {behind}; integrate and validate explicitly before finish.")
        # Explicit SHA and destination; ignore broad/default push refspecs and mirror mode.
        git("-c", f"remote.{remote}.mirror=false", "-c", "push.followTags=false",
            "push", "--no-follow-tags", "--recurse-submodules=no", remote, f"{head}:{target}")
        published = git("ls-remote", "--exit-code", remote, target).splitlines()
        if len(published) != 1 or published[0].split() != [head, target]:
            raise Stop("Remote SHA verification failed; handoff remains incomplete.")
        no_operation()
        clean(policy)
        if git("rev-parse", "HEAD") != head:
            raise Stop("Local HEAD changed during publication; handoff remains incomplete.")
        return {"status": "COMPLETE", "branch": branch, "sha": head, "published_commits": ahead}
    finally:
        lock.rmdir()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "finish"))
    parser.add_argument("--no-push", action="store_true", help="Honor an explicit no-publish instruction")
    args = parser.parse_args()
    if args.no_push and args.action != "finish":
        parser.error("--no-push applies only to finish")
    try:
        result = run(args.action, args.no_push)
        print(json.dumps(result))
        return 0 if result["status"] in ("READY", "COMPLETE") else 2
    except (Stop, ValueError, KeyError, OSError) as exc:
        # OSError text can contain configured paths; do not dump raw exception data.
        reason = str(exc) if isinstance(exc, Stop) else "Policy or local environment error; inspect locally."
        print(json.dumps({"status": "HANDOFF_INCOMPLETE" if args.action == "finish" else "BLOCKED", "reason": reason}))
        return 1


if __name__ == "__main__":
    sys.exit(main())

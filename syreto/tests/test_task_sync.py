"""Integration tests against disposable bare remotes; no network or application imports."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[2] / 'scripts/task_sync.py'


class HandoffTests(unittest.TestCase):
    def git(self, root, *args):
        p = subprocess.run(['git', '-C', str(root), *args], capture_output=True, text=True)
        self.assertEqual(p.returncode, 0, p.stderr)
        return p.stdout.strip()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.remote = self.base / 'remote.git'
        self.remote.mkdir()
        self.git(self.remote, 'init', '--bare', '--initial-branch=main')
        self.repo = self.base / 'local'
        self.repo.mkdir()
        self.git(self.repo, 'init', '--initial-branch=main')
        self.identity(self.repo)
        (self.repo / 'scripts').mkdir()
        shutil.copyfile(SCRIPT, self.repo / 'scripts/task_sync.py')
        self.policy = {'remote': 'origin', 'remote_urls': [str(self.remote)],
                       'allowed_untracked': ['.claude/settings.local.json', '.claude/launch.json']}
        (self.repo / 'scripts/task_sync.json').write_text(json.dumps(self.policy))
        self.git(self.repo, 'add', 'scripts')
        self.git(self.repo, 'commit', '-m', 'initial')
        self.git(self.repo, 'remote', 'add', 'origin', str(self.remote))
        self.git(self.repo, 'push', '-u', 'origin', 'main')
        self.peer = self.base / 'peer'
        self.git(self.base, 'clone', str(self.remote), str(self.peer))
        self.identity(self.peer)
        self.original = self.git(self.repo, 'rev-parse', 'HEAD')

    def identity(self, root):
        self.git(root, 'config', 'user.name', 'Fixture')
        self.git(root, 'config', 'user.email', 'fixture@example.invalid')

    def commit(self, root, name):
        (root / name).write_text(name)
        self.git(root, 'add', name)
        self.git(root, 'commit', '-m', name)
        return self.git(root, 'rev-parse', 'HEAD')

    def command(self, action, ok=True, *extra):
        p = subprocess.run([sys.executable, str(self.repo / 'scripts/task_sync.py'), action, *extra],
                           cwd=self.repo, capture_output=True, text=True)
        if ok:
            self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        else:
            self.assertNotEqual(p.returncode, 0)
        return json.loads(p.stdout)

    def test_synced_start_and_finish(self):
        self.assertEqual(self.command('start')['status'], 'READY')
        self.assertEqual(self.command('finish')['status'], 'COMPLETE')

    def test_behind_start_fast_forwards(self):
        sha = self.commit(self.peer, 'remote-change')
        self.git(self.peer, 'push')
        self.command('start')
        self.assertEqual(self.git(self.repo, 'rev-parse', 'HEAD'), sha)

    def test_ahead_start_blocks_finish_publishes(self):
        sha = self.commit(self.repo, 'local-change')
        self.command('start', False)
        self.command('finish')
        self.assertEqual(self.git(self.remote, 'rev-parse', 'main'), sha)

    def test_divergence_preserves_both_heads(self):
        local = self.commit(self.repo, 'local-change')
        remote = self.commit(self.peer, 'remote-change')
        self.git(self.peer, 'push')
        self.command('start', False)
        self.command('finish', False)
        self.assertEqual(self.git(self.repo, 'rev-parse', 'HEAD'), local)
        self.assertEqual(self.git(self.remote, 'rev-parse', 'main'), remote)

    def test_finish_behind_does_not_merge(self):
        self.commit(self.peer, 'remote-change')
        self.git(self.peer, 'push')
        self.command('finish', False)
        self.assertEqual(self.git(self.repo, 'rev-parse', 'HEAD'), self.original)

    def test_dirty_tracked_file_is_preserved(self):
        path = self.repo / 'scripts/task_sync.json'
        path.write_text(path.read_text() + '\n')
        self.command('start', False)
        self.command('finish', False)
        self.assertTrue(path.read_text().endswith('\n'))

    def test_exact_local_exceptions_survive(self):
        (self.repo / '.claude').mkdir()
        path = self.repo / '.claude/settings.local.json'
        path.write_text('LOCAL_SECRET_SENTINEL')
        self.assertNotIn('LOCAL_SECRET_SENTINEL', str(self.command('start')))
        self.command('finish')
        self.assertEqual(path.read_text(), 'LOCAL_SECRET_SENTINEL')
        (self.repo / '.claude/unexpected.json').write_text('x')
        self.command('start', False)

    def test_remote_tracking_local_exception_blocks_update(self):
        (self.repo / '.claude').mkdir()
        (self.repo / '.claude/settings.local.json').write_text('local')
        (self.peer / '.claude').mkdir()
        self.commit(self.peer, '.claude/settings.local.json')
        self.git(self.peer, 'push')
        self.command('start', False)
        self.assertEqual(self.git(self.repo, 'rev-parse', 'HEAD'), self.original)
        self.assertEqual((self.repo / '.claude/settings.local.json').read_text(), 'local')

    def test_tracked_exception_blocks(self):
        (self.repo / '.claude').mkdir()
        self.commit(self.repo, '.claude/settings.local.json')
        self.command('finish', False)

    def test_push_rejection_keeps_local_commit(self):
        hook = self.remote / 'hooks/pre-receive'
        hook.write_text('#!/bin/sh\nexit 1\n')
        hook.chmod(0o755)
        sha = self.commit(self.repo, 'local-change')
        self.command('finish', False)
        self.assertEqual(self.git(self.repo, 'rev-parse', 'HEAD'), sha)
        self.assertEqual(self.git(self.remote, 'rev-parse', 'main'), self.original)

    def test_network_failure_is_not_success(self):
        self.remote.rename(self.base / 'unavailable')
        self.command('start', False)
        self.command('finish', False)

    def test_wrong_push_url_blocks(self):
        self.git(self.repo, 'remote', 'set-url', '--push', 'origin', 'https://user:SECRET@example.invalid/repo')
        result = self.command('finish', False)
        self.assertNotIn('SECRET', str(result))

    def test_no_upstream_and_detached_head_block(self):
        self.git(self.repo, 'branch', '--unset-upstream')
        self.command('start', False)
        self.git(self.repo, 'checkout', '--detach')
        self.command('finish', False)

    def test_unfinished_operation_blocks(self):
        path = self.repo / '.git/MERGE_HEAD'
        path.write_text(self.original + '\n')
        self.command('start', False)
        self.assertTrue(path.exists())

    def test_explicit_no_push_is_local_only(self):
        sha = self.commit(self.repo, 'local-change')
        result = self.command('finish', False, '--no-push')
        self.assertEqual(result['status'], 'LOCAL_ONLY')
        self.assertEqual(self.git(self.remote, 'rev-parse', 'main'), self.original)
        self.assertEqual(self.git(self.repo, 'rev-parse', 'HEAD'), sha)

    def test_linked_worktree(self):
        linked = self.base / 'linked'
        self.git(self.repo, 'worktree', 'add', '-b', 'topic', str(linked))
        self.git(linked, 'push', '-u', 'origin', 'topic')
        self.repo = linked
        self.command('start')
        self.command('finish')

    def test_command_lock_blocks(self):
        (self.repo / '.git/task-sync.lock').mkdir()
        self.command('start', False)


if __name__ == '__main__':
    unittest.main()

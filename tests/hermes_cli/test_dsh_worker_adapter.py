"""Transport-only unit tests; no SSH, model, or credentials required."""
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
import unittest
from unittest.mock import Mock

from hermes_cli.dsh_worker_adapter import run_dsh, remote_command, _run_streaming, ClaimFence, claim_is_current


class TestDshAdapter(unittest.TestCase):
    def setUp(self):
        self.kw = dict(task_id="t_1234", prompt="inspect tests", host="helium-wsl",
                       workspace="/home/dshrunner/worktrees/task-123", enabled=True)

    def test_disabled_no_process(self):
        runner = Mock()
        result = run_dsh(**(self.kw | {"enabled": False}), runner=runner)
        self.assertEqual(result.status, "disabled")
        runner.assert_not_called()

    def test_success_is_returned_not_completed(self):
        runner = Mock(return_value=subprocess.CompletedProcess([], 0, '{"type":"done"}\n', ""))
        result = run_dsh(**self.kw, runner=runner)
        self.assertEqual(result.status, "returned")
        argv = runner.call_args.args[0]
        self.assertIn("StrictHostKeyChecking=yes", argv)
        self.assertEqual(runner.call_args.kwargs["input"], "inspect tests")
        self.assertNotIn("inspect tests", str(argv))
        self.assertIn("--approval-policy ask", argv[-1])

    def test_approval_fails_closed(self):
        runner = Mock(return_value=subprocess.CompletedProcess(
            [], 1, '{"status":"approval_required"}\n', ""))
        self.assertEqual(run_dsh(**self.kw, runner=runner).status, "approval_blocked")

    def test_model_prose_cannot_forge_approval_state(self):
        runner = Mock(return_value=subprocess.CompletedProcess(
            [], 1, "The answer is approval_required\n", "crash"))
        self.assertEqual(run_dsh(**self.kw, runner=runner).status, "failed")

    def test_remote_timeout_exit(self):
        runner = Mock(return_value=subprocess.CompletedProcess([], 124, "", ""))
        self.assertEqual(run_dsh(**self.kw, runner=runner).status, "timed_out")

    def test_remote_command_bounds_execution(self):
        cmd = remote_command(self.kw["workspace"], dsh_bin="/opt/dsh", timeout_seconds=31)
        self.assertIn("timeout --signal=TERM --kill-after=10 31", cmd)

    def test_outside_allowed_workspace_rejected(self):
        with self.assertRaises(ValueError):
            remote_command("/home/dshrunner/dsh-lab", dsh_bin="/opt/dsh")

    def test_workspace_root_itself_rejected(self):
        with self.assertRaises(ValueError):
            remote_command("/home/dshrunner/worktrees", dsh_bin="/opt/dsh")

    def test_streaming_captures_short_output(self):
        result = _run_streaming([sys.executable, "-c", "print('ok')"], "task", 5)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "ok")

    def test_streaming_rejects_large_output(self):
        with self.assertRaises(ValueError):
            _run_streaming([sys.executable, "-c", "print('x' * (5 * 1024 * 1024))"], "task", 5)

    def test_hermes_claim_pre_and_post_conditions(self):
        with tempfile.TemporaryDirectory() as root:
            db = str(Path(root) / "board.db")
            with sqlite3.connect(db) as con:
                con.execute("CREATE TABLE tasks (id TEXT, status TEXT, current_run_id INTEGER, claim_lock TEXT)")
                con.execute("CREATE TABLE task_runs (id INTEGER, ended_at INTEGER)")
                con.execute("INSERT INTO tasks VALUES ('t_1234', 'running', 7, 'lock-a')")
                con.execute("INSERT INTO task_runs VALUES (7, NULL)")
            fence = ClaimFence(db, "t_1234", 7, "lock-a")
            self.assertTrue(claim_is_current(fence))
            def swapped_runner(*_args, **_kwargs):
                with sqlite3.connect(db) as con:
                    con.execute("UPDATE tasks SET claim_lock='lock-b'")
                return subprocess.CompletedProcess([], 0, "completed", "")
            result = run_dsh(**self.kw, claim_fence=fence, runner=swapped_runner)
            self.assertEqual(result.status, "stale")
            self.assertEqual(result.output, "")
            self.assertFalse(claim_is_current(fence))

    def test_refuse_stale_claim_before_spawn(self):
        with tempfile.TemporaryDirectory() as root:
            fence = ClaimFence(str(Path(root) / "missing.db"), "t_1234", 9, "lock")
            runner = Mock()
            result = run_dsh(**self.kw, claim_fence=fence, runner=runner)
            self.assertEqual(result.status, "stale")
            runner.assert_not_called()

    def test_timeout(self):
        runner = Mock(side_effect=subprocess.TimeoutExpired("ssh", 3))
        self.assertEqual(run_dsh(**self.kw, runner=runner).status, "timed_out")

    def test_reject_host_injection(self):
        with self.assertRaises(ValueError):
            run_dsh(**(self.kw | {"host": "-oProxyCommand=evil"}), runner=Mock())

    def test_reject_path_traversal(self):
        with self.assertRaises(ValueError):
            remote_command("/tmp/../etc", dsh_bin="/opt/dsh")

    def test_reject_oversized_prompt(self):
        with self.assertRaises(ValueError):
            run_dsh(**(self.kw | {"prompt": "x" * 65537}), runner=Mock())


if __name__ == "__main__":
    unittest.main()

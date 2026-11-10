"""Verify delegation is fenced to a dispatcher-owned Hermes Kanban run."""
import unittest
from unittest.mock import Mock
from types import SimpleNamespace
from integrations.dsh_kanban_bridge import dispatch_from_owned_worker, register

class TestDshBridge(unittest.TestCase):
    def setUp(self):
        self.env = dict(HERMES_DSH_WORKER_ENABLED="1", HERMES_KANBAN_TASK="t_1234",
            HERMES_KANBAN_DB="/mnt/user/hermes/kanban.db", HERMES_KANBAN_RUN_ID="17",
            HERMES_KANBAN_CLAIM_LOCK="secret-claim-marker")
    def test_opt_in_required(self):
        mock = Mock()
        self.env["HERMES_DSH_WORKER_ENABLED"] = "0"
        self.assertEqual(dispatch_from_owned_worker("inspect", "t_1234", environ=self.env, runner=mock)["status"], "disabled")
        mock.assert_not_called()
    def test_unowned_fails_closed(self):
        mock=Mock()
        self.assertEqual(dispatch_from_owned_worker("inspect", "", environ=self.env, runner=mock)["status"], "disabled")
        mock.assert_not_called()
    def test_mismatch_fails_closed(self):
        mock=Mock()
        self.assertEqual(dispatch_from_owned_worker("inspect", "t_wrong", environ=self.env, runner=mock)["status"], "stale")
        mock.assert_not_called()
    def test_owner_controls_task_and_workspace(self):
        mock=Mock(return_value=SimpleNamespace(to_dict=lambda: {"status":"returned"}))
        result=dispatch_from_owned_worker("inspect", "t_1234", environ=self.env, runner=mock)
        self.assertEqual(result, {"status":"returned"})
        opts=mock.call_args.kwargs
        self.assertEqual(opts["claim_fence"].run_id,17)
        self.assertEqual(opts["workspace"],"/home/dshrunner/worktrees/t_1234-run-17")
    def test_register_without_override(self):
        ctx=Mock()
        register(ctx)
        args=ctx.register_tool.call_args.kwargs
        self.assertEqual(args["name"],"dsh_delegate_kanban")
        self.assertNotIn("override", args)

if __name__ == "__main__":
    unittest.main()

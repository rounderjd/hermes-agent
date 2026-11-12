"""Optional owner-mediated Hermes -> DSH delegation; never completes a Kanban task."""
from __future__ import annotations
import json
import os
from hermes_cli.dsh_worker_adapter import ClaimFence, run_dsh

def dispatch_from_owned_worker(prompt: str, owner_task: str, *, environ=None, runner=run_dsh) -> dict:
    env = os.environ if environ is None else environ
    if env.get("HERMES_DSH_WORKER_ENABLED") != "1":
        return {"status": "disabled", "reason": "DSH delegation not enabled"}
    if not owner_task:
        return {"status": "disabled", "reason": "Only owner-session Kanban workers may delegate"}
    task = env.get("HERMES_KANBAN_TASK", "")
    db = env.get("HERMES_KANBAN_DB", "")
    lock = env.get("HERMES_KANBAN_CLAIM_LOCK", "")
    try:
        run_id = int(env.get("HERMES_KANBAN_RUN_ID", "0"))
        timeout = int(env.get("HERMES_DSH_TIMEOUT", "900"))
    except ValueError:
        return {"status": "stale", "reason": "Invalid claim or timeout configuration"}
    if owner_task != task or not db or not lock or run_id <= 0:
        return {"status": "stale", "reason": "Incomplete or mismatched Hermes claim"}
    result = runner(task, prompt, host=env.get("HERMES_DSH_SSH_HOST", "helium-wsl"),
        workspace=f"/home/dshrunner/worktrees/{task}-run-{run_id}",
        timeout_seconds=timeout, enabled=True, claim_fence=ClaimFence(db, task, run_id, lock))
    return result.to_dict()

def _handler(args: dict, **_kwargs) -> str:
    from agent.delegation_context import owned_kanban_task
    return json.dumps(dispatch_from_owned_worker(str(args.get("prompt") or ""), owned_kanban_task()))

def register(ctx) -> None:
    ctx.register_tool(
        name="dsh_delegate_kanban", toolset="dsh_worker",
        schema={
            "name": "dsh_delegate_kanban",
            "description": "Delegate a bounded task to headless DSH from an owned Hermes Kanban session. A returned result is NOT completion.",
            "parameters": {"type": "object", "properties": {"prompt": {"type": "string"}}, "required": ["prompt"]},
        },
        handler=_handler,
    )

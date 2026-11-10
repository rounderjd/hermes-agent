"""Opt-in remote DeepSeek Harness headless executor for Hermes task handoffs.

This is a transport, not a Kanban completion authority. Hermes must separately
verify the artifacts and apply its existing task completion/approval gates.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import PurePosixPath
import re
import shlex
import subprocess
import sys
from typing import Callable

# A headless process has no Hermes completion authority: all results are candidates.
DEFAULT_WORKSPACE_ROOT = "/home/dshrunner/worktrees"

_SAFE_HOST = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_SAFE_TASK = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
_MAX_PROMPT = 64 * 1024
_MAX_CAPTURE = 4 * 1024 * 1024


@dataclass(frozen=True)
class DshResult:
    status: str
    returncode: int | None
    output: str
    error: str

    def to_dict(self) -> dict:
        return {"status": self.status, "returncode": self.returncode,
                "output": self.output, "error": self.error}


def remote_command(workspace: str, *, dsh_bin: str,
                   timeout_seconds: int = 1800,
                   workspace_root: str = DEFAULT_WORKSPACE_ROOT) -> str:
    """Quote fixed command tokens; never interpolate a task prompt into a shell."""
    path = PurePosixPath(workspace)
    if not workspace.startswith("/") or ".." in path.parts or "\n" in workspace:
        raise ValueError("workspace must be an absolute path without traversal")
    if not dsh_bin.startswith("/") or "\n" in dsh_bin:
        raise ValueError("dsh_bin must be an absolute executable path")
    args = ("timeout", "--signal=TERM", "--kill-after=10",
            str(timeout_seconds), dsh_bin, "--profile", "headless", "--json", "--permission-mode",
            "workspace-write", "--approval-policy", "ask", "-")
    return "cd " + shlex.quote(workspace) + " && exec " + shlex.join(args)


def run_dsh(
    task_id: str,
    prompt: str,
    *,
    host: str,
    workspace: str,
    dsh_bin: str = "/home/dshrunner/dsh-lab/node_modules/.bin/dsh",
    ssh_bin: str = "ssh",
    timeout_seconds: int = 1800,
    enabled: bool = False,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> DshResult:
    """Run one bounded headless task; preserve approval refusal as a blocked result.

    The SSH account must have its own restricted credentials and only a
    task-specific workspace. Never pass secrets in prompt or command args.
    """
    if not enabled:
        return DshResult("disabled", None, "", "DSH executor opt-in is disabled")
    if not _SAFE_TASK.fullmatch(task_id):
        raise ValueError("invalid task ID")
    if not _SAFE_HOST.fullmatch(host) or host.startswith("-"):
        raise ValueError("invalid SSH host")
    if not prompt.strip() or len(prompt.encode("utf-8")) > _MAX_PROMPT:
        raise ValueError("task prompt empty or too long")
    if not 1 <= timeout_seconds <= 7200:
        raise ValueError("timeout must be 1-7200 seconds")
    command = remote_command(workspace, dsh_bin=dsh_bin, timeout_seconds=timeout_seconds)
    argv = [ssh_bin, "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
            "-o", "StrictHostKeyChecking=yes", "-o", "ServerAliveInterval=15",
            "-o", "ServerAliveCountMax=2", "--", host, command]
    try:
        result = runner(argv, input=prompt, text=True, capture_output=True,
                        timeout=timeout_seconds, check=False)
    except subprocess.TimeoutExpired:
        return DshResult("timed_out", None, "", "DSH timeout; run requires review")
    except OSError as exc:
        return DshResult("transport_error", None, "", type(exc).__name__)
    # Bounded result fields; a later owner integration must additionally stream stdout
    # to capped files rather than buffering the complete subprocess output.
    output = result.stdout[:_MAX_CAPTURE]
    error = result.stderr[:8192]
    if result.returncode in (124, 137):
        status = "timed_out"  # remote GNU timeout; never claim completion
    elif result.returncode:
        status = "approval_blocked" if _is_approval_blocked(output, error) else "failed"
    else:
        # Successful process exit is evidence of execution, NOT permission to mark
        # a Kanban card done; parent must validate tests, artifacts and task run.
        status = "returned"
    return DshResult(status, result.returncode, output, error)


def _is_approval_blocked(output: str, error: str) -> bool:
    for line in output.splitlines():
        try:
            event = json.loads(line)
        except (ValueError, TypeError):
            continue
        if not isinstance(event, dict):
            continue
        # Structural status only; never classify arbitrary model prose as approval.
        if event.get("status") in {"approval_blocked", "approval_required"}:
            return True
        if event.get("type") in {"approval/unavailable", "approval/denied"}:
            return True
    return "approval/unavailable" in error or "approval/denied" in error


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Run an opt-in DSH headless assignment")
    p.add_argument("--task-id", required=True)
    p.add_argument("--host", default="helium-wsl")
    p.add_argument("--workspace", required=True)
    p.add_argument("--timeout", type=int, default=1800)
    p.add_argument("--dsh-bin", default="/home/dshrunner/dsh-lab/node_modules/.bin/dsh")
    args = p.parse_args(argv)
    prompt = sys.stdin.read(_MAX_PROMPT + 1)
    result = run_dsh(args.task_id, prompt, host=args.host,
                     workspace=args.workspace, timeout_seconds=args.timeout,
                     dsh_bin=args.dsh_bin,
                     enabled=os.environ.get("HERMES_DSH_WORKER_ENABLED") == "1")
    print(json.dumps(result.to_dict()))
    return 0 if result.status == "returned" else 1


if __name__ == "__main__":
    raise SystemExit(main())

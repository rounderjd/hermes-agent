"""Opt-in remote DeepSeek Harness headless executor for Hermes task handoffs.

This is a transport, not a Kanban completion authority. Hermes must separately
verify the artifacts and apply its existing task completion/approval gates.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from dataclasses import dataclass
import json
import os
import sqlite3
from urllib.parse import quote
from pathlib import PurePosixPath
import re
import selectors
import signal
import shlex
import subprocess
import sys
import time
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
    root = PurePosixPath(workspace_root)
    if not workspace_root.startswith("/") or ".." in root.parts:
        raise ValueError("workspace root must be an absolute path")
    if path == root or root not in path.parents:
        raise ValueError("task workspace must be strictly below allowed workspace root")
    if not dsh_bin.startswith("/") or "\n" in dsh_bin:
        raise ValueError("dsh_bin must be an absolute executable path")
    args = ("timeout", "--signal=TERM", "--kill-after=10",
            str(timeout_seconds), dsh_bin, "--profile", "headless", "--json", "--permission-mode",
            "workspace-write", "--approval-policy", "ask", "-")
    return "cd " + shlex.quote(workspace) + " && exec " + shlex.join(args)




@dataclass(frozen=True)
class ClaimFence:
    board_db: str
    task_id: str
    run_id: int
    claim_lock: str


def claim_is_current(fence: ClaimFence) -> bool:
    """Inspect a Hermes claim without writing to its Kanban database.

    A passed claim is only a point-in-time precondition; Hermes's owner-session
    completion gate remains authoritative and must check again before finalizing.
    """
    if not os.path.isabs(fence.board_db) or not _SAFE_TASK.fullmatch(fence.task_id):
        return False
    if fence.run_id <= 0 or not fence.claim_lock:
        return False
    uri = "file:" + quote(fence.board_db, safe="/") + "?mode=ro"
    try:
        with closing(sqlite3.connect(uri, uri=True, timeout=2)) as db:
            row = db.execute(
                "SELECT t.status, t.current_run_id, t.claim_lock, r.ended_at "
                "FROM tasks t LEFT JOIN task_runs r ON r.id=t.current_run_id "
                "WHERE t.id=?", (fence.task_id,)
            ).fetchone()
            return bool(row and row[0] == "running" and row[1] == fence.run_id
                        and row[2] == fence.claim_lock and row[3] is None)
    except (sqlite3.Error, OSError, ValueError):
        return False


def _run_streaming(argv: list[str], prompt: str, timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    """Bound stdout/stderr while executing SSH; terminate the process group on failure.

    No terminal is allocated. stdin is pumped nonblocking so a remote agent that
    stops reading cannot strand the owner. Output is never spooled unboundedly.
    """
    proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, start_new_session=True, bufsize=0)
    assert proc.stdin and proc.stdout and proc.stderr
    stdin_data = prompt.encode("utf-8")
    sent = 0
    stdout, stderr = bytearray(), bytearray()
    selector = selectors.DefaultSelector()
    try:
        for stream, direction, tag in (
                (proc.stdin, selectors.EVENT_WRITE, "in"),
                (proc.stdout, selectors.EVENT_READ, "out"),
                (proc.stderr, selectors.EVENT_READ, "err")):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, direction, tag)
        deadline = time.monotonic() + timeout_seconds
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(argv, timeout_seconds)
            for key, _ in selector.select(min(remaining, 0.5)):
                stream, tag = key.fileobj, key.data
                if tag == "in":
                    try:
                        size = os.write(stream.fileno(), stdin_data[sent:]) if sent < len(stdin_data) else 0
                    except BrokenPipeError:
                        size = 0
                    sent += size
                    if sent >= len(stdin_data) or size == 0:
                        selector.unregister(stream)
                        stream.close()
                    continue
                chunk = os.read(stream.fileno(), 65536)
                if not chunk:
                    selector.unregister(stream)
                    stream.close()
                    continue
                target, limit = (stdout, _MAX_CAPTURE) if tag == "out" else (stderr, 8192)
                if len(target) + len(chunk) > limit:
                    if tag == "out":
                        raise ValueError("DSH output exceeded capture limit")
                    chunk = chunk[:max(0, limit - len(target))]
                target.extend(chunk)
        rc = proc.wait(timeout=max(0.1, deadline - time.monotonic()))
        return subprocess.CompletedProcess(
            argv, rc, stdout.decode("utf-8", "replace"), stderr.decode("utf-8", "replace")
        )
    finally:
        selector.close()
        if proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
                proc.wait(timeout=2)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                if proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=2)
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            if stream and not stream.closed:
                stream.close()


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
    claim_fence: ClaimFence | None = None,
    runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
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
    if claim_fence is not None:
        if task_id != claim_fence.task_id or not claim_is_current(claim_fence):
            return DshResult("stale", None, "", "Hermes task claim is no longer current")
    command = remote_command(workspace, dsh_bin=dsh_bin, timeout_seconds=timeout_seconds)
    argv = [ssh_bin, "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
            "-o", "StrictHostKeyChecking=yes", "-o", "ServerAliveInterval=15",
            "-o", "ServerAliveCountMax=2", "--", host, command]
    # SSH connection setup and the remote kill-after grace happen outside
    # the remote GNU timeout clock. Give the local watchdog time to observe
    # remote exit before forcibly closing the SSH connection.
    local_timeout_seconds = timeout_seconds + 30
    try:
        if runner is None:
            result = _run_streaming(argv, prompt, local_timeout_seconds)
        else:
            result = runner(argv, input=prompt, text=True, capture_output=True,
                            timeout=local_timeout_seconds, check=False)
    except subprocess.TimeoutExpired:
        return DshResult("timed_out", None, "", "DSH timeout; run requires review")
    except (OSError, ValueError) as exc:
        return DshResult("transport_error", None, "", type(exc).__name__)
    if claim_fence is not None and not claim_is_current(claim_fence):
        return DshResult("stale", None, "", "Hermes task claim changed during DSH run")
    # The built-in runner captures bounded streams; injected test runners are capped here.
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
    values = [os.environ.get(k, "") for k in (
        "HERMES_KANBAN_DB", "HERMES_KANBAN_TASK",
        "HERMES_KANBAN_RUN_ID", "HERMES_KANBAN_CLAIM_LOCK"
    )]
    fence = None
    if any(values):
        if not all(values):
            print(json.dumps(DshResult("stale", None, "",
                                       "Incomplete Hermes claim context").to_dict()))
            return 1
        try:
            fence = ClaimFence(values[0], values[1], int(values[2]), values[3])
        except ValueError:
            print(json.dumps(DshResult("stale", None, "",
                                       "Invalid Hermes claim context").to_dict()))
            return 1
        if args.task_id != fence.task_id:
            print(json.dumps(DshResult("stale", None, "",
                                       "Task identifier differs from Hermes claim").to_dict()))
            return 1
    result = run_dsh(args.task_id, prompt, host=args.host,
                     workspace=args.workspace, timeout_seconds=args.timeout,
                     dsh_bin=args.dsh_bin, claim_fence=fence,
                     enabled=os.environ.get("HERMES_DSH_WORKER_ENABLED") == "1")
    print(json.dumps(result.to_dict()))
    return 0 if result.status == "returned" else 1


if __name__ == "__main__":
    raise SystemExit(main())

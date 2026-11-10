# Hermes → DeepSeek Harness worker transport (experimental)

This module provides a **transport-only**, disabled-by-default headless executor. It
does not change Kanban dispatch or install an unattended DSH worker in production.

## Contract

Hermes retains **all** responsibility for admission, claim ownership, completion,
retry budget, PR acceptance and approval states. The adapter returns one of:
`disabled`, `returned`, `failed`, `timed_out`, `transport_error`,
`approval_blocked`, `stale`. **Returned does not mean done**.

DSH is invoked through SSH on Helium WSL using:

```text
dsh --profile headless --json --permission-mode workspace-write --approval-policy ask -
```

The task is supplied through stdin; prompts never appear in command arguments.
A GNU timeout is enforced on the remote command, with a separate local SSH timeout.
Only subdirectories of `/home/dshrunner/worktrees` are permitted (no root workspace).
SSH requires verified host keys and batch-mode authentication. The gateway is
not given unrestricted permissions. DSH must run in a dedicated per-task workspace;
the remote workspace must already exist. No retries occur inside the transport. When invoked within an existing Hermes
worker, the adapter reads HERMES_KANBAN_DB/TASK/RUN_ID/CLAIM_LOCK and verifies
the current claim both before and after DSH execution (read-only database query).
A stale claim discards the DSH output. This does **not** replace the owner's
atomic completion checks. DSH exit code 124/137 is treated as timed out.
The subprocess runner drains stdout and stderr into bounded in-memory buffers.
The production bridge still needs persisted, redacted run artifacts and verified
NDJSON event parsing before unattended long-running tasks.

## Manual validation (no Kanban side effects)

```bash
export HERMES_DSH_WORKER_ENABLED=1
printf '%s' 'Inspect this workspace and report findings; make no changes.' |
  python -m hermes_cli.dsh_worker_adapter --task-id t_smoke \
    --host helium-wsl --workspace /home/dshrunner/worktrees/task-smoke
```

**Do not** enable the flag on the production dispatcher yet.

## Production integration required before rollout

1. Provision a distinct unprivileged SSH identity for Hermes→DSH, with authorized
   command restrictions and one task-scoped workspace per claim.
2. Add an opt-in dispatcher routing rule and persist executor identity, run ID,
   liveness and cancellation evidence to the existing Kanban task run.
3. Stream NDJSON into capped run artifacts, redact secrets, and verify the actual
   DSH event schema for interactive approval refusals.
4. Translate approval requests into durable `approval_blocked` records, accept
   explicit human approval through Hermes, then resume the **same** run.
5. Bind worktree ownership and enforce no concurrent Hermes/DSH writes.
6. Verify output/test/PR acceptance before an owner transitions the card to done.
7. Roll out to a handful of bounded tasks with cost and reliability comparison.

The current adapter is intentionally not wired into `_default_spawn`: simply
returning a PID would deceive Hermes's task lifecycle, because a DSH headless
process cannot complete a Hermes Kanban task through the owner-session receipt.


## Optional Hermes owner-mediated delegation plugin

The reference plugin lives under `integrations/dsh_kanban_bridge`. It is
**not installed or activated** by this PR. Pilot installation requires the
operator to copy the plugin manifest and Python entrypoint to a selected
Hermes profile plugin directory, enable the `dsh_worker` toolset, and
explicitly set `HERMES_DSH_WORKER_ENABLED=1` for that profile. Do not
enable this flag on a general/shared Hermes dispatcher.

The model-visible `dsh_delegate_kanban(prompt)` tool refuses invocation
without a dispatcher-owned Hermes Kanban session. The task ID, run ID, and
claim-lock come only from Hermes's worker context; the model cannot select
another task. The adapter verifies the current claim against the Kanban
database before and after remote execution, returning `stale` when
ownership changed.

The remote worktree path is derived as
`/home/dshrunner/worktrees/<task-id>-run-<run-id>`. The operator must first
provision that worktree with the intended repository contents and SSH
identity. The integration neither copies repositories nor grants new
network or filesystem permissions.

**Completion remains with Hermes:** a DSH return is evidence for the
Hermes worker to examine; the owner-session `kanban_complete` and PR
acceptance gates still decide task completion. Risky operations requested
by DSH must fail closed until an attended approval/resume mechanism is
implemented. This pilot is not the future automatic DSH dispatch route.

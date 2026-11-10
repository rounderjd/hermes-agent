# Hermes → DeepSeek Harness worker transport (experimental)

This PR introduces a **transport-only**, disabled-by-default headless executor. It
does not change Kanban dispatch or install an unattended DSH worker in production.

## Contract

Hermes retains **all** responsibility for admission, claim ownership, completion,
retry budget, PR acceptance and approval states. The adapter returns one of:
`disabled`, `returned`, `failed`, `timed_out`, `transport_error`,
`approval_blocked`. **Returned does not mean done**.

DSH is invoked through SSH on Helium WSL using:

```text
dsh --profile headless --json --permission-mode workspace-write --approval-policy ask -
```

The task is supplied through stdin; prompts never appear in command arguments.\nA GNU timeout is enforced on the remote command, with a separate local SSH timeout.\nOnly subdirectories of `/home/dshrunner/worktrees` are permitted (no root workspace).
SSH requires verified host keys and batch-mode authentication. The gateway is
not given unrestricted permissions. DSH must run in a dedicated per-task workspace;
the remote workspace must already exist. No retries occur inside the transport. DSH exit code 124/137 is treated as timed out.\nThe process capture remains buffered at this stage; the production bridge needs\nstreaming capped artifacts before enabling untrusted long-running tasks.

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

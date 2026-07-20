# Temporal Workflow history replay

Temporal re-executes Workflow code from Event History. A code change that makes different
deterministic decisions can make an existing workflow nondeterministic. Replay catches this before
a new worker receives tasks from that history.

The suite discovers `*.json` histories under `artifacts/histories`, registers Workflow
implementations in `workflow_registry.py`, and fails when a supplied history cannot replay.

## Run replay

From the repository root:

```bash
uv run pytest -m replay tests/replay
```

Checked-in samples:

| History | Coverage |
|---|---|
| `root-sync-replay-smoke.json` | successful `RootSyncWorkflow` path |
| `remove-objects-pre-batch.json` | compatibility branch for cleanup history created before bounded batches |

The cleanup sample protects `workflow.patched("bounded-object-cleanup-v1")`. New workflows use
bounded cleanup Activities; the preserved code branch replays the supplied earlier payload.

These files prove the harness and those exact paths only. They are not representative of another
namespace or every open workflow.

## Add histories from a target

1. Inventory the Workflow Types and behavior paths affected by the release.
2. Export raw Event History JSON using the target Temporal CLI, UI, or API.
3. Redact secrets/customer content or configure the same payload codec as the worker.
4. Place files under `artifacts/histories`; organize by environment, workflow type, build, and
   behavior.
5. Use descriptive names such as `root-sync/continue-as-new-after-page.json`.
6. Add the Workflow Type to `workflow_registry.py` if it is not registered.
7. Replay with the exact source revision and dependency lock intended for deployment.
8. Archive the inventory and test output as release evidence.

Include long-running, signaled, canceled, failed, retried, patched, and Continue-As-New paths for
every changed Workflow Type.

Temporal exports may omit Workflow ID. The test uses the relative filename as a replay-only
identity. If a payload codec needs the real ID, add top-level `workflowId`; the SDK ignores that
extra field while parsing Event History.

If no histories exist, pytest reports an explicit skip. A release process must separately enforce
its required inventory so an empty directory cannot be mistaken for compatibility.

## Compatibility rule

A successful replay proves only that the current code can replay the supplied histories. It does
not prove all open executions were sampled. Keep each open execution routed to a compatible worker
build through Worker Versioning until it closes or reaches an explicitly compatible
Continue-As-New boundary.

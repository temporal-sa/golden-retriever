# Temporal history replay

Replay detects nondeterministic Workflow changes before a worker receives tasks from an existing
execution. The test discovers every `*.json` file under `artifacts/histories`, registers the
corresponding implementations in `workflow_registry.py`, and fails on incompatibility.

## Run the suite

```bash
uv run pytest -m replay tests/replay
```

Checked-in histories provide two focused compatibility samples:

| History | Coverage |
|---|---|
| `root-sync-replay-smoke.json` | successful local `RootSyncWorkflow` execution |
| `remove-objects-pre-batch.json` | `RemoveObjectsWorkflow` compatibility branch for a one-shot object-cleanup payload |

The second sample protects the `workflow.patched("bounded-object-cleanup-v1")` boundary. New
executions use bounded cleanup Activities; histories recorded with the older payload replay through
the preserved branch.

These samples prove the harness and those exact paths. They are not representative evidence for
another namespace, deployment, or every Workflow Type.

## Add target histories

1. Export raw Workflow Event History JSON from the target Temporal namespace using its CLI, Web UI,
   or API.
2. Redact secrets/customer content or configure the same payload codec required by the worker.
3. Place the history under `artifacts/histories`; organize subdirectories by environment, type,
   build, or behavior as useful.
4. Use descriptive names such as `root-sync/continue-as-new-after-page.json`.
5. Add the Workflow Type to `workflow_registry.py` if it is not already registered.
6. Replay with the exact source revision and dependency lock intended for the worker artifact.
7. Archive the inventory and output with release evidence.

Include long-running, signaled, canceled, failed, retried, patched, and Continue-As-New paths for
every type affected by a release.

Temporal CLI exports may omit Workflow ID. The test uses the relative filename as a replay-only
identity. If a payload codec needs the real ID, add a top-level `workflowId` string; the SDK ignores
that extra field when parsing event history.

If no histories exist, the parametrized test reports an explicit skip. A release process must
separately assert that its required inventory is complete so an empty directory cannot be mistaken
for compatibility evidence.

## Compatibility rule

A successful replay proves only that this code can replay the supplied histories. It does not prove
that all open executions were sampled. Keep each open execution routed to a compatible worker build
through Worker Versioning until it closes or reaches an explicitly compatible Continue-As-New path.

# Temporal history replay

Replay detects nondeterministic workflow changes before a worker receives tasks from an existing
execution. The test discovers every `*.json` file under `artifacts/histories/`, registers the
workflow implementations in `workflow_registry.py`, and fails on the first incompatible history.

## Run the suite

From the repository root:

```bash
uv run pytest -m replay tests/replay
```

The repository includes `root-sync-replay-smoke.json`, a successful local
`RootSyncWorkflow` history. It keeps the replay harness active in the default test run, but it is
not representative evidence for another namespace, deployment, or workflow path.

## Add representative histories

1. Export Workflow Event History JSON from the target Temporal namespace using its CLI, Web UI, or
   API.
2. Preserve the raw event history and place it under `artifacts/histories/`. Subdirectories may be
   organized by environment, Workflow Type, build, or scenario.
3. Use filenames that describe the workflow and behavior, such as
   `root-sync/continue-as-new-after-page.json`.
4. Run the replay suite against the exact source revision and dependency versions planned for the
   worker artifact.
5. Retain the history inventory and replay output with the release evidence.

Include every Workflow Type affected by a release and representative long-running, signaled,
canceled, failed, retried, and Continue-As-New histories. Redact or encode sensitive payloads using
the same codec support required by the worker; do not commit production secrets or customer
content.

Temporal CLI exports may omit the Workflow ID. The test uses the relative filename as a stable
replay-only identity. If a payload codec needs the real ID, add a top-level `workflowId` string to
the JSON object; the SDK ignores that extra field while parsing event history.

If no JSON histories exist, the parametrized replay test reports an explicit skip. A release gate
should separately verify that its required history inventory is complete so an empty directory
cannot be mistaken for compatibility evidence.

## Compatibility rule

A successful replay proves only that the selected code can replay the supplied histories. It does
not prove that all open executions were sampled. Keep each open execution routed to a compatible
worker build through Worker Versioning until it closes or transitions by an explicitly compatible
Continue-As-New path.

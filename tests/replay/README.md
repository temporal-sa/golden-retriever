# Temporal history replay

Place representative Temporal CLI or Web UI JSON exports under
`artifacts/histories/`. The replay test discovers JSON files recursively and registers every
retrieval Workflow Type.

Temporal CLI exports do not include a Workflow ID, so the test uses the relative filename as a
replay identity. If a payload codec depends on the real ID, add a top-level `workflowId` string to
the JSON export; the SDK ignores that extra field while parsing the history.

Run only this suite with:

```shell
pytest -m replay tests/replay
```

When no histories are present, collection produces one explicit skip rather than a false pass.

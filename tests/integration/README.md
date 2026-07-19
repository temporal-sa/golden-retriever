# Temporal integration tests

The repository has three complementary Temporal test layers:

- fast provider contract tests run in the default suite without a server;
- `tests/integration` runs the controller, quota, provider, full workflow tree, and local starter
  against real Temporal Workflow histories;
- `tests/demo/test_temporal_late_writer.py` runs the real document workflow/Activity cancellation
  path and proves the held generation-7 writer attempts a commit after the generation-8 fence.

The Temporal scenarios are opt-in because the Python SDK may start a local server process and
download its matching Temporal CLI binary.

## Run every Temporal integration test

From the repository root:

```bash
RUN_TEMPORAL_INTEGRATION=1 uv run pytest -m integration
```

Most scenarios start SDK-managed ephemeral servers. No separately running
`temporal server start-dev` or `retrieval-worker` is required.

Run only the Northstar canceled late-writer proof:

```bash
RUN_TEMPORAL_INTEGRATION=1 uv run pytest -q tests/demo/test_temporal_late_writer.py
```

That test uses an SDK time-skipping server and in-memory persistence so it can deterministically
observe `document_commit_held`, commit the `7 -> 8` fence, cancel the real
`DocumentIngestionWorkflow`, release the bounded gate, and observe
`stale_generation_rejected`. It proves the Temporal cancellation path, not live Lakebase SQL.

## Run `tests/integration` against an existing namespace

Use a dedicated disposable namespace because the suite starts and closes executions:

```bash
TEMPORAL_INTEGRATION_ADDRESS=<FRONTEND_ADDRESS> \
TEMPORAL_INTEGRATION_NAMESPACE=<TEST_NAMESPACE> \
TEMPORAL_INTEGRATION_API_KEY=<API_KEY_IF_REQUIRED> \
RUN_TEMPORAL_INTEGRATION=1 \
uv run pytest -m integration tests/integration
```

The namespace defaults to `default`; TLS is enabled when an API key is present. The helper does not
expose separate certificate/mTLS options. Extend it before using a namespace with different
connection requirements.

The Northstar late-writer test is intentionally local and does not use the external namespace
variables.

## What the scenarios verify

- provider delay and Activity cancellation behavior;
- non-retryable provider authentication failure;
- structured provider exhaustion/reset observations;
- two callers sharing one credential and `UserQuotaWorkflow`;
- quota permit Signal-with-Start reuse;
- public local sync/deactivation starter behavior;
- controller sync and idempotent deactivation commands;
- provider -> staged body -> document mutation through the complete workflow hierarchy;
- a cancellation-resistant demo hold reaching a stale repository mutation after the fence.

Every scenario uses unique Workflow IDs and Task Queues. An external namespace retains closed test
histories according to its policy.

## What these tests do not prove

The integration suite uses in-memory or scripted adapters. Passing it does not establish target
Lakebase connectivity, migration/grant correctness, real-provider compatibility, target namespace
limits, production capacity, or a complete replay sample. Combine it with the
[replay suite](../replay/README.md), [load harness](../load/README.md), Lakebase contract tests, and
[production-readiness guide](../../docs/architecture-production-readiness.md).

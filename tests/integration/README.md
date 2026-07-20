# Temporal integration tests

Integration tests execute real Temporal Workflow Event Histories instead of calling workflow code
as ordinary Python. Use them after the default unit/contract suite and before deploying a worker
change.

These tests use in-memory or scripted provider/database adapters. They validate Temporal behavior,
not live Lakebase permissions or production provider compatibility.

## Test layers

| Layer | Location | What it exercises |
|---|---|---|
| Provider contracts | default suite | provider delays, cancellation, auth errors, quota mapping |
| General Temporal integration | `tests/integration` | controller, quota, provider, complete workflow hierarchy |
| Northstar late writer | `tests/demo/test_temporal_late_writer.py` | real ingestion cancellation after generation fence |

The Temporal scenarios are opt-in because the SDK may start a local server process and download a
matching Temporal CLI binary.

## Run with SDK-managed servers

From the repository root:

```bash
RUN_TEMPORAL_INTEGRATION=1 uv run pytest -m integration
```

Most tests create isolated ephemeral servers and unique Task Queues. Do not start
`retrieval-worker` or `temporal server start-dev` separately.

Run only the Northstar late-writer proof:

```bash
RUN_TEMPORAL_INTEGRATION=1 \
uv run pytest -q tests/demo/test_temporal_late_writer.py
```

That scenario observes a held generation-7 document, commits the generation-8 fence, cancels the
real ingestion workflow, releases the bounded hold, and verifies a stale repository mutation. Its
repository is in memory; Lakebase SQL is covered by separate tests and live deployment checks.

## Run against an existing namespace

Use a dedicated disposable namespace because the suite starts and closes executions:

```bash
TEMPORAL_INTEGRATION_ADDRESS=<FRONTEND_HOST_PORT> \
TEMPORAL_INTEGRATION_NAMESPACE=<TEST_NAMESPACE> \
TEMPORAL_INTEGRATION_API_KEY=<API_KEY_IF_REQUIRED> \
RUN_TEMPORAL_INTEGRATION=1 \
uv run pytest -m integration tests/integration
```

Defaults:

- namespace: `default`;
- TLS: enabled when an API key is present.

The helper does not expose client-certificate/mTLS configuration. Extend/review it before using a
namespace with different authentication. The Northstar late-writer test always uses its local
time-skipping server and ignores the external-namespace variables.

Never run against a shared production Task Queue. External namespaces retain closed histories
according to their retention policy.

## Behaviors covered

- provider delay and Activity cancellation;
- non-retryable provider authentication failure;
- structured quota exhaustion and reset observations;
- multiple callers sharing one quota scope;
- permit Signal-with-Start reuse;
- public local sync/deactivation starter;
- controller command idempotency;
- provider reference → staged body → generation-fenced document mutation;
- cancellation-resistant demo hold reaching a stale mutation after the fence.

## Interpreting success

A passing suite proves the tested code is deterministic and behaves correctly in the supplied
Temporal scenarios. It does not prove:

- target Lakebase connectivity/migrations/grants;
- compatibility with a real provider;
- target namespace limits or production capacity;
- replay compatibility for histories not supplied to the test.

Combine it with the [replay suite](../replay/README.md), [load harness](../load/README.md), default
Lakebase/App tests, and the [production-readiness guide](../../docs/architecture-production-readiness.md).

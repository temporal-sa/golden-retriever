# Temporal integration tests

This directory contains two layers:

- fake-provider contract tests that run in the default suite without Temporal; and
- opt-in scenarios that start workers and execute real Workflow histories in a Temporal namespace.

The opt-in scenarios are disabled by default because an ephemeral environment may start a local
server process and download the matching Temporal CLI binary.

## Run with an ephemeral server

From the repository root:

```bash
RUN_TEMPORAL_INTEGRATION=1 uv run pytest -m integration tests/integration
```

The Temporal SDK starts and stops an isolated local server. No separately running
`temporal server start-dev` or `retrieval-worker` process is required.

## Run against an existing namespace

Use a dedicated, disposable test namespace because the suite starts and closes executions:

```bash
TEMPORAL_INTEGRATION_ADDRESS=<frontend-address> \
TEMPORAL_INTEGRATION_NAMESPACE=<test-namespace> \
TEMPORAL_INTEGRATION_API_KEY=<api-key-if-required> \
RUN_TEMPORAL_INTEGRATION=1 \
uv run pytest -m integration tests/integration
```

The namespace defaults to `default`. The test client enables TLS when an API key is present. This
suite does not expose separate TLS or mTLS certificate settings; extend its environment helper
before using a namespace that requires a different connection configuration.

## What the scenarios verify

- provider delays and Activity cancellation behavior;
- non-retryable provider authentication failure;
- structured provider exhaustion/429 observations;
- two callers sharing one credential and one `UserQuotaWorkflow`;
- quota permit Signal-with-Start reuse;
- the public local sync/deactivation starter;
- controller sync and idempotent deactivation commands;
- the complete provider → staged body → document mutation topology.

Every scenario creates unique Workflow IDs and Task Queues. External namespaces can retain closed
test histories according to their retention policy.

## Scope limits

These tests use local or scripted adapters. Passing them does not establish production adapter
durability, capacity, provider compatibility, namespace configuration, or upgrade determinism.
Use the [replay suite](../replay/README.md), [load harness](../load/README.md), and
[production-readiness guide](../../docs/architecture-production-readiness.md) for those gates.

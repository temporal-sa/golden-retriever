# Temporal load harness

The opt-in load scenario measures Temporal mechanics with small synthetic workflows. It reports:

- total Event History events and approximate protobuf bytes;
- Signal counts by name;
- Signal-to-Workflow-Task-start resume latency;
- Activity schedule-to-start latency;
- dispatch counts, weighted Jain index, completion rank, and maximum consecutive dispatches by
  fairness key.

Unit tests for the measurement helpers run in the default suite. The Temporal scenario does not
start or generate load unless `RUN_TEMPORAL_LOAD=1`.

## Run with an ephemeral server

From the repository root:

```bash
RUN_TEMPORAL_LOAD=1 uv run pytest -s -m load tests/load
```

The SDK may download a matching Temporal CLI binary. The JSON report is printed to standard output
because `-s` disables pytest capture; the harness does not write result files.

Default inputs are 100 Signals, 80 large-scope Activities, 20 small-scope Activities, and a queue
rate limit of 500 Activities per second.

## Run against a dedicated namespace

```bash
TEMPORAL_LOAD_ADDRESS=<frontend-address> \
TEMPORAL_LOAD_NAMESPACE=<test-namespace> \
TEMPORAL_LOAD_API_KEY=<api-key-if-required> \
RUN_TEMPORAL_LOAD=1 \
uv run pytest -s -m load tests/load
```

The namespace defaults to `default`, and TLS is enabled when an API key is present. Use an isolated
test namespace with appropriate quotas and retention; do not run unreviewed load against a shared
production Task Queue.

## Tune the scenario

| Environment variable | Default | Meaning |
|---|---:|---|
| `TEMPORAL_LOAD_SIGNAL_COUNT` | 100 | Signals delivered to the history/resume workflow |
| `TEMPORAL_LOAD_LARGE_SCOPE_COUNT` | 80 | Activities submitted for the large fairness scope |
| `TEMPORAL_LOAD_SMALL_SCOPE_COUNT` | 20 | Activities submitted for the small fairness scope |
| `TEMPORAL_LOAD_QUEUE_RPS` | 500 | Worker-side Task Queue Activity rate limit |

All counts must be positive. Increase them gradually while monitoring namespace quotas, worker
slots, schedule-to-start latency, and history limits.

## Interpret the report

Fairness is approximate within Task Queue partitions, so the harness reports observed order and
distribution rather than asserting a globally strict schedule. Confirm Priority/Fairness support
and enable it on the target namespace before treating those measurements as a scheduler
evaluation.

This scenario does not execute the retrieval workflow tree, Northstar App, or Lakebase adapter. It
validates the measurement harness, not production capacity or SLOs. A release load test must add
realistic stores, users, resources, page sizes, document bodies, quota scopes, provider behavior,
database pool/transaction/search load, and production storage, as described in the
[production-readiness guide](../../docs/architecture-production-readiness.md).

# Temporal load harness

This opt-in harness measures selected Temporal scheduling and Event History mechanics with small
synthetic workflows. It does not exercise the retrieval workflow hierarchy, Northstar App, or
Lakebase and must not be treated as a production capacity result.

## Measurements

- total Event History event count and approximate protobuf bytes;
- Signal counts by name;
- Signal-to-Workflow-Task-start resume latency;
- Activity schedule-to-start latency;
- dispatch count/order, weighted Jain index, completion rank, and maximum consecutive dispatches
  by fairness key.

Unit tests for measurement helpers run in the default suite. The Temporal scenario starts only
when `RUN_TEMPORAL_LOAD=1`.

## Run with an ephemeral server

From the repository root:

```bash
RUN_TEMPORAL_LOAD=1 uv run pytest -s -m load tests/load
```

The SDK may download a matching Temporal CLI binary. `-s` prints the JSON report to standard
output; the harness does not create a result file.

Default workload:

- 100 Signals;
- 80 Activities in the large fairness scope;
- 20 Activities in the small fairness scope;
- 500 Activity tasks/second worker-side queue limit.

## Run against a dedicated namespace

```bash
TEMPORAL_LOAD_ADDRESS=<FRONTEND_HOST_PORT> \
TEMPORAL_LOAD_NAMESPACE=<TEST_NAMESPACE> \
TEMPORAL_LOAD_API_KEY=<API_KEY_IF_REQUIRED> \
RUN_TEMPORAL_LOAD=1 \
uv run pytest -s -m load tests/load
```

The namespace defaults to `default`; TLS is enabled with an API key. Use an isolated load-test
namespace with reviewed quotas and retention. Never send unreviewed load to shared production Task
Queues.

## Tune inputs

| Variable | Default | Meaning |
|---|---:|---|
| `TEMPORAL_LOAD_SIGNAL_COUNT` | 100 | Signals sent to the history/resume workflow |
| `TEMPORAL_LOAD_LARGE_SCOPE_COUNT` | 80 | Activities in the large fairness scope |
| `TEMPORAL_LOAD_SMALL_SCOPE_COUNT` | 20 | Activities in the small fairness scope |
| `TEMPORAL_LOAD_QUEUE_RPS` | 500 | Worker-side Activity rate limit |

All values must be positive. Increase one dimension at a time while monitoring namespace quotas,
worker slots, schedule-to-start latency, and Event History limits.

## Interpret results

Fairness is approximate within Temporal Task Queue partitions. The harness reports observed order
and distribution; it does not assert a globally strict scheduler. Confirm the target supports
Priority/Fairness before treating the results as a scheduling evaluation.

A release capacity test must add realistic stores, users, resources, page sizes, document bodies,
quota scopes, provider behavior, database pool/transaction/search load, worker replicas, and named
SLO thresholds. See the [production-readiness guide](../../docs/architecture-production-readiness.md).

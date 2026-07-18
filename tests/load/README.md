# Opt-in Temporal load harness

The load test is skipped unless explicitly enabled. It measures:

- total Event History events and approximate protobuf bytes;
- Signal counts by name;
- Signal-to-Workflow-Task-start resume latency;
- Activity schedule-to-start latency;
- dispatch counts, weighted Jain index, completion rank, and maximum consecutive dispatches by
  fairness key.

Run against an ephemeral local server (which may download Temporal CLI):

```shell
RUN_TEMPORAL_LOAD=1 pytest -s -m load tests/load
```

Or set `TEMPORAL_LOAD_ADDRESS`, `TEMPORAL_LOAD_NAMESPACE`, and optionally
`TEMPORAL_LOAD_API_KEY` to use a dedicated test namespace. Counts and queue RPS can be controlled
with `TEMPORAL_LOAD_SIGNAL_COUNT`, `TEMPORAL_LOAD_LARGE_SCOPE_COUNT`,
`TEMPORAL_LOAD_SMALL_SCOPE_COUNT`, and `TEMPORAL_LOAD_QUEUE_RPS`.

Fairness is approximate per Task Queue partition, so the harness reports observed behavior rather
than asserting a globally strict order. Enable Fairness on the target Namespace before using the
results as a scheduler evaluation.

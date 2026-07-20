# Metrics and observability

This page explains which operational signals the code emits, which signals must come from Temporal,
Lakebase, or the App platform, and how to turn them into dashboards and alerts.

The worker creates Temporal SDK metric instruments but does not configure an exporter. A deployment
must supply an OpenTelemetry- or Prometheus-enabled Temporal runtime before `Client.connect` and
verify the final exported names in its backend.

## How to read metric names

The table below lists application **base names**. Temporal SDK runtimes commonly add a
`temporal_` prefix; Prometheus translation may also add counter or unit suffixes. Confirm the actual
names and units after configuring the target exporter.

Workflow code emits through replay-aware `workflow.metric_meter`. Activities emit through
`activity.metric_meter`. Emission is fail-open: a telemetry failure does not change workflow or
Activity behavior.

## Application metrics implemented in code

| Base name | Type | Meaning |
|---|---|---|
| `retrieval_quota_permit_requests` | counter | Permit requests by accepted/denied/ignored outcome |
| `retrieval_quota_permits_granted` | counter | Permits granted |
| `retrieval_quota_grant_signal_failures` | counter | Failed grant Signal delivery |
| `retrieval_quota_pending_requests` | gauge | Pending permits in a quota coordinator |
| `retrieval_quota_in_flight` | gauge | Reserved or active permits |
| `retrieval_quota_scope_blocked` | gauge | Quota-blocked state, 0 or 1 |
| `retrieval_quota_wait_duration_ms` | histogram | Permit request-to-grant latency |
| `retrieval_provider_requests` | counter | Provider calls by operation/outcome |
| `retrieval_provider_quota_exhausted` | counter | Structured provider quota observations |
| `retrieval_lifecycle_transitions` | counter | Lifecycle transition attempts/outcomes |
| `retrieval_stale_generation_rejections` | counter | Mutations rejected by a generation fence |
| `retrieval_document_ingestion_results` | counter | Document Activity result status |
| `retrieval_deactivation_drain_duration_ms` | histogram | Owned-work drain latency/timeout status |

Allowed attributes are limited to `mutation`, `operation`, `provider`, `quota_class`, `reason`,
`status`, `transition`, and `work_class`. Unknown keys are dropped; values larger than 64 UTF-8
bytes become `other`.

Store, user, credential, cursor, request, run, workflow, operation, and idempotency identities are
deliberately excluded to prevent high cardinality and sensitive labels.

## Signals supplied elsewhere

Not every observable fact is an application metric:

| Signal | Authoritative source |
|---|---|
| App process liveness | `/healthz` and App platform status |
| Lakebase/Temporal readiness | `/readyz` |
| Migration readiness/checksum drift | both migration `--check --json` commands |
| Database pool, transaction, lock, and query latency | Lakebase/Postgres telemetry |
| Search latency and HTTP request rate | App hosting/tracing telemetry |
| Duplicate/conflicting HTTP requests | durable receipts and structured application logs |
| Cleanup documents/chunks per batch | workflow result and Northstar events |
| Northstar quota/hold/fence/stale/cleanup story | `retrieval_demo_ui.demo_events` |
| Worker pollers, slots, task latency, failures | Temporal SDK/Core metrics |
| Workflow state and open executions | Temporal visibility |

`demo_events` is a bounded presentation/audit stream, not a monitoring time series. Do not scrape
it as a high-volume metrics backend.

Cleanup counts are best-effort audit totals. If an Activity deletes a batch but its completion is
lost, a retry may delete the next batch and the workflow cumulative count may undercount. The
authoritative completion condition is zero remaining users, retrieval state, documents, and
chunks.

Do not build dashboards around nonexistent names such as `retrieval_db_transaction_total`,
`retrieval_db_pool_in_use`, or `retrieval_search_duration_ms`. Add and test those instruments in
code first.

## Temporal SDK/Core metrics to enable

Export SDK/Core signals for both Task Queues:

- Workflow and Activity Task schedule-to-start latency;
- Workflow Task execution, Activity execution, and replay latency;
- available/used worker task slots;
- poller counts, poll successes, and empty polls;
- workflow completion/failure and task-failure counters;
- Worker Deployment/build identity where the backend supports it.

The load harness reports selected Event History event/byte counts, Signal resume latency, Activity
schedule-to-start latency, and synthetic fairness. It is a test report rather than a continuous
collector.

## Coverage and known gaps

| Operational need | Coverage |
|---|---|
| Provider outcomes and quota exhaustion | application counters |
| Pending/in-flight/blocked quota and wait latency | application gauges/histogram |
| Lifecycle transitions and stale writers | application counters |
| Ingestion outcomes | application counter |
| Deactivation drain duration/timeouts | application histogram |
| Pollers, slots, task schedule-to-start | Temporal SDK/Core after exporter setup |
| Active sync/remediation totals | not instrumented |
| Sync cancellation latency | not instrumented |
| Complete deactivation failure count | use Temporal visibility until instrumented |
| Continuous Event History size | not instrumented; load harness is point-in-time |
| Lakebase pool/transaction/search metrics | use database telemetry |
| Exporters, dashboards, thresholds, paging | deployment responsibility |

These gaps do not block the deterministic demo, but they are production-readiness gates.

## Recommended dashboards

Build views by namespace, Task Queue, Worker Deployment/build, provider, quota class, operation,
and bounded status. Avoid customer identifiers.

1. **Worker health:** pollers, slots, task failures, poll results, and schedule-to-start percentiles
   for both queues.
2. **Provider and quota:** provider outcomes, exhaustion, pending/in-flight permits, blocked scopes,
   grant failures, and wait percentiles.
3. **Lifecycle safety:** transition outcomes, stale-generation rejections, ingestion outcomes, and
   deactivation drain percentiles/timeouts.
4. **Database and App:** Lakebase connections/query latency, HTTP traffic/errors, and readiness
   failures from platform telemetry.
5. **History and capacity:** open executions, history growth, Continue-As-New, pending tasks,
   duration, and throughput.

## Alert candidates

Choose thresholds from production-like tests and named SLOs. Useful conditions include:

- no healthy poller on either Task Queue;
- sustained worker slot exhaustion or schedule-to-start latency above SLO;
- quota still blocked/pending after its authoritative reset;
- grant Signal failure or provider authentication failure;
- elevated ingestion failures or unexpected stale-generation rejection rate;
- deactivation drain timeout or completion beyond SLA;
- App readiness failure, migration drift, or Lakebase connection saturation;
- Event History approaching a reviewed namespace limit;
- unexpected starts of optional drain-only Workflow Types.

Every page must include build/deployment identity and a runbook link without exposing high-cardinality
customer data. Exercise the real alert rule, exporter, routing, and paging destination before
launch.

## Validation boundary

Unit, integration, and load tests validate selected emitters and helpers. They do not configure a
durable exporter, create dashboards, choose thresholds, or test paging. Those actions belong to
the target environment and the [production-readiness gate](../architecture-production-readiness.md).

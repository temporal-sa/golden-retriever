# Metrics, events, dashboards, and alerts

The code creates a bounded set of Temporal SDK metric instruments. `retrieval-worker` does not
configure a metrics exporter. A deployment must construct an OpenTelemetry- or Prometheus-enabled
Temporal SDK runtime before `Client.connect`, then verify the exported names and labels in its own
backend.

The SDK runtime commonly prefixes instruments with `temporal_`; Prometheus translation can also
add counter and unit suffixes. The names below are the application base names, not a promise about
the final exporter spelling.

## Implemented application metrics

Workflow metrics use replay-aware `workflow.metric_meter`. Activity metrics use
`activity.metric_meter`. Emission is fail-open: a telemetry failure never changes workflow or
Activity behavior.

| Base instrument | Type | Meaning |
|---|---|---|
| `retrieval_quota_permit_requests` | Counter | Permit requests by accepted/denied/ignored outcome |
| `retrieval_quota_permits_granted` | Counter | Permits granted |
| `retrieval_quota_grant_signal_failures` | Counter | Failed grant Signal delivery |
| `retrieval_quota_pending_requests` | Gauge | Pending permits in a quota coordinator |
| `retrieval_quota_in_flight` | Gauge | Reserved or active permits |
| `retrieval_quota_scope_blocked` | Gauge | Authoritative quota-blocked state, 0 or 1 |
| `retrieval_quota_wait_duration_ms` | Histogram | Permit request-to-grant latency |
| `retrieval_provider_requests` | Counter | Provider Activity calls by operation and outcome |
| `retrieval_provider_quota_exhausted` | Counter | Structured provider quota-exhaustion observations |
| `retrieval_lifecycle_transitions` | Counter | Lifecycle transition attempts/outcomes |
| `retrieval_stale_generation_rejections` | Counter | Mutations rejected by a generation fence |
| `retrieval_document_ingestion_results` | Counter | Document Activity result status |
| `retrieval_deactivation_drain_duration_ms` | Histogram | Owned-operation drain latency and timeout status |

Allowed application attributes are limited to `mutation`, `operation`, `provider`, `quota_class`,
`reason`, `status`, `transition`, and `work_class`. Unknown keys are dropped and values larger than
64 UTF-8 bytes collapse to `other`. Store, user, credential, cursor, request, operation, run,
workflow, and idempotency identities are intentionally excluded.

## Signals that are not application metrics

The following are present as state, events, logs, or target-platform telemetry—not as instruments
created by this repository:

| Signal | Available source |
|---|---|
| Lakebase connection health | `/readyz` and Lakebase/Postgres service telemetry |
| Core/demo migration readiness | `/readyz` and the two migration `--check --json` commands |
| Database transaction/pool latency | Lakebase/Postgres monitoring only |
| Search latency/hit count | request logs or external HTTP tracing only |
| Idempotency duplicate/conflict count | durable receipts plus application logs only |
| Cleanup documents/chunks per batch | workflow result and Northstar `demo_events` only |
| Northstar quota/hold/fence/stale/cleanup timeline | `retrieval_demo_ui.demo_events` and App API |
| App request rate/latency | hosting/runtime HTTP telemetry only |

Cleanup batch document/chunk totals are best-effort audit counts. A lost Activity completion can
cause a retry to delete the next bounded batch, so the workflow's cumulative total can undercount
even though generation fencing and the final database state remain correct. Use the authoritative
terminal invariant—zero documents, chunks, users, and retrieval state—rather than those totals for
completion or compliance gates.

Do not create dashboards that query nonexistent application instruments such as
`retrieval_db_transaction_total`, `retrieval_db_pool_in_use`, or
`retrieval_search_duration_ms`. Add and test those instruments in code before depending on them.

`demo_events` is a presentation/audit stream with deduplicated event keys and bounded JSON details.
It is not a monitoring time series and should not be scraped as a high-volume metrics backend.

## Temporal SDK/Core metrics

Use SDK/Core metrics for worker and Task Queue health, including:

- Activity and Workflow Task schedule-to-start latency;
- Activity execution, Workflow Task execution, and replay latency;
- available/used worker task slots and poller counts;
- poll success/empty results and workflow completion/failure counters.

The opt-in load harness also measures selected Workflow History event/byte counts, Signal resume
latency, Activity schedule-to-start latency, and synthetic fairness. It is a test report, not a
continuous collector.

## Coverage matrix

| Operational need | Status |
|---|---|
| Provider request outcomes and quota exhaustion | Implemented application counters |
| Pending/in-flight/blocked quota state and wait time | Implemented gauges/histogram |
| Lifecycle transition and stale-writer outcomes | Implemented counters |
| Document ingestion outcomes | Implemented counter |
| Deactivation drain duration/timeout | Implemented histogram |
| Worker slots, pollers, and schedule-to-start | Available from SDK when exporter is configured |
| Active store sync/remediation totals | Not instrumented |
| Sync-cancellation latency | Not instrumented |
| Complete deactivation failure counter | Not instrumented; infer only with external workflow visibility |
| Continuous Workflow History size | Not instrumented; load harness is point-in-time |
| Lakebase pool/transaction/search/cleanup batch metrics | Not instrumented by this code |
| Dashboards, exporters, thresholds, and paging routes | Not provisioned by this repository |

These gaps are production launch gates, not missing Northstar demo functionality. The App displays
authoritative state and durable demo events without requiring these metrics.

## Recommended dashboard views

After configuring an exporter, build views by namespace, Task Queue, deployment/build, provider,
quota class, operation, and bounded status where available:

1. **Worker health:** pollers, slots, Task failures, poll results, and schedule-to-start percentiles
   for both queues.
2. **Provider/quota:** provider outcomes, exhaustion, pending/in-flight permits, blocked scopes,
   grant failures, and wait percentiles.
3. **Lifecycle safety:** transitions, stale-generation rejections, ingestion outcomes, and
   deactivation drain percentiles/timeouts.
4. **Database/App:** use Lakebase and hosting-native telemetry for connections, transaction/query
   latency, HTTP error rate, and readiness failures.
5. **History/capacity:** external visibility or periodic measurement for history growth,
   Continue-As-New, pending tasks, execution duration, and throughput.

## Alert candidates

Set thresholds from production-like measurements and named SLOs. Useful conditions include:

- no healthy pollers or sustained slot exhaustion on either Task Queue;
- schedule-to-start latency above its SLO;
- quota remaining blocked or pending after its authoritative reset;
- grant Signal failures or provider authentication failures;
- elevated ingestion failures or unexpected stale-generation rejection rate;
- deactivation drain timeout or end-to-end completion beyond its SLA;
- App readiness failure, Lakebase connection saturation, or migration drift;
- Workflow History approaching target limits;
- unexpected starts of optional drain-only Workflow Types.

Exercise every alert against the actual exporter and paging route. Include a build/deployment
identity and runbook link without exposing high-cardinality customer identifiers.

## Local validation boundary

Unit/integration/load tests verify selected emitters and measurement helpers in controlled runs.
They do not configure durable telemetry, create dashboards, select thresholds, or test a target
paging system. Those actions remain part of the
[`production-readiness guide`](../architecture-production-readiness.md).

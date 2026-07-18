# Metrics, dashboards, and alerts

This page defines the observability contract for operators. The code creates application metric
instruments, but `retrieval-worker` does **not** configure a metrics exporter. A production host
must create an OpenTelemetry- or Prometheus-enabled Temporal SDK runtime before `Client.connect`.

The SDK runtime normally prefixes instruments with `temporal_`; Prometheus export can also add
counter and unit suffixes. Confirm the final names in the selected backend instead of hard-coding
the base names below.

## Application metrics

Workflow metrics use the replay-aware `workflow.metric_meter`; Activity metrics use
`activity.metric_meter`.

| Base instrument | Type | Meaning |
|---|---|---|
| `retrieval_quota_permit_requests` | Counter | Permit requests accepted, denied, or ignored |
| `retrieval_quota_permits_granted` | Counter | Permits granted |
| `retrieval_quota_grant_signal_failures` | Counter | Failed grant Signal delivery |
| `retrieval_quota_pending_requests` | Gauge | Pending permit requests |
| `retrieval_quota_in_flight` | Gauge | Reserved or in-flight permits |
| `retrieval_quota_scope_blocked` | Gauge | Quota scope blocked state, 0 or 1 |
| `retrieval_quota_wait_duration_ms` | Histogram | Request-to-grant latency |
| `retrieval_provider_requests` | Counter | Provider calls by operation and outcome |
| `retrieval_provider_quota_exhausted` | Counter | Structured provider exhaustion or 429 responses |
| `retrieval_lifecycle_transitions` | Counter | Lifecycle transition attempts and outcomes |
| `retrieval_stale_generation_rejections` | Counter | Mutations rejected by the generation fence |
| `retrieval_document_ingestion_results` | Counter | Document mutation outcomes |
| `retrieval_deactivation_drain_duration_ms` | Histogram | Deactivation drain latency and outcome |

Allowed attributes are intentionally bounded to `mutation`, `operation`, `provider`,
`quota_class`, `reason`, `status`, `transition`, and `work_class`. Raw or hashed workflow,
request, operation, store, user, credential, and cursor identifiers are excluded. Values over 64
UTF-8 bytes collapse to `other`. Metric failures must never change workflow behavior.

## Temporal SDK and Core metrics

Use SDK/Core metrics for worker and Task Queue health rather than duplicating them in application
code. Important base instruments include:

- `activity_schedule_to_start_latency` and `workflow_task_schedule_to_start_latency`;
- `activity_execution_latency`, `workflow_task_execution_latency`, and
  `workflow_task_replay_latency`;
- `worker_task_slots_available`, `worker_task_slots_used`, and `num_pollers`;
- Task poll success/empty counters and workflow completion/failure counters.

These do not replace the application quota, lifecycle, provider, or generation-fence metrics.
They also do not provide continuous per-execution Workflow History event and byte measurements.

## Coverage status

| Operational signal | Source | Status |
|---|---|---|
| Active store sync runs | — | Missing application gauge |
| Active detached remediations | — | Missing application gauge |
| Quota scopes blocked | `retrieval_quota_scope_blocked` | Implemented |
| Pending quota permits | `retrieval_quota_pending_requests` | Implemented |
| Quota permit wait duration | `retrieval_quota_wait_duration_ms` | Implemented |
| Grants per reset window | `retrieval_quota_permits_granted` | Partial: grants counted, windows not aggregated directly |
| Quota grant Signal failures | `retrieval_quota_grant_signal_failures` | Implemented |
| Provider exhaustion/429 | `retrieval_provider_quota_exhausted` | Implemented |
| Stale-generation rejections | `retrieval_stale_generation_rejections` | Implemented |
| Sync cancellation latency | — | Missing histogram |
| Deactivation drain latency | `retrieval_deactivation_drain_duration_ms` | Implemented |
| Incomplete deactivation count | Drain histogram with `status=timed_out` | Partial: no complete counter |
| Workflow History events/bytes | Opt-in load harness | Missing continuous collector |
| Activity schedule-to-start | SDK metric | Available after runtime telemetry is enabled |

Missing and partial signals are production-readiness gaps. Dashboards and alert resources are not
provisioned by this repository.

## Dashboard views

At minimum, provide these views by namespace, Task Queue, deployment/build, provider, quota class,
operation, and bounded outcome where applicable:

1. **Worker health:** pollers, slots, poll success/empty, Workflow Task failures, Activity failures,
   and schedule-to-start percentiles for both queues.
2. **Provider and quota:** provider outcomes, exhaustion, pending and in-flight permits, blocked
   scopes, wait percentiles, grants, and grant-Signal failures.
3. **Store lifecycle:** active sync/remediation counts after instrumentation is added, transitions,
   cancellation latency, deactivation drain latency, incomplete cleanup, and final outcomes.
4. **Data safety:** stale-generation rejections and document ingestion results. A successful stale
   generation commit must be structurally impossible and treated as a critical incident if
   detected by an external audit.
5. **History and capacity:** event/byte growth, Continue-As-New frequency, execution duration,
   pending tasks, throughput, and provider queue saturation.

## Alert conditions

Choose thresholds from production-like load results and service SLOs. Alert on:

- no healthy pollers or sustained slot exhaustion on either Task Queue;
- schedule-to-start latency above its SLO;
- a quota scope still blocked or growing after its authoritative reset;
- valid reset observations followed by no grants;
- grant Signal failures or provider authentication failures;
- deactivation drain or completion above its SLA;
- remediation that remains active after its store becomes inactive;
- abnormal stale-generation rejection or ingestion-failure rates;
- Workflow History approaching configured event or payload limits;
- unexpected creation of optional drain-only Workflow Types.

Exercise every alert with the production exporter and paging route before admitting customer
traffic. Include runbook links and the deployment/build identity in alert context without exposing
high-cardinality business identifiers.

## Local validation limits

The integration and load suites verify that selected metrics and latency measurements can be
observed in controlled runs. They do not configure a durable exporter, dashboard, alert, or
production threshold. Follow the [production-readiness guide](../architecture-production-readiness.md)
and [deployment runbook](../runbooks/migration-and-rollback.md) for release requirements.

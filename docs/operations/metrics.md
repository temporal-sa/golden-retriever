# Metrics, dashboards, and alerts

## Export prerequisite

Application instruments and Temporal SDK/Core built-ins share the SDK runtime telemetry
pipeline. `retrieval-worker` currently creates the default runtime, whose
`TelemetryConfig.metrics` is `None`; therefore it exports **no metrics by default**. A
production integration must construct an OpenTelemetry or Prometheus-enabled Temporal
runtime before `Client.connect`, then verify the resulting instrument names in the backend.

The runtime applies `temporal_` as its default metric prefix. The tables below show base
instrument names from code; the exporter can add that prefix and Prometheus counter/unit
suffixes according to its runtime configuration.

## Application-defined instruments

`common/metrics.py` defines the following base names. Workflow instruments use the
replay-aware `workflow.metric_meter`; Activity instruments use `activity.metric_meter`.

| Base instrument name | Type | Current emission |
|---|---|---|
| `retrieval_quota_permit_requests` | Counter | Permit request accepted or ignored |
| `retrieval_quota_permits_granted` | Counter | Permits granted |
| `retrieval_quota_grant_signal_failures` | Counter | Grant signal delivery failed |
| `retrieval_quota_pending_requests` | Gauge | Current pending permit count |
| `retrieval_quota_in_flight` | Gauge | Current reserved/in-flight permit count |
| `retrieval_quota_scope_blocked` | Gauge | Current scope blocked state, 0 or 1 |
| `retrieval_quota_wait_duration_ms` | Histogram | Request-to-grant wait in milliseconds |
| `retrieval_provider_requests` | Counter | Provider calls by operation and outcome |
| `retrieval_provider_quota_exhausted` | Counter | Structured provider quota exhaustion/429 |
| `retrieval_lifecycle_transitions` | Counter | Lifecycle transition attempts/outcomes |
| `retrieval_stale_generation_rejections` | Counter | Generation-fence rejections |
| `retrieval_document_ingestion_results` | Counter | Ingestion results by mutation/status |
| `retrieval_deactivation_drain_duration_ms` | Histogram | Deactivation drain duration and outcome |

Allowed application attributes are intentionally bounded to `mutation`, `operation`,
`provider`, `quota_class`, `reason`, `status`, `transition`, and `work_class`. Workflow,
request, operation, store, credential, cursor, and hash identifiers are excluded. String
values over 64 UTF-8 bytes collapse to `other`, and metric failures never alter business
behavior.

## Temporal SDK/Core built-ins

Do not duplicate SDK/Core instruments as application metrics. Once runtime telemetry is
enabled, use the SDK bases below for Task Queue and worker health (normally exported with
the `temporal_` prefix):

- `activity_schedule_to_start_latency` for Activity Task Queue dispatch latency;
- `workflow_task_schedule_to_start_latency` for Workflow Task Queue dispatch latency;
- `activity_execution_latency`, `workflow_task_execution_latency`, and
  `workflow_task_replay_latency` for worker processing;
- `worker_task_slots_available`, `worker_task_slots_used`, and `num_pollers` for worker
  capacity and availability;
- the SDK task poll success/empty and workflow completion/failure counters for worker and
  execution health.

The SDK built-ins do not replace application quota, lifecycle, or generation-fence
instruments. They also do not provide this repository's required per-execution Workflow
History event/byte measurements.

## Specification coverage and rollout gaps

| Required signal | Source | Status |
|---|---|---|
| Active store sync runs | — | **Gap:** no application gauge |
| Active detached remediations | — | **Gap:** no application gauge |
| Quota scopes blocked | `retrieval_quota_scope_blocked` | Implemented |
| Pending quota permits | `retrieval_quota_pending_requests` | Implemented |
| Quota permit wait duration | `retrieval_quota_wait_duration_ms` | Implemented |
| Quota grants per reset window | `retrieval_quota_permits_granted` | **Partial:** grants are counted, but reset-window totals are not recorded directly |
| Quota grant signal failures | `retrieval_quota_grant_signal_failures` | Implemented |
| Stale-generation rejections | `retrieval_stale_generation_rejections` | Implemented |
| Sync cancellation latency | — | **Gap:** no application histogram |
| Deactivation drain latency | `retrieval_deactivation_drain_duration_ms` | Implemented |
| Deactivation incomplete count | drain histogram with `status=timed_out` | **Partial:** derivable for drain timeout, but no complete incomplete-operation counter |
| Workflow History length/size | opt-in load harness only | **Gap:** no continuous production instrument/collector |
| Activity schedule-to-start latency | SDK `activity_schedule_to_start_latency` | Available only after runtime telemetry is enabled |
| Provider 429/quota exhaustion | `retrieval_provider_quota_exhausted` | Implemented |

Runtime telemetry configuration, the two missing gauges, cancellation latency, continuous
history measurement, direct reset-window accounting, and complete deactivation-incomplete
coverage are rollout gaps. Dashboards and alerts are not provisioned by this repository.

## Required dashboards and alerts

Dashboards must combine application and SDK metrics to show:

- pending/in-flight/blocked quota state, wait percentiles, grants, and grant failures;
- provider request outcomes and quota exhaustion;
- lifecycle transitions, stale-generation rejections, and ingestion outcomes;
- cancellation and deactivation drain latency/incomplete operations;
- Activity/Workflow schedule-to-start latency, pollers, and worker slot saturation;
- active sync/remediation counts and Workflow History event/byte growth after the gaps above
  are closed.

Alert when remediation survives an inactive store, a stale-generation commit succeeds,
deactivation exceeds its SLA, a pending queue grows after reset, no grant follows a valid
reset, worker pollers/slots cannot service either Task Queue, or a new `QuotaWaitWorkflow`
or `AccessioningWorkflow` appears after cutover. Every alert must be exercised against the
production exporter before enabling V2 routing.

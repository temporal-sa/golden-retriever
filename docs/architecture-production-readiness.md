# Deployment and production readiness

This page distinguishes two different claims:

- **deployable demo artifacts:** source, migrations, manifests, worker image, deterministic fixtures,
  App, and validation commands are present;
- **production launch readiness:** a specific target has passed identity, compatibility, telemetry,
  high-availability, security, recovery, and capacity gates.

The first claim applies to this repository. The second is environment-specific and has not been
established. No Databricks App, Lakebase database, or Temporal worker was deployed while preparing
this repository.

## What is built

| Capability | Repository implementation |
|---|---|
| Store command authority | `StoreControllerWorkflow` with Update-with-Start client commands |
| Retrieval orchestration | Bounded user/resource/page/document workflow hierarchy |
| Provider admission | Shared durable `UserQuotaWorkflow`, structured quota observations, bounded queue |
| Durable persistence | Async Lakebase/Postgres repository and OAuth-aware connection pool |
| Mutation safety | Same-transaction lifecycle generation checks and durable write receipts |
| Deactivation | Fence, cancel, drain, user cleanup, bounded object cleanup, inactive precondition |
| Retrieval | Deterministic chunking and current-generation Postgres full-text search |
| Demo provider/staging | Manifest-checked Northstar fixtures and cross-process scripted controls |
| Control plane | FastAPI JSON API, durable HTTP idempotency, static UI, health/readiness |
| Packaging | Databricks App DAB/root manifest and separate worker Dockerfile |
| Change safety | Unit/contract/App/demo tests, Temporal integration harness, checked-in replay histories |

The Northstar components are a controlled demonstration, not a customer-data connector. The
packaged fixtures replace a durable object store and the scripted provider replaces a real
provider API.

## Demo deployment gate

A target is eligible for a controlled demonstration only when all of these are verified there:

- [ ] A dedicated Lakebase branch/database and explicit migration, App, and worker identities
      exist.
- [ ] Core and demo migration CLIs report ready with no checksum drift.
- [ ] `retrieval-lakebase-grant-roles` has applied the reviewed App/worker grants.
- [ ] The App role can read core state/search, use demo DML, and execute only the fixed seed
      function; it cannot mutate core retrieval tables directly.
- [ ] The worker role can perform core DML and only the required demo control/event operations.
- [ ] The App bundle validates with an explicitly selected authenticated workspace profile and
      target variables.
- [ ] The independently built worker image connects and both Task Queues show pollers.
- [ ] `/healthz` and `/readyz` pass using the deployed App identity and target Temporal namespace.
- [ ] A fresh run completes quota wait, cited retrieval, `7 -> 8` fence, stale-writer rejection,
      bounded cleanup, and final zero-row state.
- [ ] Temporal Web links, outbound network rules, secret references, and log redaction have been
      checked from the presentation environment.

Local unit/contract tests and DAB schema validation are valuable build evidence, but they do not
satisfy these live checks.

## Production launch gates

A customer-facing launch adds the following requirements beyond the controlled demo.

### Real adapters and data lifecycle

Replace the fixture staging store and scripted provider with implementations that prove:

| Boundary | Production evidence |
|---|---|
| `StagingStore` | durable body availability, encryption, integrity validation, retention/deletion, retry behavior |
| `ProviderGateway` | authorization, secret rotation, stable pagination, timeouts, cancellation, structured 429/reset mapping |
| `RetrievalRepository` | contract and race tests on the exact target database/configuration, backup/restore, retention for receipts/events |

Use either one typed `RETRIEVAL_ADAPTER_BUNDLE_FACTORY` or all three individual factories. Never
enable `RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS` in a shared environment.

### Temporal compatibility

Verify in the selected namespace:

- Update-with-Start and the SDK/server versions used by the build;
- namespace retention, payload, history, rate, and concurrent-operation limits;
- deployment-based Worker Versioning before enabling it;
- typed Search Attribute registration before enabling application attributes;
- Task Queue Priority/Fairness before asserting server support.

Replay representative histories for every affected Workflow Type and path: long-running, signaled,
canceled, failed, retried, patched, and Continue-As-New. Checked-in local histories prove the
replay harness and specific compatibility branches; they are not a sample of another namespace.

Keep open executions pinned to a compatible worker. Optional drain-only workflow placeholders
cannot substitute for an absent historical implementation.

### High availability and capacity

One `retrieval-worker` process starts both retrieval and provider pollers. Run at least two
independently scheduled replicas for every live build, distributed across failure domains. Use
graceful shutdown with at least a 60-second orchestrator termination window, and confirm both
queues have pollers before admitting work.

Measure with production adapters and realistic stores:

- Workflow and Activity schedule-to-start latency;
- provider quota wait/reset recovery and authentication failure behavior;
- event-history count/bytes and Continue-As-New frequency;
- concurrent children, Activities, Signals, and quota requests;
- database pool/transaction/search latency from target monitoring;
- cleanup batch throughput, deactivation drain time, and end-to-end completion;
- throughput and fairness at expected users, resources, pages, and documents.

The included load harness measures selected Temporal mechanics. It does not establish capacity,
database SLOs, or a production maximum.

### Telemetry and incident response

The worker creates application metric instruments but does not configure an exporter. The host must
construct an OpenTelemetry- or Prometheus-enabled Temporal SDK runtime, provision dashboards and
alerts, and exercise paging routes. The exact implemented metrics and missing signals are listed in
[`operations/metrics.md`](operations/metrics.md).

Do not infer database transaction, pool, search, or cleanup-batch metrics from the presence of the
Lakebase adapter; this repository has not instrumented those as application metrics. Use target
database telemetry until explicit instruments are added.

Define structured logging, correlation, and redaction rules. Workflow IDs and hashed identities may
be logged where policy permits; credentials, document bodies, raw idempotency keys, and clear-text
connection strings may not.

### Security and recovery

The deploying organization must review and rehearse:

- resolution of the worker's versioned base-image tags to approved immutable digests for the
  release build;
- least-privilege database grants and role rotation, including the effective database `CREATE`
  privilege implied by the Databricks App `CAN_CONNECT_AND_CREATE` resource binding;
- Temporal namespace and Task Queue authorization;
- Databricks secret scope policy and outbound network controls;
- payload codec/encryption requirements;
- Lakebase backup, point-in-time recovery, and branch lifecycle;
- Temporal disaster recovery and retention;
- fixture/demo endpoint disabling outside controlled environments;
- SLOs, paging, rollback authority, and incident ownership.

## Search visibility

When `TEMPORAL_ENABLE_SEARCH_ATTRIBUTES=true`, workflow starts can attach the typed attributes in
`retrieval.temporal.common.search_attributes`. Namespace registration is external. `CurrentPhase`
is populated at start and terminal `ResultStatus`/continuous phase upserts are not implemented, so
do not build operational gates that assume those fields remain current. The App's authoritative
store state comes from Lakebase, not Temporal visibility.

## Production canary checklist

A production build is eligible for canary only when:

- [ ] real provider and staging adapters are packaged and contract-tested;
- [ ] the exact Lakebase target passes mutation, idempotency, cleanup, search, and race tests;
- [ ] representative histories replay on every build that may receive tasks;
- [ ] namespace features, limits, retention, and Search Attributes are verified;
- [ ] immutable build identity and Worker Versioning routing are tested;
- [ ] at least two healthy replicas poll both queues for every live build;
- [ ] runtime metrics export, dashboards, alerts, and paging are exercised;
- [ ] production-like integration/load results meet named SLO thresholds;
- [ ] backup/restore, secret rotation, disaster recovery, and rollback are rehearsed;
- [ ] canary ownership and stop/rollback thresholds are documented.

## Non-negotiable rollback rules

- Never route an open execution to workflow code that cannot replay its history.
- Never decrement a committed lifecycle generation.
- Resume a post-fence failed deactivation with the same generation and stable identity.
- Never refund a provider permit after an ambiguous call; wait for an authoritative observation or
  reset.
- Keep compatible workers available until visibility confirms their executions have drained.

Operational order and SQL grants are in
[`runbooks/migration-and-rollback.md`](runbooks/migration-and-rollback.md). Recorded local evidence
is in [`artifacts/verification-2026-07-18.md`](../artifacts/verification-2026-07-18.md).

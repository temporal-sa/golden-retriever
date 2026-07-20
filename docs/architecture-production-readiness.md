# Production-readiness guide

This guide explains the evidence required to move from source code to a controlled demo and then
to a customer-facing service. It does not declare any environment production-ready; readiness is a
property of a specific build, database, Temporal namespace, identities, network, and operating
team.

If you are new to the system, read the [system specification](lakebase-temporal-demo-spec.md)
before using these gates.

## Three levels of evidence

| Level | What it proves |
|---|---|
| Source verification | The code, deterministic demo, packages, and selected Temporal histories pass locally |
| Controlled-demo readiness | One named cloud environment can run the five-document Northstar scenario safely |
| Production readiness | Real adapters, target capacity, HA, telemetry, security, recovery, and release controls are proven |

Passing a lower level never implies the next level.

## Capabilities supplied by the repository

| Capability | Implementation |
|---|---|
| Store command authority | `StoreControllerWorkflow` and Update-with-Start client commands |
| Retrieval orchestration | Bounded user/resource/page/document workflow hierarchy |
| Provider admission | Shared durable quota workflow with bounded pending state |
| Persistence | Async Lakebase/Postgres repository and OAuth-aware connection pool |
| Mutation safety | Same-transaction generation checks and durable receipts |
| Deactivation | Fence, cancel, drain, user cleanup, bounded object cleanup, zero-row finish |
| Retrieval | Deterministic chunking and current-generation Postgres full-text search |
| Demo adapters | Manifest-verified fixtures and scripted quota/hold controls |
| Google Drive adapter | Read-only API client, recursive text export, shared staging, and deletion reconciliation |
| App | FastAPI API, durable HTTP idempotency, browser UI, liveness/readiness |
| Packaging | Databricks Asset Bundle, App image, and separate worker image |
| Change safety | Unit/contract/App/demo tests, integration harness, and replay histories |

Northstar fixtures still prove the deterministic demonstration, not production data integration.
The Google Drive adapter is a real provider implementation, but each deployment must separately
validate its identity, shared staging volume, source scope, retention, and representative load.

## Source verification gate

Run the exact revision and dependency lock intended for release:

```bash
make verify
make integration
uv run pytest -m replay tests/replay
docker build -f Dockerfile.worker -t retrieval-worker:<build-id> .
```

Archive the source revision, `uv.lock`, test output, replay inventory, package hashes, image digest,
and build identity. Record skipped/opt-in scenarios explicitly.

## Controlled-demo gate

A named environment is suitable for the packaged Northstar demonstration only after every item is
verified there:

- [ ] A dedicated Lakebase branch/database exists.
- [ ] Migration owner, App role, and worker role are distinct and recorded.
- [ ] Both migration checks report `ready: true` with no checksum drift.
- [ ] Reviewed App/worker grants are applied and negative privilege tests pass.
- [ ] The Databricks bundle validates with an explicit OAuth profile and target variables.
- [ ] The App contains the expected Lakebase and three Temporal secret bindings.
- [ ] The worker image has an immutable build identity and polls both Task Queues.
- [ ] `/healthz` and `/readyz` return 200 through the deployed App identity.
- [ ] A fresh run observes quota wait/resume, four citations, the `7 → 8` fence, stale-writer
      rejection, bounded cleanup, and final zero-row state.
- [ ] Secrets, network egress, log redaction, App permissions, and optional Temporal Web links are
      checked from the presentation environment.

Use the [deployment runbook](runbooks/deploy-lakebase-temporal-demo.md) for the ordered procedure.

## Production adapters and data lifecycle

Replace demo adapters with implementations that prove these contracts. The Google Drive bundle
provides one provider/staging implementation; its deployment evidence is still environment-specific.

| Boundary | Required evidence |
|---|---|
| `StagingStore` | durable availability, encryption, hash/integrity verification, retention/deletion, retries |
| `ProviderGateway` | authorization, secret rotation, stable pagination, timeout/cancellation, structured quota mapping |
| `RetrievalRepository` | target-database contract/race tests, idempotency, backup/restore, receipt/event retention |

Configure either one typed `RETRIEVAL_ADAPTER_BUNDLE_FACTORY` or all three individual factories.
Never enable `RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS` in shared or customer environments.

Define how source deletions, access revocation, retention expiration, legal holds, and failed
partial syncs propagate through staging, retrieval data, backups, and provider credentials.

## Temporal compatibility gate

Verify against the selected namespace and SDK/server versions:

- Update-with-Start support and behavior;
- namespace retention and payload/history/rate/concurrency limits;
- deployment-based Worker Versioning before enabling it;
- Search Attribute registration before enabling application attributes;
- Task Queue Priority/Fairness before asserting server support;
- authorization for the exact namespace and Task Queues.

Replay representative histories for every affected Workflow Type and path: long-running,
signaled, canceled, failed, retried, patched, and Continue-As-New. Checked-in histories prove only
their exact paths. Keep open executions routed to a compatible build.

Optional drain-only workflow names cannot replace a missing historical implementation.

## Availability and capacity gate

One worker process polls both queues. For a live service, run at least two independently scheduled
replicas of every build receiving work, distribute them across failure domains, and allow at least
60 seconds for graceful termination.

Measure with production adapters and representative stores:

- Workflow/Activity schedule-to-start and end-to-end latency;
- provider quota waits, reset recovery, and authentication failures;
- Event History count/bytes and Continue-As-New frequency;
- concurrent children, Activities, Signals, and permit requests;
- database connection, transaction, lock, and search latency;
- cleanup throughput and deactivation drain/completion time;
- throughput and fairness across realistic users/resources/pages/documents.

The synthetic load harness validates its measurement machinery. It is not a production capacity
result.

## Observability and incident-response gate

The code creates application metric instruments but does not configure an exporter. The runtime
owner must provide an OpenTelemetry- or Prometheus-enabled Temporal runtime, dashboards, alerts,
and exercised paging routes. See [metrics and observability](operations/metrics.md).

Use Lakebase and App platform telemetry for database and HTTP signals that are not instrumented by
this code. Define structured logging, correlation, retention, and redaction. Never log credentials,
document bodies, raw idempotency keys, or clear-text connection strings.

Before launch, assign owners for:

- App, worker, Temporal namespace, Lakebase, and provider incidents;
- SLOs, alert thresholds, paging, and escalation;
- rollout stop authority and rollback execution;
- migration approval and database recovery;
- security incident and secret-rotation procedures.

## Security and recovery gate

Review and rehearse:

- immutable base-image and application dependency provenance;
- least-privilege App/worker grants and service-principal rotation;
- the database-level `CREATE` implied by App `CAN_CONNECT_AND_CREATE`;
- Temporal and Databricks authorization boundaries;
- secret-scope policy and outbound network controls;
- payload codec/encryption requirements;
- Lakebase backup, point-in-time recovery, and branch lifecycle;
- Temporal retention/disaster recovery;
- disabling demo endpoints/adapters outside controlled environments;
- recovery when App, worker, Temporal, provider, or database is partially unavailable.

## Search visibility limitations

Lakebase is the authoritative lifecycle source. Search SQL joins the current store generation and
readable state, so stale/deactivating content is hidden.

When `TEMPORAL_ENABLE_SEARCH_ATTRIBUTES=true`, workflows may attach registered typed attributes.
`CurrentPhase` is set at start, but continuous phase/result upserts are not implemented. Do not
build operational gates that assume Temporal visibility mirrors the latest database state.

## Production canary gate

- [ ] Real provider and staging adapters are packaged and contract-tested.
- [ ] The exact Lakebase target passes mutation, idempotency, cleanup, search, and race tests.
- [ ] Required histories replay on every build that may receive tasks.
- [ ] Namespace features, limits, retention, and Search Attributes are recorded.
- [ ] Immutable build identity and Worker Versioning routing are rehearsed.
- [ ] At least two healthy replicas poll both Task Queues for each live build.
- [ ] Exporters, dashboards, alerts, and paging routes are exercised.
- [ ] Representative integration/load results meet named SLO thresholds.
- [ ] Backup/restore, secret rotation, disaster recovery, and rollback are rehearsed.
- [ ] Canary ownership, duration, stop thresholds, and expansion criteria are approved.

## Rollback rules that cannot be waived

- Never route an open execution to workflow code that cannot replay its history.
- Never decrement a committed lifecycle generation.
- Resume post-fence deactivation cleanup with the same generation and stable identity.
- Never refund provider capacity after an ambiguous call; wait for an authoritative observation or
  reset.
- Keep compatible workers available until their assigned executions have drained.
- Never edit an applied migration or its checksum; correct forward or restore an approved database
  recovery point.

Operational commands are in the [migration and rollback runbook](runbooks/migration-and-rollback.md).

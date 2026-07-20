# Production readiness

This document separates what the repository guarantees from what a production deployment must
provide. Read it before connecting the worker to real stores, provider credentials, or customer
data.

## Current decision

**The repository is ready for local development and architecture evaluation, but it is not a
deployable production service by itself.** A production launch is a no-go until the required
adapters, representative replay evidence, telemetry, deployment topology, and target-environment
validation described below are complete.

Temporal is a good fit for this workload: store lifecycle, bounded fan-out, provider quota waits,
cancellation, remediation, and deactivation all require durable coordination. The workflow design
contains strong correctness boundaries, but application adapters and operational infrastructure
are intentionally left to the deploying system.

## What the repository provides

- A `StoreControllerWorkflow` that serializes sync and deactivation commands for each store.
- Bounded, joined fan-out from users through resources, pages, files, and document mutations.
- Durable, shared quota coordination without holding Activity slots while callers wait.
- A generation fence that is committed before deactivation cancellation and checked atomically by
  every persistent mutation.
- Retry-safe cursors, bounded remediation, Continue-As-New boundaries, stable opaque IDs, and
  compact document references.
- Fail-closed worker startup when production adapter factories are missing.
- Unit, contract, local history replay, Temporal integration, and synthetic load scaffolding.

The latest recorded local results are in
[`artifacts/verification-2026-07-18.md`](../artifacts/verification-2026-07-18.md). They are development
evidence, not a production capacity or compatibility claim.

## Required production integrations

### Durable adapters

The included `InMemoryRetrievalRepository`, `InMemoryStagingStore`, and `EmptyProviderGateway` are
for local execution only. A deployment must supply all three interfaces through the factory
variables documented in the [deployment runbook](runbooks/migration-and-rollback.md).

The production implementations must prove these properties:

| Adapter | Required behavior |
|---|---|
| `RetrievalRepository` | Durable lifecycle and index state; atomic generation/status compare-and-write; idempotent mutations under Activity retry; safe concurrent cleanup |
| `StagingStore` | Durable body lookup by `staging_uri`; integrity validation; retention and cleanup policy; safe retry after partial failure |
| `ProviderGateway` | Authentication and credential rotation; stable pagination; timeouts and cancellation; structured 429/reset mapping; bodies staged outside Workflow History |

Contract tests should run against the same adapter packages and storage products used in the
deployment. `RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS` must remain false.

### Temporal namespace and compatibility

Before rollout, confirm the target Temporal Cloud account or self-hosted cluster supports the SDK
features used by the selected configuration:

- Update-with-Start;
- deployment-based Worker Versioning when enabled;
- the required namespace retention and payload limits;
- custom Search Attributes when enabled;
- Task Queue Priority/Fairness when enabled.

Replay representative histories for every Workflow Type that the release can process. Include
long-running, signaled, canceled, failed, retried, and Continue-As-New executions. A checked-in
local smoke history proves the replay harness works; it cannot prove compatibility with histories
from another environment or build.

If existing open executions were created by a different worker build, keep them pinned to a
compatible build. The optional `QuotaWaitWorkflow` and `AccessioningWorkflow` placeholders cannot
replay arbitrary histories whose original implementation is absent.

### Telemetry and operations

The worker does not configure a metrics exporter. The deploying application must construct the
Temporal SDK runtime with OpenTelemetry or Prometheus metrics before `Client.connect`, then
provision and test the dashboards and alerts in [`operations/metrics.md`](operations/metrics.md).

Application metrics cover important quota, provider, lifecycle, ingestion, generation-fence, and
deactivation signals, but production coverage still needs:

- active sync and remediation gauges;
- sync-cancellation latency;
- direct grants-per-reset-window accounting;
- complete incomplete-deactivation accounting;
- continuous Workflow History event and byte measurement.

Every alert must be exercised against the production exporter. Metric labels and logs must not
contain raw store keys, user keys, credentials, cursors, or request IDs.

### Deployment topology and capacity

`retrieval-worker` starts the retrieval and provider pollers in one process. Run at least two
independently scheduled replicas for every live build so neither Task Queue depends on one
process. Distribute replicas across failure domains, use graceful shutdown, and verify readiness
for both pollers.

Run the full integration topology and a representative load scenario against a production-like
namespace using production adapters. Establish release thresholds for:

- Workflow and Activity schedule-to-start latency;
- provider quota wait and reset recovery latency;
- Workflow History event counts and bytes;
- pending children, Activities, Signals, and quota requests;
- deactivation drain time and incomplete operations;
- throughput and fairness at the expected number of stores, users, resources, and documents.

The synthetic load harness is useful for validating measurement mechanics. It does not establish
production SLOs or maximum capacity.

### Security, recovery, and ownership

The deploying organization must define and test:

- namespace and Task Queue access controls;
- secret storage, rotation, and provider credential authorization;
- payload codec or encryption requirements;
- log and metric redaction;
- database and object-store backup/restore;
- Temporal disaster-recovery strategy and retention;
- SLOs, paging routes, incident ownership, and rollback time objectives.

## Search visibility

When `TEMPORAL_ENABLE_SEARCH_ATTRIBUTES=true`, workflow starts can attach the typed attributes
listed in [`IMPLEMENTATION_MAP.md`](../IMPLEMENTATION_MAP.md). The namespace registration is
external to this repository. `ResultStatus` terminal upserts and ongoing `CurrentPhase` updates
are not implemented, so do not build operational gates that assume those fields remain current.

## Release gate

A production release is eligible for canary only when every item below is true:

- [ ] All three production adapters are packaged, configured together, and contract-tested.
- [ ] Atomic generation fencing and at-least-once idempotency are demonstrated on production
      storage.
- [ ] Representative histories pass replay against every build that can receive their tasks.
- [ ] Target namespace features, limits, retention, and Search Attributes are verified.
- [ ] Immutable deployment/build identities and Worker Versioning routing are validated.
- [ ] At least two healthy replicas poll each live Task Queue and build.
- [ ] Runtime metrics export, dashboards, paging routes, and alerts are provisioned and tested.
- [ ] Production-like integration and load results meet documented SLO thresholds.
- [ ] Security, backup/restore, disaster recovery, and secret rotation are rehearsed.
- [ ] The canary and rollback procedures in the deployment runbook have named owners.

## Non-negotiable rollback invariants

- Do not route an open execution to workflow code that cannot replay its history.
- Do not decrement a committed lifecycle generation.
- Resume a failed deactivation with the same generation and stable operation identity.
- Do not refund a provider permit whose outcome is ambiguous; wait for an authoritative quota
  observation or reset.
- Keep compatible workers available until visibility confirms their open executions have drained.

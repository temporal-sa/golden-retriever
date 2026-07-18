# Architecture production-readiness audit

- Review date: 2026-07-18
- Scope: `temporal-retrieval-v2-codex-spec.md` and the current repository
- Method: Temporal workflow design critic, prioritizing correctness, operability, and
  production readiness

## Verdict

- Status: **needs_revision**
- Production decision: **NO-GO**
- Temporal fit: **appropriate** for durable lifecycle, fan-out, quota, cancellation, and
  deactivation coordination.

The workflow architecture is broadly sound, and the local test suite gives useful
correctness coverage. Production rollout is nevertheless blocked by two missing inputs:
deployable production adapters and representative production histories. The remaining high
findings prevent a safe canary from being evaluated against the specification's operational
acceptance criteria.

## Critical findings

### C1. Production adapters are absent — rollout blocker

- Evidence: `activities/repositories.py` provides only `InMemoryRetrievalRepository` and
  `InMemoryStagingStore`; `activities/provider_api.py` provides only an
  `EmptyProviderGateway`. Their docstrings explicitly limit them to local/test use.
- Evidence: `worker.py::_load_adapters` refuses startup unless all three production factory
  variables are configured, except when the unsafe local-adapter flag is explicitly enabled.
- Risk: there is no durable lifecycle authority, staging implementation, or real provider
  integration available from this repository. Enabling the unsafe adapters would lose state
  on process restart and perform no provider retrieval.
- Required closure: implement, package, and contract-test production `RetrievalRepository`,
  `StagingStore`, and `ProviderGateway` adapters. Prove atomic generation compare-and-write,
  at-least-once idempotency, staging durability/integrity, provider authentication, 429
  mapping, and cancellation behavior. Configure the exact factories documented in the
  migration runbook and keep `RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS` false in production.

### C2. No production replay baseline exists — rollout blocker

- Evidence: `artifacts/histories/` contains only `.gitkeep`.
- Evidence: `tests/replay/test_exported_histories.py` registers the workflow inventory but
  skips when no JSON histories are present; a green default test run therefore does not prove
  replay compatibility.
- Evidence: `workflows/legacy.py` states that its placeholders cannot replay real legacy
  histories and that compatible old worker builds must remain pinned.
- Risk: changed workflow code cannot be shown deterministic for open executions, and the
  correct build-to-execution routing cannot be validated before cutover.
- Required closure: inventory every modified production Workflow Type, export representative
  histories (including long-running, signaled, canceled, failed, and Continue-As-New cases),
  make missing required fixtures a release-gate failure, replay them against the exact build
  that will process them, and verify old histories stay pinned to compatible old builds.

## High findings

### H1. Required rollout telemetry is incomplete and not exported

- Evidence: `common/metrics.py` and selected workflow/activity call sites emit a useful subset
  of bounded-cardinality quota, provider, lifecycle, ingestion, and deactivation metrics.
  However, active sync/remediation, sync cancellation, continuous workflow-history, direct
  reset-window totals, and complete deactivation-incomplete coverage remain missing or
  partial. Schedule-to-start is supplied by SDK/Core metrics rather than application code.
- Evidence: worker startup does not configure a production SDK telemetry exporter, and the
  dashboards and alerts remain prose rather than provisioned, exercised artifacts.
- Risk: the canary gates for stale-generation success, quota recovery, cancellation/drain
  latency, history growth, and legacy-workflow creation cannot currently be enforced.
- Required closure: wire SDK/runtime and application metrics to the production telemetry
  backend; provision the required dashboards and alerts; verify labels are bounded and do not
  expose credentials or store identifiers; exercise every alert before rollout.

### H2. Production-scale Temporal behavior has not been demonstrated

- Evidence: `artifacts/verification-2026-07-18.md` retains a green ephemeral-server integration
  and small synthetic load run. The load harness uses synthetic signal/fairness workflows
  rather than a production-scale full V2 topology, and it does not exercise production
  adapters.
- Risk: quota reset fan-out, pending child/activity counts, real history growth, P50/P95/P99
  resume latency, and fairness under expected load remain inconclusive.
- Required closure: run the integration and full-topology suites against a production-like
  namespace with the production adapters, then run a representative V2 load scenario and
  record thresholds/results as a release artifact.

### H3. Deployment/versioning and HA topology are not materialized

- Evidence: the worker supports pinned deployment-based versioning, but it is disabled by
  default. The repository has no deployment manifest, replica policy, readiness probe, or
  evidence for the target namespace capabilities.
- Risk: one process failure can stop both Task Queue pollers, or an incorrect build/behavior
  assignment can strand pinned executions.
- Required closure: set unique deployment/build identities, enable Worker Versioning only
  after capability verification, deploy at least two independent replicas for every live
  build and Task Queue, test graceful drain, and rehearse rollback. The minimum topology is
  now explicit in the migration runbook.

## Medium findings

### M1. Search visibility is attached but not fully operationalized

- When enabled, typed Search Attributes are attached directly to controller, sync,
  remediation, and deactivation starts. The feature remains off by default, namespace
  registration is external, `ResultStatus` is not upserted, and the initial `CurrentPhase`
  values are not advanced by the current implementations.
- Register the attributes in the target namespace, enable them deliberately, add the required
  bounded status/phase updates, and verify the operational queries required by the
  specification.

### M2. A very large failed-user cohort still crosses one payload boundary

- `FailedUserRemediationWorkflow` admits activation children in bounded batches, samples at
  most 100 error labels, and can Continue-As-New between drained batches. However,
  `RootSyncWorkflow` still carries the complete deduplicated failed-user tuple and supplies it
  in the initial detached-remediation input.
- Establish the production failure-cohort envelope before rollout. If it can approach the
  namespace payload limit, persist failures behind an opaque, paged reference or stream
  bounded idempotent batches into the stable remediation execution instead of carrying the
  complete tuple in Workflow input/result payloads.

## Unknown or inconclusive items

- Target platform and version: Temporal Cloud versus self-hosted, retention, namespace limits,
  Worker Versioning/Update-with-Start support, and Priority/Fairness configuration.
- Existing production inventory: Workflow Types, open execution counts, legacy task queues,
  current build assignments, and representative history selection.
- Scale envelope: peak starts, signals, activities, users/resources/documents per execution,
  maximum duration, payload sizes, quota scopes, and acceptable backlog.
- Production adapter behavior: database transaction/isolation model, object-store consistency
  and cleanup, provider timeout/SLA details, secret rotation, and idempotency keys.
- Security and recovery: payload encryption/codec requirements, disaster recovery, backup
  restore testing, namespace access controls, and credential handling.
- Operational ownership: final SLO thresholds, paging routes, on-call runbooks, capacity
  headroom, and tested rollback time.

## Strengths

- Stable, opaque business IDs; Run ID is not used as business identity.
- Deterministic workflow code with external effects isolated in regular Activities.
- Bounded fan-out and explicit Continue-As-New boundaries control history growth.
- Generation fencing precedes cancellation and protects late at-least-once writes.
- Shared quota state parks callers with workflow conditions instead of consuming worker slots;
  provider 429s are structured observations rather than ordinary retries.
- Detached work uses explicit stable ownership and Parent Close Policy semantics.
- Activity timeout/retry policies, compact document references, capability-gated fairness,
  bounded metric attributes, and fail-closed adapter startup are sensible production-safety
  defaults.
- Unit, replay, integration, and load scaffolding exists and the worker registry is explicit.

## Deploy checklist

- [ ] Close C1: ship and contract-test all three production adapters; unsafe adapters are
  prohibited.
- [ ] Close C2: replay the required exported histories against the intended worker builds and
  enforce fixture completeness in the release gate.
- [ ] Confirm target namespace capabilities, retention, custom Search Attributes, quotas, and
  Priority/Fairness settings.
- [ ] Set `TEMPORAL_DEPLOYMENT_NAME`, a unique immutable `TEMPORAL_BUILD_ID`, and
  `TEMPORAL_USE_WORKER_VERSIONING=true`; verify pinned routing before admitting V2 work.
- [ ] Run at least two independently scheduled replicas for each live deployment build and
  each Task Queue; verify readiness and graceful shutdown/drain.
- [ ] Execute production-like integration and load tests; archive history, latency, fairness,
  quota-reset, and pending-work results with agreed SLO thresholds.
- [ ] Enable and validate bounded-cardinality metrics, dashboards, and alerts.
- [ ] Start a deterministic canary cohort with the old entry path still available. Expand only
  while every runbook health gate holds.

## Rollback checklist

- [ ] Disable only new V2 routing; do not move open executions across incompatible builds.
- [ ] Keep at least two healthy V2 replicas for pinned V2 executions and two compatible old
  replicas for old executions until each cohort drains.
- [ ] Do not decrement an already committed lifecycle generation; resume failed deactivation
  with the same generation and stable operation identity.
- [ ] Do not refund ambiguous provider permits; wait for an authoritative observation/reset.
- [ ] Retain legacy registrations and replay artifacts through namespace retention plus the
  operational replay window, and remove them only after visibility proves zero open work.

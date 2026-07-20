# Deployment, upgrade, and rollback runbook

This runbook is for operators packaging `retrieval-worker` for a real Temporal namespace. It
covers initial deployment and later upgrades. It assumes the production-readiness release gate is
owned by the deploying organization; this repository does not include infrastructure manifests.

## Before deployment

1. Implement and contract-test production `RetrievalRepository`, `StagingStore`, and
   `ProviderGateway` adapters.
2. Build an immutable worker artifact containing the adapters and record its source revision and
   `TEMPORAL_BUILD_ID`.
3. Confirm the target namespace supports the required SDK features, retention, and limits.
4. Register custom Search Attributes if search visibility will be enabled.
5. Replay representative histories against every worker build that may process them.
6. Configure SDK runtime telemetry, dashboards, alerts, and log redaction.
7. Allocate at least two independently scheduled worker replicas per live build.
8. Agree on canary health thresholds, rollback authority, and incident ownership.

Do not deploy with the local in-memory adapters, an untested mutable build ID, or a single worker
replica.

## Configure production adapters

The worker loads zero-argument factories from importable Python modules. Configure the complete
set:

```text
RETRIEVAL_REPOSITORY_FACTORY=package.module:create_repository
RETRIEVAL_STAGING_STORE_FACTORY=package.module:create_staging_store
RETRIEVAL_PROVIDER_GATEWAY_FACTORY=package.module:create_provider_gateway
```

Each factory may be synchronous or asynchronous and must return the corresponding interface
implementation. It reads database, object-store, provider, and secret settings from the deployment
environment; the worker passes no arguments.

The factory set is all-or-nothing. Missing or partial production configuration fails startup.
Leave `RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS` unset or `false` in every shared environment.

## Configure the Temporal connection

At minimum, set values appropriate to the target namespace:

```text
TEMPORAL_ADDRESS=<frontend-address>
TEMPORAL_NAMESPACE=<namespace>
TEMPORAL_API_KEY=<secret-if-required>
TEMPORAL_TLS=true
TEMPORAL_RETRIEVAL_TASK_QUEUE=<retrieval-queue>
TEMPORAL_PROVIDER_TASK_QUEUE=<provider-queue>
TEMPORAL_DEPLOYMENT_NAME=<stable-deployment-name>
TEMPORAL_BUILD_ID=<unique-immutable-build-id>
TEMPORAL_USE_WORKER_VERSIONING=true
```

Configure provider queue RPS and workflow tuning only from measured provider limits and load-test
results. If Priority/Fairness is enabled, both
`TEMPORAL_SERVER_PRIORITY_FAIRNESS_SUPPORTED=true` and
`TEMPORAL_ENABLE_PRIORITY_FAIRNESS=true` must reflect verified server capability.

Search Attributes are optional. Register the typed schema before setting
`TEMPORAL_ENABLE_SEARCH_ATTRIBUTES=true`; otherwise workflow starts can fail.

## Verify the artifact before traffic

Run these checks against the exact artifact or source revision to be deployed:

```bash
uv run ruff check .
uv run ruff format --check .
uv run python -m compileall -q src tests
uv run pytest
RUN_TEMPORAL_INTEGRATION=1 uv run pytest -m integration tests/integration
uv run pytest -m replay tests/replay
```

For a production-like namespace, set the suite-specific address, namespace, and credential
variables described in the test README files. Archive test output, replay inputs, build identity,
and load/SLO results as release evidence.

## High-availability worker layout

One `retrieval-worker` process starts both a retrieval-queue worker and a provider-queue worker.
Run at least two independent process replicas for each live build. Confirm both Task Queues have
pollers before marking a replica ready.

During an upgrade, this minimum applies to every build that still owns pinned executions. Use
graceful shutdown so pollers stop accepting work before the process exits. If the queues are later
split into separate services, keep at least two replicas per Task Queue and live build.

## Initial rollout

1. Deploy the worker replicas without sending application commands.
2. Verify adapter connectivity, both Task Queue pollers, telemetry export, build identity, and
   Worker Versioning routing.
3. Run a non-customer smoke store through sync, cancellation, and deactivation. Confirm the
   lifecycle generation and final repository state.
4. Admit a small deterministic canary cohort through `RetrievalClient`.
5. Watch schedule-to-start latency, worker slots and pollers, provider outcomes, quota pending and
   reset behavior, stale-generation rejections, remediation, deactivation drain, and history size.
6. Expand only while the documented health thresholds remain satisfied.

Do not start application workflows directly to bypass controller serialization.

## Upgrade an existing deployment

1. Inventory open executions by Workflow Type, Task Queue, and assigned build.
2. Export representative histories from every affected cohort and replay them against the new
   artifact.
3. Register the new immutable build in the existing deployment and start at least two replicas.
4. Confirm the new build polls both queues and passes a non-customer smoke execution.
5. Route a deterministic canary cohort to the new build using the target namespace's supported
   Worker Versioning procedure.
6. Observe the canary for at least the agreed maximum workflow and quota-reset windows.
7. Promote gradually. Keep the prior compatible build healthy until all executions assigned to it
   have completed or Continue-As-New onto an explicitly compatible path.

If histories require Workflow Types not used by new executions, set
`TEMPORAL_REGISTER_LEGACY_DRAIN_TYPES=true` only on builds that contain compatible implementations.
The placeholders in this repository are not sufficient for arbitrary external histories.

## Roll back admission

Rollback changes routing for new work; it does not rewrite Workflow Event History.

1. Stop assigning new executions to the affected build or disable the application entry path.
2. Keep at least two healthy replicas of every build that owns open pinned executions.
3. Restore new-work routing to the last verified compatible build.
4. Confirm controller commands, provider queues, quota recovery, and lifecycle metrics recover.
5. Preserve the failed build's histories and telemetry for replay and diagnosis.

Never move an open execution to code that cannot replay its history.

## Recover an incomplete deactivation

Determine whether the generation fence committed:

- **Before the fence:** the controller can return to `ACTIVE` or `SYNCING`; retry the command after
  resolving the failure.
- **After the fence:** the store is fenced even if cleanup failed. Keep the generation unchanged,
  record `DEACTIVATION_FAILED`, correct the dependency, and resume with the same generation and
  stable deactivation Workflow ID.

Do not reactivate the store by decrementing its generation. Do not treat Activity cancellation as
the safety mechanism; repository compare-and-write is authoritative.

## Recover provider quota coordination

When a provider call outcome is ambiguous, do not refund its permit. Allow the next authoritative
quota observation or reset time to restore capacity. Investigate a scope that remains blocked
beyond its reset window using the quota workflow state, provider response metadata, and bounded
metrics—never by logging raw credentials or quota keys.

## Remove a worker build

Remove a build or compatibility registration only after all of the following are true:

- Temporal visibility shows zero open executions assigned to it;
- namespace retention plus the organization's replay window has elapsed where required;
- representative retained histories replay on the builds expected to handle them;
- rollback owners agree that the build is no longer part of the recovery plan.

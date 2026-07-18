# Migration and rollback runbook

## Preconditions

1. Implement and contract-test production `RetrievalRepository`, `StagingStore`, and
   `ProviderGateway` adapters. Do not use the included in-memory/empty adapters outside local
   development.
2. Export representative histories for every existing Workflow Type and pass replay tests.
3. Confirm the deployed Temporal Server/Cloud supports deployment-based Worker Versioning,
   Update-with-Start, and—only if enabled—Task Queue Priority/Fairness.
4. Register the custom Search Attributes documented in `IMPLEMENTATION_MAP.md`.
5. Keep old Workflow and Activity registrations on pinned old worker deployments.
6. Allocate at least two independently scheduled worker replicas for each live deployment
   build and Task Queue, distributed across failure domains where the platform permits.

## Production adapter bootstrap

The worker loads adapters with zero-argument factories. Configure all three variables; the
set is all-or-nothing:

```text
RETRIEVAL_REPOSITORY_FACTORY=importable.module:factory_function
RETRIEVAL_STAGING_STORE_FACTORY=importable.module:factory_function
RETRIEVAL_PROVIDER_GATEWAY_FACTORY=importable.module:factory_function
```

Each factory may be synchronous or asynchronous and must return, respectively, a
`RetrievalRepository`, `StagingStore`, or `ProviderGateway` implementation. The factory must
read its database, object-store, provider, and secret configuration from its own deployment
environment; the worker passes no arguments. The module must be importable in the worker image.

Leave `RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS` unset or `false` in production. With the
flag false, a missing factory set fails worker startup. A partial factory set always fails
startup, even when the unsafe flag is enabled. The unsafe flag permits the complete set of
non-durable local fallbacks only when none of the production factories is configured.

## High-availability worker minimum

The `retrieval-worker` entry point starts one Temporal Worker on
`TEMPORAL_RETRIEVAL_TASK_QUEUE` and one on `TEMPORAL_PROVIDER_TASK_QUEUE` in the same process.
Run at least **two independent process replicas per live deployment build** so that each queue
has at least two pollers and no single process is an availability boundary. If the queues are
later split into separate deployments, run at least two replicas per Task Queue and build.

During rollout and rollback, this minimum applies independently to every build that still owns
pinned executions: keep at least two compatible old replicas while old executions drain and at
least two V2 replicas while V2 executions remain open. Use graceful shutdown and confirm both
Task Queue pollers are ready before a replica receives a ready signal.

## Rollout

1. Deploy lifecycle-generation fields and conditional mutation activities with V2 routing
   disabled.
2. Deploy at least two V2 worker replicas as a new deployment version; keep at least two
   compatible old replicas available for pinned old executions.
3. Enable the store-controller entry path for a small deterministic cohort.
4. Enable shared quota coordination behind the current admission flag; verify permit wait,
   429 scope blocking, cancellation, and reset metrics.
5. Enable direct quota waiting for new V2 executions. Assert that no new
   `QuotaWaitWorkflow` or `AccessioningWorkflow` executions appear.
6. Separately enable priority/fairness after confirming Server/Cloud capability.
7. Expand the cohort only while stale-generation successes remain zero and deactivation,
   quota-resume, schedule-to-start, and history-size SLOs hold.

## Rollback

- Disable new V2 routing; this affects only new operations.
- Leave at least two V2 worker replicas running for pinned V2 executions and at least two old
  replicas running for old executions. Do not move an open execution across incompatible
  command sequences.
- Do not roll back an already committed lifecycle generation. If deactivation cannot
  finish, record `DEACTIVATION_FAILED` and resume cleanup with the same generation.
- Do not refund ambiguous provider permits. Let the next authoritative quota observation or
  reset restore availability.
- Retain removed-type registrations through namespace retention plus the operational replay
  window. Remove them only after visibility confirms zero open executions.

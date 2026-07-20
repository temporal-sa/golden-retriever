# Codebase guide

This guide maps the runtime design to the source tree. It is the reference for finding a workflow,
understanding its ownership boundary, and configuring a worker process.

## Package layout

All application code lives under `src/retrieval/`:

| Path | Purpose |
|---|---|
| `config.py` | Validated workflow concurrency, quota, timeout, and fairness settings |
| `temporal/client.py` | Application-facing `RetrievalClient` and controller commands |
| `temporal/runtime_config.py` | Temporal connection, Task Queue, deployment, and adapter settings |
| `temporal/worker.py` | Workflow and Activity registration; worker process entry point |
| `temporal/test_starter.py` | Executable local sync/deactivation smoke test |
| `temporal/models/` | Serializable workflow inputs, results, lifecycle state, quota state, and document references |
| `temporal/common/` | Deterministic IDs, scheduling priorities, metrics, Search Attributes, and quota waiting |
| `temporal/activities/` | Provider, persistence, ingestion, cleanup, lifecycle, and quota-bridge Activities |
| `temporal/workflows/` | The registered workflow implementations |

Tests are divided into fast unit/contract tests, opt-in Temporal integration tests, exported
history replay, and an opt-in synthetic load harness.

## Runtime entry points

| Entry point | Use |
|---|---|
| `uv run retrieval-worker` | Run the retrieval and provider workers against a Temporal namespace |
| `uv run retrieval-test-starter` | Start isolated local workers and verify sync plus deactivation |
| `RetrievalClient` | Submit idempotent sync, cancellation, and deactivation commands from Python |

`retrieval-worker` starts two Temporal `Worker` instances in one process. The retrieval worker
hosts every Workflow Type and all persistence-facing Activities. The provider worker isolates
provider API calls on a separately rate-limited Task Queue.

## Workflow inventory

`temporal/worker.py::V2_WORKFLOW_TYPES` is the authoritative registry. The replay registry mirrors
these 17 Workflow Types.

| Workflow Type | Responsibility | Completion relationship |
|---|---|---|
| `StoreControllerWorkflow` | One durable lifecycle and operation owner per store | Long-lived; starts detached operations |
| `RootSyncWorkflow` | Enumerate users in ordinary pages or bounded rounds; aggregate progress | Detached from controller; joins user work |
| `FailedUserRemediationWorkflow` | Retry failed user batches and report durable ownership | Detached from root; tracked by controller |
| `ActivateUserWorkflow` | Run recent sync, revalidate generation, run backfill, activate user | Joined by remediation |
| `UserSyncWorkflow` | Fan out over configured resource types | Joined by root or activation |
| `ResourceSyncWorkflow` | Own one resource cursor and page policy | Joined by user sync |
| `ResourcePagesWorkflow` | Fetch provider pages with a sliding child window and safe checkpoint | Joined by resource sync |
| `FilesPageWorkflow` | Fan out document upserts and deletions for one provider page | Joined by resource pages |
| `DocumentIngestionWorkflow` | Run one staged, generation-fenced document mutation | Joined by files page |
| `CommentsResyncWorkflow` | Direct comments-resource entry that delegates to resource sync | Joins resource sync |
| `UserQuotaWorkflow` | Coordinate FIFO provider permits for one external quota scope | Shared, long-lived workflow |
| `DeactivateStoreWorkflow` | Fence, cancel, drain, clean up, and finish store deactivation | Detached from controller |
| `CleanupUsersWorkflow` | Deactivate users in bounded batches | Joined by deactivation |
| `DeactivateUserWorkflow` | Route one user cleanup to its mutation child | Joined by cleanup-users |
| `DeactivateOneUserWorkflow` | Perform one user deactivation Activity | Joined |
| `DeactivateAllUsersWorkflow` | Perform all-user deactivation | Joined |
| `RemoveObjectsWorkflow` | Remove remaining retrieval objects | Joined by deactivation |

`QuotaWaitWorkflow` and `AccessioningWorkflow` exist only as optional drain-only registrations.
No primary execution path starts them. Set `TEMPORAL_REGISTER_LEGACY_DRAIN_TYPES=true` only when a
compatible deployment must continue polling histories that contain those names; the placeholders
in this repository are not substitutes for the original workflow code.

## Activity and adapter boundaries

Workflow code performs no direct network, database, filesystem, or clock-dependent side effects.
Activities call three injected ports:

| Port | Production responsibility | Included local implementation |
|---|---|---|
| `RetrievalRepository` | Authoritative lifecycle, generation, user, retrieval-state, and document mutations | `InMemoryRetrievalRepository` |
| `StagingStore` | Durable document-body lookup by `staging_uri` | `InMemoryStagingStore` |
| `ProviderGateway` | User listing, resource pagination, authentication, and quota error mapping | `EmptyProviderGateway` |

Production repository methods must compare the expected generation and allowed lifecycle state in
the same transaction as every write. Provider implementations stage bodies outside Temporal and
return compact `DocumentRef` values. They must translate provider exhaustion into
`ProviderQuotaExhausted` so the workflow can update shared quota state.

The worker loads production implementations through zero-argument `module:function` factories.
All three factories are required together; partial configuration always fails startup.

## Commands and controller state

`RetrievalClient` uses Update-with-Start so the first command can atomically create the controller.
Each command has a stable `command_id` used for bounded deduplication.

| Command | Accepted when | Result |
|---|---|---|
| `request_sync` | Generation matches; store is active; no sync, remediation, or deactivation is active | `OperationAccepted` for a stable root-sync ID |
| `cancel_sync` | The operation belongs to the store controller | `CancellationAccepted` |
| `start_deactivation` | Generation matches and no deactivation is active | `OperationAccepted` for the generation-derived deactivation ID |
| `get_status` | Controller exists | `StoreControllerSnapshot` |

The controller lifecycle states are `ACTIVE`, `SYNCING`, `DEACTIVATING`, `INACTIVE`, and
`DEACTIVATION_FAILED`. A store stays `SYNCING` until both its root sync and all detached
remediations finish.

## Sync command policy

`RetrievalClient.request_sync` copies validated process configuration into string-valued
`SyncCommand.metadata` unless the caller already supplied a value. The controller parses that
metadata into the typed root input.

| Metadata key | Default | Purpose |
|---|---|---|
| `mode` | `ordinary` | `ordinary` processes each user page as a barrier; `round` gives active users bounded page slices |
| `resource_types` | `files` | Comma-separated resource names processed for each user |
| `provider` + `credential_key` | unset | Together enable a shared quota scope; `credential_key` is opaque identity, never a secret |
| `quota_class` | `default` | Provider quota bucket within the credential scope |
| `fairness_weight` | `1` | Relative scheduling weight from 0.001 through 1000 when Priority/Fairness is active |
| `max_active_users` | `STORE_SYNC_MAX_ACTIVE_USERS` | Ordinary-mode concurrent user children |
| `user_page_size` | `STORE_SYNC_USER_PAGE_SIZE` | Active users requested per provider call |
| `round_user_window_size` | `ROUND_USER_WINDOW_SIZE` | Users admitted to one round |
| `round_page_slice_size` | `ROUND_PAGE_SLICE_SIZE` | Pages attempted per active user per round |
| `resource_concurrency` | `RESOURCE_CONCURRENCY` | Concurrent resource children per user |
| `files_page_window_size` | `FILES_PAGE_WINDOW_SIZE` | Concurrent page children per resource |
| `files_per_page_concurrency` | `FILES_PER_PAGE_CONCURRENCY` | Per-page ceiling for document children |
| `document_ingestion_concurrency` | `DOCUMENT_INGESTION_CONCURRENCY` | Second per-page document ceiling; the lower ceiling is used |
| `provider_page_size` | 100 | Requested resource-page size |
| `activation_recent_page_cap` | 5 | Recent pages attempted before activation backfill |

The client supplies `provider_task_queue` and the capability-gated `priority_fairness_enabled`
value. Callers should not override them unless they intentionally operate a compatible queue and
server configuration. All integer policy values must be positive. If either `provider` or
`credential_key` is absent, the sync performs provider calls without the shared quota coordinator.

## Stable IDs and Task Queues

Business-derived ID components are hashed with the centralized SHA-256/base32 helper in
`common/ids.py`. Temporal Run ID is never used as business identity.

| Concern | Convention |
|---|---|
| Controller | `store-controller/{opaque-store}` |
| Root sync | `store-sync/{opaque-store}/{generation}/{opaque-sequence}` |
| Remediation | `failed-user-remediation/{opaque-store}/{generation}/{opaque-sequence-or-partition}` |
| User/resource/page/document children | Stable opaque IDs derived from their logical inputs |
| Deactivation | `store-deactivation/{opaque-store}/{generation}` |
| Shared quota | `user-quota/{opaque-provider-credential-class}` |
| Retrieval Task Queue | `TEMPORAL_RETRIEVAL_TASK_QUEUE`, default `retrieval-v2` |
| Provider Task Queue | `TEMPORAL_PROVIDER_TASK_QUEUE`, default `retrieval-provider-v2` |

Detached sync, remediation, and deactivation starts use stable IDs and await the server's start
acknowledgement. The controller owns their durable registrations. Joined child workflows use
bounded concurrency and explicit cancellation behavior.

## Runtime configuration

Connection, deployment, and adapter settings come from `TemporalRuntimeConfig`:

| Environment variable | Default | Meaning |
|---|---|---|
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal frontend address |
| `TEMPORAL_NAMESPACE` | `default` | Namespace for all workflows, including quota coordinators |
| `TEMPORAL_API_KEY` | unset | Temporal Cloud/API credential |
| `TEMPORAL_TLS` | true when API key is set | Enable TLS |
| `TEMPORAL_RETRIEVAL_TASK_QUEUE` | `retrieval-v2` | Workflow and persistence Activity queue |
| `TEMPORAL_PROVIDER_TASK_QUEUE` | `retrieval-provider-v2` | Provider Activity queue |
| `TEMPORAL_DEPLOYMENT_NAME` | `retrieval-v2` | Worker Deployment name |
| `TEMPORAL_BUILD_ID` | `local` | Immutable build identifier |
| `TEMPORAL_USE_WORKER_VERSIONING` | `false` | Enable pinned deployment-based Worker Versioning |
| `TEMPORAL_REGISTER_LEGACY_DRAIN_TYPES` | `false` | Register optional drain-only workflow names |
| `TEMPORAL_SERVER_PRIORITY_FAIRNESS_SUPPORTED` | `false` | Assert server support for Priority/Fairness |
| `TEMPORAL_ENABLE_SEARCH_ATTRIBUTES` | `false` | Attach the project's typed Search Attributes |
| `RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS` | `false` | Permit local non-durable adapters |
| `RETRIEVAL_REPOSITORY_FACTORY` | unset | `module:function` repository factory |
| `RETRIEVAL_STAGING_STORE_FACTORY` | unset | `module:function` staging-store factory |
| `RETRIEVAL_PROVIDER_GATEWAY_FACTORY` | unset | `module:function` provider factory |

Workflow tuning comes from `RetrievalTemporalConfig`:

| Environment variable | Default | Meaning |
|---|---:|---|
| `STORE_SYNC_MAX_ACTIVE_USERS` | 20 | Ordinary-mode user child concurrency |
| `STORE_SYNC_USER_PAGE_SIZE` | 100 | Users requested per provider page |
| `ROUND_USER_WINDOW_SIZE` | 20 | Active users per round-mode window |
| `ROUND_PAGE_SLICE_SIZE` | 5 | Pages attempted per active user and round |
| `RESOURCE_CONCURRENCY` | 8 | Resources processed concurrently per user |
| `FILES_PAGE_WINDOW_SIZE` | 5 | Page children admitted concurrently |
| `FILES_PER_PAGE_CONCURRENCY` | 10 | Per-page document concurrency ceiling |
| `DOCUMENT_INGESTION_CONCURRENCY` | 20 | Second per-page document ceiling; the lower value is used |
| `USER_QUOTA_MAX_IN_FLIGHT` | 4 | Provider permits concurrently reserved per scope |
| `USER_QUOTA_MAX_PENDING_REQUESTS` | 350 | Pending permit ceiling; cannot exceed 350 |
| `USER_QUOTA_DEDUP_WINDOW_SIZE` | 2,000 | Recent terminal permit IDs retained |
| `USER_QUOTA_CONTINUE_AS_NEW_MESSAGE_COUNT` | 10,000 | Message-count rollover threshold |
| `DEACTIVATION_DRAIN_TIMEOUT` | `5m` | Maximum wait for owned operation drain |
| `TEMPORAL_ENABLE_PRIORITY_FAIRNESS` | `false` | Enable SDK scheduling metadata when server support is also asserted |
| `TEMPORAL_PROVIDER_QUEUE_RPS` | unset | Worker-side provider Task Queue Activity rate limit |
| `TEMPORAL_FAIRNESS_KEY_RPS_DEFAULT` | unset | Documented server-side per-key target; logged but not enforced by the SDK |

Invalid booleans, non-positive limits, unsafe quota relationships, and invalid durations fail
configuration loading.

## Search Attributes

When the namespace attributes are registered and `TEMPORAL_ENABLE_SEARCH_ATTRIBUTES=true`, starts
can include `StoreKeyHash`, `LifecycleGeneration`, `OperationType`, `CurrentPhase`, `SyncSequence`,
`Provider`, `QuotaScopeHash`, and `WorkClass` where applicable. Values derived from a store,
credential, or quota scope are opaque. `ResultStatus` is part of the intended schema but terminal
status and phase upserts are not yet implemented; see the production-readiness guide.

## Core correctness invariants

- Workflow code is deterministic; side effects and wall-clock interaction belong in Activities.
- Every persistent mutation is generation-fenced atomically with its write.
- Deactivation commits the new generation before cancellation or cleanup.
- Fan-out is finite, joined, and drained before Continue-As-New.
- A failed page checkpoints the earliest failed page's input cursor; later work may be replayed but
  is idempotent.
- Root progress carried through Continue-As-New uses cumulative counts and bounded samples.
- A new sync cannot start while remediation remains active for the store.
- Quota callers receive an exact grant or denial and never consume an Activity slot while waiting.
- Workflow history contains document references, not document bodies.
- Metrics exclude raw store, user, credential, request, and cursor identifiers.

The diagrams in [`docs/workflow-topology.md`](docs/workflow-topology.md) show where each invariant
is enforced.

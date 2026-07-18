# Repository implementation map

## Baseline inspected on 2026-07-18

The Git repository had an unborn `main` branch and contained only `.git`; `HEAD` did not
resolve to a commit. Therefore the current-state answer for each mandatory discovery item
is **none present in this repository**:

- Workflow and Activity definitions: none.
- Workflow IDs and Task Queues: none.
- Retry, timeout, Search Attribute, and Parent Close policies: none.
- Temporal dependency pin and Server/Cloud capability declaration: none.
- Worker deployment/versioning configuration: none.
- Store metadata authority or mutation activities: none.
- Quota/rate-limit workflow or namespace boundary: none.
- Representative histories and replay baseline: none.

The host initially exposed Temporal Python SDK 1.18.2, Python 3.13.7, the Temporal CLI,
and pytest 8.4.2. This project pins Temporal Python SDK 1.30.0. Server capability is
explicitly configured rather than inferred; Priority/Fairness stays disabled unless both
configuration and the deployed Server/Cloud capability permit it. Retrieval and quota
workflows share one namespace by default.

Because no production histories were provided, replay compatibility cannot be proven by
this checkout. Replay test scaffolding reports an explicit skip when no fixture is present;
the release gate must separately require complete fixtures. The rollout runbook requires
exported histories before production cutover.

## V2 logical inventory and source map

| Logical component | Workflow implementation | Main responsibilities |
|---|---|---|
| Store controller | `workflows/store_controller.py` | lifecycle state, operation registry, bounded command dedup, cancellation decisions |
| Root sync | `workflows/root_sync.py` | ordinary user pages, round scheduling, cancellation drain, detached remediation |
| Failed remediation | `workflows/failed_user_remediation.py` | detached durable tracking, bounded activation batches, safe CAN |
| Activate user | `workflows/activate_user.py` | sequential recent and backfill waves |
| User sync | `workflows/user_sync.py` | bounded joined resource fan-out |
| Resource sync | `workflows/resource_sync.py` | resource configuration and cursor ownership |
| Resource pages | `workflows/resource_pages.py` | sliding page-child window and safe drain/CAN boundary |
| Files page | `workflows/files_page.py` | bounded joined document fan-out |
| Comments resync | `workflows/comments_resync.py` | preserved comments boundary |
| Document ingestion | `workflows/document_ingestion.py` | staged-reference ingestion with generation-fenced commit |
| Shared user quota | `workflows/user_quota.py` | FIFO permits, reset timers, in-flight cap, observations |
| Store deactivation | `workflows/deactivate_store.py` | fence, cancellation, drain, cleanup, terminal lifecycle |
| Cleanup/deactivation tree | `workflows/cleanup.py` | bounded CleanupUsers, DeactivateUser/One/All, RemoveObjects types |

## Final old → V2 Workflow Type inventory

`worker.py::V2_WORKFLOW_TYPES` is the authoritative final registry. It contains exactly
**17** Workflow Types; `tests/replay/workflow_registry.py` mirrors the same 17 types.

| # | Original role or Workflow Type | Final V2 Workflow Type | Disposition |
|---:|---|---|---|
| 1 | — | `StoreControllerWorkflow` | Added lifecycle/operation owner |
| 2 | Existing shared rate-limit role; original type name was not supplied | `UserQuotaWorkflow` | Redesigned replacement |
| 3 | `RootSyncWorkflow` | `RootSyncWorkflow` | Redesigned in place; type name preserved |
| 4 | `DeactivateStoreWorkflow` | `DeactivateStoreWorkflow` | Redesigned in place; type name preserved |
| 5 | `FailedUserRemediationWorkflow` | `FailedUserRemediationWorkflow` | Preserved |
| 6 | `ActivateUserWorkflow` | `ActivateUserWorkflow` | Preserved |
| 7 | `UserSyncWorkflow` | `UserSyncWorkflow` | Preserved |
| 8 | `ResourceSyncWorkflow` | `ResourceSyncWorkflow` | Preserved |
| 9 | `ResourcePagesWorkflow` | `ResourcePagesWorkflow` | Preserved |
| 10 | `FilesPageWorkflow` | `FilesPageWorkflow` | Preserved |
| 11 | `CommentsResyncWorkflow` | `CommentsResyncWorkflow` | Preserved |
| 12 | `DocumentIngestionWorkflow` | `DocumentIngestionWorkflow` | Preserved |
| 13 | `CleanupUsersWorkflow` | `CleanupUsersWorkflow` | Preserved |
| 14 | `DeactivateUserWorkflow` | `DeactivateUserWorkflow` | Preserved |
| 15 | `DeactivateOneUserWorkflow` | `DeactivateOneUserWorkflow` | Preserved |
| 16 | `DeactivateAllUsersWorkflow` | `DeactivateAllUsersWorkflow` | Preserved |
| 17 | `RemoveObjectsWorkflow` | `RemoveObjectsWorkflow` | Preserved |

Exactly two Workflow Types are removed from **new** execution paths:

| Legacy Workflow Type | V2 new-run behavior | Drain behavior |
|---|---|---|
| `QuotaWaitWorkflow` | Never started | Keep the original compatible worker build pinned until real executions drain |
| `AccessioningWorkflow` | Never started | Keep the original compatible worker build pinned until real executions drain |

No other original Workflow Type is removed for new runs. The placeholder definitions in
`workflows/legacy.py` preserve names for greenfield registration tests only; they cannot
replay production code that was not supplied with this repository.

Supporting code:

- `models/`: lifecycle, quota, operation, sync, and document contracts.
- `common/ids.py`: centralized opaque deterministic Workflow and request IDs.
- `common/priorities.py`: WorkClass mapping and capability-gated scheduling metadata.
- `common/quota_waiter.py`: workflow-local permit request/wait/cancel state machine.
- `common/metrics.py`: replay-safe, bounded-cardinality application metric instruments.
- `activities/lifecycle.py`: atomic lifecycle persistence, idempotent generation advance,
  same-generation failed-deactivation resume, and generation-fenced commits.
- `activities/quota_client.py`: short Signal-with-Start bridge to the shared quota workflow.
- `activities/provider_api.py`: structured provider response/observation contract.
- `worker.py`: Workflow Type and Activity registration, including legacy drain support.
- `client.py`: controller Update-with-Start entry APIs.

## IDs, queues, and policy map

All business-derived path segments pass through the opaque SHA-256 helper in
`common/ids.py`; Run ID is never a business identity.

| Concern | V2 convention |
|---|---|
| Controller ID | `store-controller/{opaque-store}` |
| Sync ID | `store-sync/{opaque-store}/{generation}/{opaque-sequence}` |
| Remediation ID | `failed-user-remediation/{opaque-store}/{generation}/{opaque-sequence}` |
| Deactivation ID | `store-deactivation/{opaque-store}/{generation}` |
| Quota ID | `user-quota/{provider}/{opaque-credential}/{quota-class}` |
| Retrieval Task Queue | typed `TEMPORAL_RETRIEVAL_TASK_QUEUE`, default `retrieval-v2` |
| Provider Task Queue | typed `TEMPORAL_PROVIDER_TASK_QUEUE`, default `retrieval-provider-v2` |
| Namespace | typed `TEMPORAL_NAMESPACE`, default `default`; quota uses the same namespace |
| Detached work | explicit `ParentClosePolicy.ABANDON`, stable ID, awaited start acknowledgement |
| Activities | explicit start-to-close and bounded RetryPolicy at every scheduling site |
| Search Attributes | centralized typed names; direct start-time attachment to controller, sync, remediation, and deactivation executions when enabled |

## Search Attribute attachment

After the custom attributes are registered in the namespace and
`TEMPORAL_ENABLE_SEARCH_ATTRIBUTES=true`:

- `RetrievalClient` attaches attributes directly to `StoreControllerWorkflow` starts.
- `StoreControllerWorkflow` attaches them directly to detached `RootSyncWorkflow` and
  `DeactivateStoreWorkflow` starts.
- `RootSyncWorkflow` attaches them directly to detached
  `FailedUserRemediationWorkflow` starts.

The shared builder in `common/search_attributes.py` always supplies `StoreKeyHash`,
`LifecycleGeneration`, `OperationType`, and `CurrentPhase`; it adds the opaque
`SyncSequence`, `Provider`, opaque `QuotaScopeHash`, and `WorkClass` values where the
operation has that context. `ResultStatus` is declared for the target schema but is not yet
upserted by the current workflow implementations.

## Mutation authority

`activities/repositories.py` defines the `RetrievalRepository` protocol as the single
authority for status, generation, lifecycle transitions, retrieval state, and indexed
document mutations. Its conditional methods must atomically compare
`expected_generation` and allowed status with the write. The included in-memory adapter is
test-only; production must inject a transactional database adapter.

Every user activation, retrieval-state mutation, indexed-document upsert, object removal,
and terminal lifecycle update flows through a generation-fenced activity in
`activities/lifecycle.py`, `activities/ingestion.py`, or `activities/cleanup.py`.

## Compatibility boundary

No old definitions were supplied here to rename. V2 uses the 17 explicit type names above
and a worker registry that can register `QuotaWaitWorkflow` and `AccessioningWorkflow` as
legacy drain names while no V2 path starts them. Real old executions must remain pinned to
their original compatible worker build. Deployment-based Worker Versioning is the target
rollout strategy; exported histories remain a prerequisite for its replay gate.

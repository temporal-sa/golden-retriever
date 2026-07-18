# Workflow topology

This page is the visual companion to [`IMPLEMENTATION_MAP.md`](../IMPLEMENTATION_MAP.md). It
shows the V2 execution paths registered by `worker.py`, including the detached ownership,
bounded fan-out, shared quota, and generation-fenced deactivation boundaries. The architecture is
split into focused views so that the complete topology remains readable.

## End-to-end workflow tree

Solid arrows are child-workflow starts. Dashed arrows are Signals, cancellation, or shared
coordination rather than parent-child ownership. `joined` means the parent awaits completion;
`detached` means the start is durably acknowledged and the stable Workflow ID is tracked by the
store controller.

```mermaid
flowchart TB
    app["Application or API"]
    client["RetrievalClient"]

    subgraph control["Command serialization and lifecycle ownership"]
        direction TB
        controller[["StoreControllerWorkflow<br/>one per store; idle-only Continue-As-New"]]
    end

    subgraph sync["Sync and remediation topology"]
        direction TB
        root[["RootSyncWorkflow<br/>user pages or round scheduling; barriered Continue-As-New"]]
        user[["UserSyncWorkflow<br/>bounded resource fan-out"]]
        resource[["ResourceSyncWorkflow<br/>resource cursor owner"]]
        pages[["ResourcePagesWorkflow<br/>sliding page window; drain before Continue-As-New"]]
        files[["FilesPageWorkflow<br/>bounded document fan-out"]]
        document[["DocumentIngestionWorkflow<br/>generation-fenced mutation"]]

        remediation[["FailedUserRemediationWorkflow<br/>detached; batch-boundary Continue-As-New"]]
        activate[["ActivateUserWorkflow<br/>recent wave, fence check, backfill"]]
        comments[["CommentsResyncWorkflow<br/>preserved compatibility boundary"]]

        root -->|"joined, bounded users<br/>page or round barrier"| user
        user -->|"joined, bounded resources"| resource
        resource -->|"joined"| pages
        pages -->|"joined, sliding page window"| files
        files -->|"joined, bounded documents"| document

        root -->|"failed users; detached"| remediation
        remediation -->|"joined, bounded activation batches"| activate
        activate -->|"recent then backfill waves"| user
        comments -->|"direct compatibility start; joined"| resource
    end

    subgraph quota["Shared quota coordinator"]
        direction TB
        userQuota[["UserQuotaWorkflow<br/>one per provider + credential + class; safe Continue-As-New"]]
    end

    subgraph deactivate["Generation-fenced deactivation topology"]
        direction TB
        deactivateStore[["DeactivateStoreWorkflow<br/>fence, cancel, drain, cleanup"]]
        cleanupUsers[["CleanupUsersWorkflow<br/>bounded user batches"]]
        deactivateUser[["DeactivateUserWorkflow<br/>compatibility boundary"]]
        deactivateOne[["DeactivateOneUserWorkflow"]]
        deactivateAll[["DeactivateAllUsersWorkflow"]]
        removeObjects[["RemoveObjectsWorkflow"]]

        deactivateStore -->|"joined"| cleanupUsers
        cleanupUsers -->|"explicit user keys"| deactivateUser
        deactivateUser -->|"joined"| deactivateOne
        cleanupUsers -->|"empty user set means all users"| deactivateAll
        deactivateStore -->|"joined after user cleanup"| removeObjects
    end

    app --> client
    client ==>|"Update-with-Start<br/>request_sync, cancel_sync, start_deactivation"| controller
    client -.->|"get_status query"| controller

    controller -->|"request_sync<br/>detached, stable ID"| root
    controller -->|"start_deactivation<br/>detached, stable ID"| deactivateStore

    root -.->|"operation_status"| controller
    remediation -.->|"remediation_started / finished"| controller
    deactivateStore -.->|"fenced and terminal status"| controller
    controller -.->|"cancel_sync"| root

    root -.->|"permit requests for user-list calls"| userQuota
    pages -.->|"permit requests for page-fetch calls"| userQuota
    userQuota -.->|"quota_granted"| root
    userQuota -.->|"quota_granted"| pages

    deactivateStore -.->|"cancel owned sync and remediation"| root
    deactivateStore -.->|"cancel owned remediation"| remediation
    deactivateStore -.->|"cancel_generation"| userQuota
    controller -.->|"operation_drained"| deactivateStore
```

`CommentsResyncWorkflow` remains registered as a direct compatibility boundary but is not started
by the controller-driven V2 path. `QuotaWaitWorkflow` and `AccessioningWorkflow` are optional
legacy-drain registrations and are never started by a new V2 execution.

## Activity and Task Queue boundaries

Both workers run in the `retrieval-worker` process. Workflow Tasks and persistence-facing
Activities use `retrieval-v2`; provider calls use the separate, optionally rate-limited
`retrieval-provider-v2` queue. These are the default queue names and can be overridden through
runtime configuration.

```mermaid
flowchart LR
    subgraph retrievalQueue["retrieval-v2 Task Queue"]
        workflows["All 17 V2 Workflow Types"]
        lifecycle["Lifecycle Activities<br/>validate, activate, begin/resume deactivation,<br/>mark inactive or failed"]
        cleanup["Cleanup Activities<br/>deactivate users, remove objects"]
        ingestion["Ingestion Activity<br/>ingest staged document"]
        quotaBridge["Quota bridge Activity<br/>Signal-with-Start UserQuotaWorkflow"]
    end

    subgraph providerQueue["retrieval-provider-v2 Task Queue"]
        listUsers["provider_list_active_users"]
        fetchPage["provider_fetch_resource_page"]
    end

    repository[("RetrievalRepository<br/>authoritative lifecycle and index state")]
    staging[("StagingStore")]
    provider{{"Provider API"}}

    workflows --> lifecycle
    workflows --> cleanup
    workflows --> ingestion
    workflows --> quotaBridge
    workflows --> listUsers
    workflows --> fetchPage

    lifecycle --> repository
    cleanup --> repository
    ingestion --> repository
    ingestion --> staging
    listUsers --> provider
    fetchPage --> provider
```

## Shared quota permit loop

Only `RootSyncWorkflow` and `ResourcePagesWorkflow` make provider calls. When a quota scope is
present, they acquire a permit before scheduling the provider Activity; waiting is durable and
does not occupy a worker slot.

```mermaid
sequenceDiagram
    autonumber
    participant W as RootSync or ResourcePages
    participant B as signal_with_start_user_quota Activity
    participant Q as UserQuotaWorkflow
    participant P as Provider Activity

    W->>B: PermitRequest with deterministic request_id
    B->>Q: Signal-with-Start request_permit
    Note over Q: Reuse one coordinator per<br/>provider + credential + quota class
    Q-->>W: quota_granted Signal when capacity permits
    W->>P: Schedule provider call
    P-->>W: Structured response and optional quota observation
    opt Response carries limit, remaining, reset, or 429 data
        W-->>Q: observe_quota Signal
    end
    W-->>Q: permit_completed Signal
    alt Provider reports quota exhaustion
        Note over Q: Block the scope until the authoritative reset
        W->>B: Retry the unchanged cursor with a new request_id
    else Requester is canceled while waiting
        W-->>Q: cancel_permit Signal
    end
```

The permit cost is not refunded after provider work begins. `permit_completed` only releases the
in-flight concurrency reservation; an authoritative observation or reset restores quota.

## Deactivation order and failure boundary

The generation fence is the point of no return. Cancellation never precedes it, so late Activity
delivery remains harmless: every mutation compares the expected lifecycle generation in the same
transaction as the write.

```mermaid
flowchart TB
    command["start_deactivation Update"]
    serialize["Controller serializes and deduplicates command"]
    start[["Start DeactivateStoreWorkflow<br/>stable generation-derived ID"]]
    fence["Atomically begin or resume deactivation<br/>commit authoritative generation fence"]
    acknowledged["Signal controller: deactivation_fenced"]
    cancel["Request cancellation of tracked sync and remediation"]
    invalidate["Signal cancel_generation to tracked quota coordinators"]
    drain["Wait for operation_drained Signals<br/>bounded by drain timeout"]
    users[["CleanupUsersWorkflow tree"]]
    objects[["RemoveObjectsWorkflow"]]
    inactive["Atomically mark store INACTIVE"]
    terminal["Signal controller terminal status<br/>clear active operation ownership"]
    preFenceFailure["Pre-fence failure<br/>controller returns to ACTIVE or SYNCING"]
    postFenceFailure["Post-fence failure<br/>mark DEACTIVATION_FAILED"]
    warning["Warning-only conditions<br/>controller acknowledgement failure,<br/>cancel or quota-signal failure, drain timeout"]
    resume["Retry with same generation and stable ID"]

    command --> serialize --> start --> fence
    fence --> acknowledged --> cancel --> invalidate --> drain --> users --> objects --> inactive --> terminal

    start -.->|"failure before commit"| preFenceFailure
    acknowledged -.->|"warning"| warning
    cancel -.->|"warning"| warning
    invalidate -.->|"warning"| warning
    drain -.->|"warning"| warning
    users -.->|"failure"| postFenceFailure
    objects -.->|"failure"| postFenceFailure
    inactive -.->|"failure"| postFenceFailure
    terminal -.->|"warning yields PARTIAL"| warning
    postFenceFailure --> resume -->|"same generation"| fence
```

Warning-only conditions do not interrupt the solid-arrow cleanup path. They can produce a
`PARTIAL` terminal result after cleanup still succeeds. A committed generation is never
decremented during retry or rollback.

## Ownership and concurrency summary

| Boundary | Ownership and completion rule | Bound or barrier |
|---|---|---|
| Controller → root sync | Detached start, stable ID, controller registry | One active sync per store |
| Root sync → user sync | Joined children | User-page barrier or bounded round window |
| User sync → resource sync | Joined children | `RESOURCE_CONCURRENCY` |
| Resource pages → files page | Joined sliding window | `FILES_PAGE_WINDOW_SIZE` |
| Files page → document ingestion | Joined children | Per-page and global document bounds |
| Root sync → remediation | Detached start, stable ID, controller registry | Bounded activation batches; safe Continue-As-New |
| Remediation → activation | Joined children | At most eight or configured resource bound |
| Activation → user sync | Sequential recent and backfill waves | Generation revalidated between waves |
| Provider request → quota | Shared Signal-with-Start coordinator | Per-scope in-flight cap and reset state |
| Controller → deactivation | Detached start, stable generation ID | Fence before cancellation; bounded drain |
| Deactivation → cleanup | Joined children | Bounded user batches, then object cleanup |

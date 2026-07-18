# Workflow topology

This page is the visual guide to the runtime described in
[`IMPLEMENTATION_MAP.md`](../IMPLEMENTATION_MAP.md). It shows workflow ownership, bounded fan-out,
provider quota coordination, recovery, and store deactivation.

In the diagrams, a **joined** child must finish before its parent can finish. A **detached**
workflow is started with a stable Workflow ID, acknowledged by Temporal, and then tracked by the
store controller. Dashed arrows represent Signals, cancellation requests, or shared coordination
rather than parent-child ownership.

## Store lifecycle and command flow

Applications do not start sync or deactivation workflows directly. They call `RetrievalClient`,
which uses Update-with-Start to create or update the single controller for a store.

```mermaid
stateDiagram-v2
    [*] --> ACTIVE: controller starts
    ACTIVE --> SYNCING: request_sync accepted
    SYNCING --> ACTIVE: sync and remediation complete
    SYNCING --> SYNCING: cancel_sync requested
    ACTIVE --> DEACTIVATING: start_deactivation accepted
    SYNCING --> DEACTIVATING: start_deactivation accepted
    DEACTIVATING --> INACTIVE: cleanup succeeds
    DEACTIVATING --> DEACTIVATION_FAILED: cleanup cannot finish
    DEACTIVATION_FAILED --> DEACTIVATING: retry same generation
    INACTIVE --> INACTIVE: sync rejected
```

The controller accepts only one store operation at a time. A store remains `SYNCING` while any
detached failed-user remediation is still active, even if its root sync has already finished.

## End-to-end workflow tree

```mermaid
flowchart TB
    app["Application or API"]
    client["RetrievalClient"]

    subgraph control["Command serialization and lifecycle ownership"]
        controller[["StoreControllerWorkflow<br/>one per store; idle-only Continue-As-New"]]
    end

    subgraph sync["Sync and remediation"]
        root[["RootSyncWorkflow<br/>user pages or bounded rounds"]]
        user[["UserSyncWorkflow<br/>bounded resource fan-out"]]
        resource[["ResourceSyncWorkflow<br/>one resource cursor"]]
        pages[["ResourcePagesWorkflow<br/>sliding page window and checkpoint"]]
        files[["FilesPageWorkflow<br/>bounded document fan-out"]]
        document[["DocumentIngestionWorkflow<br/>generation-fenced mutation"]]
        remediation[["FailedUserRemediationWorkflow<br/>detached and controller-tracked"]]
        activate[["ActivateUserWorkflow<br/>recent sync, fence check, backfill"]]
        comments[["CommentsResyncWorkflow<br/>optional direct boundary"]]

        root -->|"joined, bounded users"| user
        user -->|"joined, bounded resources"| resource
        resource -->|"joined"| pages
        pages -->|"joined, sliding window"| files
        files -->|"joined, bounded documents"| document
        root -->|"failed user batches; detached"| remediation
        remediation -->|"joined, bounded batches"| activate
        activate -->|"recent then backfill"| user
        comments -->|"joined"| resource
    end

    subgraph quota["Shared provider quota"]
        userQuota[["UserQuotaWorkflow<br/>one per provider, credential, and quota class"]]
    end

    subgraph deactivate["Generation-fenced deactivation"]
        deactivateStore[["DeactivateStoreWorkflow<br/>fence, cancel, drain, cleanup"]]
        cleanupUsers[["CleanupUsersWorkflow<br/>bounded user batches"]]
        deactivateUser[["DeactivateUserWorkflow"]]
        deactivateOne[["DeactivateOneUserWorkflow"]]
        deactivateAll[["DeactivateAllUsersWorkflow"]]
        removeObjects[["RemoveObjectsWorkflow"]]

        deactivateStore -->|"joined"| cleanupUsers
        cleanupUsers -->|"explicit user keys"| deactivateUser
        deactivateUser -->|"joined"| deactivateOne
        cleanupUsers -->|"empty user set means all users"| deactivateAll
        deactivateStore -->|"after user cleanup"| removeObjects
    end

    app --> client
    client ==>|"Update-with-Start commands"| controller
    client -.->|"get_status query"| controller
    controller -->|"request_sync; detached"| root
    controller -->|"start_deactivation; detached"| deactivateStore
    controller -.->|"cancel_sync"| root

    root -.->|"operation_status"| controller
    remediation -.->|"started and finished"| controller
    deactivateStore -.->|"fenced and terminal status"| controller

    root -.->|"user-list permits"| userQuota
    pages -.->|"page-fetch permits"| userQuota
    userQuota -.->|"grant or denial"| root
    userQuota -.->|"grant or denial"| pages

    deactivateStore -.->|"cancel owned work"| root
    deactivateStore -.->|"cancel owned work"| remediation
    deactivateStore -.->|"cancel_generation"| userQuota
    controller -.->|"operation_drained"| deactivateStore
```

`CommentsResyncWorkflow` is registered for callers that use the direct comments boundary; the
controller-driven sync tree does not start it. `QuotaWaitWorkflow` and `AccessioningWorkflow` can
be registered only for draining compatible existing histories and are never started by the
current execution path. See the deployment runbook before enabling those registrations.

## Activity and Task Queue boundaries

The `retrieval-worker` process starts two Temporal workers. The queue split isolates provider API
traffic from repository and staging work, and lets operators apply a provider Activity rate limit
without throttling lifecycle operations.

```mermaid
flowchart LR
    subgraph retrievalQueue["retrieval-v2 Task Queue"]
        workflows["All registered Workflow Types"]
        lifecycle["Lifecycle Activities<br/>validate, activate, fence, finish"]
        cleanup["Cleanup Activities<br/>deactivate users, remove objects"]
        ingestion["Ingestion Activity<br/>load staged body and mutate"]
        quotaBridge["Quota bridge Activity<br/>Signal-with-Start"]
    end

    subgraph providerQueue["retrieval-provider-v2 Task Queue"]
        listUsers["provider_list_active_users"]
        fetchPage["provider_fetch_resource_page"]
    end

    repository[("RetrievalRepository<br/>lifecycle and indexed state")]
    staging[("StagingStore<br/>document bodies")]
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

The queue names shown are defaults. `TEMPORAL_RETRIEVAL_TASK_QUEUE` and
`TEMPORAL_PROVIDER_TASK_QUEUE` override them.

## Shared quota permit loop

`RootSyncWorkflow` and `ResourcePagesWorkflow` acquire a permit before scheduling a provider
Activity when their input includes a quota scope. Waiting is durable and does not occupy an
Activity worker slot.

```mermaid
sequenceDiagram
    autonumber
    participant W as Calling Workflow
    participant B as Quota bridge Activity
    participant Q as UserQuotaWorkflow
    participant P as Provider Activity

    W->>B: request permit with deterministic request_id
    B->>Q: Signal-with-Start request_permit
    Note over Q: One coordinator per provider,<br/>credential key, and quota class
    alt Invalid, disabled, or pending queue full
        Q-->>W: quota_denied(reason)
        Note over W: Caller fails promptly
    else Request admitted
        alt Caller is canceled before grant
            W-->>Q: cancel_permit
        else Capacity and quota are available
            Q-->>W: quota_granted
            W->>P: schedule provider call
            P-->>W: structured response
            opt response contains a quota observation
                W-->>Q: observe_quota
            end
            W-->>Q: permit_completed
        end
    end
    opt Provider reports quota exhaustion
        W-->>Q: observe authoritative reset
        Note over Q: Scope remains blocked until reset
        Note over W: Retry preserves the provider cursor
    end
```

The pending queue is capped at 350 requests per quota scope. `permit_completed` releases the
in-flight concurrency reservation; it does not refund provider quota. Only an authoritative quota
observation or reset restores quota capacity.

## Page failure, checkpoint, and remediation

Resource pages execute in a sliding window. Successful work after an earlier failure may run
again, so document writes and deletes must be idempotent.

```mermaid
flowchart LR
    input["Input cursor"] --> window["Start bounded page window"]
    window --> results{"All page children succeeded?"}
    results -->|"yes"| advance["Advance to next cursor"]
    advance --> more{"More pages?"}
    more -->|"yes"| window
    more -->|"no"| complete["Return resource result"]
    results -->|"no"| checkpoint["Checkpoint earliest failed page's input cursor"]
    checkpoint --> partial["Return partial or failed user result"]
    partial --> collect["Root collects bounded failed-user batches"]
    collect --> remediation[["Start detached remediation<br/>stable ID, controller-tracked"]]
    remediation --> recent["Sync recent data"]
    recent --> fence["Revalidate lifecycle generation"]
    fence --> backfill["Sync backfill"]
    backfill --> done["Signal remediation finished"]
```

Root progress passed through Continue-As-New is cumulative, while error samples, failed-user
samples, and remediation IDs remain bounded. A new store sync is rejected until remediation has
finished.

## Deactivation order and recovery

The generation fence is the point of no return. It is committed before cancellation, making late
Activity delivery safe: every persistent mutation compares the expected generation and allowed
lifecycle state in the same transaction as its write.

```mermaid
flowchart TB
    command["start_deactivation Update"]
    start[["Start DeactivateStoreWorkflow<br/>stable generation-derived ID"]]
    fence["Atomically commit lifecycle generation fence"]
    acknowledge["Signal controller: fenced"]
    cancel["Request cancellation of sync and remediation"]
    quota["Invalidate this generation's quota requests"]
    drain["Wait for controller-owned work to drain<br/>bounded timeout"]
    users[["CleanupUsersWorkflow tree"]]
    objects[["RemoveObjectsWorkflow"]]
    inactive["Atomically mark store INACTIVE"]
    terminal["Signal controller terminal result"]
    prefail["Failure before fence<br/>store remains ACTIVE or SYNCING"]
    postfail["Failure after fence<br/>mark DEACTIVATION_FAILED"]
    retry["Retry same generation and stable ID"]
    partial["Acknowledgement, cancellation, quota-signal,<br/>or drain warning; cleanup continues"]

    command --> start --> fence --> acknowledge --> cancel --> quota --> drain --> users --> objects --> inactive --> terminal
    start -.->|"failure before commit"| prefail
    acknowledge -.->|"warning"| partial
    cancel -.->|"warning"| partial
    quota -.->|"warning"| partial
    drain -.->|"warning"| partial
    users -.->|"failure"| postfail
    objects -.->|"failure"| postfail
    inactive -.->|"failure"| postfail
    postfail --> retry --> fence
```

Warnings do not interrupt cleanup, but they can produce a `PARTIAL` result. A committed generation
is never decremented during a retry, deployment rollback, or operational recovery.

## Ownership and concurrency summary

| Boundary | Completion rule | Bound or barrier |
|---|---|---|
| Controller → root sync | Detached, stable ID, controller registry | One active sync per store |
| Root sync → user sync | Joined children | User-page barrier or bounded round window |
| User sync → resource sync | Joined children | `RESOURCE_CONCURRENCY` |
| Resource pages → files page | Joined sliding window | `FILES_PAGE_WINDOW_SIZE` |
| Files page → document ingestion | Joined children | Lower of the two configured per-page document bounds |
| Root sync → remediation | Detached, stable ID, controller registry | Bounded activation batches and Continue-As-New |
| Remediation → activation | Joined children | Bounded batch |
| Activation → user sync | Sequential recent and backfill waves | Generation check between waves |
| Provider request → quota | Shared Signal-with-Start coordinator | Per-scope in-flight and pending caps |
| Controller → deactivation | Detached, stable generation ID | Fence before cancellation and bounded drain |
| Deactivation → cleanup | Joined children | Bounded user batches, then object cleanup |

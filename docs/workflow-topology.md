# Workflow and data topology

This page visualizes how requests move through the system. It assumes only the component overview
in the root [README](../README.md).

Temporal vocabulary used in the diagrams:

- a **Workflow** is deterministic code whose progress is stored in Event History;
- an **Activity** performs external I/O and may be retried;
- a **Task Queue** connects Temporal tasks to worker pollers;
- a **joined child** must finish before its parent finishes;
- a **detached operation** has a stable Workflow ID, is owned by the store controller, and may
  outlive the controller run that started it.

## Runtime processes and ownership

```mermaid
flowchart LR
    browser["Browser<br/>Northstar UI"]

    subgraph appProcess["Databricks App"]
        app["FastAPI + static UI<br/>commands, snapshots, search"]
    end

    subgraph temporal["Temporal namespace"]
        controller[["StoreControllerWorkflow"]]
        sync[["Sync workflow tree"]]
        quota[["UserQuotaWorkflow"]]
        deactivate[["Deactivation workflow tree"]]
    end

    subgraph workerProcess["Long-running retrieval-worker"]
        retrievalQueue["retrieval-v2 poller<br/>workflows + database Activities"]
        providerQueue["retrieval-provider-v2 poller<br/>provider Activities"]
        adapters["AdapterBundle<br/>repository + staging + provider"]
    end

    subgraph database["Lakebase Postgres"]
        core[("retrieval<br/>lifecycle + searchable data")]
        demo[("retrieval_demo_ui<br/>runs + controls + events")]
        fts["Postgres full-text search"]
    end

    fixtures["Manifest-verified<br/>Northstar fixtures"]

    browser <--> app
    app -->|"Update-with-Start and queries"| controller
    app -->|"state, search, controls, events"| database
    controller --> sync
    controller --> deactivate
    sync <--> quota
    retrievalQueue <--> temporal
    providerQueue <--> temporal
    retrievalQueue --> adapters
    providerQueue --> adapters
    adapters --> core
    adapters --> demo
    adapters --> fixtures
    core --> fts
```

The App serves HTTP but never polls Temporal. The worker polls Temporal but never serves HTTP.
Both use Lakebase with different database roles. A third migration identity owns the schemas.

## From an HTTP command to durable work

```mermaid
sequenceDiagram
    actor U as User
    participant A as FastAPI App
    participant C as StoreControllerWorkflow
    participant O as Detached operation
    participant W as Worker Activities
    participant D as Lakebase

    U->>A: POST command + Idempotency-Key
    A->>D: reserve/replay HTTP receipt
    A->>C: RetrievalClient Update-with-Start
    C->>O: start stable sync/deactivation ID
    C-->>A: accepted operation identity
    A->>D: store durable HTTP response
    A-->>U: 202 Accepted
    O->>W: schedule bounded work
    W->>D: generation-fenced transaction
    O-->>C: idempotent status Signal
    U->>A: poll snapshot/operation
    A-->>U: Lakebase state + controller status
```

The browser does not hold an HTTP request open while a workflow runs. HTTP idempotency is stored in
Lakebase, while workflow command deduplication belongs to the controller.

## Store controller and child workflows

```mermaid
flowchart TB
    caller["App or Python caller"] --> client["RetrievalClient"]
    client ==>|"sync / cancel / deactivate"| controller[["StoreControllerWorkflow<br/>one per store"]]

    subgraph syncTree["Detached sync with joined descendants"]
        root[["RootSyncWorkflow<br/>user windows"]]
        user[["UserSyncWorkflow<br/>resource fan-out"]]
        resource[["ResourceSyncWorkflow<br/>one cursor"]]
        pages[["ResourcePagesWorkflow<br/>sliding page window"]]
        files[["FilesPageWorkflow<br/>bounded documents"]]
        ingest[["DocumentIngestionWorkflow<br/>one fenced mutation"]]
        remediation[["FailedUserRemediationWorkflow"]]
        activate[["ActivateUserWorkflow"]]

        root --> user --> resource --> pages --> files --> ingest
        root -->|"failed users"| remediation --> activate --> user
    end

    subgraph deactivationTree["Detached deactivation with ordered cleanup"]
        deactivation[["DeactivateStoreWorkflow"]]
        cleanUsers[["CleanupUsersWorkflow"]]
        routeUser[["DeactivateUserWorkflow"]]
        oneUser[["DeactivateOneUserWorkflow"]]
        allUsers[["DeactivateAllUsersWorkflow"]]
        objects[["RemoveObjectsWorkflow<br/>bounded batches"]]

        deactivation --> cleanUsers
        cleanUsers --> routeUser --> oneUser
        cleanUsers --> allUsers
        deactivation --> objects
    end

    controller -->|"stable detached ID"| root
    controller -->|"stable generation ID"| deactivation
    controller -.->|"cancel owned work"| syncTree
    syncTree -.->|"status Signals"| controller
    deactivationTree -.->|"fence and status Signals"| controller
```

The controller serializes lifecycle decisions but does not absorb high-volume fan-out into its own
history. Page windows, document windows, and cleanup batches are finite. Continue-As-New is used
where long-lived state needs a fresh Event History.

## Task Queue boundaries

```mermaid
flowchart LR
    subgraph retrieval["retrieval-v2"]
        workflows["All registered Workflows"]
        lifecycle["Lifecycle Activities"]
        ingestion["Load, parse, chunk, commit"]
        cleanup["User and object cleanup"]
        quotaBridge["Quota Signal-with-Start bridge"]
    end

    subgraph provider["retrieval-provider-v2"]
        listUsers["list active users"]
        fetchPage["fetch resource page"]
    end

    repository[("RetrievalRepository")]
    staging[("StagingStore")]
    gateway{"ProviderGateway"}

    workflows --> lifecycle --> repository
    workflows --> ingestion --> repository
    ingestion --> staging
    workflows --> cleanup --> repository
    workflows --> quotaBridge
    workflows --> listUsers --> gateway
    workflows --> fetchPage --> gateway
```

The split allows provider calls to have their own pollers and rate limit without throttling
lifecycle or database work. Document bodies are loaded only inside the ingestion Activity.

## Shared provider quota

One quota workflow coordinates callers that share a provider credential scope.

```mermaid
sequenceDiagram
    participant W as Sync workflow
    participant B as Bridge Activity
    participant Q as UserQuotaWorkflow
    participant P as Provider Activity

    W->>B: deterministic permit request
    B->>Q: Signal-with-Start
    alt queue full or invalid
        Q-->>W: explicit denial
    else permit granted
        Q-->>W: quota_granted
        W->>P: provider call
        alt success
            P-->>W: page metadata / DocumentRefs
            W-->>Q: permit_completed
        else quota exhausted
            P-->>W: limit + retry/reset data
            W-->>Q: authoritative reset observation
            Note over W,Q: durable wait; cursor retained
            Q-->>W: grant after reset
            W->>P: retry
        end
    end
```

Waiting occurs in Workflow state, not an Activity process. The scope retains bounded pending,
in-flight, reset, and deduplication state.

## Document transaction

```mermaid
sequenceDiagram
    participant W as DocumentIngestionWorkflow
    participant A as Ingestion Activity
    participant S as StagingStore
    participant D as Lakebase

    W->>A: DocumentRef + expected generation
    A->>S: load body
    S-->>A: verified bytes
    Note over A: parse and deterministic chunking
    A->>D: begin transaction + lock store
    D-->>A: current state/generation
    alt expected generation is writable
        A->>D: resolve receipt, upsert document, replace chunks
        D-->>A: commit atomically
    else stale or wrong lifecycle state
        D-->>A: reject and roll back
    end
```

The body and chunks never become workflow payloads. A retry is safe because the receipt and
mutation share the same generation-aware transaction.

## Northstar late-writer proof

```mermaid
sequenceDiagram
    actor U as User
    participant A as App
    participant T as Temporal
    participant I as Ingestion Activity
    participant D as Lakebase

    U->>A: create run
    A->>D: seed active generation 7
    U->>A: start sync
    A->>T: controller sync command
    Note over T: one provider quota wait, then resume
    T->>I: ingest five generation-7 documents
    I->>D: commit four documents
    Note over I: fifth document waits before transaction
    U->>A: deactivate
    A->>T: controller deactivation command
    T->>D: commit active/7 → deactivating/8
    T-->>T: cancel and drain generation-7 work
    U->>A: release held writer after fence
    A->>D: set release control
    I->>D: attempt generation-7 commit
    D-->>I: stale_generation_rejected (actual 8)
    loop bounded batches
        T->>D: clean generation-8 objects
    end
    T->>D: mark inactive after all owned rows reach zero
```

The bounded hold is demonstration code. It lets cancellation arrive and then deliberately reaches
the ordinary repository transaction so the database fence is observable.

## Deactivation recovery

```mermaid
flowchart TB
    command["start_deactivation"] --> fence{"Did the fence commit?"}
    fence -->|"no"| pre["Store remains active/syncing<br/>repair and retry"]
    fence -->|"yes: N → N+1"| cancel["Cancel old work and invalidate quota"]
    cancel --> drain["Bounded drain"]
    drain --> users["Clean users at N+1"]
    users --> objects["Clean object batches at N+1"]
    objects --> zero{"All owned rows zero?"}
    zero -->|"yes"| inactive["Commit inactive at N+1"]
    zero -->|"no/error"| failed["Record failed deactivation at N+1"]
    failed --> retry["Repair dependency; retry same generation"]
    retry --> users
```

Never decrement a committed generation. After the fence, recovery resumes cleanup at the same new
generation and stable deactivation identity.

## Data visibility rules

| Operation | Allowed state | Generation rule | Atomic effect |
|---|---|---|---|
| Document upsert/delete | `active` or `syncing` | expected equals current | document, chunks, receipt |
| User/checkpoint mutation | `active` or `syncing` | expected equals current | compare and mutation |
| Begin deactivation | active/syncing or resumable failure | advance once | state and generation fence |
| Cleanup | `deactivating` | cleanup generation equals current | bounded deletion |
| Mark inactive | `deactivating` | expected equals current | only after zero-row invariant |
| Search | `active` or `syncing` | data generation equals store | stale/non-readable rows excluded |

These rules make at-least-once Activity execution safe; they do not claim exactly-once execution.

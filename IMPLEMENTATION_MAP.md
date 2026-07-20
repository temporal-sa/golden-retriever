# Implementation map

This page connects system concepts to source files, executable commands, configuration, and
database objects. Read the root [README](README.md) first if Temporal, Lakebase, or the Northstar
scenario are unfamiliar.

## Find what you need

| Question | Start here |
|---|---|
| How does a caller submit work? | `src/retrieval/temporal/client.py` |
| Where are workflows registered? | `src/retrieval/temporal/worker.py` |
| Where is lifecycle safety enforced? | `src/retrieval/lakebase/repository.py` and the core migrations |
| How are provider bodies staged and verified? | `src/retrieval/content.py`, Activity code, and `src/retrieval/demo/fixtures.py` |
| How does search work? | `src/retrieval/lakebase/search.py` |
| Where is the HTTP API/UI? | `apps/retrieval_demo` |
| How is the Databricks App deployed? | `apps/retrieval_demo/databricks.yml` and root `app.yaml` |
| How is the worker packaged? | `Dockerfile.worker` |
| How are schemas created? | `src/retrieval/lakebase/migrations` and `src/retrieval/demo/migrations` |
| Which environment variable controls a behavior? | [Configuration reference](#configuration-reference) |
| Which test proves a behavior? | `tests/unit`, `tests/contract`, `tests/lakebase`, `tests/demo`, `tests/app`, and the specialized test guides |

## Source tree

| Path | Responsibility |
|---|---|
| `src/retrieval/environment.py` | Safe, exact-directory `.env` injection for executable processes |
| `src/retrieval/config.py` | Workflow concurrency, quota, cleanup, timeout, and fairness settings |
| `src/retrieval/content.py` | UTF-8/frontmatter validation and deterministic paragraph chunking |
| `src/retrieval/temporal/client.py` | Public `RetrievalClient` and store-controller commands |
| `src/retrieval/temporal/runtime_config.py` | Temporal address, namespace, queues, versioning, and adapter selection |
| `src/retrieval/temporal/worker.py` | Workflow/Activity registration and worker process lifecycle |
| `src/retrieval/temporal/models` | Small JSON-safe workflow inputs and results |
| `src/retrieval/temporal/common` | Stable IDs, quota helpers, metrics, priorities, and Search Attributes |
| `src/retrieval/temporal/activities` | External-I/O boundaries and adapter protocols |
| `src/retrieval/temporal/workflows` | Deterministic Workflow implementations |
| `src/retrieval/lakebase` | Postgres config/pool, repository, search, migrations, and grants |
| `src/retrieval/google_drive` | Read-only Drive client, recursive provider, shared staging, reconciliation, and worker bundle |
| `src/retrieval/demo` | Northstar scenario, scripted provider, controls, events, hold gate, and service |
| `apps/retrieval_demo` | FastAPI routes, static UI, App manifest, and Databricks bundle |
| `tests` | Default and opt-in verification suites |

## Executable entry points

| Command | Purpose | External services |
|---|---|---|
| `uv run retrieval-demo-headless --json` | Rehearse the Northstar state transitions in memory | none |
| `uv run retrieval-test-starter` | Run a local sync/deactivation workflow smoke test | Temporal |
| `uv run retrieval-worker` | Poll the retrieval and provider Task Queues | Temporal and configured adapters |
| `uv run retrieval-lakebase-migrate` | Apply/check the core `retrieval` schema | Postgres |
| `uv run retrieval-demo-migrate` | Apply/check the `retrieval_demo_ui` schema | Postgres |
| `uv run retrieval-lakebase-grant-roles` | Apply explicit App and worker grants | Postgres |
| `uv run retrieval-demo-app` | Serve the FastAPI API and browser UI | Postgres and Temporal |

The App never hosts a Temporal worker. `retrieval-worker` starts two SDK workers in one process:
workflows and persistence Activities poll the retrieval queue; provider Activities poll the
provider queue.

The worker, App, migration commands, and grant command load configuration once at startup. The
headless rehearsal and local starter are self-contained.

## Workflow registry

`retrieval.temporal.worker.V2_WORKFLOW_TYPES` is the authoritative runtime registry. The replay
registry mirrors the implementations required by checked-in histories.

| Workflow | Responsibility | Lifetime |
|---|---|---|
| `StoreControllerWorkflow` | Serialize one store's commands and operation status | long-lived authority |
| `RootSyncWorkflow` | Enumerate users and aggregate sync progress | controller-owned detached operation |
| `FailedUserRemediationWorkflow` | Retry bounded failed-user sets | controller-owned detached operation |
| `ActivateUserWorkflow` | Recent sync, generation recheck, backfill, activation | joined child |
| `UserSyncWorkflow` | Bound resource fan-out for one user | joined child |
| `ResourceSyncWorkflow` | Own one resource cursor and page policy | joined child |
| `ResourcePagesWorkflow` | Maintain the sliding page window/checkpoint | joined child |
| `FilesPageWorkflow` | Bound document upsert/delete fan-out | joined child |
| `DocumentIngestionWorkflow` | Load and commit one generation-fenced document | joined child |
| `CommentsResyncWorkflow` | Optional direct comments boundary | registered delegate |
| `UserQuotaWorkflow` | Share provider admission/reset state | shared long-lived coordinator |
| `DeactivateStoreWorkflow` | Fence, cancel, drain, clean, and finish | controller-owned detached operation |
| `CleanupUsersWorkflow` | Route bounded user cleanup | joined child |
| `DeactivateUserWorkflow` | Route one user's cleanup | joined child |
| `DeactivateOneUserWorkflow` | Execute one-user deactivation | joined child |
| `DeactivateAllUsersWorkflow` | Execute all-user deactivation | joined child |
| `RemoveObjectsWorkflow` | Delete bounded object batches and Continue-As-New | joined child |

`QuotaWaitWorkflow` and `AccessioningWorkflow` are optional drain-only names. No new execution
starts them. Register them only when a known deployment needs those names to drain; placeholders
cannot replay an arbitrary historical implementation.

## Activities and adapters

Workflow code performs no database, filesystem, network, or wall-clock I/O. Activities call these
ports and hooks:

| Port or hook | Included implementations |
|---|---|
| `RetrievalRepository` | in-memory repository; `LakebaseRetrievalRepository` |
| `StagingStore` | in-memory staging; manifest-restricted fixture staging; content-addressed Drive staging |
| `ProviderGateway` | empty local provider; scripted Northstar provider; read-only Google Drive provider |
| `BeforeDocumentCommitHook` | no-op; bounded Northstar pre-commit hold |
| `IngestionEventSink` | no-op; durable Northstar event sink |

`AdapterBundle` owns one process-wide set of ports and closes each unique resource during worker
shutdown. The Northstar worker selects its bundle with:

```text
RETRIEVAL_ADAPTER_BUNDLE_FACTORY=retrieval.demo.scripted_provider:create_adapter_bundle
```

The Google Drive + Lakebase bundle is `retrieval.google_drive.bundle:create_adapter_bundle`. See the
[Google Drive integration guide](docs/google-drive-integration.md) for authentication, staging,
supported MIME types, reconciliation, and sync metadata.

Adapter configuration is fail-closed. Choose exactly one of these modes:

1. set `RETRIEVAL_ADAPTER_BUNDLE_FACTORY`; or
2. set all three individual repository, staging, and provider factory variables; or
3. for private local testing only, set `RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS=true`.

Every other combination fails worker startup.

## Stable identities and Task Queues

`RetrievalClient` uses Temporal Update-with-Start so the first command can atomically create the
store controller. Business IDs use hashed opaque components; a Temporal Run ID is never used as a
business identifier.

| Concern | Convention |
|---|---|
| Store controller | `store-controller/{opaque-store}` |
| Root sync | `store-sync/{opaque-store}/{generation}/{opaque-sequence}` |
| Remediation | `failed-user-remediation/{opaque-store}/{generation}/{opaque-partition}` |
| Deactivation | `store-deactivation/{opaque-store}/{generation}` |
| Shared quota | `user-quota/{opaque-provider-credential-class}` |
| Retrieval queue | `retrieval-v2` by default |
| Provider queue | `retrieval-provider-v2` by default |

A `command_id` deduplicates one logical command. A `sync_sequence` identifies one logical sync.
The quota `credential_key` is an opaque account identifier, never a credential value.

## Configuration reference

### Environment-file behavior

Supported executables inspect only `.env` in the current working directory. They never search
parent directories and importing a module never loads a file.

- `RETRIEVAL_ENV_FILE=<path>` selects an explicit file.
- `RETRIEVAL_ENV_FILE=` disables file loading.
- Process/container/platform variables win over file values, including an intentionally empty
  process value.
- `${NAME}` text remains literal; environment interpolation is disabled.
- A missing default `.env` is allowed. A missing explicit file is an error.

Environment files can select Python factories and can contain credentials. Treat them as trusted
configuration, keep them out of images/source control, and use mode `0600` when they contain
secrets.

### Temporal and worker variables

| Variable | Default | Meaning |
|---|---|---|
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal frontend address |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `TEMPORAL_API_KEY` | unset | Temporal Cloud credential |
| `TEMPORAL_TLS` | true with an API key; otherwise false | TLS mode |
| `TEMPORAL_RETRIEVAL_TASK_QUEUE` | `retrieval-v2` | Workflow and persistence queue |
| `TEMPORAL_PROVIDER_TASK_QUEUE` | `retrieval-provider-v2` | Provider Activity queue |
| `TEMPORAL_DEPLOYMENT_NAME` | `retrieval-v2` | Worker Deployment name |
| `TEMPORAL_BUILD_ID` | `local` | Immutable worker build identity |
| `TEMPORAL_USE_WORKER_VERSIONING` | `false` | Use deployment-based version routing |
| `TEMPORAL_REGISTER_LEGACY_DRAIN_TYPES` | `false` | Register optional drain-only names |
| `TEMPORAL_SERVER_PRIORITY_FAIRNESS_SUPPORTED` | `false` | Assert server support for priority/fairness |
| `TEMPORAL_ENABLE_SEARCH_ATTRIBUTES` | `false` | Attach registered typed Search Attributes |
| `RETRIEVAL_ADAPTER_BUNDLE_FACTORY` | unset | Typed bundle factory |
| `RETRIEVAL_REPOSITORY_FACTORY` | unset | Individual repository factory |
| `RETRIEVAL_STAGING_STORE_FACTORY` | unset | Individual staging factory |
| `RETRIEVAL_PROVIDER_GATEWAY_FACTORY` | unset | Individual provider factory |
| `RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS` | `false` | Explicit local-only fallback |

`TEMPORAL_WORKER_DEPLOYMENT_NAME` and `TEMPORAL_WORKER_BUILD_ID` are fallback names for platforms
that inject Worker Controller conventions. The primary names above take precedence.

### Google Drive variables

These variables apply when `RETRIEVAL_ADAPTER_BUNDLE_FACTORY` is
`retrieval.google_drive.bundle:create_adapter_bundle`.

| Variable | Default | Meaning |
|---|---|---|
| `GOOGLE_DRIVE_CREDENTIAL_KEY` | required | Opaque shared-quota identity; never a credential value |
| `GOOGLE_DRIVE_USER_KEY` | credential key | Stable opaque provider user identity |
| `GOOGLE_DRIVE_STAGING_DIRECTORY` | required | Absolute shared path for staged content and sync checkpoints |
| `GOOGLE_DRIVE_ROOT_FOLDER_ID` | unset | Optional folder subtree; unset lists every visible Drive file |
| `GOOGLE_DRIVE_CREDENTIALS_FILE` | ADC | Optional absolute service-account JSON secret path |
| `GOOGLE_DRIVE_SUBJECT` | unset | Optional delegated Workspace user for domain-wide delegation |
| `GOOGLE_DRIVE_MAX_FILE_BYTES` | `10485760` | Maximum downloaded source body size |
| `GOOGLE_DRIVE_REQUEST_TIMEOUT` | `60` | Drive HTTP request timeout in seconds |

See the [Google Drive integration guide](docs/google-drive-integration.md) before selecting a
staging volume or authentication mode.

### Workflow limits

| Variable | Default | Meaning |
|---|---:|---|
| `STORE_SYNC_MAX_ACTIVE_USERS` | 20 | Concurrent user children in ordinary mode |
| `STORE_SYNC_USER_PAGE_SIZE` | 100 | Provider users requested per page |
| `ROUND_USER_WINDOW_SIZE` | 20 | Users admitted per round |
| `ROUND_PAGE_SLICE_SIZE` | 5 | Pages attempted per user/round |
| `RESOURCE_CONCURRENCY` | 8 | Concurrent resources per user |
| `FILES_PAGE_WINDOW_SIZE` | 5 | Concurrent page children |
| `FILES_PER_PAGE_CONCURRENCY` | 10 | Per-page document ceiling |
| `DOCUMENT_INGESTION_CONCURRENCY` | 20 | Second document ceiling; lower ceiling wins |
| `OBJECT_CLEANUP_BATCH_SIZE` | 250 | Documents deleted by one cleanup Activity |
| `USER_QUOTA_MAX_IN_FLIGHT` | 4 | Reserved/active provider permits per scope |
| `USER_QUOTA_MAX_PENDING_REQUESTS` | 350 | Maximum queued permit requests |
| `USER_QUOTA_DEDUP_WINDOW_SIZE` | 2,000 | Retained terminal permit IDs |
| `USER_QUOTA_CONTINUE_AS_NEW_MESSAGE_COUNT` | 10,000 | Coordinator rollover threshold |
| `DEACTIVATION_DRAIN_TIMEOUT` | `5m` | Wait for controller-owned work |
| `TEMPORAL_ENABLE_PRIORITY_FAIRNESS` | `false` | Emit SDK scheduling metadata when supported |
| `TEMPORAL_PROVIDER_QUEUE_RPS` | unset | Worker-side provider Activity rate limit |
| `TEMPORAL_FAIRNESS_KEY_RPS_DEFAULT` | unset | Documented server target; not SDK-enforced |

Durations accept seconds or `ms`, `s`, `m`, and `h` suffixes. Invalid booleans, non-positive
limits, and unsafe quota relationships fail before startup.

### Lakebase/Postgres variables

Canonical `PG*` variables take precedence over `LAKEBASE_*` aliases.

| Variable | Default | Meaning |
|---|---|---|
| `PGHOST` / `LAKEBASE_HOST` | required | Database host |
| `PGPORT` / `LAKEBASE_PORT` | `5432` | Database port |
| `PGDATABASE` / `LAKEBASE_DATABASE` | required | Database name |
| `PGUSER` / `LAKEBASE_USER` | required | Database role |
| `PGSSLMODE` / `LAKEBASE_SSLMODE` | `require` | `require`, `verify-ca`, or `verify-full` |
| `LAKEBASE_ENDPOINT` | required for OAuth | Full Lakebase endpoint resource name |
| `PGPASSWORD` / `LAKEBASE_PASSWORD` | local alternative | Static password; mutually exclusive with endpoint OAuth |
| `LAKEBASE_POOL_MIN_SIZE` | `1` | Minimum pool size |
| `LAKEBASE_POOL_MAX_SIZE` | 10 App, 20 demo worker, 2 migration | Maximum pool size |
| `LAKEBASE_POOL_ACQUIRE_TIMEOUT_SECONDS` | `10` | Pool checkout timeout |
| `LAKEBASE_POOL_OPEN_TIMEOUT_SECONDS` | `30` | Initial pool readiness timeout |
| `LAKEBASE_POOL_MAX_IDLE_SECONDS` | `600` | Idle connection lifetime |
| `LAKEBASE_POOL_MAX_LIFETIME_SECONDS` | `3300` | Maximum connection lifetime |
| `LAKEBASE_POOL_RECONNECT_TIMEOUT_SECONDS` | `30` | Background reconnect timeout |
| `LAKEBASE_CONNECT_TIMEOUT_SECONDS` | `10` | Per-connection timeout |
| `LAKEBASE_STATEMENT_TIMEOUT_SECONDS` | `30` | Postgres statement timeout |
| `LAKEBASE_LOCK_TIMEOUT_SECONDS` | `5` | Postgres lock timeout |
| `LAKEBASE_HEALTH_CHECK_TIMEOUT_SECONDS` | `5` | Readiness-check timeout |
| `LAKEBASE_TRANSACTION_RETRY_LIMIT` | `3` | Retryable transaction attempts |
| `LAKEBASE_APPLICATION_NAME` | `temporal-retrieval-v2` | Postgres connection label |
| `RETRIEVAL_SEARCH_BACKEND` | `postgres_text` | Supported search implementation |

The Lakebase pool obtains a fresh Databricks OAuth database token for each new physical
connection. `lakebase_hybrid` is reserved but not implemented; selecting it fails explicitly.

### Northstar App variables

| Variable | Default | Meaning |
|---|---|---|
| `RETRIEVAL_DEMO_MODE` | `false` | Enable demo adapters and endpoints |
| `RETRIEVAL_DEMO_SCENARIO` | `northstar-v1` | Packaged scenario ID |
| `RETRIEVAL_DEMO_HOLD_TIMEOUT_SECONDS` | `30` | Maximum pre-commit hold |
| `RETRIEVAL_DEMO_CONTROL_POLL_SECONDS` | `0.25` | Cross-process control poll interval |
| `RETRIEVAL_DEMO_STORE_KEY_PREFIX` | `northstar` | Required seed-function prefix |
| `DATABRICKS_APP_PORT` | `8000` locally | App port; host is always `0.0.0.0` |
| `TEMPORAL_WEB_BASE_URL` | unset | Optional Temporal Web base URL/template |

Every App `POST` request also needs an `Idempotency-Key` header. The database stores only its hash,
the request hash, the durable response, and operation identity.

## Database objects and ownership

Core migrations create the `retrieval` schema:

- `stores`: lifecycle state and current generation;
- `store_users` and `retrieval_state`: synchronized per-store state;
- `documents` and `document_chunks`: current searchable content;
- `write_receipts`: Activity idempotency records;
- `schema_migrations`: migration names, versions, and checksums.

Demo migrations create `retrieval_demo_ui`:

- `demo_runs`, `demo_controls`, `demo_events`, and `demo_operations`;
- `api_idempotency` for durable HTTP request receipts;
- `schema_migrations` for the demo schema;
- `create_northstar_run(...)`, a fixed-purpose `SECURITY DEFINER` function.

The migration identity owns both schemas. The App gets core reads and limited demo writes; the
worker gets core mutations and limited demo control/event access. Exact grants are in
`src/retrieval/lakebase/grants.py` and the [migration runbook](docs/runbooks/migration-and-rollback.md).

## Non-negotiable invariants

- Every retrieval mutation validates lifecycle state and generation in its database transaction.
- Deactivation commits the new generation before cancellation and cleanup.
- Same idempotency key plus the same canonical payload is a duplicate success only while its
  generation remains current and writable; conflicting reuse fails.
- After a fence, stale generation wins over any historical receipt.
- Search returns only current-generation content from readable lifecycle states.
- Cleanup uses bounded batches and cannot mark a store inactive until owned rows are zero.
- Workflow history contains document references, never bodies or chunks.
- Provider waits are durable and consume no Activity worker slot.
- Credentials, document bodies, and raw idempotency keys are excluded from metrics and demo events.

For execution diagrams, read [workflow topology](docs/workflow-topology.md). For the rationale
behind these boundaries, read [ADR 0001](docs/adr/0001-workflow-boundaries.md).

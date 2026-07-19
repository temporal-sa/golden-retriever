# Codebase and configuration guide

Use this page to locate a runtime component, understand its ownership boundary, and find the
environment variable that configures it.

## Repository layout

| Path | Responsibility |
|---|---|
| `src/retrieval/environment.py` | Exact-working-directory environment-file injection for executable processes |
| `src/retrieval/config.py` | Validated workflow concurrency, quota, cleanup, timeout, and fairness settings |
| `src/retrieval/content.py` | Strict UTF-8/frontmatter parsing and deterministic paragraph chunking |
| `src/retrieval/temporal/client.py` | Application-facing `RetrievalClient` and controller commands |
| `src/retrieval/temporal/runtime_config.py` | Temporal connection, queues, versioning, and adapter selection |
| `src/retrieval/temporal/worker.py` | Workflow/Activity registration, `AdapterBundle`, and worker entry point |
| `src/retrieval/temporal/models/` | Compact, JSON-converter-safe workflow inputs and results |
| `src/retrieval/temporal/common/` | Stable IDs, quota waiting, metrics, priorities, and Search Attributes |
| `src/retrieval/temporal/activities/` | Provider, lifecycle, ingestion, cleanup, quota bridge, and adapter ports |
| `src/retrieval/temporal/workflows/` | All registered Workflow implementations |
| `src/retrieval/lakebase/` | Lakebase configuration, OAuth-aware async pool, repository, search, and core migrations |
| `src/retrieval/demo/` | Northstar fixtures, scripted provider, controls/events, hold hook, service, and demo migrations |
| `apps/retrieval_demo/` | FastAPI App, static four-panel UI, App manifest, and Databricks bundle |
| `Dockerfile.worker` | Separate long-lived Temporal worker image |
| `app.yaml`, `requirements.txt` | Effective Databricks Apps root manifest and pinned App dependencies |
| `.env.example` | Non-secret local full-stack configuration checklist |
| `Makefile` | Repeatable install, verify, integration, replay, headless, and App commands |
| `tests/unit`, `tests/contract` | Fast deterministic correctness and repository-port tests |
| `tests/lakebase`, `tests/demo`, `tests/app` | SQL/pool/search, Northstar, and HTTP application tests |
| `tests/integration`, `tests/replay`, `tests/load` | Opt-in Temporal execution, history replay, and synthetic load harnesses |

## Runtime entry points

| Command | What it runs |
|---|---|
| `uv run retrieval-worker` | Retrieval and provider Temporal workers in one process |
| `uv run retrieval-test-starter` | Isolated local Temporal sync/deactivation smoke |
| `uv run retrieval-lakebase-migrate` | Apply or check the forward-only `retrieval` schema |
| `uv run retrieval-demo-migrate` | Apply or check the forward-only `retrieval_demo_ui` schema |
| `uv run retrieval-lakebase-grant-roles` | Apply explicit App/worker grants after both migrations |
| `uv run retrieval-demo-headless --json` | No-service Northstar data-plane rehearsal |
| `uv run retrieval-demo-app` | FastAPI/static UI process on `DATABRICKS_APP_PORT` or 8000 |
| `RetrievalClient` | Submit idempotent sync, cancel, deactivation, and status operations from Python |

The App never hosts a Temporal worker. `retrieval-worker` starts two SDK workers: all Workflow Types
and persistence Activities poll the retrieval queue; provider Activities poll the provider queue.

The environment-consuming commands (`retrieval-worker`, both migration commands, the grant command,
and `retrieval-demo-app`) load process configuration once at startup. `retrieval-test-starter` and
`retrieval-demo-headless` are self-contained and do not load an environment file.

## Workflow inventory

`retrieval.temporal.worker.V2_WORKFLOW_TYPES` is the authoritative registry. The replay registry
mirrors these 17 types.

| Workflow | Responsibility | Relationship |
|---|---|---|
| `StoreControllerWorkflow` | One lifecycle and operation authority per store | Long-lived; starts detached work |
| `RootSyncWorkflow` | Enumerate users by page or bounded round and aggregate progress | Detached from controller |
| `FailedUserRemediationWorkflow` | Retry bounded failed-user batches | Detached; controller-tracked |
| `ActivateUserWorkflow` | Recent sync, generation recheck, backfill, activation | Joined by remediation |
| `UserSyncWorkflow` | Bounded resource fan-out | Joined |
| `ResourceSyncWorkflow` | Own one resource cursor and page policy | Joined |
| `ResourcePagesWorkflow` | Sliding page window and safe checkpoint | Joined |
| `FilesPageWorkflow` | Bounded document upsert/delete fan-out | Joined |
| `DocumentIngestionWorkflow` | One staged, idempotent, generation-fenced mutation | Joined |
| `CommentsResyncWorkflow` | Optional direct comments boundary | Joined delegate |
| `UserQuotaWorkflow` | Shared provider permits, reset state, and durable waits | Shared, long-lived |
| `DeactivateStoreWorkflow` | Fence, cancel, drain, user cleanup, object cleanup, inactive | Detached from controller |
| `CleanupUsersWorkflow` | Bounded user cleanup routing | Joined |
| `DeactivateUserWorkflow` | Route one user cleanup | Joined |
| `DeactivateOneUserWorkflow` | One user-deactivation Activity | Joined |
| `DeactivateAllUsersWorkflow` | All-user deactivation Activity | Joined |
| `RemoveObjectsWorkflow` | Bounded object cleanup with progress/Continue-As-New | Joined |

`QuotaWaitWorkflow` and `AccessioningWorkflow` are optional drain-only names. No current path starts
them. Register them only when a compatible deployment must drain known histories; the placeholders
cannot replay arbitrary older implementations.

## Activity ports and adapters

Workflow code performs no database, filesystem, provider, or wall-clock I/O. Activities call these
ports:

| Port/hook | Included implementations |
|---|---|
| `RetrievalRepository` | `InMemoryRetrievalRepository`; `LakebaseRetrievalRepository` |
| `StagingStore` | `InMemoryStagingStore`; manifest-restricted `FixtureStagingStore` |
| `ProviderGateway` | `EmptyProviderGateway`; `ScriptedNorthstarProvider` |
| `BeforeDocumentCommitHook` | no-op; Northstar pre-commit hold gate |
| `IngestionEventSink` | no-op; durable Northstar event sink |

`AdapterBundle` owns one process-wide set of ports/hooks and closes each unique resource on worker
shutdown. The demo bundle uses one Lakebase pool and is loaded with:

```text
RETRIEVAL_ADAPTER_BUNDLE_FACTORY=retrieval.demo.scripted_provider:create_adapter_bundle
```

Adapter selection is mutually exclusive:

1. `RETRIEVAL_ADAPTER_BUNDLE_FACTORY` may be set by itself.
2. Otherwise, all three individual factory variables must be set together.
3. Otherwise, local execution requires `RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS=true`.
4. Every other combination fails worker startup.

Individual factories use `module:function` and may return their value synchronously or
asynchronously:

```text
RETRIEVAL_REPOSITORY_FACTORY=package.module:create_repository
RETRIEVAL_STAGING_STORE_FACTORY=package.module:create_staging_store
RETRIEVAL_PROVIDER_GATEWAY_FACTORY=package.module:create_provider_gateway
```

## Commands, IDs, and Task Queues

`RetrievalClient` uses Update-with-Start so the first command can atomically create the store
controller. A `command_id` identifies one logical command and is retained for bounded
deduplication. A `sync_sequence` identifies one logical sync.

| Concern | Stable convention |
|---|---|
| Controller | `store-controller/{opaque-store}` |
| Root sync | `store-sync/{opaque-store}/{generation}/{opaque-sequence}` |
| Remediation | `failed-user-remediation/{opaque-store}/{generation}/{opaque-partition}` |
| Deactivation | `store-deactivation/{opaque-store}/{generation}` |
| Shared quota | `user-quota/{opaque-provider-credential-class}` |
| Retrieval queue | `TEMPORAL_RETRIEVAL_TASK_QUEUE`, default `retrieval-v2` |
| Provider queue | `TEMPORAL_PROVIDER_TASK_QUEUE`, default `retrieval-provider-v2` |

Business ID components are SHA-256/base32-derived. A Temporal Run ID is never used as business
identity. The `credential_key` in a quota scope is an opaque account identifier, never a token.

## Temporal process configuration

### Environment-file injection

The supported executable entry points inspect exactly `.env` in their current working directory.
They never search parent directories and never load a file merely because a Python module or config
class was imported. Set `RETRIEVAL_ENV_FILE` to select an explicit absolute or working-directory-
relative file; set it to an empty string to disable loading. A missing default file is a no-op, while
a missing or non-file explicit path fails startup.

File values only fill names absent from the existing process environment, including preserving an
existing empty value. Shell, container, and Databricks resource/secret injection therefore remain
authoritative. `${NAME}` expressions are intentionally left literal rather than interpolating
process secrets. Environment files are trusted local configuration because they can select Python
adapter factories; keep them out of source/images and restrict files containing secrets to mode
`0600`.

| Environment variable | Default | Meaning |
|---|---|---|
| `RETRIEVAL_ENV_FILE` | exact working-directory `.env` | Explicit environment file; empty disables loading |
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal frontend |
| `TEMPORAL_NAMESPACE` | `default` | Workflow namespace |
| `TEMPORAL_API_KEY` | unset | API credential |
| `TEMPORAL_TLS` | `true` when an API key is set, otherwise `false` | TLS connection mode |
| `TEMPORAL_RETRIEVAL_TASK_QUEUE` | `retrieval-v2` | Workflow and persistence queue |
| `TEMPORAL_PROVIDER_TASK_QUEUE` | `retrieval-provider-v2` | Provider Activity queue |
| `TEMPORAL_DEPLOYMENT_NAME` | `retrieval-v2` | Worker Deployment name |
| `TEMPORAL_BUILD_ID` | `local` | Build identity |
| `TEMPORAL_USE_WORKER_VERSIONING` | `false` | Pinned deployment versioning |
| `TEMPORAL_REGISTER_LEGACY_DRAIN_TYPES` | `false` | Register optional drain-only names |
| `TEMPORAL_SERVER_PRIORITY_FAIRNESS_SUPPORTED` | `false` | Operator assertion of server capability |
| `TEMPORAL_ENABLE_SEARCH_ATTRIBUTES` | `false` | Attach registered typed attributes |
| `RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS` | `false` | Explicit local-only adapter opt-in |
| `RETRIEVAL_ADAPTER_BUNDLE_FACTORY` | unset | Typed bundle factory, mutually exclusive with the settings below |
| `RETRIEVAL_REPOSITORY_FACTORY` | unset | Repository factory |
| `RETRIEVAL_STAGING_STORE_FACTORY` | unset | Staging factory |
| `RETRIEVAL_PROVIDER_GATEWAY_FACTORY` | unset | Provider factory |

## Workflow tuning

| Environment variable | Default | Meaning |
|---|---:|---|
| `STORE_SYNC_MAX_ACTIVE_USERS` | 20 | Ordinary-mode concurrent user children |
| `STORE_SYNC_USER_PAGE_SIZE` | 100 | Users requested per provider page |
| `ROUND_USER_WINDOW_SIZE` | 20 | Users admitted per round |
| `ROUND_PAGE_SLICE_SIZE` | 5 | Pages attempted per user and round |
| `RESOURCE_CONCURRENCY` | 8 | Concurrent resources per user |
| `FILES_PAGE_WINDOW_SIZE` | 5 | Concurrent page children |
| `FILES_PER_PAGE_CONCURRENCY` | 10 | Per-page document ceiling |
| `DOCUMENT_INGESTION_CONCURRENCY` | 20 | Second document ceiling; the lower ceiling wins |
| `OBJECT_CLEANUP_BATCH_SIZE` | 250 | Documents deleted per bounded cleanup Activity |
| `USER_QUOTA_MAX_IN_FLIGHT` | 4 | Reserved/in-flight provider permits per scope |
| `USER_QUOTA_MAX_PENDING_REQUESTS` | 350 | Pending permit ceiling; never above 350 |
| `USER_QUOTA_DEDUP_WINDOW_SIZE` | 2,000 | Recent terminal permit IDs retained |
| `USER_QUOTA_CONTINUE_AS_NEW_MESSAGE_COUNT` | 10,000 | Quota coordinator rollover threshold |
| `DEACTIVATION_DRAIN_TIMEOUT` | `5m` | Bounded wait for controller-owned work |
| `TEMPORAL_ENABLE_PRIORITY_FAIRNESS` | `false` | Emit SDK scheduling metadata when server support is also asserted |
| `TEMPORAL_PROVIDER_QUEUE_RPS` | unset | Worker-side provider Activity rate limit |
| `TEMPORAL_FAIRNESS_KEY_RPS_DEFAULT` | unset | Documented server target; logged, not SDK-enforced |

Durations accept seconds or an `ms`, `s`, `m`, or `h` suffix. Invalid booleans, non-positive
limits, and unsafe quota relationships fail before worker startup.

## Lakebase/Postgres configuration

Canonical `PG*` variables win over their `LAKEBASE_*` aliases.

| Environment variable | Default | Meaning |
|---|---|---|
| `PGHOST` / `LAKEBASE_HOST` | required | Database host |
| `PGPORT` / `LAKEBASE_PORT` | `5432` | Database port |
| `PGDATABASE` / `LAKEBASE_DATABASE` | required | Database name |
| `PGUSER` / `LAKEBASE_USER` | required | Database role |
| `PGSSLMODE` / `LAKEBASE_SSLMODE` | `require` | Must be `require`, `verify-ca`, or `verify-full` |
| `LAKEBASE_ENDPOINT` | required for OAuth | Full `projects/.../branches/.../endpoints/...` resource path |
| `PGPASSWORD` / `LAKEBASE_PASSWORD` | local alternative only | Static password; mutually exclusive with endpoint OAuth |
| `LAKEBASE_POOL_MIN_SIZE` | `1` | Minimum pool size |
| `LAKEBASE_POOL_MAX_SIZE` | caller default (`10` App, `20` demo worker, `2` migrations) | Maximum pool size |
| `LAKEBASE_POOL_ACQUIRE_TIMEOUT_SECONDS` | `10` | Pool checkout timeout |
| `LAKEBASE_POOL_OPEN_TIMEOUT_SECONDS` | `30` | Initial pool readiness timeout |
| `LAKEBASE_POOL_MAX_IDLE_SECONDS` | `600` | Idle connection lifetime |
| `LAKEBASE_POOL_MAX_LIFETIME_SECONDS` | `3300` | Maximum connection lifetime, below OAuth token lifetime |
| `LAKEBASE_POOL_RECONNECT_TIMEOUT_SECONDS` | `30` | Background reconnect timeout |
| `LAKEBASE_CONNECT_TIMEOUT_SECONDS` | `10` | Per-connection timeout |
| `LAKEBASE_STATEMENT_TIMEOUT_SECONDS` | `30` | Session statement timeout |
| `LAKEBASE_LOCK_TIMEOUT_SECONDS` | `5` | Session lock timeout |
| `LAKEBASE_HEALTH_CHECK_TIMEOUT_SECONDS` | `5` | Readiness check timeout |
| `LAKEBASE_TRANSACTION_RETRY_LIMIT` | `3` | Retry limit for retryable transaction failures |
| `LAKEBASE_APPLICATION_NAME` | `temporal-retrieval-v2` | Postgres connection label |
| `RETRIEVAL_SEARCH_BACKEND` | `postgres_text` | `postgres_text`; `lakebase_hybrid` is an explicit unavailable placeholder unless implemented/configured |

The pool obtains a fresh Databricks OAuth database token for every new physical connection. It is
opened, waited, health-checked, and closed explicitly by the owning process.

## Northstar and App configuration

| Environment variable | Default | Meaning |
|---|---|---|
| `RETRIEVAL_DEMO_MODE` | `false` | Required opt-in for all demo adapters and endpoints |
| `RETRIEVAL_DEMO_SCENARIO` | `northstar-v1` | Packaged scenario ID |
| `RETRIEVAL_DEMO_HOLD_TIMEOUT_SECONDS` | `30` | Held-write maximum; cannot exceed 30 seconds |
| `RETRIEVAL_DEMO_CONTROL_POLL_SECONDS` | `0.25` | Cross-process control polling interval |
| `RETRIEVAL_DEMO_STORE_KEY_PREFIX` | `northstar` | Compatibility invariant for the fixed seed function; any other value is rejected |
| `DATABRICKS_APP_PORT` | `8000` locally | App listener port; bind address is always `0.0.0.0` |
| `TEMPORAL_WEB_BASE_URL` | unset | Optional origin or template using `{namespace}` and `{workflow_id}` for UI deep links |

Every App `POST` also requires an `Idempotency-Key` HTTP header. Its hash, request hash, response,
and operation identity are stored in `retrieval_demo_ui.api_idempotency`.

## Lakebase schemas and mutation boundaries

Core migrations create:

- `retrieval.stores`, `store_users`, and `retrieval_state` for authoritative lifecycle state;
- `retrieval.documents` and `document_chunks` for current searchable content;
- `retrieval.write_receipts` for Activity idempotency;
- `retrieval.schema_migrations` and the generated English `tsvector`/GIN index.

Demo migrations create `demo_runs`, `demo_controls`, `demo_events`, `demo_operations`,
`api_idempotency`, `schema_migrations`, and `create_northstar_run(...)` in
`retrieval_demo_ui`. The migration identity owns both schemas. The App can seed only the fixed
Northstar shape through the revoked-by-default `SECURITY DEFINER` function.

Search always joins chunks to the current store row and only returns active/syncing current-
generation content. Deactivating, inactive, or stale rows are not visible even before cleanup
finishes.

## Sync command policy

The client copies validated process settings into string-valued `SyncCommand.metadata` unless the
caller supplies a compatible override.

| Metadata | Default | Purpose |
|---|---|---|
| `mode` | `ordinary` | Page barriers; `round` enables bounded user slices |
| `resource_types` | `files` | Comma-separated resources |
| `provider` + `credential_key` | unset | Together select a shared quota scope |
| `quota_class` | `default` | Provider quota bucket |
| `fairness_weight` | `1` | Relative scheduling weight from 0.001 to 1000 |
| `max_active_users` | configured | Ordinary user bound |
| `user_page_size` | configured | Provider user page size |
| `round_user_window_size` | configured | Round user bound |
| `round_page_slice_size` | configured | Round page slice |
| `resource_concurrency` | configured | Resource bound |
| `files_page_window_size` | configured | Page bound |
| `files_per_page_concurrency` | configured | File-page document bound |
| `document_ingestion_concurrency` | configured | Second document bound |
| `provider_page_size` | `100` | Resource page request size |
| `activation_recent_page_cap` | `5` | Recent pages before activation backfill |

## Core invariants

- Every retrieval write and cleanup compares generation and allowed state in its transaction.
- Deactivation commits the new generation before cancellation and cleanup.
- While a generation is current/writable, same-key/same-payload Activity retry is idempotent and
  conflicting key reuse is rejected; after a fence, stale generation takes precedence.
- Cleanup is bounded by `OBJECT_CLEANUP_BATCH_SIZE`; inactive requires zero users, state, documents,
  and chunks.
- Search excludes stale generation and non-readable lifecycle states.
- Fan-out is finite and joined; detached work has stable IDs and controller ownership.
- Provider quota waits are durable and consume no Activity worker slot.
- Workflow history contains document references, never document bodies or chunks.
- Metrics and demo events exclude credentials and document bodies.

See [`docs/workflow-topology.md`](docs/workflow-topology.md) for the execution diagrams and
[`docs/runbooks/migration-and-rollback.md`](docs/runbooks/migration-and-rollback.md) for grants and
rollout order.

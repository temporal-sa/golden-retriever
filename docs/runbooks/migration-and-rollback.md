# Database migration, upgrade, and rollback runbook

This runbook explains how to create, verify, upgrade, and recover the Postgres schemas used by
the system. It assumes the reader knows only that Lakebase is Postgres. Read the
[system specification](../lakebase-temporal-demo-spec.md#database-schemas) for the data model.

## Safety model

- Migrations are forward-only and checksum-verified.
- One migration identity owns both schemas.
- App and worker identities receive explicit object privileges but no schema ownership.
- An advisory transaction lock prevents concurrent migration runners.
- Applied migration files and checksums are immutable.
- Database recovery never decrements a committed store generation without a full, approved
  recovery/reconciliation plan.

## Identities

| Identity | Purpose |
|---|---|
| Migration owner | Own schemas, tables, hybrid indexes, ledgers, and constrained functions; apply grants |
| App role | Read core state/search; write demo state through constrained privileges |
| Worker role | Perform generation-fenced retrieval DML and limited demo control/event DML |

Keep all three distinct. The App creates a demo run through
`retrieval_demo_ui.create_demo_run(...)`, not direct core table DML.

## Configure a migration connection

For Lakebase OAuth, authenticate the Databricks SDK and set:

```bash
export DATABRICKS_CONFIG_PROFILE=<EXPLICIT_OAUTH_PROFILE>
export PGHOST=<LAKEBASE_ENDPOINT_HOST>
export PGPORT=5432
export PGDATABASE=<POSTGRES_DATABASE_NAME>
export PGUSER=<MIGRATION_OWNER_POSTGRES_ROLE>
export PGSSLMODE=require
export LAKEBASE_ENDPOINT=projects/<PROJECT>/branches/<BRANCH>/endpoints/<ENDPOINT>
export LAKEBASE_POOL_MIN_SIZE=1
export LAKEBASE_POOL_MAX_SIZE=2
unset PGPASSWORD LAKEBASE_PASSWORD
```

For a non-Lakebase SSL Postgres test database, omit `LAKEBASE_ENDPOINT` and set `PGPASSWORD`
through a secret-safe mechanism. Never set OAuth endpoint and static password together.

The executable CLIs load `.env` in the current directory unless `RETRIEVAL_ENV_FILE` selects a
different file. Process variables win. Use separate protected environment files when testing each
identity.

## Initialize a new database

### 1. Inspect status

These commands do not create or change schemas:

```bash
uv run retrieval-lakebase-migrate --check --json
uv run retrieval-demo-migrate --check --json
```

A new database returns nonzero with pending versions. An initialized database verifies stored
names and checksums before returning readiness.

### 2. Apply schemas in order

Run as the migration owner:

```bash
uv run retrieval-lakebase-migrate
uv run retrieval-demo-migrate
```

Core migrations create `retrieval`, durable `retrieval_connector` state, and Lakebase Search BM25/
ANN indexes. Demo migrations create `retrieval_demo_ui` plus constrained run/proof functions.
Lakebase Search Beta must be enabled before applying core migration 5; the migration fails closed
when its index access methods are unavailable.

### 3. Apply runtime grants

The App and worker Postgres roles must already exist:

```bash
uv run retrieval-lakebase-grant-roles \
  --app-role <APP_POSTGRES_ROLE> \
  --worker-role <WORKER_POSTGRES_ROLE>
```

The command validates nonempty/distinct role names, quotes them as SQL identifiers, and applies all
grants in one transaction. It is safe to rerun.

The App receives:

- `USAGE` on both schemas;
- core lifecycle/document/chunk/migration-ledger `SELECT`;
- demo table `SELECT` plus the required `INSERT`/`UPDATE` operations;
- demo sequence use;
- `EXECUTE` on constrained `create_demo_run` and `generation_proof` functions;
- write access to durable preflight/idempotency/operation presentation state;
- no direct core table DML from this grant set.

The worker receives:

- `USAGE` on both schemas and core ledger `SELECT`;
- required core reads, inserts, updates, and deletes;
- connector staging/checkpoint DML and `MAINTAIN` on chunks for synchronized index refresh;
- demo run/control reads, control updates, event reads/writes, and sequence use;
- no schema ownership or general DDL from this grant set.

Review `src/retrieval/lakebase/grants.py` whenever a migration adds a runtime-accessed object.
Explicit grants do not automatically include future tables.

### 4. Verify readiness

```bash
uv run retrieval-lakebase-migrate --check --json
uv run retrieval-demo-migrate --check --json
```

Both results must contain `"ready": true`, no pending versions, and no checksum drift.

### 5. Verify effective privileges

Reconnect separately as App and worker. Use transactions that are rolled back or a disposable
store. Confirm:

- App can read core state/search, write allowed demo objects, and call constrained functions;
- App cannot insert/update/delete core retrieval tables;
- worker can run repository mutations/cleanup and demo control/event operations;
- worker cannot create/alter schemas or objects;
- neither runtime owns the schemas;
- App database-level `CREATE` is recorded explicitly.

The Databricks App `postgres` binding with `CAN_CONNECT_AND_CREATE` grants database-level
`CONNECT` and `CREATE` in addition to repository object grants. Use a dedicated database. If local
policy forbids runtime DDL, the migration owner may run:

```sql
REVOKE CREATE ON DATABASE <database_name> FROM <app_role>;
```

Do this only after App binding, then reconnect as the App and prove schema creation fails while
normal readiness/search succeeds. Recheck after every resource-binding update.

`/readyz` verifies connectivity and migration ledgers; it does not perform negative privilege
tests.

## Run a Lakebase-connected full-stack rehearsal

Start Temporal:

```bash
temporal server start-dev
```

Start the worker with the worker database identity:

```bash
export TEMPORAL_ADDRESS=localhost:7233
export TEMPORAL_NAMESPACE=default
export TEMPORAL_TLS=false
export RETRIEVAL_DEMO_MODE=true
export RETRIEVAL_SEARCH_BACKEND=lakebase_hybrid
export RETRIEVAL_EMBEDDING_DIMENSION=1024
export DATABRICKS_EMBEDDING_ENDPOINT=<1024-dimensional-endpoint>
export RETRIEVAL_ADAPTER_BUNDLE_FACTORY=retrieval.demo.scripted_provider:create_adapter_bundle
uv run retrieval-worker
```

Start the App with the App database identity:

```bash
export RETRIEVAL_DEMO_MODE=true
export RETRIEVAL_SEARCH_BACKEND=lakebase_hybrid
export RETRIEVAL_EMBEDDING_DIMENSION=1024
export DATABRICKS_EMBEDDING_ENDPOINT=<1024-dimensional-endpoint>
uv run retrieval-demo-app
```

Verify:

```bash
curl --fail http://127.0.0.1:8000/healthz
curl --fail http://127.0.0.1:8000/readyz
```

Create a fresh run for every rehearsal. Never reset an old run's generation. The scripted provider
is only a local source substitute; production uses the Google Drive bundle and Lakebase connector
state.

## Add a schema change

1. Identify whether the change belongs to `retrieval` or `retrieval_demo_ui`.
2. Add the next contiguous, zero-padded migration file in the matching migration directory.
3. Do not edit any migration that may have been applied.
4. Make the change safe for existing data and compatible with both old/new runtime builds during
   rollout, or document the required admission stop.
5. Update `grants.py` when a runtime identity needs the new object.
6. Add migration discovery/checksum/SQL tests and repository/App tests.
7. Run `make verify`, integration tests, and replay tests.
8. Rehearse on a short-lived branch restored/copied from representative data.

## Pre-rollout verification

For the exact source revision and dependency lock:

```bash
make verify
make integration
RUN_TEMPORAL_INTEGRATION=1 uv run pytest -q tests/demo/test_temporal_late_writer.py
uv run pytest -m replay tests/replay
uv build
docker build -f Dockerfile.worker -t retrieval-worker:<build-id> .
```

Archive:

- source revision and dependency lock;
- test/replay results and history inventory;
- migration status before/after;
- reviewed SQL/checksums and grant diff;
- database branch/backup recovery point;
- worker image digest/build ID;
- bundle validation and App resource diff;
- approval, canary, stop, and rollback owners.

## Upgrade a running environment

1. Inventory open workflows by type, queue, and worker build.
2. Replay representative target histories on the new code.
3. Create the approved database branch/backup recovery point.
4. Stop or limit new commands if the schema change is not mixed-version compatible.
5. Apply new core migrations, then demo migrations, as the owner.
6. Rerun grants and all effective privilege checks.
7. Start at least two new worker replicas and verify both queues.
8. Route a deterministic canary through the reviewed Worker Versioning procedure.
9. Deploy/start the compatible App and run a fresh Google Drive rehearsal.
10. Expand only while named SLO/stop thresholds pass.
11. Keep the previous compatible workers until their executions drain.

## Rollback and recovery

Rollback changes admission and routing; it does not erase Temporal history or rewind database
generations.

### App or worker regression

1. Stop new commands or new-work routing to the affected build.
2. Keep every worker build that owns open executions running.
3. Restore the last verified compatible App/worker artifacts.
4. Verify both Task Queues, quota recovery, database readiness, and controller commands.
5. Preserve failed-build histories, logs, and migration state for diagnosis.

### Faulty schema migration

There are no down migrations.

- Prefer a forward corrective migration when existing data is valid.
- If restoration is required, stop admission, preserve compatible workers, restore through the
  approved Lakebase branch/backup procedure, and reconcile every store lifecycle/generation before
  resuming.
- Never edit the migration ledger or an applied file/checksum.
- Replay histories and rerun contract/readiness tests against the recovered database.

### Incomplete deactivation

Determine whether the generation fence committed:

- **before fence:** store remains `active`/`syncing`; repair the dependency and retry;
- **after fence:** preserve the new generation, repair the dependency, and resume cleanup with the
  same generation and stable deactivation identity.

Never decrement the generation and never treat cancellation as the safety mechanism.

### Provider quota recovery

After an ambiguous provider call, do not refund capacity. Wait for the next authoritative quota
observation/reset. Diagnose bounded quota workflow state and metrics without logging the credential
key.

## Retire an old worker build

Remove a build only after:

- Temporal visibility shows no open execution assigned to it;
- representative retained histories replay on intended builds;
- the required retention/replay window has elapsed;
- rollback owners agree it is no longer part of recovery.

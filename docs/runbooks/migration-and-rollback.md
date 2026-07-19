# Migration, validation, rollout, and rollback runbook

This runbook covers the built Lakebase schemas, Databricks App bundle, and separate Temporal worker.
Commands in the validation sections are safe preparation steps; they do not deploy the App.

No target deployment was performed while preparing this repository. Always substitute an explicit
workspace profile, database roles, resource paths, secret scope, and namespace reviewed for the
target environment.

## Identities and ownership

Use three distinct database identities:

| Identity | Steady-state purpose |
|---|---|
| Migration owner | Owns both `retrieval` and `retrieval_demo_ui`, migrations, tables, indexes, and seed function |
| App role | Core reads, demo DML, sequence use, fixed Northstar seed function |
| Worker role | Core retrieval DML plus limited demo controls/events |

The App does not own or create `retrieval_demo_ui`. Both migrations run as the migration owner.
Core table DML is withheld from the App; it creates a run only through the constrained
`retrieval_demo_ui.create_northstar_run(...)` `SECURITY DEFINER` function. Public access is revoked
by the demo migration.

## Connection environment

All migration/grant CLIs use `LakebaseConfig`. For Lakebase OAuth:

```bash
export PGHOST=<DATABASE_HOST>
export PGPORT=5432
export PGDATABASE=<DATABASE_NAME>
export PGUSER=<MIGRATION_OWNER_ROLE>
export PGSSLMODE=require
export LAKEBASE_ENDPOINT=projects/<PROJECT>/branches/<BRANCH>/endpoints/<ENDPOINT>
```

The Databricks SDK must be authenticated for that endpoint. For an SSL-enabled local Postgres test
database, omit `LAKEBASE_ENDPOINT` and set `PGPASSWORD` instead. Never set both endpoint OAuth and a
static password. Do not paste a password into a command line or commit an environment file.

## First-time schema and grant order

### 1. Inspect migration status

These commands make no schema changes:

```bash
uv run retrieval-lakebase-migrate --check --json
uv run retrieval-demo-migrate --check --json
```

On a new database they return nonzero with pending versions. On an initialized database they also
verify that every stored name/checksum matches the packaged migration.

### 2. Apply core, then demo migrations

Run as the migration owner:

```bash
uv run retrieval-lakebase-migrate
uv run retrieval-demo-migrate
```

Both runners are forward-only, checksum-verified, and serialized by advisory transaction locks.
Never edit an applied migration. Add the next contiguous migration instead.

### 3. Apply runtime grants

The database roles must already exist. Still using the migration owner connection:

```bash
uv run retrieval-lakebase-grant-roles \
  --app-role <APP_DB_ROLE> \
  --worker-role <WORKER_DB_ROLE>
```

The command validates that roles are nonempty/distinct, composes them as quoted SQL identifiers,
and applies the explicit grants in one transaction. It is safe to rerun after migrations.

The grant set gives the App:

- schema `USAGE`;
- `SELECT` on core stores, users, retrieval state, documents, chunks, and the core migration
  ledger—enough for store aggregates, search, and readiness, but not write receipts;
- `SELECT` on demo tables, `UPDATE` on runs/controls, and `INSERT`/`UPDATE` on events, operations,
  and HTTP idempotency receipts;
- demo sequence use; and
- `EXECUTE` on the fixed Northstar seed function.

It gives the worker:

- schema `USAGE` and core migration-ledger `SELECT`;
- core table reads; inserts/updates on stores, users, retrieval state, and documents; inserts on
  chunks/receipts; and deletes on retrieval state, documents, and chunks; and
- demo run/control reads, control updates, and event reads/inserts/updates plus sequence use.

The worker role receives neither schema ownership nor general DDL. The explicit object grants do
not give the App role schema ownership or table-alter privileges. However, a Databricks App
`postgres` resource with `CAN_CONNECT_AND_CREATE` also grants its service principal database-level
`CONNECT` and `CREATE`; the grant command does not revoke that platform permission. Consequently,
the App can create schemas in the bound database even though this application never needs to.
Review `src/retrieval/lakebase/grants.py` whenever a migration adds a runtime-accessed object;
PostgreSQL does not automatically extend these explicit grants to future tables.

### 4. Verify with the runtime identities

Open a fresh connection as each role and run the migration checks. Then verify permissions with a
transaction that is rolled back or a dedicated disposable store. At minimum confirm:

- App: core aggregate/search reads, demo reads/writes, seed function execution, and core DML denial;
- worker: repository mutations and cleanup, scripted quota controls, and event appends;
- worker: no schema creation or object-alter privilege;
- App: no core/demo object-alter privilege, and explicitly record whether database `CREATE` remains
  effective through the Databricks resource binding.

Treat the bound Lakebase database as a dedicated isolation boundary. If organizational policy
forbids runtime schema creation, have the migration owner run `REVOKE CREATE ON DATABASE
<database> FROM <app_role>` only after the App resource is bound, then reconnect as the App and
prove both that schema creation is denied and normal readiness/search operations still work.
Recheck after every resource-binding update because the platform may restore its managed grant.
Do not assume the repository's table-grant command performs this database-level revocation.

The App's `/readyz` checks Lakebase, both migration ledgers, and Temporal, but it is not a substitute
for negative privilege tests.

## Local full-stack rehearsal

Start a local Temporal server:

```bash
temporal server start-dev
```

With the schemas/grants applied, use the worker role in one terminal:

```bash
export TEMPORAL_ADDRESS=localhost:7233
export TEMPORAL_NAMESPACE=default
export TEMPORAL_TLS=false
export RETRIEVAL_DEMO_MODE=true
export RETRIEVAL_SEARCH_BACKEND=postgres_text
export RETRIEVAL_ADAPTER_BUNDLE_FACTORY=retrieval.demo.scripted_provider:create_adapter_bundle
uv run retrieval-worker
```

Use the App role and the same Temporal namespace in a second terminal:

```bash
export RETRIEVAL_DEMO_MODE=true
export RETRIEVAL_SEARCH_BACKEND=postgres_text
uv run retrieval-demo-app
```

Check the process and dependencies separately:

```bash
curl --fail http://127.0.0.1:8000/healthz
curl --fail http://127.0.0.1:8000/readyz
```

Create a fresh run for every rehearsal. Do not reset an existing generation.

## Build the separate worker artifact

Build, but do not publish or deploy, the worker image:

```bash
docker build -f Dockerfile.worker -t temporal-retrieval-worker:local .
```

Run it locally with a reviewed `worker.env` that contains worker-role Lakebase credentials, Temporal
settings, `RETRIEVAL_DEMO_MODE=true`, and the bundle factory:

```bash
docker run --rm --env-file worker.env temporal-retrieval-worker:local
```

The worker gives each Temporal poller 45 seconds to drain after `SIGTERM`, exceeding the demo's
maximum 30-second held-commit wait. Configure the container or orchestrator termination grace
period to at least 60 seconds so poller drain and adapter close
can finish before a hard kill. As a rollout gate, send `SIGTERM` during an active rehearsal,
confirm both Task Queue pollers stop accepting work, and verify the process exits cleanly within
that window.

When the Temporal development server runs on the Docker host, use the host address appropriate to
the platform (for example `host.docker.internal:7233` on Docker Desktop), not container-local
`localhost`.

The worker must run on long-lived compute independently of Databricks Apps. For a production
namespace, use immutable image/build identity, Worker Versioning, graceful shutdown, and at least
two replicas for each live build.

## Validate the Databricks App bundle without deploying

The bundle root is `apps/retrieval_demo`. It references the repository root as source so the
`retrieval` and `apps` Python packages, root `app.yaml`, and root `requirements.txt` are included.

Choose the authenticated profile explicitly; never rely on whichever Databricks profile happens to
be default:

```bash
cd apps/retrieval_demo
databricks bundle validate --profile <PROFILE> -t dev \
  --var lakebase_branch=projects/<PROJECT>/branches/<BRANCH> \
  --var lakebase_database=projects/<PROJECT>/branches/<BRANCH>/databases/<DATABASE> \
  --var temporal_secret_scope=<SECRET_SCOPE>
```

Optional variables override the default secret keys `temporal-address`, `temporal-namespace`, and
`temporal-api-key`. The target secret scope must contain those values and the App service principal
must be able to read the referenced secret resources.

The bundle's `postgres` resource uses `CAN_CONNECT_AND_CREATE` and injects the standard `PG*`
variables plus `LAKEBASE_ENDPOINT`. The App command is
`python -m apps.retrieval_demo.app`; it binds `0.0.0.0:$DATABRICKS_APP_PORT`.

That managed permission includes database-level `CONNECT` and `CREATE`. Select a dedicated demo
database, apply the effective-privilege check above, and document any post-binding `REVOKE CREATE`
required by local policy.

`databricks bundle validate` checks the bundle and target context. It does not deploy the App.
There is intentionally no deployment command in this repository's local verification sequence.

## Pre-rollout artifact verification

Run against the exact source revision/image intended for a target:

```bash
uv run ruff check .
uv run ruff format --check .
uv run python -m compileall -q src tests apps
uv run pytest
make integration
RUN_TEMPORAL_INTEGRATION=1 uv run pytest -q tests/demo/test_temporal_late_writer.py
uv run pytest -m replay tests/replay
uv build
docker build -f Dockerfile.worker -t temporal-retrieval-worker:local .
```

Archive test output, migration status, grant review, history inventory, dependency lock, image
digest, bundle validation output, and build identity. A production release also needs realistic
load/SLO evidence and the gates in
[`../architecture-production-readiness.md`](../architecture-production-readiness.md).

## Future rollout order

After the target-specific review authorizes deployment, use this dependency order:

1. select/provision the dedicated Lakebase branch/database and database roles;
2. ensure the Databricks App service principal/resource identity is known;
3. apply core migration, demo migration, then runtime grants as the migration owner;
4. start worker replicas without admitting commands and verify both queue pollers;
5. start the App and verify liveness/readiness with the App identity;
6. run a fresh non-customer Northstar rehearsal;
7. admit a small deterministic canary only after the rehearsal passes;
8. expand only while named health and rollback thresholds remain satisfied.

Do not start root sync or deactivation workflows directly to bypass controller serialization.

## Upgrade procedure

1. Inventory open executions by Workflow Type, Task Queue, and assigned worker build.
2. Export and replay representative histories, including patched cleanup and Continue-As-New paths.
3. Back up or branch the database according to the target recovery policy.
4. Apply only new forward migrations as the migration owner, then rerun runtime grants.
5. Build a new immutable worker image/build ID; do not mutate a running build identity.
6. Start at least two new replicas and verify both Task Queues before routing work.
7. Validate/start the compatible App artifact and run a fresh rehearsal store.
8. Route a deterministic canary using the namespace's reviewed Worker Versioning procedure.
9. Keep the previous compatible build healthy until all executions assigned to it close or move
   through an explicitly compatible Continue-As-New path.

## Rollback

### Application and admission rollback

Rollback changes new-work admission and routing; it does not rewrite workflow history or database
generations.

1. Stop new App commands or new-work routing to the affected build.
2. Keep every build that owns open pinned executions running.
3. Restore new work to the last verified compatible App/worker artifact.
4. Confirm controller commands, both queues, quota recovery, database readiness, and lifecycle
   outcomes recover.
5. Preserve failed-build histories, migration status, and telemetry for diagnosis.

### Schema rollback

Migrations are forward-only. Do not edit migration ledgers or run ad-hoc down migrations. If a new
schema version is faulty:

- prefer a forward corrective migration when the data remains valid;
- if restoration is required, stop admission, preserve Temporal-compatible workers, restore the
  database/branch using the target's approved recovery procedure, and reconcile every store's
  lifecycle generation before resuming;
- replay histories and rerun contract/readiness checks against the restored state.

### Incomplete deactivation

Determine whether the fence committed:

- **before the fence:** the store remains `active`/`syncing`; correct the dependency and retry the
  command;
- **after the fence:** keep the new generation, repair the dependency, and resume cleanup with the
  same generation and stable deactivation identity.

Never decrement a generation. Never treat cancellation as the data-safety mechanism.

### Provider quota recovery

Do not refund a permit after an ambiguous provider outcome. Let the next authoritative quota
observation or reset restore capacity. Diagnose a stuck scope from bounded workflow state and
metrics without logging the credential key.

## Remove an old worker build

Remove a build only after Temporal visibility shows zero open executions assigned to it,
representative retained histories replay on their intended builds, the required retention/replay
window has elapsed, and rollback owners agree it is no longer part of recovery.

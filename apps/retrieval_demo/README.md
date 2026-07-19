# Lakebase + Temporal demo App

This directory contains the single-process FastAPI presentation layer. It serves the four-panel UI
and JSON API from one Uvicorn process; the Temporal workers remain a separate long-running process.
The App reads authoritative lifecycle/search data from Lakebase and submits asynchronous commands to
Temporal. It never runs workflow Activities itself.

## Local check

From the repository root, install the demo dependencies, set the process-only Lakebase and Temporal
environment variables described in the root documentation, and apply the schemas/grants as the
migration owner:

```bash
uv sync --extra dev
uv run retrieval-lakebase-migrate
uv run retrieval-demo-migrate
uv run retrieval-lakebase-grant-roles \
  --app-role <APP_DB_ROLE> --worker-role <WORKER_DB_ROLE>
```

Start `retrieval-worker` separately with the worker role:

```bash
RETRIEVAL_DEMO_MODE=true \
RETRIEVAL_ADAPTER_BUNDLE_FACTORY=retrieval.demo.scripted_provider:create_adapter_bundle \
uv run retrieval-worker
```

Then start the App with App-role credentials:

```bash
RETRIEVAL_DEMO_MODE=true uv run retrieval-demo-app
```

The local fallback URL is <http://127.0.0.1:8000>. `DATABRICKS_APP_PORT` overrides port 8000 and the
server always binds to `0.0.0.0` as required by Databricks Apps.

Importing `apps.retrieval_demo.app` does not inspect environment variables or make network calls.
Configuration validation and connections happen inside the FastAPI lifespan.

Set `TEMPORAL_WEB_BASE_URL` to a Temporal Web origin to enable workflow deep links. The App appends
`/namespaces/<namespace>/workflows/<workflow-id>`. A custom URL template may instead contain
`{namespace}` and `{workflow_id}` placeholders. Only credential-free HTTP(S) links are emitted.

## Deployment inputs

The nested `databricks.yml` is a reusable bundle definition. Supply these values explicitly for the
target workspace; do not commit them:

- `lakebase_branch`: full `projects/.../branches/...` resource name;
- `lakebase_database`: full `projects/.../branches/.../databases/...` resource name;
- `temporal_secret_scope`: a scope containing `temporal-address`, `temporal-namespace`, and
  `temporal-api-key` (or override the three key-name variables).

The `postgres` resource grants the App service principal `CAN_CONNECT_AND_CREATE` and injects the
standard `PG*` variables plus `LAKEBASE_ENDPOINT`. Temporal values are injected from secrets; they
are not present in source or bundle variables.

The managed resource permission includes PostgreSQL `CONNECT` and `CREATE` on the selected
database. Explicit repository grants narrow access to existing objects but do not revoke this
database-level permission. Bind the App to a dedicated database; if policy requires a no-DDL App
identity, revoke database `CREATE` after binding and re-verify it after every binding update as
described in the migration runbook.

When an authenticated Databricks profile is available, validate from this directory with explicit
workspace and variables:

```bash
databricks bundle validate --profile <PROFILE> -t dev \
  --var lakebase_branch=projects/<PROJECT>/branches/<BRANCH> \
  --var lakebase_database=projects/<PROJECT>/branches/<BRANCH>/databases/<DATABASE> \
  --var temporal_secret_scope=<SECRET_SCOPE>
```

Validation is read-only. Deployment is intentionally not part of the local build or test workflow.
Before a future deployment, use the migration identity to apply core migration, demo migration,
then the explicit runtime grants; start the separate worker; and perform the readiness and browser
rehearsal checks in the repository runbook. The migration identity owns both schemas. The App gets
core reads, demo DML, and `EXECUTE` on the fixed seed function; it does not own the demo schema or
receive core DML.

## Runtime checks

- `GET /healthz` reports only process liveness.
- `GET /readyz` checks Lakebase connectivity, migration state, and Temporal connectivity.
- Every `POST` requires `Idempotency-Key`; receipts live in Lakebase so replay safety survives App
  restarts.
- A combined run snapshot keeps returning Lakebase state when a Temporal status query is temporarily
  unavailable.
- Search and ask operations fail closed once a store starts deactivating.

No App deployment has been performed from this repository. Live workspace authentication,
Lakebase role mapping, networking, secret access, and readiness remain target-specific checks.

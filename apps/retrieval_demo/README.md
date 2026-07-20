# Retrieval demo App

This directory contains the FastAPI HTTP API and browser UI for the Northstar demonstration. The
App is a command/read gateway:

- it submits commands to a store's Temporal controller;
- it reads lifecycle state, search results, controls, and events from Lakebase;
- it serves the static four-panel UI;
- it does **not** run Temporal workflows or Activities.

The long-running `retrieval-worker` is a separate process and deployment.

## Directory contents

| Path | Purpose |
|---|---|
| `app.py` | FastAPI routes, error mapping, lifespan, and Uvicorn entry point |
| `static/index.html` | Browser document |
| `static/app.js` | UI state, polling, commands, and rendering |
| `static/app.css` | UI layout and styles |
| `databricks.yml` | Databricks Asset Bundle for the App/resource bindings |
| `app.yaml` | directory-local command manifest; root `app.yaml` is the effective bundled manifest |
| `requirements.txt` | pinned App dependencies used by Databricks Apps |

## Runtime request flow

1. A browser sends an HTTP request to FastAPI.
2. Every `POST` requires an `Idempotency-Key`.
3. The service stores/replays the HTTP receipt in Lakebase.
4. Sync/deactivation commands go to `RetrievalClient` and the store controller.
5. The request returns an operation identity without waiting for workflow completion.
6. The browser polls snapshots, operations, and events.

If a Temporal status query is temporarily unavailable, snapshots still return authoritative
Lakebase state with a warning. Search and answer requests fail closed when the store is no longer
in a readable lifecycle state.

## Dependencies and identities

The App needs:

- Lakebase/Postgres with both schemas migrated;
- an App database role with the explicit repository grants;
- a reachable Temporal namespace and valid API key/TLS configuration;
- `RETRIEVAL_DEMO_MODE=true`;
- a separately deployed worker before workflow commands can progress.

The App role is read-oriented for the core schema. It creates a fixed Northstar run through the
migration-owned `SECURITY DEFINER` function and writes only allowed demo objects. It must not share
the worker database identity.

## Run locally

From the repository root, install dependencies:

```bash
uv sync --frozen --extra dev
```

Create protected per-role environment files as described in the root
[README](../../README.md#run-the-complete-demo-locally), then apply migrations/grants as the owner:

```bash
RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-lakebase-migrate
RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-demo-migrate
RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-lakebase-grant-roles \
  --app-role <APP_DATABASE_ROLE> \
  --worker-role <WORKER_DATABASE_ROLE>
```

Start the worker in one terminal:

```bash
RETRIEVAL_ENV_FILE=.env.worker \
RETRIEVAL_ADAPTER_BUNDLE_FACTORY=retrieval.demo.scripted_provider:create_adapter_bundle \
uv run retrieval-worker
```

Start the App with the App identity in another terminal:

```bash
RETRIEVAL_ENV_FILE=.env.app uv run retrieval-demo-app
```

Verify and open it:

```bash
curl --fail http://127.0.0.1:8000/healthz
curl --fail http://127.0.0.1:8000/readyz
```

The default URL is <http://127.0.0.1:8000>. `DATABRICKS_APP_PORT` overrides the port; the server
always binds to `0.0.0.0`.

Use the executable instead of invoking Uvicorn directly. `retrieval-demo-app` performs the safe
environment-file injection; an external Uvicorn command must receive an already prepared process
environment.

## Environment loading

- The executable inspects only `.env` in the current working directory.
- `RETRIEVAL_ENV_FILE=<path>` selects another file.
- `RETRIEVAL_ENV_FILE=` disables file loading.
- Process/platform values win over file values.
- `${NAME}` interpolation is disabled.
- Importing `apps.retrieval_demo.app` reads no environment and opens no connection.

Do not package or commit `.env` files.

## HTTP endpoints

| Method and path | Purpose |
|---|---|
| `GET /healthz` | Process liveness only |
| `GET /readyz` | Lakebase, both migration ledgers, and Temporal connectivity |
| `POST /api/demo/runs` | Create/replay a fresh Northstar run |
| `GET /api/demo/runs/{run_id}/snapshot` | Lifecycle/counts plus best-effort workflow status |
| `GET /api/demo/runs/{run_id}/events` | Durable event timeline |
| `GET /api/demo/runs/{run_id}/search` | Current-generation text search |
| `POST /api/demo/runs/{run_id}/sync` | Submit asynchronous sync |
| `POST /api/demo/runs/{run_id}/deactivate` | Submit asynchronous deactivation |
| `POST /api/demo/runs/{run_id}/controls/hold` | Enable the configured late-writer hold |
| `POST /api/demo/runs/{run_id}/controls/release` | Release after the generation fence |
| `POST /api/demo/runs/{run_id}/ask` | Return deterministic cited evidence |
| `GET /api/operations/{operation_id:path}` | Poll operation state |

`TEMPORAL_WEB_BASE_URL` optionally enables workflow deep links. It may be a base origin or a
template containing `{namespace}` and `{workflow_id}`. Only credential-free HTTP(S) links are
returned to the browser.

## Databricks bundle inputs

Run bundle commands from this directory. Required variables are:

| Variable | Value |
|---|---|
| `lakebase_branch` | full `projects/.../branches/...` resource name |
| `lakebase_database` | full `projects/.../branches/.../databases/...` resource name |
| `temporal_secret_scope` | scope containing address, namespace, and API key |

The default secret keys are `temporal-address`, `temporal-namespace`, and `temporal-api-key`.

Validate without deploying:

```bash
databricks bundle validate --strict --profile <PROFILE> -t dev \
  --var lakebase_branch=projects/<PROJECT>/branches/<BRANCH> \
  --var lakebase_database=projects/<PROJECT>/branches/<BRANCH>/databases/<DATABASE> \
  --var temporal_secret_scope=<SECRET_SCOPE>
```

The `postgres` binding injects canonical `PG*` values, App OAuth identity, and
`LAKEBASE_ENDPOINT`. Secret resources inject Temporal configuration. Bundle sync includes the
repository root so root `app.yaml`, root requirements, `apps`, and `src/retrieval` deploy together;
ignored environment files are excluded.

`bundle deploy` creates/updates the resource and uploads source. It does not start/deploy the App
process. After migrations/grants are ready, run:

```bash
databricks bundle run retrieval_demo --profile <PROFILE> -t dev \
  --var lakebase_branch=projects/<PROJECT>/branches/<BRANCH> \
  --var lakebase_database=projects/<PROJECT>/branches/<BRANCH>/databases/<DATABASE> \
  --var temporal_secret_scope=<SECRET_SCOPE>
```

Follow the complete [deployment runbook](../../docs/runbooks/deploy-lakebase-temporal-demo.md) for
identity creation, migrations, worker handoff, verification, and rollback.

## Troubleshooting

| Symptom | Check |
|---|---|
| Bundle summary says App URL is not deployed | Run `bundle run retrieval_demo` after prerequisites |
| App crashes with missing migrations | Apply both schemas and grants, then redeploy |
| App crashes with `Jwt is missing` | Re-enter the Temporal API key without a trailing newline |
| Database permission error | Verify App `service_principal_client_id` equals the granted Postgres role |
| `/readyz` redirects | Authenticate through Databricks OIDC first |
| `/readyz` reports Temporal false | Address, namespace, API key, TLS, and outbound egress |
| Commands stay pending | Confirm a worker polls both Task Queues/current build |
| Workflow links are absent | Configure `TEMPORAL_WEB_BASE_URL` |

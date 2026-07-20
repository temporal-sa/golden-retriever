# Durable retrieval with Temporal and Lakebase

This repository demonstrates how to synchronize searchable documents safely when work is slow,
retried, rate-limited, canceled, or still finishing during deactivation.

The central idea is simple:

- **Temporal** remembers what must happen next and coordinates work across failures.
- **Lakebase Postgres** stores what is currently true and decides whether each write is still
  allowed.
- **The Databricks App** gives people a browser and HTTP API for starting commands and inspecting
  results.
- **A separate worker process** runs Temporal workflows and Activities. The App never runs a
  worker.

The repository includes a deterministic demonstration named **Northstar**. It synchronizes five
documents, pauses once for provider quota, deliberately holds one document write, deactivates the
store, and proves that the late write cannot cross the database generation fence.

## What you can do with this repository

- run a no-service rehearsal in a few seconds;
- run the Temporal workflow hierarchy against a local Temporal server;
- run the complete App, worker, and Lakebase-backed demonstration;
- deploy the App with a Databricks Asset Bundle and deploy the worker independently;
- inspect forward-only database migrations, least-privilege grants, replay histories, and
  production-readiness gates;
- synchronize Google Docs, Sheets, Slides, and uploaded text files through the read-only Google
  Drive adapter;
- adapt the workflow and repository ports to a real provider and staging system.

The Northstar provider and its documents are demonstration fixtures. They are not a customer-data
connector or a production staging service.

## How the system works

Every retrieval data set is a **store**. One long-lived `StoreControllerWorkflow` serializes that
store's sync and deactivation commands. A sync creates a bounded hierarchy of child workflows:

```text
StoreControllerWorkflow
└── RootSyncWorkflow
    └── UserSyncWorkflow
        └── ResourceSyncWorkflow
            └── ResourcePagesWorkflow
                └── FilesPageWorkflow
                    └── DocumentIngestionWorkflow
```

Document bodies do not enter Temporal Workflow Event History. Workflows carry small references;
an Activity loads each body, verifies it, chunks it, and writes it to Lakebase.

Each store row has a lifecycle state and a monotonically increasing **generation**. Deactivation
commits `generation N → N + 1` before cancellation and cleanup. Every mutation checks the expected
generation inside the same transaction as its write. A late generation-`N` Activity therefore
fails even if cancellation arrived too late.

See the [system specification](docs/lakebase-temporal-demo-spec.md) for the complete behavior and
the [workflow topology](docs/workflow-topology.md) for diagrams.

## Repository tour

| Path | What it contains |
|---|---|
| `src/retrieval/temporal` | Temporal clients, workflows, Activities, worker, and runtime configuration |
| `src/retrieval/lakebase` | Postgres connection pool, repository, search, migrations, and grants |
| `src/retrieval/google_drive` | Drive API client, provider gateway, shared staging, and Lakebase worker bundle |
| `src/retrieval/demo` | Northstar fixtures, scripted provider, durable controls/events, and demo service |
| `apps/retrieval_demo` | FastAPI API, browser UI, and Databricks bundle |
| `tests` | unit, contract, App, integration, replay, and load tests |
| `docs` | architecture, deployment, migration, operations, and decision guides |
| `Dockerfile.worker` | long-running worker image |
| `Dockerfile.app` | standalone App image for tmprl-demo.cloud |

Use the [implementation map](IMPLEMENTATION_MAP.md) when you need exact modules, commands,
environment variables, workflow names, or database tables.

## Prerequisites

For local development you need:

- Python 3.11 or newer;
- [`uv`](https://docs.astral.sh/uv/);
- Node.js for the JavaScript syntax check in `make verify`.

Additional paths need:

- Temporal CLI for a local Temporal server;
- a TLS-enabled Lakebase/Postgres database for the complete stack;
- Databricks CLI `>=0.299.0` and an OAuth profile for Databricks deployment;
- Docker or another OCI builder for worker/App images.

The production [Google Drive integration guide](docs/google-drive-integration.md) covers its
read-only OAuth scope, supported file types, shared staging requirement, and worker configuration.

Install the locked development environment:

```bash
uv sync --frozen --extra dev
```

## Run the fastest rehearsal

This command needs no database, Temporal server, or network connection:

```bash
uv run retrieval-demo-headless --json
```

A successful result ends with generation 8, state `inactive`, zero documents/chunks, four cited
documents, and a rejected stale write. This validates the deterministic data-safety story; it does
not validate a live Temporal or Lakebase environment.

## Run a local Temporal smoke test

Start Temporal in one terminal:

```bash
temporal server start-dev
```

Run the self-contained workflow starter in another terminal:

```bash
uv run retrieval-test-starter
```

The starter creates temporary Task Queues and in-memory adapters, submits sync and deactivation
through the public `RetrievalClient`, checks the final state, and cleans up. It does not need the
deployed worker or Lakebase.

## Run the complete demo locally

The complete demo uses four roles/processes:

1. a Temporal server or namespace;
2. a Lakebase/Postgres database owned by a migration identity;
3. a long-running worker with a worker database role;
4. the FastAPI App with a separate App database role.

### 1. Create local configuration

Copy the configuration checklist and protect it because it may contain credentials:

```bash
cp .env.example .env
chmod 600 .env
```

At minimum configure the canonical Postgres values and Temporal connection:

```text
PGHOST=<database-host>
PGPORT=5432
PGDATABASE=<database-name>
PGUSER=<database-role-for-this-process>
PGSSLMODE=require

LAKEBASE_ENDPOINT=projects/<project>/branches/<branch>/endpoints/<endpoint>

TEMPORAL_ADDRESS=<host:port>
TEMPORAL_NAMESPACE=<namespace>
TEMPORAL_TLS=<true-or-false>
TEMPORAL_API_KEY=<required-for-Temporal-Cloud>

RETRIEVAL_DEMO_MODE=true
RETRIEVAL_SEARCH_BACKEND=postgres_text
```

For Lakebase OAuth, set `LAKEBASE_ENDPOINT` and leave `PGPASSWORD` unset. For ordinary SSL
Postgres, omit `LAKEBASE_ENDPOINT` and set `PGPASSWORD`. Never set both authentication methods.

One `.env` is convenient for a private local rehearsal. To test least privilege, create separate
ignored `.env.migration`, `.env.worker`, and `.env.app` files with the matching database identity.
Select one with `RETRIEVAL_ENV_FILE=<path>`.

Executable entry points inspect only `.env` in the current directory unless
`RETRIEVAL_ENV_FILE` is set. Existing process variables win, interpolation is disabled, and an
empty `RETRIEVAL_ENV_FILE` disables file loading.

### 2. Create the schemas and grants

Run these commands with the migration owner:

```bash
RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-lakebase-migrate
RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-demo-migrate
RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-lakebase-grant-roles \
  --app-role <APP_DATABASE_ROLE> \
  --worker-role <WORKER_DATABASE_ROLE>

RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-lakebase-migrate --check --json
RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-demo-migrate --check --json
```

Both checks must report `"ready": true`. The migration and grant model is explained in the
[migration runbook](docs/runbooks/migration-and-rollback.md).

### 3. Start the worker

```bash
RETRIEVAL_ENV_FILE=.env.worker \
RETRIEVAL_ADAPTER_BUNDLE_FACTORY=retrieval.demo.scripted_provider:create_adapter_bundle \
uv run retrieval-worker
```

The worker polls `retrieval-v2` for workflows and database work, and
`retrieval-provider-v2` for provider calls.

### 4. Start the App

```bash
RETRIEVAL_ENV_FILE=.env.app uv run retrieval-demo-app
```

Verify it:

```bash
curl --fail http://127.0.0.1:8000/healthz
curl --fail http://127.0.0.1:8000/readyz
```

Open <http://127.0.0.1:8000>. Create a run, synchronize it, inspect the cited answer, deactivate
the store, and release the held write only after the generation fence appears.

The [App guide](apps/retrieval_demo/README.md) documents every endpoint and runtime dependency.

## Test the project

Run the standard verification sequence:

```bash
make verify
```

It runs linting, formatting checks, compilation, the default pytest suite, the headless rehearsal,
JavaScript syntax validation, and package builds.

Specialized suites are opt-in:

```bash
make integration
uv run pytest -m replay tests/replay
RUN_TEMPORAL_LOAD=1 uv run pytest -s -m load tests/load
```

Read the [integration](tests/integration/README.md), [replay](tests/replay/README.md), and
[load](tests/load/README.md) guides before using an external namespace.

## Deploy the system

The App and worker have different lifecycles:

- deploy the App with `apps/retrieval_demo/databricks.yml` or the standalone App image;
- deploy `Dockerfile.worker` on long-running compute with independent scaling and graceful
  shutdown;
- create the database schemas and explicit role grants before starting either runtime.

Use one of these runbooks:

- [Databricks App + external worker](docs/runbooks/deploy-lakebase-temporal-demo.md)
- [tmprl-demo.cloud](docs/runbooks/deploy-tmprl-demo-cloud.md)
- [Schema upgrades and rollback](docs/runbooks/migration-and-rollback.md)

Do not use an implicit Databricks profile, put credentials in command history, start child
workflows directly, edit an applied migration, or decrement a committed store generation.

## Documentation

The [documentation guide](docs/README.md) provides reading paths and a glossary. The most useful
references are:

- [System specification](docs/lakebase-temporal-demo-spec.md)
- [Workflow and data topology](docs/workflow-topology.md)
- [Implementation map](IMPLEMENTATION_MAP.md)
- [Production-readiness guide](docs/architecture-production-readiness.md)
- [Metrics and observability](docs/operations/metrics.md)

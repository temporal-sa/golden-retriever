# Temporal retrieval with Lakebase

This repository is a reference implementation of a durable retrieval pipeline. Temporal owns
orchestration, retries, provider quota waits, cancellation, remediation, and cleanup order.
Lakebase Postgres owns the authoritative store lifecycle, searchable documents, durable
idempotency receipts, and the transaction that accepts or rejects every write.

It includes:

- the complete Temporal workflow hierarchy and public `RetrievalClient`;
- in-memory adapters and a local orchestration smoke test;
- an async Lakebase/Postgres repository, forward-only migrations, and Postgres full-text search;
- a deterministic five-document Northstar AI scenario with a quota pause and a held late writer;
- a no-service headless rehearsal of the data-safety story;
- a FastAPI Databricks App with a four-panel browser UI and durable HTTP idempotency;
- a Databricks Asset Bundle definition and a separate worker container.

The deployment artifacts are prepared and locally testable. **No Databricks App, Lakebase
database, or Temporal worker has been deployed from this repository.** Authentication, grants,
networking, migrations, and readiness must still be validated in the intended target environment.

## Start here

1. Read this page for setup and the shortest runnable paths.
2. Read [`IMPLEMENTATION_MAP.md`](IMPLEMENTATION_MAP.md) to find code and configuration.
3. Use [`docs/workflow-topology.md`](docs/workflow-topology.md) for the architecture and workflow
   diagrams.
4. Use [`docs/lakebase-temporal-demo-spec.md`](docs/lakebase-temporal-demo-spec.md) as the as-built
   Northstar architecture and presenter reference.
5. Before any rollout, follow
   [`docs/runbooks/deploy-lakebase-temporal-demo.md`](docs/runbooks/deploy-lakebase-temporal-demo.md),
   [`docs/runbooks/migration-and-rollback.md`](docs/runbooks/migration-and-rollback.md), and the
   [`production-readiness guide`](docs/architecture-production-readiness.md).

## Mental model

Each store has one long-lived `StoreControllerWorkflow`. Applications use `RetrievalClient`
Update-with-Start commands rather than starting sync or deactivation workflows directly.

```text
StoreControllerWorkflow
├── RootSyncWorkflow
│   ├── UserSyncWorkflow
│   │   └── ResourceSyncWorkflow
│   │       └── ResourcePagesWorkflow
│   │           └── FilesPageWorkflow
│   │               └── DocumentIngestionWorkflow
│   └── FailedUserRemediationWorkflow
└── DeactivateStoreWorkflow
    ├── CleanupUsersWorkflow
    └── RemoveObjectsWorkflow (bounded batches)
```

Provider bodies do not enter Workflow Event History. Workflows carry compact `DocumentRef`
metadata; the ingestion Activity loads the staged body, validates and chunks it, then calls the
repository. The Lakebase transaction locks the store row, verifies lifecycle state and generation,
writes the document, chunks, and idempotency receipt, and commits them together.

Deactivation is ordered `fence -> cancel -> drain -> bounded cleanup -> inactive`. The database
first advances generation `N` to `N + 1` and enters `deactivating`. A generation-`N` Activity that
finishes later is rejected by the database fence even if cancellation arrived too late.

See the [visual topology](docs/workflow-topology.md) for Task Queues, quota coordination, the
Northstar late-writer sequence, and data ownership.

## Prerequisites

- Python 3.11 or newer
- [`uv`](https://docs.astral.sh/uv/)
- [Temporal CLI](https://docs.temporal.io/cli) for local Temporal runs
- for the full stack, a reachable TLS-enabled Lakebase/Postgres database
- for Databricks bundle validation, Databricks CLI and an explicitly selected authenticated profile

Install all development and demo dependencies:

```bash
uv sync --extra dev
```

## Fastest check: no services

The headless rehearsal needs no Temporal server, Lakebase database, or network access:

```bash
uv run retrieval-demo-headless --json
```

It exercises the versioned Northstar fixtures, one five-second quota observation, four normal
document commits at generation 7, a held fifth document, cited evidence retrieval, a `7 -> 8`
fence, stale-writer rejection, bounded cleanup, and the final `inactive`/zero-row state. It is a
data-plane rehearsal; it does not replace the Temporal integration suite or a live Lakebase check.

## Local Temporal smoke

Start the development server in one terminal:

```bash
temporal server start-dev
```

The frontend defaults to `localhost:7233` and Temporal Web normally opens at
<http://localhost:8233>.

In another terminal run the executable orchestration smoke test:

```bash
uv run retrieval-test-starter
```

The starter creates isolated queues and local workers, sends sync and deactivation through
`RetrievalClient`, verifies the generation and final lifecycle state, and cleans up its controller.
It uses an empty provider and in-memory persistence, so no separate `retrieval-worker` is needed.

## Full local Northstar stack

This path runs the App and worker as separate processes against one Temporal namespace and one
Lakebase/Postgres database.

### 1. Configure the processes

The executable worker, App, migration, and grant commands automatically load `.env` from the
current working directory. Start with the complete, non-secret checklist and restrict the local
copy because it can contain credentials:

```bash
cp .env.example .env
chmod 600 .env
```

Fill in the standard `PG*` values. For Lakebase OAuth, set the endpoint resource path and rely on
normal Databricks SDK authentication:

```bash
export PGHOST=<database-host>
export PGPORT=5432
export PGDATABASE=<database-name>
export PGUSER=<database-role>
export PGSSLMODE=require
export LAKEBASE_ENDPOINT=projects/<PROJECT>/branches/<BRANCH>/endpoints/<ENDPOINT>

export TEMPORAL_ADDRESS=localhost:7233
export TEMPORAL_NAMESPACE=default
export TEMPORAL_TLS=false
export RETRIEVAL_DEMO_MODE=true
```

Environment injection is deliberately non-overriding: values already supplied by the shell,
container platform, or Databricks resource/secret binding win over file values. Only the exact
`.env` in the working directory is discovered; parent directories are not searched. To select a
different file, set `RETRIEVAL_ENV_FILE`, for example:

```bash
RETRIEVAL_ENV_FILE=.env.worker uv run retrieval-worker
```

An empty `RETRIEVAL_ENV_FILE` disables loading. An explicit path that is missing or is not a file
fails at startup. `${NAME}` expressions remain literal rather than reading other process secrets.
Library imports and direct config-object construction never load files.

The single `.env` is the shortest local path when one database identity is acceptable. For a
least-privilege rehearsal, instead create ignored `.env.migration`, `.env.worker`, and `.env.app`
files from the example, put the appropriate `PGUSER` and authentication in each, and restrict all
three to mode `0600`:

```bash
cp .env.example .env.migration
cp .env.example .env.worker
cp .env.example .env.app
chmod 600 .env.migration .env.worker .env.app
```

The role-specific commands below select those files. If you chose the single `.env`, omit each
`RETRIEVAL_ENV_FILE=...` prefix. Put shared Temporal settings in every role-specific file or inject
them through the shell/secret manager. Never commit any of them.

For an SSL-enabled local Postgres instance, omit `LAKEBASE_ENDPOINT` and set `PGPASSWORD` instead.
Do not set both. Canonical `PG*` names take precedence over `LAKEBASE_HOST`, `LAKEBASE_PORT`,
`LAKEBASE_DATABASE`, `LAKEBASE_USER`, `LAKEBASE_PASSWORD`, and `LAKEBASE_SSLMODE` aliases.

### 2. Apply both schemas

Run migrations with the migration identity, not the eventual App or worker identity:

```bash
RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-lakebase-migrate
RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-demo-migrate
RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-lakebase-grant-roles \
  --app-role <APP_DB_ROLE> --worker-role <WORKER_DB_ROLE>
RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-lakebase-migrate --check --json
RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-demo-migrate --check --json
```

Core migrations create `retrieval`; demo migrations create `retrieval_demo_ui` and the fixed
`SECURITY DEFINER` run-seed function. Both schemas remain owned by the migration role. The grant
command applies quoted, explicit steady-state privileges to already-existing distinct App and
worker roles; it does not create roles. Review the exact grants in the
[migration runbook](docs/runbooks/migration-and-rollback.md) before starting either process.

### 3. Start the separate worker

Leave Temporal running, then start the worker in its own terminal:

```bash
RETRIEVAL_ENV_FILE=.env.worker uv run retrieval-worker
```

The bundle shares one Lakebase connection pool across the repository, scripted provider controls,
pre-commit hold hook, and durable demo event sink. It polls `retrieval-v2` and
`retrieval-provider-v2`. Do not combine `RETRIEVAL_ADAPTER_BUNDLE_FACTORY` with the three individual
factory variables or the unsafe in-memory flag.

### 4. Start the App

In another terminal, with App-role database credentials and the same Temporal settings:

```bash
RETRIEVAL_ENV_FILE=.env.app uv run retrieval-demo-app
```

Then verify and open it:

```bash
curl --fail http://127.0.0.1:8000/healthz
curl --fail http://127.0.0.1:8000/readyz
```

Open <http://127.0.0.1:8000>, create a run, sync it, ask the default question, deactivate it, and
release the held write after the fence. `DATABRICKS_APP_PORT` overrides port 8000.

## Run a basic persistent worker

For Temporal-only development without Lakebase or the Northstar scenario:

```bash
RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS=true uv run retrieval-worker
```

These adapters disappear when the process exits and the provider returns no data. Never enable
this setting in a shared or deployed environment.

## Test and validate

Run the default unit, contract, App, Lakebase-adapter, demo, and replay tests:

```bash
uv run pytest
```

Run real Temporal scenarios using SDK-managed ephemeral servers:

```bash
make integration
```

Run replay and the opt-in synthetic load harness:

```bash
uv run pytest -m replay tests/replay
RUN_TEMPORAL_LOAD=1 uv run pytest -s -m load tests/load
```

Run static and packaging checks:

```bash
uv run ruff check .
uv run ruff format --check .
uv run python -m compileall -q src tests apps
uv build
docker build -f Dockerfile.worker -t temporal-retrieval-worker:local .
```

`make verify` runs the non-Docker verification sequence, including the headless rehearsal; it
requires `node` for the JavaScript syntax check. `make integration` and `make replay` wrap the
corresponding opt-in suites.

The Lakebase tests exercise SQL, migration, pool, repository, and search contracts with test
doubles; they do not mutate a live target. A target Lakebase migration/readiness rehearsal remains
environment-specific. Suite details: [integration](tests/integration/README.md),
[replay](tests/replay/README.md), and [load](tests/load/README.md).

## Validate the Databricks App bundle without deploying

The App bundle is rooted in `apps/retrieval_demo`. Always choose the workspace profile explicitly:

```bash
cd apps/retrieval_demo
databricks bundle validate --strict --profile <PROFILE> -t dev \
  --var lakebase_branch=projects/<PROJECT>/branches/<BRANCH> \
  --var lakebase_database=projects/<PROJECT>/branches/<BRANCH>/databases/<DATABASE> \
  --var temporal_secret_scope=<SECRET_SCOPE>
```

This validates configuration only. It does not create, update, start, or deploy an App. The bundle
binds the App to a `postgres` resource with `CAN_CONNECT_AND_CREATE` and reads Temporal address,
namespace, and API key from three secret resources. The Temporal worker is intentionally absent
from the App process and must be run from the separate container or another long-lived compute
service.

The bundle sync root includes the repository root because the effective `app.yaml`, pinned
requirements, `apps` package, and `src/retrieval` package all live there. `.env` and `*.env` files
are excluded from Git, Docker, and bundle source; the deployed App receives its environment only
from Databricks resource, secret, and platform injection.

Databricks maps `CAN_CONNECT_AND_CREATE` to database `CONNECT` and `CREATE` for the App service
principal. The repository's grant command narrows table, schema-object, and function access, but it
does not remove that database-level platform grant. Use a dedicated database as the App's security
boundary, verify the effective privilege before rollout, and see the migration runbook if policy
requires revoking database `CREATE` after resource binding.

## Production use

The Northstar fixture provider and fixture staging store are demonstration components, not customer
connectors. A production system must provide a durable staging adapter and real provider gateway,
prove its target database and Temporal compatibility, configure telemetry and high availability,
and complete the release gates in
[`docs/architecture-production-readiness.md`](docs/architecture-production-readiness.md).

For non-demo production adapters, either configure one typed bundle:

```text
RETRIEVAL_ADAPTER_BUNDLE_FACTORY=package.module:create_adapter_bundle
```

or configure all three legacy factories together:

```text
RETRIEVAL_REPOSITORY_FACTORY=package.module:create_repository
RETRIEVAL_STAGING_STORE_FACTORY=package.module:create_staging_store
RETRIEVAL_PROVIDER_GATEWAY_FACTORY=package.module:create_provider_gateway
```

The worker fails closed on missing, partial, or ambiguous adapter configuration.

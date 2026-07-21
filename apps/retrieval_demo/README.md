# Retrieval demo App

This directory contains the FastAPI command/read gateway and the guided browser experience for the
Temporal + Lakebase Google Drive demo. The App:

- runs a bounded Temporal preflight that inspects one stable Drive folder without downloading file
  bodies into Workflow history;
- submits sync and deactivation commands to the store controller;
- reads lifecycle state, hybrid search results, controls, proof, and events from Lakebase;
- presents the six-step 10-minute story and links to Google Drive, Temporal UI, and Lakebase tooling;
- does **not** run Temporal Workflows or Activities.

The long-running `retrieval-worker` is a separate deployment.

## Runtime flow

1. Preflight traverses Drive metadata and identifies the configured late-write file.
2. The App creates a fresh generation-7 run through a constrained `SECURITY DEFINER` function.
3. Sync reaches the worker. A single durable provider throttle is injected, Temporal retries it,
   and Drive traversal checkpoints/staged bodies live in Lakebase.
4. The worker embeds deterministic chunks through Databricks Model Serving and commits them under
   the generation fence.
5. Lakebase Search runs independent BM25 and ANN candidates and the App fuses them with reciprocal
   rank fusion.
6. Deactivation advances authority to generation 8 before cleanup. Releasing the held generation-7
   writer proves the stale transaction is rejected.

Every mutating HTTP request requires an `Idempotency-Key`. Receipts, preflight state, controls, and
demo events are durable in Lakebase. A temporary Temporal status outage does not replace Lakebase as
the authoritative lifecycle read.

## Required production dependencies

- Lakebase Postgres with core and demo migrations current;
- Lakebase Search Beta enabled before applying the hybrid-search migration;
- a 1024-dimensional Databricks embedding endpoint accessible to the worker and App identities;
- a stable Google Drive folder plus credentials available only to the worker;
- Temporal Cloud credentials and worker pollers on both Task Queues;
- distinct migration-owner, App, and worker database identities;
- `RETRIEVAL_DEMO_MODE=true` and `RETRIEVAL_SEARCH_BACKEND=lakebase_hybrid`.

There is no production text-search fallback. If Lakebase Search or the embedding endpoint is not
ready, deployment/readiness must fail instead of silently changing the demo.

## Run locally

Install dependencies and apply migrations/grants from the repository root:

```bash
uv sync --frozen --extra dev
RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-lakebase-migrate
RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-demo-migrate
RETRIEVAL_ENV_FILE=.env.migration uv run retrieval-lakebase-grant-roles \
  --app-role <APP_DATABASE_ROLE> \
  --worker-role <WORKER_DATABASE_ROLE>
```

The packaged scripted provider remains available for a no-Google local rehearsal:

```bash
RETRIEVAL_ENV_FILE=.env.worker \
RETRIEVAL_ADAPTER_BUNDLE_FACTORY=retrieval.demo.scripted_provider:create_adapter_bundle \
uv run retrieval-worker

RETRIEVAL_ENV_FILE=.env.app uv run retrieval-demo-app
```

For the real connector, configure the worker with
`retrieval.google_drive.bundle:create_adapter_bundle`, Lakebase staging, the root folder ID, the
held file ID, and Google credentials. See the
[Google Drive integration guide](../../docs/google-drive-integration.md).

Verify:

```bash
curl --fail http://127.0.0.1:8000/healthz
curl --fail http://127.0.0.1:8000/readyz
```

The default URL is <http://127.0.0.1:8000>. `DATABRICKS_APP_PORT` overrides the port.

## HTTP endpoints

| Method and path | Purpose |
|---|---|
| `GET /healthz` | Process liveness |
| `GET /readyz` | Lakebase, both migration ledgers, and Temporal connectivity |
| `POST /api/preflight` | Start/replay bounded Drive metadata preflight |
| `GET /api/preflight/{workflow_id}` | Poll preflight state/result |
| `GET /api/preflight/{workflow_id}/source-files` | Read the sanitized source-file list |
| `POST /api/demo/runs` | Create/replay a fresh demo run |
| `GET /api/demo/runs/{run_id}/snapshot` | Lifecycle/counts plus best-effort Workflow state |
| `GET /api/demo/runs/{run_id}/events` | Durable event evidence |
| `GET /api/demo/runs/{run_id}/search` | Current-generation Lakebase hybrid search |
| `POST /api/demo/runs/{run_id}/sync` | Submit asynchronous sync |
| `POST /api/demo/runs/{run_id}/workflows/end` | Cancel an active ingestion workflow through its controller |
| `POST /api/demo/runs/{run_id}/deactivate` | Submit asynchronous deactivation |
| `POST /api/demo/runs/{run_id}/controls/release` | Release the held writer after the fence |
| `POST /api/demo/runs/{run_id}/ask` | Return a dynamic, cited answer |
| `GET /api/demo/runs/{run_id}/proof` | Read sanitized generation/write visibility proof |
| `GET /api/demo/tooling` | Read credential-free demo tooling links |
| `GET /api/operations/{operation_id:path}` | Poll operation state |

Tool URLs must be credential-free HTTP(S) URLs without query strings or fragments.

## Databricks bundle inputs

Run bundle commands from this directory. Required variables are:

| Variable | Value |
|---|---|
| `lakebase_branch` | full `projects/.../branches/...` resource name |
| `lakebase_database` | full `projects/.../branches/.../databases/...` resource name |
| `embedding_endpoint` | 1024-dimensional Databricks Model Serving endpoint name |
| `demo_held_document_key` | `gdrive:<stable-file-id>` |
| `temporal_secret_scope` | scope containing address, namespace, and API key |

Optional presentation variables are `google_drive_folder_url` and `temporal_web_base_url`.

```bash
databricks bundle validate --strict --profile <PROFILE> -t dev \
  --var lakebase_branch=projects/<PROJECT>/branches/<BRANCH> \
  --var lakebase_database=projects/<PROJECT>/branches/<BRANCH>/databases/<DATABASE> \
  --var embedding_endpoint=<ENDPOINT> \
  --var demo_held_document_key=gdrive:<FILE_ID> \
  --var temporal_secret_scope=<SECRET_SCOPE>
```

`bundle deploy` creates/updates resources and uploads source; it does not start the App process.
After migrations, grants, worker, and Drive configuration are ready, run `bundle run retrieval_demo`
with the same variables.

Follow the [deployment runbook](../../docs/runbooks/deploy-lakebase-temporal-demo.md) and rehearse
with the [10-minute presenter runbook](../../docs/runbooks/google-drive-demo.md).

# Deploy the demo with Databricks App, Lakebase, and an external Temporal worker

This runbook creates a working cloud environment for the Northstar demonstration. It assumes no
repository history. Read the root [README](../../README.md) for the system overview and the
[specification](../lakebase-temporal-demo-spec.md) for expected behavior.

## What this deployment contains

| Component | Runs where | Purpose |
|---|---|---|
| FastAPI App/UI | Databricks Apps | Accept commands and display Lakebase/Temporal state |
| Lakebase database | Databricks Lakebase | Store lifecycle, documents, search, controls, and receipts |
| Temporal namespace | Temporal Cloud | Persist workflow coordination and timers |
| `retrieval-worker` | External long-running container platform | Poll workflows and Activities |

The App is not a worker. Starting the App without a worker makes the UI reachable, but workflow
commands cannot progress until both Task Queues have pollers.

## Required identities

Use three distinct database identities:

1. **migration owner:** owns schemas and applies migrations/grants;
2. **App service principal:** created/managed by Databricks Apps and used only by the App;
3. **worker service principal:** used by the external worker through OAuth M2M.

Do not reuse the App identity for the worker.

## Values to collect

Use a secure operator worksheet. These names appear in commands below:

| Variable | Meaning |
|---|---|
| `DBX_PROFILE` | Explicit OAuth-authenticated Databricks CLI profile |
| `BUNDLE_TARGET` | `dev` for rehearsal or `prod` for a reviewed production target |
| `LB_PROJECT_ID` | Lakebase project resource ID |
| `LB_BRANCH_NAME` | Full `projects/.../branches/...` name |
| `LB_DATABASE_NAME` | Full `projects/.../branches/.../databases/...` name |
| `LB_ENDPOINT_NAME` | Full `projects/.../branches/.../endpoints/...` name |
| `LB_ENDPOINT_HOST` | Endpoint DNS host from endpoint status |
| `LB_DATABASE` | Postgres database name, usually `databricks_postgres` |
| `MIGRATION_DB_USER` | Postgres role of the database owner/project creator |
| `TEMPORAL_SECRET_SCOPE` | Databricks scope holding three Temporal values |
| `APP_DB_ROLE` | App `service_principal_client_id` after bundle deploy |
| `WORKER_CLIENT_ID` | Worker service-principal client/application ID |
| `WORKER_BUILD_ID` | Immutable worker build ID, usually Git SHA or image digest |
| `WORKER_IMAGE` | Published immutable worker image |

Never put the Temporal API key, OAuth client secret, or database password in this worksheet,
source control, command history, or deployment logs.

## Deployment order

Order matters because the App service principal and Lakebase role do not exist until the bundle
resource is created.

```mermaid
sequenceDiagram
    participant O as Operator
    participant L as Lakebase
    participant B as Databricks bundle
    participant W as External worker
    participant T as Temporal Cloud

    O->>L: create/select project, branch, endpoint, database
    O->>B: bundle deploy (create App resource, do not start)
    B->>L: create App database role through resource binding
    O->>L: create worker role; apply migrations and grants
    O->>W: deploy immutable worker
    W->>T: poll retrieval and provider queues
    O->>B: bundle run (deploy source and start App)
```

For the first deployment, do not start the App before schemas and grants exist.

## 1. Verify local tools and source

From the repository root:

```bash
make verify
databricks version
databricks auth profiles
temporal --version
docker version
```

Requirements:

- Python 3.11+ and `uv`;
- Databricks CLI `>=0.299.0`;
- an OAuth-authenticated Databricks CLI profile;
- Docker or another OCI builder for the worker;
- Temporal Cloud namespace/address/API key;
- workspace permission to manage the Lakebase project, App, and secret scope.

Never silently use `DEFAULT` or a personal access token. If authentication is missing, create a
descriptive profile:

```bash
databricks auth login \
  --host <DATABRICKS_WORKSPACE_URL> \
  --profile <DESCRIPTIVE_PROFILE>

databricks current-user me --profile <DESCRIPTIVE_PROFILE>
```

Set the non-secret helpers:

```bash
export DBX_PROFILE=<DESCRIPTIVE_PROFILE>
export BUNDLE_TARGET=dev
export LB_PROJECT_ID=retrieval-demo
export TEMPORAL_SECRET_SCOPE=retrieval-demo-temporal
```

Pass `--profile "$DBX_PROFILE"` to every Databricks command.

## 2. Create or select Lakebase resources

Use a dedicated database because the App resource grants database-level `CONNECT` and `CREATE`.

To create a project:

```bash
databricks postgres create-project "$LB_PROJECT_ID" \
  --json '{"spec":{"display_name":"Temporal Retrieval Demo"}}' \
  --profile "$DBX_PROFILE"
```

Discover real resource names instead of constructing them from display names:

```bash
databricks postgres list-branches "projects/$LB_PROJECT_ID" \
  --profile "$DBX_PROFILE" -o json

databricks postgres list-databases \
  "projects/$LB_PROJECT_ID/branches/<BRANCH>" \
  --profile "$DBX_PROFILE" -o json

databricks postgres list-endpoints \
  "projects/$LB_PROJECT_ID/branches/<BRANCH>" \
  --profile "$DBX_PROFILE" -o json
```

Record the returned names:

```bash
export LB_BRANCH_NAME=projects/<PROJECT>/branches/<BRANCH>
export LB_DATABASE_NAME=projects/<PROJECT>/branches/<BRANCH>/databases/<DATABASE_RESOURCE_ID>
export LB_ENDPOINT_NAME=projects/<PROJECT>/branches/<BRANCH>/endpoints/<ENDPOINT>
export LB_DATABASE=databricks_postgres
```

Read endpoint state and the database host:

```bash
databricks postgres get-endpoint "$LB_ENDPOINT_NAME" \
  --profile "$DBX_PROFILE" -o json

export LB_ENDPOINT_HOST=<STATUS_HOSTS_HOST>
```

Confirm the endpoint is read/write and ready. Review min/max capacity, suspend timeout, and branch
retention/TTL. Use `postgres_text`; migration 2 creates its GIN index. Do not introduce a different
search backend during a presentation.

## 3. Create the Temporal secret scope

The bundle expects these exact default keys:

| Key | Value |
|---|---|
| `temporal-address` | Temporal Cloud `host:port` |
| `temporal-namespace` | Namespace name |
| `temporal-api-key` | Temporal Cloud API key/JWT |

Create the scope and enter each value interactively:

```bash
databricks secrets create-scope "$TEMPORAL_SECRET_SCOPE" \
  --profile "$DBX_PROFILE"

databricks secrets put-secret "$TEMPORAL_SECRET_SCOPE" temporal-address \
  --profile "$DBX_PROFILE"

databricks secrets put-secret "$TEMPORAL_SECRET_SCOPE" temporal-namespace \
  --profile "$DBX_PROFILE"

databricks secrets put-secret "$TEMPORAL_SECRET_SCOPE" temporal-api-key \
  --profile "$DBX_PROFILE"
```

Interactive entry avoids credentials in shell history. If approved automation sends a value
through standard input, ensure it does not append a newline; a newline in a JWT can make Temporal
report `Jwt is missing` even though the secret key exists.

Verify names only—the list does not return values:

```bash
databricks secrets list-secrets "$TEMPORAL_SECRET_SCOPE" \
  --profile "$DBX_PROFILE"
```

## 4. Validate and create the App resource

Run bundle commands from `apps/retrieval_demo`:

```bash
cd apps/retrieval_demo

databricks bundle validate --strict \
  --profile "$DBX_PROFILE" \
  -t "$BUNDLE_TARGET" \
  --var "lakebase_branch=$LB_BRANCH_NAME" \
  --var "lakebase_database=$LB_DATABASE_NAME" \
  --var "temporal_secret_scope=$TEMPORAL_SECRET_SCOPE"

databricks bundle deploy \
  --profile "$DBX_PROFILE" \
  -t "$BUNDLE_TARGET" \
  --var "lakebase_branch=$LB_BRANCH_NAME" \
  --var "lakebase_database=$LB_DATABASE_NAME" \
  --var "temporal_secret_scope=$TEMPORAL_SECRET_SCOPE"
```

`bundle deploy` uploads source and creates/updates the App resource. It does **not** deploy/start
the App process.

Inspect the result:

```bash
databricks bundle summary \
  --profile "$DBX_PROFILE" \
  -t "$BUNDLE_TARGET" \
  --var "lakebase_branch=$LB_BRANCH_NAME" \
  --var "lakebase_database=$LB_DATABASE_NAME" \
  --var "temporal_secret_scope=$TEMPORAL_SECRET_SCOPE"

export DEPLOYED_APP_NAME=<APP_NAME_FROM_SUMMARY>

databricks apps get "$DEPLOYED_APP_NAME" \
  --profile "$DBX_PROFILE" -o json
```

Set `APP_DB_ROLE` to `service_principal_client_id` from the App JSON:

```bash
export APP_DB_ROLE=<SERVICE_PRINCIPAL_CLIENT_ID>
```

Confirm the App has exactly one Postgres resource and the three Temporal secret resources. The
Postgres binding creates a Lakebase role whose `postgres_role` is the App client ID.

## 5. Create the worker's Lakebase role

Create or select a dedicated Databricks service principal, add it to the workspace, and issue its
OAuth M2M secret through an approved secret-management process.

```bash
export WORKER_CLIENT_ID=<WORKER_SERVICE_PRINCIPAL_CLIENT_ID>

databricks postgres list-roles "$LB_BRANCH_NAME" \
  --profile "$DBX_PROFILE" -o json
```

If no Lakebase role has this `postgres_role`, create it:

```bash
databricks postgres create-role "$LB_BRANCH_NAME" \
  --role-id retrieval-worker \
  --json "{\"spec\":{\"postgres_role\":\"$WORKER_CLIENT_ID\",\"identity_type\":\"SERVICE_PRINCIPAL\",\"auth_method\":\"LAKEBASE_OAUTH_V1\"}}" \
  --profile "$DBX_PROFILE"
```

The worker's `PGUSER` and grant role are the service-principal client ID, not the API role slug
`retrieval-worker`.

## 6. Apply migrations and grants

Return to the repository root and install the Lakebase runtime:

```bash
cd ../..
uv sync --frozen --extra lakebase
```

Use the Lakebase database owner/project creator, not the App or worker:

```bash
databricks current-user me --profile "$DBX_PROFILE" -o json
export MIGRATION_DB_USER=<CURRENT_USER_NAME>

unset DATABRICKS_HOST DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET DATABRICKS_AUTH_TYPE
export DATABRICKS_CONFIG_PROFILE="$DBX_PROFILE"
export PGHOST="$LB_ENDPOINT_HOST"
export PGPORT=5432
export PGDATABASE="$LB_DATABASE"
export PGUSER="$MIGRATION_DB_USER"
export PGSSLMODE=require
export LAKEBASE_ENDPOINT="$LB_ENDPOINT_NAME"
export LAKEBASE_POOL_MIN_SIZE=1
export LAKEBASE_POOL_MAX_SIZE=2
unset PGPASSWORD LAKEBASE_PASSWORD
```

Apply core schema, demo schema, then runtime grants:

```bash
uv run retrieval-lakebase-migrate
uv run retrieval-demo-migrate
uv run retrieval-lakebase-grant-roles \
  --app-role "$APP_DB_ROLE" \
  --worker-role "$WORKER_CLIENT_ID"

uv run retrieval-lakebase-migrate --check --json
uv run retrieval-demo-migrate --check --json
```

Both checks must return `"ready": true`. Migrations are forward-only and checksum-verified. Never
edit an applied migration; add the next numbered migration and rerun grants.

Review effective privileges with both runtime identities. If policy forbids App database DDL, the
owner may revoke database `CREATE` after binding. Recheck after every resource update because the
managed binding may restore it.

## 7. Prepare the external worker deployment

This section defines the handoff to the container platform. The worker must be long-running,
support secret injection, allow outbound TLS, and honor graceful termination.

Build and publish an immutable image:

```bash
export WORKER_BUILD_ID=<GIT_SHA_OR_IMAGE_DIGEST>
export WORKER_IMAGE=<REGISTRY>/retrieval-worker:"$WORKER_BUILD_ID"

docker build -f Dockerfile.worker -t "$WORKER_IMAGE" .
docker push "$WORKER_IMAGE"
```

Inject these secrets through the runtime:

```text
DATABRICKS_CLIENT_ID=<worker service-principal client ID>
DATABRICKS_CLIENT_SECRET=<worker OAuth secret>
TEMPORAL_API_KEY=<Temporal Cloud API key>
```

Set this non-secret environment:

```text
DATABRICKS_HOST=<workspace URL>
DATABRICKS_AUTH_TYPE=oauth-m2m

PGHOST=<Lakebase endpoint host>
PGPORT=5432
PGDATABASE=databricks_postgres
PGUSER=<worker service-principal client ID>
PGSSLMODE=require
LAKEBASE_ENDPOINT=projects/.../branches/.../endpoints/...
LAKEBASE_POOL_MIN_SIZE=1
LAKEBASE_POOL_MAX_SIZE=20
LAKEBASE_APPLICATION_NAME=retrieval-demo-worker

TEMPORAL_ADDRESS=<Temporal Cloud host:port>
TEMPORAL_NAMESPACE=<namespace>
TEMPORAL_TLS=true
TEMPORAL_RETRIEVAL_TASK_QUEUE=retrieval-v2
TEMPORAL_PROVIDER_TASK_QUEUE=retrieval-provider-v2
TEMPORAL_DEPLOYMENT_NAME=retrieval-v2
TEMPORAL_BUILD_ID=<immutable build ID>
TEMPORAL_USE_WORKER_VERSIONING=true
TEMPORAL_ENABLE_SEARCH_ATTRIBUTES=false
TEMPORAL_SERVER_PRIORITY_FAIRNESS_SUPPORTED=false

RETRIEVAL_DEMO_MODE=true
RETRIEVAL_ADAPTER_BUNDLE_FACTORY=retrieval.demo.scripted_provider:create_adapter_bundle
OBJECT_CLEANUP_BATCH_SIZE=250
```

Do not also set individual adapter factories or
`RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS`. Allow outbound TLS to Temporal Cloud, the Databricks
control plane, and Lakebase on TCP 5432. Send `SIGTERM` and allow at least 60 seconds before a hard
kill. The worker exposes no HTTP port.

After the worker starts, verify pollers for:

- `retrieval-v2` (Workflow and Activity tasks);
- `retrieval-provider-v2` (provider Activity tasks).

Only after both queues have pollers should an operator set/ramp the Worker Deployment version.
Never use `--ignore-missing-task-queues` to bypass this safety check.

## 8. Deploy and start the App

Return to the bundle directory:

```bash
cd apps/retrieval_demo

databricks bundle run retrieval_demo \
  --profile "$DBX_PROFILE" \
  -t "$BUNDLE_TARGET" \
  --var "lakebase_branch=$LB_BRANCH_NAME" \
  --var "lakebase_database=$LB_DATABASE_NAME" \
  --var "temporal_secret_scope=$TEMPORAL_SECRET_SCOPE"
```

The command starts compute, snapshots the source, installs dependencies, and deploys the command
`python -m apps.retrieval_demo.app`.

Inspect App state, permissions, and logs:

```bash
databricks apps get "$DEPLOYED_APP_NAME" --profile "$DBX_PROFILE" -o json
databricks apps get-permissions "$DEPLOYED_APP_NAME" --profile "$DBX_PROFILE"
databricks apps logs "$DEPLOYED_APP_NAME" \
  --profile "$DBX_PROFILE" --tail-lines 200
```

Expected state:

- active deployment status `SUCCEEDED`;
- App status `RUNNING`;
- compute status `ACTIVE`;
- no database permission, migration, TLS, or authentication errors in logs.

The App URL requires Databricks OIDC authentication unless permissions/policy are deliberately
changed. Through an authorized browser, verify:

```text
GET /healthz -> 200
GET /readyz  -> 200
```

An unauthenticated `/readyz` request may return an OAuth redirect; that proves the proxy is
reachable, not application readiness.

## 9. Rehearse the scenario

Use a fresh run:

1. confirm `active`, generation 7, zero documents;
2. start sync and observe one five-second quota wait/resume;
3. confirm four committed documents and a held `late-security-review.md`;
4. ask the default question and inspect four citations;
5. start deactivation and wait for `active/7 → deactivating/8`;
6. release the held write only after the fence;
7. confirm stale rejection with expected 7 and actual 8;
8. confirm final `inactive`, generation 8, zero documents/chunks.

Never rewind or reuse a contaminated run. Create another run for every rehearsal and presentation.

## Subsequent deployments

1. Run `make verify` on the exact revision.
2. Create a pre-change Lakebase branch for risky migrations.
3. Apply new core migrations, demo migrations, and grants.
4. Start the new worker build alongside the old build.
5. Verify both queues and route/ramp with the reviewed Worker Versioning procedure.
6. Run `bundle deploy`, then `bundle run retrieval_demo` with all variables.
7. Recheck App resources, permissions, logs, readiness, and database `CREATE` policy.
8. Keep old workers until all assigned executions drain.

App resource updates replace the resource list; inspect bindings after every deployment.

## Rollback

### App

Run the last verified compatible source revision through `bundle deploy` and `bundle run`.

### Worker

Restore the previous immutable image, verify both queues, and route work back through the reviewed
Worker Versioning procedure. Keep every build that owns open executions.

### Database

There are no down migrations. Prefer a forward correction. If restoration is required, stop new
commands and follow the approved Lakebase branch/backup recovery procedure while keeping
Temporal-compatible workers. Never alter an applied checksum or decrement a store generation.

## Troubleshooting

| Symptom | Likely check |
|---|---|
| Databricks profile shows invalid | OAuth login with an explicit descriptive profile |
| Bundle validation fails | CLI version and full branch/database resource names |
| App appears “not deployed” after `bundle deploy` | Run `bundle run retrieval_demo` |
| App crashes with migrations missing | Stop, apply both migrations/grants, redeploy |
| App gets `permission denied` | Compare App client ID with Lakebase `postgres_role`; rerun grants |
| Temporal says `Jwt is missing` | Re-enter API key without a trailing newline; redeploy App |
| `/readyz` redirects | Authenticate through Databricks OIDC, then retry |
| `/readyz` reports database false | Check endpoint, OAuth identity, grants, and migration checks |
| `/readyz` reports Temporal false | Check address, namespace, API key, TLS, and egress |
| Commands remain accepted but idle | Verify worker pollers on both Task Queues/current build |
| App returns 502 | Check App `RUNNING`, logs, port, and `0.0.0.0` binding |
| First request after idle is slow | Pre-warm App/Lakebase and inspect pool retry/pre-ping |

## Completion checklist

- [ ] Explicit Databricks OAuth profile recorded.
- [ ] Dedicated Lakebase resources and capacity/retention settings recorded.
- [ ] Secret scope contains address, namespace, and newline-free API key.
- [ ] App resource has expected Postgres and secret bindings.
- [ ] App and worker use distinct identities.
- [ ] Both migration checks report ready.
- [ ] Runtime grants target the exact Postgres roles.
- [ ] Worker uses an immutable build and polls both queues.
- [ ] App deployment is `SUCCEEDED`, status `RUNNING`, compute `ACTIVE`.
- [ ] Authenticated `/healthz` and `/readyz` pass.
- [ ] Fresh Northstar rehearsal passes through final zero-row state.
- [ ] Rollback owner, prior App revision, and compatible worker build are recorded.

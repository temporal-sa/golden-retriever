# Deploy Golden Retriever through tmprl-demo.cloud

This runbook covers the source-repository inputs that accompany the `DemoProject` resource at
`projects/demo/golden-retriever.yaml` in `temporal-sa/tmprl-demo-cloud-registry`.

The registry owns the Kubernetes namespace, ECR repositories and image builds, Temporal Cloud
namespace and API key, Worker Controller rollout, ingress, and smoke check. Lakebase is an external
dependency: create it and its identities before merging the registry resource.

## Runtime shape

| Component | Image source | Platform services | External secret |
| --- | --- | --- | --- |
| `app` | `Dockerfile.app` | Temporal client, HTTPS ingress on port 8000 | `lakebase-app` |
| `worker` | `Dockerfile.worker` | Versioned Temporal worker | `lakebase-worker` |

The public URL is `https://golden-retriever.tmprl-demo.cloud`. Registry authentication remains
enabled. The rollout smoke check calls `/readyz`, which verifies both Lakebase migrations and
Temporal connectivity.

## 1. Prepare Lakebase

Create or select one dedicated Lakebase Autoscaling branch, endpoint, and database. Record:

```text
LB_BRANCH_NAME=projects/<project>/branches/<branch>
LB_ENDPOINT_NAME=projects/<project>/branches/<branch>/endpoints/<endpoint>
LB_ENDPOINT_HOST=<endpoint-host>
LB_DATABASE=databricks_postgres
```

Create two dedicated Databricks service principals, add them to the workspace, issue OAuth M2M
secrets, and create their Lakebase roles if the roles do not already exist:

```bash
databricks postgres create-role "$LB_BRANCH_NAME" golden-retriever-app \
  --json "{\"spec\":{\"postgres_role\":\"$APP_CLIENT_ID\",\"identity_type\":\"SERVICE_PRINCIPAL\",\"auth_method\":\"LAKEBASE_OAUTH_V1\"}}" \
  --profile "$DBX_PROFILE"

databricks postgres create-role "$LB_BRANCH_NAME" golden-retriever-worker \
  --json "{\"spec\":{\"postgres_role\":\"$WORKER_CLIENT_ID\",\"identity_type\":\"SERVICE_PRINCIPAL\",\"auth_method\":\"LAKEBASE_OAUTH_V1\"}}" \
  --profile "$DBX_PROFILE"
```

The `postgres_role` and `PGUSER` values are the service-principal client IDs, not the role resource
slugs.

## 2. Apply migrations and grants

Use the Lakebase project creator/database owner as the migration identity. From this repository:

```bash
uv sync --frozen --extra lakebase

export DATABRICKS_CONFIG_PROFILE="$DBX_PROFILE"
export PGHOST="$LB_ENDPOINT_HOST"
export PGPORT=5432
export PGDATABASE="$LB_DATABASE"
export PGUSER="$MIGRATION_DB_USER"
export PGSSLMODE=require
export LAKEBASE_ENDPOINT="$LB_ENDPOINT_NAME"
export LAKEBASE_POOL_MIN_SIZE=1
export LAKEBASE_POOL_MAX_SIZE=2
unset DATABRICKS_HOST DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET DATABRICKS_AUTH_TYPE
unset PGPASSWORD LAKEBASE_PASSWORD

uv run retrieval-lakebase-migrate
uv run retrieval-demo-migrate
uv run retrieval-lakebase-grant-roles \
  --app-role "$APP_CLIENT_ID" \
  --worker-role "$WORKER_CLIENT_ID"
uv run retrieval-lakebase-migrate --check --json
uv run retrieval-demo-migrate --check --json
```

Both checks must return `"ready": true`. Re-run migrations and grants before a rollout whenever a
new numbered migration adds database objects.

## 3. Create project secrets

Create these AWS Secrets Manager JSON secrets in the registry's AWS account and region. Use the
AWS console or secret-safe tooling; do not commit the values or place client secrets in shell
history.

```text
tmprl-dem-cld/golden-retriever/lakebase-app
tmprl-dem-cld/golden-retriever/lakebase-worker
```

Each secret has this shape, with the matching app or worker service-principal values:

```json
{
  "DATABRICKS_HOST": "https://<workspace-host>",
  "DATABRICKS_CLIENT_ID": "<service-principal-client-id>",
  "DATABRICKS_CLIENT_SECRET": "<service-principal-oauth-secret>",
  "PGHOST": "<lakebase-endpoint-host>",
  "PGPORT": "5432",
  "PGDATABASE": "databricks_postgres",
  "PGUSER": "<same-service-principal-client-id>",
  "PGSSLMODE": "require",
  "LAKEBASE_ENDPOINT": "projects/<project>/branches/<branch>/endpoints/<endpoint>"
}
```

The registry validates that every property is present before rendering the components. The
Databricks SDK generates a short-lived Lakebase credential for each new database connection; no
database password is stored.

## 4. Validate the source images

From this repository:

```bash
make verify
docker build -f Dockerfile.app -t golden-retriever-app:local .
docker build -f Dockerfile.worker -t golden-retriever-worker:local .
```

Both images run as UID 10001. The App listens on `0.0.0.0:8000`; the worker has no HTTP port and
needs at least 45 seconds of termination grace for coordinated poller shutdown.

## 5. Validate and merge the registry resource

In `tmprl-demo-cloud-registry`, validate the complete registry before committing:

```bash
uv run --isolated --with jsonschema --with pyyaml python scripts/validate_projects.py
```

The resource intentionally tracks `lakebase-variant`, the branch containing these assets. Change
`spec.source.branch` to `main` after this implementation is merged there. Merging the registry
resource starts source reconciliation, image builds, Temporal provisioning, Worker Controller
rollout, and the `/readyz` promotion gate.

## 6. Verify the deployment

After the registry reports the project active:

```bash
curl -fsS https://golden-retriever.tmprl-demo.cloud/healthz
curl -fsS https://golden-retriever.tmprl-demo.cloud/readyz
```

Open the authenticated URL, create a Northstar run, start sync, hold and release the late writer,
ask the fixed evidence question, and deactivate the store. Confirm both `retrieval-v2` and
`retrieval-provider-v2` have current pollers in the project Temporal Cloud namespace.

The cluster needs outbound TLS access to the Temporal Cloud endpoint, the Databricks workspace
control plane, and the Lakebase endpoint on TCP 5432.

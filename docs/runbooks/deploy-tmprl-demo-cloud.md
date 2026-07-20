# Deploy through tmprl-demo.cloud

This runbook deploys the repository's App and worker images through the external
`temporal-sa/tmprl-demo-cloud-registry` platform. It is for operators who already have access to
that registry and its AWS account.

If you do not use tmprl-demo.cloud, follow the general
[Databricks + external worker runbook](deploy-lakebase-temporal-demo.md) instead.

## Division of responsibility

The source repository supplies:

- `Dockerfile.app` for the FastAPI/UI process;
- `Dockerfile.worker` for the long-running Temporal worker;
- Lakebase migrations and role grants;
- the environment contract documented below.

The tmprl-demo.cloud registry supplies:

- Kubernetes namespace and workload resources;
- ECR repositories and source-image builds;
- Temporal Cloud namespace/API key;
- Worker Controller rollout/versioning;
- HTTPS ingress, authentication, and `/readyz` smoke checks.

Lakebase remains external to the registry. Create its database and identities before merging the
registry project resource.

## Resulting runtime

| Component | Source | Platform integration | External secret |
|---|---|---|---|
| `app` | `Dockerfile.app` | HTTPS ingress, Temporal client, port 8000 | `lakebase-app` |
| `worker` | `Dockerfile.worker` | versioned Temporal worker | `lakebase-worker` |

The expected URL is `https://golden-retriever.tmprl-demo.cloud` and remains authenticated.

## Prerequisites

- Databricks OAuth profile with Lakebase/project administration;
- dedicated Lakebase branch, endpoint, and database;
- two Databricks service principals with OAuth M2M secrets;
- permission to create AWS Secrets Manager values in the registry account;
- permission to validate/merge the registry's `DemoProject` resource;
- local `uv`, Docker, and the locked repository dependencies.

## 1. Record Lakebase resources

Create/select one dedicated Lakebase branch and record the API resource names and endpoint host:

```text
LB_BRANCH_NAME=projects/<project>/branches/<branch>
LB_ENDPOINT_NAME=projects/<project>/branches/<branch>/endpoints/<endpoint>
LB_ENDPOINT_HOST=<endpoint-host>
LB_DATABASE=databricks_postgres
```

Use a database dedicated to this App boundary. Confirm endpoint capacity, retention, and outbound
network requirements.

## 2. Create App and worker Lakebase roles

Create two service principals, add them to the workspace, and issue their OAuth secrets through an
approved process. Their client IDs become the Postgres role names and `PGUSER` values.

```bash
databricks postgres create-role "$LB_BRANCH_NAME" golden-retriever-app \
  --json "{\"spec\":{\"postgres_role\":\"$APP_CLIENT_ID\",\"identity_type\":\"SERVICE_PRINCIPAL\",\"auth_method\":\"LAKEBASE_OAUTH_V1\"}}" \
  --profile "$DBX_PROFILE"

databricks postgres create-role "$LB_BRANCH_NAME" golden-retriever-worker \
  --json "{\"spec\":{\"postgres_role\":\"$WORKER_CLIENT_ID\",\"identity_type\":\"SERVICE_PRINCIPAL\",\"auth_method\":\"LAKEBASE_OAUTH_V1\"}}" \
  --profile "$DBX_PROFILE"
```

The API role slugs are labels. Runtime configuration and grants use the service-principal client
IDs returned as `postgres_role`.

## 3. Apply schemas and grants

From this repository, connect as the Lakebase project creator/database owner:

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

Both checks must report `"ready": true`. Run effective privilege tests described in the
[migration runbook](migration-and-rollback.md).

## 4. Create registry secrets

Create these AWS Secrets Manager JSON secrets in the account/region used by the registry:

```text
tmprl-dem-cld/golden-retriever/lakebase-app
tmprl-dem-cld/golden-retriever/lakebase-worker
```

Use the AWS console or approved secret-safe tooling. Do not commit or paste client secrets into
shell history.

Each secret has the same shape but uses its own App or worker identity:

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

The registry validates required properties. The Databricks SDK exchanges M2M credentials for
short-lived Lakebase credentials; no Postgres password is stored.

## 5. Validate source images

```bash
make verify
docker build -f Dockerfile.app -t golden-retriever-app:local .
docker build -f Dockerfile.worker -t golden-retriever-worker:local .
```

Both images run as UID 10001. The App listens on `0.0.0.0:8000`. The worker exposes no HTTP port
and requires at least 60 seconds of termination grace.

## 6. Validate the registry resource

The registry resource is `projects/demo/golden-retriever.yaml`. In the
`tmprl-demo-cloud-registry` repository, validate all projects before committing:

```bash
uv run --isolated --with jsonschema --with pyyaml python scripts/validate_projects.py
```

Confirm `spec.source.branch` points at the source revision/branch containing the intended App,
worker, migrations, and lock file. Merging the resource triggers source reconciliation, image
builds, Temporal provisioning, Worker Controller rollout, ingress, and readiness promotion.

To request another source build after changing external configuration such as AWS secret values,
push a real repository content change. The registry keys reconciliation to the Flux source
artifact digest, so an empty commit is intentionally deduplicated and does not start a build.

## 7. Verify the deployment

After the registry reports the project active:

```bash
curl -fsS https://golden-retriever.tmprl-demo.cloud/healthz
curl -fsS https://golden-retriever.tmprl-demo.cloud/readyz
```

Then:

1. open the authenticated URL;
2. create a fresh Northstar run;
3. start sync and observe quota wait/resume;
4. verify four committed documents and the held writer;
5. ask the fixed evidence question;
6. deactivate and release the writer after the generation fence;
7. verify stale rejection and final inactive/zero-row state;
8. confirm current pollers on `retrieval-v2` and `retrieval-provider-v2`.

The cluster requires outbound TLS to Temporal Cloud, the Databricks workspace/control plane, and
the Lakebase endpoint on TCP 5432.

## Rollback

- Stop new commands if readiness or the rehearsal fails.
- Keep worker builds that own open workflows.
- Restore the previous registry source revision/image digests through the platform's reviewed
  rollout procedure.
- Do not edit applied migrations or decrement a store generation.
- Use the [general rollback procedure](migration-and-rollback.md#rollback-and-recovery) for
  database or workflow compatibility failures.

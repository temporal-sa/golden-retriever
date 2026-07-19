# Local verification snapshot — 2026-07-18

This records evidence for the current source tree and locally built artifacts. It is not a target
capacity, target compatibility, or deployment claim. Environment-specific gates remain in the
[`production-readiness guide`](../docs/architecture-production-readiness.md).

## Environment recorded for this work

- Virtual-environment Python: 3.13.5
- Temporal Python SDK: 1.30.0
- pytest: 8.4.2
- Ruff: 0.15.22
- Databricks CLI used for offline bundle-schema validation: 0.299.2

Dependencies are locked in `uv.lock`, root `requirements.txt`, and
`apps/retrieval_demo/requirements.txt`.

## Verified local results

| Command or check | Result |
|---|---:|
| `uv lock --check` | 52-package lock coherent |
| `uv run ruff check .` | passed |
| `uv run ruff format --check .` | 111 files formatted |
| `uv run python -m compileall -q src tests apps` | passed |
| `uv run pytest -q` | 270 passed, 7 skipped |
| `RUN_TEMPORAL_INTEGRATION=1 uv run pytest -q -m integration tests/integration tests/demo` | 6 passed, 30 deselected |
| `uv run pytest -q -m replay tests/replay` | 2 passed |
| `uv run retrieval-demo-headless --json` | inactive, generation 8, zero documents/chunks, four citations, stale write rejected |
| `node --check apps/retrieval_demo/static/app.js` | passed |
| Databricks CLI offline bundle-schema validation | zero schema errors |
| `uv build` | wheel and source distribution built successfully |

The default suite covers unit, repository contract, Lakebase SQL/pool/migration/search, Northstar
fixture/service/headless, FastAPI, packaging-security, shutdown, and checked-in history replay
behavior. Its skips are the opt-in Temporal integration and load scenarios.

The full Northstar integration starts an SDK-managed time-skipping Temporal server and the real
controller/root/provider/document/deactivation/cleanup topology. It observes the five-second
quota wait, commits four cited documents, holds the fifth before commit, commits the generation-8
fence, reaches `inactive` with zero rows, then releases the held generation-7 write and observes
the stale-generation rejection. The integration suite also covers cancellation, controller
terminal queries, and Task Queue topology. It uses in-memory persistence and does not claim live
Lakebase validation.

Checked-in replay inputs are:

- `artifacts/histories/root-sync-replay-smoke.json`;
- `artifacts/histories/remove-objects-pre-batch.json`.

The headless and browser rehearsals exercised the four-panel App, a fresh UUID run, workflow IDs,
generation changes, the cited answer, and the terminal late-writer sequence. No browser console
error remained after the favicon and release-state fixes.

## Deliberately not performed

- No Lakebase project, branch, database, role, migration, grant, or data was created or changed.
- No Databricks App bundle was deployed or started in a workspace.
- No authenticated target `databricks bundle validate` ran because no explicit valid profile and
  target resource values were available.
- Docker Engine 29.6.1 was reachable, but the worker image build could not resolve base-image
  metadata from GHCR/Docker Hub before the registry deadline. Dependency installation and
  Dockerfile structure were validated separately with Python 3.12; no partial image was tagged.
- No worker image was published and no worker process was deployed.
- No cloud Temporal namespace was accessed or changed.

Before a real deployment, validate with an explicitly selected authenticated Databricks profile,
run the migrations and effective-privilege tests against a dedicated Lakebase database, verify the
managed App database `CREATE` privilege, build the worker image with approved immutable base-image
digests, and test target Temporal/Lakebase connectivity. Use the root
[README](../README.md#test-and-validate) and
[migration runbook](../docs/runbooks/migration-and-rollback.md#pre-rollout-artifact-verification)
for the exact commands.

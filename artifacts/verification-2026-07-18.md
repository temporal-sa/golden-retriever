# Verification record: 2026-07-18 source snapshot

This is a time-bounded engineering evidence record, not a setup guide or current deployment
status. It documents what was executed against the source snapshot on 2026-07-18 and what those
results did—and did not—prove.

For current commands, use the root [README](../README.md#test-the-project). For environment release
gates, use the [production-readiness guide](../docs/architecture-production-readiness.md).

## Recorded toolchain

| Tool | Version |
|---|---|
| Python virtual environment | 3.13.5 |
| Temporal Python SDK | 1.30.0 |
| pytest | 8.4.2 |
| Ruff | 0.15.22 |
| Databricks CLI | 0.299.2 |

Dependencies were locked in `uv.lock`, root `requirements.txt`, and the App requirements file.

## Recorded results

| Command/check | Result on the snapshot |
|---|---|
| `uv lock --check` | 52-package lock coherent |
| `uv run ruff check .` | passed |
| `uv run ruff format --check .` | 111 Python files formatted |
| `uv run python -m compileall -q src tests apps` | passed |
| `uv run pytest -q` | 270 passed, 7 skipped |
| Temporal integration selection | 6 passed, 30 deselected |
| replay suite | 2 passed |
| headless Northstar rehearsal | inactive at generation 8, zero documents/chunks, four citations, stale write rejected |
| JavaScript syntax check | passed |
| offline Databricks bundle schema validation | no schema errors |
| `uv build` | wheel and source distribution built |

The default suite covered deterministic unit behavior, repository contracts, Lakebase SQL/pool/
migration/search logic, Northstar state/service behavior, FastAPI routes, packaging security,
worker shutdown, and checked-in replay histories.

The integration snapshot ran the real Temporal workflow hierarchy against SDK-managed servers
with in-memory persistence. It observed the quota wait, four committed documents, held writer,
generation fence, stale rejection, bounded cleanup, and terminal inactive state. It did not execute
those mutations against Lakebase.

Checked-in replay inputs at the time were:

- `artifacts/histories/root-sync-replay-smoke.json`;
- `artifacts/histories/remove-objects-pre-batch.json`.

## Scope limitations

This record did not prove:

- live Lakebase connectivity, migrations, grants, or race behavior;
- authenticated Databricks bundle validation or App startup;
- worker image publication or a cloud worker rollout;
- target Temporal namespace compatibility/capacity;
- production provider/staging behavior;
- dashboards, alerts, backups, secret rotation, or disaster recovery.

Do not reuse the pass counts as evidence for a later revision. Re-run `make verify`, required
integration/replay/load selections, image builds, bundle validation, migration checks, and live
readiness for the exact release candidate.

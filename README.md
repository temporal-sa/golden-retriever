# Temporal Retrieval Workflow V2

This repository is a greenfield reference implementation of the supplied Temporal
Retrieval Workflow V2 specification. It adds durable store lifecycle ownership,
generation-fenced mutations, shared quota coordination, bounded fan-out, cancellation
ownership, and feature-gated Temporal priority/fairness metadata.

The checkout was empty when implementation began. Consequently, there were no existing
workflow histories, connector algorithms, persistence adapter, namespace configuration,
or deployment version to preserve. Those facts and the resulting migration seam are
recorded in [`IMPLEMENTATION_MAP.md`](IMPLEMENTATION_MAP.md).

## Setup

Python 3.11 or newer and [uv](https://docs.astral.sh/uv/) are required. Install the project and
its development dependencies with:

```bash
uv sync --extra dev
```

## Run the workflows locally

Install the [Temporal CLI](https://docs.temporal.io/cli) and start a development server in one
terminal:

```bash
temporal server start-dev
```

The default server listens on `localhost:7233`, and its Web UI is available at
<http://localhost:8233>.

In a second terminal, start the retrieval and provider workers:

```bash
RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS=true uv run retrieval-worker
```

This local-only flag uses the included non-durable repository, staging store, and empty provider
adapters. The worker polls the `retrieval-v2` and `retrieval-provider-v2` task queues. Application
code submits controller commands through `RetrievalClient`.

To actually execute and validate the workflows against the running development server, run the
smoke-test starter:

```bash
uv run retrieval-test-starter
```

The starter creates isolated test workers and a unique in-memory store, executes sync and
deactivation through the public client API, verifies their results and lifecycle generation, and
prints a JSON summary. It exits nonzero if any check fails and does not require the regular
`retrieval-worker` process to be running. Use `--address`, `--namespace`, or `--store-key` to
override its local defaults.

For an existing Temporal namespace, set `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, and, when
needed, `TEMPORAL_API_KEY` and `TEMPORAL_TLS`. Real workflow execution also requires all three
adapter factories described in the
[`migration and rollback runbook`](docs/runbooks/migration-and-rollback.md#production-adapter-bootstrap).

## Test the workflows

Run the default test suite (unit and contract tests run; opt-in Temporal, replay, and load tests
are skipped):

```bash
uv run pytest
```

Run all integration scenarios against an ephemeral local Temporal server:

```bash
RUN_TEMPORAL_INTEGRATION=1 uv run pytest -m integration tests/integration
```

The Temporal SDK may download a matching CLI binary on the first run. To use the development
server started above instead, add `TEMPORAL_INTEGRATION_ADDRESS=localhost:7233`. To run only the
end-to-end workflow topology:

```bash
RUN_TEMPORAL_INTEGRATION=1 uv run pytest -m integration \
  tests/integration/test_full_topology.py
```

Replay exported histories placed under `artifacts/histories/` with:

```bash
uv run pytest -m replay tests/replay
```

Run the opt-in load harness with:

```bash
RUN_TEMPORAL_LOAD=1 uv run pytest -s -m load tests/load
```

See the suite-specific notes in [`tests/integration`](tests/integration/README.md),
[`tests/replay`](tests/replay/README.md), and [`tests/load`](tests/load/README.md). Run the static
checks with:

```bash
uv run ruff check .
uv run ruff format --check .
```

See [`docs/runbooks/migration-and-rollback.md`](docs/runbooks/migration-and-rollback.md)
before enabling the V2 entry path.

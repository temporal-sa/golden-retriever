# Temporal Retrieval Workflow V2

This repository is a greenfield reference implementation of the supplied Temporal
Retrieval Workflow V2 specification. It adds durable store lifecycle ownership,
generation-fenced mutations, shared quota coordination, bounded fan-out, cancellation
ownership, and feature-gated Temporal priority/fairness metadata.

The checkout was empty when implementation began. Consequently, there were no existing
workflow histories, connector algorithms, persistence adapter, namespace configuration,
or deployment version to preserve. Those facts and the resulting migration seam are
recorded in [`IMPLEMENTATION_MAP.md`](IMPLEMENTATION_MAP.md).

## Development

```bash
uv sync --extra dev
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

Run a worker after configuring Temporal and persistence:

```bash
uv run retrieval-worker
```

See [`docs/runbooks/migration-and-rollback.md`](docs/runbooks/migration-and-rollback.md)
before enabling the V2 entry path.


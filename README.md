# Temporal Retrieval Workflows

This repository implements a durable retrieval pipeline with the Temporal Python SDK. It
coordinates provider pagination, document ingestion, shared provider quotas, failed-user
remediation, and store deactivation while protecting every persistent mutation with a lifecycle
generation fence.

The project is a workflow reference implementation, not a complete hosted service. It includes
Temporal workflows, activities, a Python client, local adapters, and test infrastructure. It does
not include an HTTP API, a production database adapter, an object-store adapter, or a real provider
connector.

## Start here

A new contributor should read the documentation in this order:

1. This README for setup, commands, and the system's mental model.
2. [`IMPLEMENTATION_MAP.md`](IMPLEMENTATION_MAP.md) for the source tree, runtime configuration,
   workflow inventory, IDs, and invariants.
3. [`docs/workflow-topology.md`](docs/workflow-topology.md) for the execution diagrams and failure
   boundaries.
4. [`docs/architecture-production-readiness.md`](docs/architecture-production-readiness.md) before
   treating the project as production-ready.
5. [`docs/runbooks/migration-and-rollback.md`](docs/runbooks/migration-and-rollback.md) when
   preparing a real deployment or upgrade.

## Mental model

Each store has one long-lived `StoreControllerWorkflow`. Applications send it idempotent commands
through `RetrievalClient`:

- `request_sync` starts one store sync at a time;
- `cancel_sync` cancels a tracked sync;
- `start_deactivation` advances the lifecycle generation, drains retrieval work, removes data, and
  marks the store inactive;
- `get_status` reports compact controller state.

A sync walks this hierarchy:

```text
StoreControllerWorkflow
└── RootSyncWorkflow
    └── UserSyncWorkflow
        └── ResourceSyncWorkflow
            └── ResourcePagesWorkflow
                └── FilesPageWorkflow
                    └── DocumentIngestionWorkflow (Potentially Optional)
```

Fan-out is bounded and joined at every level. Provider response bodies never enter Workflow Event
History: workflows carry `DocumentRef` metadata, while the ingestion Activity reads the body from
a `StagingStore`. Failed page work retains a retry-safe cursor and is sent to a bounded,
controller-tracked remediation workflow.

Provider calls can share a `UserQuotaWorkflow` keyed by provider, opaque credential key, and quota
class. The coordinator grants or explicitly denies permits, tracks reset observations, limits
in-flight work, and caps its pending queue at 350 requests.

Every write checks the store's authoritative lifecycle generation in the same transaction as the
mutation. Deactivation commits a new generation before requesting cancellation, so late Activity
completion cannot write into an inactive store.

## Prerequisites

- Python 3.11 or newer
- [`uv`](https://docs.astral.sh/uv/)
- [Temporal CLI](https://docs.temporal.io/cli) for the local development server

Install the project and development dependencies:

```bash
uv sync --extra dev
```

## Fastest local workflow check

Start Temporal in one terminal:

```bash
temporal server start-dev
```

The default frontend is `localhost:7233`; the Web UI is normally
[http://localhost:8233](http://localhost:8233).

In another terminal, run the executable smoke test:

```bash
uv run retrieval-test-starter
```

The starter creates isolated task queues and local workers, runs sync and deactivation through the
public client API, verifies the final generation and lifecycle state, prints a JSON result, and
cleans up its controller. It uses an empty provider, so it validates orchestration rather than real
document retrieval. It does not require `retrieval-worker` to be running.

Useful options:

```bash
uv run retrieval-test-starter --address localhost:7233 --namespace default
uv run retrieval-test-starter --store-key my-local-store
```

## Run a persistent local worker

For manual development, leave the Temporal server running and start both project workers with the
explicit local-adapter flag:

```bash
RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS=true uv run retrieval-worker
```

This process polls two Task Queues:

- `retrieval-v2` for workflows and persistence-facing activities;
- `retrieval-provider-v2` for provider activities.

The local repository and staging store are in-memory and disappear when the worker exits. The
local provider is empty. Never enable `RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS` in production.
Use `RetrievalClient` from application code to submit controller commands; there is no bundled CLI
or HTTP endpoint for arbitrary sync requests.

## Submit a sync from Python

The application must provision the store in its `RetrievalRepository` first and know its current
lifecycle generation. Then it can connect with the same runtime settings as the worker:

```python
from temporalio.client import Client

from retrieval.config import RetrievalTemporalConfig
from retrieval.temporal.client import RetrievalClient
from retrieval.temporal.models.operations import SyncCommand
from retrieval.temporal.models.sync import SyncResult
from retrieval.temporal.runtime_config import TemporalRuntimeConfig

runtime = TemporalRuntimeConfig.from_env()
config = RetrievalTemporalConfig.from_env()
temporal = await Client.connect(
    runtime.address,
    namespace=runtime.namespace,
    api_key=runtime.api_key,
    tls=runtime.tls,
)
retrieval = RetrievalClient.from_runtime(temporal, runtime=runtime, config=config)

accepted = await retrieval.request_sync(
    SyncCommand(
        command_id="sync-command-2026-07-18T120000Z",
        store_key="store-123",
        expected_generation=0,
        sync_sequence="scheduled-2026-07-18T120000Z",
        metadata={
            "provider": "example-provider",
            # This identifies a quota scope; it is not the provider secret.
            "credential_key": "provider-account-42",
            "resource_types": "files",
        },
    )
)
result = await temporal.get_workflow_handle(
    accepted.workflow_id,
    result_type=SyncResult,
).result()
```

Reuse a `command_id` only when retrying the same logical command. `sync_sequence` is the stable
identity of the sync operation. The `credential_key` must be an opaque account or quota-scope key,
never an access token. See [`IMPLEMENTATION_MAP.md`](IMPLEMENTATION_MAP.md#sync-command-policy) for
all supported policy metadata and the cancellation/deactivation command types.

## Test and validate

Run the default suite, which includes unit tests, contract tests, and the checked-in replay smoke
history:

```bash
uv run pytest
```

Run real Temporal integration scenarios against SDK-managed ephemeral servers:

```bash
RUN_TEMPORAL_INTEGRATION=1 uv run pytest -m integration tests/integration
```

Run only the complete provider-to-document topology:

```bash
RUN_TEMPORAL_INTEGRATION=1 uv run pytest -m integration \
  tests/integration/test_full_topology.py
```

Replay checked-in and locally exported histories:

```bash
uv run pytest -m replay tests/replay
```

Run the opt-in synthetic load harness:

```bash
RUN_TEMPORAL_LOAD=1 uv run pytest -s -m load tests/load
```

Run static validation:

```bash
uv run ruff check .
uv run ruff format --check .
uv run python -m compileall -q src tests
```

The suite-specific guides explain external-server options and limitations:
[`integration`](tests/integration/README.md), [`replay`](tests/replay/README.md), and
[`load`](tests/load/README.md).

## Production use

The worker fails closed unless all three production adapter factories are configured:

```text
RETRIEVAL_REPOSITORY_FACTORY=package.module:create_repository
RETRIEVAL_STAGING_STORE_FACTORY=package.module:create_staging_store
RETRIEVAL_PROVIDER_GATEWAY_FACTORY=package.module:create_provider_gateway
```

Production also requires namespace preparation, representative history replay, telemetry export,
Worker Versioning, high-availability worker replicas, and production-scale validation. The exact
requirements and known gaps are maintained in the
[`production-readiness guide`](docs/architecture-production-readiness.md).

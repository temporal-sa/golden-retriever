# Documentation guide

This directory explains a retrieval system built with Temporal, Lakebase Postgres, and a
Databricks App. You do not need prior knowledge of the repository to use these guides.

Start with the root [README](../README.md). It explains the problem, the three runtime processes,
and the quickest ways to run or test the project.

## Choose a reading path

| Goal | Read in this order |
|---|---|
| Understand the system | [System specification](lakebase-temporal-demo-spec.md), then [workflow topology](workflow-topology.md) |
| Find code or configuration | [Implementation map](../IMPLEMENTATION_MAP.md) |
| Run the demo locally | [Root README](../README.md#run-the-complete-demo-locally), then [App guide](../apps/retrieval_demo/README.md) |
| Implement the Google Drive demo | [Google Drive demo implementation specification](google-drive-demo-implementation-spec.md) |
| Present the 10-minute demo | [Google Drive presenter runbook](runbooks/google-drive-demo.md) |
| Deploy to Databricks | [Databricks deployment runbook](runbooks/deploy-lakebase-temporal-demo.md) |
| Deploy through tmprl-demo.cloud | [tmprl-demo.cloud runbook](runbooks/deploy-tmprl-demo-cloud.md) |
| Connect Google Drive | [Google Drive integration](google-drive-integration.md) |
| Change database schemas | [Migration and rollback runbook](runbooks/migration-and-rollback.md) |
| Evaluate production readiness | [Production-readiness guide](architecture-production-readiness.md) |
| Operate dashboards and alerts | [Metrics and observability](operations/metrics.md) |
| Understand a design choice | [Workflow-boundary ADR](adr/0001-workflow-boundaries.md) |
| Run specialized tests | [Integration](../tests/integration/README.md), [replay](../tests/replay/README.md), or [load](../tests/load/README.md) |

## Terms used throughout the documentation

- **Activity:** Temporal code that performs external I/O, such as a provider call or database
  transaction. Temporal may execute an Activity more than once.
- **App:** the FastAPI process that serves the browser UI and HTTP API. It submits Temporal
  commands and reads Lakebase; it does not run workers.
- **Databricks Asset Bundle (DAB):** the configuration in
  `apps/retrieval_demo/databricks.yml` that creates and updates the Databricks App and its resource
  bindings.
- **Generation:** a monotonically increasing number on each store. A mutation is accepted only
  when it targets the current generation.
- **Lakebase:** Databricks' managed Postgres service. It stores the authoritative lifecycle and
  retrieval data.
- **Northstar:** the packaged five-document source substitute used only for local rehearsals.
- **Store:** one independently synchronized and deactivated retrieval data set.
- **Task Queue:** the Temporal queue from which a worker polls work. This project uses one queue
  for workflows/database work and one for provider calls.
- **Temporal worker:** the long-running process that executes workflows and Activities. It is
  deployed separately from the App.
- **Workflow:** deterministic Temporal code that durably coordinates work, retries, timers,
  cancellation, and child workflows.

## Documentation conventions

- Commands run from the repository root unless a section says otherwise.
- Values such as `<PROFILE>` or `<PROJECT>` are placeholders; replace the whole value, including
  angle brackets.
- Examples use `dev` for a rehearsal environment. Production deployment always requires an
  environment-specific review.
- Never commit `.env` files, API keys, OAuth client secrets, database passwords, or exported
  customer histories.
- A successful local test is evidence about the source tree, not proof that a cloud target is
  configured correctly.

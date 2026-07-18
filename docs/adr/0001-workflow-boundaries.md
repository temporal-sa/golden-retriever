# ADR 0001: Retrieval V2 workflow boundaries

- Status: accepted for the greenfield reference implementation
- Date: 2026-07-18

## Decision

Keep user-round, resource, page-window, file-page, document-ingestion, remediation, and
cleanup boundaries as distinct Workflow Types. Add a long-lived store controller for
low-volume lifecycle decisions and one quota workflow per real provider credential/quota
class. Remove request-scoped quota waits and accessioning only from new execution paths.

The store controller starts operations with stable IDs and tracks them by durable state;
it never performs retrieval fan-out. Sync and deactivation operations are detached with
`ABANDON`, then report terminal state by idempotent signal. This makes controller
Continue-As-New independent of in-memory child handles.

Quota is admission control, not dispatch scheduling. A short Activity performs atomic
Signal-with-Start, then the requesting Workflow parks on `workflow.wait_condition`.
Priority and fairness metadata is attached only to provider work after a permit exists.

Store deactivation always follows `fence -> cancel -> drain -> cleanup`. Mutating
activities condition their commits on the authoritative status and generation. Activity
cancellation is therefore an optimization; the generation fence is the safety invariant.

Workflow history carries `DocumentRef` values only. Bodies live in a staging/object store
and are loaded by the ingestion Activity.

## Consequences

- More Workflow Types remain registered, but each boundary owns bounded concurrency,
  retry state, or history partitioning.
- A quota request costs signals and a short client Activity but consumes no worker slot
  while blocked.
- Shared quota work is canceled at request scope; the shared coordinator is never canceled
  because one caller ends.
- Deployment-based Worker Versioning and representative replay histories are mandatory at
  rollout time because this greenfield checkout has no legacy histories.


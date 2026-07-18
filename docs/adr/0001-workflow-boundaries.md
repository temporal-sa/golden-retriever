# ADR 0001: Retrieval workflow boundaries

- Status: accepted
- Date: 2026-07-18

## Context

Retrieval must coordinate several kinds of durable state at different scales: one store lifecycle,
many users and resources, paginated provider data, individual document mutations, shared external
quotas, failed-user remediation, and store cleanup. A single workflow would accumulate excessive
history and mix lifecycle ownership with high-volume fan-out. Using Activities for coordination
would make waits, cancellation, and ownership dependent on worker processes.

The design also has two correctness constraints:

1. provider response bodies must not inflate Workflow Event History; and
2. deactivation must prevent late, retried Activities from mutating an inactive store.

## Decision

Keep store control, root sync, user, resource, page window, file page, document ingestion,
remediation, activation, quota, and cleanup as distinct Workflow Types.

The long-lived `StoreControllerWorkflow` owns low-volume lifecycle decisions. It serializes
idempotent commands, starts sync and deactivation operations with stable IDs, and tracks their
durable status. It does not perform retrieval fan-out.

Root sync, failed-user remediation, and store deactivation are detached from the controller after
Temporal acknowledges their stable start. They report status through idempotent Signals. Joined
children own bounded concurrency, retry state, cursor checkpoints, or a deliberate history
partition.

Use one `UserQuotaWorkflow` per provider, opaque credential key, and quota class. A short Activity
performs Signal-with-Start, then the caller waits durably on a workflow condition. Provider
Priority/Fairness metadata applies only after quota admission.

Store deactivation always follows `fence → cancel → drain → cleanup`. Every mutating Activity
compares the expected lifecycle generation and allowed state in the same transaction as its
write. Cancellation reduces wasted work; the generation fence provides safety.

Workflow inputs and results carry compact `DocumentRef` metadata. Document bodies remain in a
staging or object store and are loaded by the ingestion Activity.

## Rationale

- The controller gives each store one durable command and lifecycle authority.
- Detached operations can outlive a controller run while remaining discoverable by stable ID.
- Joined child boundaries keep failure propagation and concurrency ownership explicit.
- Page and root Continue-As-New boundaries keep Event History bounded.
- Shared quota state survives caller and worker restarts without occupying Activity slots.
- Staged document bodies bound payload size and replay cost.
- Atomic generation checks make at-least-once Activity delivery safe across deactivation.

## Alternatives considered

### One workflow per store for all retrieval work

Rejected because high-volume child work, signals, and provider waits would share one long-lived
history with lifecycle commands. Continue-As-New would also have to carry much more mutable state.

### Request-scoped quota wait workflows

Rejected for the primary path because they fragment one real provider quota across many workflow
instances and require extra coordination to enforce a shared limit.

### Waiting or sleeping inside Activities

Rejected because it consumes Activity capacity and makes durable admission state harder to
inspect, cancel, deduplicate, and recover.

### Cancellation as the deactivation safety boundary

Rejected because Activity completion and retry can race with cancellation. A committed generation
fence is authoritative even when cancellation is delayed or ignored.

### Put document bodies in workflow payloads

Rejected because provider pages and histories would become large, expensive to replay, and more
likely to contain sensitive content.

## Consequences

- The worker registers more Workflow Types, and compatible implementations must remain available
  for any open histories that reference them.
- Detached work requires stable IDs, explicit controller registration, idempotent status Signals,
  and operational visibility.
- Quota acquisition adds a short client Activity and Signals, but blocked requests use no Activity
  worker slot.
- Production adapters must implement atomic generation compare-and-write and idempotent mutations.
- Workflow upgrades require representative history replay and compatible Worker Versioning
  routing.

## Reconsider this decision when

- measured history and scheduling behavior shows a boundary is unnecessary or too coarse;
- Temporal introduces a simpler durable shared-admission primitive with equivalent semantics;
- provider APIs expose a different quota model that cannot be represented by the current scope;
- the persistence model can no longer provide atomic generation-fenced mutations.

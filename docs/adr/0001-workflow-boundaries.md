# ADR 0001: Separate workflow boundaries by ownership and scale

- Status: accepted
- Decision date: 2026-07-18

An architecture decision record (ADR) explains a durable design choice, the alternatives that were
considered, and the conditions that would justify revisiting it. This ADR concerns how retrieval
work is divided among Temporal Workflow Types.

## Context

One store sync can involve many users, resources, provider pages, and documents. The system must
also coordinate shared provider quota, retry failed users, deactivate a store, and clean its data.
These concerns operate at different scales and lifetimes.

The design must satisfy three constraints:

1. provider response bodies must not inflate Temporal Workflow Event History;
2. at-least-once Activity execution must not duplicate database effects; and
3. deactivation must reject late generation writes even when cancellation loses a race.

A **joined child** must complete before its parent. A **detached operation** has a stable Workflow
ID, is durably started, and reports status to the store controller without making the controller
wait for the entire high-volume history.

## Decision

Use distinct Workflow Types for store control, root sync, user/resource/page/document fan-out,
failed-user remediation, user activation, shared quota, deactivation, and cleanup.

The long-lived `StoreControllerWorkflow` is the command and lifecycle authority for one store. It
serializes idempotent sync/cancel/deactivation commands, starts stable detached operations, and
tracks their status. It does not perform retrieval fan-out.

Root sync, failed-user remediation, and deactivation are detached after Temporal acknowledges their
stable start. Their bounded descendants are joined, so each parent owns its failures,
concurrency, cursor/checkpoint, and completion.

Use one `UserQuotaWorkflow` per provider, opaque credential key, and quota class. A short Activity
performs Signal-with-Start; callers then wait durably without occupying Activity capacity.

Deactivation always uses `fence → cancel → drain → bounded cleanup`. Every mutating Activity
compares the expected generation and lifecycle state inside the same Lakebase transaction as its
write. Cancellation reduces work; the database generation fence provides safety.

Workflow messages carry compact `DocumentRef` values. The ingestion Activity loads, verifies,
parses, and chunks the body from staging. Bodies and chunks never become workflow payloads.

## Why this design was selected

- The controller gives each store one durable command authority.
- Detached operations can survive controller Continue-As-New while remaining discoverable by
  stable ID.
- Joined children make concurrency ownership and failure propagation explicit.
- Root/page/cleanup boundaries keep Event History bounded.
- Shared quota state survives worker/caller restarts and coordinates real provider capacity.
- Staged bodies bound payload size, sensitivity, and replay cost.
- Atomic generation checks plus durable receipts make Activity retries safe.

## Alternatives considered

### Put all store work in one Workflow

Rejected because lifecycle commands, fan-out, provider waits, and high-volume child events would
share one long-lived history. Continue-As-New would also have to carry a much larger mutable state.

### Create one quota-wait Workflow per request

Rejected because many request-scoped workflows would fragment one real provider limit and require
another coordination layer to enforce shared capacity.

### Sleep or wait inside Activities

Rejected because waiting would consume worker capacity and make admission state harder to inspect,
deduplicate, cancel, and recover.

### Rely on cancellation to protect deactivation

Rejected because an Activity may complete or retry after cancellation is requested. Only an atomic
database generation check can reject that late mutation.

### Put document bodies in workflow payloads

Rejected because histories would become large, costly to replay, and more likely to contain
sensitive content.

## Consequences

- The worker registers more Workflow Types and must preserve compatible implementations for open
  histories.
- Detached operations require stable IDs, controller registration, idempotent status Signals, and
  operational visibility.
- Quota admission adds a bridge Activity and Signals, while durable waiting uses no Activity slot.
- Production adapters must implement atomic generation compare-and-write and idempotent mutations.
- Database migrations/ownership stay outside workflows; App and worker identities receive explicit
  grants.
- Workflow upgrades require representative replay and compatible Worker Versioning routing.

## Revisit this decision when

- measurements show a boundary creates more scheduling/history cost than it saves;
- Temporal offers a simpler shared-admission primitive with equivalent durability;
- a provider quota model cannot be represented by the current scope;
- the target database cannot provide atomic generation-fenced transactions;
- real workload evidence supports a different fan-out or Continue-As-New boundary.

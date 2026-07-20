# 10-minute Google Drive demo runbook

## Audience and takeaway

This demo is for Databricks employees. The sentence to leave them with is:

> Temporal remembers what must finish; Lakebase decides what is still authoritative. Together they
> turn retries, mutable source data, hybrid retrieval, and late-write races into a small application
> with explicit invariants.

The browser experience is the narrative surface. Google Drive makes the source real, Temporal UI
shows durable execution, and Lakebase tooling proves the database behavior.

## Stable-folder contract

Use one permanent Drive folder ID. Keep four to eight short, searchable files in it so names and
answers remain legible on screen. You may edit files or add/move/delete files between rehearsals;
that change is part of the story.

Keep one permanent file as the held late writer:

- its Drive file ID must stay stable;
- it must remain inside the configured root folder and be searchable;
- `GOOGLE_DRIVE_HELD_FILE_ID=<file-id>` on the worker and
  `demo_held_document_key=gdrive:<file-id>` in the App bundle must refer to the same file;
- renaming or editing it is safe, but deleting it or moving it outside the root breaks the race.

The held file may be an uploaded PDF with embedded text. For the 37 MB FlightFactor manual, set
`GOOGLE_DRIVE_MAX_FILE_BYTES=52428800`; PDF OCR is not part of this demo.

The App receives only the folder/tooling URLs. Google credentials and file bodies belong to the
worker; staged bodies and traversal checkpoints belong to Lakebase.

## Rehearsal gate

Complete this once immediately before the audience arrives:

- Lakebase Search Beta is enabled and both migration checks are current.
- The embedding endpoint returns 1024-dimensional vectors and both runtime identities can query it.
- The worker polls `retrieval-v2` and `retrieval-provider-v2` and has a 60-second graceful-stop window.
- `RETRIEVAL_ADAPTER_BUNDLE_FACTORY=retrieval.google_drive.bundle:create_adapter_bundle` and
  `RETRIEVAL_STAGING_BACKEND=lakebase` are set on the worker.
- The stable folder ID, held file ID, Drive credentials, Temporal credentials, and Lakebase OAuth
  identity are present in the worker secret store.
- App `/readyz` is green and its Google Drive, Temporal UI, and Lakebase links open without embedded
  credentials, query strings, or fragments.
- A fresh rehearsal reaches inactive generation 8. Do not reuse that run for the presentation.

Have the three tool tabs authenticated and ready, but start the audience on the App.

## Live run of show

### 0:00–1:00 — Make the source real

Open Google Drive from the App header and show the stable folder. Mention one or two recognizable
files, then return to the App and select **Inspect Drive folder**.

Say: “This is a bounded Temporal Workflow, but it moves only metadata through history. The provider
Activity reads Drive; bodies stay out of Workflow history.”

Wait for the source list. Point out the file tagged **held writer**. Do not start if preflight cannot
find it.

### 1:00–3:00 — Let Temporal absorb provider failure

Select **Create fresh run**, then **Start sync**. The run begins at generation 7 and the real Drive
provider injects exactly one durable five-second throttle.

Open **Technical details** and then Temporal UI. Show the controller/sync execution and the provider
retry or quota wait. Avoid narrating every Workflow; emphasize that a worker can restart because
the traversal cursor and staged bodies are in Lakebase.

Say: “The application issued one command. Temporal owns waiting, retry, cancellation, and recovery.”

Return when the status says recovered and documents/chunks are visible. One file remains held just
before its database commit.

### 3:00–5:00 — Show the Lakebase retrieval path

Ask a natural question about the current folder. You can use the default or change it to match files
you added before the demo.

Point to each result's BM25 rank, vector rank, reciprocal-rank-fusion score, and committed generation.
Both candidate queries apply the same lifecycle/generation visibility predicate before fusion.

Open Lakebase tooling. Show the `retrieval.document_chunks` table and the `lakebase_bm25` and
`lakebase_ann` indexes, or run a prepared read-only inspection. Do not improvise DDL during the demo.

Say: “Lakebase is doing more than storing application state: one Postgres transaction owns connector
checkpoints, write receipts, the authority fence, and retrieval indexes.”

### 5:00–7:00 — Move authority before cleanup

Return to the App and select **Commit generation fence**. Wait until Lakebase authority displays
generation 8. Cleanup may still be running; that is intentional.

Say: “Deactivation does not first chase every in-flight writer. It advances authority atomically.
From this moment, every generation-7 writer is stale.”

In Temporal UI, show that the held Activity execution still exists. In Lakebase tooling, show the
store at generation 8 if useful.

### 7:00–8:30 — Release the race

Select **Release late writer**. The Activity resumes and reaches the same normal commit path. The
transaction compares expected generation 7 with actual generation 8 and rejects the write.

Open **Technical details** and point to the durable `stale_generation_rejected` event with both
generations. This is the memorable proof: orchestration resumed correctly, and database authority
kept the retrieval corpus correct.

### 8:30–10:00 — Prove and recap

Select **Load Lakebase proof**. Call out:

- lifecycle `inactive`, generation 8;
- durable write receipts remain as evidence;
- visible current-generation documents/chunks are zero even if physical cleanup timing differs.

Finish with the three recap cards:

- **Temporal:** durable execution, retries, cancellation, and visibility.
- **Lakebase:** connector state, hybrid search, durable receipts, and a transactional authority fence.
- **Application:** commands, reads, and a few explicit invariants—no bespoke retry scheduler or race
  coordinator.

## Presenter recovery

| Symptom | Recovery |
|---|---|
| Preflight does not complete | Use Technical details/Temporal UI; verify provider poller and Drive credentials. Do not create a run. |
| Held file is missing | Restore it to the stable folder or update both held-file settings, then preflight again. |
| Sync is idle | Verify both Task Queue pollers. Starting another App run does not repair a missing worker. |
| Retrieval fails | Check the embedding endpoint and Lakebase Search/index readiness; do not switch to a fallback backend. |
| Deactivation reports failure | Use the visible **Retry deactivation** action; command idempotency and the generation fence make the retry safe. |
| A run was contaminated | Create a fresh run. Never decrement or reset a committed generation. |

## After the demo

Leave the stable Drive folder intact for future edits. Runs, events, receipts, and proof remain
durable; apply the environment's normal retention policy later rather than deleting state during the
presentation.

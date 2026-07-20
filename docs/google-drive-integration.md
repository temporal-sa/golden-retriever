# Google Drive integration

The Google Drive adapter copies searchable Drive content through the existing Temporal provider
boundary and commits it with the same Lakebase lifecycle-generation fence as every other document.
It is read-only: the worker lists and downloads Drive files but never changes Drive content or
permissions.

## What it supports

| Drive content | Searchable representation |
|---|---|
| Google Docs | plain text export |
| Google Sheets | CSV export of the first sheet |
| Google Slides | plain text export |
| Uploaded `text/*` files | original UTF-8 bytes |
| JSON, XML, YAML, SQL, RTF, and related text application types | original UTF-8 bytes |

Folders are traversed recursively when `GOOGLE_DRIVE_ROOT_FOLDER_ID` is set. Without a root folder,
the adapter lists every file visible to the configured identity. Shared-drive items are included.
Folders, shortcuts, unsupported binary formats, empty documents, and files above the configured
byte limit are not indexed.

Each page is cached outside Temporal history before its result is returned. An ambiguous Activity
retry therefore returns the same compact document references rather than a newly observed page.
After a complete scan, the adapter compares the visible document IDs with its previous baseline.
Missing, trashed, moved-out, or newly unsupported files become durable tombstones and are submitted
for idempotent deletion on later scans too.

## Google Cloud setup

1. Enable the Google Drive API in the Google Cloud project used by the worker.
2. Choose one read identity:
   - Application Default Credentials on the worker;
   - a service-account JSON file mounted from a secret; or
   - a service account with domain-wide delegation and `GOOGLE_DRIVE_SUBJECT` set to the delegated
     Workspace user.
3. If you are not using domain-wide delegation, share the target folder or files with the service
   account.
4. Grant only the `https://www.googleapis.com/auth/drive.readonly` OAuth scope.

Never commit a service-account key. Prefer workload identity/Application Default Credentials when
the deployment platform supports it. If a key file is unavoidable, mount it read-only from the
platform secret manager.

## Worker configuration

Install both production extras when running outside the checked-in worker image:

```bash
uv sync --frozen --no-dev --extra lakebase --extra google-drive
```

Configure the worker alongside the ordinary Lakebase and Temporal variables:

```text
RETRIEVAL_ADAPTER_BUNDLE_FACTORY=retrieval.google_drive.bundle:create_adapter_bundle

GOOGLE_DRIVE_CREDENTIAL_KEY=workspace-primary
GOOGLE_DRIVE_USER_KEY=drive-user
GOOGLE_DRIVE_STAGING_DIRECTORY=/absolute/shared/retrieval-google-drive
GOOGLE_DRIVE_ROOT_FOLDER_ID=<optional-folder-id>
GOOGLE_DRIVE_CREDENTIALS_FILE=<optional-absolute-secret-path>
GOOGLE_DRIVE_SUBJECT=<optional-delegated-user@example.com>
GOOGLE_DRIVE_MAX_FILE_BYTES=10485760
GOOGLE_DRIVE_REQUEST_TIMEOUT=60
```

`GOOGLE_DRIVE_CREDENTIAL_KEY` and `GOOGLE_DRIVE_USER_KEY` are opaque identifiers, not credentials.
The credential key scopes the shared Temporal quota coordinator; use the same value for callers
that consume the same Google quota. The user key is the stable provider user identifier carried by
the workflow.

`GOOGLE_DRIVE_STAGING_DIRECTORY` is required and must be an absolute, durable path mounted at the
same location on every provider and retrieval worker replica. It contains immutable staged content,
page-result caches, traversal cursors, reconciliation baselines, and tombstones. Local ephemeral
storage is suitable only for single-worker development. Do not delete or mutate this directory
while workflows can still retry. Apply least-privilege filesystem permissions and include its
retention and backup policy in the deployment runbook.

The bundled worker image installs the `google-drive` extra. It does not embed credentials or choose
a staging volume.

## Submit a Drive sync

Drive syncs use the normal `RetrievalClient`. Add this metadata to `SyncCommand` so provider calls
share the correct quota workflow:

```python
config = GoogleDriveConfig.from_env()
command = SyncCommand(
    command_id="drive-sync-command-2026-07-20",
    store_key="team-drive",
    expected_generation=0,
    sync_sequence="drive-sync-2026-07-20",
    metadata=config.sync_metadata(page_size=100),
)
accepted = await retrieval.request_sync(command)
```

Create the Lakebase store before submitting its first sync, just as for any other provider. A store
controller permits only one active sync at a time, so reconciliation checkpoints for a store cannot
race each other.

## Failure and quota behavior

- Drive `401` responses fail as non-retryable invalid credentials after one forced token refresh.
- Drive `403` rate-limit reasons and `429` responses feed the shared quota workflow, including
  `Retry-After` when present.
- Drive `5xx` responses remain retryable Temporal Activity failures.
- Invalid requests, unsupported resources, incomplete searches, and corrupt local checkpoints fail
  non-retryably instead of silently accepting partial data.
- A file that disappears between listing and download is omitted; reconciliation deletes its prior
  indexed version after the complete scan.
- Rejected Drive page tokens restart that folder page, matching Drive's documented token recovery
  behavior. Generation-fenced document writes remain idempotent if a page is observed twice.

This integration does not parse PDFs, Office binaries, images, or audio/video. Add a conversion
stage before `GoogleDriveStagingStore` if those formats are required; the ingestion boundary must
still receive bounded UTF-8 content and verify its hash.

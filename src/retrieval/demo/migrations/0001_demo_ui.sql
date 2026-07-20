CREATE SCHEMA IF NOT EXISTS retrieval_demo_ui;

CREATE TABLE IF NOT EXISTS retrieval_demo_ui.schema_migrations (
    version integer PRIMARY KEY CHECK (version > 0),
    name text NOT NULL,
    checksum text NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    actor text NOT NULL DEFAULT current_user
);

CREATE TABLE IF NOT EXISTS retrieval_demo_ui.demo_runs (
    run_id uuid PRIMARY KEY,
    store_key text NOT NULL UNIQUE REFERENCES retrieval.stores (store_key) ON DELETE RESTRICT,
    display_name text NOT NULL,
    baseline_generation bigint NOT NULL CHECK (baseline_generation >= 0),
    status text NOT NULL CHECK (
        status IN ('ready', 'syncing', 'deactivating', 'completed', 'failed')
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz
);

CREATE INDEX IF NOT EXISTS demo_runs_created_at_idx
    ON retrieval_demo_ui.demo_runs (created_at DESC);

CREATE TABLE IF NOT EXISTS retrieval_demo_ui.demo_controls (
    run_id uuid PRIMARY KEY REFERENCES retrieval_demo_ui.demo_runs (run_id) ON DELETE CASCADE,
    quota_once_pending boolean NOT NULL DEFAULT true,
    quota_retry_after_seconds double precision NOT NULL
        CHECK (quota_retry_after_seconds > 0),
    held_document_key text NOT NULL,
    hold_before_commit boolean NOT NULL DEFAULT true,
    release_requested boolean NOT NULL DEFAULT false,
    control_version bigint NOT NULL DEFAULT 0 CHECK (control_version >= 0),
    quota_wait_request_id text,
    quota_wait_operation text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS retrieval_demo_ui.demo_events (
    event_id bigserial PRIMARY KEY,
    event_key text NOT NULL,
    run_id uuid NOT NULL REFERENCES retrieval_demo_ui.demo_runs (run_id) ON DELETE CASCADE,
    store_key text NOT NULL,
    event_type text NOT NULL CHECK (
        event_type IN (
            'run_created',
            'quota_injected',
            'quota_wait_started',
            'quota_wait_completed',
            'document_commit_held',
            'document_committed',
            'deactivation_fenced',
            'held_commit_released',
            'stale_generation_rejected',
            'cleanup_batch_completed',
            'store_inactive'
        )
    ),
    operation_id text,
    workflow_id text,
    document_key text,
    expected_generation bigint,
    actual_generation bigint,
    details jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (pg_column_size(details) <= 4096),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (run_id, event_key)
);

CREATE INDEX IF NOT EXISTS demo_events_timeline_idx
    ON retrieval_demo_ui.demo_events (run_id, event_id);

CREATE TABLE IF NOT EXISTS retrieval_demo_ui.demo_operations (
    operation_id text PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES retrieval_demo_ui.demo_runs (run_id) ON DELETE CASCADE,
    store_key text NOT NULL,
    operation_type text NOT NULL CHECK (
        operation_type IN ('create_run', 'sync', 'deactivation', 'hold', 'release', 'ask')
    ),
    status text NOT NULL CHECK (
        status IN ('accepted', 'running', 'completed', 'failed', 'canceled', 'rejected')
    ),
    command_id text NOT NULL UNIQUE,
    workflow_id text,
    lifecycle_generation bigint,
    result jsonb NOT NULL DEFAULT '{}'::jsonb,
    message text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS demo_operations_run_idx
    ON retrieval_demo_ui.demo_operations (run_id, created_at DESC);

CREATE TABLE IF NOT EXISTS retrieval_demo_ui.api_idempotency (
    scope text NOT NULL,
    idempotency_key_hash text NOT NULL CHECK (idempotency_key_hash ~ '^[0-9a-f]{64}$'),
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    status_code integer NOT NULL CHECK (status_code BETWEEN 100 AND 599),
    response jsonb NOT NULL,
    operation_id text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (scope, idempotency_key_hash)
);

CREATE OR REPLACE FUNCTION retrieval_demo_ui.create_northstar_run(
    p_run_id uuid,
    p_store_key text,
    p_display_name text,
    p_baseline_generation bigint,
    p_quota_retry_after_seconds double precision,
    p_held_document_key text,
    p_hold_before_commit boolean
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, retrieval, retrieval_demo_ui
AS $$
BEGIN
    IF p_display_name <> 'Northstar AI'
       OR p_baseline_generation <> 7
       OR p_quota_retry_after_seconds <> 5.0
       OR p_held_document_key <> 'late-security-review.md'
       OR p_hold_before_commit IS NOT TRUE
       OR p_store_key !~ '^northstar-[0-9a-f]{12}$' THEN
        RAISE EXCEPTION 'invalid fixed Northstar run seed';
    END IF;

    INSERT INTO retrieval.stores (
        store_key,
        display_name,
        lifecycle_state,
        lifecycle_generation
    ) VALUES (
        p_store_key,
        p_display_name,
        'active',
        p_baseline_generation
    ) ON CONFLICT (store_key) DO NOTHING;

    IF NOT EXISTS (
        SELECT 1
        FROM retrieval.stores AS stores
        WHERE stores.store_key = p_store_key
          AND stores.display_name = p_display_name
          AND stores.lifecycle_state = 'active'
          AND stores.lifecycle_generation = p_baseline_generation
    ) THEN
        RAISE EXCEPTION 'Northstar store key already exists with different attributes';
    END IF;

    INSERT INTO retrieval_demo_ui.demo_runs (
        run_id,
        store_key,
        display_name,
        baseline_generation,
        status
    ) VALUES (
        p_run_id,
        p_store_key,
        p_display_name,
        p_baseline_generation,
        'ready'
    ) ON CONFLICT (run_id) DO NOTHING;

    IF NOT EXISTS (
        SELECT 1
        FROM retrieval_demo_ui.demo_runs AS runs
        WHERE runs.run_id = p_run_id
          AND runs.store_key = p_store_key
          AND runs.display_name = p_display_name
          AND runs.baseline_generation = p_baseline_generation
    ) THEN
        RAISE EXCEPTION 'Northstar run ID already exists with different attributes';
    END IF;

    INSERT INTO retrieval_demo_ui.demo_controls (
        run_id,
        quota_once_pending,
        quota_retry_after_seconds,
        held_document_key,
        hold_before_commit,
        release_requested
    ) VALUES (
        p_run_id,
        true,
        p_quota_retry_after_seconds,
        p_held_document_key,
        p_hold_before_commit,
        false
    ) ON CONFLICT (run_id) DO NOTHING;

    INSERT INTO retrieval_demo_ui.demo_events (
        event_key,
        run_id,
        store_key,
        event_type,
        expected_generation,
        actual_generation,
        details
    ) VALUES (
        'run:created',
        p_run_id,
        p_store_key,
        'run_created',
        p_baseline_generation,
        p_baseline_generation,
        jsonb_build_object('display_name', p_display_name)
    ) ON CONFLICT (run_id, event_key) DO NOTHING;
END;
$$;

REVOKE ALL ON SCHEMA retrieval_demo_ui FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA retrieval_demo_ui FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA retrieval_demo_ui FROM PUBLIC;
REVOKE ALL ON FUNCTION retrieval_demo_ui.create_northstar_run(
    uuid, text, text, bigint, double precision, text, boolean
) FROM PUBLIC;

-- Deployment grants are intentionally identity-specific and are applied by the migration role:
--   App: USAGE on schema; SELECT on read models; UPDATE demo_controls; INSERT demo_events,
--        demo_operations, api_idempotency; USAGE on demo_events_event_id_seq;
--        EXECUTE create_northstar_run.
--   Worker: USAGE; SELECT demo_runs/demo_controls; UPDATE demo_controls; INSERT demo_events;
--           USAGE on demo_events_event_id_seq.

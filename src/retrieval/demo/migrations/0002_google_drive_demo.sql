CREATE OR REPLACE FUNCTION retrieval_demo_ui.create_demo_run(
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
    IF length(p_display_name) NOT BETWEEN 1 AND 120
       OR p_baseline_generation <> 7
       OR p_quota_retry_after_seconds <> 5.0
       OR p_held_document_key !~ '^(gdrive:[A-Za-z0-9_-]+|late-security-review[.]md)$'
       OR p_hold_before_commit IS NOT TRUE
       OR p_store_key !~ '^northstar-[0-9a-f]{12}$' THEN
        RAISE EXCEPTION 'invalid constrained demo run seed';
    END IF;

    INSERT INTO retrieval.stores (
        store_key, display_name, lifecycle_state, lifecycle_generation
    ) VALUES (
        p_store_key, p_display_name, 'active', p_baseline_generation
    ) ON CONFLICT (store_key) DO NOTHING;

    IF NOT EXISTS (
        SELECT 1 FROM retrieval.stores AS stores
        WHERE stores.store_key = p_store_key
          AND stores.display_name = p_display_name
          AND stores.lifecycle_state = 'active'
          AND stores.lifecycle_generation = p_baseline_generation
    ) THEN
        RAISE EXCEPTION 'demo store key already exists with different attributes';
    END IF;

    INSERT INTO retrieval_demo_ui.demo_runs (
        run_id, store_key, display_name, baseline_generation, status
    ) VALUES (
        p_run_id, p_store_key, p_display_name, p_baseline_generation, 'ready'
    ) ON CONFLICT (run_id) DO NOTHING;

    IF NOT EXISTS (
        SELECT 1 FROM retrieval_demo_ui.demo_runs AS runs
        WHERE runs.run_id = p_run_id
          AND runs.store_key = p_store_key
          AND runs.display_name = p_display_name
          AND runs.baseline_generation = p_baseline_generation
    ) THEN
        RAISE EXCEPTION 'demo run ID already exists with different attributes';
    END IF;

    INSERT INTO retrieval_demo_ui.demo_controls (
        run_id, quota_once_pending, quota_retry_after_seconds,
        held_document_key, hold_before_commit, release_requested
    ) VALUES (
        p_run_id, true, p_quota_retry_after_seconds,
        p_held_document_key, p_hold_before_commit, false
    ) ON CONFLICT (run_id) DO NOTHING;

    INSERT INTO retrieval_demo_ui.demo_events (
        event_key, run_id, store_key, event_type,
        expected_generation, actual_generation, details
    ) VALUES (
        'run:created', p_run_id, p_store_key, 'run_created',
        p_baseline_generation, p_baseline_generation,
        jsonb_build_object('display_name', p_display_name)
    ) ON CONFLICT (run_id, event_key) DO NOTHING;
END;
$$;

CREATE OR REPLACE FUNCTION retrieval_demo_ui.generation_proof(p_store_key text)
RETURNS TABLE (
    lifecycle_state text,
    lifecycle_generation bigint,
    physical_documents bigint,
    physical_chunks bigint,
    durable_write_receipts bigint,
    visible_documents bigint,
    visible_chunks bigint
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, retrieval, retrieval_demo_ui
AS $$
    SELECT
        stores.lifecycle_state,
        stores.lifecycle_generation,
        (SELECT count(*) FROM retrieval.documents AS documents
         WHERE documents.store_key = stores.store_key),
        (SELECT count(*) FROM retrieval.document_chunks AS chunks
         WHERE chunks.store_key = stores.store_key),
        (SELECT count(*) FROM retrieval.write_receipts AS receipts
         WHERE receipts.store_key = stores.store_key),
        (SELECT count(*) FROM retrieval.documents AS documents
         WHERE documents.store_key = stores.store_key
           AND stores.lifecycle_state IN ('active', 'syncing')
           AND documents.lifecycle_generation = stores.lifecycle_generation),
        (SELECT count(*) FROM retrieval.document_chunks AS chunks
         WHERE chunks.store_key = stores.store_key
           AND stores.lifecycle_state IN ('active', 'syncing')
           AND chunks.lifecycle_generation = stores.lifecycle_generation)
    FROM retrieval.stores AS stores
    JOIN retrieval_demo_ui.demo_runs AS runs ON runs.store_key = stores.store_key
    WHERE stores.store_key = p_store_key
$$;

REVOKE ALL ON FUNCTION retrieval_demo_ui.create_demo_run(
    uuid, text, text, bigint, double precision, text, boolean
) FROM PUBLIC;
REVOKE ALL ON FUNCTION retrieval_demo_ui.generation_proof(text) FROM PUBLIC;

CREATE TABLE IF NOT EXISTS retrieval_demo_ui.preflight_runs (
    workflow_id text PRIMARY KEY CHECK (length(workflow_id) BETWEEN 1 AND 300),
    request_id text NOT NULL UNIQUE CHECK (length(request_id) BETWEEN 1 AND 100),
    status text NOT NULL CHECK (
        status IN ('running', 'completed', 'failed', 'canceled', 'terminated', 'timed_out')
    ),
    result jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

REVOKE ALL ON TABLE retrieval_demo_ui.preflight_runs FROM PUBLIC;

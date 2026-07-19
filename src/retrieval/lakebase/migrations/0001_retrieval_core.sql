CREATE SCHEMA IF NOT EXISTS retrieval;

CREATE TABLE IF NOT EXISTS retrieval.schema_migrations (
    version integer PRIMARY KEY CHECK (version > 0),
    name text NOT NULL,
    checksum text NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    actor text NOT NULL DEFAULT current_user
);

CREATE TABLE IF NOT EXISTS retrieval.stores (
    store_key text PRIMARY KEY CHECK (length(store_key) > 0),
    display_name text NOT NULL CHECK (length(display_name) > 0),
    lifecycle_state text NOT NULL CHECK (
        lifecycle_state IN (
            'active',
            'syncing',
            'deactivating',
            'inactive',
            'deactivation_failed'
        )
    ),
    lifecycle_generation bigint NOT NULL CHECK (lifecycle_generation >= 0),
    last_lifecycle_transition timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS retrieval.store_users (
    store_key text NOT NULL REFERENCES retrieval.stores (store_key) ON DELETE RESTRICT,
    user_key text NOT NULL CHECK (length(user_key) > 0),
    active boolean NOT NULL DEFAULT true,
    lifecycle_generation bigint NOT NULL CHECK (lifecycle_generation >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (store_key, user_key)
);

CREATE INDEX IF NOT EXISTS store_users_active_generation_idx
    ON retrieval.store_users (store_key, lifecycle_generation)
    WHERE active;

CREATE TABLE IF NOT EXISTS retrieval.retrieval_state (
    store_key text NOT NULL REFERENCES retrieval.stores (store_key) ON DELETE RESTRICT,
    state_key text NOT NULL CHECK (length(state_key) > 0),
    state_value jsonb NOT NULL,
    lifecycle_generation bigint NOT NULL CHECK (lifecycle_generation >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (store_key, state_key)
);

CREATE TABLE IF NOT EXISTS retrieval.documents (
    store_key text NOT NULL REFERENCES retrieval.stores (store_key) ON DELETE RESTRICT,
    document_key text NOT NULL CHECK (length(document_key) > 0),
    source_version text NOT NULL,
    staging_uri text NOT NULL,
    content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    title text NOT NULL,
    source_uri text,
    body_hash text NOT NULL CHECK (body_hash ~ '^[0-9a-f]{64}$'),
    lifecycle_generation bigint NOT NULL CHECK (lifecycle_generation >= 0),
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (store_key, document_key)
);

CREATE INDEX IF NOT EXISTS documents_store_generation_idx
    ON retrieval.documents (store_key, lifecycle_generation);

CREATE TABLE IF NOT EXISTS retrieval.document_chunks (
    store_key text NOT NULL,
    document_key text NOT NULL,
    chunk_ordinal integer NOT NULL CHECK (chunk_ordinal >= 0),
    chunk_text text NOT NULL CHECK (length(chunk_text) > 0),
    chunk_hash text NOT NULL CHECK (chunk_hash ~ '^[0-9a-f]{64}$'),
    lifecycle_generation bigint NOT NULL CHECK (lifecycle_generation >= 0),
    PRIMARY KEY (store_key, document_key, chunk_ordinal),
    CONSTRAINT document_chunks_document_fk
        FOREIGN KEY (store_key, document_key)
        REFERENCES retrieval.documents (store_key, document_key)
        ON DELETE CASCADE,
    CONSTRAINT document_chunks_content_unique
        UNIQUE (store_key, document_key, chunk_hash)
);

CREATE INDEX IF NOT EXISTS document_chunks_store_generation_idx
    ON retrieval.document_chunks (store_key, lifecycle_generation);

CREATE TABLE IF NOT EXISTS retrieval.write_receipts (
    store_key text NOT NULL REFERENCES retrieval.stores (store_key) ON DELETE RESTRICT,
    idempotency_key text NOT NULL CHECK (length(idempotency_key) > 0),
    operation_type text NOT NULL CHECK (
        operation_type IN ('upsert_document', 'delete_document')
    ),
    document_key text NOT NULL,
    lifecycle_generation bigint NOT NULL CHECK (lifecycle_generation >= 0),
    payload_hash text NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (store_key, idempotency_key)
);

CREATE INDEX IF NOT EXISTS write_receipts_created_at_idx
    ON retrieval.write_receipts (created_at);

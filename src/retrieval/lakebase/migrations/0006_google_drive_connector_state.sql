CREATE SCHEMA IF NOT EXISTS retrieval_connector;

CREATE TABLE IF NOT EXISTS retrieval_connector.staged_content (
    content_hash text PRIMARY KEY CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    body bytea NOT NULL,
    staged_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS retrieval_connector.checkpoints (
    checkpoint_key text PRIMARY KEY CHECK (length(checkpoint_key) > 0),
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

REVOKE ALL ON SCHEMA retrieval_connector FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA retrieval_connector FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA retrieval_connector FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA retrieval_connector FROM PUBLIC;

ALTER DEFAULT PRIVILEGES IN SCHEMA retrieval_connector
    REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA retrieval_connector
    REVOKE ALL ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA retrieval_connector
    REVOKE ALL ON FUNCTIONS FROM PUBLIC;

ALTER TABLE retrieval.document_chunks
    ADD COLUMN IF NOT EXISTS search_vector tsvector
    GENERATED ALWAYS AS (to_tsvector('english', coalesce(chunk_text, ''))) STORED;

CREATE INDEX IF NOT EXISTS document_chunks_search_vector_idx
    ON retrieval.document_chunks USING gin (search_vector);

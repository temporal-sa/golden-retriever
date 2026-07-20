-- Lakebase Search is an intentional deployment prerequisite for this demo.
-- The migration fails loudly if the project has not enabled the Beta feature.
CREATE EXTENSION IF NOT EXISTS lakebase_vector CASCADE;
CREATE EXTENSION IF NOT EXISTS lakebase_text;

ALTER TABLE retrieval.document_chunks
    ADD COLUMN IF NOT EXISTS embedding VECTOR(1024),
    ADD COLUMN IF NOT EXISTS embedding_model text;

CREATE INDEX IF NOT EXISTS document_chunks_embedding_ann_idx
    ON retrieval.document_chunks
    USING lakebase_ann (embedding vector_cosine_ops);

CREATE INDEX IF NOT EXISTS document_chunks_search_bm25_idx
    ON retrieval.document_chunks
    USING lakebase_bm25 (search_vector);

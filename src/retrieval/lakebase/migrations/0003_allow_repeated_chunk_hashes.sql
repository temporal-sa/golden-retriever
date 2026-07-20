-- Chunk position is the row identity. Repeated text is valid within one
-- document, so equal content hashes must not collide at different ordinals.
ALTER TABLE retrieval.document_chunks
    DROP CONSTRAINT IF EXISTS document_chunks_content_unique;

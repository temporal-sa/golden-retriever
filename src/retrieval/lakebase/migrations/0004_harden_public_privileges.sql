-- Explicitly harden both pre-existing and freshly created core objects. This
-- avoids inheriting permissive PUBLIC grants from a reused schema or owner
-- whose default privileges were customized before these migrations ran.
REVOKE ALL ON SCHEMA retrieval FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA retrieval FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA retrieval FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA retrieval FROM PUBLIC;

ALTER DEFAULT PRIVILEGES IN SCHEMA retrieval
    REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA retrieval
    REVOKE ALL ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA retrieval
    REVOKE ALL ON FUNCTIONS FROM PUBLIC;

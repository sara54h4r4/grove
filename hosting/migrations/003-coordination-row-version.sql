-- Retain the old snapshot as an archive. Only storage_version=2 is served by
-- the new backend; normal operations never SELECT its snapshot column.
ALTER TABLE grove_coordination
    ADD COLUMN IF NOT EXISTS storage_version INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS event_seq BIGINT NOT NULL DEFAULT 0;

-- Heartbeat clocks and cursor movement do not change the heavy context.
-- Every other changed row advances this version atomically with the CAS.
ALTER TABLE grove_coordination
    ADD COLUMN IF NOT EXISTS context_revision BIGINT NOT NULL DEFAULT 0;

-- One authoritative cloud state per repository. revision is a compare-and-swap
-- fence: a lost concurrent update is retried before returning any result.
-- The versioned snapshot is evaluated by Grove's existing SQLite state machine;
-- serverless memory/filesystem are never a durable state owner.
CREATE TABLE IF NOT EXISTS grove_coordination (
    repository_id TEXT PRIMARY KEY,
    revision BIGINT NOT NULL DEFAULT 0,
    snapshot JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

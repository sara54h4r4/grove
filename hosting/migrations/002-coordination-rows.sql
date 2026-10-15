-- Durable, indexed rows replace per-request whole-history snapshot transfer.
-- data preserves the exact SQLite column order; no second command engine.
CREATE TABLE IF NOT EXISTS grove_coordination_rows (
    repository_id TEXT NOT NULL REFERENCES grove_coordination(repository_id),
    table_name TEXT NOT NULL CHECK (table_name IN (
        'coord_meta', 'coord_sessions', 'coord_messages', 'coord_decisions',
        'coord_decision_acks', 'coord_claims', 'coord_transfers', 'coord_events'
    )),
    row_key JSONB NOT NULL,
    data JSONB NOT NULL CHECK (jsonb_typeof(data) = 'array'),
    event_seq BIGINT GENERATED ALWAYS AS (
        CASE WHEN table_name = 'coord_events' THEN (data->>0)::bigint END
    ) STORED,
    recipient TEXT GENERATED ALWAYS AS (
        CASE table_name WHEN 'coord_events' THEN data->>3
                        WHEN 'coord_messages' THEN data->>2 END
    ) STORED,
    sender TEXT GENERATED ALWAYS AS (
        CASE WHEN table_name = 'coord_messages' THEN data->>1 END
    ) STORED,
    request_key TEXT GENERATED ALWAYS AS (
        CASE WHEN table_name = 'coord_messages' THEN data->>3 END
    ) STORED,
    acked_at BIGINT GENERATED ALWAYS AS (
        CASE WHEN table_name = 'coord_messages' THEN (data->>7)::bigint END
    ) STORED,
    created_at BIGINT GENERATED ALWAYS AS (
        CASE table_name WHEN 'coord_events' THEN (data->>6)::bigint
                        WHEN 'coord_messages' THEN (data->>5)::bigint END
    ) STORED,
    PRIMARY KEY (repository_id, table_name, row_key)
);

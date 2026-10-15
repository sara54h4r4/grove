-- Validate cached context against durable state and refresh all volatile
-- session fields in the same database snapshot. No cached authority is trusted.
SELECT g.revision, g.event_seq, g.context_revision, COALESCE((
    SELECT jsonb_agg(jsonb_build_array(
        r.data->0, (r.data->>1)::jsonb->'heartbeat_at', r.data->2, r.data->3
    ))
    FROM grove_coordination_rows r
    WHERE r.repository_id = g.repository_id AND r.table_name = 'coord_sessions'
), '[]'::jsonb)
FROM grove_coordination g WHERE g.repository_id = $1 AND g.storage_version = 2;

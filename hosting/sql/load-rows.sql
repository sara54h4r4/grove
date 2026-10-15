-- $1 repository ID, $2 typed command JSON. Every subquery shares one MVCC
-- snapshot, including the revision fence and global event high watermark.
WITH input AS (
    SELECT $2::jsonb AS command
), viewer AS MATERIALIZED (
    SELECT r.data
    FROM grove_coordination_rows r, input i
    WHERE r.repository_id = $1 AND r.table_name = 'coord_sessions'
      AND r.row_key = jsonb_build_array(i.command->>'session_id')
), selected_decisions AS MATERIALIZED (
    SELECT r.data
    FROM grove_coordination_rows r, input i
    WHERE r.repository_id = $1 AND r.table_name = 'coord_decisions'
      AND (
        (i.command->>'op' = 'sync' AND EXISTS (
            SELECT 1 FROM viewer v,
                jsonb_array_elements_text((r.data->>2)::jsonb) a(path),
                jsonb_array_elements_text((v.data->>1)::jsonb->'scope') b(path)
            WHERE a.path = '.' OR b.path = '.' OR a.path = b.path
               OR starts_with(a.path, b.path || '/') OR starts_with(b.path, a.path || '/')
        )) OR
        (i.command->>'op' IN ('propose','approve','ack_decision')
         AND r.data->>0 = i.command->>'decision_id')
      )
      AND (r.data->>6 = 'approved'
           OR r.data->>1 = i.command->>'revision'
           OR (r.data->>1)::bigint = (
               SELECT MAX((d.data->>1)::bigint)
               FROM grove_coordination_rows d
               WHERE d.repository_id = $1 AND d.table_name = 'coord_decisions'
                 AND d.data->>0 = r.data->>0
           ))
), event_window AS MATERIALIZED (
    SELECT r.data, r.event_seq,
        EXISTS (
            SELECT 1 FROM viewer v,
                jsonb_array_elements_text((r.data->>4)::jsonb) a(path),
                jsonb_array_elements_text((v.data->>1)::jsonb->'scope') b(path)
            WHERE a.path = '.' OR b.path = '.' OR a.path = b.path
               OR starts_with(a.path, b.path || '/') OR starts_with(b.path, a.path || '/')
        ) AS relevant
    FROM grove_coordination_rows r, input i
    WHERE i.command->>'op' = 'sync' AND EXISTS (SELECT 1 FROM viewer)
      AND r.repository_id = $1 AND r.table_name = 'coord_events'
      AND r.event_seq > COALESCE((i.command->>'after')::bigint,
                                 (SELECT (v.data->>2)::bigint FROM viewer v))
      AND (r.recipient IS NULL OR r.recipient = i.command->>'session_id'
           OR r.data->>2 = i.command->>'session_id')
    ORDER BY r.event_seq LIMIT 1001
), ranked_events AS (
    SELECT data, event_seq, relevant,
        SUM(relevant::integer) OVER (ORDER BY event_seq) AS relevant_rank
    FROM event_window
), projected_events AS (
    -- The engine scans at most 1000 headers and returns at most limit payloads.
    -- Unread payloads stay in Postgres. Existing events are immutable, so these
    -- placeholders are never persisted by the row-diff commit.
    SELECT event_seq,
        CASE WHEN relevant AND relevant_rank <= (i.command->>'limit')::bigint
             THEN data ELSE jsonb_set(data, '{5}', to_jsonb('null'::text)) END AS data
    FROM ranked_events, input i
)
SELECT g.revision, g.event_seq, g.context_revision, jsonb_build_object(
    'version', 1,
    'tables', jsonb_build_object(
        'coord_meta', jsonb_build_array(jsonb_build_array('repository_id', g.repository_id)),
        'coord_sessions', COALESCE((
            SELECT jsonb_agg(r.data ORDER BY r.row_key)
            FROM grove_coordination_rows r
            WHERE r.repository_id = g.repository_id AND r.table_name = 'coord_sessions'
              AND (i.command->>'op' IN ('register','sync','claim','capture_transfer',
                                       'get_transfer','check_transfer','review_transfer')
                   OR r.row_key = jsonb_build_array(i.command->>'session_id')
                   OR (i.command->>'op' = 'send' AND r.row_key = jsonb_build_array(i.command->>'to')))
        ), '[]'::jsonb),
        'coord_messages', COALESCE((
            SELECT jsonb_agg(m.data ORDER BY m.created_at, (m.data->>0) COLLATE "C")
            FROM (
                SELECT r.data, r.created_at, r.row_key
                FROM grove_coordination_rows r
                WHERE r.repository_id = g.repository_id AND r.table_name = 'coord_messages'
                  AND ((i.command->>'op' = 'sync' AND r.recipient = i.command->>'session_id' AND r.acked_at IS NULL)
                       OR (i.command->>'op' = 'send' AND r.sender = i.command->>'session_id' AND r.request_key = i.command->>'key')
                       OR (i.command->>'op' = 'ack_message' AND r.row_key = jsonb_build_array(i.command->>'message_id')))
                ORDER BY r.created_at, (r.data->>0) COLLATE "C" LIMIT 100
            ) m
        ), '[]'::jsonb),
        'coord_decisions', COALESCE((SELECT jsonb_agg(data) FROM selected_decisions), '[]'::jsonb),
        'coord_decision_acks', COALESCE((
            SELECT jsonb_agg(r.data)
            FROM grove_coordination_rows r
            WHERE r.repository_id = g.repository_id AND r.table_name = 'coord_decision_acks'
              AND r.data->>0 = i.command->>'session_id'
              AND ((i.command->>'op' = 'sync' AND EXISTS (
                    SELECT 1 FROM selected_decisions d
                    WHERE d.data->>0 = r.data->>1 AND d.data->>1 = r.data->>2 AND d.data->>6 = 'approved'))
                   OR (i.command->>'op' = 'ack_decision' AND r.data->>1 = i.command->>'decision_id'
                       AND r.data->>2 = i.command->>'revision'))
        ), '[]'::jsonb),
        'coord_claims', COALESCE((
            SELECT jsonb_agg(r.data ORDER BY r.row_key)
            FROM grove_coordination_rows r
            WHERE r.repository_id = g.repository_id AND r.table_name = 'coord_claims'
              AND (i.command->>'op' IN ('sync','claim')
                   OR (i.command->>'op' IN ('renew_claim','release_claim')
                       AND r.row_key = jsonb_build_array(i.command->>'claim_id')))
        ), '[]'::jsonb),
        'coord_transfers', COALESCE((
            SELECT jsonb_agg(t.data ORDER BY (t.data->>0) COLLATE "C")
            FROM (
                SELECT r.data, r.row_key FROM grove_coordination_rows r
                WHERE r.repository_id = g.repository_id AND r.table_name = 'coord_transfers'
                  AND (i.command->>'op' = 'sync'
                       OR (i.command->>'op' IN ('capture_transfer','get_transfer','check_transfer','review_transfer')
                           AND r.row_key = jsonb_build_array(i.command->>'transfer_id')))
                ORDER BY (r.data->>0) COLLATE "C" LIMIT 100
            ) t
        ), '[]'::jsonb),
        'coord_events', COALESCE((SELECT jsonb_agg(data ORDER BY event_seq) FROM projected_events), '[]'::jsonb)
    )
)
FROM grove_coordination g, input i
WHERE g.repository_id = $1 AND g.storage_version = 2;

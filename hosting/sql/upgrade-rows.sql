-- One atomic, database-side conversion. No historical payload crosses the
-- network during upgrade; the original snapshot remains recoverable.
WITH upgraded AS (
    UPDATE grove_coordination
    SET storage_version = 2,
        event_seq = COALESCE((
            SELECT MAX((e->>0)::bigint)
            FROM jsonb_array_elements(snapshot->'tables'->'coord_events') e
        ), 0)
    WHERE repository_id = $1 AND storage_version = 1
    RETURNING repository_id, snapshot
), records AS (
    SELECT repository_id, t.key AS table_name, e.data
    FROM upgraded
    CROSS JOIN LATERAL jsonb_each(COALESCE(snapshot->'tables',
        jsonb_build_object('coord_meta', jsonb_build_array(
            jsonb_build_array('repository_id', repository_id))))) t
    CROSS JOIN LATERAL jsonb_array_elements(t.value) e(data)
)
INSERT INTO grove_coordination_rows(repository_id, table_name, row_key, data)
SELECT repository_id, table_name,
    CASE table_name
        WHEN 'coord_decisions' THEN jsonb_build_array(data->0, data->1)
        WHEN 'coord_decision_acks' THEN jsonb_build_array(data->0, data->1, data->2)
        ELSE jsonb_build_array(data->0)
    END,
    data
FROM records;

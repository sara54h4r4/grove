-- Import is refused once any non-metadata state exists. It never clears or
-- overwrites a working repository. The archive remains available in snapshot.
WITH permission AS MATERIALIZED (
    SELECT set_config('grove.snapshot_import', 'on', true)
), accepted AS (
    UPDATE grove_coordination g
    SET revision = GREATEST(revision + 1, $4::bigint + 1),
        event_seq = $3::bigint, snapshot = $2::jsonb,
        context_revision = context_revision + 1, updated_at = now()
    FROM permission
    WHERE repository_id = $1 AND storage_version = 2 AND revision = 0 AND event_seq = 0
      AND NOT EXISTS (SELECT 1 FROM grove_coordination_rows r
                      WHERE r.repository_id = g.repository_id AND r.table_name <> 'coord_meta')
    RETURNING repository_id, revision
), records AS (
    SELECT a.repository_id, t.key AS table_name, e.data
    FROM accepted a, jsonb_each(($2::jsonb)->'tables') t,
         jsonb_array_elements(t.value) e(data)
), written AS (
    INSERT INTO grove_coordination_rows(repository_id, table_name, row_key, data)
    SELECT repository_id, table_name,
        CASE table_name
            WHEN 'coord_decisions' THEN jsonb_build_array(data->0, data->1)
            WHEN 'coord_decision_acks' THEN jsonb_build_array(data->0, data->1, data->2)
            ELSE jsonb_build_array(data->0)
        END, data
    FROM records
    ON CONFLICT (repository_id, table_name, row_key) DO UPDATE SET data = EXCLUDED.data
)
SELECT revision FROM accepted;

-- The fence, all changed rows, and appended events commit in one statement.
-- A losing CAS writes nothing and returns no response to the caller.
WITH accepted AS (
    UPDATE grove_coordination
    SET revision = revision + 1, event_seq = $4::bigint,
        context_revision = context_revision + CASE WHEN $5::boolean THEN 1 ELSE 0 END,
        updated_at = now()
    WHERE repository_id = $1 AND revision = $2::bigint AND storage_version = 2
    RETURNING repository_id, revision, context_revision
), written AS (
    INSERT INTO grove_coordination_rows(repository_id, table_name, row_key, data)
    SELECT a.repository_id, p.table_name, p.row_key, p.data
    FROM accepted a, jsonb_to_recordset($3::jsonb) p(table_name text, row_key jsonb, data jsonb)
    ON CONFLICT (repository_id, table_name, row_key) DO UPDATE SET data = EXCLUDED.data
)
SELECT revision, context_revision FROM accepted;

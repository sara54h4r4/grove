-- Old deployments always UPDATE snapshot, even for reads. Reject those writes
-- after row-store activation so they cannot acknowledge stale, divergent state.
CREATE OR REPLACE FUNCTION grove_coordination_protect_archive() RETURNS trigger
LANGUAGE plpgsql AS $function$
BEGIN
    IF OLD.storage_version = 2
       AND current_setting('grove.snapshot_import', true) IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION 'legacy coordination writer fenced; use the current service';
    END IF;
    RETURN NEW;
END
$function$;

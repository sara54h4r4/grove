#!/usr/bin/env python3
"""Preserve a legacy Grove snapshot while moving to an empty indexed store.

Inputs are explicit private env/archive paths. Output is metadata only. Freeze
fences legacy writes before the final import; no history is deleted or trimmed.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
from urllib.parse import urlparse
from urllib.request import Request, urlopen

TABLES = ["coord_meta", "coord_sessions", "coord_messages", "coord_decisions",
          "coord_decision_acks", "coord_claims", "coord_transfers", "coord_events"]
LIMIT = 18 * 1024 * 1024


def variables(path):
    values = {}
    for line in Path(path).read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, raw = line.split("=", 1)
        parsed = shlex.split(raw)
        if parsed and parsed[0] != "[SENSITIVE]":
            values[key] = parsed[0]
    return values


def query(connection, sql, params):
    parsed = urlparse(connection)
    if parsed.scheme not in ("postgres", "postgresql") or not parsed.hostname.endswith(".neon.tech"):
        raise RuntimeError("Expected the explicitly selected Neon connection")
    endpoint = "https://api." + parsed.hostname.split(".", 1)[1] + "/sql"
    request = Request(endpoint, data=json.dumps({"query": sql, "params": params}).encode(),
                      headers={"Content-Type": "application/json", "Neon-Connection-String": connection,
                               "Neon-Raw-Text-Output": "true", "Neon-Array-Mode": "true"})
    with urlopen(request, timeout=30) as response:
        raw = response.read(LIMIT + 1)
    if len(raw) > LIMIT:
        raise RuntimeError("Migration SQL response exceeds traffic bound")
    return json.loads(raw), len(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["export", "freeze", "verify", "import"])
    parser.add_argument("--source-env", required=True)
    parser.add_argument("--target-env", required=True)
    parser.add_argument("--client-env", required=True)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--source-project", required=True)
    parser.add_argument("--target-project", required=True)
    parser.add_argument("--binary", default="target/debug/coord")
    args = parser.parse_args()
    source, target, client = [variables(path) for path in
                              (args.source_env, args.target_env, args.client_env)]
    if source.get("NEON_PROJECT_ID") != args.source_project:
        raise RuntimeError("Source project does not match the selected env file")
    if target.get("GROVE_COORD_NEON_PROJECT_ID") != args.target_project:
        raise RuntimeError("Target project does not match the selected env file")
    if args.source_project == args.target_project:
        raise RuntimeError("Source and destination projects must differ")
    source_connection = source["DATABASE_URL_UNPOOLED"]
    target_connection = target["GROVE_COORD_DATABASE_URL_UNPOOLED"]
    repository = client["GROVE_REPOSITORY_ID"]
    archive_path = Path(args.archive)
    if args.action == "export":
        result, size = query(source_connection,
                             "SELECT revision,snapshot FROM grove_coordination WHERE repository_id=$1",
                             [repository])
        row = result["rows"][0]
        snapshot = json.loads(row[1]) if isinstance(row[1], str) else row[1]
        if snapshot["version"] != 1 or set(snapshot["tables"]) != set(TABLES):
            raise RuntimeError("Unsupported source snapshot")
        if ["repository_id", repository] not in snapshot["tables"]["coord_meta"]:
            raise RuntimeError("Source repository identity mismatch")
        archive = {"repository_id": repository, "revision": int(row[0]), "snapshot": snapshot}
        archive_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(archive_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w") as output:
            json.dump(archive, output)
            output.write("\n")
        print(json.dumps({"exported_revision": archive["revision"], "sql_response_bytes": size,
                          "archive": str(archive_path),
                          "rows": {name: len(rows) for name, rows in snapshot["tables"].items()}}))
        return
    archive = json.loads(archive_path.read_text())
    if archive["repository_id"] != repository:
        raise RuntimeError("Archive repository mismatch")
    if args.action == "freeze":
        root = Path(__file__).resolve().parents[1]
        for name in ("003-coordination-row-version.sql", "004-fence-legacy-writes.sql"):
            query(source_connection, (root / "hosting/migrations" / name).read_text(), [])
        query(source_connection, "CREATE OR REPLACE TRIGGER grove_coordination_legacy_fence BEFORE UPDATE OF snapshot ON grove_coordination FOR EACH ROW EXECUTE FUNCTION grove_coordination_protect_archive()", [])
        frozen, _ = query(source_connection,
                          "UPDATE grove_coordination SET storage_version=2 WHERE repository_id=$1 AND revision=$2::bigint RETURNING revision",
                          [repository, str(archive["revision"])])
        if not frozen["rows"]:
            raise RuntimeError("Source changed since export; export a fresh archive before freezing")
        print(json.dumps({"source_fenced": True, "revision": archive["revision"]}))
    elif args.action == "import":
        source_state, _ = query(source_connection,
                                "SELECT revision,storage_version FROM grove_coordination WHERE repository_id=$1",
                                [repository])
        if [int(v) for v in source_state["rows"][0]] != [archive["revision"], 2]:
            raise RuntimeError("Source is not fenced at the exported revision")
        env = os.environ.copy()
        env.update(target)
        env.update({key: client[key] for key in ("GROVE_COORD_URL", "GROVE_REPOSITORY_ID",
                                                "GROVE_COORD_TOKEN", "GROVE_VERCEL_PROTECTION_BYPASS")})
        subprocess.run([args.binary, "--import-snapshot", str(archive_path)], env=env, check=True)
    elif args.action == "verify":
        # Hash the same PostgreSQL JSONB representation on each side. Only
        # eight counts/digests cross the network, never another full history.
        names = ",".join("('" + table + "')" for table in TABLES)
        old_sql = "SELECT t.name,count(e.data),md5(COALESCE(string_agg(e.data::text,'|' ORDER BY e.data::text COLLATE \"C\"),'')) FROM grove_coordination g CROSS JOIN (VALUES " + names + ") t(name) LEFT JOIN LATERAL jsonb_array_elements(g.snapshot->'tables'->t.name) e(data) ON true WHERE g.repository_id=$1 GROUP BY t.name ORDER BY t.name"
        new_sql = "SELECT t.name,count(r.data),md5(COALESCE(string_agg(r.data::text,'|' ORDER BY r.data::text COLLATE \"C\"),'')) FROM (VALUES " + names + ") t(name) LEFT JOIN grove_coordination_rows r ON r.repository_id=$1 AND r.table_name=t.name GROUP BY t.name ORDER BY t.name"
        old, old_bytes = query(source_connection, old_sql, [repository])
        new, new_bytes = query(target_connection, new_sql, [repository])
        if old["rows"] != new["rows"]:
            raise RuntimeError("Source/destination row counts or content digests differ")
        print(json.dumps({"all_eight_tables_match": True, "rows_and_digests": new["rows"],
                          "verification_sql_bytes": old_bytes + new_bytes}))


if __name__ == "__main__":
    main()

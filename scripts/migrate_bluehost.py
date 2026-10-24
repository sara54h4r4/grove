#!/usr/bin/env python3
"""Move a fenced Grove row store to an empty Bluehost store. Metadata-only output.

Credentials come from explicit private env files. Export is private JSONL, never
an HTTP download or a source artifact. An incomplete import stays unavailable.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from migrate_coordination import TABLES, query, variables


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def fingerprints(records):
    tables = {table: {} for table in TABLES}
    for record in records:
        columns = 2 if record["table_name"] == "coord_decisions" else 3 if record["table_name"] == "coord_decision_acks" else 1
        if (not isinstance(record["data"], list) or len(record["data"]) < columns
                or record["row_key"] != record["data"][:columns]):
            raise RuntimeError("archive row key does not match its data")
        key = hashlib.sha256(encode(record["row_key"])).hexdigest()
        table = tables[record["table_name"]]
        if key in table:
            raise RuntimeError("archive contains a duplicate row key")
        table[key] = hashlib.sha256(encode(record["data"])).hexdigest()
    return {name: {"rows": len(rows), "sha256": hashlib.sha256(
                "".join(rows[key] for key in sorted(rows)).encode()).hexdigest()}
            for name, rows in tables.items()}


def store(values, repository, action, **params):
    url = urlparse(values["GROVE_STORE_URL"])
    if (url.scheme != "https" or not url.hostname or url.username or url.password or url.query or url.fragment):
        raise RuntimeError("target store must use HTTPS without embedded credentials")
    body = encode(dict(action=action, repository_id=repository, **params))
    if len(body) > 4_194_304:
        raise RuntimeError("migration request exceeds page bound")
    request = Request(url.geturl(), data=body, headers={"Content-Type": "application/json",
                      "User-Agent": "Grove-Coordination/0.4.2",
                      "X-Grove-Store-Token": values["GROVE_STORE_TOKEN"]})
    try:
        with urlopen(request, timeout=30) as response:
            raw = response.read(17 * 1024 * 1024 + 1)
    except HTTPError as error:
        raise RuntimeError(f"destination rejected {action} (HTTP {error.code}); no response body logged") from None
    if len(raw) > 17 * 1024 * 1024:
        raise RuntimeError("store response exceeds migration size bound")
    result = json.loads(raw)
    if result.get("ok") is not True:
        raise RuntimeError("destination operation failed; no response body logged")
    return result["rows"]


def archive_records(path):
    with Path(path).open() as source:
        next(source)
        for line in source:
            row = json.loads(line)
            if "fingerprints" in row:
                break
            yield row


def checked_archive(path, repository):
    with Path(path).open() as source:
        meta = json.loads(next(source))
        footer = None
        for line in source:
            row = json.loads(line)
            if footer is not None:
                raise RuntimeError("archive has records after its footer")
            if "fingerprints" in row:
                footer = row
    if meta.get("format") != "grove-rows-v1" or meta.get("repository_id") != repository:
        raise RuntimeError("archive format or repository mismatch")
    if footer is None or fingerprints(archive_records(path)) != footer["fingerprints"]:
        raise RuntimeError("archive content verification failed")
    return meta, footer["fingerprints"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["inspect", "freeze", "export", "import", "verify"])
    parser.add_argument("--source-env", required=True)
    parser.add_argument("--source-project", required=True)
    parser.add_argument("--client-env", required=True)
    parser.add_argument("--target-env", required=True)
    parser.add_argument("--archive", required=True)
    args = parser.parse_args()
    source, client, target = [variables(path) for path in
                              (args.source_env, args.client_env, args.target_env)]
    if source.get("GROVE_COORD_NEON_PROJECT_ID", source.get("NEON_PROJECT_ID")) != args.source_project:
        raise RuntimeError("source env does not match explicitly selected project")
    connection = source.get("GROVE_COORD_DATABASE_URL_UNPOOLED", source.get("DATABASE_URL_UNPOOLED"))
    if not connection:
        raise RuntimeError("selected source has no direct database connection")
    repository = client["GROVE_REPOSITORY_ID"]

    def source_state():
        result, _ = query(connection, "SELECT revision,event_seq,context_revision,storage_version FROM grove_coordination WHERE repository_id=$1", [repository])
        if len(result["rows"]) != 1:
            raise RuntimeError("source repository is not initialized")
        return list(map(int, result["rows"][0]))

    if args.action == "inspect":
        state = source_state()
        print(json.dumps({"source_project": args.source_project, "revision": state[0],
                          "event_watermark": state[1], "source_fenced": state[3] == 3}))
        return
    if args.action == "freeze":
        store(target, repository, "initialize")
        destination = store(target, repository, "fingerprint")[0]
        if destination[0] != 0 or destination[1] != 0 or any(
                (table != "coord_meta" and value["rows"]) or (table == "coord_meta" and value["rows"] != 1)
                for table, value in destination[2].items()):
            raise RuntimeError("freeze requires a verified empty destination")
        state = source_state()
        if state[3] == 2:
            result, _ = query(connection, "UPDATE grove_coordination SET storage_version=3 WHERE repository_id=$1 AND revision=$2::bigint AND storage_version=2 RETURNING revision", [repository, str(state[0])])
            if not result["rows"]:
                raise RuntimeError("source changed before fencing; retry freeze")
        elif state[3] != 3:
            raise RuntimeError("source is not a supported row store")
        print(json.dumps({"source_fenced": True, "revision": state[0]}))
        return
    if args.action == "export":
        state = source_state()
        if state[3] != 3:
            raise RuntimeError("fence source before exporting current rows")
        archive = Path(args.archive)
        archive.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(archive, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        meta = dict(format="grove-rows-v1", repository_id=repository, revision=state[0],
                    high=state[1], context=state[2], source_project=args.source_project)
        last = None
        count = 0
        with os.fdopen(descriptor, "wb") as output:
            output.write(encode(meta) + b"\n")
            while True:
                sql = "SELECT table_name,row_key,data FROM grove_coordination_rows WHERE repository_id=$1"
                params = [repository]
                if last is not None:
                    sql += " AND (table_name,row_key)>($2,$3::jsonb)"
                    params += [last[0], encode(last[1]).decode()]
                result, _ = query(connection, sql + " ORDER BY table_name,row_key LIMIT 32", params)
                if not result["rows"]:
                    break
                for table, key, data in result["rows"]:
                    key = json.loads(key) if isinstance(key, str) else key
                    data = json.loads(data) if isinstance(data, str) else data
                    output.write(encode(dict(table_name=table, row_key=key, data=data)) + b"\n")
                    last = (table, key)
                    count += 1
            output.flush()
            if source_state() != state:
                raise RuntimeError("source changed during export; archive remains incomplete")
            digest = fingerprints(archive_records(archive))
            output.write(encode({"fingerprints": digest}) + b"\n")
        print(json.dumps({"exported_rows": count, "revision": state[0], "private_archive": str(archive)}))
        return
    meta, expected = checked_archive(args.archive, repository)
    if meta["source_project"] != args.source_project:
        raise RuntimeError("archive source project mismatch")
    if args.action == "import":
        if source_state() != [meta["revision"], meta["high"], meta["context"], 3]:
            raise RuntimeError("source is not fenced at the exported state")
        store(target, repository, "import_begin", revision=meta["revision"], high=meta["high"], context=meta["context"])
        page, size = [], 0
        for row in archive_records(args.archive):
            next_size = len(encode(row))
            if page and (len(page) >= 64 or size + next_size > 1_048_576):
                store(target, repository, "import_rows", patch=page)
                page, size = [], 0
            page.append(row)
            size += next_size
        if page:
            store(target, repository, "import_rows", patch=page)
        store(target, repository, "import_finish", fingerprints=expected)
    actual = store(target, repository, "fingerprint")[0]
    if actual[0] != meta["revision"] + 1 or actual[1] != meta["high"] or actual[2] != expected:
        raise RuntimeError("destination content or watermark differs from archive")
    print(json.dumps({"all_eight_tables_match": True, "revision": actual[0], "event_watermark": actual[1]}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Controlled diagnostics only. HTTP response bodies and credentials are
        # never printed; archive rows stay in the explicit private file.
        if isinstance(error, RuntimeError):
            print(str(error), file=sys.stderr)
        else:
            print("migration failed; inspect selected private configuration", file=sys.stderr)
        sys.exit(1)

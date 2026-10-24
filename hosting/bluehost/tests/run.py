#!/usr/bin/env python3
"""Disposable PHP/MySQL integration tests. No hosting account or real credentials."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import uuid
import sys
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

ROOT = Path(__file__).resolve().parents[3]
IMAGE = "mariadb:11.4@sha256:250e1609e93c0e6aecf64bfbab2d84d5e89d2481540fe77447ff6c855c3b1873"
TOKEN = "local-fixture-only-not-a-real-credential-" * 2
REPO = "aaaaaaaa-1111-4444-8888-aaaaaaaaaaaa"


def main():
    env = os.environ.copy()
    for key in ("DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH"):
        env.pop(key, None)
    prefix = "grove-bluehost-test-" + uuid.uuid4().hex[:10]
    network, database, server = prefix + "-net", prefix + "-db", prefix + "-php"

    def docker(*args, check=True):
        result = subprocess.run(["docker", "--host=unix:///var/run/docker.sock", *args],
                                env=env, check=False, capture_output=True, text=True)
        if check and result.returncode:
            raise RuntimeError("local Docker fixture failed: " + result.stderr)
        return result

    docker("build", "-t", "grove-bluehost-fixture", "-f", "hosting/bluehost/tests/Dockerfile",
           "hosting/bluehost")
    with tempfile.TemporaryDirectory(prefix="grove-bluehost-") as work:
        work = Path(work)
        (work / "public_html/grove-store").mkdir(parents=True)
        shutil.copytree(ROOT / "hosting/bluehost/private", work / ".grove")
        config = {"dsn": f"mysql:host={database};dbname=grove_fixture;charset=utf8mb4",
                  "username": "grove", "password": "local-fixture-only",
                  "repository_id": REPO, "store_token": TOKEN, "retention_days": 7}
        config_path = work / ".grove/config.php"
        config_path.write_text("<?php return json_decode('" + json.dumps(config) + "', true);\n")
        config_path.chmod(0o600)
        try:
            docker("network", "create", network)
            docker("run", "-d", "--name", database, "--network", network,
                   "--tmpfs", "/var/lib/mysql:rw,size=512m",
                   "-e", "MARIADB_ROOT_PASSWORD=local-fixture-root-only",
                   "-e", "MARIADB_DATABASE=grove_fixture", "-e", "MARIADB_USER=grove",
                   "-e", "MARIADB_PASSWORD=local-fixture-only", IMAGE)
            for _ in range(200):
                if docker("exec", database, "healthcheck.sh", "--connect", "--innodb_initialized", check=False).returncode == 0:
                    break
                time.sleep(0.2)
            else:
                raise RuntimeError("local MariaDB fixture did not become ready")
            docker("run", "-d", "--name", server, "--network", network,
                   "-p", "127.0.0.1:45218:8080", "-e", "PHP_CLI_SERVER_WORKERS=8",
                   "-v", f"{work}:/fixture:ro",
                   "-v", f"{ROOT / 'hosting/bluehost/public'}:/fixture/public_html/grove-store:ro",
                   "-v", f"{ROOT / 'hosting/bluehost/tests'}:/tests:ro",
                   "grove-bluehost-fixture", "php", "-S", "0.0.0.0:8080", "-t", "/fixture/public_html")
            # Loopback fixture traffic never goes through the Internet proxy.
            opener = build_opener(ProxyHandler({}))
            url = "http://127.0.0.1:45218/grove-store/"
            for _ in range(100):
                try:
                    opener.open(url, timeout=1)
                except HTTPError as error:
                    assert error.code == 405
                    break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("local PHP fixture did not become ready")
            request = Request(url, data=b'{"dsn":"never-echo-this"}', headers={"Content-Type": "application/json"})
            try:
                opener.open(request, timeout=5)
                raise AssertionError("unauthenticated store accepted request")
            except HTTPError as error:
                assert error.code == 401
                assert b"never-echo-this" not in error.read()
            run_env = os.environ.copy()
            run_env.update(GROVE_TEST_STORE_ENDPOINT=url, GROVE_TEST_STORE_TOKEN=TOKEN,
                           GROVE_TEST_STORE_REPOSITORY=REPO)
            subprocess.run(["cargo", "test", "-p", "grove-cloud", "--lib", "--locked",
                            "bluehost_projection", "--", "--ignored", "--nocapture"],
                           cwd=ROOT, env=run_env, check=True)
            storage_test = docker("exec", server, "php", "/tests/storage.php", check=False)
            # The test file is supplied separately from the public directory.
            if storage_test.returncode:
                raise RuntimeError(storage_test.stdout + storage_test.stderr)
            print(storage_test.stdout)
            sys.path.insert(0, str(ROOT / "scripts"))
            from migrate_bluehost import checked_archive, encode, fingerprints

            def call(action, **params):
                request = Request(url, data=encode(dict(action=action, repository_id=REPO, **params)),
                                  headers={"Content-Type": "application/json", "X-Grove-Store-Token": TOKEN})
                with opener.open(request, timeout=10) as response:
                    return json.load(response)["rows"]

            current = call("fingerprint")[0]
            special = dict(table_name="coord_meta", row_key=["unicode-fixture"],
                           data=["unicode-fixture", "Grove / café \u2028 \u2029 \\ \" \n"])
            call("commit", revision=current[0], high=current[1], context=True, patch=[special])
            records, table, digest, revision = [], "", "", None
            while True:
                row = call("export", table=table, hash=digest, revision=revision)[0]
                revision = row[0]
                if not row[4]:
                    break
                records.extend(row[4])
                table, digest = row[5:7]
            actual = call("fingerprint")[0][2]
            assert fingerprints(records) == actual, "Python export and PHP import digests differ"
            archive = work / "fixture.jsonl"
            meta = dict(format="grove-rows-v1", repository_id=REPO, revision=revision,
                        high=row[1], context=row[2], source_project="fixture-only")
            archive.write_bytes(b"\n".join([encode(meta), *map(encode, records), encode({"fingerprints": actual})]) + b"\n")
            assert checked_archive(archive, REPO)[1] == actual
            archive.write_bytes(archive.read_bytes().replace(b"unicode-fixture", b"changed-fixture", 1))
            try:
                checked_archive(archive, REPO)
                raise AssertionError("corrupt archive passed validation")
            except RuntimeError:
                pass
            print("BLUEHOST_ARCHIVE_RECEIPT " + json.dumps({"all_eight_python_php_digests_match": True,
                                                           "corrupt_archive_rejected": True}))
        except Exception:
            print(docker("logs", "--tail", "60", database, check=False).stdout)
            print(docker("logs", "--tail", "60", database, check=False).stderr)
            print(docker("logs", "--tail", "40", server, check=False).stderr)
            raise
        finally:
            docker("rm", "-f", "-v", server, database, check=False)
            docker("network", "rm", network, check=False)


if __name__ == "__main__":
    main()

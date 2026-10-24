# Grove on Bluehost shared hosting

Grove can reuse a PHP/MySQL hosting plan. Vercel keeps the existing `/coord`
endpoint and tested Rust command engine. A private PHP endpoint owns indexed
rows in a **separate MySQL database and database user** on Bluehost. PHP and
MySQL run together; agent polling does not consume a Neon transfer allowance.
Bluehost's CPU, connection, storage and account bandwidth policies still apply.

The screenshot supplied for this setup shows **Starter Hosting**, cPanel,
File Manager, FTP and SSH key controls. It establishes the upload controls;
the actual PHP/MySQL versions and privileges still need a hosted check.
The implementation requires PHP **8.1+**, PDO MySQL, HTTPS, and InnoDB on
MySQL 5.7.7+/8 or MariaDB 10.4+. The automated fixture uses PHP 8.3 and
MariaDB 11.4; it is not evidence of the Bluehost account's configuration.

## Install through cPanel

1. Open **cPanel → MySQL Database Wizard** (sometimes **Manage My Databases**).
   Create a new database named `grove`, a new database user, and grant that
   user privileges **only on the new database**. Record the complete names,
   including the cPanel account prefix. Generate a strong password; letters
   and numbers avoid quoting mistakes when editing the PHP configuration.
2. Upload the Grove installation ZIP into the account's **Home directory**,
   one level above `public_html`, and extract it there. The layout must be:

   ```text
   /home/CPANELUSER/.grove/Store.php
   /home/CPANELUSER/.grove/schema.sql
   /home/CPANELUSER/.grove/config.php
   /home/CPANELUSER/public_html/grove-store/index.php
   /home/CPANELUSER/public_html/grove-store/.htaccess
   ```

   Enable **Show Hidden Files** in File Manager to see `.grove`. Keep private
   files outside every website document root. The endpoint refuses a private
   directory inside its own website root. For an addon domain with a different
   document root, adjust the single `$private` path in `index.php` to the actual
   private directory. The PHP process needs read access to that directory.
3. For a generic ZIP, copy `.grove/config.example.php` to `.grove/config.php`.
   Set the new database name, database username and database password there.
   Keep the existing Grove repository UUID. Generate a new **backend-only**
   store token with 32 random bytes (64 hex characters). A personalized private
   ZIP can supply the repository ID and store token in advance.

Do not place the ZIP, configuration, SQL dump, env file or private backup under
`public_html`. Remove the uploaded ZIP after extraction. There is no WordPress
plugin to activate and no existing website database to edit.

A GET to `https://YOUR_DOMAIN/grove-store/` should return JSON and HTTP 405.
That confirms routing only. An authenticated `initialize` followed by `load`
checks the real database connection, table permissions and transaction engine.
Credentials never go in a URL or chat message.

The server transport identifies itself as `Grove-Coordination/0.4.2`. During
preparation the existing website returned HTTP 200 for that identity and a
ModSecurity HTTP 406 page for generic automated requests. This homepage check
does not validate the new API; verify the authenticated API after installation.
Keep the site's security controls enabled while diagnosing any API rejection.

## Server configuration and cutover

These **Vercel backend** variables select the store:

```dotenv
GROVE_COORD_BACKEND=bluehost
GROVE_STORE_URL=https://YOUR_DOMAIN/grove-store/
GROVE_STORE_TOKEN=the-same-backend-token-as-the-private-PHP-config
```

`GROVE_STORE_TOKEN` and the MySQL password are sensitive credentials. Keep
the former in the private handoff/Vercel server configuration and the latter
only in Bluehost's private config. Agents never receive database credentials
or the backend store token. Their existing **four runtime Environment
variables in Codex Cloud stay unchanged**, including the two sensitive client
values; do not convert them to setup-only Secrets.

An operator performs the following only after the hosted store is reachable:

1. Build and review the exact Grove commit. Verify the selected Vercel team,
   project and current source database project. Initialize the **empty** target.
2. Use `scripts/migrate_bluehost.py freeze` to verify the empty destination and
   fence the source at `storage_version=3`. The old Vercel backend requires
   version 2 for reads/commits, so no in-flight write can escape the fence.
3. Use `export` to save current rows into a mode-0600 private JSONL archive.
   This exports the live row store, not the obsolete snapshot from October 15.
4. Use `import` and `verify`. The destination stays paused until all eight
   table counts and SHA-256 content digests match. Interrupted import pages
   are repeatable; an existing live destination cannot be overwritten.
5. Stage a Vercel production deployment with the three backend settings above
   and the existing client authentication settings. Verify Hello, a labelled
   disposable session, send/retry/sync/ACK and recovery. Promote that tested
   deployment to the existing alias. Keep the source fenced as a private archive.

The migration tool takes explicit `--source-env`, `--source-project`,
`--client-env`, `--target-env`, and `--archive` paths for every action. It prints
metadata only. The target env file contains `GROVE_STORE_URL` and
`GROVE_STORE_TOKEN`; the source file contains the selected project ID and
direct Neon connection. If an import response is lost, run `verify` before
deciding whether it needs resuming. Source fencing causes a maintenance gap;
retain queued work and the same deduplication keys throughout it.

After promotion, never point production back to the old archive: it will not
contain subsequent messages. A later migration needs a fresh, consistent backup
from the active store. Remove obsolete database credentials from new deployments
after verification; do not disable Vercel Deployment Protection.

## Retention, polling and safety

The default retention window is **seven days**. Hourly opportunistic maintenance
removes up to 256 old rows from each of events, acknowledged messages and expired
claims. Unread messages, session identities, decisions/ACKs and transfer records
are retained. Cleanup has a bounded backlog; seven days is an eligibility
threshold, not a promise that every eligible row disappears at that instant.
With no requests there is no maintenance work.

Removing an acknowledged message ends its deduplication record. Thus keyed
send retries remain protected while unread and for **at least seven days after
acknowledgement**; do not replay an ancient completed send as new work. Cleanup
preserves the global event watermark, so new events never reuse old cursors.
Sync includes `history_truncated` and `history_available_after` when a cursor
precedes removed history. The inbox and current decisions remain authoritative.

Use **one poller per session**, normally every **30 seconds**, backing off toward
60 seconds while idle. Brief five-second polling is appropriate for a bounded
active handoff. Honor `poll_after_seconds` on sync responses: drain an available
page promptly, then return to normal polling. Sync before significant work and
heartbeat after Git/task changes and at least once per minute while active.
Do not add a second MCP poller alongside a CLI wrapper. Keep the existing
60-second wrapper timeout and transient-error backoff.

An unchanged warm sync reads only the root revision and **changed** session
clocks through an index. Historical session clocks and acknowledged message
bodies stay on the host. Event replay scans at most 1,001 lightweight headers;
irrelevant event bodies stay beside MySQL. All changed rows and the event
watermark commit inside one InnoDB transaction behind a locked revision fence.
Clients receive no result from a discarded attempt. Failed maintenance cannot
turn an already accepted command into an error response.

Existing Grove 0.4.2 CLI and MCP clients can use the new store through the same
Vercel endpoint. **No reinstall is needed for this backend migration.** Update
any five-second polling wrapper separately; the response hint alone does not
change old wrappers. MCP restart is needed only if its local client is replaced.

## Local verification and packaging

```bash
python3 hosting/bluehost/tests/run.py
python3 scripts/package_bluehost.py --output /tmp/grove-bluehost.zip
```

The fixture starts disposable PHP and MariaDB containers on an isolated network,
binds HTTP to loopback, uses fixture credentials, and removes its containers,
network and temporary data. It requires Docker and the Rust toolchain. It tests
complete engine equivalence, same-key/distinct-key concurrency, clock deltas,
bounded event replay, retention, import checksums, loading/overwrite gates and
transaction rollback. No account, model call or production data is used.

The generic ZIP contains source, an example config and this guide. It never
includes real credentials, production coordination data or a completed config.

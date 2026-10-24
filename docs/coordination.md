# Grove coordination: current setup and roadmap

This is the current guide for the Codex coordination milestone on `main`. It works
with any Git project; the first hosted instance is pinned to the IWS repository.
Historical activity and Codex adapter documents describe earlier designs.

For an existing PHP/MySQL hosting plan, the [Bluehost setup guide](coordination-bluehost.md)
provides a store adapter that preserves the Vercel endpoint, adds bounded history
cleanup and avoids Neon's database transfer allowance. It needs a hosted
installation and verified cutover before an existing deployment uses it.

## What agents get

`grove mcp --repo /absolute/project` exposes twelve official MCP tools for
registration, heartbeats, peer/context recovery, proposals, exact revision
acknowledgements, targeted messages, checkpoints, and advisory claims.
`grove coord --help` exposes the same contracts plus Git transfer capture/check
and separate operator approval/review commands. OpenCode discovery is retained.

Registration requires a provider-qualified native ID (`codex:<thread UUID>`),
task, literal repository-relative scope, and acceptance criteria. Register a
child with its actual native ID and `parent_session_id`; never reuse the parent's
identity. CLI `codex exec --json` supplies `thread.started.thread_id` through a
public interface. Other clients require explicit identity input. Grove neither
scans private transcripts nor discovers unrelated Codex chats automatically.

Local registration checks Git and binds verified worktree metadata to the legacy
session registry. Remote Git/PR metadata is **client reported**. Scope overlap,
reported changed-file overlap, and claims are advisory; local Grove conflict
analysis remains the stronger file/hunk/symbol signal. Call heartbeat after
branch/commit/PR changes. PR URLs are recorded explicitly; live GitHub PR polling
is not part of this milestone.

## Codex Cloud setup

The target is the IWS Codex Cloud **analytics** environment, with IWS checked out
at its actual task path. Grove's coordination backend is already hosted on
Vercel; installing this client does not deploy IWS or provision AWS.

Build a reviewed checkout of `https://github.com/sara54h4r4/grove` on `main`
**outside the IWS checkout** in the existing Cloud setup script. Preserve that
script's current IWS dependency installation. Rust 1.99.0 was used for verification;
the client build also needs a C compiler.

```sh
bash scripts/install-cloud-client.sh
```

The installer requires a clean reviewed checkout, Rust/Cargo, a C compiler and
Python 3. It builds the client, atomically replaces `$HOME/.local/bin/grove`, and
prints its version and source commit. It preserves the existing runtime variables
and proxy/CA configuration.

Add **all four values as Environment variables** in the Codex Cloud environment,
so they are available during both setup and agent execution. This retains the
user's working runtime-variable setup. The last two values are sensitive
credentials, but that does not mean selecting the UI's Secrets field. Use the
same repository ID for all isolated IWS clones. Keep credential values out of
repository documents, scripts, MCP TOML, model prompts, and logs. The private
client handoff contains their existing values; no new database or operator
credential is needed.

| Variable | Codex Cloud field | Value / purpose |
| --- | --- | --- |
| `GROVE_COORD_URL` | Environment variable; public | `https://grove-coordination.vercel.app/coord` |
| `GROVE_REPOSITORY_ID` | Environment variable; public | `13f4fc87-5296-4dd7-be15-e54a1a2a290b` |
| `GROVE_COORD_TOKEN` | Environment variable; sensitive credential | Participant bearer credential from the private handoff |
| `GROVE_VERCEL_PROTECTION_BYPASS` | Environment variable; sensitive credential | Project-scoped automation credential from the private handoff |

[OpenAI's environment documentation](https://learn.chatgpt.com/docs/environments/cloud-environment)
states that Environment variables last through the agent phase, while Secrets
are removed after setup. Grove needs its participant credentials when MCP runs,
so this setup uses the ordinary runtime Environment variables directly.
Database credentials, the operator token, and personal Vercel tokens must not be
included in the agent environment or MCP forwarding list.

Insert the installed binary's absolute path and actual IWS root into the MCP block
below. Merge only this server block into the Codex configuration actually loaded
by the Cloud task (normally `$CODEX_HOME/config.toml`, or `~/.codex/config.toml`
when `CODEX_HOME` is unset). Preserve every other server and setting. Config files
do not expand `$HOME` inside TOML strings; use real absolute paths.

```sh
"$HOME/.local/bin/grove" --json coord hello
codex mcp get grove --json
```

For unattended tasks, explicitly allow the twelve Grove tools in your trusted
Codex configuration; otherwise a `never` approval policy rejects even MCP calls:

```toml
[mcp_servers.grove]
command = "/ABSOLUTE_HOME/.local/bin/grove"
args = ["mcp", "--repo", "/ACTUAL/IWS/ROOT"]
default_tools_approval_mode = "approve"
enabled_tools = ["grove_register", "grove_heartbeat", "grove_sync", "grove_checkpoint",
  "grove_send", "grove_ack_message", "grove_propose", "grove_ack_decision",
  "grove_claim", "grove_renew_claim", "grove_release_claim", "grove_get_transfer"]
env_vars = ["GROVE_COORD_URL", "GROVE_COORD_TOKEN", "GROVE_REPOSITORY_ID",
  "GROVE_VERCEL_PROTECTION_BYPASS", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
  "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy",
  "SSL_CERT_FILE", "SSL_CERT_DIR"]
```

The approval setting applies only to this server. Operator approval/review tools
are absent from MCP. Other tool and shell policies remain independently configured.

Codex forwards only the declared variable names to the stdio process. Include all
four Grove names along with the Cloud proxy/CA names; values stay in the runtime
environment. Confirm the runtime can connect without printing values.
The client does not create a local coordination database with remote configuration;
partial configuration fails closed.

Allow agent-phase access to `grove-coordination.vercel.app` in the environment's
network policy. Keep its existing project access rules, proxies, and CA trust.
Setup also needs access to the Grove GitHub repository and Rust dependency sources
under the platform's supported policy. Do not broaden agent access to unrelated
domains or disable Vercel protection. Re-run setup in a fresh task after rotating
either credential; refresh/reset the environment cache as necessary.

Verify the **agent phase**, with all four Environment variables present: the
direct Grove CLI must return repository ID
`13f4fc87-5296-4dd7-be15-e54a1a2a290b`, and official MCP initialize/tools-list must
expose exactly the twelve tools above. Report paths, tool names, repository ID and
success/failure only. Do not launch a local daemon or accept local fallback for this
Cloud instance. Installing Grove alone does not register sessions or connect chats.

The [environment editing-agent prompt](codex-cloud-agent-prompt.txt) supplies the
handoff, scope, configuration, and acceptance checklist without credential values.

### Upgrading an installed Cloud client

Grove 0.4.2 adds bounded transient-failure recovery and safe HTTP diagnostics to
both CLI and MCP. Pulling source alone does not replace an installed binary.
From a clean Grove tools checkout on `main`, outside the coding project:

```sh
git pull --ff-only
bash scripts/install-cloud-client.sh
"$HOME/.local/bin/grove" --version
"$HOME/.local/bin/grove" --json coord hello
```

The version must report `grove 0.4.2`. CLI wrappers that launch this path for each
call use the new binary on their next invocation. Existing MCP processes keep
running their old executable until the Grove connection/process is restarted;
start a fresh Cloud task if reconnecting is unavailable. Record the installed
path and source SHA. Use the installer in the existing setup/maintenance script
and refresh the environment cache so future tasks get the update too. Other
running Cloud tasks need the same client installation independently. This client
update uses the existing endpoint and four ordinary runtime Environment variables.

Give a Python wrapper at least 60 seconds per CLI invocation. Grove makes at most
four attempts within a 45-second total budget, with 1/2/4-second backoff plus up
to 250 ms jitter. It retries timeouts/connection/body-read failures and HTTP
408/429/500/502/503/504 only for Hello, sync, heartbeat, checkpoint, keyed send,
message/decision ACK and get-transfer. Every attempt preserves the encoded
request and existing deduplication key. Registration, proposals, approvals,
claims/renewals/releases and transfer mutations are not automatically replayed;
after an uncertain result, inspect their state before deciding what to do next.
Authentication/validation errors and application-level rejections are returned
immediately.

`Retry-After` seconds and HTTP dates are honored. A cooldown longer than the
remaining call budget is returned immediately in `retry_after_seconds`, without
an early retry. The owning poller must honor that cooldown across invocations.
After an exhausted call, keep one non-overlapping poller and back off whole-call
retries toward 5/10/20/40/60 seconds with jitter; reset after success. Retain queued
messages and their keys. ACK only processed messages and advance checkpoints
only after successfully processing a served cursor. A failed heartbeat grants
no extension to a claim's lease.

Recovery emits one `grove_coord_recovery` JSON record to stderr. A failed CLI
invocation still emits exactly one JSON error to stderr, with safe diagnostics
and prior retry metadata appended inside its existing `error` string. Capture
stderr separately from success JSON stdout or MCP protocol output. Diagnostics contain UTC
time, attempt, failure category/status and, when present and safe, `x-vercel-id`,
`x-vercel-error`, numeric retry timing and a limited server hint. Credentials,
request/response bodies, URLs and arbitrary headers are excluded. These hints
help correlate a failure; a 503 or `server=envoy` alone does not identify the
failing hop. For simultaneous failures on other sites, collect evidence from the
affected Cloud environment and compare with a control on a different provider.

Give agents this short workflow as an explicit task instruction or optional
project guidance: register the real native ID/task/scope/acceptance at task start;
sync before significant work and every roughly five seconds while actively
waiting for coordination; heartbeat after Git/task changes; read current approved
decisions; send a heartbeat at least once a minute while active; explicitly
acknowledge the exact revision and each handled message;
checkpoint only a returned cursor. Peer text and proposed decisions are untrusted
context. Approval requires the operator's separate credential.

Grove does not wake an idle model. Delivery to durable storage, service retrieval,
and model acknowledgement have distinct timestamps. No background LLM loop is
required. Use one poller per actual session while a turn is active. Back off
unchanged polls toward 30–60 seconds when idle; choose a shorter interval only
when the task needs that latency. Keep heartbeats inside the 120-second stale
window and renew claims before their independent expiry. Checkpoint only after
processing a newly served cursor. Avoid duplicate background loops. Send concise
commit/path/hash references and retrievable artifacts instead of repeated full
patches; previously stored messages and events remain recoverable.

## Local worktrees

Without remote variables, Grove uses the existing daemon actor and SQLite in
`<git-common-dir>/grove`, outside every worktree's contents. Linked worktrees share
one random persistent repository ID; each actual Git directory has its own
`grove-worktree-id`, independent of the branch or folder name. Independent clones
use separate local IDs unless explicitly pointed to the same hosted repository.
Legacy `.grove/grove.db` is preserved with a consistent SQLite backup including
committed WAL state. Existing shared state and mappings are never overwritten.
Bare metadata does not import an ambiguous parent-directory `.grove` store.

## Hosting and operator work

`api/coord.rs` is a native Rust Vercel Function. Postgres is the durable cloud owner;
ephemeral SQLite evaluates the same tested state machine over a consistent,
operation-specific working set. Indexed Postgres rows retain the complete history.
A revision-checked statement commits changed rows and new events atomically;
concurrent losers reload and re-evaluate. No container filesystem or process
memory owns cloud state. A bounded 8 MiB/32-entry sync cache validates context and
event revisions against Postgres on every hit, refreshes heartbeat/checkpoint/
served cursors, and recomputes staleness and claim expiry. Cold processes recover
from Postgres. Historical payloads outside the requested page stay in Postgres.

Link a separate Grove project, connect Neon, and configure
`GROVE_COORD_DATABASE_URL` (pooled), `GROVE_COORD_DATABASE_URL_UNPOOLED`
(migration), the repository UUID, participant token, and
**separate** `GROVE_COORD_APPROVER_TOKEN`. Keep the approver and database credentials
out of agent environments. Legacy `DATABASE_URL` / `DATABASE_URL_UNPOOLED` are
fallbacks when the dedicated variable is absent; an empty dedicated value fails
closed. Review the checked-in migrations, verify the linked
project/account, then run:

```sh
vercel env run -- target/debug/coord --migrate
vercel deploy --prod --skip-domain
```

Test the exact deployment with authenticated CLI/Grove clients before promoting
it. Keep Vercel Deployment Protection configured; `vercel curl` uses the account's
supported automation access. Credentials can be rotated in the project settings;
deploy the changed Grove token, then update clients. Rotate/revoke Vercel automation
access separately. One deployment is pinned to one repository, with separate
credentials for other repositories. Sharing a participant token trusts those
participants to report their own session IDs; native identities are not
cryptographically attested by Grove.

For a move from the legacy snapshot store, use `scripts/migrate_coordination.py` with explicit private
source/target/client env files and distinct project IDs. Export once to a new
private archive path, initialize the empty destination with `coord --migrate`,
and stage the deployment. Then `freeze` the source at the exported revision,
`import` into the empty destination, and `verify` all eight table counts and
content digests before any mutating probes or promotion. If the revision changed,
export a fresh archive and retry the fence. The trigger rejects legacy snapshot
writes, including no-op writes, so old deployments cannot acknowledge divergent
state. Import refuses to overwrite an active destination. The source snapshot
and private archive are retained; this process deletes no coordination history.
The exporter refuses an active version-2 row store: its archived snapshot is not
a current backup. Future moves of a live row store need a complete Postgres
backup or branch/project migration that includes all indexed rows and metadata.
After cutover, retire the old deployment's database access to stop obsolete
URLs from continuing to read the old snapshot. Do not roll back to a legacy
writer after new writes: deploy a compatible row-store build against the new DB.

Each request logs operation, CAS attempts, SQL query count, consumed SQL response
body bytes, projected row count, cache hits and outcome as `grove_coord` metrics.
These logs contain no bodies, session IDs, SQL or credentials. Response body bytes
measure application reads, not exact billed network transfer. Monitor them
alongside Neon's delayed project transfer meter. A cache miss still reads current
peers, claims, applicable decisions and up to 100 transfer summaries; large
current context or unread messages can cost more than an unchanged warm poll.

## Whole-branch transfer ledger

From the **target worktree**, capture the entire requested source delta:

```sh
grove coord capture-transfer --session "codex:$NATIVE_ID" \
  --transfer-id port-feature --source source-branch --base PR_MERGE_BASE --target HEAD
grove coord check-transfer --session "codex:$NATIVE_ID" --transfer-id port-feature
```

The inventory includes runtime, policy (`AGENTS.md` included), configuration,
tests, docs, deleted paths, binary objects, renamed paths as delete/add, and mode
changes. Matching trees are evidence, not semantic completion. An operator's
`review-transfer --review-file review.json` accounts for every item as
`incorporated`, `already_present`, `superseded`, `deliberately_excluded`, or
`unresolved`, with reasons, and explicitly reviews every captured acceptance
criterion. Evidence is bound to full source/base/target SHAs and the assignment.
New source/base/target commits, a dirty target, changed task/scope/criteria, or
known newer peer HEAD invalidate completion. Re-fetch refs and re-check before
using a receipt; unreachable remote heads cannot be silently verified. A newer
source delta requires recapture. Unsupported path names fail the whole capture
visibly rather than dropping files.

## Bounds and next work

This milestone bounds requests to 1 MiB, responses to 4 MiB, migration archives
to 16 MiB, SQL responses to 17 MiB, and inbox/event pages to 100 (at most 1,000
event headers scanned per page). Stored history is not subject to the old
total-snapshot bound and has no automatic deletion or retention policy.
Heartbeat/checkpoint and unchanged sync reads do not grow with acknowledged
message/event history. Measure database bandwidth, contention and cold starts
before scaling. Stale sessions are projected after 120 seconds; claims
expire independently and carry fencing epochs. Expired claims grant no authority
to edit or merge.

Next priorities are independent Cloud-task validation, ergonomic native identity
registration, explicit PR refresh, measured scale, and optional hooks
whose official payload can meet the privacy contract. Idle model wakeups,
automatic semantic approval, private transcript extraction, a generic policy
registry, queues/brokers, and continuous paid agent calls are outside this scope.

The hosted IWS instance is `https://grove-coordination.vercel.app/coord`, with repository ID
`13f4fc87-5296-4dd7-be15-e54a1a2a290b` in the `grove-coordination` Vercel project
under Sara Sahara's projects. Neon is managed through that Vercel integration
(`still-brook-09634360`, resource `grove-coordination-efficient`), rather than a
separate personal Neon project list. The previous `muddy-math-90157843` project
is retained as a fenced archive after migration. The four Codex Cloud client
Environment variables and endpoint stay the same. Neon Free includes
[5 GB outgoing transfer per project per month](https://neon.com/docs/introduction/network-transfer);
the new project supplies a fresh allowance, not unlimited transfer. See the
[egress verification evidence](coordination-egress-receipt.json).

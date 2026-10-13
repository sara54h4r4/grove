# Grove coordination: current setup and roadmap

This is the current guide for the `codex/iws-coordination` milestone. It works
with any Git project; the first hosted instance is pinned to the IWS repository.
Historical activity and Codex adapter documents describe earlier designs.

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

Build a reviewed checkout of this Grove branch in your Cloud setup script:

```sh
cargo build --workspace --locked
install -m 755 target/debug/grove /usr/local/bin/grove
```

Store these variables in the Codex Cloud environment, with credential values
marked secret. Use the same repository ID for all isolated clones of that
project. Keep them out of repository documents and model prompts.

| Variable | Purpose |
| --- | --- |
| `GROVE_COORD_URL` | Exact HTTPS endpoint ending in `/coord` |
| `GROVE_REPOSITORY_ID` | Service-pinned repository UUID |
| `GROVE_COORD_TOKEN` | Repository participant bearer credential |
| `GROVE_VERCEL_PROTECTION_BYPASS` | Project-scoped Vercel automation access, when protection applies |

Configure the official Codex MCP surface in the setup environment:

```sh
codex mcp add grove -- grove mcp --repo "$PWD"
grove --json coord hello
```

For unattended tasks, explicitly allow the twelve Grove tools in your trusted
Codex configuration; otherwise a `never` approval policy rejects even MCP calls:

```toml
[mcp_servers.grove]
command = "/usr/local/bin/grove"
args = ["mcp", "--repo", "/absolute/project"]
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

Codex forwards only the declared `env_vars` to the stdio client; these are names,
not credential values in configuration. Preserve the Cloud environment's proxy
and certificate variables so HTTPS follows its supported network policy. The
client does not create a local coordination
database when remote configuration is present. Partial remote configuration
fails closed. Set credentials as ordinary runtime environment variables if your
Cloud secrets are available only to setup; confirm the runtime can access them
without printing their values. Network policy must allow the endpoint. Installing
Grove in a Cloud image alone does not register sessions or connect separate chats.

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
required; agents choose their polling interval while a turn is active.

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
ephemeral SQLite evaluates the same tested state machine. A revision-checked
Postgres update commits the complete snapshot atomically; concurrent losers reload
and re-evaluate. No container filesystem or process memory owns cloud state.

Link a separate Grove project, connect Neon, and configure `DATABASE_URL` (pooled),
`DATABASE_URL_UNPOOLED` (migration), the repository UUID, participant token, and
**separate** `GROVE_COORD_APPROVER_TOKEN`. Keep the approver and database credentials
out of agent environments. Review the checked-in migration, verify the linked
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

This milestone bounds requests to 1 MiB, responses to 4 MiB, snapshots to 16 MiB,
and inbox/event pages to 100. History currently has no automatic retention; archive
or migrate deliberately before reaching the snapshot bound. Each request reads
and writes a snapshot, including context retrieval. Measure database bandwidth,
contention and cold starts before scaling; native Postgres tables and retention
are the next storage work. Stale sessions are projected after 120 seconds; claims
expire independently and carry fencing epochs. Expired claims grant no authority
to edit or merge.

Next priorities are independent Cloud-task validation, ergonomic native identity
registration, explicit PR refresh, measured retention/scale, and optional hooks
whose official payload can meet the privacy contract. Idle model wakeups,
automatic semantic approval, private transcript extraction, a generic policy
registry, queues/brokers, and continuous paid agent calls are outside this scope.

The hosted IWS instance is `https://grove-coordination.vercel.app/coord`, with repository ID
`13f4fc87-5296-4dd7-be15-e54a1a2a290b` in the `grove-coordination` Vercel project
under Sara Sahara's projects. Neon is managed through that Vercel integration
(`muddy-math-90157843`), rather than a separate personal Neon project list.

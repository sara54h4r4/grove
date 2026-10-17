# Source evaluation and verification

## Recovered baseline

The feature branch was recovered at `cc41e421bae51402eee1a6a42b3dd581c2a66f60`
(seven commits beyond main). This milestone starts from it and preserves its
history, Grove branding, MIT/Apache licenses, conflict engine, daemon actor,
provider-neutral registry, and OpenCode adapter. Before changes, the workspace
passed **891 tests with one ignored**. Codex integration was a design document;
it was not an implemented or verified adapter.

## Inspected alternatives

Implementation and tests were read before selection. No broad registry, policy
framework, sidecar broker, or extra daemon from these sources is integrated.

| Source / pinned revision | License | Evidence and decision |
| --- | --- | --- |
| [AI Crew Sync](https://github.com/joaquinbejar/ai-crew-sync) `8897d7a30a06edf000ba81f308d42e13384f3455` | MIT | Reviewed Rust MCP/Postgres/outboxes, delivery/ack and notes code. Session/window inbox ideas informed the design. Its Postgres/NATS integration suite was **not run**. No implementation copied. |
| [Agent Commons](https://github.com/t54-labs/agent-commons) `6e911236127dc6cf0d231add87e302974151a7f4` | Apache-2.0 | Reviewed stdlib SQLite messaging/claims and broad registries. Baseline core suite: **43 pass, one daemon-stop failure** (44 total). Used epoch fencing as a design reference; no code copied. |
| [Cooperative Worktree Agents](https://github.com/pedroantonnio/cooperative-worktree-agents) `04a8fe944000353ea6baa9715df303e2acac0a92` | MIT | Full workflow test script passed. Reused the small `common_git_dir`/`state_root` placement approach, rewritten in Rust, to share state outside worktree contents. Notice in `THIRD_PARTY_NOTICES.md`; modifications include consistent legacy WAL backup and no-clobber installation. No integration-command executor copied. |
| Agent Mail Rust `227544ff8f35b7b7640c53f65d293989d4912c7c` | MIT text plus OpenAI/Anthropic restrictive rider | License inspected only. Implementation excluded, neither read nor copied. |

New integration dependencies are the official Rust MCP SDK `rmcp 3.5.1`
(Apache-2.0), `schemars 1.2.3` (MIT), `axum 0.8.9` (MIT), and Vercel's native Rust
runtime `vercel_runtime 2.4.1` (Apache-2.0). The checked-in Cargo lock pins resolved
dependencies. Neon's official driver was inspected to verify its SQL-over-HTTP
endpoint/headers; no driver source was copied. New Grove code owns registration,
decisions, messages, claims and transfer receipts in one coordination engine.

## Reproducible checks

```sh
cargo build --workspace --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all --check
python3 scripts/coordination_acceptance.py --binary target/debug/grove
```

The black-box script uses disposable repos, worktrees, an independent clone,
real daemon/socket/HTTP processes and a real MCP client. It measures service
enqueue/read/ack separately; asserts targeted delivery, idempotency, restart
recovery beyond checkpoints, claim expiry/fencing, scope/approval behavior and
whole-delta completeness. A partial runtime port leaves `AGENTS.md`, configuration,
documentation and the runtime test unresolved. Its semantic review is a **fixture
operator**, not an independently assessed real implementation review. Loopback
HTTP from an isolated clone is not proof of separate Cloud environments.

The real-model demonstration is opt-in (ordinary model usage):

```sh
python3 scripts/codex_coordination_demo.py --repo /isolated/iws-clone \
  --out /outside-repo/grove-demo --binary /absolute/grove
```

It runs two independent `codex exec` processes using `gpt-6.1-sol`, obtains IDs
from public JSON events, registers through MCP, retrieves/acks the approved
fixture revision, handles a targeted request/handoff, and resumes the actual
recipient native session to recover a pending message. The operator approval
is synthetic. It preserves public events and a receipt; it never scans Codex's
private storage, edits IWS tracked files, merges, pushes, or deploys IWS. Two CLI
sessions within this environment do not establish separate managed Cloud tasks.

For IWS, `.ai/NAVIGATION.AGENTS.local.md` is absent in the isolated checkout;
navigation therefore fails closed to exact targeted reads. No broad repository
search, Next.js build, or paid generation API is part of this demonstration.

## Observed results (2026-10-13)

The final Rust workspace passed **915 tests, one existing ignored**. The changed
CLI suite also passed 141 tests after the legacy-root correction. Strict Clippy
for all targets, formatting, Git whitespace checks and Python compilation passed.
The final disposable-process acceptance receipt contains 32 assertions (readiness
poll checks vary with process startup), with no missed or duplicate message
effects. Committed WAL migration, ambiguous bare-root isolation, and literal
Next.js paths/renames/deletions/binary/mode evidence have dedicated tests.

Clean Linux CI passed the build, workspace tests, strict Clippy, formatting, and
disposable coordination acceptance at `823856feeb23c98ef7c01306ff9673842a6bfbc1`.
It first exposed an existing OpenCode test race: concurrent tests changed
process-wide environment variables. The follow-up preserves all nine
configuration tests and supplies independent values to the same production
parser, removing unsafe environment mutation. macOS then exposed a separate
fixture race: its listener exited after 500 ms, potentially before native TLS
client construction finished. That one-shot mock now remains available until
the request arrives or its guard cancels it; the lifecycle assertion is unchanged.
After these fixes, the focused OpenCode suite passed 29 tests with the same one
ignored live-server test; strict Clippy for all daemon targets and local formatting
also passed. Follow-up CI validation is tracked in the
[current PR checks](https://github.com/sara54h4r4/grove/pull/1/checks).

The production endpoint is <https://grove-coordination.vercel.app/coord>. Real
HTTPS from this Codex Cloud workspace reached native Vercel Rust Functions and
Neon Postgres. Missing participant credentials returned 401; the wrong repository
and missing operator role returned 403; malformed typed commands returned 400.
Eight concurrent retries committed one message. A fresh deployment recovered
the previous deployment's peers from Postgres. The instance uses Vercel's managed
Neon Free plan under the user's Vercel team. No IWS deployment or merge occurred.

Two actual `gpt-6.1-sol` CLI sessions, in separate isolated IWS worktrees at
`74a117e7bf76581b30a31f63a81f2e6cb2378c83`, registered their public native UUIDs,
discovered each other through the hosted endpoint, acknowledged exactly revision
1, exchanged and acknowledged a request/handoff, then resumed the recipient's
same native session to recover a pending message. The successful flow took 74.42
seconds. Request/handoff service-read delays were 1.742/1.394 seconds; model-ack
delays were 8.281/9.257 seconds. The recovery probe was served after 13.417 seconds
and acknowledged after 19.907 seconds. There were no missed or duplicate message
effects in this fixture. Tracked IWS files stayed clean.

Setup exposed an expired Codex CLI login, the difference between MCP approval
modes `auto` and `approve`, and Codex's explicit MCP environment allowlist.
The final configuration forwards Grove's four variable names plus proxy/CA
variables, and allows only twelve coordination tools. Operator and database
credentials are excluded. Those setup failures are preserved in public logs;
they are not counted as successful hosted delivery. Nine completed model turns
including recorded setup retries reported 3,503,217 aggregate input tokens
(3,196,032 cached) and 20,520 output tokens. Interrupted turns may not report
usage. Currency billing is not exposed by the CLI; these are ordinary Codex
account calls, not a recurring background agent.

The current Cloud setup uses all four settings as runtime Environment variables,
following the user's working setup. The two credentials remain sensitive values,
while Codex Cloud's Secrets field is setup-only. The editing-agent prompt runs
Grove directly and forwards all four client variable names plus proxy/CA names.
No credential values are embedded in the prompt or MCP TOML. The separate
analytics environment still needs configuration and agent-phase validation.

Receipts: [local](coordination-local-receipt.json),
[hosted](coordination-hosted-receipt.json), and
[real Codex sessions](coordination-codex-receipt.json). Complete public event logs
remain outside the repository at `/workspace/research/codex-live-demo-verified`.
A separately cloned IWS checkout also discovered both real native peers over
production HTTPS, without creating a local coordination database or sharing
Git metadata with their worktrees. Its participant was an explicitly synthetic
transport fixture. The two CLI sessions and independent clone share this Cloud
execution environment; they are not independently managed Codex Cloud tasks.
The user's separate analytics environment still needs the four client variables and trusted MCP configuration from the
guide; this chat has no settings API for that environment. A private ignored
`.env.grove-client` contains only its client settings; secrets were not committed.

Neon's selected Free resource advertises 1 GB storage and 100 CU-hours per project.
Vercel uses the existing account's plan. Active polling consumes function requests,
database compute and network transfer; Grove itself never calls an LLM. The
snapshot backend described in this October 13 receipt is superseded by the
October 15 row-store fix below. False alarms on independent real work, semantic transfer review by a real
operator, unrelated-session discovery, and idle-model wakeup remain unmeasured.

## Egress correction (2026-10-15)

The old deployment fetched a 4,430,582-byte SQL response containing the complete
repository snapshot on every operation, including unchanged polls. Cursor
filtering occurred after that transfer. The replacement uses indexed Postgres
rows, operation-specific reads, an atomic revision fence for changed rows/events,
and a bounded sync cache validated against database versions and session clocks
on every hit. Historical messages/events and the original archive are preserved.

The workspace passed 915 tests (two ignored by default), strict all-target
Clippy, formatting and the 32-assertion disposable-process acceptance. The new
PostgreSQL acceptance is required separately in Linux CI and passes locally on
PGlite 0.3.14 (Apache-2.0), an embedded PostgreSQL engine used only by tests. Its
exact version and integrity are pinned in the fixture's package lock; it is not
a production dependency and no upstream implementation was copied.

The same acceptance passed on the fresh Neon project through the production SQL
HTTP transport. It compares operation responses and every persisted row to the
original SQLite state machine, and checks concurrent same-key retries, distinct
concurrent sends, recipient ACKs, cursors/page/scan boundaries, decision authority,
claim fencing/expiry, transfer policy, restart recovery, empty-only import and
legacy-writer fencing. Acknowledged history grows to 1,000 messages of 22 KiB each
plus their event copies in the local fixture without inflating hot reads.

Replaying a private copy of the actual source data in an isolated Neon repository
used 2,150 SQL response bytes for heartbeat, 183,670 for a cold unchanged sync,
and 1,863 for warm sync, including heartbeat refresh. These are measured consumed
SQL response bodies, not exact billed packets. Warm-instance reuse is not
guaranteed by Vercel; first sync and context-changing calls still cost more.
The test makes no model calls and does not impersonate a native session in the
live repository. The [machine-readable receipt](coordination-egress-receipt.json)
records migration and production verification separately.

Reproduce without cloud credentials:

```sh
npm ci --prefix scripts/postgres-fixture --ignore-scripts
node scripts/postgres-fixture/server.mjs
# In another terminal:
GROVE_TEST_SQL_ENDPOINT=http://127.0.0.1:45217 cargo test -p grove-cloud --lib --locked postgres_projection -- --ignored --nocapture
```

The fresh `grove-coordination-efficient` project belongs to the same Vercel
Marketplace account and stays on Free. Neon documents 5 GB outgoing transfer per
project per month, so this is a fresh allowance combined with lower consumption,
not an unlimited database. The client endpoint, repository UUID and four ordinary
runtime Environment variables are unchanged; database credentials stay server-side.

Production cutover preserved revision 2858 from the source, imported it at
revision 2859 in `still-brook-09634360`, and compared all eight table counts and
content digests before probing. The canonical endpoint now resolves to
`dpl_8NbyVmTyj2fMZJzmFcD8Ca7c4pfo` (source `11789ed978ca34d318c77cf04702736beb4d9e2a`).
The old source remains readable through the connected account as a fenced
archive. Its application role password was rotated, and both the retired SQL
credential and the old deployment's ability to read that database were tested
and rejected. The new database remains healthy. Initial production logs measured
1,941-byte warm syncs and approximately 2.2 KB heartbeats. Linux and macOS CI both
passed the implementation head, including the required PostgreSQL acceptance.

The bounded production observation lasted 900.86 seconds and completed 51
operations with no failures: registration, 32 syncs, 16 heartbeats, a checkpoint
and Hello. Runtime metrics recorded **795,984 SQL response body bytes** in total,
including the initial history/context reads; 30 syncs used the validated cache.
Warm sync stayed at 1,941 bytes and heartbeat at 2,214 bytes. At the old fixed
snapshot size and one attempt per call, 51 operations would read approximately
226 MB; that is a counterfactual estimate, not a historical billing measurement.
Neon's new-project meter still reported zero at the final read despite these
measured reads, so exact billed transfer and sustained independent-agent workload
cost remain unmeasured. After the window, all original eight table counts and
content digests still matched the source, the synthetic session was marked
stopped, and tracked IWS files and its validation branch were unchanged.

## Transient client failures (2026-10-17)

Grove 0.4.2 adds bounded retry/backoff to the shared CLI/MCP HTTP transport. It
replays only operations with safe repeat semantics, preserving the original
encoded request and deduplication key. Up to four attempts fit within a
45-second budget. Transient HTTP/transport failures retain safe UTC/status/
request-ID diagnostics; authentication, validation and application errors are
not retried. `Retry-After` seconds and HTTP dates constrain retry timing. A
longer cooldown is returned to the owning poller. A failed CLI call keeps one
JSON error on stderr; successful recovery emits one diagnostic JSON record on
stderr while stdout retains the original success/MCP protocol contract.

The workspace passed **926 tests, two ignored**, strict all-target Clippy and
formatting. Eleven new focused tests cover transient status recovery, exhausted
attempt/deadline budgets, cooldowns, excluded operations and credential redaction
including retry history. Tests using the real SQLite command engine commit a
send before returning a simulated 503 or withholding its response until timeout;
recovery returns the existing message with one durable message effect.

Separate real-process CLI and official MCP tests inject a loopback 503, then
verify successful recovery, identical replay, clean JSON/protocol streams and
safe stderr diagnostics. Authentication fails once without retry, and exhausted
CLI retries still produce one parseable JSON error. The existing 32-assertion
disposable coordination acceptance also passes. These fixtures use no model
calls or production writes. They validate resilience; they do not establish the
cause of the reported Editorial environment outage.

The Cloud installer builds a reviewed clean tools checkout and atomically
replaces the installed executable. See the [upgrade instructions](coordination.md#upgrading-an-installed-cloud-client).
Pulling source alone leaves existing executables unchanged. CLI wrappers use
the replacement on their next invocation; long-lived MCP processes need a
reconnection, and future Cloud tasks need refreshed setup/cache state.

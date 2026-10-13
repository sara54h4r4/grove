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
database compute and snapshot bandwidth; Grove itself never calls an LLM. Snapshot
size/retention and contention are explicit milestone bounds, not production-scale
claims. False alarms on independent real work, semantic transfer review by a real
operator, unrelated-session discovery, and idle-model wakeup remain unmeasured.

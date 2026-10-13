# Grove Agent Activity and Coordination Design

Historical design. See [coordination.md](coordination.md) for implemented Codex
coordination, current storage placement, hosting and bounded capabilities.

## Status

This is the working design for `feature/agent-file-activity`. The feature has
pivoted from static worktree labels to a provider-neutral local agent
observability and coordination control plane.

The existing worktree conflict-analysis engine, daemon state actor, socket
server, CLI, and TUI remain the foundation. Agent activity is integrated into
those systems; it is not a second daemon and does not replace conflict analysis.

## Product direction

Grove should answer, using metadata rather than conversation contents:

- which provider-qualified agent sessions exist;
- which folder, repository, worktree, and branch each session uses;
- whether each session is discovered, active, idle, stopped, or stale;
- which files changed and how confidently a session can be attributed;
- where simultaneous agents overlap;
- which explicit tasks, resource claims, dependencies, and handoffs agents have
  deliberately published.

The intended progression is:

```text
discovery -> session identity -> workspace binding -> live activity
          -> conflict detection -> explicit coordination -> human/agent APIs
```

## Privacy boundary

Provider adapters and public APIs use sparse allowlists. Grove must never
ingest, retain, print, log, persist, or expose prompts, messages, responses,
reasoning, conversation titles, command text, source/file contents, tool
payloads, raw model events, full API/SSE bodies, credentials, environment
values, authentication files, token counts, or cost data.

Allowed data is limited to provider/session identity, parent identity, a safe
role, working directory, repository-relative paths, repository/worktree/branch
identity, lifecycle, safe event type, timestamps, a safe process ID when
available, and coordination metadata intentionally submitted to Grove.

External responses are deserialized directly into allowlisted structures.
Unknown fields are discarded. Raw response/event bodies are never logged or
stored after parsing. Debug and error output must redact URLs and credentials.
Grove must not read process environments or conversation/transcript databases.

## Architecture

### Filesystem truth

The existing watcher continues to send:

```rust
StateMessage::FileChanged { workspace_id, path }
```

`DaemonState` retains one transient `FileActivity` per workspace-relative path.
The watcher remains the source of truth for the path, first/last change time,
and count. Paths outside the registered worktree and events at the worktree
root are ignored. Removing a workspace removes its file-activity records.

### Provider-neutral sessions

`grove-lib` owns shared serializable types:

- `AgentProvider`
- `SessionLifecycle`
- `DiscoverySource`
- `AgentSession`

Session identity is `(provider, native_session_id)`. Identical native IDs from
different providers do not collide, multiple sessions in one directory remain
distinct, and parent/child IDs are preserved.

The daemon's `AgentSessionRegistry` owns in-memory session state. It binds a
session directory to the most-specific containing Grove workspace and rebinds
all sessions when workspace registration changes. Unbound sessions are kept.

Snapshots are observations, not absence proofs. A session missing from one
snapshot is not stopped. `Stopped` requires an explicit reliable adapter
signal. `compacted` is an independent attribute and never a lifecycle.
Discovery-only snapshots do not downgrade a live lifecycle, and older provider
timestamps do not regress registry time.

### OpenCode adapter path

OpenCode currently supplies two complementary feeds:

- a periodic full metadata snapshot for discovery and recovery;
- an SSE stream for live normalized lifecycle deltas.

Both clients are owned by daemon tasks rather than the state actor, so network
I/O never blocks the actor. They send `AgentSessionUpdates` and
`AgentSessionEvent` messages to `DaemonState`.

When configured, the event supervisor takes a full snapshot before its initial
connection and before each reconnect, applies bounded exponential backoff, and
is explicitly cancelled and awaited during shutdown. The periodic poller
remains a recovery mechanism; neither task infers stop from absence.

Verified against the local OpenCode 1.18.8 server:

- snapshot endpoint: `GET /api/session`;
- event endpoint: `GET /api/event`;
- snapshot directory: `location.directory`;
- snapshot times: Unix milliseconds;
- SSE records use `data:` JSON envelopes.

Current upstream schemas use `properties` event payloads and unprefixed server
routes. The parser accepts only the safe metadata fields needed from both the
verified legacy envelope and current `properties` shapes. Endpoint
version/capability selection remains adapter work; Grove does not silently
replace the verified 1.18.8 routes.

The normalized OpenCode events applied today are server connected, session
created/updated/compacted/deleted, and message/message-part updates. Partial
events never create incomplete sessions. A complete session-created event needs
an identity and non-empty directory. Explicit deletion transitions an existing
session to stopped.

### File attribution

`FileActivity` includes a latest `FileAttribution` without replacing watcher
truth:

| Confidence | Meaning |
| --- | --- |
| `Exact` | A provider explicitly supplied both session identity and repository-relative file path. |
| `Strong` | Exactly one active session was bound to the workspace when the watcher event arrived. |
| `Ambiguous` | Multiple active sessions were bound to the workspace. All candidates are returned. |
| `StaticFallback` | No active session matched; an explicit `.grove/agents.toml` rule did. |
| `Unknown` | No reliable session or explicit static rule matched. |

`Exact` is reserved and is never inferred from timing. Known upstream
OpenCode `file.edited` metadata supplies a file path but not a session identity,
so it cannot currently produce exact attribution. Repeated watcher events
preserve the original first-change time and count while recomputing the latest
attribution.

Static mapping precedence remains exact path, exact branch, branch glob, then
worktree-name glob. Workspace-name fallback is still available through the
legacy assignment query, but it is not presented as attributed agent identity.

### Read APIs

The existing Unix-socket protocol and CLI expose:

- `get_file_activity`, optionally filtered by workspace and limit;
- `get_agent_assignments`, for legacy/static labels;
- `get_agent_sessions`, optionally filtered by provider, workspace, and
  lifecycle.

File activity now serializes attribution confidence and candidate session
identities. Session responses expose only `AgentSession`'s allowlisted fields
and are ordered newest-first.

### Provider adapter boundary

OpenCode-specific network schemas stay outside shared registry and task
orchestration code. `grove-daemon::provider` defines an object-safe
`ProviderAdapter` boundary for:

- allowlisted session snapshots;
- normalized lifecycle events;
- sanitized provider errors;
- declared snapshot, event, and health capabilities;
- explicit optional exact-file and parent/child capabilities.

`OpenCodeAdapter` owns the provider-specific snapshot and SSE clients and maps
their safe results into this contract. `ProviderTasks` owns the generic
immediate/periodic poller and snapshot-before-connect event supervisor. Daemon
startup constructs one adapter object, and ordered shutdown cancels and awaits
both tasks before stopping `DaemonState`.

OpenCode currently declares full metadata snapshots, lifecycle events,
connection-state health, native parent IDs, and no exact file attribution.
The generic runners have metadata-only fake-adapter contract tests and no
OpenCode-specific response schema assumptions.

Codex and Claude Code adapters must prefer official hooks, APIs, plugins, or
explicit metadata publishing. Process scanning is only a fallback presence
signal and cannot manufacture distinct sessions when several chats share a
process.

### Future coordination

Coordination is explicit, never inferred from chats. Provider-neutral records
will cover:

- tasks and deliberately published safe summaries/states/dependencies;
- advisory, shared, or exclusive file/directory claims with expiry;
- offered, accepted, completed, or rejected handoffs;
- append-only human-readable audit events.

These records will be exposed through the existing socket/CLI and later through
an agent-readable local interface such as MCP. Only after the backend semantics
are stable should the TUI add a session/activity view and Grove begin a local
web dashboard bound to `127.0.0.1`.

## Reliability invariants

- Provider I/O does not run inside `DaemonState`.
- Only one request per poller/supervisor is in flight.
- Every spawned integration task has cancellation and is awaited.
- Unknown or partial external events fail closed without corrupting lifecycle.
- No personal server, hardcoded port, machine-specific path, or live prompt is
  required by the default test suite.
- Live compatibility checks require explicit environment configuration and
  print aggregate counts only.
- Existing worktree conflict analysis and dirty-workspace dispatch continue
  unchanged.

## Open questions

- How should adapters negotiate legacy versus current provider endpoints?
- Which official Codex and Claude Code surfaces expose safe distinct session
  identity, lifecycle, working directory, and parent relationships?
- Can any provider publish both session identity and repository-relative edit
  path without exposing edit/tool payloads?
- Which transient session and coordination records should eventually persist,
  and with what retention policy?

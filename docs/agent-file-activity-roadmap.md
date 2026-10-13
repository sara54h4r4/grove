# Grove Agent Activity Roadmap

Historical feature-branch roadmap. The current milestone, setup and remaining
work are maintained in [coordination.md](coordination.md).

Last updated: 2026-07-29

This is the running checklist for `feature/agent-file-activity`. The current
goal is real provider-qualified session observability and explicit multi-agent
coordination, with file activity as one signal.

## Completed foundation

- [x] Audit the staged, unstaged, and untracked feature implementation.
- [x] Establish the WSL Cargo baseline without changing unrelated TUI behavior.
- [x] Preserve existing dirty-workspace and conflict-analysis dispatch.
- [x] Retain per-worktree, repository-relative `FileActivity` truth.
- [x] Add filtered daemon/socket/CLI file-activity queries.
- [x] Add `.grove/agents.toml` as a non-authoritative fallback.
- [x] Enforce global static mapping precedence: path, branch, branch glob,
  worktree-name glob.

## Provider-neutral session foundation

- [x] Add provider, lifecycle, discovery-source, and allowlisted session types.
- [x] Key sessions by `(provider, native_session_id)`.
- [x] Preserve multiple sessions per directory and parent/child relationships.
- [x] Bind sessions to the most-specific containing registered workspace.
- [x] Rebind on workspace registration/removal/path changes.
- [x] Prevent discovery snapshots from downgrading live lifecycle.
- [x] Prevent older provider timestamps from regressing registry time.
- [x] Add filtered, newest-first `get_agent_sessions` query support.

## OpenCode discovery and live state

- [x] Add sparse allowlisted `/api/session` snapshot deserialization.
- [x] Add immediate and periodic non-overlapping snapshot polling.
- [x] Add metadata-only SSE normalization without retaining/logging raw events.
- [x] Support verified legacy `data` and current upstream `properties` event
  envelopes.
- [x] Apply safe normalized session/message events in `DaemonState`.
- [x] Ignore incomplete/unknown-session partial events.
- [x] Preserve compaction as a boolean and require explicit deletion for stop.
- [x] Run SSE only when configured, with cancellation and awaited shutdown.
- [x] Reconnect with bounded exponential backoff and snapshot-before-connect
  recovery.
- [x] Reject credential-bearing URLs and redact client/config Debug output.
- [x] Reject absent or empty required credentials without echoing values.
- [x] Replace fragile personal live tests with an ignored, explicitly configured
  aggregate-only compatibility procedure.

## Attribution confidence

- [x] Preserve watcher facts instead of replacing them with inferred activity.
- [x] Add `Exact`, `Strong`, `Ambiguous`, `StaticFallback`, and `Unknown`.
- [x] Attribute one active workspace session as strong.
- [x] Return all active candidates when attribution is ambiguous.
- [x] Ignore idle/stopped/stale sessions during active correlation.
- [x] Use only explicit static rules as static fallback.
- [x] Recompute latest attribution on repeated writes.
- [x] Reserve exact attribution for a provider signal containing both session
  identity and repository-relative path.

## Verification snapshot

- [x] Focused registry, event, SSE recovery, configuration, attribution, and
  socket tests.
- [x] Daemon suite: 310 passed, 1 explicitly configured live test ignored.
- [x] Formatting, workspace check, Clippy, workspace tests, and workspace build
  passed after all current code edits.
- [x] Full workspace tests passed with only the two documented desktop-terminal
  skips; baseline Clippy warnings are recorded in the handoff report.

## Provider adapter abstraction

- [x] Define an object-safe provider-neutral adapter contract.
- [x] Model snapshot, lifecycle-event, and health capabilities.
- [x] Model exact-file and parent/child capabilities explicitly.
- [x] Add sanitized provider-neutral adapter error categories.
- [x] Put OpenCode snapshot and SSE clients behind `OpenCodeAdapter`.
- [x] Move polling, reconnect recovery, and ordered shutdown into generic
  `ProviderTasks`.
- [x] Remove the superseded OpenCode-specific orchestration traits and loops.
- [x] Add metadata-only fake-adapter and concrete OpenCode contract tests.

## Next three implementation stages

### 1. Codex metadata investigation and adapter

- [ ] Investigate official APIs/hooks/plugins with a narrow metadata-only
  experiment.
- [ ] Label verified facts, inferences, and unknowns.
- [ ] Implement only safe identity/lifecycle/directory data actually supported.
- [ ] Use process presence only as a clearly marked fallback.

### 2. Claude Code metadata investigation and adapter

- [ ] Repeat the official-surface and privacy audit independently.
- [ ] Do not read conversation JSONL/transcript bodies or infer chat identity
  from a shared process.
- [ ] Normalize only verified safe metadata through the provider interface.

### 3. Coordination model and read APIs

- [ ] Add explicit `AgentTask`, `ResourceClaim`, dependency, and `Handoff`
  models.
- [ ] Add read-only conflict/dependency/claim queries before mutations.
- [ ] Define expiry and heartbeat semantics without inferring intent.

## Later stages

- [ ] Add socket/CLI coordination queries and mutations with expiry/heartbeat.
- [ ] Add an agent-readable local coordination interface such as MCP.
- [ ] Add a basic session/activity/attribution view to the existing TUI.
- [ ] Add a local web dashboard only after backend semantics stabilize.
- [ ] Harden WSL/native-Windows discovery, privacy, persistence, packaging, and
  end-to-end recovery tests.

## Known constraints and open questions

- OpenCode 1.18.8 was verified at `/api/session` and `/api/event`; current
  upstream routes differ, so endpoint capability/version selection is pending.
- Known OpenCode file-edit metadata lacks a session ID and cannot establish
  exact attribution.
- Safe distinct-session discovery surfaces for Codex and Claude Code remain to
  be verified.
- The Codex desktop terminal presents as a TTY, so one existing grove-tui
  non-TTY assertion is skipped only in this environment during the full suite.

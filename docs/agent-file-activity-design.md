# Agent File Activity and Repository Tree Design

## Status

Draft design for the `feature/agent-file-activity` branch.

## Summary

This feature extends Grove so it can passively visualize which files are being
changed in each Git worktree and attribute those changes to the coding agent
assigned to that worktree.

The implementation begins with a basic recent-file activity view, expands into a
polished live activity system, and ultimately supports a graphical repository
tree where files and directories display agent, worktree, Git status, activity,
and conflict information.

The design preserves Grove's existing conflict-analysis behaviour. Live file
activity is an additional subsystem built alongside the current analysis
pipeline, not a replacement for it.

## Goals

The feature should:

1. Detect filesystem changes inside every Grove-managed worktree.
2. Preserve the exact changed path and associated worktree.
3. Attribute each worktree to an agent such as Codex, Claude, or OpenCode.
4. Display files that are active, recently active, or modified.
5. Update the interface without requiring integration with agent chat sessions.
6. Warn when multiple worktrees are changing the same repository-relative file.
7. Support a complete expandable repository tree in a graphical interface.
8. Work reliably when Grove runs inside WSL.
9. Preserve Grove's existing worktree conflict and dependency analysis.
10. Remain useful when no agent mapping is configured.

## Non-goals

This feature will not:

1. Read Claude, Codex, or OpenCode conversations.
2. Inspect prompts, model output, or terminal text.
3. Determine which file an agent is merely reading or reasoning about.
4. Claim that a file is being edited when no filesystem activity has occurred.
5. manage or launch coding agents.
6. replace Grove's current TUI.
7. initially provide native Windows support for the complete Grove daemon.
8. persist transient "active now" state across restarts.

## Terminology

### Workspace

A Grove-managed Git worktree. Grove currently represents worktrees as
`Workspace` values identified by a `WorkspaceId`.

### Agent

A human-readable label assigned to a workspace, such as:

- Codex
- Claude
- OpenCode
- Manual
- Unassigned

An agent is inferred from configuration and worktree metadata. Grove does not
inspect the corresponding operating-system process.

### Repository-relative path

A normalized path relative to the root of a worktree, for example:

```text
crates/grove-daemon/src/state.rs
```
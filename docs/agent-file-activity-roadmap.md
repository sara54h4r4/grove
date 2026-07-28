# Agent File Activity Roadmap

## Purpose

Implement Grove's agent file activity feature in small, testable stages.

The order is:

1. Basic recent-file tracking
2. Polished live activity
3. Repository-tree backend
4. Graphical repository tree
5. WSL hardening
6. Packaging and release

Do not begin a later stage until the current stage builds and passes its tests.

---

## Stage 0 — Baseline

### Tasks

- Confirm the feature worktree is active.
- Confirm Grove builds in WSL.
- Run formatting checks.
- Run Clippy.
- Run workspace tests.
- Record pre-existing failures.
- Run the workspace build.

### Commands

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
cargo test --workspace -- --skip new_event_handler_emits_tick_or_input_stream_event
cargo build --workspace
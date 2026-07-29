use std::collections::HashMap;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use grove_lib::{AgentProvider, AgentSession, DiscoverySource, SessionLifecycle, WorkspaceId};

/// Payload for inserting or updating a session in the registry.
///
/// Fields that are `None` or use a default will not overwrite existing values on
/// update.  On first insert every provided field is stored as-is.
///
/// When `created_at` or `updated_at` are `None` the registry falls back to
/// [`Utc::now()`] for the respective timestamp.
#[derive(Debug, Clone)]
pub struct SessionUpdate {
    pub provider: AgentProvider,
    pub native_session_id: String,
    pub parent_native_session_id: Option<String>,
    pub agent_role: Option<String>,
    pub directory: PathBuf,
    pub lifecycle: SessionLifecycle,
    pub discovery_source: DiscoverySource,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

/// In-memory registry of agent sessions keyed by `(AgentProvider, native_session_id)`.
///
/// This registry is provider-neutral: sessions from different providers never
/// collide, and the same directory may hold multiple independent sessions.
#[derive(Debug, Clone)]
pub struct AgentSessionRegistry {
    sessions: HashMap<(AgentProvider, String), AgentSession>,
    workspace_paths: HashMap<WorkspaceId, PathBuf>,
}

impl AgentSessionRegistry {
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
            workspace_paths: HashMap::new(),
        }
    }

    /// Register (or replace) the set of known Grove workspace paths.
    ///
    /// These paths are used by [`resolve_workspace_id`] to bind sessions to
    /// workspaces.  Calling this method does **not** retroactively re-bind
    /// existing sessions that were already upserted — they keep whatever
    /// `workspace_id` was computed at the time of their last upsert.
    pub fn set_workspace_paths(&mut self, paths: HashMap<WorkspaceId, PathBuf>) {
        self.workspace_paths = paths;
    }

    /// Return the registered workspace paths.
    pub fn workspace_paths(&self) -> &HashMap<WorkspaceId, PathBuf> {
        &self.workspace_paths
    }

    /// Re-evaluate the `workspace_id` binding for every session based on the
    /// current set of registered workspace paths.
    ///
    /// Call this after workspaces are added, removed, or their paths change.
    /// Sessions whose directory falls inside a registered workspace are bound
    /// to the most specific match; all others are set to `None`.
    pub fn rebind_all_sessions(&mut self) {
        let paths = self.workspace_paths.clone();
        for session in self.sessions.values_mut() {
            session.workspace_id = Self::resolve_workspace_id_inner(&session.directory, &paths);
        }
    }

    fn resolve_workspace_id_inner(
        directory: &Path,
        workspace_paths: &HashMap<WorkspaceId, PathBuf>,
    ) -> Option<WorkspaceId> {
        let canonical = if directory.is_absolute() {
            directory.to_path_buf()
        } else {
            let Ok(cwd) = std::env::current_dir() else {
                return None;
            };
            cwd.join(directory)
        };

        workspace_paths
            .iter()
            .filter(|(_, ws_path)| canonical.starts_with(ws_path))
            .max_by_key(|(_, ws_path)| ws_path.as_os_str().len())
            .map(|(id, _)| *id)
    }

    /// Insert a new session or update an existing one.
    ///
    /// *Identity* – the record is looked up by `(provider, native_session_id)`.
    /// A second upsert with the same pair updates the same record.
    ///
    /// *Timestamps* – `first_seen_at` and `created_at` are set once on first
    /// insert and never overwritten.  `updated_at` and `last_seen_at` advance
    /// to the current time on every upsert.
    ///
    /// *Workspace binding* – the registry matches `directory` against the
    /// registered workspace paths via [`resolve_workspace_id`].  A session
    /// outside any registered workspace keeps `workspace_id = None`.
    ///
    /// *Compaction guard* – this method never clears the `compacted` flag.
    /// Once `compacted` is `true` it stays `true`.
    pub fn upsert(&mut self, update: SessionUpdate) {
        let now = Utc::now();
        let workspace_id = self.resolve_workspace_id(&update.directory);

        let key = (update.provider, update.native_session_id.clone());

        match self.sessions.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut occupied) => {
                let existing = occupied.get_mut();
                let parent = update
                    .parent_native_session_id
                    .or(existing.parent_native_session_id.take());
                existing.parent_native_session_id = parent;
                if let Some(role) = update.agent_role {
                    existing.agent_role = Some(role);
                }
                existing.directory = update.directory;
                if update.lifecycle != SessionLifecycle::Discovered
                    || existing.lifecycle == SessionLifecycle::Discovered
                {
                    existing.lifecycle = update.lifecycle;
                }
                existing.updated_at = existing.updated_at.max(update.updated_at.unwrap_or(now));
                existing.last_seen_at = now;
                existing.discovery_source = update.discovery_source;
                existing.workspace_id = workspace_id;
            }
            std::collections::hash_map::Entry::Vacant(vacant) => {
                vacant.insert(AgentSession {
                    provider: update.provider,
                    native_session_id: update.native_session_id,
                    parent_native_session_id: update.parent_native_session_id,
                    agent_role: update.agent_role,
                    directory: update.directory,
                    workspace_id,
                    lifecycle: update.lifecycle,
                    created_at: update.created_at.unwrap_or(now),
                    updated_at: update.updated_at.unwrap_or(now),
                    first_seen_at: now,
                    last_seen_at: now,
                    discovery_source: update.discovery_source,
                    compacted: false,
                });
            }
        }
    }

    /// Mark a session as compacted without changing its lifecycle.
    ///
    /// Returns `None` when the session does not exist.
    pub fn compact(&mut self, provider: AgentProvider, native_session_id: &str) -> Option<()> {
        let session = self
            .sessions
            .get_mut(&(provider, native_session_id.to_string()))?;
        session.compacted = true;
        Some(())
    }

    /// Look up a session by provider and native ID.
    pub fn get(&self, provider: AgentProvider, native_session_id: &str) -> Option<&AgentSession> {
        self.sessions
            .get(&(provider, native_session_id.to_string()))
    }

    /// Return all sessions for a given provider.
    pub fn get_by_provider(&self, provider: AgentProvider) -> Vec<&AgentSession> {
        self.sessions
            .iter()
            .filter(|((p, _), _)| *p == provider)
            .map(|(_, s)| s)
            .collect()
    }

    /// Return all sessions whose `directory` is under a given path.
    pub fn get_by_directory(&self, directory: &Path) -> Vec<&AgentSession> {
        self.sessions
            .values()
            .filter(|s| s.directory.starts_with(directory))
            .collect()
    }

    /// Return child sessions of a given parent session.
    pub fn get_children(
        &self,
        provider: AgentProvider,
        parent_native_session_id: &str,
    ) -> Vec<&AgentSession> {
        self.sessions
            .values()
            .filter(|s| {
                s.provider == provider
                    && s.parent_native_session_id.as_deref() == Some(parent_native_session_id)
            })
            .collect()
    }

    /// Return all sessions currently in the registry.
    pub fn all(&self) -> Vec<&AgentSession> {
        self.sessions.values().collect()
    }

    /// Number of sessions in the registry.
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    // ── Workspace matching ────────────────────────────────────────────────

    /// Given a session directory, find the most specific (longest path) matching
    /// registered workspace.
    ///
    /// If no registered workspace contains the directory, returns `None`.
    pub fn resolve_workspace_id(&self, directory: &Path) -> Option<WorkspaceId> {
        Self::resolve_workspace_id_inner(directory, &self.workspace_paths)
    }
}

impl Default for AgentSessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn op_encode_update(
        native_id: &str,
        directory: &str,
        lifecycle: SessionLifecycle,
    ) -> SessionUpdate {
        SessionUpdate {
            provider: AgentProvider::OpenCode,
            native_session_id: native_id.to_string(),
            parent_native_session_id: None,
            agent_role: None,
            directory: PathBuf::from(directory),
            lifecycle,
            discovery_source: DiscoverySource::ProcessScan,
            created_at: None,
            updated_at: None,
        }
    }

    fn claude_update(
        native_id: &str,
        directory: &str,
        lifecycle: SessionLifecycle,
    ) -> SessionUpdate {
        SessionUpdate {
            provider: AgentProvider::ClaudeCode,
            native_session_id: native_id.to_string(),
            parent_native_session_id: None,
            agent_role: None,
            directory: PathBuf::from(directory),
            lifecycle,
            discovery_source: DiscoverySource::ApiPoll,
            created_at: None,
            updated_at: None,
        }
    }

    // ── Stable identity across repeated updates ───────────────────────

    #[test]
    fn stable_identity_on_repeated_upsert() {
        let mut reg = AgentSessionRegistry::new();
        reg.upsert(op_encode_update(
            "ses-1",
            "/tmp/ws-a",
            SessionLifecycle::Active,
        ));

        reg.upsert(op_encode_update(
            "ses-1",
            "/tmp/ws-a",
            SessionLifecycle::Idle,
        ));

        assert_eq!(reg.len(), 1);
        let s = reg.get(AgentProvider::OpenCode, "ses-1").unwrap();
        assert_eq!(s.lifecycle, SessionLifecycle::Idle);
    }

    // ── Provider-separated IDs ────────────────────────────────────────

    #[test]
    fn same_native_id_different_providers_are_separate() {
        let mut reg = AgentSessionRegistry::new();

        reg.upsert(op_encode_update(
            "shared-id",
            "/tmp/ws-a",
            SessionLifecycle::Active,
        ));
        reg.upsert(claude_update(
            "shared-id",
            "/tmp/ws-b",
            SessionLifecycle::Active,
        ));

        assert_eq!(reg.len(), 2);

        let open_code_sessions = reg.get_by_provider(AgentProvider::OpenCode);
        assert_eq!(open_code_sessions.len(), 1);

        let claude_sessions = reg.get_by_provider(AgentProvider::ClaudeCode);
        assert_eq!(claude_sessions.len(), 1);
    }

    // ── Multiple sessions in one directory ────────────────────────────

    #[test]
    fn multiple_sessions_in_same_directory() {
        let mut reg = AgentSessionRegistry::new();

        reg.upsert(op_encode_update(
            "ses-a",
            "/tmp/shared",
            SessionLifecycle::Active,
        ));
        reg.upsert(claude_update(
            "ses-b",
            "/tmp/shared",
            SessionLifecycle::Active,
        ));

        let in_dir = reg.get_by_directory(Path::new("/tmp/shared"));
        assert_eq!(in_dir.len(), 2);

        // Same provider, different native IDs: still separate
        reg.upsert(op_encode_update(
            "ses-c",
            "/tmp/shared",
            SessionLifecycle::Active,
        ));
        assert_eq!(reg.len(), 3);
    }

    // ── Parent-session linkage ────────────────────────────────────────

    #[test]
    fn parent_child_linkage() {
        let mut reg = AgentSessionRegistry::new();

        let parent = op_encode_update("parent-1", "/tmp/ws-a", SessionLifecycle::Active);
        reg.upsert(parent);

        let mut child = op_encode_update("child-1", "/tmp/ws-a/sub", SessionLifecycle::Active);
        child.parent_native_session_id = Some("parent-1".to_string());
        reg.upsert(child);

        let children = reg.get_children(AgentProvider::OpenCode, "parent-1");
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].native_session_id, "child-1");

        // Different provider: no cross-talk
        let cross = reg.get_children(AgentProvider::ClaudeCode, "parent-1");
        assert!(cross.is_empty());
    }

    // ── Active -> Idle lifecycle changes ──────────────────────────────

    #[test]
    fn active_to_idle_lifecycle_change() {
        let mut reg = AgentSessionRegistry::new();

        reg.upsert(op_encode_update(
            "ses-1",
            "/tmp/ws-a",
            SessionLifecycle::Active,
        ));
        assert_eq!(
            reg.get(AgentProvider::OpenCode, "ses-1").unwrap().lifecycle,
            SessionLifecycle::Active
        );

        reg.upsert(op_encode_update(
            "ses-1",
            "/tmp/ws-a",
            SessionLifecycle::Idle,
        ));
        assert_eq!(
            reg.get(AgentProvider::OpenCode, "ses-1").unwrap().lifecycle,
            SessionLifecycle::Idle
        );
    }

    // ── Explicit stopped transition ───────────────────────────────────

    #[test]
    fn explicit_stopped_transition() {
        let mut reg = AgentSessionRegistry::new();

        reg.upsert(op_encode_update(
            "ses-1",
            "/tmp/ws-a",
            SessionLifecycle::Active,
        ));
        assert_eq!(
            reg.get(AgentProvider::OpenCode, "ses-1").unwrap().lifecycle,
            SessionLifecycle::Active
        );

        // Only an explicit lifecycle signal changes to Stopped
        reg.upsert(op_encode_update(
            "ses-1",
            "/tmp/ws-a",
            SessionLifecycle::Stopped,
        ));
        assert_eq!(
            reg.get(AgentProvider::OpenCode, "ses-1").unwrap().lifecycle,
            SessionLifecycle::Stopped
        );
    }

    // ── Compaction does not alter lifecycle ───────────────────────────

    #[test]
    fn compaction_does_not_change_lifecycle() {
        let mut reg = AgentSessionRegistry::new();

        reg.upsert(op_encode_update(
            "ses-1",
            "/tmp/ws-a",
            SessionLifecycle::Active,
        ));
        reg.compact(AgentProvider::OpenCode, "ses-1");

        let session = reg.get(AgentProvider::OpenCode, "ses-1").unwrap();
        assert!(session.compacted);
        assert_eq!(session.lifecycle, SessionLifecycle::Active);

        // Compacting a non-existent session returns None
        assert!(
            reg.compact(AgentProvider::OpenCode, "no-such-session")
                .is_none()
        );
    }

    // ── Correct workspace binding ─────────────────────────────────────

    #[test]
    fn workspace_binding_on_upsert() {
        let mut reg = AgentSessionRegistry::new();
        let ws_id = WorkspaceId::default();
        let mut paths = HashMap::new();
        paths.insert(ws_id, PathBuf::from("/home/user/project"));
        reg.set_workspace_paths(paths);

        reg.upsert(op_encode_update(
            "ses-1",
            "/home/user/project/src",
            SessionLifecycle::Active,
        ));

        let session = reg.get(AgentProvider::OpenCode, "ses-1").unwrap();
        assert_eq!(session.workspace_id, Some(ws_id));
    }

    // ── Most-specific nested workspace wins ───────────────────────────

    #[test]
    fn most_specific_nested_workspace_wins() {
        let mut reg = AgentSessionRegistry::new();

        // Different WorkspaceIds so we can distinguish
        let broad_id = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, b"broad");
        let narrow_id = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, b"narrow");

        let mut paths = HashMap::new();
        paths.insert(broad_id, PathBuf::from("/home/user"));
        paths.insert(narrow_id, PathBuf::from("/home/user/project"));
        reg.set_workspace_paths(paths);

        reg.upsert(op_encode_update(
            "ses-1",
            "/home/user/project/src/lib",
            SessionLifecycle::Active,
        ));

        let session = reg.get(AgentProvider::OpenCode, "ses-1").unwrap();
        assert_eq!(session.workspace_id, Some(narrow_id));
    }

    // ── Unmatched directory remains unbound ───────────────────────────

    #[test]
    fn unmatched_directory_remains_unbound() {
        let mut reg = AgentSessionRegistry::new();
        let mut paths = HashMap::new();
        paths.insert(WorkspaceId::default(), PathBuf::from("/home/user/project"));
        reg.set_workspace_paths(paths);

        reg.upsert(op_encode_update(
            "ses-1",
            "/tmp/outside",
            SessionLifecycle::Active,
        ));

        let session = reg.get(AgentProvider::OpenCode, "ses-1").unwrap();
        assert_eq!(session.workspace_id, None);
    }

    // ── First-seen and created-at stability ───────────────────────────

    #[test]
    fn first_seen_and_created_at_stable_across_updates() {
        let mut reg = AgentSessionRegistry::new();

        reg.upsert(op_encode_update(
            "ses-1",
            "/tmp/ws-a",
            SessionLifecycle::Active,
        ));
        let first_seen = reg
            .get(AgentProvider::OpenCode, "ses-1")
            .unwrap()
            .first_seen_at;
        let created = reg
            .get(AgentProvider::OpenCode, "ses-1")
            .unwrap()
            .created_at;

        // Sleep briefly to ensure timestamps would differ
        std::thread::sleep(std::time::Duration::from_millis(2));

        reg.upsert(op_encode_update(
            "ses-1",
            "/tmp/ws-a",
            SessionLifecycle::Idle,
        ));
        let s = reg.get(AgentProvider::OpenCode, "ses-1").unwrap();

        assert_eq!(s.first_seen_at, first_seen);
        assert_eq!(s.created_at, created);
        assert!(s.updated_at > created);
        assert!(s.last_seen_at > first_seen);
    }

    #[test]
    fn discovered_update_does_not_downgrade_live_lifecycle() {
        let mut reg = AgentSessionRegistry::new();
        reg.upsert(op_encode_update(
            "ses-1",
            "/tmp/ws-a",
            SessionLifecycle::Active,
        ));

        let mut snapshot = op_encode_update("ses-1", "/tmp/ws-a", SessionLifecycle::Discovered);
        snapshot.discovery_source = DiscoverySource::ApiPoll;
        reg.upsert(snapshot);

        let session = reg.get(AgentProvider::OpenCode, "ses-1").unwrap();
        assert_eq!(session.lifecycle, SessionLifecycle::Active);
    }

    #[test]
    fn directory_move_outside_registered_workspace_unbinds_session() {
        let mut reg = AgentSessionRegistry::new();
        let workspace_id = uuid::Uuid::new_v4();
        reg.set_workspace_paths(HashMap::from([(
            workspace_id,
            PathBuf::from("/home/user/project"),
        )]));

        reg.upsert(op_encode_update(
            "ses-1",
            "/home/user/project/src",
            SessionLifecycle::Active,
        ));
        assert_eq!(
            reg.get(AgentProvider::OpenCode, "ses-1")
                .unwrap()
                .workspace_id,
            Some(workspace_id)
        );

        reg.upsert(op_encode_update(
            "ses-1",
            "/home/user/other-project",
            SessionLifecycle::Active,
        ));

        assert_eq!(
            reg.get(AgentProvider::OpenCode, "ses-1")
                .unwrap()
                .workspace_id,
            None
        );
    }

    #[test]
    fn older_provider_timestamp_does_not_regress_updated_at() {
        let mut reg = AgentSessionRegistry::new();
        let newer = DateTime::from_timestamp_millis(2_000).unwrap();
        let older = DateTime::from_timestamp_millis(1_000).unwrap();

        let mut initial = op_encode_update("ses-1", "/tmp/ws-a", SessionLifecycle::Active);
        initial.updated_at = Some(newer);
        reg.upsert(initial);

        let mut stale = op_encode_update("ses-1", "/tmp/ws-a", SessionLifecycle::Idle);
        stale.updated_at = Some(older);
        reg.upsert(stale);

        assert_eq!(
            reg.get(AgentProvider::OpenCode, "ses-1")
                .unwrap()
                .updated_at,
            newer
        );
    }
}

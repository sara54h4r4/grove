//! Supported explicit Codex registration. No transcript/private-store reads;
//! no assumption that an App Server owns independently started clients.
use crate::{event_stream::AgentEvent, provider::*, session_registry::SessionUpdate};
use grove_lib::coordination::Session;
use grove_lib::{AgentProvider, DiscoverySource, SessionLifecycle};
use std::{path::PathBuf, time::Duration};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub struct CodexAdapter {
    database_path: PathBuf,
}
impl CodexAdapter {
    pub fn new(database_path: PathBuf) -> Self {
        Self { database_path }
    }
}

pub fn normalize(s: &Session) -> Option<SessionUpdate> {
    // A remote cloud path can equal a local path without denoting the same
    // worktree. Remote peers remain visible in coordination, never local edit
    // attribution. The daemon verifies the persistent worktree marker.
    (s.provider == "codex" && s.observation.verification == "local_git").then(|| SessionUpdate {
        provider: AgentProvider::Codex,
        native_session_id: s.native_session_id.clone(),
        parent_native_session_id: s
            .parent_session_id
            .as_ref()
            .and_then(|p| p.strip_prefix("codex:").map(str::to_owned)),
        agent_role: Some(s.task.clone()),
        directory: PathBuf::from(&s.observation.worktree),
        lifecycle: match s.status.as_str() {
            "active" => SessionLifecycle::Active,
            "idle" => SessionLifecycle::Idle,
            "stopped" => SessionLifecycle::Stopped,
            _ => SessionLifecycle::Stale,
        },
        discovery_source: DiscoverySource::Manual,
        created_at: chrono::DateTime::from_timestamp_millis(s.registered_at),
        updated_at: chrono::DateTime::from_timestamp_millis(s.heartbeat_at),
    })
}

impl ProviderAdapter for CodexAdapter {
    fn provider(&self) -> AgentProvider {
        AgentProvider::Codex
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            snapshots: SnapshotCapability::FullMetadata,
            events: EventCapability::Unsupported,
            health: HealthCapability::ConnectionState,
            exact_file_activity: ExactFileActivityCapability::Unsupported,
            parent_sessions: ParentSessionCapability::NativeIds,
        }
    }
    fn fetch_snapshot(
        &self,
    ) -> AdapterFuture<'_, Result<Vec<SessionUpdate>, ProviderAdapterError>> {
        let path = self.database_path.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let conn = rusqlite::Connection::open_with_flags(
                    path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )
                .map_err(|_| ProviderAdapterError::RequestFailed)?;
                let sessions =
                    crate::coordination::sessions(&conn, chrono::Utc::now().timestamp_millis())
                        .map_err(|_| ProviderAdapterError::MalformedMetadata)?;
                Ok(sessions.iter().filter_map(normalize).collect())
            })
            .await
            .map_err(|_| ProviderAdapterError::RequestFailed)?
        })
    }
    fn listen(
        &self,
        _events_tx: mpsc::Sender<AgentEvent>,
        _shutdown_rx: oneshot::Receiver<()>,
    ) -> AdapterFuture<'_, Result<(), ProviderAdapterError>> {
        Box::pin(async { Err(ProviderAdapterError::InvalidEventStream) })
    }
}

pub const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Constant-time comparison for configured bearer credentials. Empty/missing
/// credentials never authorize, including the empty/empty case.
pub fn credential_matches(expected: Option<&str>, supplied: Option<&str>) -> bool {
    let (Some(a), Some(b)) = (expected, supplied) else {
        return false;
    };
    if a.is_empty() || a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}

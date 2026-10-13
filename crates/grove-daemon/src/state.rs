use chrono::Utc;
use grove_lib::graph::{GraphOverlay, ImportGraph};
use grove_lib::{
    AgentProvider, AgentSession, AgentSessionRef, AttributionConfidence, CommitHash,
    DiscoverySource, FileActivity, FileAttribution, OrthogonalityScore, SessionLifecycle,
    Workspace, WorkspaceId, WorkspacePairAnalysis,
};
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::agent_config::{self, AgentsConfig};
use crate::db::Database;
use crate::event_stream::{AgentEvent, EventKind};
use crate::session_registry::{AgentSessionRegistry, SessionUpdate};
use crate::worker::{WorkerMessage, build_base_graph_from_workspace, canonical_pair};

// === Configuration ===

/// Configuration for connecting to an OpenCode server.
///
/// Credentials are redacted in Debug output and never exposed in error
/// messages, logs, or panic messages.
#[derive(Clone)]
pub struct OpenCodeConfig {
    pub server_url: String,
    pub(crate) username: String,
    pub(crate) password: String,
}

impl std::fmt::Debug for OpenCodeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenCodeConfig")
            .field("server_url", &"<configured>")
            .field("username", &"***")
            .field("password", &"***")
            .finish()
    }
}

impl OpenCodeConfig {
    /// Read OpenCode connection settings from environment variables.
    ///
    /// Returns `Ok(None)` when `OPENCODE_SERVER_URL` is absent (integration disabled).
    /// Returns `Ok(Some(...))` when all three variables are present and non-empty.
    /// Returns `Err(...)` when URL is set but username or password is missing.
    ///
    /// Credentials are **never** included in any returned error message.
    pub fn from_env() -> Result<Option<Self>, String> {
        Self::from_lookup(std::env::var)
    }

    fn from_lookup(
        mut variable: impl FnMut(&'static str) -> Result<String, std::env::VarError>,
    ) -> Result<Option<Self>, String> {
        let url = match variable("OPENCODE_SERVER_URL") {
            Ok(v) => v,
            Err(std::env::VarError::NotPresent) => return Ok(None),
            Err(_) => {
                return Err("OPENCODE_SERVER_URL is set but not valid Unicode".to_string());
            }
        };

        if url.trim().is_empty() {
            return Err("OPENCODE_SERVER_URL is set but empty".to_string());
        }
        let url = url.trim();
        validate_opencode_server_url(url)?;

        let username = match variable("OPENCODE_SERVER_USERNAME") {
            Ok(v) => v,
            Err(_) => {
                return Err(
                    "OPENCODE_SERVER_URL is set but OPENCODE_SERVER_USERNAME is not set"
                        .to_string(),
                );
            }
        };
        if username.trim().is_empty() {
            return Err(
                "OPENCODE_SERVER_URL is set but OPENCODE_SERVER_USERNAME is empty".to_string(),
            );
        }

        let password = match variable("OPENCODE_SERVER_PASSWORD") {
            Ok(v) => v,
            Err(_) => {
                return Err(
                    "OPENCODE_SERVER_URL is set but OPENCODE_SERVER_PASSWORD is not set"
                        .to_string(),
                );
            }
        };
        if password.trim().is_empty() {
            return Err(
                "OPENCODE_SERVER_URL is set but OPENCODE_SERVER_PASSWORD is empty".to_string(),
            );
        }

        Ok(Some(Self {
            server_url: url.to_string(),
            username,
            password,
        }))
    }
}

fn validate_opencode_server_url(value: &str) -> Result<(), String> {
    let url = reqwest::Url::parse(value)
        .map_err(|_| "OPENCODE_SERVER_URL must be a valid HTTP(S) URL".to_string())?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("OPENCODE_SERVER_URL must use http or https".to_string());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("OPENCODE_SERVER_URL must not contain embedded credentials".to_string());
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err("OPENCODE_SERVER_URL must not contain a query or fragment".to_string());
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct GroveConfig {
    pub base_branch: String,
    pub watch_interval_ms: u64,
    pub max_warm_asts: usize,
    pub max_worktrees: usize,
    pub analysis_timeout_ms: u64,
    pub socket_idle_timeout_ms: u64,
    pub socket_state_reply_timeout_ms: u64,
    pub max_file_size_kb: u64,
    pub circuit_breaker_threshold: usize,
    pub ignore_patterns: Vec<String>,
    pub respect_gitignore: bool,
    pub grove_dir: Option<PathBuf>,
    pub opencode: Option<OpenCodeConfig>,
}

impl Default for GroveConfig {
    fn default() -> Self {
        Self {
            base_branch: "main".to_string(),
            watch_interval_ms: 500,
            max_warm_asts: 5000,
            max_worktrees: 20,
            analysis_timeout_ms: 30_000,
            socket_idle_timeout_ms: 300_000,
            socket_state_reply_timeout_ms: 5_000,
            max_file_size_kb: 1024,
            circuit_breaker_threshold: 100,
            ignore_patterns: vec![
                "node_modules".to_string(),
                "target".to_string(),
                "dist".to_string(),
                "build".to_string(),
                ".git/objects".to_string(),
                ".git/lfs".to_string(),
                "*.min.js".to_string(),
                "*.map".to_string(),
            ],
            respect_gitignore: true,
            grove_dir: None,
            opencode: None,
        }
    }
}

// === State Messages ===

pub enum StateMessage {
    Coordinate {
        repository_id: Option<String>,
        command: grove_lib::coordination::CoordCommand,
        operator: bool,
        reply: oneshot::Sender<Result<serde_json::Value, String>>,
    },
    FileChanged {
        workspace_id: WorkspaceId,
        path: PathBuf,
    },
    AnalysisComplete {
        pair: (WorkspaceId, WorkspaceId),
        result: WorkspacePairAnalysis,
    },
    AnalysisFailure {
        pair: (WorkspaceId, WorkspaceId),
    },
    Query {
        request: QueryRequest,
        reply: oneshot::Sender<QueryResponse>,
    },
    BaseRefChanged {
        new_commit: CommitHash,
    },
    BaseGraphReady {
        graph: ImportGraph,
        base_commit: CommitHash,
    },
    WorktreeReindexComplete {
        workspace_id: WorkspaceId,
        overlay: GraphOverlay,
    },
    RegisterWorkspace {
        workspace: Workspace,
        reply: oneshot::Sender<Result<(), String>>,
    },
    RemoveWorkspace {
        workspace_id: WorkspaceId,
        reply: oneshot::Sender<Result<(), String>>,
    },
    SyncWorktrees {
        desired: Vec<Workspace>,
        reply: oneshot::Sender<Result<SyncResult, String>>,
    },
    AttachWorker {
        worker_tx: mpsc::Sender<WorkerMessage>,
    },
    /// Submit one or more session updates from an agent adapter.
    ///
    /// Each update is upserted into the session registry.  Sessions *not*
    /// present in this batch are **not** touched — in particular they are
    /// never automatically transitioned to `Stopped`.
    AgentSessionUpdates {
        updates: Vec<SessionUpdate>,
    },
    /// A single event from the OpenCode event stream.
    ///
    /// The handler translates the provider-neutral [`AgentEvent`] into the
    /// appropriate registry operations according to its [`EventKind`].
    /// Events without a `native_session_id` are silently ignored.
    AgentSessionEvent {
        event: AgentEvent,
    },
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct SyncResult {
    pub workspaces: Vec<Workspace>,
    pub added: Vec<WorkspaceId>,
    pub removed: Vec<WorkspaceId>,
    pub unchanged: Vec<WorkspaceId>,
}

#[derive(Debug, Clone)]
pub enum QueryRequest {
    ListWorkspaces,
    GetWorkspace {
        workspace_id: WorkspaceId,
    },
    GetPairAnalysis {
        workspace_a: WorkspaceId,
        workspace_b: WorkspaceId,
    },
    GetAllAnalyses,
    GetStatus,
    AwaitAnalysis {
        timeout_ms: u64,
    },
    GetFileActivity {
        workspace_id: Option<WorkspaceId>,
        limit: Option<usize>,
    },
    GetAgentAssignments,
    GetAgentSessions {
        provider: Option<AgentProvider>,
        workspace_id: Option<WorkspaceId>,
        lifecycle: Option<SessionLifecycle>,
    },
}

#[derive(Debug, Clone)]
pub enum QueryResponse {
    Workspaces(Vec<Workspace>),
    Workspace(Option<Workspace>),
    PairAnalysis(Option<WorkspacePairAnalysis>),
    AllAnalyses(Vec<WorkspacePairAnalysis>),
    Status {
        workspace_count: usize,
        analysis_count: usize,
        base_commit: CommitHash,
    },
    AwaitAnalysis {
        in_flight: usize,
        analysis_count: usize,
    },
    FileActivity(Vec<FileActivity>),
    AgentAssignments(Vec<grove_lib::AgentAssignment>),
    AgentSessions(Vec<AgentSession>),
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "event", content = "data", rename_all = "snake_case")]
pub enum DaemonEvent {
    AnalysisStarted {
        workspace_a: WorkspaceId,
        workspace_b: WorkspaceId,
    },
    AnalysisComplete {
        workspace_a: WorkspaceId,
        workspace_b: WorkspaceId,
        score: OrthogonalityScore,
    },
    WorkspaceAdded {
        workspace_id: WorkspaceId,
        name: String,
        branch: String,
    },
    WorkspaceRemoved {
        workspace_id: WorkspaceId,
    },
    BaseRefChanged {
        new_commit: CommitHash,
    },
}

// === Daemon State ===

pub struct DaemonState {
    config: GroveConfig,
    workspaces: HashMap<WorkspaceId, Workspace>,
    base_graph: Arc<ImportGraph>,
    base_commit: CommitHash,
    workspace_overlays: HashMap<WorkspaceId, GraphOverlay>,
    pair_analyses: HashMap<(WorkspaceId, WorkspaceId), WorkspacePairAnalysis>,
    dirty_workspaces: Vec<WorkspaceId>,
    in_flight_pairs: HashSet<(WorkspaceId, WorkspaceId)>,
    file_activity: HashMap<WorkspaceId, HashMap<PathBuf, FileActivity>>,
    agent_config: Option<AgentsConfig>,
    analysis_waiters: Vec<oneshot::Sender<QueryResponse>>,
    event_tx: broadcast::Sender<DaemonEvent>,
    worker_tx: Option<mpsc::Sender<WorkerMessage>>,
    db: Option<Database>,
    self_tx: Option<mpsc::Sender<StateMessage>>,
    base_graph_building: bool,
    base_graph_build_failed: bool,
    session_registry: AgentSessionRegistry,
}

impl DaemonState {
    pub fn new(config: GroveConfig, db: Option<Database>) -> Self {
        let (event_tx, _event_rx) = broadcast::channel(256);
        Self::new_with_event_tx(config, db, event_tx)
    }

    pub fn new_with_event_tx(
        config: GroveConfig,
        db: Option<Database>,
        event_tx: broadcast::Sender<DaemonEvent>,
    ) -> Self {
        Self {
            config,
            workspaces: HashMap::new(),
            base_graph: Arc::new(ImportGraph::new()),
            base_commit: String::new(),
            workspace_overlays: HashMap::new(),
            pair_analyses: HashMap::new(),
            dirty_workspaces: Vec::new(),
            in_flight_pairs: HashSet::new(),
            file_activity: HashMap::new(),
            agent_config: None,
            analysis_waiters: Vec::new(),
            event_tx,
            worker_tx: None,
            db,
            self_tx: None,
            base_graph_building: false,
            base_graph_build_failed: false,
            session_registry: AgentSessionRegistry::new(),
        }
    }

    pub fn with_persisted_state(mut self) -> Self {
        if let Some(ref db) = self.db {
            match db.load_workspaces() {
                Ok(workspaces) => {
                    for ws in workspaces {
                        self.workspaces.insert(ws.id, ws);
                    }
                    info!(count = self.workspaces.len(), "loaded workspaces from db");
                }
                Err(e) => {
                    warn!(error = %e, "failed to load workspaces from db");
                }
            }

            match db.load_pair_analyses() {
                Ok(analyses) => {
                    for a in analyses {
                        self.pair_analyses.insert((a.workspace_a, a.workspace_b), a);
                    }
                    info!(
                        count = self.pair_analyses.len(),
                        "loaded pair analyses from db"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "failed to load pair analyses from db");
                }
            }
        }

        // Load agent assignment configuration
        if let Some(ref grove_dir) = self.config.grove_dir {
            match agent_config::load_agents_config(grove_dir) {
                Ok(Some(cfg)) => {
                    info!(count = cfg.agents.len(), "loaded agent assignment config");
                    self.agent_config = Some(cfg);
                }
                Ok(None) => {
                    debug!("no agents.toml found, using fallback assignments");
                }
                Err(e) => {
                    warn!(error = %e, "failed to load agent config");
                }
            }
        }

        // Restore the cached base import graph so that persisted analyses
        // (which may include dependency overlaps) remain valid on restart.
        if let Some(ref db) = self.db {
            match db.load_base_graph_cache() {
                Ok(Some((base_commit, graph))) => {
                    let edge_count: usize = graph.imports.values().map(Vec::len).sum();
                    info!(
                        base_commit = %base_commit,
                        files = graph.exports.len(),
                        edges = edge_count,
                        "restored base import graph from cache"
                    );
                    self.base_graph = Arc::new(graph);
                    self.base_commit = base_commit;
                }
                Ok(None) => {
                    debug!("no cached base graph found");
                }
                Err(e) => {
                    warn!(error = %e, "failed to load base graph cache");
                }
            }
        }

        // If the base graph is still empty after loading, mark all workspaces
        // dirty so the graph build triggers on the first dispatch cycle.
        if self.base_graph.is_empty() && !self.workspaces.is_empty() {
            for workspace_id in self.workspaces.keys().copied() {
                self.dirty_workspaces.push(workspace_id);
            }
        }

        // Sync workspace paths into the session registry and bind any sessions
        // that were persisted but may now match a workspace.
        self.sync_session_registry_workspace_paths();
        self.session_registry.rebind_all_sessions();

        self
    }

    /// Run the actor loop until Shutdown is received.
    ///
    /// Once shutdown starts, the receiver is closed to reject new sends, then
    /// queued messages are drained. Queued queries are still answered so
    /// in-flight request/response paths do not hang while the daemon exits.
    pub async fn run(mut self, mut rx: mpsc::Receiver<StateMessage>) {
        info!("state actor started");

        while let Some(msg) = rx.recv().await {
            let shutdown_requested = match msg {
                StateMessage::FileChanged { workspace_id, path } => {
                    self.handle_file_changed(workspace_id, &path).await;
                    false
                }
                StateMessage::AnalysisComplete { pair, result } => {
                    self.handle_analysis_complete(pair, result);
                    false
                }
                StateMessage::AnalysisFailure { pair } => {
                    self.handle_analysis_failure(pair);
                    false
                }
                StateMessage::Query { request, reply } => {
                    self.handle_query(request, reply);
                    false
                }
                StateMessage::Coordinate {
                    repository_id,
                    command,
                    operator,
                    reply,
                } => {
                    let result = self.coordinate(repository_id.as_deref(), command, operator);
                    let _ = reply.send(result);
                    false
                }
                StateMessage::BaseRefChanged { new_commit } => {
                    self.handle_base_ref_changed(new_commit);
                    false
                }
                StateMessage::BaseGraphReady { graph, base_commit } => {
                    self.handle_base_graph_ready(graph, base_commit);
                    self.dispatch_dirty_workspaces().await;
                    false
                }
                StateMessage::WorktreeReindexComplete {
                    workspace_id,
                    overlay,
                } => {
                    self.handle_worktree_reindex_complete(workspace_id, overlay);
                    false
                }
                StateMessage::RegisterWorkspace { workspace, reply } => {
                    let result = self.handle_register_workspace(workspace);
                    if reply.send(result).is_err() {
                        debug!("register reply channel dropped");
                    }
                    false
                }
                StateMessage::RemoveWorkspace {
                    workspace_id,
                    reply,
                } => {
                    let result = self.handle_remove_workspace(workspace_id);
                    if reply.send(result).is_err() {
                        debug!("remove reply channel dropped");
                    }
                    false
                }
                StateMessage::SyncWorktrees { desired, reply } => {
                    let result = self.handle_sync_worktrees(desired);
                    if reply.send(result).is_err() {
                        debug!("sync reply channel dropped");
                    }
                    self.dispatch_dirty_workspaces().await;
                    false
                }
                StateMessage::AgentSessionUpdates { updates } => {
                    self.handle_agent_session_updates(updates);
                    false
                }
                StateMessage::AgentSessionEvent { event } => {
                    self.handle_agent_session_event(event);
                    false
                }
                StateMessage::AttachWorker { worker_tx } => {
                    self.handle_attach_worker(worker_tx);
                    self.dispatch_dirty_workspaces().await;
                    false
                }
                StateMessage::Shutdown => {
                    info!("state actor shutting down");
                    true
                }
            };

            if shutdown_requested {
                rx.close();
                self.drain_queued_messages(&mut rx).await;
                break;
            }
        }

        info!("state actor stopped");
    }

    async fn drain_queued_messages(&mut self, rx: &mut mpsc::Receiver<StateMessage>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                StateMessage::Query { request, reply } => {
                    self.handle_query_during_shutdown(request, reply);
                }
                StateMessage::RegisterWorkspace { reply, .. } => {
                    if reply
                        .send(Err("daemon is shutting down".to_string()))
                        .is_err()
                    {
                        debug!("register reply channel dropped during shutdown drain");
                    }
                }
                StateMessage::RemoveWorkspace { reply, .. } => {
                    if reply
                        .send(Err("daemon is shutting down".to_string()))
                        .is_err()
                    {
                        debug!("remove reply channel dropped during shutdown drain");
                    }
                }
                StateMessage::SyncWorktrees { reply, .. } => {
                    if reply
                        .send(Err("daemon is shutting down".to_string()))
                        .is_err()
                    {
                        debug!("sync reply channel dropped during shutdown drain");
                    }
                }
                StateMessage::AttachWorker { .. } => {
                    debug!("dropping worker attachment queued after shutdown");
                }
                StateMessage::AgentSessionUpdates { .. } => {
                    debug!("dropping agent session updates queued after shutdown");
                }
                StateMessage::Coordinate { reply, .. } => {
                    let _ = reply.send(Err("daemon is shutting down".into()));
                }
                StateMessage::AgentSessionEvent { .. } => {
                    debug!("dropping agent session event queued after shutdown");
                }
                StateMessage::FileChanged { .. }
                | StateMessage::AnalysisComplete { .. }
                | StateMessage::AnalysisFailure { .. }
                | StateMessage::BaseRefChanged { .. }
                | StateMessage::BaseGraphReady { .. }
                | StateMessage::WorktreeReindexComplete { .. } => {
                    debug!("dropping state mutation queued after shutdown");
                }
                StateMessage::Shutdown => {
                    debug!("dropping duplicate shutdown message");
                }
            }
        }
    }

    // === Message Handlers ===

    fn coordinate(
        &mut self,
        repository_id: Option<&str>,
        mut command: grove_lib::coordination::CoordCommand,
        operator: bool,
    ) -> Result<serde_json::Value, String> {
        use grove_lib::coordination::CoordCommand;
        // Remote metadata is an assertion. Only independently checked local
        // HEAD/worktree metadata gets the local_git label.
        let observation = match &mut command {
            CoordCommand::Register { observation, .. }
            | CoordCommand::Heartbeat { observation, .. } => observation.as_mut(),
            _ => None,
        };
        if let Some(obs) = observation {
            obs.verification = "client_reported".into();
            if self
                .workspaces
                .values()
                .any(|w| w.path.to_string_lossy() == obs.worktree)
                && let Ok(repo) = gix::discover(&obs.worktree)
                && std::fs::read_to_string(repo.git_dir().join("grove-worktree-id"))
                    .is_ok_and(|id| id.trim() == obs.worktree_id)
                && self
                    .config
                    .grove_dir
                    .as_ref()
                    .and_then(|p| p.parent())
                    .and_then(|p| p.canonicalize().ok())
                    == repo.common_dir().canonicalize().ok()
                && repo
                    .head_id()
                    .is_ok_and(|id| id.to_hex().to_string() == obs.commit)
                && repo.head_name().ok().flatten().is_some_and(|name| {
                    name.as_bstr().to_string().strip_prefix("refs/heads/") == Some(&obs.branch)
                })
            {
                obs.verification = "local_git".into();
            }
        }
        let db = self
            .db
            .as_mut()
            .ok_or("durable coordination requires a database")?;
        let sync = matches!(&command, CoordCommand::Sync { .. });
        let mut result = crate::coordination::execute(
            &mut db.conn,
            repository_id,
            command,
            chrono::Utc::now().timestamp_millis(),
            operator,
        )
        .map_err(|e| e.to_string())?;
        if sync {
            result["local_change_analyses"] =
                serde_json::to_value(self.pair_analyses.values().collect::<Vec<_>>())
                    .map_err(|e| e.to_string())?;
        }
        // The legacy registry is a projection of persisted Codex registrations.
        if let Ok(sessions) =
            crate::coordination::sessions(&db.conn, chrono::Utc::now().timestamp_millis())
        {
            for update in sessions.iter().filter_map(crate::codex::normalize) {
                self.session_registry.upsert(update);
            }
        }
        Ok(result)
    }

    async fn handle_file_changed(&mut self, workspace_id: WorkspaceId, path: &Path) {
        if !self.workspaces.contains_key(&workspace_id) {
            warn!(workspace_id = %workspace_id, "file change for unknown workspace");
            return;
        }

        debug!(workspace_id = %workspace_id, path = %path.display(), "file changed");

        // Track file activity: convert absolute watcher path to workspace-relative
        if let Some(workspace) = self.workspaces.get(&workspace_id) {
            match path.strip_prefix(&workspace.path) {
                Ok(relative) if !relative.as_os_str().is_empty() => {
                    let now = Utc::now();
                    let relative = relative.to_path_buf();
                    let attribution = self.attribute_file_change(workspace, now);
                    self.file_activity
                        .entry(workspace_id)
                        .or_default()
                        .entry(relative.clone())
                        .and_modify(|entry| {
                            entry.last_changed_at = now;
                            entry.change_count += 1;
                            entry.attribution = attribution.clone();
                        })
                        .or_insert_with(|| FileActivity {
                            workspace_id,
                            relative_path: relative,
                            first_changed_at: now,
                            last_changed_at: now,
                            change_count: 1,
                            attribution,
                        });
                }
                Ok(_) => {
                    debug!(workspace_id = %workspace_id, "file change at worktree root, ignoring");
                }
                Err(_) => {
                    debug!(
                        workspace_id = %workspace_id,
                        path = %path.display(),
                        worktree = %workspace.path.display(),
                        "file change outside worktree, ignoring"
                    );
                }
            }
        }

        if !self.dirty_workspaces.contains(&workspace_id) {
            self.dirty_workspaces.push(workspace_id);
        }

        self.dispatch_dirty_workspaces().await;
    }

    fn attribute_file_change(
        &self,
        workspace: &Workspace,
        attributed_at: chrono::DateTime<Utc>,
    ) -> FileAttribution {
        let mut sessions: Vec<AgentSessionRef> = self
            .session_registry
            .all()
            .into_iter()
            .filter(|session| {
                session.workspace_id == Some(workspace.id)
                    && session.lifecycle == SessionLifecycle::Active
            })
            .map(|session| AgentSessionRef {
                provider: session.provider,
                native_session_id: session.native_session_id.clone(),
            })
            .collect();
        sessions.sort_by(|a, b| {
            provider_sort_key(a.provider)
                .cmp(&provider_sort_key(b.provider))
                .then_with(|| a.native_session_id.cmp(&b.native_session_id))
        });

        let confidence = match sessions.len() {
            1 => AttributionConfidence::Strong,
            2.. => AttributionConfidence::Ambiguous,
            _ => AttributionConfidence::Unknown,
        };

        if confidence != AttributionConfidence::Unknown {
            return FileAttribution {
                confidence,
                sessions,
                static_agent_name: None,
                attributed_at,
            };
        }

        let static_agent_name =
            agent_config::resolve_explicit_assignment(&self.agent_config, workspace);
        FileAttribution {
            confidence: if static_agent_name.is_some() {
                AttributionConfidence::StaticFallback
            } else {
                AttributionConfidence::Unknown
            },
            sessions,
            static_agent_name,
            attributed_at,
        }
    }

    fn handle_analysis_complete(
        &mut self,
        pair: (WorkspaceId, WorkspaceId),
        result: WorkspacePairAnalysis,
    ) {
        let score = result.score;
        self.in_flight_pairs.remove(&canonical_pair(pair.0, pair.1));

        info!(
            workspace_a = %pair.0,
            workspace_b = %pair.1,
            score = ?result.score,
            overlaps = result.overlaps.len(),
            "analysis complete"
        );

        if let Some(ref db) = self.db
            && let Err(e) = db.save_pair_analysis(&result)
        {
            error!(error = %e, "failed to persist pair analysis");
        }

        self.pair_analyses.insert(pair, result);
        self.emit_event(DaemonEvent::AnalysisComplete {
            workspace_a: pair.0,
            workspace_b: pair.1,
            score,
        });

        if self.in_flight_pairs.is_empty() {
            self.drain_analysis_waiters();
        }
    }

    fn handle_analysis_failure(&mut self, pair: (WorkspaceId, WorkspaceId)) {
        self.in_flight_pairs.remove(&canonical_pair(pair.0, pair.1));

        warn!(
            workspace_a = %pair.0,
            workspace_b = %pair.1,
            "analysis failed; pair removed from in-flight set"
        );

        if self.in_flight_pairs.is_empty() {
            self.drain_analysis_waiters();
        }
    }

    fn handle_attach_worker(&mut self, worker_tx: mpsc::Sender<WorkerMessage>) {
        self.worker_tx = Some(worker_tx);
        info!("worker channel attached to state actor");
    }

    /// Apply a batch of session updates to the registry.
    ///
    /// Each update is upserted individually.  Sessions not present in the batch
    /// are never modified — polling snapshots therefore cannot accidentally mark
    /// sessions as `Stopped`.
    fn handle_agent_session_updates(&mut self, updates: Vec<SessionUpdate>) {
        let count = updates.len();
        for update in updates {
            if update.provider == AgentProvider::Codex
                && self
                    .session_registry
                    .get(update.provider, &update.native_session_id)
                    .is_some_and(|s| update.updated_at.is_some_and(|t| t < s.updated_at))
            {
                continue;
            }
            self.session_registry.upsert(update);
        }
        debug!(count, "applied agent session updates");
    }

    /// Apply a single event-stream event to the session registry.
    ///
    /// Behaviour per [`EventKind`]:
    ///
    /// | Kind | Action |
    /// |---|---|
    /// | `ServerConnected` | no-op |
    /// | `SessionCreated` | upsert with `EventStream` discovery source |
    /// | `SessionUpdated` | update only if session exists; preserve incomplete |
    /// | `MessagePartUpdated` | map status → lifecycle, update `last_seen_at` |
    /// | `MessageUpdated` | update `last_seen_at` only |
    /// | `SessionCompacted` | set `compacted = true`; preserve lifecycle |
    /// | `SessionDeleted` | set lifecycle to `Stopped` |
    ///
    /// Events without a session ID are silently ignored.
    fn handle_agent_session_event(&mut self, event: AgentEvent) {
        let native_session_id = match event.native_session_id {
            Some(ref id) if !id.is_empty() => id.clone(),
            _ => return,
        };

        match event.event_kind {
            EventKind::ServerConnected => {}
            EventKind::SessionCreated => {
                let directory = match event.directory {
                    Some(d) if !d.as_os_str().is_empty() => d,
                    None => return,
                    Some(_) => return,
                };
                self.session_registry.upsert(SessionUpdate {
                    provider: event.provider,
                    native_session_id,
                    parent_native_session_id: event.parent_native_session_id,
                    agent_role: event.agent_role,
                    directory,
                    lifecycle: event.lifecycle.unwrap_or(SessionLifecycle::Discovered),
                    discovery_source: DiscoverySource::EventStream,
                    created_at: event.timestamp,
                    updated_at: event.timestamp,
                });
            }
            EventKind::SessionUpdated => {
                let existing = match self
                    .session_registry
                    .get(event.provider, &native_session_id)
                {
                    Some(s) => s.clone(),
                    None => return,
                };
                self.session_registry.upsert(SessionUpdate {
                    provider: event.provider,
                    native_session_id,
                    parent_native_session_id: event
                        .parent_native_session_id
                        .or(existing.parent_native_session_id),
                    agent_role: event.agent_role.or(existing.agent_role),
                    directory: event.directory.unwrap_or(existing.directory),
                    lifecycle: event.lifecycle.unwrap_or(existing.lifecycle),
                    discovery_source: existing.discovery_source,
                    created_at: None,
                    updated_at: event.timestamp,
                });
            }
            EventKind::MessagePartUpdated => {
                let existing = match self
                    .session_registry
                    .get(event.provider, &native_session_id)
                {
                    Some(s) => s.clone(),
                    None => return,
                };
                let lifecycle = event.lifecycle.unwrap_or(existing.lifecycle);
                self.session_registry.upsert(SessionUpdate {
                    provider: event.provider,
                    native_session_id,
                    parent_native_session_id: existing.parent_native_session_id,
                    agent_role: existing.agent_role,
                    directory: event.directory.unwrap_or(existing.directory),
                    lifecycle,
                    discovery_source: existing.discovery_source,
                    created_at: None,
                    updated_at: event.timestamp,
                });
            }
            EventKind::MessageUpdated => {
                let existing = match self
                    .session_registry
                    .get(event.provider, &native_session_id)
                {
                    Some(s) => s.clone(),
                    None => return,
                };
                self.session_registry.upsert(SessionUpdate {
                    provider: event.provider,
                    native_session_id,
                    parent_native_session_id: existing.parent_native_session_id,
                    agent_role: existing.agent_role,
                    directory: existing.directory,
                    lifecycle: event.lifecycle.unwrap_or(existing.lifecycle),
                    discovery_source: existing.discovery_source,
                    created_at: None,
                    updated_at: event.timestamp,
                });
            }
            EventKind::SessionCompacted => {
                let existing = match self
                    .session_registry
                    .get(event.provider, &native_session_id)
                {
                    Some(s) => s.clone(),
                    None => return,
                };
                let sid = native_session_id.clone();
                self.session_registry.upsert(SessionUpdate {
                    provider: event.provider,
                    native_session_id,
                    parent_native_session_id: existing.parent_native_session_id,
                    agent_role: existing.agent_role,
                    directory: existing.directory,
                    lifecycle: existing.lifecycle,
                    discovery_source: existing.discovery_source,
                    created_at: None,
                    updated_at: event.timestamp,
                });
                self.session_registry.compact(event.provider, &sid);
            }
            EventKind::SessionDeleted => {
                let existing = match self
                    .session_registry
                    .get(event.provider, &native_session_id)
                {
                    Some(s) => s.clone(),
                    None => return,
                };
                self.session_registry.upsert(SessionUpdate {
                    provider: event.provider,
                    native_session_id,
                    parent_native_session_id: existing.parent_native_session_id,
                    agent_role: existing.agent_role,
                    directory: existing.directory,
                    lifecycle: SessionLifecycle::Stopped,
                    discovery_source: existing.discovery_source,
                    created_at: None,
                    updated_at: event.timestamp,
                });
            }
        }
    }

    fn handle_query(&mut self, request: QueryRequest, reply: oneshot::Sender<QueryResponse>) {
        match request {
            QueryRequest::ListWorkspaces => {
                let workspaces: Vec<Workspace> = self.workspaces.values().cloned().collect();
                Self::send_query_reply(reply, QueryResponse::Workspaces(workspaces));
            }
            QueryRequest::GetWorkspace { workspace_id } => {
                Self::send_query_reply(
                    reply,
                    QueryResponse::Workspace(self.workspaces.get(&workspace_id).cloned()),
                );
            }
            QueryRequest::GetPairAnalysis {
                workspace_a,
                workspace_b,
            } => {
                let analysis = self
                    .pair_analyses
                    .get(&(workspace_a, workspace_b))
                    .or_else(|| self.pair_analyses.get(&(workspace_b, workspace_a)))
                    .cloned();
                Self::send_query_reply(reply, QueryResponse::PairAnalysis(analysis));
            }
            QueryRequest::GetAllAnalyses => {
                let analyses: Vec<WorkspacePairAnalysis> =
                    self.pair_analyses.values().cloned().collect();
                Self::send_query_reply(reply, QueryResponse::AllAnalyses(analyses));
            }
            QueryRequest::GetStatus => Self::send_query_reply(
                reply,
                QueryResponse::Status {
                    workspace_count: self.workspaces.len(),
                    analysis_count: self.pair_analyses.len(),
                    base_commit: self.base_commit.clone(),
                },
            ),
            QueryRequest::AwaitAnalysis { timeout_ms } => {
                self.handle_await_analysis_query(timeout_ms, reply);
            }
            QueryRequest::GetFileActivity {
                workspace_id,
                limit,
            } => {
                let mut all = Vec::new();
                match workspace_id {
                    Some(wid) => {
                        if let Some(per_ws) = self.file_activity.get(&wid) {
                            all.extend(per_ws.values().cloned());
                        }
                    }
                    None => {
                        for per_ws in self.file_activity.values() {
                            all.extend(per_ws.values().cloned());
                        }
                    }
                }
                all.sort_by_key(|a| Reverse(a.last_changed_at));
                if let Some(limit) = limit {
                    all.truncate(limit);
                }
                Self::send_query_reply(reply, QueryResponse::FileActivity(all));
            }
            QueryRequest::GetAgentAssignments => {
                let workspaces: Vec<Workspace> = self.workspaces.values().cloned().collect();
                let assignments =
                    agent_config::resolve_assignments(&self.agent_config, &workspaces);
                Self::send_query_reply(reply, QueryResponse::AgentAssignments(assignments));
            }
            QueryRequest::GetAgentSessions {
                provider,
                workspace_id,
                lifecycle,
            } => {
                let mut sessions: Vec<AgentSession> = self
                    .session_registry
                    .all()
                    .into_iter()
                    .filter(|s| {
                        if let Some(ref p) = provider {
                            s.provider == *p
                        } else {
                            true
                        }
                    })
                    .filter(|s| {
                        if let Some(ref wid) = workspace_id {
                            s.workspace_id == Some(*wid)
                        } else {
                            true
                        }
                    })
                    .filter(|s| {
                        if let Some(ref lc) = lifecycle {
                            s.lifecycle == *lc
                        } else {
                            true
                        }
                    })
                    .cloned()
                    .collect();
                sessions.sort_by_key(|s| Reverse(s.updated_at));
                Self::send_query_reply(reply, QueryResponse::AgentSessions(sessions));
            }
        }
    }

    fn handle_query_during_shutdown(
        &mut self,
        request: QueryRequest,
        reply: oneshot::Sender<QueryResponse>,
    ) {
        match request {
            QueryRequest::AwaitAnalysis { .. } => {
                let response = QueryResponse::AwaitAnalysis {
                    in_flight: self.in_flight_pairs.len(),
                    analysis_count: self.pair_analyses.len(),
                };
                Self::send_query_reply(reply, response);
            }
            QueryRequest::ListWorkspaces
            | QueryRequest::GetWorkspace { .. }
            | QueryRequest::GetPairAnalysis { .. }
            | QueryRequest::GetAllAnalyses
            | QueryRequest::GetStatus
            | QueryRequest::GetFileActivity { .. }
            | QueryRequest::GetAgentAssignments
            | QueryRequest::GetAgentSessions { .. } => {
                self.handle_query(request, reply);
            }
        }
    }

    fn handle_await_analysis_query(
        &mut self,
        timeout_ms: u64,
        reply: oneshot::Sender<QueryResponse>,
    ) {
        let in_flight = self.in_flight_pairs.len();
        if in_flight == 0 || timeout_ms == 0 {
            let response = QueryResponse::AwaitAnalysis {
                in_flight,
                analysis_count: self.pair_analyses.len(),
            };
            Self::send_query_reply(reply, response);
            return;
        }

        self.analysis_waiters.push(reply);
    }

    fn drain_analysis_waiters(&mut self) {
        if self.analysis_waiters.is_empty() {
            return;
        }

        let response = QueryResponse::AwaitAnalysis {
            in_flight: 0,
            analysis_count: self.pair_analyses.len(),
        };

        let waiters = std::mem::take(&mut self.analysis_waiters);
        for waiter in waiters {
            Self::send_query_reply(waiter, response.clone());
        }
    }

    fn send_query_reply(reply: oneshot::Sender<QueryResponse>, response: QueryResponse) {
        if reply.send(response).is_err() {
            debug!("query reply channel dropped");
        }
    }

    fn emit_event(&self, event: DaemonEvent) {
        if self.event_tx.send(event).is_err() {
            debug!("dropping daemon event: no active subscribers");
        }
    }

    fn handle_base_ref_changed(&mut self, new_commit: CommitHash) {
        if new_commit == self.base_commit {
            debug!("base ref unchanged, skipping rebuild");
            return;
        }

        info!(
            old_commit = %self.base_commit,
            new_commit = %new_commit,
            "base ref changed, will rebuild base graph"
        );

        self.base_commit = new_commit;
        // Full base graph rebuild is deferred until next dispatch; clear stale graph now.
        self.base_graph = Arc::new(ImportGraph::new());
        self.base_graph_building = false;
        self.base_graph_build_failed = false;
        // Clear stale analyses since they reference the old base.
        self.pair_analyses.clear();
        self.workspace_overlays.clear();
        self.in_flight_pairs.clear();
        self.drain_analysis_waiters();

        if let Some(ref db) = self.db {
            if let Err(e) = db.delete_all_pair_analyses() {
                error!(error = %e, "failed to delete stale pair analyses from db");
            }
            if let Err(e) = db.delete_base_graph_cache() {
                error!(error = %e, "failed to delete stale base graph cache from db");
            }
        }

        for workspace_id in self.workspaces.keys().copied() {
            if !self.dirty_workspaces.contains(&workspace_id) {
                self.dirty_workspaces.push(workspace_id);
            }
        }
        self.emit_event(DaemonEvent::BaseRefChanged {
            new_commit: self.base_commit.clone(),
        });
    }

    fn handle_worktree_reindex_complete(
        &mut self,
        workspace_id: WorkspaceId,
        overlay: GraphOverlay,
    ) {
        info!(workspace_id = %workspace_id, "worktree reindex complete");
        self.workspace_overlays.insert(workspace_id, overlay);
        self.dirty_workspaces.retain(|id| *id != workspace_id);
    }

    fn handle_register_workspace(&mut self, workspace: Workspace) -> Result<(), String> {
        let id = workspace.id;

        // If already registered (same ID), update metadata and return Ok.
        if self.workspaces.contains_key(&id) {
            info!(workspace_id = %id, name = %workspace.name, "workspace already registered, updating");
            if let Some(ref db) = self.db
                && let Err(e) = db.save_workspace(&workspace)
            {
                error!(error = %e, "failed to persist workspace update");
                return Err(format!("persistence error: {e}"));
            }
            self.workspaces.insert(id, workspace);
            self.sync_session_registry_workspace_paths();
            self.session_registry.rebind_all_sessions();
            return Ok(());
        }

        if self.workspaces.len() >= self.config.max_worktrees {
            warn!(
                workspace_id = %id,
                current_count = self.workspaces.len(),
                max_worktrees = self.config.max_worktrees,
                "rejecting workspace registration: maximum worktree limit reached"
            );
            return Err(format!(
                "maximum worktree limit ({}) reached",
                self.config.max_worktrees
            ));
        }

        info!(workspace_id = %id, name = %workspace.name, "registering workspace");

        if let Some(ref db) = self.db
            && let Err(e) = db.save_workspace(&workspace)
        {
            error!(error = %e, "failed to persist workspace");
            return Err(format!("persistence error: {e}"));
        }

        let event = DaemonEvent::WorkspaceAdded {
            workspace_id: id,
            name: workspace.name.clone(),
            branch: workspace.branch.clone(),
        };
        self.workspaces.insert(id, workspace);
        self.sync_session_registry_workspace_paths();
        self.session_registry.rebind_all_sessions();
        self.emit_event(event);
        Ok(())
    }

    fn handle_remove_workspace(&mut self, workspace_id: WorkspaceId) -> Result<(), String> {
        if self.workspaces.remove(&workspace_id).is_none() {
            // Idempotent: removing a non-existent workspace is a no-op.
            debug!(workspace_id = %workspace_id, "remove requested for unknown workspace, ignoring");
            return Ok(());
        }

        info!(workspace_id = %workspace_id, "removing workspace");

        self.workspace_overlays.remove(&workspace_id);
        self.file_activity.remove(&workspace_id);
        self.dirty_workspaces.retain(|id| *id != workspace_id);

        // Remove all pair analyses involving this workspace
        self.pair_analyses
            .retain(|(a, b), _| *a != workspace_id && *b != workspace_id);
        self.in_flight_pairs
            .retain(|(a, b)| *a != workspace_id && *b != workspace_id);

        if let Some(ref db) = self.db
            && let Err(e) = db.delete_workspace(workspace_id)
        {
            error!(error = %e, "failed to delete workspace from db");
        }

        self.sync_session_registry_workspace_paths();
        self.session_registry.rebind_all_sessions();
        self.emit_event(DaemonEvent::WorkspaceRemoved { workspace_id });
        Ok(())
    }

    fn handle_sync_worktrees(&mut self, desired: Vec<Workspace>) -> Result<SyncResult, String> {
        let desired_ids: HashMap<WorkspaceId, Workspace> =
            desired.into_iter().map(|ws| (ws.id, ws)).collect();

        let current_ids: Vec<WorkspaceId> = self.workspaces.keys().cloned().collect();

        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut unchanged = Vec::new();

        // Remove stale workspaces
        for id in &current_ids {
            if !desired_ids.contains_key(id) {
                self.handle_remove_workspace(*id)?;
                removed.push(*id);
            }
        }

        // Add or update desired workspaces
        for (id, ws) in &desired_ids {
            if self.workspaces.contains_key(id) {
                unchanged.push(*id);
            } else {
                added.push(*id);
            }
            self.handle_register_workspace(ws.clone())?;
        }

        // Mark all synced workspaces as dirty so the worker runs initial analysis.
        for id in added.iter().chain(unchanged.iter()) {
            if !self.dirty_workspaces.contains(id) {
                self.dirty_workspaces.push(*id);
            }
        }

        let workspaces: Vec<Workspace> = self.workspaces.values().cloned().collect();

        Ok(SyncResult {
            workspaces,
            added,
            removed,
            unchanged,
        })
    }

    async fn dispatch_dirty_workspaces(&mut self) {
        if self.worker_tx.is_none() {
            debug!("worker not attached yet, retaining dirty workspaces");
            return;
        }

        // Kick off an async base graph build if needed. Dispatch is deferred
        // until the build completes so workers receive the populated graph.
        // If the build previously failed (e.g. fake paths in tests), proceed
        // with an empty graph — dependency overlaps will be unavailable but
        // all other analysis layers still work.
        if self.base_graph.is_empty()
            && !self.base_graph_building
            && !self.base_graph_build_failed
            && self.self_tx.is_some()
        {
            self.spawn_base_graph_build();
            return;
        }

        if self.base_graph_building {
            return;
        }

        let dirty = self.dirty_workspaces.clone();
        let mut still_dirty = Vec::new();

        for workspace_id in dirty {
            if self
                .dispatch_pairs_for_workspace(workspace_id)
                .await
                .is_err()
            {
                still_dirty.push(workspace_id);
            }
        }

        self.dirty_workspaces = still_dirty;
    }

    fn spawn_base_graph_build(&mut self) {
        let workspace = self.workspaces.values().next().cloned();
        let Some(workspace) = workspace else {
            return;
        };
        let Some(ref self_tx) = self.self_tx else {
            // No self_tx means we're in a direct test context; skip async build.
            return;
        };

        self.base_graph_building = true;
        let config = self.config.clone();
        let self_tx = self_tx.clone();
        let workspace_id = workspace.id;

        tokio::task::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                build_base_graph_from_workspace(&config, &workspace)
            })
            .await;

            let (graph, base_commit) = match result {
                Ok(Ok(pair)) => pair,
                Ok(Err(e)) => {
                    warn!(
                        workspace_id = %workspace_id,
                        error = %e,
                        "failed to build base import graph; dependency overlaps may be incomplete"
                    );
                    (ImportGraph::new(), String::new())
                }
                Err(e) => {
                    error!(
                        workspace_id = %workspace_id,
                        error = %e,
                        "base graph build task panicked"
                    );
                    (ImportGraph::new(), String::new())
                }
            };

            if let Err(e) = self_tx
                .send(StateMessage::BaseGraphReady { graph, base_commit })
                .await
            {
                warn!(
                    workspace_id = %workspace_id,
                    error = %e,
                    "failed to send base graph ready message"
                );
            }
        });
    }

    fn handle_base_graph_ready(&mut self, graph: ImportGraph, base_commit: CommitHash) {
        self.base_graph_building = false;

        if graph.is_empty() {
            self.base_graph_build_failed = true;
            debug!("base graph build returned empty graph; dependency overlaps unavailable");
            return;
        }

        let edge_count: usize = graph.imports.values().map(Vec::len).sum();
        info!(
            base_commit = %base_commit,
            files = graph.exports.len(),
            edges = edge_count,
            "base import graph ready"
        );

        self.base_graph = Arc::new(graph);
        self.base_commit = base_commit.clone();

        if let Some(ref db) = self.db
            && let Err(e) = db.save_base_graph_cache(&base_commit, &self.base_graph)
        {
            error!(error = %e, "failed to persist base graph cache");
        }

        // Re-mark all workspaces dirty so they re-analyze with the populated graph,
        // picking up dependency overlaps that the initial pass missed.
        for workspace_id in self.workspaces.keys().copied() {
            if !self.dirty_workspaces.contains(&workspace_id) {
                self.dirty_workspaces.push(workspace_id);
            }
        }
    }

    async fn dispatch_pairs_for_workspace(&mut self, workspace_id: WorkspaceId) -> Result<(), ()> {
        let Some(worker_tx) = self.worker_tx.clone() else {
            return Err(());
        };

        let pair_targets: Vec<WorkspaceId> = self
            .workspaces
            .keys()
            .copied()
            .filter(|id| *id != workspace_id)
            .collect();

        for other_id in pair_targets {
            let pair = canonical_pair(workspace_id, other_id);
            if self.in_flight_pairs.contains(&pair) {
                continue;
            }

            let Some(workspace_a) = self.workspaces.get(&pair.0).cloned() else {
                continue;
            };
            let Some(workspace_b) = self.workspaces.get(&pair.1).cloned() else {
                continue;
            };

            self.in_flight_pairs.insert(pair);
            let send_result = tokio::time::timeout(
                Duration::from_millis(self.config.analysis_timeout_ms),
                worker_tx.send(WorkerMessage::AnalyzePair {
                    workspace_a,
                    workspace_b,
                    base_graph: self.base_graph.clone(),
                }),
            )
            .await;

            match send_result {
                Ok(Ok(())) => {
                    self.emit_event(DaemonEvent::AnalysisStarted {
                        workspace_a: pair.0,
                        workspace_b: pair.1,
                    });
                }
                Ok(Err(e)) => {
                    self.in_flight_pairs.remove(&pair);
                    warn!(
                        workspace_a = %pair.0,
                        workspace_b = %pair.1,
                        error = %e,
                        "worker channel closed while dispatching analysis pair"
                    );
                    return Err(());
                }
                Err(_) => {
                    self.in_flight_pairs.remove(&pair);
                    warn!(
                        workspace_a = %pair.0,
                        workspace_b = %pair.1,
                        timeout_ms = self.config.analysis_timeout_ms,
                        "timed out dispatching analysis pair to worker queue"
                    );
                    return Err(());
                }
            }

            debug!(
                workspace_a = %pair.0,
                workspace_b = %pair.1,
                "dispatched analysis pair to workers"
            );
        }

        Ok(())
    }

    // ── Workspace path synchronisation ─────────────────────────────────

    /// Copy the current workspace paths into the session registry's workspace
    /// path index so that [`rebind_all_sessions`] produces correct bindings.
    fn sync_session_registry_workspace_paths(&mut self) {
        let paths: HashMap<WorkspaceId, PathBuf> = self
            .workspaces
            .iter()
            .map(|(id, ws)| (*id, ws.path.clone()))
            .collect();
        self.session_registry.set_workspace_paths(paths);
    }

    // === Accessors (for testing) ===

    pub fn workspace_count(&self) -> usize {
        self.workspaces.len()
    }

    pub fn analysis_count(&self) -> usize {
        self.pair_analyses.len()
    }

    pub fn base_commit(&self) -> &str {
        &self.base_commit
    }

    pub fn dirty_workspaces(&self) -> &[WorkspaceId] {
        &self.dirty_workspaces
    }

    pub fn session_registry(&self) -> &AgentSessionRegistry {
        &self.session_registry
    }
}

fn provider_sort_key(provider: AgentProvider) -> u8 {
    match provider {
        AgentProvider::OpenCode => 0,
        AgentProvider::ClaudeCode => 1,
        AgentProvider::Codex => 2,
        AgentProvider::Unknown => 3,
    }
}

/// Create a state actor and return the sender handle.
pub fn spawn_state_actor(
    config: GroveConfig,
    db: Option<Database>,
) -> (
    mpsc::Sender<StateMessage>,
    broadcast::Sender<DaemonEvent>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = mpsc::channel(256);
    let (event_tx, _event_rx) = broadcast::channel(256);
    let mut state =
        DaemonState::new_with_event_tx(config, db, event_tx.clone()).with_persisted_state();
    state.self_tx = Some(tx.clone());
    let handle = tokio::spawn(state.run(rx));
    (tx, event_tx, handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::WorkerMessage;
    use chrono::Utc;
    use grove_lib::{MergeOrder, OrthogonalityScore, WorkspaceMetadata};
    use std::process::Command;
    use tempfile::tempdir;
    use tokio::time::{Duration, timeout};
    use uuid::Uuid;

    fn make_workspace(name: &str) -> Workspace {
        Workspace {
            id: Uuid::new_v4(),
            name: name.to_string(),
            branch: format!("feat/{name}"),
            path: PathBuf::from(format!("/worktrees/{name}")),
            base_ref: "main".to_string(),
            created_at: Utc::now(),
            last_activity: Utc::now(),
            metadata: WorkspaceMetadata::default(),
        }
    }

    fn make_workspace_at_path(name: &str, branch: &str, path: PathBuf) -> Workspace {
        Workspace {
            id: Uuid::new_v4(),
            name: name.to_string(),
            branch: branch.to_string(),
            path,
            base_ref: "main".to_string(),
            created_at: Utc::now(),
            last_activity: Utc::now(),
            metadata: WorkspaceMetadata::default(),
        }
    }

    fn run_git(repo: &Path, args: &[&str]) {
        let output = Command::new("git")
            .current_dir(repo)
            .args(args)
            .output()
            .expect("git command should run");
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn write_file(path: &Path, content: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("parent dirs should be created");
        }
        std::fs::write(path, content).expect("file should be written");
    }

    #[tokio::test]
    async fn register_and_query_workspace() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);

        let ws = make_workspace("auth-refactor");
        let ws_id = ws.id;

        // Register
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::RegisterWorkspace {
            workspace: ws,
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert!(reply_rx.await.unwrap().is_ok());

        // Query
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::GetWorkspace {
                workspace_id: ws_id,
            },
            reply: reply_tx,
        })
        .await
        .unwrap();

        match reply_rx.await.unwrap() {
            QueryResponse::Workspace(Some(ws)) => {
                assert_eq!(ws.name, "auth-refactor");
            }
            other => panic!("unexpected response: {other:?}"),
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn list_workspaces() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);

        for name in &["alpha", "beta", "gamma"] {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(StateMessage::RegisterWorkspace {
                workspace: make_workspace(name),
                reply: reply_tx,
            })
            .await
            .unwrap();
            reply_rx.await.unwrap().unwrap();
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::ListWorkspaces,
            reply: reply_tx,
        })
        .await
        .unwrap();

        match reply_rx.await.unwrap() {
            QueryResponse::Workspaces(ws) => assert_eq!(ws.len(), 3),
            other => panic!("unexpected response: {other:?}"),
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn remove_workspace_cleans_up() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);

        let ws = make_workspace("to-remove");
        let ws_id = ws.id;

        // Register
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::RegisterWorkspace {
            workspace: ws,
            reply: reply_tx,
        })
        .await
        .unwrap();
        reply_rx.await.unwrap().unwrap();

        // Remove
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::RemoveWorkspace {
            workspace_id: ws_id,
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert!(reply_rx.await.unwrap().is_ok());

        // Verify gone
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::GetWorkspace {
                workspace_id: ws_id,
            },
            reply: reply_tx,
        })
        .await
        .unwrap();

        match reply_rx.await.unwrap() {
            QueryResponse::Workspace(None) => {}
            other => panic!("expected None, got: {other:?}"),
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn file_changed_marks_dirty() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);

        let ws = make_workspace("active");
        let ws_id = ws.id;

        // Register
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::RegisterWorkspace {
            workspace: ws,
            reply: reply_tx,
        })
        .await
        .unwrap();
        reply_rx.await.unwrap().unwrap();

        // File change
        tx.send(StateMessage::FileChanged {
            workspace_id: ws_id,
            path: PathBuf::from("src/main.rs"),
        })
        .await
        .unwrap();

        // Query status to confirm actor processed the message
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::GetStatus,
            reply: reply_tx,
        })
        .await
        .unwrap();

        match reply_rx.await.unwrap() {
            QueryResponse::Status {
                workspace_count, ..
            } => assert_eq!(workspace_count, 1),
            other => panic!("unexpected: {other:?}"),
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn analysis_complete_stores_result() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);

        let ws_a = make_workspace("a");
        let ws_b = make_workspace("b");
        let id_a = ws_a.id;
        let id_b = ws_b.id;

        // Register both
        for ws in [ws_a, ws_b] {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(StateMessage::RegisterWorkspace {
                workspace: ws,
                reply: reply_tx,
            })
            .await
            .unwrap();
            reply_rx.await.unwrap().unwrap();
        }

        // Send analysis result
        let analysis = WorkspacePairAnalysis {
            workspace_a: id_a,
            workspace_b: id_b,
            score: OrthogonalityScore::Yellow,
            overlaps: vec![],
            merge_order_hint: MergeOrder::Either,
            last_computed: Utc::now(),
        };
        tx.send(StateMessage::AnalysisComplete {
            pair: (id_a, id_b),
            result: analysis,
        })
        .await
        .unwrap();

        // Query it back
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::GetPairAnalysis {
                workspace_a: id_a,
                workspace_b: id_b,
            },
            reply: reply_tx,
        })
        .await
        .unwrap();

        match reply_rx.await.unwrap() {
            QueryResponse::PairAnalysis(Some(a)) => {
                assert_eq!(a.score, OrthogonalityScore::Yellow);
            }
            other => panic!("expected analysis, got: {other:?}"),
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn await_analysis_returns_completed_immediately_when_idle() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::AwaitAnalysis { timeout_ms: 30_000 },
            reply: reply_tx,
        })
        .await
        .unwrap();

        match reply_rx.await.unwrap() {
            QueryResponse::AwaitAnalysis {
                in_flight,
                analysis_count,
            } => {
                assert_eq!(in_flight, 0);
                assert_eq!(analysis_count, 0);
            }
            other => panic!("unexpected response: {other:?}"),
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn await_analysis_waiter_completes_when_in_flight_pair_finishes() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);
        let ws_a = make_workspace("await-a");
        let ws_b = make_workspace("await-b");
        let id_a = ws_a.id;

        for ws in [ws_a, ws_b] {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(StateMessage::RegisterWorkspace {
                workspace: ws,
                reply: reply_tx,
            })
            .await
            .unwrap();
            reply_rx.await.unwrap().unwrap();
        }

        let (worker_tx, mut worker_rx) = mpsc::channel(8);
        tx.send(StateMessage::AttachWorker { worker_tx })
            .await
            .unwrap();

        tx.send(StateMessage::FileChanged {
            workspace_id: id_a,
            path: PathBuf::from("src/lib.rs"),
        })
        .await
        .unwrap();

        let pair = match timeout(Duration::from_secs(1), worker_rx.recv())
            .await
            .expect("analysis dispatch should happen")
            .expect("worker should receive analysis message")
        {
            WorkerMessage::AnalyzePair {
                workspace_a,
                workspace_b,
                ..
            } => (workspace_a.id, workspace_b.id),
        };

        let (await_reply_tx, await_reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::AwaitAnalysis { timeout_ms: 30_000 },
            reply: await_reply_tx,
        })
        .await
        .unwrap();

        let mut await_reply_rx = await_reply_rx;
        assert!(
            timeout(Duration::from_millis(50), &mut await_reply_rx)
                .await
                .is_err()
        );

        tx.send(StateMessage::AnalysisComplete {
            pair,
            result: WorkspacePairAnalysis {
                workspace_a: pair.0,
                workspace_b: pair.1,
                score: OrthogonalityScore::Green,
                overlaps: vec![],
                merge_order_hint: MergeOrder::Either,
                last_computed: Utc::now(),
            },
        })
        .await
        .unwrap();

        match timeout(Duration::from_secs(1), &mut await_reply_rx)
            .await
            .expect("await_analysis reply should arrive")
            .unwrap()
        {
            QueryResponse::AwaitAnalysis {
                in_flight,
                analysis_count,
            } => {
                assert_eq!(in_flight, 0);
                assert_eq!(analysis_count, 1);
            }
            other => panic!("unexpected response: {other:?}"),
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn base_ref_change_clears_analyses() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);

        let ws_a = make_workspace("a");
        let ws_b = make_workspace("b");
        let id_a = ws_a.id;
        let id_b = ws_b.id;

        for ws in [ws_a, ws_b] {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(StateMessage::RegisterWorkspace {
                workspace: ws,
                reply: reply_tx,
            })
            .await
            .unwrap();
            reply_rx.await.unwrap().unwrap();
        }

        // Add an analysis
        tx.send(StateMessage::AnalysisComplete {
            pair: (id_a, id_b),
            result: WorkspacePairAnalysis {
                workspace_a: id_a,
                workspace_b: id_b,
                score: OrthogonalityScore::Green,
                overlaps: vec![],
                merge_order_hint: MergeOrder::Either,
                last_computed: Utc::now(),
            },
        })
        .await
        .unwrap();

        // Base ref changes → analyses cleared
        tx.send(StateMessage::BaseRefChanged {
            new_commit: "new-abc123".to_string(),
        })
        .await
        .unwrap();

        // Query — should be gone
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::GetAllAnalyses,
            reply: reply_tx,
        })
        .await
        .unwrap();

        match reply_rx.await.unwrap() {
            QueryResponse::AllAnalyses(analyses) => assert!(analyses.is_empty()),
            other => panic!("unexpected: {other:?}"),
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn max_worktrees_enforced() {
        let config = GroveConfig {
            max_worktrees: 2,
            ..GroveConfig::default()
        };
        let (tx, _event_tx, handle) = spawn_state_actor(config, None);

        for name in &["a", "b"] {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(StateMessage::RegisterWorkspace {
                workspace: make_workspace(name),
                reply: reply_tx,
            })
            .await
            .unwrap();
            reply_rx.await.unwrap().unwrap();
        }

        // Third should fail
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::RegisterWorkspace {
            workspace: make_workspace("c"),
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert!(reply_rx.await.unwrap().is_err());

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn rapid_register_and_query_all_responses_arrive() {
        let config = GroveConfig {
            max_worktrees: 200,
            ..GroveConfig::default()
        };
        let (tx, _event_tx, handle) = spawn_state_actor(config, None);

        let mut tasks = Vec::new();
        for i in 0..100 {
            let tx_cloned = tx.clone();
            tasks.push(tokio::spawn(async move {
                let ws = make_workspace(&format!("ws-{i:03}"));
                let ws_id = ws.id;

                let (reg_reply_tx, reg_reply_rx) = oneshot::channel();
                tx_cloned
                    .send(StateMessage::RegisterWorkspace {
                        workspace: ws,
                        reply: reg_reply_tx,
                    })
                    .await
                    .unwrap();
                assert!(reg_reply_rx.await.unwrap().is_ok());

                let (query_reply_tx, query_reply_rx) = oneshot::channel();
                tx_cloned
                    .send(StateMessage::Query {
                        request: QueryRequest::GetWorkspace {
                            workspace_id: ws_id,
                        },
                        reply: query_reply_tx,
                    })
                    .await
                    .unwrap();

                match query_reply_rx.await.unwrap() {
                    QueryResponse::Workspace(Some(found)) => assert_eq!(found.id, ws_id),
                    other => panic!("unexpected query response: {other:?}"),
                }
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::ListWorkspaces,
            reply: reply_tx,
        })
        .await
        .unwrap();
        match reply_rx.await.unwrap() {
            QueryResponse::Workspaces(workspaces) => assert_eq!(workspaces.len(), 100),
            other => panic!("unexpected response: {other:?}"),
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn register_then_remove_same_workspace_immediately() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);
        let ws = make_workspace("quick-remove");
        let ws_id = ws.id;

        let (reg_reply_tx, reg_reply_rx) = oneshot::channel();
        tx.send(StateMessage::RegisterWorkspace {
            workspace: ws,
            reply: reg_reply_tx,
        })
        .await
        .unwrap();

        let (remove_reply_tx, remove_reply_rx) = oneshot::channel();
        tx.send(StateMessage::RemoveWorkspace {
            workspace_id: ws_id,
            reply: remove_reply_tx,
        })
        .await
        .unwrap();

        assert!(reg_reply_rx.await.unwrap().is_ok());
        assert!(remove_reply_rx.await.unwrap().is_ok());

        let (query_reply_tx, query_reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::GetWorkspace {
                workspace_id: ws_id,
            },
            reply: query_reply_tx,
        })
        .await
        .unwrap();
        assert!(matches!(
            query_reply_rx.await.unwrap(),
            QueryResponse::Workspace(None)
        ));

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn query_after_remove_message_returns_none() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);
        let ws = make_workspace("remove-query");
        let ws_id = ws.id;

        let (reg_reply_tx, reg_reply_rx) = oneshot::channel();
        tx.send(StateMessage::RegisterWorkspace {
            workspace: ws,
            reply: reg_reply_tx,
        })
        .await
        .unwrap();
        reg_reply_rx.await.unwrap().unwrap();

        let (remove_reply_tx, remove_reply_rx) = oneshot::channel();
        tx.send(StateMessage::RemoveWorkspace {
            workspace_id: ws_id,
            reply: remove_reply_tx,
        })
        .await
        .unwrap();
        remove_reply_rx.await.unwrap().unwrap();

        let (query_reply_tx, query_reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::GetWorkspace {
                workspace_id: ws_id,
            },
            reply: query_reply_tx,
        })
        .await
        .unwrap();
        assert!(matches!(
            query_reply_rx.await.unwrap(),
            QueryResponse::Workspace(None)
        ));

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn get_pair_analysis_query_is_order_insensitive() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);
        let ws_a = make_workspace("pair-a");
        let ws_b = make_workspace("pair-b");
        let id_a = ws_a.id;
        let id_b = ws_b.id;

        for ws in [ws_a, ws_b] {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(StateMessage::RegisterWorkspace {
                workspace: ws,
                reply: reply_tx,
            })
            .await
            .unwrap();
            reply_rx.await.unwrap().unwrap();
        }

        let analysis = WorkspacePairAnalysis {
            workspace_a: id_a,
            workspace_b: id_b,
            score: OrthogonalityScore::Red,
            overlaps: vec![],
            merge_order_hint: MergeOrder::NeedsCoordination,
            last_computed: Utc::now(),
        };
        tx.send(StateMessage::AnalysisComplete {
            pair: (id_a, id_b),
            result: analysis,
        })
        .await
        .unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::GetPairAnalysis {
                workspace_a: id_b,
                workspace_b: id_a,
            },
            reply: reply_tx,
        })
        .await
        .unwrap();

        match reply_rx.await.unwrap() {
            QueryResponse::PairAnalysis(Some(found)) => {
                assert_eq!(found.workspace_a, id_a);
                assert_eq!(found.workspace_b, id_b);
                assert_eq!(found.score, OrthogonalityScore::Red);
            }
            other => panic!("unexpected response: {other:?}"),
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn file_changed_dispatches_analysis_for_related_pairs() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);
        let ws_a = make_workspace("dispatch-a");
        let ws_b = make_workspace("dispatch-b");
        let id_a = ws_a.id;
        let id_b = ws_b.id;

        for ws in [ws_a, ws_b] {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(StateMessage::RegisterWorkspace {
                workspace: ws,
                reply: reply_tx,
            })
            .await
            .unwrap();
            reply_rx.await.unwrap().unwrap();
        }

        let (worker_tx, mut worker_rx) = mpsc::channel(8);
        tx.send(StateMessage::AttachWorker { worker_tx })
            .await
            .unwrap();

        tx.send(StateMessage::FileChanged {
            workspace_id: id_a,
            path: PathBuf::from("src/lib.rs"),
        })
        .await
        .unwrap();

        let dispatched = timeout(Duration::from_secs(1), worker_rx.recv())
            .await
            .expect("dispatch should happen before timeout")
            .expect("worker queue should receive a message");

        match dispatched {
            WorkerMessage::AnalyzePair {
                workspace_a,
                workspace_b,
                ..
            } => {
                let expected = canonical_pair(id_a, id_b);
                assert_eq!((workspace_a.id, workspace_b.id), expected);
            }
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn dispatch_timeout_retains_dirty_workspace_and_clears_in_flight_pair() {
        let mut state = DaemonState::new(
            GroveConfig {
                analysis_timeout_ms: 10,
                ..GroveConfig::default()
            },
            None,
        );
        let ws_a = make_workspace("timeout-a");
        let ws_b = make_workspace("timeout-b");
        let id_a = ws_a.id;
        let id_b = ws_b.id;

        state.handle_register_workspace(ws_a.clone()).unwrap();
        state.handle_register_workspace(ws_b.clone()).unwrap();

        let (worker_tx, _worker_rx) = mpsc::channel(1);
        worker_tx
            .try_send(WorkerMessage::AnalyzePair {
                workspace_a: ws_a,
                workspace_b: ws_b,
                base_graph: Arc::new(ImportGraph::new()),
            })
            .unwrap();
        state.handle_attach_worker(worker_tx);

        state.dirty_workspaces.push(id_a);
        state.dispatch_dirty_workspaces().await;

        assert_eq!(state.dirty_workspaces, vec![id_a]);
        assert!(!state.in_flight_pairs.contains(&canonical_pair(id_a, id_b)));
    }

    #[tokio::test]
    async fn dispatch_closed_worker_channel_retains_dirty_workspace_and_clears_in_flight_pair() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let ws_a = make_workspace("closed-a");
        let ws_b = make_workspace("closed-b");
        let id_a = ws_a.id;
        let id_b = ws_b.id;

        state.handle_register_workspace(ws_a).unwrap();
        state.handle_register_workspace(ws_b).unwrap();

        let (worker_tx, worker_rx) = mpsc::channel(1);
        drop(worker_rx);
        state.handle_attach_worker(worker_tx);

        state.dirty_workspaces.push(id_a);
        state.dispatch_dirty_workspaces().await;

        assert_eq!(state.dirty_workspaces, vec![id_a]);
        assert!(!state.in_flight_pairs.contains(&canonical_pair(id_a, id_b)));
    }

    #[tokio::test]
    async fn duplicate_file_changed_messages_do_not_duplicate_dirty_workspace() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let ws = make_workspace("dup-dirty");
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();

        state
            .handle_file_changed(ws_id, Path::new("src/a.rs"))
            .await;
        state
            .handle_file_changed(ws_id, Path::new("src/b.rs"))
            .await;
        state
            .handle_file_changed(ws_id, Path::new("src/c.rs"))
            .await;

        assert_eq!(state.dirty_workspaces().len(), 1);
        assert_eq!(state.dirty_workspaces()[0], ws_id);
    }

    #[tokio::test]
    async fn file_changed_for_unknown_workspace_is_ignored() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        state
            .handle_file_changed(Uuid::new_v4(), Path::new("src/ghost.rs"))
            .await;
        assert!(state.dirty_workspaces().is_empty());
    }

    #[test]
    fn removing_unknown_workspace_is_noop_without_side_effects() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        state
            .handle_register_workspace(make_workspace("still-here"))
            .unwrap();
        assert_eq!(state.workspace_count(), 1);

        // Removing unknown workspace should succeed as a no-op
        let result = state.handle_remove_workspace(Uuid::new_v4());
        assert!(result.is_ok());
        assert_eq!(state.workspace_count(), 1);
    }

    #[test]
    fn base_ref_same_commit_does_not_clear_analyses() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let id_a = Uuid::new_v4();
        let id_b = Uuid::new_v4();

        state.handle_base_ref_changed("base-123".to_string());
        state.handle_analysis_complete(
            (id_a, id_b),
            WorkspacePairAnalysis {
                workspace_a: id_a,
                workspace_b: id_b,
                score: OrthogonalityScore::Yellow,
                overlaps: vec![],
                merge_order_hint: MergeOrder::Either,
                last_computed: Utc::now(),
            },
        );
        assert_eq!(state.analysis_count(), 1);

        state.handle_base_ref_changed("base-123".to_string());
        assert_eq!(state.analysis_count(), 1);
        assert_eq!(state.base_commit(), "base-123");
    }

    #[tokio::test]
    async fn dropped_query_reply_channel_does_not_break_actor() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);

        let (dropped_reply_tx, dropped_reply_rx) = oneshot::channel();
        drop(dropped_reply_rx);
        tx.send(StateMessage::Query {
            request: QueryRequest::GetStatus,
            reply: dropped_reply_tx,
        })
        .await
        .unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::GetStatus,
            reply: reply_tx,
        })
        .await
        .unwrap();
        match reply_rx.await.unwrap() {
            QueryResponse::Status {
                workspace_count,
                analysis_count,
                ..
            } => {
                assert_eq!(workspace_count, 0);
                assert_eq!(analysis_count, 0);
            }
            other => panic!("unexpected response: {other:?}"),
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_file_change_burst_keeps_actor_responsive() {
        let (tx, _event_tx, handle) = spawn_state_actor(
            GroveConfig {
                max_worktrees: 5,
                ..GroveConfig::default()
            },
            None,
        );
        let ws = make_workspace("burst");
        let ws_id = ws.id;

        let (register_reply_tx, register_reply_rx) = oneshot::channel();
        tx.send(StateMessage::RegisterWorkspace {
            workspace: ws,
            reply: register_reply_tx,
        })
        .await
        .unwrap();
        register_reply_rx.await.unwrap().unwrap();

        let mut tasks = Vec::new();
        for i in 0..256 {
            let tx_cloned = tx.clone();
            tasks.push(tokio::spawn(async move {
                tx_cloned
                    .send(StateMessage::FileChanged {
                        workspace_id: ws_id,
                        path: PathBuf::from(format!("src/{i}.rs")),
                    })
                    .await
                    .unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::GetStatus,
            reply: reply_tx,
        })
        .await
        .unwrap();
        match reply_rx.await.unwrap() {
            QueryResponse::Status {
                workspace_count,
                analysis_count,
                ..
            } => {
                assert_eq!(workspace_count, 1);
                assert_eq!(analysis_count, 0);
            }
            other => panic!("unexpected response: {other:?}"),
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn register_existing_workspace_is_idempotent() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);

        let ws = make_workspace("idempotent");
        let _ws_id = ws.id;

        // Register first time
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::RegisterWorkspace {
            workspace: ws.clone(),
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert!(reply_rx.await.unwrap().is_ok());

        // Register same workspace again — should succeed, not error
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::RegisterWorkspace {
            workspace: ws,
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert!(reply_rx.await.unwrap().is_ok());

        // Should still have exactly 1 workspace
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::GetStatus,
            reply: reply_tx,
        })
        .await
        .unwrap();
        match reply_rx.await.unwrap() {
            QueryResponse::Status {
                workspace_count, ..
            } => assert_eq!(workspace_count, 1),
            other => panic!("unexpected: {other:?}"),
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn remove_nonexistent_workspace_is_noop() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::RemoveWorkspace {
            workspace_id: Uuid::new_v4(),
            reply: reply_tx,
        })
        .await
        .unwrap();
        // Should succeed (no-op), not error
        assert!(reply_rx.await.unwrap().is_ok());

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn queued_query_after_shutdown_is_drained_and_replied() {
        let (tx, rx) = mpsc::channel(8);
        let state = DaemonState::new(GroveConfig::default(), None);

        let (query_reply_tx, query_reply_rx) = oneshot::channel();

        tx.try_send(StateMessage::Shutdown).unwrap();
        tx.try_send(StateMessage::Query {
            request: QueryRequest::GetStatus,
            reply: query_reply_tx,
        })
        .unwrap();
        drop(tx);

        state.run(rx).await;

        match query_reply_rx.await.unwrap() {
            QueryResponse::Status {
                workspace_count,
                analysis_count,
                base_commit,
            } => {
                assert_eq!(workspace_count, 0);
                assert_eq!(analysis_count, 0);
                assert_eq!(base_commit, "");
            }
            other => panic!("expected status response, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn sync_worktrees_adds_new_and_removes_stale() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);

        // Register two workspaces
        let ws_a = make_workspace("alpha");
        let ws_b = make_workspace("beta");
        let id_a = ws_a.id;
        let id_b = ws_b.id;
        for ws in [ws_a.clone(), ws_b.clone()] {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(StateMessage::RegisterWorkspace {
                workspace: ws,
                reply: reply_tx,
            })
            .await
            .unwrap();
            reply_rx.await.unwrap().unwrap();
        }

        // Sync with only alpha + a new gamma — beta should be removed
        let ws_gamma = make_workspace("gamma");
        let id_gamma = ws_gamma.id;
        let desired = vec![ws_a, ws_gamma];

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::SyncWorktrees {
            desired,
            reply: reply_tx,
        })
        .await
        .unwrap();
        let result = reply_rx.await.unwrap().unwrap();

        assert!(result.added.contains(&id_gamma));
        assert!(result.removed.contains(&id_b));
        assert!(result.unchanged.contains(&id_a));

        // Verify state: 2 workspaces (alpha + gamma)
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::GetStatus,
            reply: reply_tx,
        })
        .await
        .unwrap();
        match reply_rx.await.unwrap() {
            QueryResponse::Status {
                workspace_count, ..
            } => assert_eq!(workspace_count, 2),
            other => panic!("unexpected: {other:?}"),
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn dispatch_rebuilds_base_graph_for_worker_messages() {
        let temp = tempdir().expect("temp dir should be created");
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).expect("repo dir should be created");

        run_git(&repo, &["init", "-b", "main"]);
        run_git(&repo, &["config", "user.email", "grove@example.com"]);
        run_git(&repo, &["config", "user.name", "Grove Tests"]);

        write_file(
            &repo.join("src/shared.ts"),
            "export function authenticate(token: string): boolean { return token.length > 0; }\n",
        );
        write_file(
            &repo.join("src/api.ts"),
            "import { authenticate } from \"./shared\";\nexport function checkout(token: string): boolean { return authenticate(token); }\n",
        );
        run_git(&repo, &["add", "."]);
        run_git(&repo, &["commit", "-m", "initial"]);

        run_git(&repo, &["checkout", "-b", "feat/a"]);
        write_file(
            &repo.join("src/shared.ts"),
            "export function authenticate(token: string): boolean { return token.length > 1; }\n",
        );
        run_git(&repo, &["commit", "-am", "feat a"]);

        let ws_a = make_workspace_at_path("feat/a", "refs/heads/feat/a", repo.clone());
        let ws_b = make_workspace_at_path("feat/b", "refs/heads/feat/b", repo.clone());
        let ws_a_id = ws_a.id;

        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);
        let (worker_tx, mut worker_rx) = mpsc::channel(8);
        tx.send(StateMessage::AttachWorker { worker_tx })
            .await
            .unwrap();

        for workspace in [ws_a, ws_b] {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(StateMessage::RegisterWorkspace {
                workspace,
                reply: reply_tx,
            })
            .await
            .unwrap();
            reply_rx.await.unwrap().unwrap();
        }

        tx.send(StateMessage::FileChanged {
            workspace_id: ws_a_id,
            path: PathBuf::from("src/shared.ts"),
        })
        .await
        .unwrap();

        // The base graph is built asynchronously off the actor thread. Dispatch
        // is deferred until the build completes, so the first worker message
        // should carry the populated graph.
        let msg = timeout(Duration::from_secs(5), worker_rx.recv())
            .await
            .expect("worker should receive a pair dispatch")
            .expect("worker message should be present");

        match msg {
            WorkerMessage::AnalyzePair { base_graph, .. } => {
                let dependents = base_graph.get_dependents(&PathBuf::from("src/shared.ts"));
                assert!(
                    dependents.contains(&&PathBuf::from("src/api.ts")),
                    "base graph should include shared.ts -> api.ts dependency edge"
                );
            }
        }

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    // ── File activity tracking ────────────────────────────────────────────

    #[tokio::test]
    async fn file_activity_absolute_path_becomes_relative() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let root = PathBuf::from("/tmp/worktree-a");
        let ws = make_workspace_at_path("a", "main", root.clone());
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();

        state
            .handle_file_changed(ws_id, &root.join("src/lib.rs"))
            .await;

        let ws_activity = state.file_activity.get(&ws_id).unwrap();
        assert_eq!(ws_activity.len(), 1);
        let entry = ws_activity.get(&PathBuf::from("src/lib.rs")).unwrap();
        assert_eq!(entry.workspace_id, ws_id);
        assert_eq!(entry.relative_path, PathBuf::from("src/lib.rs"));
        assert_eq!(entry.change_count, 1);
    }

    #[tokio::test]
    async fn file_activity_without_session_or_static_mapping_has_unknown_attribution() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let root = PathBuf::from("/tmp/worktree-unknown");
        let ws = make_workspace_at_path("unknown", "main", root.clone());
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();

        state
            .handle_file_changed(ws_id, &root.join("src/lib.rs"))
            .await;

        let entry = state
            .file_activity
            .get(&ws_id)
            .unwrap()
            .get(&PathBuf::from("src/lib.rs"))
            .unwrap();
        assert_eq!(
            entry.attribution.confidence,
            grove_lib::AttributionConfidence::Unknown
        );
        assert!(entry.attribution.sessions.is_empty());
        assert_eq!(entry.attribution.static_agent_name, None);
    }

    #[tokio::test]
    async fn file_activity_with_one_active_session_has_strong_attribution() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let root = PathBuf::from("/tmp/worktree-strong");
        let ws = make_workspace_at_path("strong", "main", root.clone());
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();
        upsert_dummy(
            &mut state,
            "session-strong",
            root.to_str().unwrap(),
            SessionLifecycle::Active,
        );

        state
            .handle_file_changed(ws_id, &root.join("src/lib.rs"))
            .await;

        let attribution = &state
            .file_activity
            .get(&ws_id)
            .unwrap()
            .get(&PathBuf::from("src/lib.rs"))
            .unwrap()
            .attribution;
        assert_eq!(
            attribution.confidence,
            grove_lib::AttributionConfidence::Strong
        );
        assert_eq!(attribution.sessions.len(), 1);
        assert_eq!(
            attribution.sessions[0].provider,
            grove_lib::AgentProvider::OpenCode
        );
        assert_eq!(attribution.sessions[0].native_session_id, "session-strong");
    }

    #[tokio::test]
    async fn file_activity_with_multiple_active_sessions_has_ambiguous_attribution() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let root = PathBuf::from("/tmp/worktree-ambiguous");
        let ws = make_workspace_at_path("ambiguous", "main", root.clone());
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();
        upsert_dummy(
            &mut state,
            "session-b",
            root.to_str().unwrap(),
            SessionLifecycle::Active,
        );
        state.session_registry.upsert(SessionUpdate {
            provider: AgentProvider::Codex,
            native_session_id: "session-a".to_string(),
            parent_native_session_id: None,
            agent_role: None,
            directory: root.clone(),
            lifecycle: SessionLifecycle::Active,
            discovery_source: DiscoverySource::ProcessScan,
            created_at: None,
            updated_at: None,
        });

        state
            .handle_file_changed(ws_id, &root.join("src/lib.rs"))
            .await;

        let attribution = &state
            .file_activity
            .get(&ws_id)
            .unwrap()
            .get(&PathBuf::from("src/lib.rs"))
            .unwrap()
            .attribution;
        assert_eq!(
            attribution.confidence,
            grove_lib::AttributionConfidence::Ambiguous
        );
        assert_eq!(attribution.sessions.len(), 2);
        assert_eq!(attribution.sessions[0].provider, AgentProvider::OpenCode);
        assert_eq!(attribution.sessions[1].provider, AgentProvider::Codex);
    }

    #[tokio::test]
    async fn file_activity_ignores_non_active_sessions_during_correlation() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let root = PathBuf::from("/tmp/worktree-mixed");
        let ws = make_workspace_at_path("mixed", "main", root.clone());
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();
        upsert_dummy(
            &mut state,
            "session-active",
            root.to_str().unwrap(),
            SessionLifecycle::Active,
        );
        state.session_registry.upsert(SessionUpdate {
            provider: AgentProvider::Codex,
            native_session_id: "session-idle".to_string(),
            parent_native_session_id: None,
            agent_role: None,
            directory: root.clone(),
            lifecycle: SessionLifecycle::Idle,
            discovery_source: DiscoverySource::ProcessScan,
            created_at: None,
            updated_at: None,
        });

        state
            .handle_file_changed(ws_id, &root.join("src/lib.rs"))
            .await;

        let attribution = &state
            .file_activity
            .get(&ws_id)
            .unwrap()
            .get(&PathBuf::from("src/lib.rs"))
            .unwrap()
            .attribution;
        assert_eq!(
            attribution.confidence,
            grove_lib::AttributionConfidence::Strong
        );
        assert_eq!(attribution.sessions.len(), 1);
        assert_eq!(attribution.sessions[0].native_session_id, "session-active");
    }

    #[tokio::test]
    async fn file_activity_uses_explicit_static_mapping_when_no_session_is_active() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let root = PathBuf::from("/tmp/worktree-static");
        let ws = make_workspace_at_path("static", "feature/static", root.clone());
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();
        state.agent_config = Some(AgentsConfig {
            agents: vec![crate::agent_config::AgentEntry {
                name: "configured-agent".to_string(),
                path: None,
                branch: Some("feature/static".to_string()),
                branch_pattern: None,
                worktree_pattern: None,
            }],
        });

        state
            .handle_file_changed(ws_id, &root.join("src/lib.rs"))
            .await;

        let attribution = &state
            .file_activity
            .get(&ws_id)
            .unwrap()
            .get(&PathBuf::from("src/lib.rs"))
            .unwrap()
            .attribution;
        assert_eq!(
            attribution.confidence,
            grove_lib::AttributionConfidence::StaticFallback
        );
        assert_eq!(
            attribution.static_agent_name.as_deref(),
            Some("configured-agent")
        );
        assert!(attribution.sessions.is_empty());
    }

    #[tokio::test]
    async fn repeated_file_activity_recomputes_latest_attribution() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let root = PathBuf::from("/tmp/worktree-latest");
        let ws = make_workspace_at_path("latest", "main", root.clone());
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();
        let path = root.join("src/lib.rs");

        state.handle_file_changed(ws_id, &path).await;
        upsert_dummy(
            &mut state,
            "session-latest",
            root.to_str().unwrap(),
            SessionLifecycle::Active,
        );
        state.handle_file_changed(ws_id, &path).await;

        let entry = state
            .file_activity
            .get(&ws_id)
            .unwrap()
            .get(&PathBuf::from("src/lib.rs"))
            .unwrap();
        assert_eq!(entry.change_count, 2);
        assert_eq!(
            entry.attribution.confidence,
            grove_lib::AttributionConfidence::Strong
        );
        assert_eq!(
            entry.attribution.sessions[0].native_session_id,
            "session-latest"
        );
    }

    #[tokio::test]
    async fn file_activity_outside_worktree_is_rejected() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let root = PathBuf::from("/tmp/worktree-a");
        let ws = make_workspace_at_path("a", "main", root);
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();

        state
            .handle_file_changed(ws_id, &PathBuf::from("/other/path/file.rs"))
            .await;

        assert!(state.file_activity.is_empty());
    }

    #[tokio::test]
    async fn file_activity_repeated_writes_update_one_record() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let root = PathBuf::from("/tmp/worktree-a");
        let ws = make_workspace_at_path("a", "main", root.clone());
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();

        let path = root.join("src/lib.rs");
        state.handle_file_changed(ws_id, &path).await;
        state.handle_file_changed(ws_id, &path).await;

        let ws_activity = state.file_activity.get(&ws_id).unwrap();
        assert_eq!(ws_activity.len(), 1);
        let entry = ws_activity.get(&PathBuf::from("src/lib.rs")).unwrap();
        assert_eq!(entry.change_count, 2);
    }

    #[tokio::test]
    async fn file_activity_timestamps_on_repeated_writes() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let root = PathBuf::from("/tmp/worktree-a");
        let ws = make_workspace_at_path("a", "main", root.clone());
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();

        let path = root.join("src/lib.rs");
        state.handle_file_changed(ws_id, &path).await;
        let entry_after_first = state
            .file_activity
            .get(&ws_id)
            .unwrap()
            .get(&PathBuf::from("src/lib.rs"))
            .unwrap()
            .clone();

        tokio::time::sleep(Duration::from_millis(5)).await;

        state.handle_file_changed(ws_id, &path).await;
        let entry = state
            .file_activity
            .get(&ws_id)
            .unwrap()
            .get(&PathBuf::from("src/lib.rs"))
            .unwrap();

        assert_eq!(entry.first_changed_at, entry_after_first.first_changed_at);
        assert!(
            entry.last_changed_at > entry.first_changed_at,
            "last_changed_at ({last}) should be later than first_changed_at ({first})",
            last = entry.last_changed_at,
            first = entry.first_changed_at,
        );
        assert_eq!(entry.change_count, 2);
    }

    #[tokio::test]
    async fn file_activity_change_count_increments() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let root = PathBuf::from("/tmp/worktree-a");
        let ws = make_workspace_at_path("a", "main", root.clone());
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();

        let path = root.join("src/lib.rs");
        for _ in 0..5 {
            state.handle_file_changed(ws_id, &path).await;
        }

        let entry = state
            .file_activity
            .get(&ws_id)
            .unwrap()
            .get(&PathBuf::from("src/lib.rs"))
            .unwrap();
        assert_eq!(entry.change_count, 5);
    }

    #[tokio::test]
    async fn file_activity_identical_relative_paths_in_separate_worktrees() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let root_a = PathBuf::from("/tmp/worktree-a");
        let root_b = PathBuf::from("/tmp/worktree-b");
        let ws_a = make_workspace_at_path("a", "main", root_a.clone());
        let ws_b = make_workspace_at_path("b", "main", root_b.clone());
        let id_a = ws_a.id;
        let id_b = ws_b.id;
        state.handle_register_workspace(ws_a).unwrap();
        state.handle_register_workspace(ws_b).unwrap();

        state
            .handle_file_changed(id_a, &root_a.join("src/lib.rs"))
            .await;
        state
            .handle_file_changed(id_b, &root_b.join("src/lib.rs"))
            .await;

        let a_activity = state.file_activity.get(&id_a).unwrap();
        let b_activity = state.file_activity.get(&id_b).unwrap();
        assert_eq!(a_activity.len(), 1);
        assert_eq!(b_activity.len(), 1);
        // Both have the same relative path but under different workspace IDs
        assert_eq!(
            a_activity
                .get(&PathBuf::from("src/lib.rs"))
                .unwrap()
                .workspace_id,
            id_a
        );
        assert_eq!(
            b_activity
                .get(&PathBuf::from("src/lib.rs"))
                .unwrap()
                .workspace_id,
            id_b
        );
    }

    #[tokio::test]
    async fn file_activity_unknown_workspace_rejected() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        state
            .handle_file_changed(Uuid::new_v4(), &PathBuf::from("/some/path/file.rs"))
            .await;
        assert!(state.file_activity.is_empty());
    }

    #[tokio::test]
    async fn file_activity_worktree_root_not_stored() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let root = PathBuf::from("/tmp/worktree-a");
        let ws = make_workspace_at_path("a", "main", root.clone());
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();

        // Send the worktree root itself as the changed path
        state.handle_file_changed(ws_id, &root).await;

        assert!(state.file_activity.is_empty());
    }

    #[tokio::test]
    async fn file_activity_removing_workspace_clears_activity() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let root = PathBuf::from("/tmp/worktree-a");
        let ws = make_workspace_at_path("a", "main", root.clone());
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();

        state
            .handle_file_changed(ws_id, &root.join("src/lib.rs"))
            .await;
        assert!(state.file_activity.contains_key(&ws_id));

        state.handle_remove_workspace(ws_id).unwrap();
        assert!(!state.file_activity.contains_key(&ws_id));
    }

    // ── File activity query ───────────────────────────────────────────────

    #[tokio::test]
    async fn get_file_activity_query_returns_flattened_sorted_results() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let root_a = PathBuf::from("/tmp/worktree-a");
        let root_b = PathBuf::from("/tmp/worktree-b");
        let ws_a = make_workspace_at_path("a", "main", root_a.clone());
        let ws_b = make_workspace_at_path("b", "main", root_b.clone());
        let id_a = ws_a.id;
        let id_b = ws_b.id;
        state.handle_register_workspace(ws_a).unwrap();
        state.handle_register_workspace(ws_b).unwrap();

        state
            .handle_file_changed(id_a, &root_a.join("src/a.rs"))
            .await;
        state
            .handle_file_changed(id_b, &root_b.join("src/b.rs"))
            .await;
        state
            .handle_file_changed(id_a, &root_a.join("src/a2.rs"))
            .await;

        // Query all — should get 3 entries sorted newest first
        let (reply_tx, reply_rx) = oneshot::channel();
        state.handle_query(
            QueryRequest::GetFileActivity {
                workspace_id: None,
                limit: None,
            },
            reply_tx,
        );
        let result = reply_rx.await.unwrap();
        let activities = match result {
            QueryResponse::FileActivity(a) => a,
            other => panic!("expected FileActivity, got: {other:?}"),
        };
        assert_eq!(activities.len(), 3);

        // Query filtered by workspace
        let (reply_tx, reply_rx) = oneshot::channel();
        state.handle_query(
            QueryRequest::GetFileActivity {
                workspace_id: Some(id_a),
                limit: None,
            },
            reply_tx,
        );
        let result = reply_rx.await.unwrap();
        let activities = match result {
            QueryResponse::FileActivity(a) => a,
            other => panic!("expected FileActivity, got: {other:?}"),
        };
        assert_eq!(activities.len(), 2);
        assert!(activities.iter().all(|a| a.workspace_id == id_a));

        // Query with limit
        let (reply_tx, reply_rx) = oneshot::channel();
        state.handle_query(
            QueryRequest::GetFileActivity {
                workspace_id: None,
                limit: Some(1),
            },
            reply_tx,
        );
        let result = reply_rx.await.unwrap();
        let activities = match result {
            QueryResponse::FileActivity(a) => a,
            other => panic!("expected FileActivity, got: {other:?}"),
        };
        assert_eq!(activities.len(), 1);
    }

    #[tokio::test]
    async fn get_agent_assignments_query_returns_fallback_names() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        let ws = make_workspace("test-ws");
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        state.handle_query(QueryRequest::GetAgentAssignments, reply_tx);
        let result = reply_rx.await.unwrap();
        let assignments = match result {
            QueryResponse::AgentAssignments(a) => a,
            other => panic!("expected AgentAssignments, got: {other:?}"),
        };
        assert_eq!(assignments.len(), 1);
        // Without agents.toml, fallback to workspace name
        assert_eq!(assignments[0].workspace_id, ws_id);
        assert_eq!(assignments[0].agent_name, "test-ws");
    }

    // ── Agent session registry integration ─────────────────────────────────

    #[tokio::test]
    async fn agent_session_updates_reach_registry() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);

        let updates = vec![
            SessionUpdate {
                provider: grove_lib::AgentProvider::OpenCode,
                native_session_id: "ses-1".to_string(),
                parent_native_session_id: None,
                agent_role: None,
                directory: PathBuf::from("/tmp/ws"),
                lifecycle: grove_lib::SessionLifecycle::Active,
                discovery_source: grove_lib::DiscoverySource::ProcessScan,
                created_at: None,
                updated_at: None,
            },
            SessionUpdate {
                provider: grove_lib::AgentProvider::ClaudeCode,
                native_session_id: "ses-2".to_string(),
                parent_native_session_id: None,
                agent_role: None,
                directory: PathBuf::from("/tmp/ws"),
                lifecycle: grove_lib::SessionLifecycle::Discovered,
                discovery_source: grove_lib::DiscoverySource::ApiPoll,
                created_at: None,
                updated_at: None,
            },
        ];

        tx.send(StateMessage::AgentSessionUpdates { updates })
            .await
            .unwrap();

        // Query to ensure actor processed the message
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::GetStatus,
            reply: reply_tx,
        })
        .await
        .unwrap();
        reply_rx.await.unwrap();

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn repeated_session_updates_remain_one_session() {
        let (tx, _event_tx, handle) = spawn_state_actor(GroveConfig::default(), None);

        fn make_update(native_id: &str, lifecycle: grove_lib::SessionLifecycle) -> SessionUpdate {
            SessionUpdate {
                provider: grove_lib::AgentProvider::OpenCode,
                native_session_id: native_id.to_string(),
                parent_native_session_id: None,
                agent_role: None,
                directory: PathBuf::from("/tmp/ws"),
                lifecycle,
                discovery_source: grove_lib::DiscoverySource::ProcessScan,
                created_at: None,
                updated_at: None,
            }
        }

        // First update: Active
        tx.send(StateMessage::AgentSessionUpdates {
            updates: vec![make_update("ses-1", grove_lib::SessionLifecycle::Active)],
        })
        .await
        .unwrap();

        // Second update: Idle — same session
        tx.send(StateMessage::AgentSessionUpdates {
            updates: vec![make_update("ses-1", grove_lib::SessionLifecycle::Idle)],
        })
        .await
        .unwrap();

        // Query to drain
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(StateMessage::Query {
            request: QueryRequest::GetStatus,
            reply: reply_tx,
        })
        .await
        .unwrap();
        reply_rx.await.unwrap();

        tx.send(StateMessage::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    // ── Agent session event tests ─────────────────────────────────────────

    fn make_event(
        kind: EventKind,
        session_id: Option<&str>,
        parent_id: Option<&str>,
        directory: Option<&str>,
        lifecycle: Option<SessionLifecycle>,
        agent_role: Option<&str>,
        timestamp: Option<i64>,
    ) -> AgentEvent {
        AgentEvent {
            provider: AgentProvider::OpenCode,
            native_session_id: session_id.map(|s| s.to_string()),
            parent_native_session_id: parent_id.map(|s| s.to_string()),
            event_kind: kind,
            timestamp: timestamp.and_then(chrono::DateTime::from_timestamp_millis),
            lifecycle,
            agent_role: agent_role.map(|s| s.to_string()),
            directory: directory.map(PathBuf::from),
            compacted: false,
        }
    }

    fn upsert_dummy(state: &mut DaemonState, id: &str, dir: &str, lifecycle: SessionLifecycle) {
        state.session_registry.upsert(SessionUpdate {
            provider: AgentProvider::OpenCode,
            native_session_id: id.to_string(),
            parent_native_session_id: None,
            agent_role: None,
            directory: PathBuf::from(dir),
            lifecycle,
            discovery_source: DiscoverySource::ProcessScan,
            created_at: None,
            updated_at: None,
        });
    }

    #[test]
    fn server_connected_is_noop() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        state.handle_agent_session_event(make_event(
            EventKind::ServerConnected,
            None,
            None,
            None,
            None,
            None,
            None,
        ));
        assert!(state.session_registry().is_empty());
    }

    #[test]
    fn event_without_session_id_is_ignored() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        // Every event kind that would otherwise do something
        for kind in &[
            EventKind::ServerConnected,
            EventKind::SessionCreated,
            EventKind::SessionUpdated,
            EventKind::MessagePartUpdated,
            EventKind::MessageUpdated,
            EventKind::SessionCompacted,
            EventKind::SessionDeleted,
        ] {
            state.handle_agent_session_event(make_event(
                kind.clone(),
                None,
                None,
                None,
                None,
                None,
                None,
            ));
        }
        assert!(state.session_registry().is_empty());
    }

    #[test]
    fn session_created_without_directory_is_ignored() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        state.handle_agent_session_event(make_event(
            EventKind::SessionCreated,
            Some("ses-1"),
            None,
            None,
            None,
            None,
            None,
        ));
        assert!(state.session_registry().is_empty());
    }

    #[test]
    fn session_created_with_empty_directory_is_ignored() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        state.handle_agent_session_event(make_event(
            EventKind::SessionCreated,
            Some("ses-1"),
            None,
            Some(""),
            None,
            None,
            None,
        ));
        assert!(state.session_registry().is_empty());
    }

    #[test]
    fn session_created_without_explicit_lifecycle_is_discovered() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        state.handle_agent_session_event(make_event(
            EventKind::SessionCreated,
            Some("ses-1"),
            None,
            Some("/tmp/ws"),
            None,
            None,
            None,
        ));

        let session = state
            .session_registry()
            .get(AgentProvider::OpenCode, "ses-1")
            .unwrap();
        assert_eq!(session.lifecycle, SessionLifecycle::Discovered);
    }

    #[test]
    fn session_created_creates_session() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        state.handle_agent_session_event(make_event(
            EventKind::SessionCreated,
            Some("ses-1"),
            Some("parent-1"),
            Some("/tmp/ws"),
            Some(SessionLifecycle::Active),
            Some("opencode"),
            Some(1718455200000),
        ));

        let s = state
            .session_registry()
            .get(AgentProvider::OpenCode, "ses-1")
            .expect("session should exist");
        assert_eq!(s.parent_native_session_id.as_deref(), Some("parent-1"));
        assert_eq!(s.agent_role.as_deref(), Some("opencode"));
        assert_eq!(s.directory, PathBuf::from("/tmp/ws"));
        assert_eq!(s.lifecycle, SessionLifecycle::Active);
        assert_eq!(s.discovery_source, DiscoverySource::EventStream);
        assert!(s.created_at.timestamp_millis() == 1718455200000);
    }

    #[test]
    fn partial_event_for_unknown_session_ignored() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        // session.updated without existing session
        state.handle_agent_session_event(make_event(
            EventKind::SessionUpdated,
            Some("ses-unknown"),
            None,
            None,
            None,
            None,
            None,
        ));
        assert!(state.session_registry().is_empty());

        // message.part.updated without existing session
        state.handle_agent_session_event(make_event(
            EventKind::MessagePartUpdated,
            Some("ses-unknown"),
            None,
            None,
            None,
            None,
            None,
        ));
        assert!(state.session_registry().is_empty());

        // message.updated without existing session
        state.handle_agent_session_event(make_event(
            EventKind::MessageUpdated,
            Some("ses-unknown"),
            None,
            None,
            None,
            None,
            None,
        ));
        assert!(state.session_registry().is_empty());

        // session.deleted without existing session
        state.handle_agent_session_event(make_event(
            EventKind::SessionDeleted,
            Some("ses-unknown"),
            None,
            None,
            None,
            None,
            None,
        ));
        assert!(state.session_registry().is_empty());
    }

    #[test]
    fn session_created_then_active_and_idle_transitions() {
        let mut state = DaemonState::new(GroveConfig::default(), None);

        // Create as Active
        state.handle_agent_session_event(make_event(
            EventKind::SessionCreated,
            Some("ses-1"),
            None,
            Some("/tmp/ws"),
            Some(SessionLifecycle::Active),
            None,
            Some(1000),
        ));
        assert_eq!(
            state
                .session_registry()
                .get(AgentProvider::OpenCode, "ses-1")
                .unwrap()
                .lifecycle,
            SessionLifecycle::Active
        );

        // message.part.updated with completed → Idle
        state.handle_agent_session_event(make_event(
            EventKind::MessagePartUpdated,
            Some("ses-1"),
            None,
            None,
            Some(SessionLifecycle::Idle),
            None,
            Some(2000),
        ));
        assert_eq!(
            state
                .session_registry()
                .get(AgentProvider::OpenCode, "ses-1")
                .unwrap()
                .lifecycle,
            SessionLifecycle::Idle
        );

        // message.part.updated with running → Active
        state.handle_agent_session_event(make_event(
            EventKind::MessagePartUpdated,
            Some("ses-1"),
            None,
            None,
            Some(SessionLifecycle::Active),
            None,
            Some(3000),
        ));
        assert_eq!(
            state
                .session_registry()
                .get(AgentProvider::OpenCode, "ses-1")
                .unwrap()
                .lifecycle,
            SessionLifecycle::Active
        );
    }

    #[test]
    fn compacted_preserves_lifecycle() {
        let mut state = DaemonState::new(GroveConfig::default(), None);

        upsert_dummy(&mut state, "ses-1", "/tmp/ws", SessionLifecycle::Active);

        state.handle_agent_session_event(make_event(
            EventKind::SessionCompacted,
            Some("ses-1"),
            None,
            None,
            None,
            None,
            Some(1000),
        ));

        let s = state
            .session_registry()
            .get(AgentProvider::OpenCode, "ses-1")
            .unwrap();
        assert!(s.compacted);
        assert_eq!(s.lifecycle, SessionLifecycle::Active);
    }

    #[test]
    fn deleted_becomes_stopped() {
        let mut state = DaemonState::new(GroveConfig::default(), None);

        upsert_dummy(&mut state, "ses-1", "/tmp/ws", SessionLifecycle::Active);

        state.handle_agent_session_event(make_event(
            EventKind::SessionDeleted,
            Some("ses-1"),
            None,
            None,
            None,
            None,
            Some(1000),
        ));

        let s = state
            .session_registry()
            .get(AgentProvider::OpenCode, "ses-1")
            .unwrap();
        assert_eq!(s.lifecycle, SessionLifecycle::Stopped);
        // Still in registry
        assert_eq!(state.session_registry().len(), 1);
    }

    #[test]
    fn parent_child_metadata_preserved() {
        let mut state = DaemonState::new(GroveConfig::default(), None);

        // Create child with parent
        state.handle_agent_session_event(make_event(
            EventKind::SessionCreated,
            Some("child-1"),
            Some("parent-1"),
            Some("/tmp/ws"),
            Some(SessionLifecycle::Active),
            Some("opencode"),
            Some(1000),
        ));

        // Session updated should preserve parent and role
        state.handle_agent_session_event(make_event(
            EventKind::SessionUpdated,
            Some("child-1"),
            None,
            None,
            None,
            None,
            Some(2000),
        ));

        let s = state
            .session_registry()
            .get(AgentProvider::OpenCode, "child-1")
            .unwrap();
        assert_eq!(s.parent_native_session_id.as_deref(), Some("parent-1"));
        assert_eq!(s.agent_role.as_deref(), Some("opencode"));
    }

    #[test]
    fn workspace_binding_preserved_during_event_updates() {
        let mut state = DaemonState::new(GroveConfig::default(), None);

        // Register a workspace
        let ws = make_workspace_at_path("project", "main", PathBuf::from("/home/user/project"));
        state.handle_register_workspace(ws).unwrap();

        // Create a session inside that workspace
        state.handle_agent_session_event(make_event(
            EventKind::SessionCreated,
            Some("ses-1"),
            None,
            Some("/home/user/project/src"),
            Some(SessionLifecycle::Active),
            None,
            Some(1000),
        ));

        let ws_id = state
            .session_registry()
            .get(AgentProvider::OpenCode, "ses-1")
            .unwrap()
            .workspace_id;
        assert!(
            ws_id.is_some(),
            "workspace_id should be bound after creation"
        );

        // An event update must not unbind it
        state.handle_agent_session_event(make_event(
            EventKind::MessagePartUpdated,
            Some("ses-1"),
            None,
            None,
            Some(SessionLifecycle::Idle),
            None,
            Some(2000),
        ));

        let ws_id_after = state
            .session_registry()
            .get(AgentProvider::OpenCode, "ses-1")
            .unwrap()
            .workspace_id;
        assert_eq!(ws_id_after, ws_id, "workspace binding must survive updates");
    }

    #[test]
    fn message_updated_preserves_metadata() {
        let mut state = DaemonState::new(GroveConfig::default(), None);

        upsert_dummy(&mut state, "ses-1", "/tmp/ws", SessionLifecycle::Active);

        state.handle_agent_session_event(make_event(
            EventKind::MessageUpdated,
            Some("ses-1"),
            None,
            None,
            None,
            None,
            Some(1000),
        ));

        let s = state
            .session_registry()
            .get(AgentProvider::OpenCode, "ses-1")
            .unwrap();
        assert_eq!(s.directory, PathBuf::from("/tmp/ws"));
        assert_eq!(s.lifecycle, SessionLifecycle::Active);
    }

    #[test]
    fn message_updated_applies_explicit_normalized_lifecycle_transition() {
        let mut state = DaemonState::new(GroveConfig::default(), None);
        upsert_dummy(&mut state, "ses-1", "/tmp/ws", SessionLifecycle::Active);

        state.handle_agent_session_event(make_event(
            EventKind::MessageUpdated,
            Some("ses-1"),
            None,
            None,
            Some(SessionLifecycle::Idle),
            None,
            Some(1000),
        ));

        let session = state
            .session_registry()
            .get(AgentProvider::OpenCode, "ses-1")
            .unwrap();
        assert_eq!(session.lifecycle, SessionLifecycle::Idle);
    }

    #[test]
    fn unknown_part_status_preserves_lifecycle() {
        let mut state = DaemonState::new(GroveConfig::default(), None);

        upsert_dummy(&mut state, "ses-1", "/tmp/ws", SessionLifecycle::Active);

        // message.part.updated with None lifecycle (unknown status)
        state.handle_agent_session_event(make_event(
            EventKind::MessagePartUpdated,
            Some("ses-1"),
            None,
            None,
            None,
            None,
            Some(1000),
        ));

        let s = state
            .session_registry()
            .get(AgentProvider::OpenCode, "ses-1")
            .unwrap();
        assert_eq!(s.lifecycle, SessionLifecycle::Active);
    }

    // ── Direct state access tests (no actor) ───────────────────────────────

    #[test]
    fn workspace_registration_binds_existing_session() {
        let mut state = DaemonState::new(GroveConfig::default(), None);

        // Upsert a session before any workspace is registered — no binding.
        state.session_registry.upsert(SessionUpdate {
            provider: grove_lib::AgentProvider::OpenCode,
            native_session_id: "ses-1".to_string(),
            parent_native_session_id: None,
            agent_role: None,
            directory: PathBuf::from("/home/user/project/src"),
            lifecycle: grove_lib::SessionLifecycle::Active,
            discovery_source: grove_lib::DiscoverySource::ProcessScan,
            created_at: None,
            updated_at: None,
        });

        // Session should be unbound initially
        assert!(
            state
                .session_registry()
                .get(grove_lib::AgentProvider::OpenCode, "ses-1")
                .unwrap()
                .workspace_id
                .is_none()
        );

        // Register a workspace whose path covers the session's directory
        let ws = make_workspace_at_path("project", "main", PathBuf::from("/home/user/project"));
        state.handle_register_workspace(ws).unwrap();

        // Session should now be bound
        let session = state
            .session_registry()
            .get(grove_lib::AgentProvider::OpenCode, "ses-1")
            .unwrap();
        assert!(session.workspace_id.is_some());
    }

    #[test]
    fn workspace_removal_unbinds_session_without_deleting_it() {
        let mut state = DaemonState::new(GroveConfig::default(), None);

        // Register a workspace first
        let ws = make_workspace_at_path("project", "main", PathBuf::from("/home/user/project"));
        let ws_id = ws.id;
        state.handle_register_workspace(ws).unwrap();

        // Upsert a session that binds to this workspace
        state.session_registry.upsert(SessionUpdate {
            provider: grove_lib::AgentProvider::OpenCode,
            native_session_id: "ses-1".to_string(),
            parent_native_session_id: None,
            agent_role: None,
            directory: PathBuf::from("/home/user/project/src"),
            lifecycle: grove_lib::SessionLifecycle::Active,
            discovery_source: grove_lib::DiscoverySource::ProcessScan,
            created_at: None,
            updated_at: None,
        });

        // Confirm it's bound
        assert!(
            state
                .session_registry()
                .get(grove_lib::AgentProvider::OpenCode, "ses-1")
                .unwrap()
                .workspace_id
                .is_some()
        );

        // Remove the workspace
        state.handle_remove_workspace(ws_id).unwrap();

        // Session still exists but is now unbound
        let session = state
            .session_registry()
            .get(grove_lib::AgentProvider::OpenCode, "ses-1")
            .unwrap();
        assert!(
            session.workspace_id.is_none(),
            "session should be unbound, not deleted"
        );
        assert_eq!(session.native_session_id, "ses-1");
    }

    #[test]
    fn missing_sessions_in_snapshot_not_marked_stopped() {
        let mut state = DaemonState::new(GroveConfig::default(), None);

        // Register a session
        state.session_registry.upsert(SessionUpdate {
            provider: grove_lib::AgentProvider::OpenCode,
            native_session_id: "ses-keep".to_string(),
            parent_native_session_id: None,
            agent_role: None,
            directory: PathBuf::from("/tmp/ws"),
            lifecycle: grove_lib::SessionLifecycle::Active,
            discovery_source: grove_lib::DiscoverySource::ProcessScan,
            created_at: None,
            updated_at: None,
        });

        // Submit a snapshot that does NOT include ses-keep
        state.handle_agent_session_updates(vec![SessionUpdate {
            provider: grove_lib::AgentProvider::OpenCode,
            native_session_id: "ses-other".to_string(),
            parent_native_session_id: None,
            agent_role: None,
            directory: PathBuf::from("/tmp/other"),
            lifecycle: grove_lib::SessionLifecycle::Active,
            discovery_source: grove_lib::DiscoverySource::ProcessScan,
            created_at: None,
            updated_at: None,
        }]);

        // ses-keep must still be Active, not Stopped
        let session = state
            .session_registry()
            .get(grove_lib::AgentProvider::OpenCode, "ses-keep")
            .unwrap();
        assert_eq!(
            session.lifecycle,
            grove_lib::SessionLifecycle::Active,
            "session missing from snapshot must not be marked Stopped"
        );
        assert_eq!(state.session_registry().len(), 2);
    }

    #[test]
    fn registry_size_accessible_after_updates() {
        let mut state = DaemonState::new(GroveConfig::default(), None);

        state.session_registry.upsert(SessionUpdate {
            provider: grove_lib::AgentProvider::OpenCode,
            native_session_id: "a".to_string(),
            parent_native_session_id: None,
            agent_role: None,
            directory: PathBuf::from("/tmp/a"),
            lifecycle: grove_lib::SessionLifecycle::Active,
            discovery_source: grove_lib::DiscoverySource::ProcessScan,
            created_at: None,
            updated_at: None,
        });
        state.session_registry.upsert(SessionUpdate {
            provider: grove_lib::AgentProvider::ClaudeCode,
            native_session_id: "b".to_string(),
            parent_native_session_id: None,
            agent_role: None,
            directory: PathBuf::from("/tmp/b"),
            lifecycle: grove_lib::SessionLifecycle::Discovered,
            discovery_source: grove_lib::DiscoverySource::ApiPoll,
            created_at: None,
            updated_at: None,
        });

        assert_eq!(state.session_registry().len(), 2);
    }

    // ── OpenCodeConfig tests ────────────────────────────────────────────────

    /// Evaluate the production parser against independent test values.
    fn opencode_config(
        url: Option<&str>,
        user: Option<&str>,
        pass: Option<&str>,
    ) -> Result<Option<OpenCodeConfig>, String> {
        OpenCodeConfig::from_lookup(|name| {
            let value = match name {
                "OPENCODE_SERVER_URL" => url,
                "OPENCODE_SERVER_USERNAME" => user,
                "OPENCODE_SERVER_PASSWORD" => pass,
                _ => None,
            };
            value
                .map(str::to_owned)
                .ok_or(std::env::VarError::NotPresent)
        })
    }

    #[test]
    fn opencode_config_no_env_vars_disabled() {
        let result = opencode_config(None, None, None);
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn opencode_config_all_vars_configured() {
        let result = opencode_config(
            Some("http://localhost:8080"),
            Some("testuser"),
            Some("testpass"),
        );
        let cfg = result.unwrap().unwrap();
        assert_eq!(cfg.server_url, "http://localhost:8080");
    }

    #[test]
    fn opencode_config_url_without_username_errors() {
        let result = opencode_config(Some("http://localhost:8080"), None, Some("somepass"));
        let err = result.unwrap_err();
        assert!(
            err.contains("OPENCODE_SERVER_USERNAME"),
            "error should mention USERNAME, got: {err}"
        );
        assert!(!err.contains("somepass"), "error must not leak password");
    }

    #[test]
    fn opencode_config_url_without_password_errors() {
        let result = opencode_config(Some("http://localhost:8080"), Some("someuser"), None);
        let err = result.unwrap_err();
        assert!(
            err.contains("OPENCODE_SERVER_PASSWORD"),
            "error should mention PASSWORD, got: {err}"
        );
        assert!(!err.contains("someuser"), "error must not leak username");
    }

    #[test]
    fn opencode_config_password_without_username_errors() {
        let result = opencode_config(Some("http://localhost:8080"), None, Some("secret123"));
        let err = result.unwrap_err();
        assert!(
            err.contains("OPENCODE_SERVER_USERNAME"),
            "error should mention USERNAME, got: {err}"
        );
        assert!(!err.contains("secret123"), "error must not leak password");
    }

    #[test]
    fn opencode_config_rejects_empty_credentials() {
        let result = opencode_config(Some("http://localhost:8080"), Some("  "), Some("testpass"));
        let error = result.unwrap_err();
        assert!(error.contains("OPENCODE_SERVER_USERNAME"));
        assert!(!error.contains("testpass"));

        let result = opencode_config(Some("http://localhost:8080"), Some("testuser"), Some(""));
        let error = result.unwrap_err();
        assert!(error.contains("OPENCODE_SERVER_PASSWORD"));
        assert!(!error.contains("testuser"));
    }

    #[test]
    fn opencode_config_debug_hides_credentials() {
        let cfg = OpenCodeConfig {
            server_url: "http://localhost:8080".to_string(),
            username: "admin".to_string(),
            password: "supersecret".to_string(),
        };
        let debug_str = format!("{cfg:?}");
        assert!(
            !debug_str.contains("admin"),
            "Debug must not contain username: {debug_str}"
        );
        assert!(
            !debug_str.contains("supersecret"),
            "Debug must not contain password: {debug_str}"
        );
        assert!(
            debug_str.contains("***"),
            "Debug should mask values: {debug_str}"
        );
    }

    #[test]
    fn opencode_config_error_messages_do_not_expose_credentials() {
        let result = opencode_config(Some("http://localhost:8080"), None, Some("s3cret"));
        let err = result.unwrap_err();
        assert!(
            !err.contains("s3cret"),
            "error must not expose password: {err}"
        );
        assert!(
            !err.contains("localhost"),
            "error should not contain URL value: {err}"
        );

        let result = opencode_config(Some("http://localhost:8080"), Some("admin"), None);
        let err = result.unwrap_err();
        assert!(
            !err.contains("admin"),
            "error must not expose username: {err}"
        );
        assert!(
            !err.contains("localhost"),
            "error should not contain URL value: {err}"
        );
    }

    #[test]
    fn opencode_server_url_validation_rejects_credential_bearing_urls() {
        assert!(validate_opencode_server_url("http://127.0.0.1:4096").is_ok());
        assert!(validate_opencode_server_url("https://opencode.local").is_ok());

        for url in [
            "http://user:pass@127.0.0.1:4096",
            "http://127.0.0.1:4096?token=secret",
            "http://127.0.0.1:4096/#secret",
            "file:///tmp/opencode.sock",
        ] {
            let error = validate_opencode_server_url(url).unwrap_err();
            assert!(!error.contains(url));
            assert!(!error.contains("secret"));
            assert!(!error.contains("pass"));
        }
    }
}

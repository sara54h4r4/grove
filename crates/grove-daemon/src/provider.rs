use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use grove_lib::AgentProvider;
use tokio::sync::{mpsc, oneshot};

use crate::event_stream::AgentEvent;
use crate::session_registry::SessionUpdate;
use crate::state::StateMessage;

/// Boxed future used by the object-safe provider adapter boundary.
pub type AdapterFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Availability of full, allowlisted session snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotCapability {
    Unsupported,
    FullMetadata,
}

/// Availability of normalized live lifecycle events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventCapability {
    Unsupported,
    LifecycleStream,
}

/// How Grove can determine adapter health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthCapability {
    ConnectionState,
    ProviderReported,
}

/// Whether a provider can safely identify both the editing session and path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactFileActivityCapability {
    Unsupported,
    SessionAndRelativePath,
}

/// Whether provider-native parent/child session IDs are available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParentSessionCapability {
    Unsupported,
    NativeIds,
}

/// Static metadata capabilities exposed by one provider adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderCapabilities {
    pub snapshots: SnapshotCapability,
    pub events: EventCapability,
    pub health: HealthCapability,
    pub exact_file_activity: ExactFileActivityCapability,
    pub parent_sessions: ParentSessionCapability,
}

/// Sanitized provider failure categories.
///
/// Variants intentionally contain no URLs, credentials, response bodies, raw
/// events, or provider payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderAdapterError {
    RequestFailed,
    AuthenticationFailed,
    UnexpectedStatusCode(u16),
    MalformedMetadata,
    InvalidMetadata,
    PaginationFailed,
    InvalidEventStream,
    ChannelClosed,
    Shutdown,
}

impl fmt::Display for ProviderAdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RequestFailed => write!(f, "provider request failed"),
            Self::AuthenticationFailed => write!(f, "provider authentication failed"),
            Self::UnexpectedStatusCode(code) => {
                write!(f, "provider returned unexpected HTTP status code {code}")
            }
            Self::MalformedMetadata => write!(f, "provider returned malformed metadata"),
            Self::InvalidMetadata => write!(f, "provider returned invalid metadata"),
            Self::PaginationFailed => write!(f, "provider snapshot pagination failed"),
            Self::InvalidEventStream => write!(f, "provider event stream was invalid"),
            Self::ChannelClosed => write!(f, "provider event channel closed"),
            Self::Shutdown => write!(f, "provider adapter shut down"),
        }
    }
}

impl std::error::Error for ProviderAdapterError {}

/// Provider-neutral boundary for session snapshots and normalized live events.
///
/// Implementations must deserialize external data through sparse allowlists and
/// must never expose credentials, raw payloads, conversation content, tool
/// payloads, file contents, token counts, or cost data.
pub trait ProviderAdapter: fmt::Debug + Send + Sync {
    fn provider(&self) -> AgentProvider;

    fn capabilities(&self) -> ProviderCapabilities;

    fn fetch_snapshot(&self)
    -> AdapterFuture<'_, Result<Vec<SessionUpdate>, ProviderAdapterError>>;

    fn listen(
        &self,
        events_tx: mpsc::Sender<AgentEvent>,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> AdapterFuture<'_, Result<(), ProviderAdapterError>>;
}

/// Poll a provider snapshot immediately and then at a fixed recovery interval.
///
/// Requests are awaited serially, so one poller never overlaps its own
/// snapshots. Successful metadata updates are forwarded to the state actor.
pub async fn run_provider_polling(
    adapter: Arc<dyn ProviderAdapter>,
    state_tx: mpsc::Sender<StateMessage>,
    interval: Duration,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = &mut shutdown_rx => return,
            _ = ticker.tick() => {
                match adapter.fetch_snapshot().await {
                    Ok(updates) if !updates.is_empty() => {
                        if state_tx
                            .send(StateMessage::AgentSessionUpdates { updates })
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(
                            provider = ?adapter.provider(),
                            error = %error,
                            "provider snapshot poll failed, will retry"
                        );
                    }
                }
            }
        }
    }
}

/// Reconnect timing shared by provider event supervisors.
#[derive(Debug, Clone, Copy)]
pub struct ReconnectPolicy {
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
        }
    }
}

impl ReconnectPolicy {
    fn next_delay(self, current: Duration) -> Duration {
        current
            .saturating_mul(2)
            .min(self.max_delay.max(self.initial_delay))
    }
}

/// Supervise normalized provider events with snapshot-before-connect recovery.
pub async fn run_provider_event_stream(
    adapter: Arc<dyn ProviderAdapter>,
    state_tx: mpsc::Sender<StateMessage>,
    reconnect_policy: ReconnectPolicy,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    let mut reconnect_delay = reconnect_policy.initial_delay;

    loop {
        let snapshot = tokio::select! {
            biased;
            _ = &mut shutdown_rx => return,
            result = adapter.fetch_snapshot() => result,
        };

        match snapshot {
            Ok(updates) => {
                if !updates.is_empty()
                    && state_tx
                        .send(StateMessage::AgentSessionUpdates { updates })
                        .await
                        .is_err()
                {
                    return;
                }
            }
            Err(error) => {
                tracing::warn!(
                    provider = ?adapter.provider(),
                    error = %error,
                    "provider recovery snapshot failed, delaying event-stream connection"
                );
                if !wait_for_reconnect(reconnect_delay, &mut shutdown_rx).await {
                    return;
                }
                reconnect_delay = reconnect_policy.next_delay(reconnect_delay);
                continue;
            }
        }

        let (events_tx, mut events_rx) = mpsc::channel(64);
        let (connection_shutdown_tx, connection_shutdown_rx) = oneshot::channel();
        let listen = adapter.listen(events_tx, connection_shutdown_rx);
        tokio::pin!(listen);
        let mut connection_shutdown_tx = Some(connection_shutdown_tx);
        let mut received_event = false;

        let stream_result = loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_rx => {
                    if let Some(tx) = connection_shutdown_tx.take() {
                        let _ = tx.send(());
                    }
                    let _ = listen.await;
                    return;
                }
                result = &mut listen => break result,
                event = events_rx.recv() => {
                    let Some(event) = event else {
                        continue;
                    };
                    received_event = true;
                    if state_tx
                        .send(StateMessage::AgentSessionEvent { event })
                        .await
                        .is_err()
                    {
                        if let Some(tx) = connection_shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                        let _ = listen.await;
                        return;
                    }
                }
            }
        };

        while let Some(event) = events_rx.recv().await {
            received_event = true;
            if state_tx
                .send(StateMessage::AgentSessionEvent { event })
                .await
                .is_err()
            {
                return;
            }
        }

        match stream_result {
            Ok(()) => tracing::warn!(
                provider = ?adapter.provider(),
                "provider event stream closed, reconnecting"
            ),
            Err(ProviderAdapterError::Shutdown) => return,
            Err(error) => {
                tracing::warn!(
                    provider = ?adapter.provider(),
                    error = %error,
                    "provider event stream failed, reconnecting"
                );
            }
        }

        if received_event {
            reconnect_delay = reconnect_policy.initial_delay;
        }
        if !wait_for_reconnect(reconnect_delay, &mut shutdown_rx).await {
            return;
        }
        reconnect_delay = reconnect_policy.next_delay(reconnect_delay);
    }
}

async fn wait_for_reconnect(delay: Duration, shutdown_rx: &mut oneshot::Receiver<()>) -> bool {
    tokio::select! {
        biased;
        _ = shutdown_rx => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

/// Owned polling and event-supervisor tasks for one provider adapter.
pub struct ProviderTasks {
    provider: AgentProvider,
    poller_shutdown: oneshot::Sender<()>,
    poller_handle: tokio::task::JoinHandle<()>,
    event_shutdown: oneshot::Sender<()>,
    event_handle: tokio::task::JoinHandle<()>,
}

/// Start both recovery polling and live event supervision for one adapter.
pub fn spawn_provider_tasks(
    adapter: Arc<dyn ProviderAdapter>,
    state_tx: mpsc::Sender<StateMessage>,
    poll_interval: Duration,
    reconnect_policy: ReconnectPolicy,
) -> ProviderTasks {
    let provider = adapter.provider();

    let (poller_shutdown, poller_shutdown_rx) = oneshot::channel();
    let poller_handle = tokio::spawn(run_provider_polling(
        adapter.clone(),
        state_tx.clone(),
        poll_interval,
        poller_shutdown_rx,
    ));

    let (event_shutdown, event_shutdown_rx) = oneshot::channel();
    let event_handle = tokio::spawn(run_provider_event_stream(
        adapter,
        state_tx,
        reconnect_policy,
        event_shutdown_rx,
    ));

    ProviderTasks {
        provider,
        poller_shutdown,
        poller_handle,
        event_shutdown,
        event_handle,
    }
}

impl ProviderTasks {
    /// Stop live events first, then polling, and await both tasks.
    pub async fn shutdown(self) {
        let _ = self.event_shutdown.send(());
        if let Err(error) = self.event_handle.await {
            tracing::warn!(
                provider = ?self.provider,
                error = %error,
                "provider event-supervisor task panicked"
            );
        }

        let _ = self.poller_shutdown.send(());
        if let Err(error) = self.poller_handle.await {
            tracing::warn!(
                provider = ?self.provider,
                error = %error,
                "provider poller task panicked"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use grove_lib::{DiscoverySource, SessionLifecycle};
    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::event_stream::EventKind;
    use crate::state::StateMessage;

    enum FakeListenResult {
        Return(Result<(), ProviderAdapterError>),
        WaitForShutdown,
    }

    type FakeEventScript = (Vec<AgentEvent>, FakeListenResult);

    struct FakeProviderAdapter {
        snapshots: Mutex<VecDeque<Result<Vec<SessionUpdate>, ProviderAdapterError>>>,
        event_scripts: Mutex<VecDeque<FakeEventScript>>,
    }

    impl fmt::Debug for FakeProviderAdapter {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("FakeProviderAdapter").finish()
        }
    }

    impl FakeProviderAdapter {
        fn new(snapshots: Vec<Result<Vec<SessionUpdate>, ProviderAdapterError>>) -> Self {
            Self {
                snapshots: Mutex::new(VecDeque::from(snapshots)),
                event_scripts: Mutex::new(VecDeque::new()),
            }
        }

        fn with_events(
            snapshots: Vec<Result<Vec<SessionUpdate>, ProviderAdapterError>>,
            event_scripts: Vec<FakeEventScript>,
        ) -> Self {
            Self {
                snapshots: Mutex::new(VecDeque::from(snapshots)),
                event_scripts: Mutex::new(VecDeque::from(event_scripts)),
            }
        }
    }

    impl ProviderAdapter for FakeProviderAdapter {
        fn provider(&self) -> AgentProvider {
            AgentProvider::Codex
        }

        fn capabilities(&self) -> ProviderCapabilities {
            ProviderCapabilities {
                snapshots: SnapshotCapability::FullMetadata,
                events: EventCapability::LifecycleStream,
                health: HealthCapability::ConnectionState,
                exact_file_activity: ExactFileActivityCapability::Unsupported,
                parent_sessions: ParentSessionCapability::Unsupported,
            }
        }

        fn fetch_snapshot(
            &self,
        ) -> AdapterFuture<'_, Result<Vec<SessionUpdate>, ProviderAdapterError>> {
            Box::pin(async move {
                self.snapshots
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(Ok(Vec::new()))
            })
        }

        fn listen(
            &self,
            events_tx: mpsc::Sender<AgentEvent>,
            shutdown_rx: oneshot::Receiver<()>,
        ) -> AdapterFuture<'_, Result<(), ProviderAdapterError>> {
            Box::pin(async move {
                let Some((events, result)) = self.event_scripts.lock().unwrap().pop_front() else {
                    return Err(ProviderAdapterError::Shutdown);
                };
                for event in events {
                    events_tx
                        .send(event)
                        .await
                        .map_err(|_| ProviderAdapterError::ChannelClosed)?;
                }
                match result {
                    FakeListenResult::Return(result) => result,
                    FakeListenResult::WaitForShutdown => {
                        let _ = shutdown_rx.await;
                        Err(ProviderAdapterError::Shutdown)
                    }
                }
            })
        }
    }

    fn session(id: &str) -> SessionUpdate {
        SessionUpdate {
            provider: AgentProvider::Codex,
            native_session_id: id.to_string(),
            parent_native_session_id: None,
            agent_role: None,
            directory: PathBuf::from("/tmp/provider-contract"),
            lifecycle: SessionLifecycle::Discovered,
            discovery_source: DiscoverySource::ApiPoll,
            created_at: None,
            updated_at: None,
        }
    }

    fn event(id: &str) -> AgentEvent {
        AgentEvent {
            provider: AgentProvider::Codex,
            native_session_id: Some(id.to_string()),
            parent_native_session_id: None,
            event_kind: EventKind::MessageUpdated,
            timestamp: None,
            lifecycle: None,
            agent_role: None,
            directory: None,
            compacted: false,
        }
    }

    #[tokio::test]
    async fn provider_poller_forwards_immediate_snapshot() {
        tokio::time::pause();
        let adapter: Arc<dyn ProviderAdapter> =
            Arc::new(FakeProviderAdapter::new(vec![Ok(vec![session(
                "snapshot-1",
            )])]));
        let (state_tx, mut state_rx) = mpsc::channel(4);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(run_provider_polling(
            adapter,
            state_tx,
            Duration::from_secs(60),
            shutdown_rx,
        ));

        match state_rx.recv().await.expect("immediate snapshot message") {
            StateMessage::AgentSessionUpdates { updates } => {
                assert_eq!(updates.len(), 1);
                assert_eq!(updates[0].provider, AgentProvider::Codex);
                assert_eq!(updates[0].native_session_id, "snapshot-1");
            }
            _ => panic!("expected AgentSessionUpdates"),
        }

        shutdown_tx.send(()).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn provider_poller_retries_after_sanitized_failure() {
        tokio::time::pause();
        let adapter: Arc<dyn ProviderAdapter> = Arc::new(FakeProviderAdapter::new(vec![
            Err(ProviderAdapterError::RequestFailed),
            Ok(vec![session("snapshot-retry")]),
        ]));
        let (state_tx, mut state_rx) = mpsc::channel(4);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(run_provider_polling(
            adapter,
            state_tx,
            Duration::from_secs(10),
            shutdown_rx,
        ));

        tokio::task::yield_now().await;
        assert!(
            state_rx.try_recv().is_err(),
            "failed snapshot must not mutate state"
        );

        tokio::time::advance(Duration::from_secs(10)).await;
        match state_rx.recv().await.expect("retry snapshot message") {
            StateMessage::AgentSessionUpdates { updates } => {
                assert_eq!(updates[0].native_session_id, "snapshot-retry");
            }
            _ => panic!("expected AgentSessionUpdates"),
        }

        shutdown_tx.send(()).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn provider_event_supervisor_recovers_snapshot_before_every_connection() {
        tokio::time::pause();
        let adapter: Arc<dyn ProviderAdapter> = Arc::new(FakeProviderAdapter::with_events(
            vec![
                Ok(vec![session("snapshot-1")]),
                Ok(vec![session("snapshot-2")]),
            ],
            vec![
                (
                    vec![event("event-1")],
                    FakeListenResult::Return(Err(ProviderAdapterError::RequestFailed)),
                ),
                (vec![event("event-2")], FakeListenResult::WaitForShutdown),
            ],
        ));
        let (state_tx, mut state_rx) = mpsc::channel(8);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(run_provider_event_stream(
            adapter,
            state_tx,
            ReconnectPolicy {
                initial_delay: Duration::from_secs(1),
                max_delay: Duration::from_secs(8),
            },
            shutdown_rx,
        ));

        match state_rx.recv().await.expect("first recovery snapshot") {
            StateMessage::AgentSessionUpdates { updates } => {
                assert_eq!(updates[0].native_session_id, "snapshot-1");
            }
            _ => panic!("snapshot must precede first connection"),
        }
        match state_rx.recv().await.expect("first live event") {
            StateMessage::AgentSessionEvent { event } => {
                assert_eq!(event.native_session_id.as_deref(), Some("event-1"));
            }
            _ => panic!("expected first normalized event"),
        }

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;

        match state_rx.recv().await.expect("reconnect recovery snapshot") {
            StateMessage::AgentSessionUpdates { updates } => {
                assert_eq!(updates[0].native_session_id, "snapshot-2");
            }
            _ => panic!("snapshot must precede reconnection"),
        }
        match state_rx.recv().await.expect("second live event") {
            StateMessage::AgentSessionEvent { event } => {
                assert_eq!(event.native_session_id.as_deref(), Some("event-2"));
            }
            _ => panic!("expected second normalized event"),
        }

        shutdown_tx.send(()).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn provider_tasks_start_both_recovery_paths_and_stop_cleanly() {
        tokio::time::pause();
        let adapter: Arc<dyn ProviderAdapter> = Arc::new(FakeProviderAdapter::with_events(
            vec![
                Ok(vec![session("shared-snapshot")]),
                Ok(vec![session("shared-snapshot")]),
            ],
            vec![(Vec::new(), FakeListenResult::WaitForShutdown)],
        ));
        let (state_tx, mut state_rx) = mpsc::channel(8);

        let tasks = spawn_provider_tasks(
            adapter,
            state_tx,
            Duration::from_secs(60),
            ReconnectPolicy::default(),
        );

        for _ in 0..2 {
            match state_rx.recv().await.expect("initial recovery snapshot") {
                StateMessage::AgentSessionUpdates { updates } => {
                    assert_eq!(updates[0].native_session_id, "shared-snapshot");
                }
                _ => panic!("expected AgentSessionUpdates"),
            }
        }

        tokio::time::timeout(Duration::from_secs(1), tasks.shutdown())
            .await
            .expect("provider tasks should stop cleanly");
    }
}

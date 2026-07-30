use std::fmt;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use grove_lib::{AgentProvider, SessionLifecycle};
use reqwest::StatusCode;
use serde::Deserialize;

// ── Event kind ──────────────────────────────────────────────────────────

/// Recognised OpenCode event types.
///
/// Unknown event types are silently ignored by the parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventKind {
    ServerConnected,
    SessionCreated,
    SessionUpdated,
    SessionCompacted,
    SessionDeleted,
    MessagePartUpdated,
    MessageUpdated,
}

impl EventKind {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "server.connected" => Some(Self::ServerConnected),
            "session.created" => Some(Self::SessionCreated),
            "session.updated" => Some(Self::SessionUpdated),
            "session.compacted" => Some(Self::SessionCompacted),
            "session.deleted" => Some(Self::SessionDeleted),
            "message.part.updated" => Some(Self::MessagePartUpdated),
            "message.updated" => Some(Self::MessageUpdated),
            _ => None,
        }
    }
}

// ── Provider-neutral event ──────────────────────────────────────────────

/// A metadata-safe, provider-neutral event from the OpenCode event stream.
///
/// Only allowlisted fields are retained.  Sensitive content such as messages,
/// commands, tool I/O, reasoning, credentials, titles, prompts, and
/// responses are **never** extracted, returned, stored, or logged.
#[derive(Debug, Clone)]
pub struct AgentEvent {
    pub provider: AgentProvider,
    pub native_session_id: Option<String>,
    pub parent_native_session_id: Option<String>,
    pub event_kind: EventKind,
    pub timestamp: Option<DateTime<Utc>>,
    pub lifecycle: Option<SessionLifecycle>,
    pub agent_role: Option<String>,
    pub directory: Option<PathBuf>,
    pub compacted: bool,
}

// ── Error type ──────────────────────────────────────────────────────────

/// Errors that can occur during event stream consumption.
///
/// The `Display` and `Debug` impls never include credentials, raw event
/// payloads, or any other sensitive data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventStreamError {
    RequestFailed,
    AuthenticationFailed,
    UnexpectedStatusCode(u16),
    InvalidEventStream,
    ChannelClosed,
    Shutdown,
}

impl fmt::Display for EventStreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RequestFailed => write!(f, "event stream request failed"),
            Self::AuthenticationFailed => write!(f, "authentication failed"),
            Self::UnexpectedStatusCode(code) => {
                write!(f, "unexpected HTTP status code {code}")
            }
            Self::InvalidEventStream => write!(f, "invalid event stream"),
            Self::ChannelClosed => write!(f, "event channel closed"),
            Self::Shutdown => write!(f, "shutdown signal received"),
        }
    }
}

impl std::error::Error for EventStreamError {}

// ── Event stream client ────────────────────────────────────────────────

/// HTTP client that connects to the OpenCode SSE endpoint and yields
/// metadata-safe [`AgentEvent`] values.
///
/// Credentials are redacted in `Debug` output and never appear in error
/// messages.
pub struct EventStreamClient {
    base_url: String,
    username: String,
    password: String,
    client: reqwest::Client,
}

impl fmt::Debug for EventStreamClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventStreamClient")
            .field("base_url", &"<configured>")
            .field("username", &"***")
            .field("password", &"***")
            .finish()
    }
}

impl EventStreamClient {
    pub fn new(base_url: &str, username: &str, password: &str) -> Self {
        let client = reqwest::Client::builder()
            .user_agent("grove-daemon")
            .build()
            .expect("reqwest Client should build");

        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            username: username.to_string(),
            password: password.to_string(),
            client,
        }
    }

    /// Connect to the OpenCode SSE stream and forward parsed events.
    ///
    /// Returns when:
    /// - The server closes the connection  (`Ok(())`)
    /// - A shutdown signal is received    (`Err(Shutdown)`)
    /// - An auth / request error occurs   (`Err(AuthenticationFailed | RequestFailed)`)
    ///
    /// Raw event JSON is consumed during parsing and never retained.
    /// Unknown event types are silently ignored.
    /// Credentials are never exposed in errors or Debug output.
    pub async fn listen(
        &self,
        events_tx: tokio::sync::mpsc::Sender<AgentEvent>,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), EventStreamError> {
        use futures::StreamExt;

        let url = format!("{}/api/event", self.base_url);

        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|_| EventStreamError::RequestFailed)?;

        let status = response.status();
        if status == StatusCode::UNAUTHORIZED {
            return Err(EventStreamError::AuthenticationFailed);
        }
        if !status.is_success() {
            return Err(EventStreamError::UnexpectedStatusCode(status.as_u16()));
        }

        let mut parser = SseParser::new();
        let mut stream = response.bytes_stream();

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    return Err(EventStreamError::Shutdown);
                }
                chunk = stream.next() => {
                    match chunk {
                        Some(Ok(bytes)) => {
                            let events = parser.feed(&bytes);
                            Self::forward_events(&events_tx, events).await?;
                        }
                        Some(Err(_)) => {
                            return Err(EventStreamError::InvalidEventStream);
                        }
                        None => {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    async fn forward_events(
        tx: &tokio::sync::mpsc::Sender<AgentEvent>,
        events: Vec<AgentEvent>,
    ) -> Result<(), EventStreamError> {
        for event in events {
            if tx.send(event).await.is_err() {
                return Err(EventStreamError::ChannelClosed);
            }
        }
        Ok(())
    }
}

// ── SSE parser (private) ────────────────────────────────────────────────

/// Incremental SSE parser that yields `AgentEvent` values.
///
/// The OpenCode SSE stream uses `data:` lines containing a JSON envelope:
///
/// ```text
/// data: {"id":"evt_...","type":"<event_type>","data":{<payload>}}
///
/// : heartbeat
/// ```
///
/// There are no `event:` SSE fields — the event type lives inside the JSON.
/// Heartbeat lines (`: heartbeat`) and other SSE comment lines are ignored.
struct SseParser {
    line_buffer: Vec<u8>,
    event_data: Vec<u8>,
    discard_event: bool,
}

impl SseParser {
    const MAX_EVENT_BYTES: usize = 256 * 1024;

    fn new() -> Self {
        Self {
            line_buffer: Vec::new(),
            event_data: Vec::new(),
            discard_event: false,
        }
    }

    /// Feed raw bytes and return any complete events that were parsed.
    fn feed(&mut self, bytes: &[u8]) -> Vec<AgentEvent> {
        let mut events = Vec::new();

        for &byte in bytes {
            if byte == b'\n' {
                let mut line = std::mem::take(&mut self.line_buffer);
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                self.handle_line(&line, &mut events);
            } else if self.line_buffer.len() < Self::MAX_EVENT_BYTES {
                self.line_buffer.push(byte);
            } else {
                self.line_buffer.clear();
                self.event_data.clear();
                self.discard_event = true;
            }
        }

        events
    }

    fn handle_line(&mut self, line: &[u8], events: &mut Vec<AgentEvent>) {
        if line.is_empty() {
            if !self.discard_event
                && !self.event_data.is_empty()
                && let Some(event) = Self::parse_event(&self.event_data)
            {
                events.push(event);
            }
            self.event_data.clear();
            self.discard_event = false;
            return;
        }

        if self.discard_event {
            return;
        }

        let Some(mut data) = line.strip_prefix(b"data:") else {
            return;
        };
        if data.first() == Some(&b' ') {
            data = &data[1..];
        }

        let extra = data.len() + usize::from(!self.event_data.is_empty());
        if self.event_data.len().saturating_add(extra) > Self::MAX_EVENT_BYTES {
            self.event_data.clear();
            self.discard_event = true;
            return;
        }

        if !self.event_data.is_empty() {
            self.event_data.push(b'\n');
        }
        self.event_data.extend_from_slice(data);
    }

    /// Parse one complete event through sparse, allowlisted structures.
    ///
    /// Unknown fields are discarded by Serde during deserialization. The raw
    /// JSON byte buffer is dropped immediately after this call.
    fn parse_event(data: &[u8]) -> Option<AgentEvent> {
        let envelope: SparseEventEnvelope = serde_json::from_slice(data).ok()?;
        let kind = EventKind::from_str(envelope.event_type.as_deref()?)?;

        // OpenCode 1.18.x uses `data`; current bus events use `properties`.
        // Some wrappers place the bus `properties` object inside `data`.
        let outer_payload = envelope.data.as_ref().or(envelope.properties.as_ref());
        let payload = outer_payload
            .and_then(|value| value.properties.as_deref())
            .or(outer_payload);
        let info = payload.and_then(|value| value.info.as_deref());
        let part = payload.and_then(|value| value.part.as_ref());

        let native_session_id = payload
            .and_then(|value| value.session_id.clone())
            .or_else(|| info.and_then(|value| value.session_id.clone()))
            .or_else(|| part.and_then(|value| value.session_id.clone()))
            .or_else(|| {
                matches!(
                    kind,
                    EventKind::SessionCreated
                        | EventKind::SessionUpdated
                        | EventKind::SessionCompacted
                        | EventKind::SessionDeleted
                )
                .then(|| {
                    payload
                        .and_then(|value| value.id.clone())
                        .or_else(|| info.and_then(|value| value.id.clone()))
                })
                .flatten()
            });

        let directory = envelope
            .location
            .as_ref()
            .and_then(|value| value.directory.as_deref())
            .map(PathBuf::from)
            .or_else(|| envelope.directory.as_deref().map(PathBuf::from))
            .or_else(|| {
                payload
                    .and_then(|value| value.location.as_ref())
                    .and_then(|value| value.directory.as_deref())
                    .map(PathBuf::from)
            })
            .or_else(|| {
                payload
                    .and_then(|value| value.directory.as_deref())
                    .map(PathBuf::from)
            })
            .or_else(|| {
                info.and_then(|value| value.location.as_ref())
                    .and_then(|value| value.directory.as_deref())
                    .map(PathBuf::from)
            })
            .or_else(|| {
                info.and_then(|value| value.directory.as_deref())
                    .map(PathBuf::from)
            });

        let lifecycle = part
            .and_then(|value| value.state.as_ref())
            .and_then(|value| value.status.as_deref())
            .and_then(part_status_to_lifecycle)
            .or_else(|| {
                payload
                    .and_then(|value| value.lifecycle.as_deref())
                    .and_then(lifecycle_from_str)
            })
            .or_else(|| {
                info.and_then(|value| value.lifecycle.as_deref())
                    .and_then(lifecycle_from_str)
            });

        let timestamp = payload
            .and_then(|value| value.timestamp)
            .or_else(|| info.and_then(|value| value.timestamp))
            .or_else(|| {
                part.and_then(|value| value.state.as_ref())
                    .and_then(|value| value.time.as_ref())
                    .and_then(SparseTime::latest_activity)
            })
            .or_else(|| {
                part.and_then(|value| value.time.as_ref())
                    .and_then(SparseTime::latest_activity)
            })
            .or_else(|| {
                payload
                    .and_then(|value| value.time.as_ref())
                    .and_then(|value| value.for_event(&kind))
            })
            .or_else(|| {
                info.and_then(|value| value.time.as_ref())
                    .and_then(|value| value.for_event(&kind))
            })
            .and_then(ms_to_datetime);
        let compacted = matches!(kind, EventKind::SessionCompacted);

        Some(AgentEvent {
            provider: AgentProvider::OpenCode,
            native_session_id,
            parent_native_session_id: payload
                .and_then(|value| value.parent_id.clone())
                .or_else(|| info.and_then(|value| value.parent_id.clone())),
            event_kind: kind,
            timestamp,
            lifecycle,
            agent_role: payload
                .and_then(|value| value.agent.clone())
                .or_else(|| info.and_then(|value| value.agent.clone())),
            directory,
            compacted,
        })
    }
}

#[derive(Default, Deserialize)]
struct SparseEventEnvelope {
    #[serde(default, rename = "type")]
    event_type: Option<String>,
    #[serde(default)]
    data: Option<SparseEventPayload>,
    #[serde(default)]
    properties: Option<SparseEventPayload>,
    #[serde(default)]
    location: Option<SparseLocation>,
    #[serde(default)]
    directory: Option<String>,
}

#[derive(Default, Deserialize)]
struct SparseEventPayload {
    #[serde(default)]
    id: Option<String>,
    #[serde(default, rename = "sessionID")]
    session_id: Option<String>,
    #[serde(default, rename = "parentID")]
    parent_id: Option<String>,
    #[serde(default)]
    agent: Option<String>,
    #[serde(default)]
    directory: Option<String>,
    #[serde(default)]
    location: Option<SparseLocation>,
    #[serde(default)]
    time: Option<SparseTime>,
    #[serde(default)]
    timestamp: Option<i64>,
    #[serde(default)]
    lifecycle: Option<String>,
    #[serde(default)]
    info: Option<Box<SparseEventPayload>>,
    #[serde(default)]
    part: Option<SparsePart>,
    #[serde(default)]
    properties: Option<Box<SparseEventPayload>>,
}

#[derive(Default, Deserialize)]
struct SparseLocation {
    #[serde(default)]
    directory: Option<String>,
}

#[derive(Default, Deserialize)]
struct SparsePart {
    #[serde(default, rename = "sessionID")]
    session_id: Option<String>,
    #[serde(default)]
    state: Option<SparsePartState>,
    #[serde(default)]
    time: Option<SparseTime>,
}

#[derive(Default, Deserialize)]
struct SparsePartState {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    time: Option<SparseTime>,
}

#[derive(Default, Deserialize)]
struct SparseTime {
    #[serde(default)]
    created: Option<i64>,
    #[serde(default)]
    updated: Option<i64>,
    #[serde(default)]
    start: Option<i64>,
    #[serde(default)]
    end: Option<i64>,
    #[serde(default)]
    completed: Option<i64>,
}

impl SparseTime {
    fn latest_activity(&self) -> Option<i64> {
        self.completed
            .or(self.end)
            .or(self.updated)
            .or(self.start)
            .or(self.created)
    }

    fn for_event(&self, kind: &EventKind) -> Option<i64> {
        match kind {
            EventKind::SessionCreated => self.created.or(self.updated),
            EventKind::SessionUpdated | EventKind::SessionCompacted | EventKind::SessionDeleted => {
                self.updated.or(self.created)
            }
            EventKind::ServerConnected
            | EventKind::MessagePartUpdated
            | EventKind::MessageUpdated => self.latest_activity(),
        }
    }
}

fn lifecycle_from_str(value: &str) -> Option<SessionLifecycle> {
    match value {
        "active" => Some(SessionLifecycle::Active),
        "idle" => Some(SessionLifecycle::Idle),
        "stopped" => Some(SessionLifecycle::Stopped),
        "stale" => Some(SessionLifecycle::Stale),
        "discovered" => Some(SessionLifecycle::Discovered),
        _ => None,
    }
}

fn part_status_to_lifecycle(value: &str) -> Option<SessionLifecycle> {
    match value {
        "running" | "pending" | "streaming" => Some(SessionLifecycle::Active),
        "completed" => Some(SessionLifecycle::Idle),
        _ => None,
    }
}

fn ms_to_datetime(ms: i64) -> Option<DateTime<Utc>> {
    if ms < 0 {
        return None;
    }
    let secs = ms / 1000;
    let nsecs = ((ms % 1000) * 1_000_000) as u32;
    DateTime::from_timestamp(secs, nsecs)
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::Path;

    // ── Helpers ────────────────────────────────────────────────────────

    struct MockServer(Option<std::thread::JoinHandle<()>>);

    impl Drop for MockServer {
        fn drop(&mut self) {
            if let Some(h) = self.0.take() {
                let _ = h.join();
            }
        }
    }

    fn spawn_mock_sse(events: &[&str], status: u16) -> (String, MockServer) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}");
        let body: Vec<String> = events.iter().map(|s| s.to_string()).collect();

        let handle = std::thread::spawn(move || {
            if let Some(Ok(mut stream)) = listener.incoming().next() {
                let mut buf = [0u8; 4096];
                let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(500)));
                let _ = stream.read(&mut buf);

                let header = if status == 200 {
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n"
                } else {
                    "HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                };
                let mut response = header.to_string();
                for event in &body {
                    response.push_str(event);
                }
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });

        (url, MockServer(Some(handle)))
    }

    fn make_client(url: &str) -> EventStreamClient {
        EventStreamClient::new(url, "testuser", "testpass")
    }

    /// Run the listener and collect up to `n` events, then shut down.
    async fn collect_events(
        client: EventStreamClient,
        n: usize,
    ) -> (Vec<AgentEvent>, Result<(), EventStreamError>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let handle = tokio::spawn(async move { client.listen(tx, shutdown_rx).await });

        let mut events = Vec::with_capacity(n);
        for _ in 0..n {
            match tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv()).await {
                Ok(Some(event)) => events.push(event),
                Ok(None) => break,
                Err(_) => break,
            }
        }

        let _ = shutdown_tx.send(());
        let result = handle.await.unwrap();
        (events, result)
    }

    // ── server.connected ───────────────────────────────────────────────

    #[tokio::test]
    async fn server_connected_event() {
        let events = vec![
            "data: {\"id\":\"evt_1\",\"type\":\"server.connected\",\"data\":{}}\n",
            "\n",
        ];
        let (url, _server) = spawn_mock_sse(&events, 200);
        let client = make_client(&url);

        let (parsed, _) = collect_events(client, 1).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].event_kind, EventKind::ServerConnected);
        assert_eq!(parsed[0].provider, AgentProvider::OpenCode);
    }

    // ── session.created metadata ───────────────────────────────────────

    #[tokio::test]
    async fn session_created_metadata() {
        let events = vec![
            "data: {\"id\":\"evt_1\",\"type\":\"session.created\",\"data\":{\"id\":\"ses-1\",\"parentID\":null,\"agent\":\"opencode\",\"location\":{\"directory\":\"/home/user/pro\"},\"time\":{\"created\":1718455200000,\"updated\":1718458800000}}}\n",
            "\n",
        ];
        let (url, _server) = spawn_mock_sse(&events, 200);
        let client = make_client(&url);

        let (parsed, _) = collect_events(client, 1).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].event_kind, EventKind::SessionCreated);
        assert_eq!(parsed[0].native_session_id.as_deref(), Some("ses-1"));
        assert!(parsed[0].parent_native_session_id.is_none());
        assert_eq!(parsed[0].agent_role.as_deref(), Some("opencode"));
        assert_eq!(
            parsed[0].directory.as_deref(),
            Some(Path::new("/home/user/pro"))
        );
        assert!(parsed[0].timestamp.is_some());
    }

    // ── parent / child session metadata ────────────────────────────────

    #[tokio::test]
    async fn modern_properties_info_session_metadata_is_normalized() {
        let events = vec![
            "data: {\"type\":\"session.created\",\"properties\":{\"info\":{\"id\":\"ses-modern\",\"parentID\":\"parent-modern\",\"directory\":\"/home/user/modern\",\"time\":{\"created\":1718455200000,\"updated\":1718458800000}}}}\n",
            "\n",
        ];
        let (url, _server) = spawn_mock_sse(&events, 200);
        let client = make_client(&url);

        let (parsed, _) = collect_events(client, 1).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].native_session_id.as_deref(), Some("ses-modern"));
        assert_eq!(
            parsed[0].parent_native_session_id.as_deref(),
            Some("parent-modern")
        );
        assert_eq!(
            parsed[0].directory.as_deref(),
            Some(Path::new("/home/user/modern"))
        );
        assert_eq!(
            parsed[0].timestamp.unwrap().timestamp_millis(),
            1718455200000
        );
    }

    #[tokio::test]
    async fn parent_child_session_metadata() {
        let events = vec![
            "data: {\"id\":\"evt_1\",\"type\":\"session.created\",\"data\":{\"id\":\"child-1\",\"parentID\":\"parent-1\",\"agent\":null,\"location\":null}}\n",
            "\n",
        ];
        let (url, _server) = spawn_mock_sse(&events, 200);
        let client = make_client(&url);

        let (parsed, _) = collect_events(client, 1).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].event_kind, EventKind::SessionCreated);
        assert_eq!(parsed[0].native_session_id.as_deref(), Some("child-1"));
        assert_eq!(
            parsed[0].parent_native_session_id.as_deref(),
            Some("parent-1")
        );
        assert!(parsed[0].agent_role.is_none());
        assert!(parsed[0].directory.is_none());
    }

    // ── running and completed part statuses ────────────────────────────

    #[tokio::test]
    async fn running_pending_and_completed_part_status() {
        let events = vec![
            "data: {\"id\":\"evt_1\",\"type\":\"message.part.updated\",\"location\":{\"directory\":\"/tmp\"},\"data\":{\"sessionID\":\"ses-1\",\"part\":{\"state\":{\"status\":\"running\",\"time\":{\"start\":1718455200000}}}}}\n",
            "\n",
            "data: {\"id\":\"evt_2\",\"type\":\"message.part.updated\",\"location\":{\"directory\":\"/tmp\"},\"data\":{\"sessionID\":\"ses-1\",\"part\":{\"state\":{\"status\":\"completed\",\"time\":{\"start\":1718458800000}}}}}\n",
            "\n",
        ];
        let (url, _server) = spawn_mock_sse(&events, 200);
        let client = make_client(&url);

        let (parsed, _) = collect_events(client, 2).await;
        assert_eq!(parsed.len(), 2);

        assert_eq!(parsed[0].event_kind, EventKind::MessagePartUpdated);
        assert_eq!(parsed[0].native_session_id.as_deref(), Some("ses-1"));
        assert_eq!(parsed[0].lifecycle, Some(SessionLifecycle::Active));
        assert!(parsed[0].timestamp.is_some());
        assert!(parsed[0].agent_role.is_none());
        assert!(parsed[0].parent_native_session_id.is_none());
        assert!(!parsed[0].compacted);

        assert_eq!(parsed[1].event_kind, EventKind::MessagePartUpdated);
        assert_eq!(parsed[1].native_session_id.as_deref(), Some("ses-1"));
        assert_eq!(parsed[1].lifecycle, Some(SessionLifecycle::Idle));
        assert!(parsed[1].timestamp.is_some());
    }

    // ── session.compacted ──────────────────────────────────────────────

    #[tokio::test]
    async fn session_compacted_event() {
        let events = vec![
            "data: {\"id\":\"evt_1\",\"type\":\"session.compacted\",\"data\":{\"id\":\"ses-1\"}}\n",
            "\n",
        ];
        let (url, _server) = spawn_mock_sse(&events, 200);
        let client = make_client(&url);

        let (parsed, _) = collect_events(client, 1).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].event_kind, EventKind::SessionCompacted);
        assert_eq!(parsed[0].native_session_id.as_deref(), Some("ses-1"));
        assert!(parsed[0].compacted);
    }

    // ── session.deleted ────────────────────────────────────────────────

    #[tokio::test]
    async fn session_deleted_event() {
        let events = vec![
            "data: {\"id\":\"evt_1\",\"type\":\"session.deleted\",\"data\":{\"id\":\"ses-1\"}}\n",
            "\n",
        ];
        let (url, _server) = spawn_mock_sse(&events, 200);
        let client = make_client(&url);

        let (parsed, _) = collect_events(client, 1).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].event_kind, EventKind::SessionDeleted);
        assert_eq!(parsed[0].native_session_id.as_deref(), Some("ses-1"));
    }

    // ── unknown event ignored ──────────────────────────────────────────

    #[tokio::test]
    async fn unknown_event_ignored() {
        let events = vec![
            "data: {\"id\":\"evt_1\",\"type\":\"message.created\",\"data\":{\"id\":\"ses-1\"}}\n",
            "\n",
            "data: {\"id\":\"evt_2\",\"type\":\"some.random.event\",\"data\":{}}\n",
            "\n",
            "data: {\"id\":\"evt_3\",\"type\":\"server.connected\",\"data\":{}}\n",
            "\n",
        ];
        let (url, _server) = spawn_mock_sse(&events, 200);
        let client = make_client(&url);

        let (parsed, _) = collect_events(client, 1).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].event_kind, EventKind::ServerConnected);
    }

    // ── sensitive fields ignored ───────────────────────────────────────

    #[tokio::test]
    async fn sensitive_fields_ignored() {
        let events = vec![
            "data: {\"id\":\"evt_1\",\"type\":\"session.created\",\"data\":{\"id\":\"ses-1\",\"title\":\"My Conversation\",\"prompts\":[\"hello\"],\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"parts\":[{\"type\":\"text\",\"text\":\"hello\"}],\"tool_inputs\":{\"cmd\":\"ls\"},\"tool_outputs\":{\"result\":\"file1\"},\"commands\":[\"ls\"],\"model_responses\":[\"I can help\"],\"file_contents\":{\"/path/to/file\":\"secret\"},\"credentials\":{\"api_key\":\"sk-1234\"},\"reasoning\":\"thinking...\"}}\n",
            "\n",
        ];
        let (url, _server) = spawn_mock_sse(&events, 200);
        let client = make_client(&url);

        let (parsed, _) = collect_events(client, 1).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].event_kind, EventKind::SessionCreated);
        assert_eq!(parsed[0].native_session_id.as_deref(), Some("ses-1"));
        assert!(parsed[0].agent_role.is_none());
        assert!(parsed[0].parent_native_session_id.is_none());
        assert!(parsed[0].directory.is_none());
    }

    // ── malformed event handled safely ─────────────────────────────────

    #[tokio::test]
    async fn malformed_event_handled_safely() {
        let events = vec![
            "data: not-json\n",
            "\n",
            "data: {\"id\":\"evt_2\",\"type\":\"server.connected\",\"data\":{}}\n",
            "\n",
        ];
        let (url, _server) = spawn_mock_sse(&events, 200);
        let client = make_client(&url);

        let (parsed, _) = collect_events(client, 1).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].event_kind, EventKind::ServerConnected);
    }

    // ── heartbeat lines ignored ────────────────────────────────────────

    #[tokio::test]
    async fn heartbeat_line_ignored() {
        let events = vec![
            ": heartbeat\n",
            "\n",
            "data: {\"id\":\"evt_1\",\"type\":\"server.connected\",\"data\":{}}\n",
            "\n",
        ];
        let (url, _server) = spawn_mock_sse(&events, 200);
        let client = make_client(&url);

        let (parsed, _) = collect_events(client, 1).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].event_kind, EventKind::ServerConnected);
    }

    // ── shutdown closes the stream ─────────────────────────────────────

    #[tokio::test]
    async fn shutdown_closes_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}");

        let server_handle = std::thread::spawn(move || {
            if let Some(Ok(mut stream)) = listener.incoming().next() {
                let mut buf = [0u8; 4096];
                let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(500)));
                let _ = stream.read(&mut buf);
                let header = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n";
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.flush();
                std::thread::sleep(std::time::Duration::from_millis(100));
                let event =
                    "data: {\"id\":\"evt_1\",\"type\":\"server.connected\",\"data\":{}}\n\n";
                let _ = stream.write_all(event.as_bytes());
                let _ = stream.flush();
                std::thread::sleep(std::time::Duration::from_secs(10));
            }
        });

        let client = EventStreamClient::new(&url, "testuser", "testpass");
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let handle = tokio::spawn(async move { client.listen(tx, shutdown_rx).await });

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        shutdown_tx.send(()).unwrap();

        let result = handle.await.unwrap();
        assert_eq!(result, Err(EventStreamError::Shutdown));
        server_handle.join().unwrap();
    }

    // ── credentials absent from Debug and errors ───────────────────────

    #[test]
    fn debug_hides_credentials() {
        let client = EventStreamClient::new("http://localhost:8080", "secretuser", "secretpass");
        let debug_str = format!("{client:?}");
        assert!(
            !debug_str.contains("secretuser"),
            "Debug must not contain username: {debug_str}"
        );
        assert!(
            !debug_str.contains("secretpass"),
            "Debug must not contain password: {debug_str}"
        );
        assert!(
            debug_str.contains("***"),
            "Debug should mask credentials: {debug_str}"
        );
    }

    #[test]
    fn debug_does_not_expose_url_userinfo_path_or_query() {
        let client = EventStreamClient::new(
            "http://url-user:url-pass@localhost:8080/private-token?access_token=query-secret",
            "basic-user",
            "basic-pass",
        );
        let debug = format!("{client:?}");

        for secret in [
            "url-user",
            "url-pass",
            "private-token",
            "query-secret",
            "basic-user",
            "basic-pass",
        ] {
            assert!(!debug.contains(secret), "Debug exposed {secret}: {debug}");
        }
    }

    #[test]
    fn error_messages_sanitized() {
        let err = EventStreamError::AuthenticationFailed;
        let msg = err.to_string();
        assert_eq!(msg, "authentication failed");
        assert!(!msg.contains("testuser"));
        assert!(!msg.contains("testpass"));

        let err = EventStreamError::RequestFailed;
        let msg = err.to_string();
        assert_eq!(msg, "event stream request failed");
    }

    // ── authentication failure ─────────────────────────────────────────

    #[tokio::test]
    async fn authentication_failure() {
        let (url, _server) = spawn_mock_sse(&[], 401);
        let client = make_client(&url);
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let result = client.listen(tx, shutdown_rx).await;
        assert_eq!(result, Err(EventStreamError::AuthenticationFailed));
    }

    // ── incremental parsing across chunks (direct parser test) ──────────

    #[test]
    fn incremental_parsing_across_chunks() {
        let mut parser = SseParser::new();

        let events = parser
            .feed(b"data: {\"id\":\"evt_1\",\"type\":\"session.created\",\"data\":{\"id\":\"se");
        assert!(events.is_empty(), "no event until blank line");

        let events = parser.feed(b"s-1\"}}\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_kind, EventKind::SessionCreated);
        assert_eq!(events[0].native_session_id.as_deref(), Some("ses-1"));
    }

    // ── message.updated event ──────────────────────────────────────────

    #[tokio::test]
    async fn message_updated_event() {
        let events = vec![
            "data: {\"id\":\"evt_1\",\"type\":\"message.updated\",\"data\":{\"sessionID\":\"ses-1\",\"timestamp\":1718455200000}}\n",
            "\n",
        ];
        let (url, _server) = spawn_mock_sse(&events, 200);
        let client = make_client(&url);

        let (parsed, _) = collect_events(client, 1).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].event_kind, EventKind::MessageUpdated);
        assert_eq!(parsed[0].native_session_id.as_deref(), Some("ses-1"));
        assert!(parsed[0].timestamp.is_some());
    }

    #[tokio::test]
    async fn modern_message_info_session_id_is_normalized() {
        let events = vec![
            "data: {\"type\":\"message.updated\",\"properties\":{\"info\":{\"id\":\"msg-1\",\"sessionID\":\"ses-modern\",\"time\":{\"created\":1718455200000}}}}\n",
            "\n",
        ];
        let (url, _server) = spawn_mock_sse(&events, 200);
        let client = make_client(&url);

        let (parsed, _) = collect_events(client, 1).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].event_kind, EventKind::MessageUpdated);
        assert_eq!(parsed[0].native_session_id.as_deref(), Some("ses-modern"));
    }

    // ── no-op on data lines without blank line terminator ──────────────

    #[test]
    fn no_event_without_blank_line() {
        let mut parser = SseParser::new();
        let events =
            parser.feed(b"data: {\"id\":\"evt_1\",\"type\":\"server.connected\",\"data\":{}}\n");
        assert!(events.is_empty());
    }

    #[test]
    fn parser_accepts_crlf_and_data_field_without_space() {
        let mut parser = SseParser::new();
        let events = parser.feed(b"data:{\"type\":\"server.connected\",\"data\":{}}\r\n\r\n");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_kind, EventKind::ServerConnected);
    }
}

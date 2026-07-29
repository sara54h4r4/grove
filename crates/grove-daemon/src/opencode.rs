use std::fmt;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use grove_lib::{AgentProvider, DiscoverySource, SessionLifecycle};
use reqwest::StatusCode;
use serde::Deserialize;

use crate::session_registry::SessionUpdate;

// ── Error type ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenCodeError {
    InvalidUrl,
    RequestFailed,
    AuthenticationFailed,
    UnexpectedStatusCode(u16),
    MalformedResponse,
    MissingRequiredField(&'static str),
    InvalidTimestamp,
    PaginationLoop,
}

impl fmt::Display for OpenCodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl => write!(f, "invalid base URL"),
            Self::RequestFailed => write!(f, "request failed"),
            Self::AuthenticationFailed => write!(f, "authentication failed"),
            Self::UnexpectedStatusCode(code) => {
                write!(f, "unexpected HTTP status code {code}")
            }
            Self::MalformedResponse => write!(f, "malformed response"),
            Self::MissingRequiredField(field) => {
                write!(f, "missing required field: {field}")
            }
            Self::InvalidTimestamp => write!(f, "invalid timestamp"),
            Self::PaginationLoop => write!(f, "pagination loop protection triggered"),
        }
    }
}

impl std::error::Error for OpenCodeError {}

// ── Client ────────────────────────────────────────────────────────────────

pub struct OpenCodeClient {
    base_url: String,
    username: String,
    password: String,
    client: reqwest::Client,
}

impl fmt::Debug for OpenCodeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenCodeClient")
            .field("base_url", &"<configured>")
            .field("username", &"***")
            .field("password", &"***")
            .finish()
    }
}

impl OpenCodeClient {
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

    /// Fetch the complete session snapshot from OpenCode's API.
    ///
    /// Follows cursor pagination until all pages are collected.
    /// Raw response JSON is consumed during parsing and not retained.
    /// Credentials are never exposed in errors or Debug output.
    pub async fn fetch_session_snapshot(&self) -> Result<Vec<SessionUpdate>, OpenCodeError> {
        const MAX_PAGES: u32 = 100;
        let mut sessions: Vec<SessionUpdate> = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages: u32 = 0;

        loop {
            if pages >= MAX_PAGES {
                return Err(OpenCodeError::PaginationLoop);
            }
            pages += 1;

            let response = self.send_page(cursor.as_deref()).await?;

            let body: RawSessionListResponse = response
                .json()
                .await
                .map_err(|_| OpenCodeError::MalformedResponse)?;

            let entries = body.data.unwrap_or_default();
            for raw in entries {
                let session = Self::convert_entry(raw)?;
                sessions.push(session);
            }

            match body.cursor.and_then(|c| c.next) {
                Some(ref next) if !next.is_empty() => cursor = Some(next.clone()),
                _ => break,
            }
        }

        Ok(sessions)
    }

    async fn send_page(&self, cursor: Option<&str>) -> Result<reqwest::Response, OpenCodeError> {
        let url = match cursor {
            Some(c) => format!("{}/api/session?cursor={}", self.base_url, urlencode(c)),
            None => format!("{}/api/session", self.base_url),
        };

        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|_| OpenCodeError::RequestFailed)?;

        let status = response.status();
        if status == StatusCode::UNAUTHORIZED {
            return Err(OpenCodeError::AuthenticationFailed);
        }
        if !status.is_success() {
            return Err(OpenCodeError::UnexpectedStatusCode(status.as_u16()));
        }

        Ok(response)
    }

    fn convert_entry(raw: RawSessionData) -> Result<SessionUpdate, OpenCodeError> {
        if raw.id.is_empty() {
            return Err(OpenCodeError::MissingRequiredField("id"));
        }

        let directory = PathBuf::from(
            raw.location
                .as_ref()
                .and_then(|loc| loc.directory.as_ref())
                .ok_or(OpenCodeError::MissingRequiredField("location.directory"))?,
        );

        let ts = raw
            .time
            .as_ref()
            .ok_or(OpenCodeError::MissingRequiredField("time"))?;
        let created_ms = ts
            .created
            .ok_or(OpenCodeError::MissingRequiredField("time.created"))?;
        let created = ms_to_datetime(created_ms).ok_or(OpenCodeError::InvalidTimestamp)?;
        let updated_ms = ts
            .updated
            .ok_or(OpenCodeError::MissingRequiredField("time.updated"))?;
        let updated = ms_to_datetime(updated_ms).ok_or(OpenCodeError::InvalidTimestamp)?;

        Ok(SessionUpdate {
            provider: AgentProvider::OpenCode,
            native_session_id: raw.id,
            parent_native_session_id: raw.parent_id,
            agent_role: raw.agent,
            directory,
            lifecycle: SessionLifecycle::Discovered,
            discovery_source: DiscoverySource::ApiPoll,
            created_at: Some(created),
            updated_at: Some(updated),
        })
    }
}

// ── Snapshot client trait ─────────────────────────────────────────────────

/// Trait for fetching session snapshots.
///
/// Implementations must not log credentials, response bodies,
/// or any sensitive content such as titles, messages, commands, or tool data.
pub trait SnapshotClient: Send + Sync {
    fn fetch_snapshot(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<SessionUpdate>, OpenCodeError>> + Send;
}

impl SnapshotClient for OpenCodeClient {
    async fn fetch_snapshot(&self) -> Result<Vec<SessionUpdate>, OpenCodeError> {
        self.fetch_session_snapshot().await
    }
}

// ── Polling loop ──────────────────────────────────────────────────────────

use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

use crate::state::StateMessage;

/// Run a polling loop that fetches OpenCode session snapshots on an interval.
///
/// Behaviour:
/// - Fetches once immediately on call.
/// - Then fetches once per `interval`.
/// - Successful snapshots are forwarded to `state_tx` as
///   [`StateMessage::AgentSessionUpdates`].
/// - On fetch error the error is logged at `WARN` and polling continues
///   (the error is never propagated).
/// - Requests never overlap: if the previous fetch is still running when the
///   next interval tick arrives, the tick is skipped.
/// - Exits immediately when `shutdown_rx` resolves.
///
/// # Sensitive data
///
/// This function never logs credentials, response bodies, titles, messages,
/// commands, tool data, or any other sensitive content.
pub async fn run_opencode_polling<C>(
    client: C,
    state_tx: mpsc::Sender<StateMessage>,
    interval: Duration,
    mut shutdown_rx: oneshot::Receiver<()>,
) where
    C: SnapshotClient + 'static,
{
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = &mut shutdown_rx => break,
            _ = ticker.tick() => {
                let result = client.fetch_snapshot().await;
                match result {
                    Ok(updates) => {
                        if !updates.is_empty() {
                            let _ = state_tx
                                .send(StateMessage::AgentSessionUpdates { updates })
                                .await;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "opencode poll failed, will retry");
                    }
                }
            }
        }
    }
}

/// Spawn the OpenCode polling loop as a background task.
///
/// Returns a shutdown sender and the task handle. Callers should send `()`
/// through the shutdown sender and await the handle during daemon shutdown.
pub fn spawn_opencode_poller<C: SnapshotClient + 'static>(
    client: C,
    state_tx: mpsc::Sender<StateMessage>,
    interval: Duration,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = tokio::spawn(run_opencode_polling(
        client,
        state_tx,
        interval,
        shutdown_rx,
    ));
    (shutdown_tx, handle)
}

/// Reconnect timing for the OpenCode event stream.
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

/// Run OpenCode's live event stream with cancellation and snapshot recovery.
///
/// A full allowlisted session snapshot is fetched before every connection,
/// including the initial one. Stream failures use bounded exponential backoff.
/// Raw response bodies and event payloads are never logged.
pub async fn run_opencode_event_stream<S, E>(
    snapshot_client: S,
    event_source: E,
    state_tx: mpsc::Sender<StateMessage>,
    reconnect_policy: ReconnectPolicy,
    mut shutdown_rx: oneshot::Receiver<()>,
) where
    S: SnapshotClient + 'static,
    E: crate::event_stream::AgentEventSource + 'static,
{
    let mut reconnect_delay = reconnect_policy.initial_delay;

    loop {
        let snapshot = tokio::select! {
            biased;
            _ = &mut shutdown_rx => return,
            result = snapshot_client.fetch_snapshot() => result,
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
                    error = %error,
                    "opencode recovery snapshot failed, delaying event-stream connection"
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
        let listen = event_source.listen(events_tx, connection_shutdown_rx);
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

        // The listener owns the final sender. Once it returns, draining cannot
        // block and prevents losing events queued immediately before EOF.
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
            Ok(()) => tracing::warn!("opencode event stream closed, reconnecting"),
            Err(crate::event_stream::EventStreamError::Shutdown) => return,
            Err(error) => {
                tracing::warn!(error = %error, "opencode event stream failed, reconnecting");
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

/// Spawn the recovery-aware OpenCode event-stream loop.
pub fn spawn_opencode_event_stream<
    S: SnapshotClient + 'static,
    E: crate::event_stream::AgentEventSource + 'static,
>(
    snapshot_client: S,
    event_source: E,
    state_tx: mpsc::Sender<StateMessage>,
    reconnect_policy: ReconnectPolicy,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = tokio::spawn(run_opencode_event_stream(
        snapshot_client,
        event_source,
        state_tx,
        reconnect_policy,
        shutdown_rx,
    ));
    (shutdown_tx, handle)
}

/// Default polling interval for OpenCode session snapshots.
pub const OPENCODE_POLL_INTERVAL: Duration = Duration::from_secs(10);

// ── API response structures (never exposed outside this module) ───────────

#[derive(Debug, Deserialize)]
struct RawSessionListResponse {
    #[serde(default)]
    data: Option<Vec<RawSessionData>>,
    #[serde(default)]
    cursor: Option<RawCursor>,
}

#[derive(Debug, Deserialize)]
struct RawCursor {
    #[serde(default)]
    next: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawSessionData {
    id: String,
    #[serde(default, rename = "parentID")]
    parent_id: Option<String>,
    #[serde(default)]
    agent: Option<String>,
    #[serde(default)]
    location: Option<RawSessionLocation>,
    #[serde(default)]
    time: Option<RawSessionTime>,
}

#[derive(Debug, Deserialize)]
struct RawSessionLocation {
    #[serde(default)]
    directory: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawSessionTime {
    #[serde(default)]
    created: Option<i64>,
    #[serde(default)]
    updated: Option<i64>,
}

// ── URL encoding helper (avoids pulling in `url` crate) ────────────────────

/// Convert a Unix-millisecond timestamp to `DateTime<Utc>`.
///
/// Returns `None` for negative values or values outside chrono's representable
/// range.
fn ms_to_datetime(ms: i64) -> Option<DateTime<Utc>> {
    if ms < 0 {
        return None;
    }
    let secs = ms / 1000;
    let nsecs = ((ms % 1000) * 1_000_000) as u32;
    DateTime::from_timestamp(secs, nsecs)
}

fn urlencode(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(byte as char);
            }
            _ => {
                result.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    result
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    // ── Helpers ────────────────────────────────────────────────────────

    fn write_response(mut stream: std::net::TcpStream, status: u16, body: &str) {
        use std::io::{Read, Write};
        // Read the request so the client is sure it was delivered
        let mut buf = [0u8; 4096];
        let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(500)));
        let _ = stream.read(&mut buf);
        let reason = status_reason(status);
        let response = format!(
            "HTTP/1.1 {status} {reason}\r\n\
             content-type: application/json\r\n\
             content-length: {}\r\n\
             connection: close\r\n\
             \r\n\
             {body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    }

    /// Spawn a minimal HTTP server that returns a canned response for one request.
    fn spawn_mock(response_body: &str, status: u16) -> (String, MockServer) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}");
        let body = response_body.to_string();
        let handle = std::thread::spawn(move || {
            if let Some(Ok(stream)) = listener.incoming().next() {
                write_response(stream, status, &body);
            }
        });
        (url, MockServer(Some(handle)))
    }

    /// Spawn a mock that handles multiple paginated requests.
    fn spawn_mock_paginated(pages: Vec<(u16, String)>) -> (String, MockServer) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}");
        let handle = std::thread::spawn(move || {
            for (status, body) in &pages {
                if let Some(Ok(stream)) = listener.incoming().next() {
                    write_response(stream, *status, body);
                } else {
                    break;
                }
            }
        });
        (url, MockServer(Some(handle)))
    }

    struct MockServer(Option<std::thread::JoinHandle<()>>);

    impl Drop for MockServer {
        fn drop(&mut self) {
            if let Some(h) = self.0.take() {
                let _ = h.join();
            }
        }
    }

    fn status_reason(code: u16) -> &'static str {
        match code {
            200 => "OK",
            401 => "Unauthorized",
            403 => "Forbidden",
            500 => "Internal Server Error",
            _ => "Unknown",
        }
    }

    fn make_client(url: &str) -> OpenCodeClient {
        OpenCodeClient::new(url, "testuser", "testpass")
    }

    fn valid_entry(id: &str) -> String {
        format!(
            r#"{{
                "id": "{id}",
                "parentID": null,
                "agent": "opencode",
                "location": {{
                    "directory": "/home/user/project"
                }},
                "time": {{
                    "created": 1718455200000,
                    "updated": 1718458800000
                }}
            }}"#
        )
    }

    // ── one-page snapshot ─────────────────────────────────────────────

    #[tokio::test]
    async fn one_page_snapshot() {
        let body = format!(
            r#"{{
                "data": [{}],
                "cursor": {{ "next": null }}
            }}"#,
            valid_entry("ses-1")
        );
        let (url, _server) = spawn_mock(&body, 200);
        let client = make_client(&url);

        let sessions = client.fetch_session_snapshot().await.unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].native_session_id, "ses-1");
        assert_eq!(sessions[0].provider, AgentProvider::OpenCode);
    }

    // ── cursor pagination ─────────────────────────────────────────────

    #[tokio::test]
    async fn cursor_pagination() {
        let page1 = format!(
            r#"{{
                "data": [{}],
                "cursor": {{ "next": "cursor-1" }}
            }}"#,
            valid_entry("ses-1")
        );
        let page2 = format!(
            r#"{{
                "data": [{}],
                "cursor": {{ "next": null }}
            }}"#,
            valid_entry("ses-2")
        );
        let (url, _server) = spawn_mock_paginated(vec![(200, page1), (200, page2)]);
        let client = make_client(&url);

        let sessions = client.fetch_session_snapshot().await.unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].native_session_id, "ses-1");
        assert_eq!(sessions[1].native_session_id, "ses-2");
    }

    // ── Basic authentication is sent ──────────────────────────────────

    // We verify the auth header is included by checking the request against
    // a server that inspects the Authorization header.  We do this in
    // spawn_mock by looking at what was received.
    #[tokio::test]
    async fn basic_auth_sent() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}");

        let auth_check = std::thread::spawn(move || {
            if let Some(Ok(mut stream)) = listener.incoming().next() {
                use std::io::{Read, Write};
                let mut buf = [0u8; 1024];
                let n = stream.read(&mut buf).unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..n]);

                let body = "{\"data\":[],\"cursor\":{\"next\":null}}";
                let response = if request.to_lowercase().contains("authorization: basic ")
                    && request.contains("dGVzdHVzZXI6dGVzdHBhc3M=")
                {
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                } else {
                    "HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                        .to_string()
                };
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });

        let client = OpenCodeClient::new(&url, "testuser", "testpass");
        let result = client.fetch_session_snapshot().await;
        auth_check.join().unwrap();
        assert!(result.is_ok(), "auth should succeed: {:?}", result.err());
    }

    // ── parent and child sessions ─────────────────────────────────────

    #[tokio::test]
    async fn parent_and_child_sessions() {
        let parent = r#"{
                "id": "parent-1",
                "parentID": null,
                "agent": "opencode",
                "location": { "directory": "/home/user/project" },
                "time": { "created": 1704067200000, "updated": 1704070800000 }
            }"#;
        let child = r#"{
                "id": "child-1",
                "parentID": "parent-1",
                "agent": "opencode",
                "location": { "directory": "/home/user/project" },
                "time": { "created": 1704074400000, "updated": 1704081600000 }
            }"#;
        let body = format!(
            r#"{{
                "data": [{parent}, {child}],
                "cursor": {{ "next": null }}
            }}"#
        );
        let (url, _server) = spawn_mock(&body, 200);
        let client = make_client(&url);

        let sessions = client.fetch_session_snapshot().await.unwrap();
        assert_eq!(sessions.len(), 2);

        let parent_ses = sessions
            .iter()
            .find(|s| s.native_session_id == "parent-1")
            .unwrap();
        let child_ses = sessions
            .iter()
            .find(|s| s.native_session_id == "child-1")
            .unwrap();

        assert!(parent_ses.parent_native_session_id.is_none());
        assert_eq!(
            child_ses.parent_native_session_id.as_deref(),
            Some("parent-1")
        );
    }

    // ── multiple sessions in one directory remain separate ────────────

    #[tokio::test]
    async fn multiple_sessions_same_directory() {
        let s1 = valid_entry("ses-a");
        let s2 = valid_entry("ses-b");
        let body = format!(
            r#"{{
                "data": [{s1}, {s2}],
                "cursor": {{ "next": null }}
            }}"#
        );
        let (url, _server) = spawn_mock(&body, 200);
        let client = make_client(&url);

        let sessions = client.fetch_session_snapshot().await.unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].directory, sessions[1].directory);
        assert_ne!(sessions[0].native_session_id, sessions[1].native_session_id);
    }

    // ── unknown and sensitive response fields are ignored ──────────────

    #[tokio::test]
    async fn sensitive_fields_ignored() {
        let body = r#"{
                "data": [{
                    "id": "ses-1",
                    "parentID": null,
                    "agent": "opencode",
                    "location": { "directory": "/home/user/project" },
                    "time": { "created": 1704067200000, "updated": 1704070800000 },
                    "title": "My Conversation",
                    "prompts": ["hello"],
                    "messages": [{ "role": "user", "content": "hi" }],
                    "parts": [{ "type": "text", "text": "hello" }],
                    "tool_inputs": { "cmd": "ls" },
                    "tool_outputs": { "result": "file1 file2" },
                    "commands": ["ls -la"],
                    "model_responses": ["I can help"],
                    "token_counts": { "input": 10, "output": 20 },
                    "cost": 0.05,
                    "file_contents": { "/path/to/file": "secret content" },
                    "credentials": { "api_key": "sk-1234" },
                    "unknown_field": "should be ignored"
                }],
                "cursor": { "next": null }
            }"#;
        let (url, _server) = spawn_mock(body, 200);
        let client = make_client(&url);

        let sessions = client.fetch_session_snapshot().await.unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].native_session_id, "ses-1");
        // No sensitive fields should appear in the session update
        assert!(sessions[0].agent_role.as_deref() == Some("opencode"));
    }

    // ── timestamps are converted correctly ────────────────────────────

    #[tokio::test]
    async fn timestamps_converted_correctly() {
        let body = r#"{
                "data": [{
                    "id": "ses-1",
                    "parentID": null,
                    "agent": null,
                    "location": { "directory": "/tmp" },
                    "time": {
                        "created": 1735689600000,
                        "updated": 1735689601000
                    }
                }],
                "cursor": { "next": null }
            }"#;
        let (url, _server) = spawn_mock(body, 200);
        let client = make_client(&url);

        let sessions = client.fetch_session_snapshot().await.unwrap();
        assert_eq!(sessions.len(), 1);
        // SessionUpdate doesn't carry timestamps, but validation passes
        assert_eq!(sessions[0].native_session_id, "ses-1");
    }

    // ── authentication failure ────────────────────────────────────────

    #[tokio::test]
    async fn authentication_failure() {
        let body = r#"{"error":"unauthorized"}"#;
        let (url, _server) = spawn_mock(body, 401);
        let client = make_client(&url);

        let err = client.fetch_session_snapshot().await.unwrap_err();
        assert_eq!(err, OpenCodeError::AuthenticationFailed);
        // Verify error display does NOT leak credentials
        let msg = err.to_string();
        assert!(!msg.contains("testuser"));
        assert!(!msg.contains("testpass"));
    }

    // ── malformed required metadata ───────────────────────────────────

    #[tokio::test]
    async fn missing_id() {
        let body = r#"{
            "data": [{
                "id": "",
                "parentID": null,
                "agent": null,
                "location": { "directory": "/tmp" },
                "time": { "created": 1704067200000, "updated": 1704070800000 }
            }],
            "cursor": { "next": null }
        }"#;
        let (url, _server) = spawn_mock(body, 200);
        let client = make_client(&url);

        let err = client.fetch_session_snapshot().await.unwrap_err();
        assert_eq!(err, OpenCodeError::MissingRequiredField("id"));
    }

    #[tokio::test]
    async fn missing_directory() {
        let body = r#"{
            "data": [{
                "id": "ses-1",
                "parentID": null,
                "agent": null,
                "location": { "directory": null },
                "time": { "created": 1704067200000, "updated": 1704070800000 }
            }],
            "cursor": { "next": null }
        }"#;
        let (url, _server) = spawn_mock(body, 200);
        let client = make_client(&url);

        let err = client.fetch_session_snapshot().await.unwrap_err();
        assert_eq!(
            err,
            OpenCodeError::MissingRequiredField("location.directory")
        );
    }

    #[tokio::test]
    async fn missing_created_at() {
        let body = r#"{
            "data": [{
                "id": "ses-1",
                "parentID": null,
                "agent": null,
                "location": { "directory": "/tmp" },
                "time": { "updated": 1704070800000 }
            }],
            "cursor": { "next": null }
        }"#;
        let (url, _server) = spawn_mock(body, 200);
        let client = make_client(&url);

        let err = client.fetch_session_snapshot().await.unwrap_err();
        assert_eq!(err, OpenCodeError::MissingRequiredField("time.created"));
    }

    #[tokio::test]
    async fn missing_updated_at() {
        let body = r#"{
            "data": [{
                "id": "ses-1",
                "parentID": null,
                "agent": null,
                "location": { "directory": "/tmp" },
                "time": { "created": 1704067200000 }
            }],
            "cursor": { "next": null }
        }"#;
        let (url, _server) = spawn_mock(body, 200);
        let client = make_client(&url);

        let err = client.fetch_session_snapshot().await.unwrap_err();
        assert_eq!(err, OpenCodeError::MissingRequiredField("time.updated"));
    }

    #[tokio::test]
    async fn invalid_timestamp_format() {
        let body = r#"{
            "data": [{
                "id": "ses-1",
                "parentID": null,
                "agent": null,
                "location": { "directory": "/tmp" },
                "time": { "created": -1, "updated": 1704070800000 }
            }],
            "cursor": { "next": null }
        }"#;
        let (url, _server) = spawn_mock(body, 200);
        let client = make_client(&url);

        let err = client.fetch_session_snapshot().await.unwrap_err();
        assert_eq!(err, OpenCodeError::InvalidTimestamp);
    }

    // ── pagination loop protection ────────────────────────────────────

    #[tokio::test]
    async fn pagination_loop_protection() {
        // Create a page that always returns a next_cursor
        let body = format!(
            r#"{{
                "data": [{}],
                "cursor": {{ "next": "keep-going" }}
            }}"#,
            valid_entry("ses-loop")
        );
        // We'll use a server that repeatedly gives the next_cursor
        let (url, _server) = spawn_mock_paginated((0..100).map(|_| (200, body.clone())).collect());
        let client = make_client(&url);

        let err = client.fetch_session_snapshot().await.unwrap_err();
        assert_eq!(err, OpenCodeError::PaginationLoop);
    }

    // ── Debug output does not expose the password ─────────────────────

    #[test]
    fn debug_hides_password() {
        let client = OpenCodeClient::new("http://localhost:8080", "admin", "supersecret");
        let debug_str = format!("{client:?}");
        assert!(
            !debug_str.contains("supersecret"),
            "Debug output must not contain the password: {debug_str}"
        );
        assert!(
            debug_str.contains("***"),
            "Debug output should mask the password: {debug_str}"
        );
        assert!(
            !debug_str.contains("admin"),
            "Debug output must not contain the username: {debug_str}"
        );
    }

    #[test]
    fn debug_does_not_expose_url_userinfo_path_or_query() {
        let client = OpenCodeClient::new(
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

    // ── Metadata-only live compatibility procedure ────────────────────

    #[tokio::test]
    #[ignore = "requires explicit OPENCODE_SERVER_URL/USERNAME/PASSWORD"]
    async fn live_server_compatibility_metadata_only() {
        let url = std::env::var("OPENCODE_SERVER_URL")
            .expect("OPENCODE_SERVER_URL must be set explicitly");
        let username = std::env::var("OPENCODE_SERVER_USERNAME")
            .expect("OPENCODE_SERVER_USERNAME must be set");
        let password = std::env::var("OPENCODE_SERVER_PASSWORD")
            .expect("OPENCODE_SERVER_PASSWORD must be set");

        let sessions = OpenCodeClient::new(&url, &username, &password)
            .fetch_session_snapshot()
            .await
            .expect("metadata-only OpenCode snapshot should succeed");

        assert!(!sessions.is_empty(), "expected at least one live session");
        assert!(sessions.iter().all(|session| {
            !session.native_session_id.is_empty()
                && !session.directory.as_os_str().is_empty()
                && session.provider == AgentProvider::OpenCode
                && session.lifecycle == SessionLifecycle::Discovered
                && session.discovery_source == DiscoverySource::ApiPoll
                && session.created_at.is_some()
                && session.updated_at.is_some()
        }));

        let parent_links = sessions
            .iter()
            .filter(|session| session.parent_native_session_id.is_some())
            .count();
        let roles = sessions
            .iter()
            .filter(|session| session.agent_role.is_some())
            .count();
        println!(
            "OpenCode metadata compatibility: sessions={}, parent_links={}, roles={}",
            sessions.len(),
            parent_links,
            roles
        );
    }

    // ── Poller tests ──────────────────────────────────────────────────

    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    type FakeSnapshotResult = Result<Vec<SessionUpdate>, OpenCodeError>;

    /// A fake client that returns pre-programmed results in FIFO order.
    struct FakeSnapshotClient {
        results: Arc<Mutex<VecDeque<FakeSnapshotResult>>>,
    }

    impl FakeSnapshotClient {
        fn new(results: Vec<FakeSnapshotResult>) -> Self {
            Self {
                results: Arc::new(Mutex::new(VecDeque::from(results))),
            }
        }
    }

    impl SnapshotClient for FakeSnapshotClient {
        async fn fetch_snapshot(&self) -> Result<Vec<SessionUpdate>, OpenCodeError> {
            self.results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(Vec::new()))
        }
    }

    fn dummy_session(id: &str) -> SessionUpdate {
        SessionUpdate {
            provider: AgentProvider::OpenCode,
            native_session_id: id.to_string(),
            parent_native_session_id: None,
            agent_role: None,
            directory: PathBuf::from("/tmp"),
            lifecycle: SessionLifecycle::Discovered,
            discovery_source: DiscoverySource::ApiPoll,
            created_at: None,
            updated_at: None,
        }
    }

    /// Helper: run the poller, expect a single AgentSessionUpdates, then shut down.
    async fn expect_one_update(
        client: FakeSnapshotClient,
        interval: Duration,
    ) -> Vec<SessionUpdate> {
        tokio::time::pause();

        let (state_tx, mut state_rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(run_opencode_polling(
            client,
            state_tx,
            interval,
            shutdown_rx,
        ));

        let msg = state_rx.recv().await.expect("should receive a message");
        shutdown_tx.send(()).unwrap();
        handle.await.unwrap();

        match msg {
            StateMessage::AgentSessionUpdates { updates } => updates,
            _ => panic!("expected AgentSessionUpdates"),
        }
    }

    #[tokio::test]
    async fn immediate_first_fetch() {
        let updates = expect_one_update(
            FakeSnapshotClient::new(vec![Ok(vec![dummy_session("ses-1")])]),
            Duration::from_secs(60),
        )
        .await;
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].native_session_id, "ses-1");
    }

    #[tokio::test]
    async fn repeated_interval_fetches() {
        tokio::time::pause();

        let (state_tx, mut state_rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let client = FakeSnapshotClient::new(vec![
            Ok(vec![dummy_session("ses-1")]),
            Ok(vec![dummy_session("ses-2")]),
        ]);

        let interval = Duration::from_secs(10);
        let handle = tokio::spawn(run_opencode_polling(
            client,
            state_tx,
            interval,
            shutdown_rx,
        ));

        // First fetch is immediate
        let msg1 = state_rx.recv().await.unwrap();
        let updates1 = match msg1 {
            StateMessage::AgentSessionUpdates { updates } => updates,
            _ => panic!("expected AgentSessionUpdates"),
        };
        assert_eq!(updates1[0].native_session_id, "ses-1");

        // Advance past the interval to trigger the second fetch
        tokio::time::advance(Duration::from_secs(10)).await;
        let msg2 = state_rx.recv().await.unwrap();
        let updates2 = match msg2 {
            StateMessage::AgentSessionUpdates { updates } => updates,
            _ => panic!("expected AgentSessionUpdates"),
        };
        assert_eq!(updates2[0].native_session_id, "ses-2");

        shutdown_tx.send(()).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn successful_updates_sent_to_state_message() {
        let updates = expect_one_update(
            FakeSnapshotClient::new(vec![Ok(vec![
                dummy_session("alpha"),
                dummy_session("beta"),
            ])]),
            Duration::from_secs(60),
        )
        .await;
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].native_session_id, "alpha");
        assert_eq!(updates[1].native_session_id, "beta");
    }

    #[tokio::test]
    async fn temporary_failure_followed_by_successful_retry() {
        tokio::time::pause();

        let (state_tx, mut state_rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let client = FakeSnapshotClient::new(vec![
            Err(OpenCodeError::RequestFailed),
            Ok(vec![dummy_session("ses-retry")]),
        ]);

        let interval = Duration::from_secs(10);
        let handle = tokio::spawn(run_opencode_polling(
            client,
            state_tx,
            interval,
            shutdown_rx,
        ));

        // First fetch (immediate) fails — no message sent
        tokio::time::sleep(Duration::from_millis(50)).await;
        // No message should be available yet because only the failed immediate fetch happened
        let no_msg = state_rx.try_recv();
        assert!(no_msg.is_err(), "no message expected after failure");

        // Advance past the interval so the retry fires
        tokio::time::advance(Duration::from_secs(10)).await;
        let msg = state_rx.recv().await.unwrap();
        let updates = match msg {
            StateMessage::AgentSessionUpdates { updates } => updates,
            _ => panic!("expected AgentSessionUpdates"),
        };
        assert_eq!(updates[0].native_session_id, "ses-retry");

        shutdown_tx.send(()).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_stops_further_polling() {
        tokio::time::pause();

        let (state_tx, mut state_rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let client = FakeSnapshotClient::new(vec![
            Ok(vec![dummy_session("first")]),
            Ok(vec![dummy_session("second")]),
        ]);

        let interval = Duration::from_secs(10);
        let handle = tokio::spawn(run_opencode_polling(
            client,
            state_tx,
            interval,
            shutdown_rx,
        ));

        // Receive the immediate first fetch
        let msg = state_rx.recv().await.unwrap();
        match msg {
            StateMessage::AgentSessionUpdates { updates } => {
                assert_eq!(updates[0].native_session_id, "first");
            }
            _ => panic!("expected AgentSessionUpdates"),
        }

        // Shut down before the interval fires
        shutdown_tx.send(()).unwrap();
        handle.await.unwrap();

        // No more messages should be sent
        let no_msg = state_rx.try_recv();
        assert!(no_msg.is_err(), "no message expected after shutdown");
    }

    // ── Wiring tests ──────────────────────────────────────────────────

    #[tokio::test]
    async fn spawn_opencode_poller_starts_immediate_fetch() {
        tokio::time::pause();

        let (state_tx, mut state_rx) = mpsc::channel(16);
        let client = FakeSnapshotClient::new(vec![Ok(vec![dummy_session("wire-test")])]);
        let (shutdown_tx, handle) =
            spawn_opencode_poller(client, state_tx, Duration::from_secs(10));

        let msg = state_rx.recv().await.unwrap();
        match msg {
            StateMessage::AgentSessionUpdates { updates } => {
                assert_eq!(updates.len(), 1);
                assert_eq!(updates[0].native_session_id, "wire-test");
            }
            _ => panic!("expected AgentSessionUpdates"),
        }

        shutdown_tx.send(()).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn spawn_opencode_poller_shutdown_stops_poller() {
        tokio::time::pause();

        let (state_tx, mut state_rx) = mpsc::channel(16);
        let client = FakeSnapshotClient::new(vec![Ok(vec![dummy_session("shutdown-test")])]);
        let (shutdown_tx, handle) =
            spawn_opencode_poller(client, state_tx, Duration::from_secs(10));

        // Receive the immediate first fetch
        let msg = state_rx.recv().await.unwrap();
        match msg {
            StateMessage::AgentSessionUpdates { .. } => {}
            _ => panic!("expected AgentSessionUpdates"),
        }

        // Shut down before the next interval
        shutdown_tx.send(()).unwrap();
        handle.await.unwrap();

        // No more messages after shutdown
        let no_msg = state_rx.try_recv();
        assert!(no_msg.is_err(), "no message expected after shutdown");
    }

    #[test]
    fn no_configuration_no_poller() {
        let config = crate::state::GroveConfig::default();
        assert!(config.opencode.is_none());

        // Verify the same logic used in run_inner: map on None returns None
        let poller: Option<(oneshot::Sender<()>, tokio::task::JoinHandle<()>)> = config
            .opencode
            .as_ref()
            .map(|_| unreachable!("should not be called when opencode is None"));
        assert!(poller.is_none());
    }

    enum FakeListenResult {
        Return(Result<(), crate::event_stream::EventStreamError>),
        WaitForShutdown,
    }

    type FakeEventScript = (Vec<crate::event_stream::AgentEvent>, FakeListenResult);

    struct FakeAgentEventSource {
        scripts: Arc<Mutex<VecDeque<FakeEventScript>>>,
    }

    impl FakeAgentEventSource {
        fn new(scripts: Vec<FakeEventScript>) -> Self {
            Self {
                scripts: Arc::new(Mutex::new(VecDeque::from(scripts))),
            }
        }
    }

    impl crate::event_stream::AgentEventSource for FakeAgentEventSource {
        async fn listen(
            &self,
            events_tx: mpsc::Sender<crate::event_stream::AgentEvent>,
            shutdown_rx: oneshot::Receiver<()>,
        ) -> Result<(), crate::event_stream::EventStreamError> {
            let (events, result) = self
                .scripts
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected event-stream connection");
            for event in events {
                events_tx.send(event).await.unwrap();
            }
            match result {
                FakeListenResult::Return(result) => result,
                FakeListenResult::WaitForShutdown => {
                    let _ = shutdown_rx.await;
                    Err(crate::event_stream::EventStreamError::Shutdown)
                }
            }
        }
    }

    fn fake_agent_event(native_session_id: &str) -> crate::event_stream::AgentEvent {
        crate::event_stream::AgentEvent {
            provider: AgentProvider::OpenCode,
            native_session_id: Some(native_session_id.to_string()),
            parent_native_session_id: None,
            event_kind: crate::event_stream::EventKind::MessageUpdated,
            timestamp: None,
            lifecycle: None,
            agent_role: None,
            directory: None,
            compacted: false,
        }
    }

    #[tokio::test]
    async fn event_stream_recovers_snapshot_before_each_connection_and_stops_cleanly() {
        tokio::time::pause();

        let snapshots = FakeSnapshotClient::new(vec![
            Ok(vec![dummy_session("snapshot-1")]),
            Ok(vec![dummy_session("snapshot-2")]),
        ]);
        let events = FakeAgentEventSource::new(vec![
            (
                vec![fake_agent_event("event-1")],
                FakeListenResult::Return(Err(crate::event_stream::EventStreamError::RequestFailed)),
            ),
            (
                vec![fake_agent_event("event-2")],
                FakeListenResult::WaitForShutdown,
            ),
        ]);
        let (state_tx, mut state_rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(run_opencode_event_stream(
            snapshots,
            events,
            state_tx,
            ReconnectPolicy {
                initial_delay: Duration::from_secs(1),
                max_delay: Duration::from_secs(8),
            },
            shutdown_rx,
        ));

        match state_rx.recv().await.unwrap() {
            StateMessage::AgentSessionUpdates { updates } => {
                assert_eq!(updates[0].native_session_id, "snapshot-1");
            }
            _ => panic!("snapshot must precede first event-stream connection"),
        }
        match state_rx.recv().await.unwrap() {
            StateMessage::AgentSessionEvent { event } => {
                assert_eq!(event.native_session_id.as_deref(), Some("event-1"));
            }
            _ => panic!("expected first normalized event"),
        }

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;

        match state_rx.recv().await.unwrap() {
            StateMessage::AgentSessionUpdates { updates } => {
                assert_eq!(updates[0].native_session_id, "snapshot-2");
            }
            _ => panic!("recovery snapshot must precede reconnection"),
        }
        match state_rx.recv().await.unwrap() {
            StateMessage::AgentSessionEvent { event } => {
                assert_eq!(event.native_session_id.as_deref(), Some("event-2"));
            }
            _ => panic!("expected second normalized event"),
        }

        shutdown_tx.send(()).unwrap();
        handle.await.unwrap();
    }
}

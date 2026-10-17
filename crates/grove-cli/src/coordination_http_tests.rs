use super::*;
use axum::{Router, body::Body, extract::State, http::Response as HttpResponse, routing::post};
use serde_json::json;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
struct Reply {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: String,
    delay: Duration,
}

impl Reply {
    fn status(status: u16) -> Self {
        Self {
            status,
            headers: vec![],
            body: json!({"ok":true,"data":{"recovered":true}}).to_string(),
            delay: Duration::ZERO,
        }
    }
}

#[derive(Default)]
struct StateData {
    replies: VecDeque<Reply>,
    requests: Vec<(HeaderMap, Vec<u8>, Instant)>,
    database: Option<rusqlite::Connection>,
    repository: String,
}

struct Fixture {
    data: Arc<Mutex<StateData>>,
    client: Client,
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handler(
    State(state): State<Arc<Mutex<StateData>>>,
    headers: HeaderMap,
    bytes: axum::body::Bytes,
) -> HttpResponse<Body> {
    let (reply, engine) = {
        let mut state = state.lock().unwrap();
        state
            .requests
            .push((headers, bytes.to_vec(), Instant::now()));
        let repository = state.repository.clone();
        let number = state.requests.len() as i64;
        let engine = state.database.as_mut().map(|db| {
            let envelope: Value = serde_json::from_slice(&bytes).unwrap();
            let response = grove_daemon::coordination::execute(
                db,
                Some(&repository),
                serde_json::from_value(envelope["command"].clone()).unwrap(),
                10_000 + number,
                false,
            )
            .unwrap();
            json!({"ok":true,"data":response}).to_string()
        });
        (
            state
                .replies
                .pop_front()
                .unwrap_or_else(|| Reply::status(200)),
            engine,
        )
    };
    tokio::time::sleep(reply.delay).await;
    let mut response = HttpResponse::builder().status(reply.status);
    for (key, value) in reply.headers {
        response = response.header(key, value);
    }
    response
        .body(Body::from(if reply.status == 200 {
            engine.unwrap_or(reply.body)
        } else {
            reply.body
        }))
        .unwrap()
}

impl Fixture {
    async fn new(replies: Vec<Reply>) -> Self {
        let data = Arc::new(Mutex::new(StateData {
            replies: replies.into(),
            ..Default::default()
        }));
        let app = Router::new()
            .route("/coord", post(handler))
            .with_state(data.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/coord", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            data,
            client: Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
            url,
            task,
        }
    }
    fn request(&self, command: Value) -> Request {
        self.client
            .post(&self.url)
            .header("authorization", "Bearer participant-fixture-credential")
            .header("x-vercel-protection-bypass", "bypass-fixture-credential")
            .header("x-grove-approver-token", "operator-fixture-credential")
            .json(&json!({"repository_id":"fixture","command":command}))
            .build()
            .unwrap()
    }
    async fn call(&self, safe: bool, policy: Policy) -> Result<Value> {
        call_with_policy(
            &self.client,
            self.request(json!({"op":"hello"})),
            safe,
            policy,
        )
        .await
    }
    fn count(&self) -> usize {
        self.data.lock().unwrap().requests.len()
    }
}

fn fast() -> Policy {
    Policy {
        attempts: 4,
        budget: Duration::from_secs(2),
        initial_backoff: Duration::from_millis(1),
        jitter: Duration::ZERO,
    }
}

#[tokio::test]
async fn retry_after_a_committed_send_preserves_key_payload_and_one_effect() {
    let mut first = Reply::status(503);
    first.headers = vec![
        ("x-vercel-id", "cle1::iad1::fixture-100-abcd".into()),
        ("retry-after", "0".into()),
    ];
    let fixture = Fixture::new(vec![first, Reply::status(200)]).await;
    let repository = uuid::Uuid::new_v4().to_string();
    let mut db = rusqlite::Connection::open_in_memory().unwrap();
    grove_daemon::coordination::migrate(&db).unwrap();
    db.execute(
        "UPDATE coord_meta SET value=?1 WHERE key='repository_id'",
        [&repository],
    )
    .unwrap();
    for session in ["codex:sender-fixture", "codex:recipient-fixture"] {
        let command=serde_json::from_value(json!({"op":"register","session_id":session,"task":"fixture","scope":["."],"acceptance":["once"],"observation":{"worktree_id":session,"worktree":"/fixture","branch":"fixture","commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","dirty":false,"verification":"client_reported"}})).unwrap();
        grove_daemon::coordination::execute(&mut db, Some(&repository), command, 1, false).unwrap();
    }
    {
        let mut state = fixture.data.lock().unwrap();
        state.database = Some(db);
        state.repository = repository;
    }
    let command = json!({"op":"send","session_id":"codex:sender-fixture","to":"codex:recipient-fixture","kind":"handoff","body":"preserve queued work","key":"existing-dedup-key"});
    let request = fixture.request(command);
    let result = call_with_policy(&fixture.client, request, true, fast())
        .await
        .unwrap();
    assert_eq!(result["deduplicated"], true);
    let state = fixture.data.lock().unwrap();
    assert_eq!(state.requests.len(), 2);
    assert_eq!(state.requests[0].1, state.requests[1].1);
    for key in [
        "authorization",
        "x-vercel-protection-bypass",
        "x-grove-approver-token",
    ] {
        assert_eq!(state.requests[0].0[key], state.requests[1].0[key]);
    }
    let db = state.database.as_ref().unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM coord_messages", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM coord_events WHERE kind='message'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn authentication_validation_and_application_errors_are_not_retried() {
    for status in [400, 401, 403, 404, 409, 413, 501] {
        let fixture = Fixture::new(vec![Reply::status(status)]).await;
        let error = fixture.call(true, fast()).await.unwrap_err().to_string();
        assert!(error.contains(&format!("HTTP {status}")));
        assert_eq!(fixture.count(), 1);
    }
    let mut reply = Reply::status(200);
    reply.body = json!({"ok":false,"error":"decision revision changed"}).to_string();
    let fixture = Fixture::new(vec![reply]).await;
    assert_eq!(
        fixture.call(true, fast()).await.unwrap_err().to_string(),
        "decision revision changed"
    );
    assert_eq!(fixture.count(), 1);
}

#[tokio::test]
async fn transient_statuses_recover_but_unsafe_commands_are_never_replayed() {
    for status in [408, 429, 500, 502, 503, 504] {
        let fixture = Fixture::new(vec![Reply::status(status), Reply::status(200)]).await;
        assert_eq!(fixture.call(true, fast()).await.unwrap()["recovered"], true);
        assert_eq!(fixture.count(), 2);
        let fixture = Fixture::new(vec![Reply::status(status)]).await;
        assert!(
            fixture
                .call(false, fast())
                .await
                .unwrap_err()
                .to_string()
                .contains("retry_safe=false")
        );
        assert_eq!(fixture.count(), 1);
    }
}

#[tokio::test]
async fn retries_stop_at_attempt_limit_and_report_request_metadata() {
    let mut reply = Reply::status(503);
    reply.headers = vec![
        ("x-vercel-id", "cle1::iad1::fixture-100-abcd".into()),
        ("x-vercel-error", "FUNCTION_INVOCATION_FAILED".into()),
        ("server", "envoy".into()),
    ];
    let fixture = Fixture::new(vec![reply; 4]).await;
    let error = fixture.call(true, fast()).await.unwrap_err().to_string();
    for expected in [
        "HTTP 503",
        "\"attempt\":4",
        "cle1::iad1::fixture-100-abcd",
        "FUNCTION_INVOCATION_FAILED",
        "envoy",
        "retry_budget_exhausted=true",
    ] {
        assert!(error.contains(expected), "{error}");
    }
    assert_eq!(fixture.count(), 4);
}

#[tokio::test]
async fn retry_after_is_honored_and_a_long_cooldown_returns_without_an_early_retry() {
    let mut reply = Reply::status(503);
    reply.headers.push(("retry-after", "1".into()));
    let fixture = Fixture::new(vec![reply.clone()]).await;
    fixture.call(true, fast()).await.unwrap();
    {
        let state = fixture.data.lock().unwrap();
        assert!(state.requests[1].2.duration_since(state.requests[0].2) >= Duration::from_secs(1));
    }
    reply.headers[0].1 = "120".into();
    let fixture = Fixture::new(vec![reply]).await;
    let error = fixture.call(true, fast()).await.unwrap_err().to_string();
    assert!(error.contains("\"retry_after_seconds\":120"));
    assert_eq!(fixture.count(), 1);
}

#[tokio::test]
async fn total_deadline_cancels_a_stalled_request() {
    let mut reply = Reply::status(200);
    reply.delay = Duration::from_secs(2);
    let fixture = Fixture::new(vec![reply]).await;
    let policy = Policy {
        budget: Duration::from_millis(50),
        ..fast()
    };
    assert!(
        fixture
            .call(true, policy)
            .await
            .unwrap_err()
            .to_string()
            .contains("deadline")
    );
    assert_eq!(fixture.count(), 1);
}

#[tokio::test]
async fn connection_failures_retry_without_exposing_the_endpoint() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let client = Client::builder().no_proxy().build().unwrap();
    for safe in [true, false] {
        let request = client
            .post(format!("http://{address}/private-endpoint"))
            .body("private request body")
            .build()
            .unwrap();
        let error = call_with_policy(&client, request, safe, fast())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(if safe {
            "\"attempt\":4"
        } else {
            "\"attempt\":1"
        }));
        assert!(!error.contains("private-endpoint"));
        assert!(!error.contains(&address.to_string()));
    }
}

#[tokio::test]
async fn a_timeout_after_commit_recovers_without_a_duplicate_send() {
    let mut first = Reply::status(200);
    first.delay = Duration::from_secs(1);
    let mut fixture = Fixture::new(vec![first]).await;
    fixture.client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_millis(150))
        .build()
        .unwrap();
    let repository = uuid::Uuid::new_v4().to_string();
    let mut db = rusqlite::Connection::open_in_memory().unwrap();
    grove_daemon::coordination::migrate(&db).unwrap();
    db.execute(
        "UPDATE coord_meta SET value=?1 WHERE key='repository_id'",
        [&repository],
    )
    .unwrap();
    for session in ["codex:sender-fixture", "codex:recipient-fixture"] {
        let command=serde_json::from_value(json!({"op":"register","session_id":session,"task":"fixture","scope":["."],"acceptance":["once"],"observation":{"worktree_id":session,"worktree":"/fixture","branch":"fixture","commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","dirty":false,"verification":"client_reported"}})).unwrap();
        grove_daemon::coordination::execute(&mut db, Some(&repository), command, 1, false).unwrap();
    }
    {
        let mut state = fixture.data.lock().unwrap();
        state.database = Some(db);
        state.repository = repository;
    }
    let request=fixture.request(json!({"op":"send","session_id":"codex:sender-fixture","to":"codex:recipient-fixture","kind":"handoff","body":"one effect after timeout","key":"timeout-dedup-key"}));
    let result = call_with_policy(&fixture.client, request, true, fast())
        .await
        .unwrap();
    assert_eq!(result["deduplicated"], true);
    let state = fixture.data.lock().unwrap();
    assert_eq!(state.requests.len(), 2);
    assert_eq!(state.requests[0].1, state.requests[1].1);
    assert_eq!(
        state
            .database
            .as_ref()
            .unwrap()
            .query_row("SELECT count(*) FROM coord_messages", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn error_diagnostics_exclude_credentials_bodies_and_arbitrary_headers() {
    let mut reply = Reply::status(503);
    reply.headers = vec![
        ("x-vercel-id", "cle1::participant-fixture-credential".into()),
        ("x-vercel-error", "bypass-fixture-credential".into()),
        ("set-cookie", "operator-fixture-credential".into()),
        ("location", "https://secret-host/secret-path".into()),
        ("proxy-status", "secret-proxy-details".into()),
    ];
    reply.body = "secret body and participant-fixture-credential".into();
    let fixture = Fixture::new(vec![reply; 4]).await;
    let request = fixture.request(json!({"op":"hello"}));
    let values = secrets(&request);
    let error = call_with_policy(&fixture.client, request, true, fast())
        .await
        .unwrap_err()
        .to_string();
    for private in values.iter().map(String::as_str).chain([
        "secret body",
        "secret-host",
        "secret-path",
        "secret-proxy-details",
    ]) {
        assert!(!error.contains(private));
    }
}

#[test]
fn retry_after_supports_http_dates_and_ignores_invalid_or_secret_values() {
    let now = DateTime::parse_from_rfc3339("2026-10-17T18:00:00.500Z")
        .unwrap()
        .with_timezone(&Utc);
    let mut headers = HeaderMap::new();
    for (value, expected) in [
        ("12", Some(12)),
        ("Sat, 17 Oct 2026 18:00:03 GMT", Some(3)),
        ("Sat, 17 Oct 2026 17:59:00 GMT", Some(0)),
        ("nonsense", None),
        ("-1", None),
    ] {
        headers.insert("retry-after", value.parse().unwrap());
        assert_eq!(retry_after(&headers, now, &[]), expected);
    }
    headers.insert("retry-after", "12".parse().unwrap());
    assert_eq!(retry_after(&headers, now, &["12".into()]), None);
    assert!(safe_header(&headers, "missing", &[]).is_none());
    headers.insert(
        "x-vercel-id",
        "untrusted value with spaces".parse().unwrap(),
    );
    assert!(safe_header(&headers, "x-vercel-id", &[]).is_none());
}

#[test]
fn only_commands_with_safe_replay_semantics_are_eligible() {
    assert!(retry_safe(&CoordCommand::Hello));
    for command in [
        json!({"op":"sync","session_id":"codex:fixture"}),
        json!({"op":"heartbeat","session_id":"codex:fixture"}),
        json!({"op":"checkpoint","session_id":"codex:fixture","cursor":1}),
        json!({"op":"send","session_id":"codex:a","to":"codex:b","kind":"handoff","body":"body","key":"fixed-key"}),
        json!({"op":"ack_message","session_id":"codex:b","message_id":"message"}),
        json!({"op":"ack_decision","session_id":"codex:a","decision_id":"policy","revision":1}),
        json!({"op":"get_transfer","session_id":"codex:a","transfer_id":"transfer"}),
    ] {
        assert!(retry_safe(&serde_json::from_value(command).unwrap()));
    }
    for command in [
        json!({"op":"register","session_id":"codex:a","task":"task","scope":["."],"acceptance":["done"]}),
        json!({"op":"propose","session_id":"codex:a","decision_id":"policy","expected_revision":0,"scope":["."],"body":"policy","provenance":"user"}),
        json!({"op":"approve","decision_id":"policy","revision":1,"authority":"user"}),
        json!({"op":"claim","session_id":"codex:a","path":"src","ttl_seconds":30}),
        json!({"op":"renew_claim","session_id":"codex:a","claim_id":"claim","epoch":1,"ttl_seconds":30}),
        json!({"op":"release_claim","session_id":"codex:a","claim_id":"claim","epoch":1}),
    ] {
        assert!(!retry_safe(&serde_json::from_value(command).unwrap()));
    }
}

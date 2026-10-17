//! Shared CLI/MCP transport. Retry only repeatable operations, preserving the
//! original encoded request. Never log bodies, endpoints or arbitrary headers.
use super::{Result, unwrap_response};
use chrono::{DateTime, SecondsFormat, Utc};
use grove_lib::coordination::CoordCommand;
use reqwest::{Client, Request, Response, StatusCode, header::HeaderMap};
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;
use tokio::time::Instant;

pub(super) fn retry_safe(command: &CoordCommand) -> bool {
    if let CoordCommand::Send { key, .. } = command {
        return !key.trim().is_empty();
    }
    matches!(
        command,
        CoordCommand::Hello
            | CoordCommand::Sync { .. }
            | CoordCommand::Heartbeat { .. }
            | CoordCommand::Checkpoint { .. }
            | CoordCommand::AckMessage { .. }
            | CoordCommand::AckDecision { .. }
            | CoordCommand::GetTransfer { .. }
    )
}

#[derive(Clone, Copy)]
struct Policy {
    attempts: usize,
    budget: Duration,
    initial_backoff: Duration,
    jitter: Duration,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            attempts: 4,
            budget: Duration::from_secs(45),
            initial_backoff: Duration::from_secs(1),
            jitter: Duration::from_millis(250),
        }
    }
}

#[derive(Serialize)]
struct Failure {
    utc: String,
    attempt: usize,
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    vercel_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    vercel_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    server_hint: Option<&'static str>,
}

#[derive(Serialize)]
struct Retry {
    failure: Failure,
    next_attempt: usize,
    delay_ms: u64,
}

impl Failure {
    fn new(attempt: usize, kind: &'static str) -> Self {
        Self {
            utc: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            attempt,
            kind,
            http_status: None,
            vercel_request_id: None,
            vercel_error: None,
            retry_after_seconds: None,
            server_hint: None,
        }
    }

    fn response(response: &Response, attempt: usize, secrets: &[String]) -> Self {
        let mut result = Self::new(attempt, "http");
        result.http_status = Some(response.status().as_u16());
        result.vercel_request_id = safe_header(response.headers(), "x-vercel-id", secrets);
        result.vercel_error = safe_header(response.headers(), "x-vercel-error", secrets);
        result.retry_after_seconds = retry_after(response.headers(), Utc::now(), secrets);
        result.server_hint = match response
            .headers()
            .get("server")
            .and_then(|v| v.to_str().ok())
            .filter(|value| !secrets.iter().any(|secret| value.contains(secret)))
        {
            Some(value) if value.eq_ignore_ascii_case("envoy") => Some("envoy"),
            Some(value) if value.eq_ignore_ascii_case("vercel") => Some("vercel"),
            Some(value) if value.eq_ignore_ascii_case("cloudflare") => Some("cloudflare"),
            Some(value) if value.eq_ignore_ascii_case("nginx") => Some("nginx"),
            _ => None,
        };
        result
    }

    fn message(&self, safe: bool, exhausted: bool, retries: &[Retry]) -> String {
        let prefix = match self.http_status {
            Some(status) if self.kind == "http" => {
                format!("coordination service rejected request (HTTP {status})")
            }
            Some(status) => format!("coordination response unavailable (HTTP {status})"),
            None => "coordination service unavailable".into(),
        };
        let mut diagnostics = serde_json::to_value(self).unwrap_or(Value::Null);
        if !retries.is_empty() {
            diagnostics["prior_retries"] = serde_json::to_value(retries).unwrap_or(Value::Null);
        }
        format!(
            "{prefix}; diagnostics={}; retry_safe={safe}; retry_budget_exhausted={exhausted}",
            diagnostics
        )
    }
}

fn secrets(request: &Request) -> Vec<String> {
    [
        "authorization",
        "x-vercel-protection-bypass",
        "x-grove-approver-token",
    ]
    .into_iter()
    .filter_map(|key| request.headers().get(key)?.to_str().ok())
    .map(|value| value.strip_prefix("Bearer ").unwrap_or(value).to_string())
    .filter(|value| !value.is_empty())
    .collect()
}

fn safe_header(headers: &HeaderMap, name: &str, secrets: &[String]) -> Option<String> {
    let value = headers.get(name)?.to_str().ok()?;
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_:.".contains(&b))
        || secrets.iter().any(|secret| value.contains(secret))
    {
        return None;
    }
    Some(value.to_string())
}

fn retry_after(headers: &HeaderMap, now: DateTime<Utc>, secrets: &[String]) -> Option<u64> {
    let value = headers.get("retry-after")?.to_str().ok()?;
    if value.len() > 128 || secrets.iter().any(|secret| value.contains(secret)) {
        return None;
    }
    if let Ok(seconds) = value.parse() {
        return Some(seconds);
    }
    let date = DateTime::parse_from_rfc2822(value).ok()?;
    let millis = date.signed_duration_since(now).num_milliseconds().max(0) as u64;
    Some(millis.saturating_add(999) / 1000)
}

fn transient(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

fn jitter(max: Duration) -> Duration {
    let cap = max.as_millis().min(u64::MAX as u128 - 1) as u64;
    let random = uuid::Uuid::new_v4().as_u128() as u64;
    Duration::from_millis(random % (cap + 1))
}

enum Outcome {
    Complete(std::result::Result<Value, String>),
    Failed(Failure, bool),
}

async fn attempt(client: &Client, request: Request, number: usize, secrets: &[String]) -> Outcome {
    let mut response = match client.execute(request).await {
        Ok(response) => response,
        Err(error) => {
            // reqwest's Display may include URLs or proxy credentials.
            let kind = if error.is_timeout() {
                "timeout"
            } else if error.is_connect() {
                "connect"
            } else {
                "transport"
            };
            return Outcome::Failed(Failure::new(number, kind), !error.is_builder());
        }
    };
    if !response.status().is_success() {
        return Outcome::Failed(
            Failure::response(&response, number, secrets),
            transient(response.status()),
        );
    }
    let mut bytes = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if bytes.len() + chunk.len() > 4_194_304 {
                    return Outcome::Complete(Err(
                        "coordination response exceeds size limit".into()
                    ));
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(_) => {
                let mut failure = Failure::response(&response, number, secrets);
                failure.kind = "response_body";
                return Outcome::Failed(failure, true);
            }
        }
    }
    Outcome::Complete(match serde_json::from_slice(&bytes) {
        Ok(response) => unwrap_response(response).map_err(|error| error.to_string()),
        Err(_) => Err("invalid coordination response".into()),
    })
}

pub(super) async fn call(client: &Client, request: Request, safe: bool) -> Result<Value> {
    call_with_policy(client, request, safe, Policy::default()).await
}

async fn call_with_policy(
    client: &Client,
    request: Request,
    safe: bool,
    policy: Policy,
) -> Result<Value> {
    let secrets = secrets(&request);
    let deadline = Instant::now() + policy.budget;
    let mut retries = Vec::new();
    for number in 1..=policy.attempts {
        let copy = request
            .try_clone()
            .ok_or("coordination request is not replayable")?;
        let outcome =
            tokio::time::timeout_at(deadline, attempt(client, copy, number, &secrets)).await;
        let (failure, retryable) = match outcome {
            Ok(Outcome::Complete(result)) => {
                match result {
                    Ok(value) => {
                        if !retries.is_empty() {
                            // One recovery record on stderr. Failed CLI calls
                            // still produce exactly one final JSON error.
                            eprintln!(
                                "{}",
                                serde_json::json!({"grove_coord_recovery":{"attempts":number,"retries":retries}})
                            );
                        }
                        return Ok(value);
                    }
                    Err(error) => {
                        return Err(if retries.is_empty() {
                            error
                        } else {
                            format!(
                                "{error}; prior_transient_failures={}",
                                serde_json::to_string(&retries).unwrap_or_default()
                            )
                        }
                        .into());
                    }
                }
            }
            Ok(Outcome::Failed(failure, retryable)) => (failure, retryable),
            Err(_) => {
                return Err(Failure::new(number, "deadline")
                    .message(safe, true, &retries)
                    .into());
            }
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        let base = policy
            .initial_backoff
            .saturating_mul(1 << (number - 1).min(10));
        let requested = Duration::from_secs(failure.retry_after_seconds.unwrap_or(0));
        let delay = base.max(requested).saturating_add(jitter(policy.jitter));
        let exhausted = number == policy.attempts || delay >= remaining;
        if !safe || !retryable || exhausted {
            return Err(failure
                .message(safe, safe && retryable && exhausted, &retries)
                .into());
        }
        retries.push(Retry {
            failure,
            next_attempt: number + 1,
            delay_ms: delay.as_millis() as u64,
        });
        tokio::time::sleep(delay).await;
    }
    unreachable!("retry policy always makes at least one attempt")
}

#[cfg(test)]
#[path = "coordination_http_tests.rs"]
mod tests;

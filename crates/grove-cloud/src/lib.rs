//! Stateless Vercel backend. An indexed remote store owns durable state. A bounded
//! working set is evaluated by the local engine, then changed rows and events
//! commit together behind an atomic revision fence. Cache memory owns no state.
mod bluehost;
mod cache;
mod storage;
use grove_daemon::{
    coordination,
    coordination_snapshot::{self, Snapshot},
};
use grove_lib::coordination::CoordCommand;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
pub use storage::Archive;

pub const MAX_REQUEST_BYTES: usize = 1_048_576;
pub const MAX_SNAPSHOT_BYTES: usize = 16_777_216;
pub const MAX_RESPONSE_BYTES: usize = 4_194_304;
const MAX_CAS_ATTEMPTS: u32 = 12;

type Result<T> = std::result::Result<T, String>;

struct DbReply {
    data: Value,
    bytes: usize,
}

#[derive(Default, Serialize)]
struct RequestMetrics {
    operation: String,
    attempts: u32,
    db_queries: usize,
    db_response_bytes: usize,
    projected_rows: usize,
    projection_cache_hits: usize,
    ok: bool,
}

fn integer(value: &Value) -> Result<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|v| v.parse().ok()))
        .ok_or_else(|| "invalid coordination state integer".into())
}

fn decoded<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T> {
    match value.as_str() {
        Some(raw) => serde_json::from_str(raw),
        None => serde_json::from_value(value.clone()),
    }
    .map_err(|_| "invalid stored coordination rows".into())
}

fn neon_http_endpoint(connection: &str) -> Result<reqwest::Url> {
    let parsed =
        reqwest::Url::parse(connection).map_err(|_| "invalid database connection configuration")?;
    if !matches!(parsed.scheme(), "postgres" | "postgresql") {
        return Err("database connection must use Postgres".into());
    }
    let host = parsed
        .host_str()
        .filter(|h| h.ends_with(".neon.tech"))
        .ok_or("database must be the configured Neon endpoint")?;
    // Match the official driver's regional SQL endpoint: replace the endpoint
    // label (including -pooler) with api, retaining the regional suffix.
    let (_, region) = host.split_once('.').ok_or("invalid database endpoint")?;
    reqwest::Url::parse(&format!("https://api.{region}/sql"))
        .map_err(|_| "invalid database endpoint".into())
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub repository_id: String,
    pub command: CoordCommand,
}

pub struct Config {
    pub repository_id: String,
    token: String,
    approver: Option<String>,
    bluehost: bool,
    client: reqwest::Client,
    endpoint: reqwest::Url,
    connection: reqwest::header::HeaderValue,
    cache: Mutex<cache::Cache>,
}

impl Config {
    pub fn from_environment(migration: bool) -> Result<Self> {
        let repository_id =
            std::env::var("GROVE_REPOSITORY_ID").map_err(|_| "GROVE_REPOSITORY_ID is missing")?;
        uuid::Uuid::parse_str(&repository_id).map_err(|_| "invalid repository identity")?;
        let token =
            std::env::var("GROVE_COORD_TOKEN").map_err(|_| "GROVE_COORD_TOKEN is missing")?;
        if token.len() < 24 {
            return Err("GROVE_COORD_TOKEN must contain at least 24 bytes".into());
        }
        let bluehost = match std::env::var("GROVE_COORD_BACKEND").as_deref() {
            Ok("bluehost") => true,
            Ok("neon") | Err(std::env::VarError::NotPresent) => false,
            _ => return Err("GROVE_COORD_BACKEND must be bluehost or neon".into()),
        };
        let (endpoint, connection) = if bluehost {
            bluehost::configuration(
                &std::env::var("GROVE_STORE_URL").map_err(|_| "GROVE_STORE_URL is missing")?,
                &std::env::var("GROVE_STORE_TOKEN").map_err(|_| "GROVE_STORE_TOKEN is missing")?,
            )?
        } else {
            let fallback = if migration {
                "DATABASE_URL_UNPOOLED"
            } else {
                "DATABASE_URL"
            };
            let preferred = format!("GROVE_COORD_{fallback}");
            let connection = match std::env::var(&preferred) {
                Ok(value) if !value.is_empty() => value,
                Ok(_) => return Err(format!("{preferred} is empty")),
                Err(std::env::VarError::NotPresent) => {
                    std::env::var(fallback).map_err(|_| format!("{fallback} is missing"))?
                }
                Err(_) => return Err("invalid database configuration".into()),
            };
            let endpoint = neon_http_endpoint(&connection)?;
            let mut connection = reqwest::header::HeaderValue::from_str(&connection)
                .map_err(|_| "invalid database connection header")?;
            connection.set_sensitive(true);
            (endpoint, connection)
        };
        let mut builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(12))
            .redirect(reqwest::redirect::Policy::none());
        if bluehost {
            builder = builder.user_agent(bluehost::USER_AGENT);
        }
        let client = builder
            .build()
            .map_err(|_| "cannot construct database transport")?;
        Ok(Self {
            repository_id,
            token,
            approver: std::env::var("GROVE_COORD_APPROVER_TOKEN").ok(),
            bluehost,
            client,
            endpoint,
            connection,
            cache: Mutex::new(cache::Cache::default()),
        })
    }
    pub fn authenticate(
        &self,
        bearer: Option<&str>,
        approver: Option<&str>,
        command: &CoordCommand,
    ) -> Result<bool> {
        if !grove_daemon::codex::credential_matches(Some(&self.token), bearer) {
            return Err("authentication required".into());
        }
        let operator = grove_daemon::codex::credential_matches(self.approver.as_deref(), approver);
        if command.operator_only() && !operator {
            return Err("operator credential required".into());
        }
        Ok(operator)
    }
    async fn query_measured(&self, sql: &str, params: Vec<Value>) -> Result<DbReply> {
        // Static, bound Neon SQL or the private Bluehost storage protocol.
        let request = self.client.post(self.endpoint.clone());
        let request = if self.bluehost {
            request
                .header("x-grove-store-token", self.connection.clone())
                .json(&bluehost::request(sql, params)?)
        } else {
            request
                .header("Neon-Connection-String", self.connection.clone())
                .header("Neon-Raw-Text-Output", "true")
                .header("Neon-Array-Mode", "true")
                .json(&json!({"query":sql,"params":params}))
        };
        let mut response = request
            .send()
            .await
            .map_err(|_| "coordination database unavailable")?;
        if !response.status().is_success() {
            return Err(format!(
                "coordination database rejected query (HTTP {})",
                response.status().as_u16()
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| "database response unavailable")?
        {
            if bytes.len() + chunk.len() > MAX_SNAPSHOT_BYTES + 1_048_576 {
                return Err("coordination database response exceeds size bound".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        let data =
            serde_json::from_slice(&bytes).map_err(|_| "invalid coordination database response")?;
        if self.bluehost {
            return bluehost::decode(data, bytes.len());
        }
        Ok(DbReply {
            data,
            bytes: bytes.len(),
        })
    }
    async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Value> {
        self.query_measured(sql, params)
            .await
            .map(|reply| reply.data)
    }
    async fn working_set(
        &self,
        command: &CoordCommand,
        key: &str,
        metrics: &mut RequestMetrics,
    ) -> Result<(i64, i64, i64, Snapshot, i64)> {
        let cached = if matches!(command, CoordCommand::Sync { .. }) {
            self.cache.lock().ok().and_then(|mut cache| cache.get(key))
        } else {
            None
        };
        if let Some(mut cached) = cached {
            let mut params = vec![json!(self.repository_id)];
            if self.bluehost {
                params.push(json!(cached.revision));
            }
            let state = self.query_measured(storage::CHECK, params).await?;
            metrics.db_queries += 1;
            metrics.db_response_bytes += state.bytes;
            let row = state.data["rows"]
                .as_array()
                .and_then(|rows| rows.first())
                .ok_or("coordination schema/repository is not initialized")?;
            let (revision, high, context) =
                (integer(&row[0])?, integer(&row[1])?, integer(&row[2])?);
            let checkpoint_owner = match command {
                CoordCommand::Sync {
                    session_id,
                    after: None,
                    ..
                } => Some(session_id.as_str()),
                _ => None,
            };
            if high == cached.high
                && context == cached.context
                && cache::refresh(
                    &mut cached.snapshot,
                    &row[3],
                    checkpoint_owner,
                    self.bluehost,
                )?
            {
                metrics.projection_cache_hits += 1;
                return Ok((
                    revision,
                    high,
                    context,
                    cached.snapshot,
                    row.get(4).map(integer).transpose()?.unwrap_or(0),
                ));
            }
            if let Ok(mut cache) = self.cache.lock() {
                cache.remove(key);
            }
        }
        let state = self
            .query_measured(storage::LOAD, vec![json!(self.repository_id), json!(key)])
            .await?;
        metrics.db_queries += 1;
        metrics.db_response_bytes += state.bytes;
        let row = state.data["rows"]
            .as_array()
            .and_then(|rows| rows.first())
            .ok_or("coordination schema/repository is not initialized")?;
        Ok((
            integer(&row[0])?,
            integer(&row[1])?,
            integer(&row[2])?,
            decoded(&row[3])?,
            row.get(4).map(integer).transpose()?.unwrap_or(0),
        ))
    }
    fn remember(&self, key: &str, context: i64, high: i64, revision: i64, snapshot: Snapshot) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(key.to_string(), context, high, revision, snapshot);
        }
    }
    /// Run once after environment verification; migrations are tracked source.
    pub async fn migrate(&self) -> Result<()> {
        if self.bluehost {
            self.query(bluehost::INITIALIZE, vec![json!(self.repository_id)])
                .await?;
            return Ok(());
        }
        self.query(
            include_str!("../../../hosting/migrations/001-coordination.sql"),
            vec![],
        )
        .await?;
        self.query(
            include_str!("../../../hosting/migrations/002-coordination-rows.sql"),
            vec![],
        )
        .await?;
        self.query(
            include_str!("../../../hosting/migrations/003-coordination-row-version.sql"),
            vec![],
        )
        .await?;
        self.query(
            include_str!("../../../hosting/migrations/004-fence-legacy-writes.sql"),
            vec![],
        )
        .await?;
        self.query(
            include_str!("../../../hosting/migrations/005-coordination-context-version.sql"),
            vec![],
        )
        .await?;
        self.query("CREATE OR REPLACE TRIGGER grove_coordination_legacy_fence BEFORE UPDATE OF snapshot ON grove_coordination FOR EACH ROW EXECUTE FUNCTION grove_coordination_protect_archive()",vec![]).await?;
        for sql in [
            "CREATE UNIQUE INDEX IF NOT EXISTS grove_rows_event_seq ON grove_coordination_rows(repository_id,event_seq) WHERE table_name='coord_events'",
            "CREATE INDEX IF NOT EXISTS grove_rows_inbox ON grove_coordination_rows(repository_id,recipient,acked_at,created_at) WHERE table_name='coord_messages'",
            "CREATE UNIQUE INDEX IF NOT EXISTS grove_rows_dedup ON grove_coordination_rows(repository_id,sender,request_key) WHERE table_name='coord_messages'",
            "CREATE INDEX IF NOT EXISTS grove_rows_decisions ON grove_coordination_rows(repository_id,(data->>0),((data->>1)::bigint)) WHERE table_name='coord_decisions'",
        ] {
            self.query(sql, vec![]).await?;
        }
        self.query("INSERT INTO grove_coordination(repository_id) VALUES($1) ON CONFLICT(repository_id) DO NOTHING",vec![json!(self.repository_id)]).await?;
        self.query(storage::UPGRADE, vec![json!(self.repository_id)])
            .await?;
        Ok(())
    }
    /// Operator CLI only: imports into an empty destination and retains the
    /// archive. There is no overwrite/delete flag or remotely exposed tool.
    pub async fn import_snapshot(&self, archive: Archive) -> Result<()> {
        if self.bluehost {
            return Err("Bluehost imports require the staged row-store migration tool".into());
        }
        if archive.repository_id != self.repository_id || archive.revision < 0 {
            return Err("archive repository/revision mismatch".into());
        }
        let (snapshot, _) = evaluate(
            &self.repository_id,
            Some(&archive.snapshot),
            CoordCommand::Hello,
            false,
            chrono::Utc::now().timestamp_millis(),
        )?;
        let high = snapshot.tables["coord_events"]
            .iter()
            .filter_map(|row| row.first().and_then(Value::as_i64))
            .max()
            .unwrap_or(0);
        let raw = serde_json::to_string(&snapshot).map_err(|_| "cannot encode archive")?;
        if raw.len() > MAX_SNAPSHOT_BYTES {
            return Err("archive exceeds import size bound".into());
        }
        let result = self
            .query(
                storage::IMPORT,
                vec![
                    json!(self.repository_id),
                    json!(raw),
                    json!(high.to_string()),
                    json!(archive.revision.to_string()),
                ],
            )
            .await?;
        if result["rows"].as_array().is_none_or(Vec::is_empty) {
            return Err("snapshot import requires an initialized, empty destination".into());
        }
        Ok(())
    }
    pub async fn coordinate(self: &Arc<Self>, envelope: Envelope, operator: bool) -> Result<Value> {
        let mut metrics = RequestMetrics {
            operation: serde_json::to_value(&envelope.command)
                .ok()
                .and_then(|v| v["op"].as_str().map(str::to_owned))
                .unwrap_or_else(|| "unknown".into()),
            ..RequestMetrics::default()
        };
        let result = self
            .coordinate_inner(envelope, operator, &mut metrics, None)
            .await;
        metrics.ok = result.is_ok();
        // Names/counts only: never bodies, IDs, SQL text, headers or credentials.
        eprintln!(
            "grove_coord {}",
            serde_json::to_string(&metrics).unwrap_or_else(|_| "metrics unavailable".into())
        );
        result
    }
    async fn coordinate_inner(
        self: &Arc<Self>,
        envelope: Envelope,
        operator: bool,
        metrics: &mut RequestMetrics,
        now: Option<i64>,
    ) -> Result<Value> {
        if envelope.repository_id != self.repository_id {
            return Err("repository identity mismatch".into());
        }
        if let CoordCommand::Sync { limit, after, .. } = &envelope.command {
            if *limit == 0 || *limit > grove_lib::coordination::MAX_PAGE {
                return Err("limit must be 1..100".into());
            }
            if after.is_some_and(|v| v < 0) {
                return Err("invalid event cursor".into());
            }
        }
        let command_json = serde_json::to_string(&envelope.command)
            .map_err(|_| "cannot encode coordination command")?;
        let cacheable = matches!(envelope.command, CoordCommand::Sync { .. });
        for attempt in 0..MAX_CAS_ATTEMPTS {
            metrics.attempts += 1;
            let (revision, high, context, snapshot, history_floor) = self
                .working_set(&envelope.command, &command_json, metrics)
                .await?;
            metrics.projected_rows += snapshot.tables.values().map(Vec::len).sum::<usize>();
            let before = snapshot.clone();
            let command = envelope.command.clone();
            let repo = self.repository_id.clone();
            let evaluated = tokio::task::spawn_blocking(move || {
                evaluate_projected(
                    &repo,
                    Some(&snapshot),
                    command,
                    operator,
                    now.unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
                    Some(high),
                )
            })
            .await
            .map_err(|_| "coordination engine unavailable")?;
            let (snapshot, mut result, next_high) = evaluated?;
            if self.bluehost && matches!(envelope.command, CoordCommand::Sync { .. }) {
                let after = match &envelope.command {
                    CoordCommand::Sync { after, .. } => {
                        after.unwrap_or_else(|| result["checkpoint"].as_i64().unwrap_or(0))
                    }
                    _ => 0,
                };
                result["history_available_after"] = json!(history_floor);
                result["history_truncated"] = json!(after < history_floor);
                result["poll_after_seconds"] =
                    json!(if result["has_more"] == true { 0 } else { 30 });
            }
            let patch = storage::changes(&before, &snapshot)?;
            if patch.is_empty() {
                // The SELECT is the linearization point for unchanged reads.
                if cacheable {
                    self.remember(&command_json, context, next_high, revision, snapshot);
                }
                return Ok(result);
            }
            let changes_context = storage::changes_context(&before, &patch)?;
            let patch = serde_json::to_string(&patch).map_err(|_| "cannot encode row changes")?;
            let committed = self
                .query_measured(
                    storage::COMMIT,
                    vec![
                        json!(self.repository_id),
                        json!(revision.to_string()),
                        json!(patch),
                        json!(next_high.to_string()),
                        json!(changes_context),
                    ],
                )
                .await?;
            metrics.db_queries += 1;
            metrics.db_response_bytes += committed.bytes;
            if committed.data["rows"]
                .as_array()
                .is_some_and(|rows| !rows.is_empty())
            {
                if cacheable {
                    let new_context = integer(&committed.data["rows"][0][1])?;
                    self.remember(
                        &command_json,
                        new_context,
                        next_high,
                        revision + 1,
                        snapshot,
                    );
                }
                return Ok(result);
            }
            // A concurrent request won. Re-evaluate from its committed state;
            // no response or notification from this discarded result escapes.
            tokio::time::sleep(Duration::from_millis(5 + (attempt as u64) * 7)).await;
        }
        Err("coordination contention; retry the same request/key".into())
    }
}

pub fn evaluate(
    repo: &str,
    snapshot: Option<&Snapshot>,
    command: CoordCommand,
    operator: bool,
    now: i64,
) -> Result<(Snapshot, Value)> {
    let (snapshot, response, _) = evaluate_projected(repo, snapshot, command, operator, now, None)?;
    Ok((snapshot, response))
}

fn evaluate_projected(
    repo: &str,
    snapshot: Option<&Snapshot>,
    command: CoordCommand,
    operator: bool,
    now: i64,
    high: Option<i64>,
) -> Result<(Snapshot, Value, i64)> {
    let mut conn =
        rusqlite::Connection::open_in_memory().map_err(|_| "cannot create coordination engine")?;
    coordination::migrate(&conn).map_err(|_| "cannot initialize coordination engine")?;
    if let Some(snapshot) = snapshot {
        coordination_snapshot::restore(&mut conn, snapshot, repo).map_err(|e| e.to_string())?;
    } else {
        conn.execute(
            "UPDATE coord_meta SET value=?1 WHERE key='repository_id'",
            [repo],
        )
        .map_err(|_| "cannot initialize repository identity")?;
    }
    if let Some(high) = high {
        let existing: i64 = conn
            .query_row("SELECT COALESCE(MAX(seq),0) FROM coord_events", [], |r| {
                r.get(0)
            })
            .map_err(|_| "cannot read event watermark")?;
        if high < existing {
            return Err("invalid global event watermark".into());
        }
        conn.execute("DELETE FROM sqlite_sequence WHERE name='coord_events'", [])
            .map_err(|_| "cannot initialize event sequence")?;
        conn.execute(
            "INSERT INTO sqlite_sequence(name,seq) VALUES('coord_events',?1)",
            [high],
        )
        .map_err(|_| "cannot initialize event sequence")?;
    }
    let mut command = command;
    match &mut command {
        CoordCommand::Register {
            observation: Some(obs),
            ..
        }
        | CoordCommand::Heartbeat {
            observation: Some(obs),
            ..
        } => obs.verification = "client_reported".into(),
        _ => {}
    }
    let response = coordination::execute(&mut conn, Some(repo), command, now, operator)
        .map_err(|e| e.to_string())?;
    if serde_json::to_vec(&response)
        .map_err(|_| "cannot encode coordination response")?
        .len()
        > MAX_RESPONSE_BYTES
    {
        return Err("coordination response exceeds size bound; reduce scope/page".into());
    }
    let snapshot = coordination_snapshot::export(&conn).map_err(|e| e.to_string())?;
    let high = conn
        .query_row(
            "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name='coord_events'),0)",
            [],
            |r| r.get(0),
        )
        .map_err(|_| "cannot read event sequence")?;
    Ok((snapshot, response, high))
}

#[cfg(test)]
#[path = "postgres_tests.rs"]
mod postgres_tests;

#[cfg(test)]
#[path = "bluehost_tests.rs"]
mod bluehost_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sql_http_endpoint_uses_neons_regional_api_for_pooled_and_direct_connections() {
        for label in ["ep-fixture", "ep-fixture-pooler"] {
            let connection = format!(
                "postgresql://fixture:unused@{label}.us-east-1.aws.neon.tech/db?sslmode=require"
            );
            assert_eq!(
                neon_http_endpoint(&connection).unwrap().as_str(),
                "https://api.us-east-1.aws.neon.tech/sql"
            );
        }
        assert!(neon_http_endpoint("postgresql://fixture:unused@localhost/db").is_err());
        assert!(neon_http_endpoint("https://ep-fixture.us-east-1.aws.neon.tech/db").is_err());
    }
    #[test]
    fn cloud_evaluation_recovers_the_same_store_without_filesystem_state() {
        let repo = uuid::Uuid::new_v4().to_string();
        let (snap, hello) = evaluate(&repo, None, CoordCommand::Hello, false, 100).unwrap();
        let (recovered, again) =
            evaluate(&repo, Some(&snap), CoordCommand::Hello, false, 200).unwrap();
        assert_eq!(hello, again);
        assert_eq!(snap.tables, recovered.tables);
    }
    #[test]
    fn cloud_client_cannot_promote_reported_git_metadata_to_local_verification() {
        let repo = uuid::Uuid::new_v4().to_string();
        let command=serde_json::from_value(json!({"op":"register","session_id":"codex:real-cloud-thread","task":"fixture","scope":["."],"acceptance":["recover peers"],
            "observation":{"worktree_id":"distinct-cloud-worktree","worktree":"/workspace/iws","branch":"main","commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","dirty":false,"pr_url":null,"verification":"local_git"}})).unwrap();
        let (_, registered) = evaluate(&repo, None, command, false, 100).unwrap();
        assert_eq!(registered["observation"]["verification"], "client_reported");
    }
}

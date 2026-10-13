//! Stateless Vercel backend. Postgres commits the shared state with an atomic
//! revision compare-and-swap; the exact local coordination engine evaluates it.
//! This bounded milestone avoids two independently implemented inbox/policy
//! stores. Native Postgres tables can replace snapshot persistence later.
use grove_daemon::{
    coordination,
    coordination_snapshot::{self, Snapshot},
};
use grove_lib::coordination::CoordCommand;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

pub const MAX_REQUEST_BYTES: usize = 1_048_576;
pub const MAX_SNAPSHOT_BYTES: usize = 16_777_216;
pub const MAX_RESPONSE_BYTES: usize = 4_194_304;
const MAX_CAS_ATTEMPTS: u32 = 12;

type Result<T> = std::result::Result<T, String>;

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
    client: reqwest::Client,
    endpoint: reqwest::Url,
    connection: reqwest::header::HeaderValue,
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
        let key = if migration {
            "DATABASE_URL_UNPOOLED"
        } else {
            "DATABASE_URL"
        };
        let connection = std::env::var(key).map_err(|_| format!("{key} is missing"))?;
        let endpoint = neon_http_endpoint(&connection)?;
        let mut connection = reqwest::header::HeaderValue::from_str(&connection)
            .map_err(|_| "invalid database connection header")?;
        connection.set_sensitive(true);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(12))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| "cannot construct database transport")?;
        Ok(Self {
            repository_id,
            token,
            approver: std::env::var("GROVE_COORD_APPROVER_TOKEN").ok(),
            client,
            endpoint,
            connection,
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
    async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Value> {
        // Neon's official SQL-over-HTTP protocol; all SQL is static, values bound.
        let mut response = self
            .client
            .post(self.endpoint.clone())
            .header("Neon-Connection-String", self.connection.clone())
            .header("Neon-Raw-Text-Output", "true")
            .header("Neon-Array-Mode", "true")
            .json(&json!({"query":sql,"params":params}))
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
                return Err("coordination database response exceeds milestone size bound".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| "invalid coordination database response".into())
    }
    /// Run once after environment verification; migrations are tracked source.
    pub async fn migrate(&self) -> Result<()> {
        self.query(
            include_str!("../../../hosting/migrations/001-coordination.sql"),
            vec![],
        )
        .await?;
        self.query("INSERT INTO grove_coordination(repository_id) VALUES($1) ON CONFLICT(repository_id) DO NOTHING",vec![json!(self.repository_id)]).await?;
        Ok(())
    }
    pub async fn coordinate(self: &Arc<Self>, envelope: Envelope, operator: bool) -> Result<Value> {
        if envelope.repository_id != self.repository_id {
            return Err("repository identity mismatch".into());
        }
        for attempt in 0..MAX_CAS_ATTEMPTS {
            let state = self
                .query(
                    "SELECT revision,snapshot FROM grove_coordination WHERE repository_id=$1",
                    vec![json!(self.repository_id)],
                )
                .await?;
            let row = state["rows"]
                .as_array()
                .and_then(|r| r.first())
                .ok_or("coordination schema/repository is not initialized")?;
            let revision = row[0]
                .as_i64()
                .or_else(|| row[0].as_str().and_then(|v| v.parse().ok()))
                .ok_or("invalid coordination state revision")?;
            let snapshot: Option<Snapshot> = if row[1].is_null() {
                None
            } else if let Some(raw) = row[1].as_str() {
                Some(
                    serde_json::from_str(raw)
                        .map_err(|_| "invalid stored coordination snapshot")?,
                )
            } else {
                Some(
                    serde_json::from_value(row[1].clone())
                        .map_err(|_| "invalid stored coordination snapshot")?,
                )
            };
            let command = envelope.command.clone();
            let repo = self.repository_id.clone();
            let evaluated = tokio::task::spawn_blocking(move || {
                evaluate(
                    &repo,
                    snapshot.as_ref(),
                    command,
                    operator,
                    chrono::Utc::now().timestamp_millis(),
                )
            })
            .await
            .map_err(|_| "coordination engine unavailable")?;
            let (snapshot, result) = evaluated?;
            let raw =
                serde_json::to_string(&snapshot).map_err(|_| "cannot encode coordination state")?;
            if raw.len() > MAX_SNAPSHOT_BYTES {
                return Err("coordination state exceeds 16 MiB milestone bound; export/archive before adding more state".into());
            }
            let committed=self.query("UPDATE grove_coordination SET snapshot=$2::jsonb,revision=revision+1,updated_at=now() WHERE repository_id=$1 AND revision=$3::bigint RETURNING revision",
                vec![json!(self.repository_id),json!(raw),json!(revision.to_string())]).await?;
            if committed["rows"]
                .as_array()
                .is_some_and(|rows| !rows.is_empty())
            {
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
    Ok((snapshot, response))
}

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

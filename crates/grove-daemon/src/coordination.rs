//! Durable coordination, owned and written exclusively by DaemonState.
//! SQLite transactions commit state and its event together; reads never ack.

use grove_lib::coordination::*;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::collections::HashSet;

#[derive(Debug, thiserror::Error)]
pub enum CoordError {
    #[error("coordination database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("invalid coordination record: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Invalid(String),
}
type Result<T> = std::result::Result<T, CoordError>;

#[cfg(test)]
#[path = "coordination_tests.rs"]
mod tests;

fn require(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(CoordError::Invalid(message.into()))
    }
}
fn text(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.contains('\0')
}
fn identifier(value: &str) -> bool {
    text(value, 256) && !value.contains(['\n', '\r'])
}
fn scopes(values: &[String]) -> bool {
    !values.is_empty() && values.len() <= 64 && values.iter().all(|v| valid_scope(v))
}
fn commit(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|b| b.is_ascii_hexdigit())
}

pub fn migrate(conn: &Connection) -> std::result::Result<(), rusqlite::Error> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS coord_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS coord_sessions (
           session_id TEXT PRIMARY KEY, payload TEXT NOT NULL,
           checkpoint INTEGER NOT NULL DEFAULT 0, served_cursor INTEGER NOT NULL DEFAULT 0);
         CREATE TABLE IF NOT EXISTS coord_messages (
           id TEXT PRIMARY KEY, sender TEXT NOT NULL, recipient TEXT NOT NULL,
           request_key TEXT NOT NULL, request_json TEXT NOT NULL, created_at INTEGER NOT NULL,
           served_at INTEGER, acked_at INTEGER, UNIQUE(sender, request_key));
         CREATE INDEX IF NOT EXISTS coord_inbox ON coord_messages(recipient, acked_at, created_at);
         CREATE TABLE IF NOT EXISTS coord_decisions (
           id TEXT NOT NULL, revision INTEGER NOT NULL, scope TEXT NOT NULL, body TEXT NOT NULL,
           provenance TEXT NOT NULL, proposer TEXT NOT NULL, status TEXT NOT NULL,
           authority TEXT, created_at INTEGER NOT NULL, approved_at INTEGER,
           PRIMARY KEY(id, revision));
         CREATE TABLE IF NOT EXISTS coord_decision_acks (
           session_id TEXT NOT NULL, decision_id TEXT NOT NULL, revision INTEGER NOT NULL,
           acked_at INTEGER NOT NULL, PRIMARY KEY(session_id, decision_id, revision));
         CREATE TABLE IF NOT EXISTS coord_claims (
           id TEXT PRIMARY KEY, session_id TEXT NOT NULL, path TEXT NOT NULL,
           epoch INTEGER NOT NULL, expires_at INTEGER NOT NULL, released INTEGER NOT NULL,
           UNIQUE(session_id, path));
         CREATE TABLE IF NOT EXISTS coord_transfers (
           id TEXT PRIMARY KEY, session_id TEXT NOT NULL, payload TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS coord_events (
           seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, actor TEXT,
           recipient TEXT, scope TEXT NOT NULL, payload TEXT NOT NULL, created_at INTEGER NOT NULL);
         CREATE INDEX IF NOT EXISTS coord_event_recipient ON coord_events(recipient, seq);",
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO coord_meta(key,value) VALUES('repository_id',?1)",
        [uuid::Uuid::new_v4().to_string()],
    )?;
    Ok(())
}

pub fn repository_id(conn: &Connection) -> Result<String> {
    Ok(conn.query_row(
        "SELECT value FROM coord_meta WHERE key='repository_id'",
        [],
        |r| r.get(0),
    )?)
}

fn session(conn: &Connection, id: &str) -> Result<Session> {
    require(identifier(id), "invalid session identity")?;
    let raw: Option<String> = conn
        .query_row(
            "SELECT payload FROM coord_sessions WHERE session_id=?1",
            [id],
            |r| r.get(0),
        )
        .optional()?;
    serde_json::from_str(
        &raw.ok_or_else(|| CoordError::Invalid("session is not registered".into()))?,
    )
    .map_err(Into::into)
}

pub fn sessions(conn: &Connection, now: i64) -> Result<Vec<Session>> {
    let mut stmt = conn.prepare("SELECT payload FROM coord_sessions ORDER BY session_id")?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    rows.map(|r| {
        let mut s: Session = serde_json::from_str(&r?)?;
        if s.status != "stopped" && now - s.heartbeat_at > DEFAULT_STALE_MS {
            s.status = "stale".into();
        }
        Ok(s)
    })
    .collect()
}

fn save_session(tx: &Transaction<'_>, s: &Session) -> Result<()> {
    tx.execute(
        "INSERT INTO coord_sessions(session_id,payload) VALUES(?1,?2)
        ON CONFLICT(session_id) DO UPDATE SET payload=excluded.payload",
        params![s.session_id, serde_json::to_string(s)?],
    )?;
    Ok(())
}

fn event(
    tx: &Transaction<'_>,
    kind: &str,
    actor: Option<&str>,
    recipient: Option<&str>,
    scope: &[String],
    payload: Value,
    now: i64,
) -> Result<i64> {
    tx.execute("INSERT INTO coord_events(kind,actor,recipient,scope,payload,created_at) VALUES(?1,?2,?3,?4,?5,?6)",
        params![kind,actor,recipient,serde_json::to_string(scope)?,payload.to_string(),now])?;
    Ok(tx.last_insert_rowid())
}

fn validate_observation(obs: &GitObservation) -> Result<()> {
    require(
        identifier(&obs.worktree_id)
            && text(&obs.worktree, 4096)
            && text(&obs.branch, 1024)
            && commit(&obs.commit)
            && obs.pr_url.as_ref().is_none_or(|v| text(v, 2048))
            && obs.base_commit.as_ref().is_none_or(|c| commit(c))
            && obs.changed_paths.len() <= 2000
            && obs.changed_paths.iter().all(|p| text(p, 4096)),
        "invalid git observation",
    )
}

fn claims(conn: &Connection, now: i64, peers: &[Session]) -> Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        "SELECT id,session_id,path,epoch,expires_at,released FROM coord_claims ORDER BY id",
    )?;
    let rows = stmt.query_map([], |r| Ok(json!({"claim_id":r.get::<_,String>(0)?,"session_id":r.get::<_,String>(1)?,
        "path":r.get::<_,String>(2)?,"epoch":r.get::<_,i64>(3)?,"expires_at":r.get::<_,i64>(4)?,"released":r.get::<_,bool>(5)?})))?;
    let mut values: Vec<Value> = rows.collect::<std::result::Result<_, _>>()?;
    for c in &mut values {
        c["expired"] = json!(c["expires_at"].as_i64().unwrap_or(0) <= now);
        c["stale_owner"] = json!(
            peers
                .iter()
                .find(|p| p.session_id == c["session_id"])
                .is_none_or(|p| p.status == "stale" || p.status == "stopped")
        );
    }
    let copy = values.clone();
    for c in &mut values {
        c["overlaps"] = json!(
            copy.iter()
                .filter(|other| other["session_id"] != c["session_id"]
                    && other["released"] == false
                    && other["expired"] == false
                    && scopes_overlap(
                        c["path"].as_str().unwrap_or(""),
                        other["path"].as_str().unwrap_or("")
                    ))
                .map(|v| v["claim_id"].clone())
                .collect::<Vec<_>>()
        );
    }
    Ok(values)
}

fn decisions(conn: &Connection, viewer: &Session) -> Result<(Vec<Value>, Vec<Value>)> {
    let mut stmt = conn.prepare("SELECT id,revision,scope,body,provenance,proposer,status,authority,created_at,approved_at
        FROM coord_decisions WHERE status='approved' OR (status='proposed' AND revision=(SELECT MAX(d.revision) FROM coord_decisions d WHERE d.id=coord_decisions.id)) ORDER BY id,revision")?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_,String>(2)?,json!({"decision_id":r.get::<_,String>(0)?,"revision":r.get::<_,i64>(1)?,
        "body":r.get::<_,String>(3)?,"provenance":r.get::<_,String>(4)?,"proposer":r.get::<_,String>(5)?,
        "status":r.get::<_,String>(6)?,"authority":r.get::<_,Option<String>>(7)?,"created_at":r.get::<_,i64>(8)?,"approved_at":r.get::<_,Option<i64>>(9)?}))))?;
    let mut current = Vec::new();
    let mut proposed = Vec::new();
    for row in rows {
        let (raw, mut d) = row?;
        let scope: Vec<String> = serde_json::from_str(&raw)?;
        if !relevant(&scope, &viewer.scope) {
            continue;
        }
        d["scope"] = json!(scope);
        let acked: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM coord_decision_acks WHERE session_id=?1 AND decision_id=?2 AND revision=?3)",
            params![viewer.session_id,d["decision_id"].as_str(),d["revision"].as_i64()],|r|r.get(0))?;
        d["acknowledged"] = json!(acked);
        if d["status"] == "approved" {
            current.push(d);
        } else {
            proposed.push(d);
        }
    }
    Ok((current, proposed))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferRecord {
    pub session_id: String,
    pub task: String,
    pub scope: Vec<String>,
    pub manifest: TransferManifest,
    pub acceptance: Vec<String>,
    pub observation: Option<TransferObservation>,
    pub review: Option<TransferReview>,
    pub checked_at: Option<i64>,
}

fn transfer(conn: &Connection, id: &str) -> Result<TransferRecord> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT payload FROM coord_transfers WHERE id=?1",
            [id],
            |r| r.get(0),
        )
        .optional()?;
    decode(raw.ok_or_else(|| CoordError::Invalid("transfer not found".into()))?)
}
fn decode<T: DeserializeOwned>(raw: String) -> Result<T> {
    Ok(serde_json::from_str(&raw)?)
}
fn save_transfer(
    tx: &Transaction<'_>,
    id: &str,
    owner: &str,
    record: &TransferRecord,
) -> Result<()> {
    tx.execute(
        "INSERT INTO coord_transfers(id,session_id,payload) VALUES(?1,?2,?3)
        ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
        params![id, owner, serde_json::to_string(record)?],
    )?;
    Ok(())
}
fn transfer_owner(conn: &Connection, id: &str) -> Result<String> {
    Ok(conn.query_row(
        "SELECT session_id FROM coord_transfers WHERE id=?1",
        [id],
        |r| r.get(0),
    )?)
}

pub fn transfer_status(record: &TransferRecord, peers: &[Session]) -> Value {
    let m = &record.manifest;
    let o = record.observation.as_ref();
    let source_stale =
        o.is_none_or(|o| o.source_commit != m.source_commit || o.base_commit != m.base_commit);
    let assignment_stale = peers
        .iter()
        .find(|s| s.session_id == record.session_id)
        .is_none_or(|s| {
            s.task != record.task || s.scope != record.scope || s.acceptance != record.acceptance
        });
    let known_changes: Vec<Value> = [&m.source_ref,&m.target_ref].iter().filter_map(|reference| {
        let branch = reference.strip_prefix("refs/heads/").unwrap_or(reference);
        let latest = peers.iter().filter(|s|s.observation.branch == branch).max_by_key(|s|s.heartbeat_at)?;
        let observed = if *reference == &m.source_ref { &m.source_commit } else { &o?.target_commit };
        (latest.observation.commit != *observed).then(||json!({"reference":reference,"expected":observed,"reported":latest.observation.commit,"session_id":latest.session_id}))
    }).collect();
    let review_valid = record.review.as_ref().zip(o).is_some_and(|(r, o)| {
        r.source_commit == o.source_commit
            && r.base_commit == o.base_commit
            && r.target_commit == o.target_commit
    });
    let items: Vec<Value> = m.items.iter().map(|i| {
        let actual = o.and_then(|o|o.target_entries.get(&i.path));
        let auto = if actual.is_some_and(|v|*v == i.after) {
            if i.initial_target == i.after { "already_present" } else { "incorporated" }
        } else { "unresolved" };
        let reviewed = record.review.as_ref().filter(|_|review_valid).and_then(|r|r.items.iter().find(|r|r.path == i.path));
        json!({"path":i.path,"before":i.before,"source":i.after,"target":actual,
            "disposition":reviewed.map(|r|serde_json::to_value(&r.disposition).unwrap()).unwrap_or(json!(auto)),
            "evidence":reviewed.map(|r|r.evidence.as_str()),"verification":if reviewed.is_some(){"operator_review"}else{"tree_entry_comparison"}})
    }).collect();
    let unresolved: Vec<String> = items
        .iter()
        .filter(|v| v["disposition"] == "unresolved")
        .map(|v| v["path"].as_str().unwrap().into())
        .collect();
    let criteria_complete = review_valid
        && record.review.as_ref().is_some_and(|r| {
            r.criteria.len() == record.acceptance.len() && r.criteria.iter().all(|c| c.accepted)
        });
    let dirty = o.is_none_or(|o| o.target_dirty);
    json!({"source_commit":m.source_commit,"base_commit":m.base_commit,"target_commit":o.map(|o|&o.target_commit),
        "items":items,"unresolved":unresolved,"source_stale":source_stale,"known_ref_changes":known_changes,
        "review_valid":review_valid,"task":record.task,"scope":record.scope,"assignment_stale":assignment_stale,"acceptance":record.acceptance,"criteria_reviewed":criteria_complete,
        "target_dirty":dirty,"checked_at":record.checked_at,"requires_git_recheck":true,
        "complete_at_observed_commits":!source_stale && !assignment_stale && known_changes.is_empty() && unresolved.is_empty() && criteria_complete && !dirty,
        "provenance":"client_git_observation"})
}

pub fn execute(
    conn: &mut Connection,
    requested_repo: Option<&str>,
    command: CoordCommand,
    now: i64,
    approver: bool,
) -> Result<Value> {
    let repo = repository_id(conn)?;
    require(
        requested_repo.is_none_or(|r| r == repo),
        "repository identity mismatch",
    )?;
    if !matches!(command, CoordCommand::Hello) {
        require(requested_repo.is_some(), "repository_id is required")?;
    }
    require(
        !command.operator_only() || approver,
        "operator credential required",
    )?;
    if !matches!(command, CoordCommand::Register { .. })
        && let Some(id) = command.session_id()
    {
        session(conn, id)?;
    }
    let tx = conn.transaction()?;
    let result = match command {
        CoordCommand::Hello => {
            json!({"repository_id":repo,"protocol_version":1,"delivery":"durable_polling","stale_after_ms":DEFAULT_STALE_MS})
        }
        CoordCommand::Register {
            session_id,
            task,
            scope,
            acceptance,
            parent_session_id,
            observation,
        } => {
            require(
                identifier(&session_id)
                    && text(&task, 2048)
                    && scopes(&scope)
                    && !acceptance.is_empty()
                    && acceptance.len() <= 64
                    && acceptance.iter().all(|c| text(c, 4096)),
                "registration requires stable identity, task, literal scope and acceptance criteria",
            )?;
            let (provider, native) = session_id.split_once(':').ok_or_else(|| {
                CoordError::Invalid(
                    "session_id must be provider:native_id (e.g. codex:thread-uuid)".into(),
                )
            })?;
            require(
                matches!(provider, "codex" | "opencode" | "claude") && identifier(native),
                "invalid provider-qualified session identity",
            )?;
            if let Some(parent) = &parent_session_id {
                require(parent != &session_id, "session cannot be its own parent")?;
                session(&tx, parent)?;
                let mut cursor = Some(parent.clone());
                let mut visited = HashSet::new();
                while let Some(id) = cursor {
                    require(
                        id != session_id && visited.insert(id.clone()),
                        "parent relationship cycle",
                    )?;
                    cursor = session(&tx, &id)?.parent_session_id;
                }
            }
            let mut obs = observation.ok_or_else(|| {
                CoordError::Invalid("registration requires git metadata from the client".into())
            })?;
            validate_observation(&obs)?;
            let previous = session(&tx, &session_id).ok();
            if obs.pr_url.is_none() {
                obs.pr_url = previous.as_ref().and_then(|s| s.observation.pr_url.clone());
            }
            let s = Session {
                session_id: session_id.clone(),
                provider: provider.into(),
                native_session_id: native.into(),
                parent_session_id,
                task,
                scope,
                acceptance,
                observation: obs,
                status: "active".into(),
                registered_at: previous.as_ref().map_or(now, |s| s.registered_at),
                heartbeat_at: now,
            };
            save_session(&tx, &s)?;
            if previous.as_ref().is_none_or(|p| {
                p.task != s.task
                    || p.scope != s.scope
                    || p.acceptance != s.acceptance
                    || p.observation != s.observation
                    || p.parent_session_id != s.parent_session_id
                    || p.status != s.status
            }) {
                event(
                    &tx,
                    "session_updated",
                    Some(&session_id),
                    None,
                    &s.scope,
                    json!(s),
                    now,
                )?;
            }
            json!(s)
        }
        CoordCommand::Heartbeat {
            session_id,
            status,
            observation,
        } => {
            let mut s = session(&tx, &session_id)?;
            let old = s.clone();
            if let Some(status) = status {
                require(
                    matches!(status.as_str(), "active" | "idle" | "stopped"),
                    "invalid session status",
                )?;
                s.status = status;
            }
            if let Some(mut obs) = observation {
                validate_observation(&obs)?;
                if obs.pr_url.is_none() {
                    obs.pr_url = s.observation.pr_url.clone();
                }
                s.observation = obs;
            }
            s.heartbeat_at = now;
            save_session(&tx, &s)?;
            if old.observation != s.observation || old.status != s.status {
                event(
                    &tx,
                    "session_updated",
                    Some(&session_id),
                    None,
                    &s.scope,
                    json!(s),
                    now,
                )?;
            }
            json!(s)
        }
        CoordCommand::Sync {
            session_id,
            after,
            limit,
        } => {
            require(limit > 0 && limit <= MAX_PAGE, "limit must be 1..100")?;
            let viewer = session(&tx, &session_id)?;
            let checkpoint: i64 = tx.query_row(
                "SELECT checkpoint FROM coord_sessions WHERE session_id=?1",
                [&session_id],
                |r| r.get(0),
            )?;
            let cursor = after.unwrap_or(checkpoint);
            require(cursor >= 0, "invalid event cursor")?;
            let high: i64 =
                tx.query_row("SELECT COALESCE(MAX(seq),0) FROM coord_events", [], |r| {
                    r.get(0)
                })?;
            require(cursor <= high, "cursor exceeds event high watermark")?;
            let mut stmt=tx.prepare("SELECT seq,kind,scope,payload,created_at FROM coord_events WHERE seq>?1 AND (recipient IS NULL OR recipient=?2 OR actor=?2) ORDER BY seq LIMIT 1001")?;
            let rows = stmt.query_map(params![cursor, session_id], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)?,
                ))
            })?;
            let mut updates = Vec::new();
            let mut next = cursor;
            let mut scanned = 0;
            for row in rows {
                let (seq, kind, raw_scope, raw, created) = row?;
                if updates.len() == limit || scanned == 1000 {
                    break;
                }
                scanned += 1;
                next = seq;
                if relevant(&decode::<Vec<String>>(raw_scope)?, &viewer.scope) {
                    updates.push(json!({"cursor":seq,"kind":kind,"data":decode::<Value>(raw)?,"created_at":created}));
                }
            }
            drop(stmt);
            // With fewer than the scan/page bound, only invisible events remain.
            if updates.len() < limit && scanned < 1000 {
                next = high;
            }
            tx.execute(
                "UPDATE coord_sessions SET served_cursor=MAX(served_cursor,?2) WHERE session_id=?1",
                params![session_id, next],
            )?;
            let mut stmt=tx.prepare("SELECT id,request_json,created_at,served_at FROM coord_messages WHERE recipient=?1 AND acked_at IS NULL ORDER BY created_at,id LIMIT 100")?;
            let rows = stmt.query_map([&session_id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, Option<i64>>(3)?,
                ))
            })?;
            let mut inbox = Vec::new();
            for row in rows {
                let (id, raw, created, served) = row?;
                inbox.push(json!({"message_id":id,"request":decode::<Value>(raw)?,"enqueued_at":created,"service_read_at":served.unwrap_or(now)}));
            }
            drop(stmt);
            for m in &inbox {
                tx.execute(
                    "UPDATE coord_messages SET served_at=COALESCE(served_at,?2) WHERE id=?1",
                    params![m["message_id"].as_str(), now],
                )?;
            }
            let peers = sessions(&tx, now)?;
            let claim_view = claims(&tx, now, &peers)?;
            let (current, proposed) = decisions(&tx, &viewer)?;
            let assignment_overlaps:Vec<Value>=peers.iter().filter(|s|s.session_id!=session_id && relevant(&s.scope,&viewer.scope)).map(|s|
                json!({"session_id":s.session_id,"task":s.task,"same_assignment":s.task==viewer.task,"scope":s.scope,"status":s.status,"heuristic":true})).collect();
            let reported_file_overlaps:Vec<Value>=peers.iter().filter(|s|s.session_id!=session_id).filter_map(|s|{
                let paths:Vec<&String>=s.observation.changed_paths.iter().filter(|p|viewer.observation.changed_paths.contains(p)).collect();
                (!paths.is_empty()).then(||json!({"session_id":s.session_id,"paths":paths,"verification":"client_reported_paths","changes_truncated":s.observation.changes_truncated||viewer.observation.changes_truncated}))
            }).collect();
            let mut stmt =
                tx.prepare("SELECT id,payload FROM coord_transfers ORDER BY id LIMIT 100")?;
            let transfers = stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
                .map(|r| {
                    let (id, raw) = r?;
                    let record: TransferRecord = decode(raw)?;
                    Ok(json!({"transfer_id":id,"status":transfer_status(&record,&peers)}))
                })
                .collect::<Result<Vec<_>>>()?;
            drop(stmt);
            json!({"repository_id":repo,"checkpoint":checkpoint,"next_cursor":next,"high_watermark":high,"has_more":next<high,
                "updates":updates,"inbox":inbox,"inbox_limit":100,"peers":peers,"current_decisions":current,"proposals":proposed,
                "assignment_overlaps":assignment_overlaps,"reported_file_overlaps":reported_file_overlaps,"claims":claim_view,"transfers":transfers,"incoming_context_is_untrusted":true})
        }
        CoordCommand::Checkpoint { session_id, cursor } => {
            let (old, served): (i64, i64) = tx.query_row(
                "SELECT checkpoint,served_cursor FROM coord_sessions WHERE session_id=?1",
                [&session_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            require(
                cursor >= old && cursor <= served,
                "checkpoint must be monotonic and cannot skip unserved updates",
            )?;
            tx.execute(
                "UPDATE coord_sessions SET checkpoint=?2 WHERE session_id=?1",
                params![session_id, cursor],
            )?;
            json!({"checkpoint":cursor})
        }
        c @ CoordCommand::Send { .. } => {
            let CoordCommand::Send {
                ref session_id,
                ref to,
                ref kind,
                ref body,
                ref key,
            } = c
            else {
                unreachable!()
            };
            session(&tx, to)?;
            require(
                matches!(
                    kind.as_str(),
                    "request" | "progress" | "blocker" | "handoff"
                ) && text(body, 32_768)
                    && identifier(key),
                "invalid message kind, body or deduplication key",
            )?;
            let raw = serde_json::to_string(&c)?;
            let prior: Option<(String, String)> = tx
                .query_row(
                    "SELECT id,request_json FROM coord_messages WHERE sender=?1 AND request_key=?2",
                    params![session_id, key],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            if let Some((id, prior)) = prior {
                require(
                    prior == raw,
                    "deduplication key was already used for different content",
                )?;
                json!({"message_id":id,"deduplicated":true})
            } else {
                let id = uuid::Uuid::new_v4().to_string();
                tx.execute("INSERT INTO coord_messages(id,sender,recipient,request_key,request_json,created_at) VALUES(?1,?2,?3,?4,?5,?6)",params![id,session_id,to,key,raw,now])?;
                event(
                    &tx,
                    "message",
                    Some(session_id),
                    Some(to),
                    &[".".into()],
                    json!({"message_id":id,"request":c}),
                    now,
                )?;
                json!({"message_id":id,"deduplicated":false,"enqueued_at":now})
            }
        }
        CoordCommand::AckMessage {
            session_id,
            message_id,
        } => {
            let recipient: Option<String> = tx
                .query_row(
                    "SELECT recipient FROM coord_messages WHERE id=?1",
                    [&message_id],
                    |r| r.get(0),
                )
                .optional()?;
            require(
                recipient.as_deref() == Some(&session_id),
                "message acknowledgement requires its recipient",
            )?;
            let n = tx.execute(
                "UPDATE coord_messages SET acked_at=?2 WHERE id=?1 AND acked_at IS NULL",
                params![message_id, now],
            )?;
            let sender: String = tx.query_row(
                "SELECT sender FROM coord_messages WHERE id=?1",
                [&message_id],
                |r| r.get(0),
            )?;
            if n > 0 {
                event(
                    &tx,
                    "message_acknowledged",
                    Some(&session_id),
                    Some(&sender),
                    &[".".into()],
                    json!({"message_id":message_id}),
                    now,
                )?;
            }
            let (queued, served, acked): (i64, Option<i64>, i64) = tx.query_row(
                "SELECT created_at,served_at,acked_at FROM coord_messages WHERE id=?1",
                [&message_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            json!({"message_id":message_id,"enqueued_at":queued,"service_read_at":served,"acknowledged_at":acked})
        }
        CoordCommand::Propose {
            session_id,
            decision_id,
            expected_revision,
            scope,
            body,
            provenance,
        } => {
            require(
                identifier(&decision_id)
                    && scopes(&scope)
                    && text(&body, 32_768)
                    && text(&provenance, 4096),
                "invalid decision proposal",
            )?;
            let current: i64 = tx.query_row(
                "SELECT COALESCE(MAX(revision),0) FROM coord_decisions WHERE id=?1",
                [&decision_id],
                |r| r.get(0),
            )?;
            require(
                current == expected_revision,
                "decision revision changed; retrieve current revisions before proposing",
            )?;
            let revision = current + 1;
            tx.execute("INSERT INTO coord_decisions(id,revision,scope,body,provenance,proposer,status,created_at) VALUES(?1,?2,?3,?4,?5,?6,'proposed',?7)",params![decision_id,revision,serde_json::to_string(&scope)?,body,provenance,session_id,now])?;
            event(
                &tx,
                "decision_proposed",
                Some(&session_id),
                None,
                &scope,
                json!({"decision_id":decision_id,"revision":revision,"status":"proposed"}),
                now,
            )?;
            json!({"decision_id":decision_id,"revision":revision,"status":"proposed"})
        }
        CoordCommand::Approve {
            decision_id,
            revision,
            authority,
        } => {
            require(
                text(&authority, 4096),
                "approval requires authority/provenance",
            )?;
            let newest: i64 = tx.query_row(
                "SELECT COALESCE(MAX(revision),0) FROM coord_decisions WHERE id=?1",
                [&decision_id],
                |r| r.get(0),
            )?;
            require(
                revision > 0 && revision == newest,
                "only the latest decision revision can be approved",
            )?;
            let (raw_scope, status): (String, String) = tx.query_row(
                "SELECT scope,status FROM coord_decisions WHERE id=?1 AND revision=?2",
                params![decision_id, revision],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if status != "approved" {
                tx.execute("UPDATE coord_decisions SET status='superseded' WHERE id=?1 AND status='approved'",[&decision_id])?;
                tx.execute("UPDATE coord_decisions SET status='approved',authority=?3,approved_at=?4 WHERE id=?1 AND revision=?2",params![decision_id,revision,authority,now])?;
                event(
                    &tx,
                    "decision_approved",
                    None,
                    None,
                    &decode::<Vec<String>>(raw_scope)?,
                    json!({"decision_id":decision_id,"revision":revision,"authority":authority,"supersedes_prior_approved_revision":true}),
                    now,
                )?;
            }
            json!({"decision_id":decision_id,"revision":revision,"status":"approved"})
        }
        CoordCommand::AckDecision {
            session_id,
            decision_id,
            revision,
        } => {
            let raw:Option<String>=tx.query_row("SELECT scope FROM coord_decisions WHERE id=?1 AND revision=?2 AND status='approved'",params![decision_id,revision],|r|r.get(0)).optional()?;
            let scope: Vec<String> = decode(raw.ok_or_else(|| {
                CoordError::Invalid(
                    "decision revision is not currently approved (possibly superseded)".into(),
                )
            })?)?;
            require(
                relevant(&scope, &session(&tx, &session_id)?.scope),
                "decision is outside session scope",
            )?;
            let n=tx.execute("INSERT OR IGNORE INTO coord_decision_acks(session_id,decision_id,revision,acked_at) VALUES(?1,?2,?3,?4)",params![session_id,decision_id,revision,now])?;
            if n > 0 {
                event(
                    &tx,
                    "decision_acknowledged",
                    Some(&session_id),
                    None,
                    &scope,
                    json!({"decision_id":decision_id,"revision":revision}),
                    now,
                )?;
            }
            json!({"decision_id":decision_id,"revision":revision,"acknowledged":true})
        }
        CoordCommand::Claim {
            session_id,
            path,
            ttl_seconds,
        } => {
            require(
                valid_scope(&path) && (5..=3600).contains(&ttl_seconds),
                "claim requires literal path and ttl_seconds 5..3600",
            )?;
            let prior:Option<(String,i64,i64,bool)>=tx.query_row("SELECT id,epoch,expires_at,released FROM coord_claims WHERE session_id=?1 AND path=?2",params![session_id,path],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
            let (id, epoch) = match prior {
                Some((id, e, expiry, false)) if expiry > now => (id, e),
                Some((id, e, _, _)) => (id, e + 1),
                None => (uuid::Uuid::new_v4().to_string(), 1),
            };
            // Repeated acquisition never renews an existing lease. Renewal is fenced.
            tx.execute("INSERT INTO coord_claims(id,session_id,path,epoch,expires_at,released) VALUES(?1,?2,?3,?4,?5,0)
                ON CONFLICT(id) DO UPDATE SET epoch=excluded.epoch,expires_at=CASE WHEN coord_claims.epoch=excluded.epoch THEN coord_claims.expires_at ELSE excluded.expires_at END,released=0",params![id,session_id,path,epoch,now+ttl_seconds*1000])?;
            event(
                &tx,
                "claim",
                Some(&session_id),
                None,
                &[path],
                json!({"claim_id":id,"epoch":epoch}),
                now,
            )?;
            let peers = sessions(&tx, now)?;
            claims(&tx, now, &peers)?
                .into_iter()
                .find(|v| v["claim_id"] == id)
                .unwrap()
        }
        CoordCommand::RenewClaim {
            session_id,
            claim_id,
            epoch,
            ttl_seconds,
        } => {
            require(
                (5..=3600).contains(&ttl_seconds),
                "ttl_seconds must be 5..3600",
            )?;
            let n=tx.execute("UPDATE coord_claims SET expires_at=?4 WHERE id=?1 AND session_id=?2 AND epoch=?3 AND released=0 AND expires_at>?5",params![claim_id,session_id,epoch,now+ttl_seconds*1000,now])?;
            require(
                n == 1,
                "claim is expired, released, or owned by another session/epoch",
            )?;
            json!({"claim_id":claim_id,"epoch":epoch,"expires_at":now+ttl_seconds*1000})
        }
        CoordCommand::ReleaseClaim {
            session_id,
            claim_id,
            epoch,
        } => {
            let n = tx.execute(
                "UPDATE coord_claims SET released=1 WHERE id=?1 AND session_id=?2 AND epoch=?3",
                params![claim_id, session_id, epoch],
            )?;
            require(n == 1, "claim belongs to another session/epoch")?;
            json!({"claim_id":claim_id,"epoch":epoch,"released":true})
        }
        CoordCommand::CaptureTransfer {
            session_id,
            transfer_id,
            manifest,
        } => {
            require(
                identifier(&transfer_id)
                    && commit(&manifest.source_commit)
                    && commit(&manifest.base_commit)
                    && commit(&manifest.initial_target_commit)
                    && text(&manifest.source_ref, 1024)
                    && text(&manifest.base_ref, 1024)
                    && text(&manifest.target_ref, 1024)
                    && !manifest.items.is_empty()
                    && manifest.items.len() <= 2000,
                "invalid transfer manifest",
            )?;
            let mut paths = HashSet::new();
            for i in &manifest.items {
                require(
                    valid_scope(&i.path)
                        && i.path != "."
                        && paths.insert(&i.path)
                        && i.before != i.after,
                    "invalid or duplicate transfer path",
                )?;
                for e in [&i.before, &i.after, &i.initial_target]
                    .into_iter()
                    .flatten()
                {
                    require(
                        commit(&e.oid)
                            && matches!(e.mode.as_str(), "100644" | "100755" | "120000" | "160000"),
                        "invalid tree entry",
                    )?;
                }
            }
            if let Ok(owner) = transfer_owner(&tx, &transfer_id) {
                require(owner == session_id, "transfer belongs to another session")?;
            }
            let owner = session(&tx, &session_id)?;
            let record = TransferRecord {
                session_id: session_id.clone(),
                task: owner.task,
                scope: owner.scope,
                manifest,
                acceptance: owner.acceptance,
                observation: None,
                review: None,
                checked_at: None,
            };
            save_transfer(&tx, &transfer_id, &session_id, &record)?;
            event(
                &tx,
                "transfer_captured",
                Some(&session_id),
                None,
                &[".".into()],
                json!({"transfer_id":transfer_id,"source_commit":record.manifest.source_commit,"paths":record.manifest.items.len()}),
                now,
            )?;
            json!({"transfer_id":transfer_id,"manifest":record.manifest,"acceptance":record.acceptance})
        }
        CoordCommand::GetTransfer {
            session_id: _,
            transfer_id,
        } => {
            let record = transfer(&tx, &transfer_id)?;
            json!({"transfer_id":transfer_id,"record":record,"status":transfer_status(&record,&sessions(&tx,now)?)})
        }
        CoordCommand::CheckTransfer {
            session_id,
            transfer_id,
            observation,
        } => {
            require(
                transfer_owner(&tx, &transfer_id)? == session_id,
                "transfer check requires its owner",
            )?;
            let mut record = transfer(&tx, &transfer_id)?;
            require(
                commit(&observation.source_commit)
                    && commit(&observation.base_commit)
                    && commit(&observation.target_commit)
                    && observation.target_entries.len() == record.manifest.items.len()
                    && record
                        .manifest
                        .items
                        .iter()
                        .all(|i| observation.target_entries.contains_key(&i.path)),
                "transfer observation must cover every captured path",
            )?;
            record.observation = Some(observation);
            record.checked_at = Some(now);
            save_transfer(&tx, &transfer_id, &session_id, &record)?;
            let status = transfer_status(&record, &sessions(&tx, now)?);
            event(
                &tx,
                "transfer_checked",
                Some(&session_id),
                None,
                &[".".into()],
                json!({"transfer_id":transfer_id,"status":status}),
                now,
            )?;
            status
        }
        CoordCommand::ReviewTransfer {
            transfer_id,
            review,
        } => {
            let mut record = transfer(&tx, &transfer_id)?;
            let obs = record
                .observation
                .as_ref()
                .ok_or_else(|| CoordError::Invalid("check transfer before reviewing it".into()))?;
            require(
                review.source_commit == obs.source_commit
                    && review.source_commit == record.manifest.source_commit
                    && review.base_commit == obs.base_commit
                    && review.base_commit == record.manifest.base_commit
                    && review.target_commit == obs.target_commit
                    && text(&review.reviewer, 4096),
                "review must identify current source/base/target commits and reviewer",
            )?;
            let mut paths = HashSet::new();
            for i in &review.items {
                require(
                    record.manifest.items.iter().any(|m| m.path == i.path)
                        && paths.insert(&i.path)
                        && text(&i.evidence, 8192),
                    "review path/evidence is missing, duplicated, or outside captured scope",
                )?;
            }
            let mut indices = HashSet::new();
            require(
                review.criteria.len() == record.acceptance.len(),
                "review must account for every acceptance criterion",
            )?;
            for c in &review.criteria {
                require(
                    c.index < record.acceptance.len()
                        && indices.insert(c.index)
                        && text(&c.evidence, 8192),
                    "invalid criterion review",
                )?;
            }
            record.review = Some(review);
            let owner = transfer_owner(&tx, &transfer_id)?;
            save_transfer(&tx, &transfer_id, &owner, &record)?;
            let status = transfer_status(&record, &sessions(&tx, now)?);
            event(
                &tx,
                "transfer_reviewed",
                None,
                None,
                &[".".into()],
                json!({"transfer_id":transfer_id,"status":status}),
                now,
            )?;
            status
        }
    };
    tx.commit()?;
    Ok(result)
}

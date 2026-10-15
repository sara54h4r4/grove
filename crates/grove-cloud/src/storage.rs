//! Project a consistent working set, evaluate the existing engine, and persist
//! only changed rows. Postgres owns all state, including immutable history.
use crate::Result;
use grove_daemon::coordination_snapshot::Snapshot;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const LOAD: &str = include_str!("../../../hosting/sql/load-rows.sql");
pub const COMMIT: &str = include_str!("../../../hosting/sql/commit-rows.sql");
pub const UPGRADE: &str = include_str!("../../../hosting/sql/upgrade-rows.sql");
pub const IMPORT: &str = include_str!("../../../hosting/sql/import-rows.sql");
pub const CHECK: &str = include_str!("../../../hosting/sql/check-context.sql");

#[derive(Serialize)]
pub struct Record {
    pub table_name: String,
    pub row_key: Value,
    pub data: Vec<Value>,
}

fn records(snapshot: &Snapshot) -> Result<BTreeMap<(String, String), Record>> {
    let mut result = BTreeMap::new();
    for (table, rows) in &snapshot.tables {
        let key_columns = match table.as_str() {
            "coord_decisions" => 2,
            "coord_decision_acks" => 3,
            _ => 1,
        };
        for row in rows {
            if row.len() < key_columns {
                return Err("invalid coordination row key".into());
            }
            let key = Value::Array(row[..key_columns].to_vec());
            let map_key = (table.clone(), key.to_string());
            if result
                .insert(
                    map_key,
                    Record {
                        table_name: table.clone(),
                        row_key: key,
                        data: row.clone(),
                    },
                )
                .is_some()
            {
                return Err("duplicate coordination row key".into());
            }
        }
    }
    Ok(result)
}

pub fn changes(before: &Snapshot, after: &Snapshot) -> Result<Vec<Record>> {
    let before = records(before)?;
    let mut after = records(after)?;
    // Current commands never delete history or identities. Fail closed if a
    // future engine command introduces deletion without a reviewed migration.
    if before.keys().any(|key| !after.contains_key(key)) {
        return Err("coordination operation unexpectedly removed a row".into());
    }
    after.retain(|key, value| before.get(key).is_none_or(|old| old.data != value.data));
    Ok(after.into_values().collect())
}

pub fn changes_context(before: &Snapshot, patch: &[Record]) -> Result<bool> {
    for record in patch {
        if record.table_name != "coord_sessions" {
            return Ok(true);
        }
        let Some(old) = before.tables["coord_sessions"]
            .iter()
            .find(|row| row[0] == record.data[0])
        else {
            return Ok(true);
        };
        let mut old: grove_lib::coordination::Session = crate::decoded(&old[1])?;
        let mut new: grove_lib::coordination::Session = crate::decoded(&record.data[1])?;
        old.heartbeat_at = 0;
        new.heartbeat_at = 0;
        if serde_json::to_value(old).map_err(|_| "invalid session")?
            != serde_json::to_value(new).map_err(|_| "invalid session")?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Archive {
    pub repository_id: String,
    pub revision: i64,
    pub snapshot: Snapshot,
}

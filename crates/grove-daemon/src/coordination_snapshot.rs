//! A versioned snapshot of coordination tables only. Cloud functions use
//! SQLite as the same tested state machine, while Postgres alone owns durable
//! state. No conflict cache, provider transcripts, or credentials are exported.
use crate::coordination::{CoordError, repository_id};
use rusqlite::{
    Connection, params_from_iter,
    types::{Value as SqlValue, ValueRef},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

const TABLES: &[&str] = &[
    "coord_meta",
    "coord_sessions",
    "coord_messages",
    "coord_decisions",
    "coord_decision_acks",
    "coord_claims",
    "coord_transfers",
    "coord_events",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub version: u32,
    pub tables: BTreeMap<String, Vec<Vec<Value>>>,
}

pub fn export(conn: &Connection) -> Result<Snapshot, CoordError> {
    let mut tables = BTreeMap::new();
    for &table in TABLES {
        let mut stmt = conn.prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))?;
        let count = stmt.column_count();
        let rows = stmt
            .query_map([], |row| {
                (0..count)
                    .map(|index| {
                        Ok(match row.get_ref(index)? {
                            ValueRef::Null => Value::Null,
                            ValueRef::Integer(v) => Value::from(v),
                            ValueRef::Real(v) => Value::from(v),
                            ValueRef::Text(v) => Value::String(
                                std::str::from_utf8(v)
                                    .map_err(|_| rusqlite::Error::InvalidQuery)?
                                    .into(),
                            ),
                            ValueRef::Blob(_) => return Err(rusqlite::Error::InvalidQuery),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?
            .collect::<Result<Vec<_>, _>>()?;
        tables.insert(table.into(), rows);
    }
    Ok(Snapshot { version: 1, tables })
}

pub fn restore(
    conn: &mut Connection,
    snapshot: &Snapshot,
    expected_repo: &str,
) -> Result<(), CoordError> {
    if snapshot.version != 1
        || snapshot.tables.len() != TABLES.len()
        || snapshot
            .tables
            .keys()
            .any(|t| !TABLES.contains(&t.as_str()))
    {
        return Err(CoordError::Invalid(
            "unsupported coordination snapshot version/tables".into(),
        ));
    }
    let tx = conn.transaction()?;
    for &table in TABLES {
        tx.execute(&format!("DELETE FROM {table}"), [])?;
        let count = tx
            .prepare(&format!("SELECT * FROM {table} LIMIT 0"))?
            .column_count();
        let placeholders = vec!["?"; count].join(",");
        let mut insert = tx.prepare(&format!("INSERT INTO {table} VALUES({placeholders})"))?;
        for row in &snapshot.tables[table] {
            if row.len() != count {
                return Err(CoordError::Invalid(
                    "coordination snapshot column count mismatch".into(),
                ));
            }
            let values = row
                .iter()
                .map(|value| match value {
                    Value::Null => Ok(SqlValue::Null),
                    Value::String(s) => Ok(SqlValue::Text(s.clone())),
                    Value::Number(n) if n.is_i64() => Ok(SqlValue::Integer(n.as_i64().unwrap())),
                    _ => Err(CoordError::Invalid(
                        "unsupported coordination snapshot value".into(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            insert.execute(params_from_iter(values))?;
        }
    }
    if repository_id(&tx)? != expected_repo {
        return Err(CoordError::Invalid(
            "coordination snapshot repository mismatch".into(),
        ));
    }
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use grove_lib::coordination::CoordCommand;
    #[test]
    fn snapshot_roundtrip_preserves_inbox_cursors_and_repository_identity() {
        let mut original = Connection::open_in_memory().unwrap();
        crate::coordination::migrate(&original).unwrap();
        let repo = repository_id(&original).unwrap();
        original.execute("INSERT INTO coord_events(seq,kind,scope,payload,created_at) VALUES(42,'message','[\".\"]','{}',10)",[]).unwrap();
        original.execute("INSERT INTO coord_messages(id,sender,recipient,request_key,request_json,created_at) VALUES('pending','codex:a','codex:b','key','{}',10)",[]).unwrap();
        let snapshot = export(&original).unwrap();
        let mut restored = Connection::open_in_memory().unwrap();
        crate::coordination::migrate(&restored).unwrap();
        restore(&mut restored, &snapshot, &repo).unwrap();
        assert_eq!(export(&restored).unwrap().tables, snapshot.tables);
        restored.execute("INSERT INTO coord_events(kind,scope,payload,created_at) VALUES('message','[\".\"]','{}',11)",[]).unwrap();
        assert_eq!(restored.last_insert_rowid(), 43);
        assert_eq!(
            crate::coordination::execute(&mut original, Some(&repo), CoordCommand::Hello, 0, false)
                .unwrap()["repository_id"],
            repo
        );
    }
    #[test]
    fn snapshot_repository_and_unknown_table_checks_are_atomic() {
        let mut conn = Connection::open_in_memory().unwrap();
        crate::coordination::migrate(&conn).unwrap();
        let repo = repository_id(&conn).unwrap();
        let snapshot = export(&conn).unwrap();
        assert!(restore(&mut conn, &snapshot, "another repository").is_err());
        assert_eq!(repository_id(&conn).unwrap(), repo);
        let mut invalid = snapshot;
        invalid.tables.insert("workspaces".into(), vec![]);
        assert!(restore(&mut conn, &invalid, &repo).is_err());
    }
}

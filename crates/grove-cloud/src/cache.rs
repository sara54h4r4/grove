//! A bounded optimization, never a state owner. Each hit requires a durable
//! context/version check and a fresh set of heartbeat/cursor fields.
use crate::{Result, decoded};
use grove_daemon::coordination_snapshot::Snapshot;
use grove_lib::coordination::Session;
use serde_json::Value;
use std::{collections::BTreeMap, time::Instant};

const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_ENTRIES: usize = 32;

#[derive(Clone)]
pub struct Entry {
    pub context: i64,
    pub high: i64,
    pub revision: i64,
    pub snapshot: Snapshot,
    bytes: usize,
    used: Instant,
}

#[derive(Default)]
pub struct Cache {
    entries: BTreeMap<String, Entry>,
}

impl Cache {
    pub fn get(&mut self, key: &str) -> Option<Entry> {
        let entry = self.entries.get_mut(key)?;
        entry.used = Instant::now();
        Some(entry.clone())
    }
    pub fn remove(&mut self, key: &str) {
        self.entries.remove(key);
    }
    pub fn insert(
        &mut self,
        key: String,
        context: i64,
        high: i64,
        revision: i64,
        snapshot: Snapshot,
    ) {
        let Ok(bytes) = serde_json::to_vec(&snapshot).map(|v| v.len()) else {
            return;
        };
        if bytes > MAX_BYTES {
            return;
        }
        self.entries.remove(&key);
        while self.entries.len() >= MAX_ENTRIES
            || self
                .entries
                .values()
                .map(|entry| entry.bytes)
                .sum::<usize>()
                + bytes
                > MAX_BYTES
        {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.used)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            self.entries.remove(&oldest);
        }
        self.entries.insert(
            key,
            Entry {
                context,
                high,
                revision,
                snapshot,
                bytes,
                used: Instant::now(),
            },
        );
    }
}

pub fn refresh(
    snapshot: &mut Snapshot,
    clocks: &Value,
    checkpoint_owner: Option<&str>,
    delta: bool,
) -> Result<bool> {
    let clocks: Vec<Vec<Value>> = decoded(clocks)?;
    let sessions = snapshot
        .tables
        .get_mut("coord_sessions")
        .ok_or("missing sessions")?;
    if !delta && clocks.len() != sessions.len() {
        return Ok(false);
    }
    for clock in &clocks {
        let Some(row) = sessions.iter_mut().find(|row| clock.first() == row.first()) else {
            return Ok(false);
        };
        if clock.len() != 4 {
            return Err("invalid session clock projection".into());
        }
        if row[0].as_str() == checkpoint_owner && row[2] != clock[2] {
            return Ok(false);
        }
        let mut session: Session = decoded(&row[1])?;
        session.heartbeat_at = clock[1].as_i64().ok_or("invalid heartbeat clock")?;
        row[1] = Value::String(
            serde_json::to_string(&session).map_err(|_| "cannot refresh session clock")?,
        );
        row[2] = clock[2].clone();
        row[3] = clock[3].clone();
    }
    Ok(true)
}

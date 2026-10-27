//! Durable resource reservations. No CI jobs are dispatched by this module.
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path, time::Duration};
use thiserror::Error;

type Budgets = BTreeMap<String, u32>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Host {
    /// Maximum number of simultaneous jobs on this physical host.
    pub slots: u32,
    /// Maximum pool units simultaneously consumed on this host.
    pub pools: Budgets,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Capacity {
    pub pools: Budgets,
    /// Keys identify physical machines, not runner labels.
    pub hosts: BTreeMap<String, Host>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Request {
    pub pools: Budgets,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Reservation {
    pub id: String,
    pub host: String,
    pub request: Request,
}
#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("capacity configuration or request is invalid")]
    Invalid,
    #[error("a different capacity configuration is active while reservations exist")]
    ConfigBusy,
    #[error("capacity configuration changed; reopen the scheduler")]
    ConfigChanged,
    #[error("reservation identifier is already used by another request")]
    IdConflict,
    #[error("reservation identifier has already been released and cannot be reused")]
    IdFinished,
    #[error("stored reservation refers to an unknown host or pool")]
    Corrupt,
}

pub struct Scheduler {
    conn: Connection,
    capacity: Capacity,
    encoded: String,
}
impl Scheduler {
    pub fn open(path: impl AsRef<Path>, capacity: Capacity) -> Result<Self, SchedulerError> {
        validate_capacity(&capacity)?;
        let encoded = serde_json::to_string(&capacity)?;
        let mut conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch("CREATE TABLE IF NOT EXISTS capacity_config (id INTEGER PRIMARY KEY CHECK(id = 1), body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS capacity_reservations (id TEXT PRIMARY KEY, host TEXT NOT NULL, request TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS capacity_finished (id TEXT PRIMARY KEY NOT NULL);")?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old: Option<String> = tx
            .query_row("SELECT body FROM capacity_config WHERE id=1", [], |r| {
                r.get(0)
            })
            .optional()?;
        if old.as_ref().is_some_and(|o| o != &encoded) {
            let count: i64 =
                tx.query_row("SELECT COUNT(*) FROM capacity_reservations", [], |r| {
                    r.get(0)
                })?;
            if count != 0 {
                return Err(SchedulerError::ConfigBusy);
            }
        }
        tx.execute("INSERT INTO capacity_config(id, body) VALUES(1, ?1) ON CONFLICT(id) DO UPDATE SET body=excluded.body", [&encoded])?;
        tx.commit()?;
        Ok(Self {
            conn,
            capacity,
            encoded,
        })
    }

    /// Reserves one host and the requested global and host-local budgets in a
    /// single SQLite write transaction. Retries with the same id are idempotent.
    /// None means capacity is temporarily unavailable, never authorization to run.
    pub fn reserve(
        &mut self,
        id: &str,
        request: &Request,
    ) -> Result<Option<Reservation>, SchedulerError> {
        if id.is_empty()
            || request.pools.is_empty()
            || request
                .pools
                .iter()
                .any(|(k, v)| k.is_empty() || *v == 0 || !self.capacity.pools.contains_key(k))
        {
            return Err(SchedulerError::Invalid);
        }
        if request
            .pools
            .iter()
            .any(|(name, amount)| *amount > self.capacity.pools[name])
            || !self.capacity.hosts.values().any(|host| {
                request
                    .pools
                    .iter()
                    .all(|(name, amount)| host.pools.get(name).is_some_and(|limit| amount <= limit))
            })
        {
            return Err(SchedulerError::Invalid);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active_config: String =
            tx.query_row("SELECT body FROM capacity_config WHERE id=1", [], |r| {
                r.get(0)
            })?;
        if active_config != self.encoded {
            return Err(SchedulerError::ConfigChanged);
        }
        let finished: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM capacity_finished WHERE id=?1)",
            [id],
            |r| r.get(0),
        )?;
        if finished {
            return Err(SchedulerError::IdFinished);
        }
        let existing: Option<(String, String)> = tx
            .query_row(
                "SELECT host, request FROM capacity_reservations WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((host, encoded)) = existing {
            let old: Request = serde_json::from_str(&encoded)?;
            if old != *request {
                return Err(SchedulerError::IdConflict);
            }
            return Ok(Some(Reservation {
                id: id.into(),
                host,
                request: old,
            }));
        }
        let reservations: Vec<(String, String)> = {
            let mut stmt = tx.prepare("SELECT host, request FROM capacity_reservations")?;
            let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<Result<_, _>>()?
        };
        let mut global = BTreeMap::<String, u64>::new();
        let mut per_host = BTreeMap::<String, BTreeMap<String, u64>>::new();
        let mut slots = BTreeMap::<String, u64>::new();
        for (host, encoded) in reservations {
            if !self.capacity.hosts.contains_key(&host) {
                return Err(SchedulerError::Corrupt);
            }
            *slots.entry(host.clone()).or_default() += 1;
            let held: Request = serde_json::from_str(&encoded)?;
            for (name, amount) in held.pools {
                if !self.capacity.pools.contains_key(&name)
                    || !self.capacity.hosts[&host].pools.contains_key(&name)
                    || amount == 0
                {
                    return Err(SchedulerError::Corrupt);
                }
                *global.entry(name.clone()).or_default() += u64::from(amount);
                *per_host
                    .entry(host.clone())
                    .or_default()
                    .entry(name)
                    .or_default() += u64::from(amount);
            }
        }
        if request.pools.iter().any(|(name, amount)| {
            global.get(name).copied().unwrap_or(0) + u64::from(*amount)
                > u64::from(self.capacity.pools[name])
        }) {
            return Ok(None);
        }
        let selected = self
            .capacity
            .hosts
            .iter()
            .find(|(name, host)| {
                slots.get(*name).copied().unwrap_or(0) < u64::from(host.slots)
                    && request.pools.iter().all(|(pool, amount)| {
                        host.pools.get(pool).is_some_and(|limit| {
                            per_host
                                .get(*name)
                                .and_then(|p| p.get(pool))
                                .copied()
                                .unwrap_or(0)
                                + u64::from(*amount)
                                <= u64::from(*limit)
                        })
                    })
            })
            .map(|(name, _)| name.clone());
        let Some(host) = selected else {
            return Ok(None);
        };
        tx.execute(
            "INSERT INTO capacity_reservations(id, host, request) VALUES(?1, ?2, ?3)",
            params![id, host, serde_json::to_string(request)?],
        )?;
        tx.commit()?;
        Ok(Some(Reservation {
            id: id.into(),
            host,
            request: request.clone(),
        }))
    }
    /// Release only after the provider has confirmed that the job is no longer running.
    /// Reservations intentionally do not expire on process restart.
    pub fn release(&mut self, id: &str) -> Result<bool, SchedulerError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let deleted = tx.execute("DELETE FROM capacity_reservations WHERE id=?1", [id])? != 0;
        if deleted {
            tx.execute("INSERT INTO capacity_finished(id) VALUES(?1)", [id])?;
        }
        tx.commit()?;
        Ok(deleted)
    }
}
fn validate_capacity(capacity: &Capacity) -> Result<(), SchedulerError> {
    if capacity.pools.is_empty()
        || capacity.hosts.is_empty()
        || capacity.pools.iter().any(|(k, v)| k.is_empty() || *v == 0)
    {
        return Err(SchedulerError::Invalid);
    }
    for (name, host) in &capacity.hosts {
        if name.is_empty()
            || host.slots == 0
            || host.pools.is_empty()
            || host
                .pools
                .iter()
                .any(|(k, v)| *v == 0 || !capacity.pools.contains_key(k))
        {
            return Err(SchedulerError::Invalid);
        }
    }
    Ok(())
}

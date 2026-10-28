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
    #[error("dispatch is managed by its durable lifecycle")]
    DispatchManaged,
    #[error("dispatch transition or run identity is invalid")]
    DispatchTransition,
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
            CREATE TABLE IF NOT EXISTS capacity_finished (id TEXT PRIMARY KEY NOT NULL);
            CREATE TABLE IF NOT EXISTS dispatch_intents (id TEXT PRIMARY KEY NOT NULL, plan TEXT NOT NULL, state TEXT NOT NULL, run_id TEXT);")?;
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
        self.reserve_inner(id, request, None)
    }
    /// Atomically record the frozen dispatch intent and reserve capacity.
    /// No provider request is made here.
    pub fn reserve_dispatch(
        &mut self,
        id: &str,
        request: &Request,
        plan: &DispatchPlan,
    ) -> Result<Option<Reservation>, SchedulerError> {
        if !plan.valid() {
            return Err(SchedulerError::Invalid);
        }
        self.reserve_inner(id, request, Some(plan))
    }
    fn reserve_inner(
        &mut self,
        id: &str,
        request: &Request,
        plan: Option<&DispatchPlan>,
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
            let stored_plan: Option<String> = tx
                .query_row("SELECT plan FROM dispatch_intents WHERE id=?1", [id], |r| {
                    r.get(0)
                })
                .optional()?;
            match (stored_plan, plan) {
                (None, None) => {}
                (Some(stored), Some(plan))
                    if serde_json::from_str::<DispatchPlan>(&stored)? == *plan => {}
                _ => return Err(SchedulerError::IdConflict),
            }
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
        if let Some(plan) = plan {
            tx.execute(
                "INSERT INTO dispatch_intents(id,plan,state) VALUES(?1,?2,'prepared')",
                params![id, serde_json::to_string(plan)?],
            )?;
        }
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
        let managed: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM dispatch_intents WHERE id=?1)",
            [id],
            |r| r.get(0),
        )?;
        if managed {
            return Err(SchedulerError::DispatchManaged);
        }
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

/// Frozen dispatch identity. Provider authentication, runner binding and policy
/// trust remain the responsibility of the eventual dispatcher.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DispatchPlan {
    pub installation_id: u64,
    pub repository_id: u64,
    pub workflow_id: u64,
    pub integration_sha: String,
    pub staging_ref: String,
    pub policy_digest: String,
}
impl DispatchPlan {
    fn valid(&self) -> bool {
        fn hex(value: &str, len: usize) -> bool {
            value.len() == len
                && value
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        }
        let id = self.staging_ref.strip_prefix("refs/heads/sluss/");
        self.installation_id != 0
            && self.repository_id != 0
            && self.workflow_id != 0
            && hex(&self.integration_sha, 40)
            && hex(&self.policy_digest, 64)
            && id.is_some_and(|id| {
                (16..=64).contains(&id.len())
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            })
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchState {
    Prepared,
    /// The send may have happened, even if the process crashed before the HTTP request.
    Sending,
    Observed {
        run_id: u64,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDispatch {
    pub reservation: Reservation,
    pub plan: DispatchPlan,
    pub state: DispatchState,
}
impl Scheduler {
    /// Only the worker that changes Prepared to Sending may issue the request.
    /// Sending is deliberately not retried after a crash or ambiguous response.
    pub fn claim_dispatch(&mut self, id: &str) -> Result<bool, SchedulerError> {
        Ok(self.conn.execute(
            "UPDATE dispatch_intents SET state='sending' WHERE id=?1 AND state='prepared'",
            [id],
        )? == 1)
    }
    /// Bind a provider-observed run. The caller must authenticate and verify
    /// the run identity against the frozen plan before recording it.
    pub fn observe_dispatch(&mut self, id: &str, run_id: u64) -> Result<(), SchedulerError> {
        if run_id == 0 {
            return Err(SchedulerError::Invalid);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: Option<(String, Option<String>)> = tx
            .query_row(
                "SELECT state,run_id FROM dispatch_intents WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let run = run_id.to_string();
        match row {
            Some((state, None)) if state == "sending" => {
                tx.execute(
                    "UPDATE dispatch_intents SET state='observed',run_id=?2 WHERE id=?1",
                    params![id, run],
                )?;
            }
            Some((state, Some(old)))
                if (state == "observed" || state == "finished") && old == run => {}
            _ => return Err(SchedulerError::DispatchTransition),
        }
        tx.commit()?;
        Ok(())
    }
    /// The caller must first confirm with the provider that this exact run can
    /// no longer consume capacity. Capacity and terminal state change atomically.
    pub fn finish_dispatch(&mut self, id: &str, run_id: u64) -> Result<(), SchedulerError> {
        if run_id == 0 {
            return Err(SchedulerError::Invalid);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: Option<(String, Option<String>)> = tx
            .query_row(
                "SELECT state,run_id FROM dispatch_intents WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let run = run_id.to_string();
        match row {
            Some((state, Some(old))) if state == "finished" && old == run => return Ok(()),
            Some((state, Some(old))) if state == "observed" && old == run => {}
            _ => return Err(SchedulerError::DispatchTransition),
        }
        if tx.execute("DELETE FROM capacity_reservations WHERE id=?1", [id])? != 1 {
            return Err(SchedulerError::Corrupt);
        }
        tx.execute("INSERT INTO capacity_finished(id) VALUES(?1)", [id])?;
        tx.execute(
            "UPDATE dispatch_intents SET state='finished' WHERE id=?1",
            [id],
        )?;
        tx.commit()?;
        Ok(())
    }
    /// Read durable, nonterminal intents for startup recovery. Sending intents
    /// need provider reconciliation, never blind redispatch.
    pub fn pending_dispatches(&self) -> Result<Vec<PendingDispatch>, SchedulerError> {
        let mut stmt = self.conn.prepare("SELECT d.id,d.plan,d.state,d.run_id,r.host,r.request FROM dispatch_intents d LEFT JOIN capacity_reservations r ON r.id=d.id WHERE d.state != 'finished' ORDER BY d.id")?;
        let mut rows = stmt.query([])?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let plan: DispatchPlan = serde_json::from_str(&row.get::<_, String>(1)?)?;
            let state: String = row.get(2)?;
            let run: Option<String> = row.get(3)?;
            let host: Option<String> = row.get(4)?;
            let request: Option<String> = row.get(5)?;
            let state = match (state.as_str(), run) {
                ("prepared", None) => DispatchState::Prepared,
                ("sending", None) => DispatchState::Sending,
                ("observed", Some(run)) => DispatchState::Observed {
                    run_id: run
                        .parse::<u64>()
                        .ok()
                        .filter(|n| *n != 0)
                        .ok_or(SchedulerError::Corrupt)?,
                },
                _ => return Err(SchedulerError::Corrupt),
            };
            if !plan.valid() {
                return Err(SchedulerError::Corrupt);
            }
            result.push(PendingDispatch {
                reservation: Reservation {
                    id,
                    host: host.ok_or(SchedulerError::Corrupt)?,
                    request: serde_json::from_str(&request.ok_or(SchedulerError::Corrupt)?)?,
                },
                plan,
                state,
            });
        }
        Ok(result)
    }
}

//! Durable resource reservations. No CI jobs are dispatched by this module.
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path, time::Duration};
use thiserror::Error;

type Budgets = BTreeMap<String, u32>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Host {
    /// Maximum number of simultaneous jobs on this physical host.
    pub slots: u32,
    /// GitHub runner IDs registered on this physical host, set by the operator.
    #[serde(default)]
    pub runner_ids: std::collections::BTreeSet<u64>,
    /// Maximum pool units simultaneously consumed on this host.
    pub pools: Budgets,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    #[error(transparent)]
    Metrics(#[from] crate::metrics::MetricsError),
    #[error(transparent)]
    Store(#[from] crate::store::StoreError),
    #[error(transparent)]
    CiObservation(#[from] crate::verification::CiObservationError),
    #[error("provider CI observations disagree for the same run")]
    CiConflict,
    #[error("capacity configuration or request is invalid")]
    Invalid,
    #[error(
        "capacity configuration cannot change while reservations exist or the release topology is frozen"
    )]
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
    #[error("batch, queue, or repository changed before dispatch")]
    StaleBatch,
    #[error("dispatch is not attached to an invalidated batch with a known run")]
    NotStale,
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
            CREATE TABLE IF NOT EXISTS capacity_release_topology (id INTEGER PRIMARY KEY CHECK(id = 1), body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS capacity_reservations (id TEXT PRIMARY KEY, host TEXT NOT NULL, request TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS capacity_finished (id TEXT PRIMARY KEY NOT NULL);
            CREATE TABLE IF NOT EXISTS dispatch_intents (id TEXT PRIMARY KEY NOT NULL, plan TEXT NOT NULL, state TEXT NOT NULL, run_id TEXT);
            CREATE TABLE IF NOT EXISTS dispatch_batches (id TEXT PRIMARY KEY NOT NULL, batch_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS ci_observations (id TEXT PRIMARY KEY NOT NULL, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS dispatch_receipt_sources (id TEXT PRIMARY KEY NOT NULL, source TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS stale_cancellations (id TEXT PRIMARY KEY NOT NULL, run_id TEXT NOT NULL, state TEXT NOT NULL);")?;
        crate::metrics::ensure_schema(&conn)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old: Option<String> = tx
            .query_row("SELECT body FROM capacity_config WHERE id=1", [], |r| {
                r.get(0)
            })
            .optional()?;
        let release_topology: Option<String> = tx
            .query_row(
                "SELECT body FROM capacity_release_topology WHERE id=1",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if release_topology
            .as_ref()
            .is_some_and(|locked| locked != &encoded)
        {
            return Err(SchedulerError::ConfigBusy);
        }
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
        self.reserve_inner(id, request, None, None)
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
        self.reserve_inner(id, request, Some(plan), None)
    }
    /// Reserve a dispatch bound to a frozen batch in the same database.
    /// Provider policy, staging-ref protection and runner placement still need
    /// independent verification before executing this intent.
    pub fn reserve_batch_dispatch(
        &mut self,
        batch_id: &str,
        request: &Request,
        plan: &DispatchPlan,
    ) -> Result<Option<Reservation>, SchedulerError> {
        if !plan.valid() || plan.staging_ref != format!("refs/heads/sluss/{batch_id}") {
            return Err(SchedulerError::Invalid);
        }
        self.reserve_inner(batch_id, request, Some(plan), Some(batch_id))
    }
    fn reserve_inner(
        &mut self,
        id: &str,
        request: &Request,
        plan: Option<&DispatchPlan>,
        batch_id: Option<&str>,
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
        if let (Some(batch_id), Some(plan)) = (batch_id, plan) {
            validate_batch_dispatch(&tx, batch_id, plan)?;
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
            let stored_batch: Option<String> = tx
                .query_row(
                    "SELECT batch_id FROM dispatch_batches WHERE id=?1",
                    [id],
                    |r| r.get(0),
                )
                .optional()?;
            if stored_batch.as_deref() != batch_id {
                return Err(SchedulerError::IdConflict);
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
        if let Some(batch_id) = batch_id {
            tx.execute(
                "INSERT INTO dispatch_batches(id,batch_id) VALUES(?1,?2)",
                params![id, batch_id],
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
    let mut runner_ids = std::collections::BTreeSet::new();
    for (name, host) in &capacity.hosts {
        if host
            .runner_ids
            .iter()
            .any(|id| *id == 0 || !runner_ids.insert(*id))
        {
            return Err(SchedulerError::Invalid);
        }
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
    /// Exact required GitHub job names, frozen before dispatch.
    #[serde(default)]
    pub required_jobs: std::collections::BTreeSet<String>,
    /// Explicit workflow suites and optional operator-owned selection policy.
    #[serde(default)]
    pub suites: std::collections::BTreeSet<String>,
    #[serde(default)]
    pub selection_policy_digest: Option<String>,
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
            && !self.suites.is_empty()
            && self.suites.len() <= 100
            && self.suites.iter().all(|name| {
                !name.is_empty()
                    && name.len() <= 100
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            })
            && match &self.selection_policy_digest {
                Some(digest) => hex(digest, 64),
                None => self.suites.len() == 1 && self.suites.contains("full"),
            }
            && !self.required_jobs.is_empty()
            && self.required_jobs.len() <= 100
            && self.required_jobs.iter().all(|name| {
                !name.is_empty() && name.len() <= 256 && !name.chars().any(char::is_control)
            })
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
    Accepted {
        run_id: u64,
    },
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
    /// Verify that a provider target check uses the frozen batch base and the
    /// branch from the latest bound reconciliation. Generic dispatches may
    /// only use the explicitly unbound API.
    pub fn validate_dispatch_target(
        &mut self,
        id: &str,
        target_base: Option<(&str, &str)>,
    ) -> Result<(), SchedulerError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: Option<(String, String)> = tx.query_row(
            "SELECT b.batch_id,d.plan FROM dispatch_batches b JOIN dispatch_intents d ON d.id=b.id WHERE b.id=?1",
            [id], |r| Ok((r.get(0)?, r.get(1)?)),
        ).optional()?;
        match (row, target_base) {
            (None, None) => {}
            (Some((batch_id, body)), Some((target, base))) => {
                let plan: DispatchPlan = serde_json::from_str(&body)?;
                validate_batch_dispatch(&tx, &batch_id, &plan)?;
                let batch_body: String =
                    tx.query_row("SELECT body FROM batches WHERE id=?1", [&batch_id], |r| {
                        r.get(0)
                    })?;
                let batch: crate::batch::Batch = serde_json::from_str(&batch_body)?;
                let current: Option<(String, String)> = tx.query_row(
                    "SELECT observed_target.branch,observed_base.sha FROM observed_target JOIN observed_base ON observed_target.id=observed_base.id WHERE observed_target.id=1",
                    [], |r| Ok((r.get(0)?, r.get(1)?)),
                ).optional()?;
                if current
                    .as_ref()
                    .is_none_or(|(name, sha)| name != target || sha != base)
                    || batch.base_sha != base
                {
                    return Err(SchedulerError::StaleBatch);
                }
            }
            _ => return Err(SchedulerError::StaleBatch),
        }
        tx.commit()?;
        Ok(())
    }

    /// Only the worker that changes Prepared to Sending may issue the request.
    /// Sending is deliberately not retried after a crash or ambiguous response.
    pub fn claim_dispatch(&mut self, id: &str) -> Result<bool, SchedulerError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let intent: Option<(String, String)> = tx
            .query_row(
                "SELECT state,plan FROM dispatch_intents WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let Some((state, plan)) = intent else {
            return Ok(false);
        };
        if state != "prepared" {
            return Ok(false);
        }
        let batch_id: Option<String> = tx
            .query_row(
                "SELECT batch_id FROM dispatch_batches WHERE id=?1",
                [id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(batch_id) = batch_id {
            validate_batch_dispatch(&tx, &batch_id, &serde_json::from_str(&plan)?)?;
        }
        let claimed = tx.execute(
            "UPDATE dispatch_intents SET state='sending' WHERE id=?1 AND state='prepared'",
            [id],
        )? == 1;
        tx.commit()?;
        Ok(claimed)
    }
    /// Cancel only before a provider request is claimed; this atomically frees
    /// the reservation and permanently retires the identifier.
    pub fn cancel_prepared(&mut self, id: &str) -> Result<bool, SchedulerError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "UPDATE dispatch_intents SET state='cancelled' WHERE id=?1 AND state='prepared'",
            [id],
        )?;
        if changed == 0 {
            return Ok(false);
        }
        if tx.execute("DELETE FROM capacity_reservations WHERE id=?1", [id])? != 1 {
            return Err(SchedulerError::Corrupt);
        }
        tx.execute("INSERT INTO capacity_finished(id) VALUES(?1)", [id])?;
        tx.commit()?;
        Ok(true)
    }
    /// Save the direct provider receipt before validating the run details.
    pub fn record_receipt(&mut self, id: &str, run_id: u64) -> Result<(), SchedulerError> {
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
                    "UPDATE dispatch_intents SET state='accepted',run_id=?2 WHERE id=?1",
                    params![id, run],
                )?;
            }
            Some((state, Some(old)))
                if (state == "accepted" || state == "observed" || state == "finished")
                    && old == run => {}
            _ => return Err(SchedulerError::DispatchTransition),
        }
        tx.execute("INSERT INTO dispatch_receipt_sources(id,source) VALUES(?1,'direct') ON CONFLICT(id) DO UPDATE SET source='direct'", [id])?;
        tx.commit()?;
        Ok(())
    }
    /// The source describes how the run ID was learned, not trusted CI evidence.
    pub fn receipt_source(&self, id: &str) -> Result<Option<String>, SchedulerError> {
        Ok(self
            .conn
            .query_row(
                "SELECT source FROM dispatch_receipt_sources WHERE id=?1",
                [id],
                |r| r.get(0),
            )
            .optional()?)
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
                tx.execute(
                    "INSERT INTO dispatch_receipt_sources(id,source) VALUES(?1,'recovered')",
                    [id],
                )?;
            }
            Some((state, Some(old))) if state == "accepted" && old == run => {
                tx.execute(
                    "UPDATE dispatch_intents SET state='observed' WHERE id=?1",
                    [id],
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
        let mut stmt = self.conn.prepare("SELECT d.id,d.plan,d.state,d.run_id,r.host,r.request FROM dispatch_intents d LEFT JOIN capacity_reservations r ON r.id=d.id WHERE d.state NOT IN ('finished','cancelled') ORDER BY d.id")?;
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
                ("accepted", Some(run)) => DispatchState::Accepted {
                    run_id: run
                        .parse::<u64>()
                        .ok()
                        .filter(|n| *n != 0)
                        .ok_or(SchedulerError::Corrupt)?,
                },
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

fn validate_batch_dispatch(
    tx: &rusqlite::Transaction<'_>,
    batch_id: &str,
    plan: &DispatchPlan,
) -> Result<(), SchedulerError> {
    use crate::batch::{Batch, State};
    let binding: Option<(i64, i64)> = tx
        .query_row(
            "SELECT installation,repository FROM repository_binding WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if binding
        != Some((
            i64::try_from(plan.installation_id).map_err(|_| SchedulerError::StaleBatch)?,
            i64::try_from(plan.repository_id).map_err(|_| SchedulerError::StaleBatch)?,
        ))
    {
        return Err(SchedulerError::StaleBatch);
    }
    let paused: bool = tx.query_row(
        "SELECT value != 0 FROM settings WHERE key='paused'",
        [],
        |r| r.get(0),
    )?;
    let pending: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM reconciliations WHERE installation=?1 AND repository=?2)",
        params![plan.installation_id as i64, plan.repository_id as i64],
        |r| r.get(0),
    )?;
    let body: Option<String> = tx
        .query_row("SELECT body FROM batches WHERE id=?1", [batch_id], |r| {
            r.get(0)
        })
        .optional()?;
    let batch: Batch = serde_json::from_str(&body.ok_or(SchedulerError::StaleBatch)?)?;
    if paused
        || pending
        || batch.id != batch_id
        || batch.state
            != (State::Testing {
                integration_sha: plan.integration_sha.clone(),
            })
    {
        return Err(SchedulerError::StaleBatch);
    }
    let observed_base: Option<String> = tx
        .query_row("SELECT sha FROM observed_base WHERE id=1", [], |r| r.get(0))
        .optional()?;
    if observed_base
        .as_deref()
        .is_some_and(|sha| sha != batch.base_sha)
    {
        return Err(SchedulerError::StaleBatch);
    }
    let policy: Option<String> = tx
        .query_row(
            "SELECT digest FROM batch_policy WHERE batch_id=?1",
            [batch_id],
            |r| r.get(0),
        )
        .optional()?;
    if policy.as_deref() != Some(&plan.policy_digest) {
        return Err(SchedulerError::StaleBatch);
    }
    if let Some(digest) = &plan.selection_policy_digest {
        let body: Option<String> = tx
            .query_row(
                "SELECT body FROM batch_selection WHERE batch_id=?1",
                [batch_id],
                |r| r.get(0),
            )
            .optional()?;
        let selection: crate::selective::FrozenSelection =
            serde_json::from_str(&body.ok_or(SchedulerError::StaleBatch)?)?;
        if &selection.policy_digest != digest
            || selection.base_sha != batch.base_sha
            || selection.integration_sha != plan.integration_sha
            || selection.selection.suites() != &plan.suites
        {
            return Err(SchedulerError::StaleBatch);
        }
    }
    for candidate in batch.candidates {
        let number = i64::try_from(candidate.number).map_err(|_| SchedulerError::StaleBatch)?;
        let head: Option<String> = tx
            .query_row(
                "SELECT head_sha FROM candidates WHERE number=?1",
                [number],
                |r| r.get(0),
            )
            .optional()?;
        if head.as_deref() != Some(&candidate.head_sha) {
            return Err(SchedulerError::StaleBatch);
        }
        let current: Option<String> = tx
            .query_row(
                "SELECT digest FROM candidate_evidence WHERE number=?1",
                [number],
                |r| r.get(0),
            )
            .optional()?;
        let frozen: Option<String> = tx
            .query_row(
                "SELECT digest FROM batch_evidence WHERE batch_id=?1 AND number=?2",
                params![batch_id, number],
                |r| r.get(0),
            )
            .optional()?;
        if current.is_none() || current != frozen {
            return Err(SchedulerError::StaleBatch);
        }
    }
    Ok(())
}

impl Scheduler {
    /// Record completed, host-checked job timings from a provider observation.
    /// This is accounting data only, not CI attestation or permission to land.
    pub fn record_job_metrics(
        &mut self,
        id: &str,
        snapshot: &crate::github::ActionsSnapshot,
    ) -> Result<usize, SchedulerError> {
        use crate::{
            metrics::{JobMeasurement, MetricsError},
            verification::{Placement, check_host_placement},
        };
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let config: String =
            tx.query_row("SELECT body FROM capacity_config WHERE id=1", [], |r| {
                r.get(0)
            })?;
        if config != self.encoded {
            return Err(SchedulerError::ConfigChanged);
        }
        let row: Option<(String,String,Option<String>,String)> = tx.query_row(
            "SELECT d.plan,d.state,d.run_id,r.host FROM dispatch_intents d JOIN capacity_reservations r ON r.id=d.id WHERE d.id=?1", [id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))
        ).optional()?;
        let (body, state, run_id, host) = row.ok_or(SchedulerError::DispatchTransition)?;
        let plan: DispatchPlan = serde_json::from_str(&body)?;
        let run = &snapshot.run;
        let branch = plan
            .staging_ref
            .strip_prefix("refs/heads/")
            .ok_or(SchedulerError::Invalid)?;
        if state != "observed"
            || run_id.as_deref() != Some(&run.id.to_string())
            || run.id == 0
            || run.run_attempt != 1
            || run.workflow_id != plan.workflow_id
            || run.repository.id != plan.repository_id
            || run.head_repository.as_ref().map(|r| r.id) != Some(plan.repository_id)
            || run.head_sha != plan.integration_sha
            || run.head_branch.as_deref() != Some(branch)
            || run.event != "workflow_dispatch"
        {
            return Err(SchedulerError::DispatchTransition);
        }
        let host_config = self
            .capacity
            .hosts
            .get(&host)
            .ok_or(SchedulerError::Corrupt)?;
        let placement = check_host_placement(snapshot, &host_config.runner_ids)
            .map_err(|_| MetricsError::InvalidJob)?;
        let runner_id = match placement {
            Placement::Waiting => return Ok(0),
            Placement::Confirmed { runner_id } => runner_id,
        };
        let mut inserted = 0;
        for job in &snapshot.jobs {
            if job.status != "completed" || job.conclusion.as_deref() == Some("skipped") {
                continue;
            }
            if job.id == 0
                || job.run_id != run.id
                || job.head_sha != run.head_sha
                || job.name.is_empty()
                || job.runner_id != Some(runner_id)
            {
                return Err(MetricsError::InvalidJob.into());
            }
            let start = crate::metrics::timestamp_ms(
                job.started_at.as_deref().ok_or(MetricsError::InvalidJob)?,
            )?;
            let end = crate::metrics::timestamp_ms(
                job.completed_at
                    .as_deref()
                    .ok_or(MetricsError::InvalidJob)?,
            )?;
            if end < start {
                return Err(MetricsError::InvalidJob.into());
            }
            let conclusion = job
                .conclusion
                .as_ref()
                .filter(|s| !s.is_empty())
                .ok_or(MetricsError::InvalidJob)?;
            let measurement = JobMeasurement {
                repository_id: plan.repository_id,
                job_id: job.id,
                run_id: run.id,
                attempt: run.run_attempt,
                batch_id: id.into(),
                head_sha: run.head_sha.clone(),
                name: job.name.clone(),
                host: host.clone(),
                runner_id,
                started_ms: start,
                completed_ms: end,
                conclusion: conclusion.clone(),
            };
            inserted += usize::from(crate::metrics::insert(&tx, &measurement)?);
        }
        tx.commit()?;
        Ok(inserted)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredCiObservation {
    run_id: u64,
    plan: DispatchPlan,
    observation: crate::verification::CiObservation,
}
impl Scheduler {
    /// Persist a provider result comparison for a still-current batch. This is
    /// deliberately not an attestation and does not authorize landing.
    pub fn record_ci_observation(
        &mut self,
        id: &str,
        snapshot: &crate::github::ActionsSnapshot,
    ) -> Result<crate::verification::CiObservation, SchedulerError> {
        use crate::verification::{
            CiObservation, CiObservationError, Placement, check_host_placement, observe_ci,
        };
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active: String =
            tx.query_row("SELECT body FROM capacity_config WHERE id=1", [], |r| {
                r.get(0)
            })?;
        if active != self.encoded {
            return Err(SchedulerError::ConfigChanged);
        }
        let row: Option<(String,String,Option<String>,String,String)> = tx.query_row(
            "SELECT d.plan,d.state,d.run_id,r.host,b.batch_id FROM dispatch_intents d JOIN capacity_reservations r ON r.id=d.id JOIN dispatch_batches b ON b.id=d.id WHERE d.id=?1", [id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))
        ).optional()?;
        let (body, state, run_id, host, batch_id) =
            row.ok_or(SchedulerError::DispatchTransition)?;
        let plan: DispatchPlan = serde_json::from_str(&body)?;
        let run_id = run_id
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|id| *id != 0)
            .ok_or(SchedulerError::DispatchTransition)?;
        if state != "observed" || !plan.valid() {
            return Err(SchedulerError::DispatchTransition);
        }
        validate_batch_dispatch(&tx, &batch_id, &plan)?;
        let observation = observe_ci(&plan, run_id, snapshot)?;
        let host_config = self
            .capacity
            .hosts
            .get(&host)
            .ok_or(SchedulerError::Corrupt)?;
        let placement = check_host_placement(snapshot, &host_config.runner_ids)
            .map_err(|_| CiObservationError::Ambiguous)?;
        if matches!(observation, CiObservation::Pending) {
            return Ok(observation);
        }
        if !matches!(placement, Placement::Confirmed { .. }) {
            return Err(CiObservationError::Ambiguous.into());
        }
        let record = StoredCiObservation {
            run_id,
            plan,
            observation: observation.clone(),
        };
        let existing: Option<String> = tx
            .query_row("SELECT body FROM ci_observations WHERE id=?1", [id], |r| {
                r.get(0)
            })
            .optional()?;
        if let Some(body) = existing {
            let previous: StoredCiObservation = serde_json::from_str(&body)?;
            if previous != record {
                return Err(SchedulerError::CiConflict);
            }
        } else {
            tx.execute(
                "INSERT INTO ci_observations(id,body) VALUES(?1,?2)",
                params![id, serde_json::to_string(&record)?],
            )?;
        }
        tx.commit()?;
        Ok(observation)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleRun {
    pub plan: DispatchPlan,
    pub run_id: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancellationClaim {
    New(StaleRun),
    AlreadyRequested,
    Uncertain,
}
impl Scheduler {
    /// Retire unstarted reservations for invalidated batches in one transaction.
    pub fn cancel_invalidated_prepared(&mut self) -> Result<usize, SchedulerError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let rows: Vec<(String, String)> = {
            let mut stmt = tx.prepare("SELECT d.id,b.body FROM dispatch_intents d JOIN dispatch_batches x ON x.id=d.id JOIN batches b ON b.id=x.batch_id WHERE d.state='prepared' ORDER BY d.id")?;
            let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<Result<_, _>>()?
        };
        let mut cancelled = 0;
        for (id, body) in rows {
            let batch: crate::batch::Batch = serde_json::from_str(&body)?;
            if !matches!(batch.state, crate::batch::State::Invalidated { .. }) {
                continue;
            }
            if tx.execute(
                "UPDATE dispatch_intents SET state='cancelled' WHERE id=?1 AND state='prepared'",
                [&id],
            )? != 1
                || tx.execute("DELETE FROM capacity_reservations WHERE id=?1", [&id])? != 1
            {
                return Err(SchedulerError::Corrupt);
            }
            tx.execute("INSERT INTO capacity_finished(id) VALUES(?1)", [&id])?;
            cancelled += 1;
        }
        tx.commit()?;
        Ok(cancelled)
    }
    /// List only invalidated, batch-bound dispatches with a known provider run.
    pub fn stale_dispatches(&self) -> Result<Vec<String>, SchedulerError> {
        let mut stmt = self.conn.prepare("SELECT d.id,b.body FROM dispatch_intents d JOIN dispatch_batches x ON x.id=d.id JOIN batches b ON b.id=x.batch_id WHERE d.state IN ('accepted','observed') ORDER BY d.id")?;
        let mut rows = stmt.query([])?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let batch: crate::batch::Batch = serde_json::from_str(&row.get::<_, String>(1)?)?;
            if matches!(batch.state, crate::batch::State::Invalidated { .. }) {
                result.push(id);
            }
        }
        Ok(result)
    }
    /// Read a stale run identity before contacting GitHub. State is checked
    /// again atomically when the cancellation attempt is claimed.
    pub fn stale_run(&self, id: &str) -> Result<StaleRun, SchedulerError> {
        let row: Option<(String,String,Option<String>,String)> = self.conn.query_row("SELECT d.plan,d.state,d.run_id,b.body FROM dispatch_intents d JOIN dispatch_batches x ON x.id=d.id JOIN batches b ON b.id=x.batch_id WHERE d.id=?1", [id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        stale_run_from_row(row)
    }
    pub fn claim_stale_cancellation(
        &mut self,
        id: &str,
    ) -> Result<CancellationClaim, SchedulerError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: Option<(String,String,Option<String>,String)> = tx.query_row("SELECT d.plan,d.state,d.run_id,b.body FROM dispatch_intents d JOIN dispatch_batches x ON x.id=d.id JOIN batches b ON b.id=x.batch_id WHERE d.id=?1", [id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        let stale = stale_run_from_row(row)?;
        let prior: Option<(String, String)> = tx
            .query_row(
                "SELECT run_id,state FROM stale_cancellations WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let result = match prior {
            Some((run, state)) if run == stale.run_id.to_string() && state == "accepted" => {
                CancellationClaim::AlreadyRequested
            }
            Some((run, state)) if run == stale.run_id.to_string() && state == "sending" => {
                CancellationClaim::Uncertain
            }
            Some(_) => return Err(SchedulerError::Corrupt),
            None => {
                tx.execute(
                    "INSERT INTO stale_cancellations(id,run_id,state) VALUES(?1,?2,'sending')",
                    params![id, stale.run_id.to_string()],
                )?;
                CancellationClaim::New(stale)
            }
        };
        tx.commit()?;
        Ok(result)
    }
    pub fn record_stale_cancellation(
        &mut self,
        id: &str,
        run_id: u64,
    ) -> Result<(), SchedulerError> {
        let updated = self.conn.execute("UPDATE stale_cancellations SET state='accepted' WHERE id=?1 AND run_id=?2 AND state='sending'", params![id,run_id.to_string()])?;
        if updated != 1 {
            return Err(SchedulerError::DispatchTransition);
        }
        Ok(())
    }
}
fn stale_run_from_row(
    row: Option<(String, String, Option<String>, String)>,
) -> Result<StaleRun, SchedulerError> {
    let (body, state, run, batch) = row.ok_or(SchedulerError::NotStale)?;
    let plan: DispatchPlan = serde_json::from_str(&body)?;
    let batch: crate::batch::Batch = serde_json::from_str(&batch)?;
    let run_id = run
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|n| *n != 0)
        .ok_or(SchedulerError::NotStale)?;
    if !plan.valid()
        || !matches!(state.as_str(), "accepted" | "observed")
        || !matches!(batch.state, crate::batch::State::Invalidated { .. })
    {
        return Err(SchedulerError::NotStale);
    }
    Ok(StaleRun { plan, run_id })
}

impl Scheduler {
    /// Finalize the scheduling lifecycle of a directly received CI run without
    /// releasing its runner reservation or granting any merge authorization.
    pub fn complete_ci_only(&mut self, id: &str) -> Result<(), SchedulerError> {
        use crate::{
            batch::{Batch, State},
            verification::CiObservation,
        };
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active: String =
            tx.query_row("SELECT body FROM capacity_config WHERE id=1", [], |r| {
                r.get(0)
            })?;
        if active != self.encoded {
            return Err(SchedulerError::ConfigChanged);
        }
        type CompletionRow = (String, String, Option<String>, String, String, i64, String);
        let row: Option<CompletionRow> = tx.query_row(
            "SELECT d.plan,d.state,d.run_id,b.batch_id,q.body,q.version,c.body FROM dispatch_intents d JOIN dispatch_batches b ON b.id=d.id JOIN batches q ON q.id=b.batch_id JOIN ci_observations c ON c.id=d.id WHERE d.id=?1",
            [id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)),
        ).optional()?;
        let (plan_body, state, run, batch_id, batch_body, version, observation_body) =
            row.ok_or(SchedulerError::DispatchTransition)?;
        let plan: DispatchPlan = serde_json::from_str(&plan_body)?;
        let observation: StoredCiObservation = serde_json::from_str(&observation_body)?;
        let run_id = run
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .ok_or(SchedulerError::DispatchTransition)?;
        let source: Option<String> = tx
            .query_row(
                "SELECT source FROM dispatch_receipt_sources WHERE id=?1",
                [id],
                |r| r.get(0),
            )
            .optional()?;
        if state != "observed"
            || source.as_deref() != Some("direct")
            || observation.run_id != run_id
            || observation.plan != plan
        {
            return Err(SchedulerError::DispatchTransition);
        }
        let conclusion = match observation.observation {
            CiObservation::Successful => "success".to_owned(),
            CiObservation::Failed { conclusion } if !conclusion.is_empty() => conclusion,
            _ => return Err(SchedulerError::DispatchTransition),
        };
        let mut batch: Batch = serde_json::from_str(&batch_body)?;
        if batch.state
            == (State::CiComplete {
                integration_sha: plan.integration_sha.clone(),
                run_id,
                conclusion: conclusion.clone(),
            })
        {
            return Ok(());
        }
        validate_batch_dispatch(&tx, &batch_id, &plan)?;
        for candidate in &batch.candidates {
            let number = i64::try_from(candidate.number).map_err(|_| SchedulerError::StaleBatch)?;
            let digest: String = tx.query_row(
                "SELECT digest FROM batch_evidence WHERE batch_id=?1 AND number=?2",
                params![batch_id, number],
                |r| r.get(0),
            )?;
            tx.execute("INSERT INTO ci_candidate_completions(number,head_sha,digest,base_sha,batch_id,conclusion) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(number) DO UPDATE SET head_sha=excluded.head_sha,digest=excluded.digest,base_sha=excluded.base_sha,batch_id=excluded.batch_id,conclusion=excluded.conclusion", params![number,candidate.head_sha,digest,batch.base_sha,batch_id,conclusion])?;
        }
        batch.state = State::CiComplete {
            integration_sha: plan.integration_sha,
            run_id,
            conclusion,
        };
        if tx.execute(
            "UPDATE batches SET body=?1,version=version+1 WHERE id=?2 AND version=?3",
            params![serde_json::to_string(&batch)?, batch_id, version],
        )? != 1
        {
            return Err(SchedulerError::StaleBatch);
        }
        investigate_ci_batch(&tx, &batch)?;
        tx.commit()?;
        Ok(())
    }
}

/// Split failures as experiments. Only continue into a failing subgroup when
/// its sibling independently succeeded. Every result remains CI-only state.
fn investigate_ci_batch(
    tx: &rusqlite::Transaction<'_>,
    batch: &crate::batch::Batch,
) -> Result<(), SchedulerError> {
    use crate::batch::State;
    let enabled: bool = tx
        .query_row(
            "SELECT enabled FROM ci_bisection_config WHERE id=1",
            [],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(false);
    if !enabled {
        return Ok(());
    }
    let parent: Option<String> = tx
        .query_row(
            "SELECT parent_id FROM split_tasks WHERE child_id=?1 AND state='planned'",
            [&batch.id],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(parent) = parent {
        // Integration-conflict splits use the same task table. Only compare
        // siblings when a CI investigation actually owns their parent.
        let pending: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM ci_investigations WHERE batch_id=?1 AND state='pending')",
            [&parent],
            |r| r.get(0),
        )?;
        if !pending {
            return Ok(());
        }
        let children: Vec<(String, Option<String>)> = {
            let mut stmt = tx.prepare(
                "SELECT state,child_id FROM split_tasks WHERE parent_id=?1 ORDER BY side",
            )?;
            let rows = stmt.query_map([&parent], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<Result<_, _>>()?
        };
        if children.len() != 2
            || children
                .iter()
                .any(|(state, id)| state != "planned" || id.is_none())
        {
            return Ok(());
        }
        let mut observed = Vec::new();
        for (_, child) in children {
            let id = child.ok_or(SchedulerError::Corrupt)?;
            let (candidate, _) = match crate::store::checked_batch(tx, &id) {
                Ok(value) => value,
                Err(
                    crate::store::StoreError::StaleBatch | crate::store::StoreError::InvalidBatch,
                ) => {
                    tx.execute(
                        "UPDATE ci_investigations SET state='stale' WHERE batch_id=?1",
                        [&parent],
                    )?;
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            };
            match &candidate.state {
                State::CiComplete { conclusion, .. } => {
                    observed.push((candidate.clone(), conclusion.clone()))
                }
                _ => return Ok(()),
            }
        }
        if observed[0].0.base_sha != observed[1].0.base_sha {
            return Ok(());
        }
        let mut plans = Vec::new();
        for (child, _) in &observed {
            let body: String = tx.query_row(
                "SELECT plan FROM dispatch_intents WHERE id=?1",
                [&child.id],
                |r| r.get(0),
            )?;
            plans.push(serde_json::from_str::<DispatchPlan>(&body)?);
        }
        let comparable = plans[0].installation_id == plans[1].installation_id
            && plans[0].repository_id == plans[1].repository_id
            && plans[0].workflow_id == plans[1].workflow_id
            && plans[0].policy_digest == plans[1].policy_digest
            && plans[0].required_jobs == plans[1].required_jobs
            && plans[0].suites == plans[1].suites
            && plans[0].selection_policy_digest == plans[1].selection_policy_digest;
        if !comparable {
            crate::store::finish_ci_investigation(tx, &parent, "inconclusive")?;
            return Ok(());
        }
        let failure = match (observed[0].1.as_str(), observed[1].1.as_str()) {
            ("failure", "success") => Some(&observed[0].0),
            ("success", "failure") => Some(&observed[1].0),
            _ => None,
        };
        let state = match failure {
            Some(failed) if failed.candidates.len() == 1 => "needs-review",
            Some(_) => "investigating",
            None => "inconclusive",
        };
        if state == "investigating" {
            tx.execute("UPDATE ci_investigations SET state='investigating' WHERE batch_id=?1 AND state='pending'", [&parent])?;
        } else {
            crate::store::finish_ci_investigation(tx, &parent, state)?;
        }
        if let Some(failed) = failure.filter(|failed| failed.candidates.len() > 1) {
            crate::store::enqueue_bisection(tx, failed)?;
            tx.execute(
                "INSERT OR IGNORE INTO ci_investigations(batch_id,state) VALUES(?1,'pending')",
                [&failed.id],
            )?;
        }
    } else if matches!(&batch.state, State::CiComplete { conclusion, .. } if conclusion == "failure")
        && batch.candidates.len() > 1
    {
        crate::store::enqueue_bisection(tx, batch)?;
        tx.execute(
            "INSERT OR IGNORE INTO ci_investigations(batch_id,state) VALUES(?1,'pending')",
            [&batch.id],
        )?;
    }
    Ok(())
}

impl Scheduler {
    /// Free a completed CI-only reservation after provider revalidation. The
    /// caller must establish runner isolation and the release topology contract.
    pub fn release_completed_ci_only(
        &mut self,
        id: &str,
        run_id: u64,
    ) -> Result<(), SchedulerError> {
        use crate::batch::{Batch, State};
        crate::verification::check_release_topology(&self.capacity)
            .map_err(|_| SchedulerError::Invalid)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active: String =
            tx.query_row("SELECT body FROM capacity_config WHERE id=1", [], |r| {
                r.get(0)
            })?;
        if active != self.encoded {
            return Err(SchedulerError::ConfigChanged);
        }
        type ReleaseRow = (String, String, Option<String>, String, String, String);
        let row: Option<ReleaseRow> = tx.query_row(
            "SELECT d.plan,d.state,d.run_id,q.body,c.body,s.source FROM dispatch_intents d JOIN dispatch_batches b ON b.id=d.id JOIN batches q ON q.id=b.batch_id JOIN ci_observations c ON c.id=d.id JOIN dispatch_receipt_sources s ON s.id=d.id WHERE d.id=?1",
            [id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)),
        ).optional()?;
        let (plan_body, state, saved_run, batch_body, observation_body, source) =
            row.ok_or(SchedulerError::DispatchTransition)?;
        let plan: DispatchPlan = serde_json::from_str(&plan_body)?;
        let batch: Batch = serde_json::from_str(&batch_body)?;
        let observed: StoredCiObservation = serde_json::from_str(&observation_body)?;
        let conclusion = match &observed.observation {
            crate::verification::CiObservation::Successful => "success",
            crate::verification::CiObservation::Failed { conclusion } if !conclusion.is_empty() => {
                conclusion.as_str()
            }
            _ => return Err(SchedulerError::DispatchTransition),
        };
        if run_id == 0
            || saved_run.as_deref() != Some(&run_id.to_string())
            || source != "direct"
            || observed.run_id != run_id
            || observed.plan != plan
            || batch.state
                != (State::CiComplete {
                    integration_sha: plan.integration_sha,
                    run_id,
                    conclusion: conclusion.into(),
                })
        {
            return Err(SchedulerError::DispatchTransition);
        }
        if state == "finished" {
            return Ok(());
        }
        if state != "observed" {
            return Err(SchedulerError::DispatchTransition);
        }
        finish_bounded_reservation(&tx, id, &self.encoded)?;
        tx.commit()?;
        Ok(())
    }

    /// Free capacity for a directly received run whose batch was invalidated.
    /// The caller must first verify that the provider run has terminated.
    pub fn release_invalidated_ci_only(
        &mut self,
        id: &str,
        run_id: u64,
    ) -> Result<(), SchedulerError> {
        use crate::batch::{Batch, State};
        crate::verification::check_release_topology(&self.capacity)
            .map_err(|_| SchedulerError::Invalid)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active: String =
            tx.query_row("SELECT body FROM capacity_config WHERE id=1", [], |r| {
                r.get(0)
            })?;
        if active != self.encoded {
            return Err(SchedulerError::ConfigChanged);
        }
        type StaleReleaseRow = (String, String, Option<String>, String, String);
        let row: Option<StaleReleaseRow> = tx.query_row(
            "SELECT d.plan,d.state,d.run_id,q.body,s.source FROM dispatch_intents d JOIN dispatch_batches b ON b.id=d.id JOIN batches q ON q.id=b.batch_id JOIN dispatch_receipt_sources s ON s.id=d.id WHERE d.id=?1",
            [id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)),
        ).optional()?;
        let (plan_body, state, saved_run, batch_body, source) =
            row.ok_or(SchedulerError::DispatchTransition)?;
        let plan: DispatchPlan = serde_json::from_str(&plan_body)?;
        let batch: Batch = serde_json::from_str(&batch_body)?;
        if run_id == 0
            || saved_run.as_deref() != Some(&run_id.to_string())
            || source != "direct"
            || !plan.valid()
            || batch.id != id
            || !matches!(batch.state, State::Invalidated { .. })
        {
            return Err(SchedulerError::DispatchTransition);
        }
        if state == "finished" {
            return Ok(());
        }
        if state != "observed" {
            return Err(SchedulerError::DispatchTransition);
        }
        finish_bounded_reservation(&tx, id, &self.encoded)?;
        tx.commit()?;
        Ok(())
    }
}

fn finish_bounded_reservation(
    tx: &rusqlite::Transaction<'_>,
    id: &str,
    encoded: &str,
) -> Result<(), SchedulerError> {
    tx.execute(
        "INSERT OR IGNORE INTO capacity_release_topology(id,body) VALUES(1,?1)",
        [encoded],
    )?;
    let locked: String = tx.query_row(
        "SELECT body FROM capacity_release_topology WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    if locked != encoded {
        return Err(SchedulerError::ConfigChanged);
    }
    if tx.execute("DELETE FROM capacity_reservations WHERE id=?1", [id])? != 1 {
        return Err(SchedulerError::Corrupt);
    }
    tx.execute("INSERT INTO capacity_finished(id) VALUES(?1)", [id])?;
    tx.execute(
        "UPDATE dispatch_intents SET state='finished' WHERE id=?1",
        [id],
    )?;
    Ok(())
}

//! Observed job runtime metrics. These do not attest CI or authorize landing.
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("job identity or timestamps are invalid or incomplete")]
    InvalidJob,
    #[error("the same provider job was observed with conflicting immutable data")]
    ConflictingJob,
    #[error("metric total exceeds the supported range")]
    Overflow,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JobMeasurement {
    pub repository_id: u64,
    pub job_id: u64,
    pub run_id: u64,
    pub attempt: u32,
    pub batch_id: String,
    pub head_sha: String,
    pub name: String,
    pub host: String,
    pub runner_id: u64,
    pub started_ms: i64,
    pub completed_ms: i64,
    pub conclusion: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    pub completed_jobs: u64,
    pub runner_milliseconds: u64,
    pub rerun_jobs: u64,
    pub rerun_milliseconds: u64,
    /// Completed observed scheduling-delay intervals, grouped by requested pool.
    pub capacity_waits: BTreeMap<String, CapacityWaitSummary>,
}
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CapacityWaitSummary {
    pub intervals: u64,
    pub elapsed_milliseconds: u64,
}
pub(crate) fn ensure_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS ci_job_metrics (repository_id TEXT NOT NULL, job_id TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(repository_id,job_id));
        CREATE INDEX IF NOT EXISTS ci_job_metrics_batch_attempt ON ci_job_metrics(repository_id, json_extract(body, '$.batch_id'), json_extract(body, '$.attempt')) WHERE json_valid(body);
        CREATE TABLE IF NOT EXISTS ci_capacity_waits (id INTEGER PRIMARY KEY, batch_id TEXT NOT NULL, request TEXT NOT NULL, started_ms INTEGER NOT NULL, ended_ms INTEGER);
        CREATE UNIQUE INDEX IF NOT EXISTS ci_capacity_waits_open ON ci_capacity_waits(batch_id) WHERE ended_ms IS NULL;")
}
pub(crate) fn insert(
    tx: &Transaction<'_>,
    measurement: &JobMeasurement,
) -> Result<bool, MetricsError> {
    let repository = measurement.repository_id.to_string();
    let job = measurement.job_id.to_string();
    let encoded = serde_json::to_string(measurement)?;
    let existing: Option<String> = tx
        .query_row(
            "SELECT body FROM ci_job_metrics WHERE repository_id=?1 AND job_id=?2",
            params![repository, job],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(existing) = existing {
        let old: JobMeasurement = serde_json::from_str(&existing)?;
        if old != *measurement {
            return Err(MetricsError::ConflictingJob);
        }
        return Ok(false);
    }
    tx.execute(
        "INSERT INTO ci_job_metrics(repository_id,job_id,body) VALUES(?1,?2,?3)",
        params![repository, job, encoded],
    )?;
    Ok(true)
}
/// Record a first failed reservation observation, not a claim that the pool
/// was continuously saturated between polls. Idempotent across restarts.
pub(crate) fn begin_capacity_wait(
    tx: &Transaction<'_>,
    batch_id: &str,
    request: &crate::scheduler::Request,
) -> Result<(), MetricsError> {
    let Some(now) = clock_ms() else {
        return Ok(());
    };
    let encoded = serde_json::to_string(request)?;
    let open: Option<(i64, String)> = tx
        .query_row(
            "SELECT id,request FROM ci_capacity_waits WHERE batch_id=?1 AND ended_ms IS NULL",
            [batch_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if let Some((id, previous)) = open {
        if previous == encoded {
            return Ok(());
        }
        // An operator changed the request before reservation; don't attribute
        // the old partial interval to the new pool requirements.
        tx.execute("DELETE FROM ci_capacity_waits WHERE id=?1", [id])?;
    }
    tx.execute(
        "INSERT INTO ci_capacity_waits(batch_id,request,started_ms) VALUES(?1,?2,?3)",
        params![batch_id, encoded, now],
    )?;
    Ok(())
}

/// Close only matching intervals. An unusable wall-clock sample is discarded
/// rather than blocking the scheduler or emitting a negative delay.
pub(crate) fn end_capacity_wait(
    tx: &Transaction<'_>,
    batch_id: &str,
    request: &crate::scheduler::Request,
) -> Result<(), MetricsError> {
    let Some(now) = clock_ms() else {
        return Ok(());
    };
    let open: Option<(i64, String, i64)> = tx.query_row(
        "SELECT id,request,started_ms FROM ci_capacity_waits WHERE batch_id=?1 AND ended_ms IS NULL",
        [batch_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
    ).optional()?;
    if let Some((id, previous, started)) = open {
        if previous != serde_json::to_string(request)? || now < started {
            tx.execute("DELETE FROM ci_capacity_waits WHERE id=?1", [id])?;
        } else {
            tx.execute(
                "UPDATE ci_capacity_waits SET ended_ms=?2 WHERE id=?1 AND ended_ms IS NULL",
                params![id, now],
            )?;
        }
    }
    Ok(())
}
fn clock_ms() -> Option<i64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
}

pub(crate) fn summarize(conn: &Connection) -> Result<Summary, MetricsError> {
    let mut stmt = conn.prepare("SELECT body FROM ci_job_metrics")?;
    let mut rows = stmt.query([])?;
    let mut count = 0u64;
    let mut milliseconds = 0u64;
    let mut rerun_jobs = 0u64;
    let mut rerun_milliseconds = 0u64;
    while let Some(row) = rows.next()? {
        let measurement: JobMeasurement = serde_json::from_str(&row.get::<_, String>(0)?)?;
        let duration = measurement
            .completed_ms
            .checked_sub(measurement.started_ms)
            .filter(|value| *value >= 0)
            .ok_or(MetricsError::InvalidJob)?;
        milliseconds = milliseconds
            .checked_add(duration as u64)
            .ok_or(MetricsError::Overflow)?;
        count = count.checked_add(1).ok_or(MetricsError::Overflow)?;
        if measurement.attempt > 1 {
            rerun_jobs = rerun_jobs.checked_add(1).ok_or(MetricsError::Overflow)?;
            rerun_milliseconds = rerun_milliseconds
                .checked_add(duration as u64)
                .ok_or(MetricsError::Overflow)?;
        }
    }
    let mut capacity_waits: BTreeMap<String, CapacityWaitSummary> = BTreeMap::new();
    let mut stmt = conn.prepare(
        "SELECT request,started_ms,ended_ms FROM ci_capacity_waits WHERE ended_ms IS NOT NULL",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let request: crate::scheduler::Request = serde_json::from_str(&row.get::<_, String>(0)?)?;
        let started: i64 = row.get(1)?;
        let ended: i64 = row.get(2)?;
        let duration = ended
            .checked_sub(started)
            .filter(|value| *value >= 0)
            .ok_or(MetricsError::InvalidJob)? as u64;
        for pool in request.pools.keys() {
            let entry = capacity_waits.entry(pool.clone()).or_default();
            entry.intervals = entry
                .intervals
                .checked_add(1)
                .ok_or(MetricsError::Overflow)?;
            entry.elapsed_milliseconds = entry
                .elapsed_milliseconds
                .checked_add(duration)
                .ok_or(MetricsError::Overflow)?;
        }
    }
    Ok(Summary {
        completed_jobs: count,
        runner_milliseconds: milliseconds,
        rerun_jobs,
        rerun_milliseconds,
        capacity_waits,
    })
}
pub(crate) fn timestamp_ms(value: &str) -> Result<i64, MetricsError> {
    let timestamp =
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
            .map_err(|_| MetricsError::InvalidJob)?;
    i64::try_from(timestamp.unix_timestamp_nanos().div_euclid(1_000_000))
        .map_err(|_| MetricsError::InvalidJob)
}

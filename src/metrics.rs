//! Observed job runtime metrics. These do not attest CI or authorize landing.
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
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
}
pub(crate) fn ensure_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS ci_job_metrics (repository_id TEXT NOT NULL, job_id TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(repository_id,job_id));")
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
pub(crate) fn summarize(conn: &Connection) -> Result<Summary, MetricsError> {
    let mut stmt = conn.prepare("SELECT body FROM ci_job_metrics")?;
    let mut rows = stmt.query([])?;
    let mut count = 0u64;
    let mut milliseconds = 0u64;
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
    }
    Ok(Summary {
        completed_jobs: count,
        runner_milliseconds: milliseconds,
    })
}
pub(crate) fn timestamp_ms(value: &str) -> Result<i64, MetricsError> {
    let timestamp =
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
            .map_err(|_| MetricsError::InvalidJob)?;
    i64::try_from(timestamp.unix_timestamp_nanos().div_euclid(1_000_000))
        .map_err(|_| MetricsError::InvalidJob)
}

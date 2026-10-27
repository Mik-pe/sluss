use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use thiserror::Error;

use crate::batch::{Batch, Candidate};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("batch already exists or was changed by another worker")]
    Conflict,
    #[error("invalid PR number or empty head SHA")]
    InvalidCandidate,
    #[error("batch candidate is no longer in the queue at the frozen head")]
    StaleCandidate,
    #[error("database is bound to a different GitHub installation or repository")]
    RepositoryMismatch,
    #[error("repository policy has changed; explicit migration is required")]
    PolicyMismatch,
}

pub struct Store {
    conn: Connection,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            INSERT OR IGNORE INTO settings VALUES ('paused', 0);
            CREATE TABLE IF NOT EXISTS candidates (
                number INTEGER PRIMARY KEY CHECK(number > 0),
                head_sha TEXT NOT NULL CHECK(length(head_sha) > 0),
                sequence INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS batches (
                id TEXT PRIMARY KEY, version INTEGER NOT NULL, body TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS repository_binding (
                id INTEGER PRIMARY KEY CHECK(id = 1), installation INTEGER NOT NULL, repository INTEGER NOT NULL, policy_digest TEXT NOT NULL
            );",
        )?;
        Ok(Self { conn })
    }

    pub fn paused(&self) -> Result<bool, StoreError> {
        Ok(self
            .conn
            .query_row("SELECT value FROM settings WHERE key='paused'", [], |r| {
                r.get::<_, i64>(0)
            })?
            != 0)
    }

    pub fn set_paused(&self, paused: bool) -> Result<(), StoreError> {
        self.conn.execute(
            "UPDATE settings SET value=?1 WHERE key='paused'",
            [paused as i64],
        )?;
        Ok(())
    }

    /// Persist a reconciled provider observation. This is not an admission decision.
    /// Changed heads move to the back; repeated observations preserve ordering.
    pub fn observe_candidate(&mut self, candidate: &Candidate) -> Result<(), StoreError> {
        if candidate.number == 0
            || candidate.number > i64::MAX as u64
            || candidate.head_sha.is_empty()
        {
            return Err(StoreError::InvalidCandidate);
        }
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO candidates (number, head_sha, sequence)
            VALUES (?1, ?2, (SELECT COALESCE(MAX(sequence), 0) + 1 FROM candidates))
            ON CONFLICT(number) DO UPDATE SET head_sha=excluded.head_sha, sequence=excluded.sequence
            WHERE candidates.head_sha != excluded.head_sha",
            params![candidate.number as i64, candidate.head_sha],
        )?;
        invalidate_batches(&tx, candidate.number, Some(&candidate.head_sha))?;
        tx.commit()?;
        Ok(())
    }

    pub fn remove_candidate(&mut self, number: u64) -> Result<bool, StoreError> {
        let sql_number = i64::try_from(number).map_err(|_| StoreError::InvalidCandidate)?;
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let removed = tx.execute("DELETE FROM candidates WHERE number=?1", [sql_number])? != 0;
        invalidate_batches(&tx, number, None)?;
        tx.commit()?;
        Ok(removed)
    }

    /// Replace queue observations and acknowledge a reconciliation atomically.
    /// A newer webhook revision makes this entire write fail. One database is
    /// restricted to one repository until the queue is explicitly redesigned.
    pub fn apply_reconciliation(
        &mut self,
        job: &crate::webhook::Reconciliation,
        policy_digest: &str,
        candidates: &[Candidate],
    ) -> Result<(), StoreError> {
        if job.installation <= 0
            || job.repository <= 0
            || job.revision <= 0
            || policy_digest.len() != 64
            || !policy_digest
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err(StoreError::InvalidCandidate);
        }
        let mut numbers = std::collections::BTreeSet::new();
        if candidates.iter().any(|c| {
            c.number == 0
                || c.number > i64::MAX as u64
                || c.head_sha.is_empty()
                || !numbers.insert(c.number)
        }) {
            return Err(StoreError::InvalidCandidate);
        }
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let binding: Option<(i64, i64, String)> = tx
            .query_row(
                "SELECT installation,repository,policy_digest FROM repository_binding WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if let Some((installation, repository, digest)) = binding {
            if (installation, repository) != (job.installation, job.repository) {
                return Err(StoreError::RepositoryMismatch);
            }
            if digest != policy_digest {
                return Err(StoreError::PolicyMismatch);
            }
        } else {
            let existing: i64 = tx.query_row(
                "SELECT (SELECT COUNT(*) FROM candidates) + (SELECT COUNT(*) FROM batches)",
                [],
                |r| r.get(0),
            )?;
            if existing != 0 {
                return Err(StoreError::RepositoryMismatch);
            }
        }
        let revision: Option<i64> = tx
            .query_row(
                "SELECT revision FROM reconciliations WHERE installation=?1 AND repository=?2",
                params![job.installation, job.repository],
                |r| r.get(0),
            )
            .optional()?;
        if revision != Some(job.revision) {
            return Err(StoreError::Conflict);
        }
        tx.execute(
            "INSERT OR IGNORE INTO repository_binding(id,installation,repository,policy_digest) VALUES(1,?1,?2,?3)",
            params![job.installation, job.repository, policy_digest],
        )?;
        let old: Vec<u64> = {
            let mut stmt = tx.prepare("SELECT number FROM candidates")?;
            let rows = stmt.query_map([], |r| Ok(r.get::<_, i64>(0)? as u64))?;
            rows.collect::<Result<_, _>>()?
        };
        for number in old {
            if !numbers.contains(&number) {
                tx.execute("DELETE FROM candidates WHERE number=?1", [number as i64])?;
                invalidate_batches(&tx, number, None)?;
            }
        }
        for candidate in candidates {
            tx.execute("INSERT INTO candidates(number,head_sha,sequence) VALUES(?1,?2,(SELECT COALESCE(MAX(sequence),0)+1 FROM candidates)) ON CONFLICT(number) DO UPDATE SET head_sha=excluded.head_sha,sequence=excluded.sequence WHERE candidates.head_sha!=excluded.head_sha",params![candidate.number as i64,candidate.head_sha])?;
            invalidate_batches(&tx, candidate.number, Some(&candidate.head_sha))?;
        }
        tx.execute(
            "DELETE FROM reconciliations WHERE installation=?1 AND repository=?2 AND revision=?3",
            params![job.installation, job.repository, job.revision],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn candidates(&self) -> Result<Vec<Candidate>, StoreError> {
        let mut stmt = self
            .conn
            .prepare("SELECT number, head_sha FROM candidates ORDER BY sequence, number")?;
        let result = stmt
            .query_map([], |r| {
                Ok(Candidate {
                    number: r.get::<_, i64>(0)? as u64,
                    head_sha: r.get(1)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(result)
    }

    pub fn insert_batch(&mut self, batch: &Batch) -> Result<(), StoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for candidate in &batch.candidates {
            let number =
                i64::try_from(candidate.number).map_err(|_| StoreError::InvalidCandidate)?;
            let head: Option<String> = tx
                .query_row(
                    "SELECT head_sha FROM candidates WHERE number=?1",
                    [number],
                    |r| r.get(0),
                )
                .optional()?;
            if head.as_deref() != Some(&candidate.head_sha) {
                return Err(StoreError::StaleCandidate);
            }
        }
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO batches(id, version, body) VALUES (?1, 0, ?2)",
            params![batch.id, serde_json::to_string(batch)?],
        )?;
        if inserted != 1 {
            return Err(StoreError::Conflict);
        }
        tx.commit()?;
        Ok(())
    }

    pub fn batch(&self, id: &str) -> Result<Option<(Batch, i64)>, StoreError> {
        let row: Option<(String, i64)> = self
            .conn
            .query_row("SELECT body, version FROM batches WHERE id=?1", [id], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .optional()?;
        row.map(|(body, version)| Ok((serde_json::from_str(&body)?, version)))
            .transpose()
    }

    /// Optimistic concurrency prevents a stale worker from overwriting a newer state.
    pub fn update_batch(&self, batch: &Batch, version: i64) -> Result<(), StoreError> {
        let updated = self.conn.execute(
            "UPDATE batches SET version=version+1, body=?1 WHERE id=?2 AND version=?3",
            params![serde_json::to_string(batch)?, batch.id, version],
        )?;
        if updated != 1 {
            return Err(StoreError::Conflict);
        }
        Ok(())
    }
}

/// Run in the same write transaction as the observation so a stale worker's
/// optimistic update cannot restore a batch invalidated by a newer head.
fn invalidate_batches(
    tx: &Transaction<'_>,
    number: u64,
    head: Option<&str>,
) -> Result<(), StoreError> {
    let rows = {
        let mut stmt = tx.prepare("SELECT id, body FROM batches")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    for (id, body) in rows {
        let mut batch: Batch = serde_json::from_str(&body)?;
        let stale = batch
            .candidates
            .iter()
            .any(|c| c.number == number && head != Some(c.head_sha.as_str()));
        if stale
            && batch
                .invalidate(format!("PR #{number} changed or left the queue"))
                .is_ok()
        {
            tx.execute(
                "UPDATE batches SET version=version+1, body=?1 WHERE id=?2",
                params![serde_json::to_string(&batch)?, id],
            )?;
        }
    }
    Ok(())
}

use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use thiserror::Error;

use crate::batch::{Batch, Candidate, State};

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
    #[error("batch inputs or state transition are invalid")]
    InvalidBatch,
    #[error("queue has not been bound by provider reconciliation")]
    Unbound,
    #[error("batch is paused, stale, or awaiting reconciliation")]
    StaleBatch,
    #[error("system clock is outside the supported range")]
    Clock,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AdmissionCandidate {
    pub candidate: Candidate,
    pub evidence_digest: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailedCandidate {
    pub number: u64,
    pub head_sha: String,
    pub batch_id: String,
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
            CREATE TABLE IF NOT EXISTS candidate_evidence (number INTEGER PRIMARY KEY, digest TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS candidate_stability (number INTEGER PRIMARY KEY, changed_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS batch_evidence (batch_id TEXT NOT NULL, number INTEGER NOT NULL, digest TEXT NOT NULL, PRIMARY KEY(batch_id, number));
            CREATE TABLE IF NOT EXISTS batch_policy (batch_id TEXT PRIMARY KEY NOT NULL, digest TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS split_tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, parent_id TEXT NOT NULL, side INTEGER NOT NULL, members TEXT NOT NULL, state TEXT NOT NULL, child_id TEXT, UNIQUE(parent_id,side));
            CREATE TABLE IF NOT EXISTS failed_singletons (number INTEGER PRIMARY KEY, head_sha TEXT NOT NULL, digest TEXT NOT NULL, batch_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS repository_binding (
                id INTEGER PRIMARY KEY CHECK(id = 1), installation INTEGER NOT NULL, repository INTEGER NOT NULL, policy_digest TEXT NOT NULL
            );",
        )?;
        Ok(Self { conn })
    }

    /// Scope fixed by successful repository reconciliation.
    pub fn repository_scope(&self) -> Result<(u64, u64), StoreError> {
        let binding: Option<(i64, i64)> = self
            .conn
            .query_row(
                "SELECT installation,repository FROM repository_binding WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let (installation, repository) = binding.ok_or(StoreError::Unbound)?;
        Ok((
            u64::try_from(installation)
                .ok()
                .filter(|n| *n > 0)
                .ok_or(StoreError::RepositoryMismatch)?,
            u64::try_from(repository)
                .ok()
                .filter(|n| *n > 0)
                .ok_or(StoreError::RepositoryMismatch)?,
        ))
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
        let now = now_ms()?;
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute("INSERT INTO candidate_stability(number,changed_ms) VALUES(?1,?2) ON CONFLICT(number) DO UPDATE SET changed_ms=excluded.changed_ms", params![candidate.number as i64,now])?;
        tx.execute(
            "INSERT INTO candidates (number, head_sha, sequence)
            VALUES (?1, ?2, (SELECT COALESCE(MAX(sequence), 0) + 1 FROM candidates))
            ON CONFLICT(number) DO UPDATE SET head_sha=excluded.head_sha, sequence=excluded.sequence
            WHERE candidates.head_sha != excluded.head_sha",
            params![candidate.number as i64, candidate.head_sha],
        )?;
        tx.execute(
            "DELETE FROM failed_singletons WHERE number=?1 AND head_sha<>?2",
            params![candidate.number as i64, candidate.head_sha],
        )?;
        let removed_evidence = tx.execute(
            "DELETE FROM candidate_evidence WHERE number=?1",
            [candidate.number as i64],
        )?;
        invalidate_batches(
            &tx,
            candidate.number,
            if removed_evidence > 0 {
                None
            } else {
                Some(&candidate.head_sha)
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn remove_candidate(&mut self, number: u64) -> Result<bool, StoreError> {
        let sql_number = i64::try_from(number).map_err(|_| StoreError::InvalidCandidate)?;
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let removed = tx.execute("DELETE FROM candidates WHERE number=?1", [sql_number])? != 0;
        tx.execute(
            "DELETE FROM candidate_evidence WHERE number=?1",
            [sql_number],
        )?;
        tx.execute(
            "DELETE FROM candidate_stability WHERE number=?1",
            [sql_number],
        )?;
        tx.execute(
            "DELETE FROM failed_singletons WHERE number=?1",
            [sql_number],
        )?;
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
        self.apply_reconciliation_inner(job, policy_digest, candidates, None)
    }
    /// Apply observed review and authorization evidence with the queue update.
    pub fn apply_admission_reconciliation(
        &mut self,
        job: &crate::webhook::Reconciliation,
        policy_digest: &str,
        admitted: &[AdmissionCandidate],
    ) -> Result<(), StoreError> {
        let candidates: Vec<Candidate> = admitted.iter().map(|v| v.candidate.clone()).collect();
        let digests: Vec<String> = admitted.iter().map(|v| v.evidence_digest.clone()).collect();
        if digests.iter().any(|d| {
            d.len() != 64
                || !d
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        }) {
            return Err(StoreError::InvalidCandidate);
        }
        self.apply_reconciliation_inner(job, policy_digest, &candidates, Some(&digests))
    }
    fn apply_reconciliation_inner(
        &mut self,
        job: &crate::webhook::Reconciliation,
        policy_digest: &str,
        candidates: &[Candidate],
        digests: Option<&[String]>,
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
        let now = now_ms()?;
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
                tx.execute(
                    "DELETE FROM candidate_evidence WHERE number=?1",
                    [number as i64],
                )?;
                tx.execute(
                    "DELETE FROM candidate_stability WHERE number=?1",
                    [number as i64],
                )?;
                tx.execute(
                    "DELETE FROM failed_singletons WHERE number=?1",
                    [number as i64],
                )?;
                invalidate_batches(&tx, number, None)?;
            }
        }
        for (index, candidate) in candidates.iter().enumerate() {
            let existing_evidence: Option<String> = tx
                .query_row(
                    "SELECT digest FROM candidate_evidence WHERE number=?1",
                    [candidate.number as i64],
                    |r| r.get(0),
                )
                .optional()?;
            let new_evidence = digests.map(|values| values[index].as_str());
            let old_head: Option<String> = tx
                .query_row(
                    "SELECT head_sha FROM candidates WHERE number=?1",
                    [candidate.number as i64],
                    |r| r.get(0),
                )
                .optional()?;
            let changed = old_head.as_deref() != Some(&candidate.head_sha)
                || existing_evidence.as_deref() != new_evidence
                || new_evidence.is_none();
            if changed {
                tx.execute(
                    "DELETE FROM failed_singletons WHERE number=?1",
                    [candidate.number as i64],
                )?;
                tx.execute("INSERT INTO candidate_stability(number,changed_ms) VALUES(?1,?2) ON CONFLICT(number) DO UPDATE SET changed_ms=excluded.changed_ms", params![candidate.number as i64,now])?;
            } else {
                // A pre-migration candidate starts its debounce window now.
                tx.execute(
                    "INSERT OR IGNORE INTO candidate_stability(number,changed_ms) VALUES(?1,?2)",
                    params![candidate.number as i64, now],
                )?;
            }
            if existing_evidence.as_deref() != new_evidence {
                invalidate_batches(&tx, candidate.number, None)?;
            }
            if let Some(digest) = new_evidence {
                tx.execute("INSERT INTO candidate_evidence(number,digest) VALUES(?1,?2) ON CONFLICT(number) DO UPDATE SET digest=excluded.digest", params![candidate.number as i64, digest])?;
            } else {
                tx.execute(
                    "DELETE FROM candidate_evidence WHERE number=?1",
                    [candidate.number as i64],
                )?;
                // Without admission evidence no existing batch may remain usable.
                invalidate_batches(&tx, candidate.number, None)?;
            }
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

    /// Current singleton failures, excluding records invalidated by new observations.
    pub fn failed_candidates(&self) -> Result<Vec<FailedCandidate>, StoreError> {
        let mut stmt = self.conn.prepare("SELECT f.number,f.head_sha,f.batch_id FROM failed_singletons f JOIN candidates c ON c.number=f.number AND c.head_sha=f.head_sha JOIN candidate_evidence e ON e.number=f.number AND e.digest=f.digest ORDER BY c.sequence,c.number")?;
        let rows = stmt.query_map([], |r| {
            Ok(FailedCandidate {
                number: r.get::<_, i64>(0)? as u64,
                head_sha: r.get(1)?,
                batch_id: r.get(2)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Freeze an ordered batch from a reconciled queue in one transaction.
    /// The supplied base is an observation, not proof that the target is
    /// unchanged; callers must refresh provider state before integration.
    pub fn plan_batch(
        &mut self,
        id: &str,
        base_sha: &str,
        max_batch_size: usize,
        max_active_batches: usize,
    ) -> Result<Option<Batch>, StoreError> {
        self.plan_batch_debounced(
            id,
            base_sha,
            max_batch_size,
            max_active_batches,
            Duration::ZERO,
        )
    }
    /// Select only candidates whose head and admission evidence have remained
    /// unchanged for the requested duration. An unchanged scan preserves age.
    pub fn plan_batch_debounced(
        &mut self,
        id: &str,
        base_sha: &str,
        max_batch_size: usize,
        max_active_batches: usize,
        debounce: Duration,
    ) -> Result<Option<Batch>, StoreError> {
        let now = now_ms()?;
        let debounce_ms =
            i64::try_from(debounce.as_millis()).map_err(|_| StoreError::InvalidBatch)?;
        let valid_sha = |value: &str| {
            value.len() == 40
                && value
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if !(16..=64).contains(&id.len())
            || !id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            || !valid_sha(base_sha)
            || !(1..=100).contains(&max_batch_size)
            || !(1..=100).contains(&max_active_batches)
        {
            return Err(StoreError::InvalidBatch);
        }
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let binding: Option<(i64, i64)> = tx
            .query_row(
                "SELECT installation,repository FROM repository_binding WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let (installation, repository) = binding.ok_or(StoreError::Unbound)?;
        let paused: bool = tx.query_row(
            "SELECT value != 0 FROM settings WHERE key='paused'",
            [],
            |r| r.get(0),
        )?;
        let pending: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM reconciliations WHERE installation=?1 AND repository=?2)",
            params![installation, repository],
            |r| r.get(0),
        )?;
        if paused || pending {
            return Ok(None);
        }
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM batches WHERE id=?1)",
            [id],
            |r| r.get(0),
        )?;
        if exists {
            return Err(StoreError::Conflict);
        }
        let mut active = 0usize;
        let mut occupied = std::collections::BTreeSet::new();
        {
            let mut stmt = tx.prepare("SELECT body FROM batches")?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let batch: Batch = serde_json::from_str(&row.get::<_, String>(0)?)?;
                if matches!(
                    batch.state,
                    State::Planned
                        | State::Building
                        | State::Testing { .. }
                        | State::Verified { .. }
                ) {
                    active += 1;
                    occupied.extend(batch.candidates.iter().map(|c| c.number));
                }
            }
        }
        if active >= max_active_batches {
            return Ok(None);
        }
        let mut selected = Vec::new();
        let mut selected_task = None;
        let mut held_for_splits = std::collections::BTreeSet::new();
        let tasks: Vec<(i64, String)> = {
            let mut stmt =
                tx.prepare("SELECT id,members FROM split_tasks WHERE state='pending' ORDER BY id")?;
            let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<Result<_, _>>()?
        };
        for (task_id, body) in tasks {
            let members: Vec<AdmissionCandidate> = serde_json::from_str(&body)?;
            if members.is_empty() {
                return Err(StoreError::InvalidBatch);
            }
            let mut stale = false;
            let mut ready = true;
            for member in &members {
                let number = i64::try_from(member.candidate.number)
                    .map_err(|_| StoreError::InvalidCandidate)?;
                let head: Option<String> = tx
                    .query_row(
                        "SELECT head_sha FROM candidates WHERE number=?1",
                        [number],
                        |r| r.get(0),
                    )
                    .optional()?;
                let evidence: Option<String> = tx
                    .query_row(
                        "SELECT digest FROM candidate_evidence WHERE number=?1",
                        [number],
                        |r| r.get(0),
                    )
                    .optional()?;
                let changed: Option<i64> = tx
                    .query_row(
                        "SELECT changed_ms FROM candidate_stability WHERE number=?1",
                        [number],
                        |r| r.get(0),
                    )
                    .optional()?;
                if head.as_deref() != Some(&member.candidate.head_sha)
                    || evidence.as_deref() != Some(&member.evidence_digest)
                {
                    stale = true;
                }
                if occupied.contains(&member.candidate.number)
                    || (debounce_ms > 0
                        && changed
                            .is_none_or(|time| time < 0 || now.saturating_sub(time) < debounce_ms))
                {
                    ready = false;
                }
            }
            if stale {
                tx.execute(
                    "UPDATE split_tasks SET state='stale' WHERE id=?1 AND state='pending'",
                    [task_id],
                )?;
                continue;
            }
            held_for_splits.extend(members.iter().map(|m| m.candidate.number));
            if selected_task.is_none() && ready {
                if members.len() > max_batch_size {
                    return Err(StoreError::InvalidBatch);
                }
                selected = members.into_iter().map(|m| m.candidate).collect();
                selected_task = Some(task_id);
            }
        }
        if selected_task.is_none() {
            let mut stmt =
                tx.prepare("SELECT number,head_sha FROM candidates ORDER BY sequence,number")?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let number = row.get::<_, i64>(0)? as u64;
                let sha: String = row.get(1)?;
                if !valid_sha(&sha) {
                    return Err(StoreError::InvalidCandidate);
                }
                if !occupied.contains(&number) && !held_for_splits.contains(&number) {
                    let blocked: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM failed_singletons f JOIN candidate_evidence e ON e.number=f.number WHERE f.number=?1 AND f.head_sha=?2 AND f.digest=e.digest)", params![number as i64,sha], |r| r.get(0))?;
                    if blocked {
                        continue;
                    }
                    let changed: Option<i64> = tx
                        .query_row(
                            "SELECT changed_ms FROM candidate_stability WHERE number=?1",
                            [number as i64],
                            |r| r.get(0),
                        )
                        .optional()?;
                    if debounce_ms > 0
                        && changed
                            .is_none_or(|time| time < 0 || now.saturating_sub(time) < debounce_ms)
                    {
                        continue;
                    }
                    let evidence: Option<String> = tx
                        .query_row(
                            "SELECT digest FROM candidate_evidence WHERE number=?1",
                            [number as i64],
                            |r| r.get(0),
                        )
                        .optional()?;
                    if evidence.is_none() {
                        return Err(StoreError::InvalidCandidate);
                    }
                    selected.push(Candidate {
                        number,
                        head_sha: sha,
                    });
                    if selected.len() == max_batch_size {
                        break;
                    }
                }
            }
        }
        if selected.is_empty() {
            tx.commit()?;
            return Ok(None);
        }
        let batch = Batch::new(id.into(), base_sha.into(), selected)
            .map_err(|_| StoreError::InvalidBatch)?;
        tx.execute(
            "INSERT INTO batches(id,version,body) VALUES(?1,0,?2)",
            params![id, serde_json::to_string(&batch)?],
        )?;
        for candidate in &batch.candidates {
            let digest: String = tx.query_row(
                "SELECT digest FROM candidate_evidence WHERE number=?1",
                [candidate.number as i64],
                |r| r.get(0),
            )?;
            tx.execute(
                "INSERT INTO batch_evidence(batch_id,number,digest) VALUES(?1,?2,?3)",
                params![id, candidate.number as i64, digest],
            )?;
        }
        if let Some(task_id) = selected_task {
            tx.execute("UPDATE split_tasks SET state='planned',child_id=?2 WHERE id=?1 AND state='pending'", params![task_id,id])?;
        }
        tx.commit()?;
        Ok(Some(batch))
    }

    pub fn insert_batch(&mut self, batch: &Batch) -> Result<(), StoreError> {
        if batch.state != State::Planned
            || Batch::new(
                batch.id.clone(),
                batch.base_sha.clone(),
                batch.candidates.clone(),
            )
            .is_err()
        {
            return Err(StoreError::InvalidBatch);
        }
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
        // Even direct inserts cannot put the same PR into two live batches.
        {
            let mut stmt = tx.prepare("SELECT body FROM batches")?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let existing: Batch = serde_json::from_str(&row.get::<_, String>(0)?)?;
                if matches!(
                    existing.state,
                    State::Planned
                        | State::Building
                        | State::Testing { .. }
                        | State::Verified { .. }
                ) && existing
                    .candidates
                    .iter()
                    .any(|a| batch.candidates.iter().any(|b| a.number == b.number))
                {
                    return Err(StoreError::Conflict);
                }
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

    /// Begin or resume a frozen integration after checking the queue atomically.
    pub fn start_integration(&mut self, id: &str) -> Result<Batch, StoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let (mut batch, version) = checked_batch(&tx, id)?;
        match batch.state {
            State::Planned => {
                batch.begin_build().map_err(|_| StoreError::InvalidBatch)?;
                tx.execute(
                    "UPDATE batches SET body=?1,version=version+1 WHERE id=?2 AND version=?3",
                    params![serde_json::to_string(&batch)?, id, version],
                )?;
            }
            State::Building | State::Testing { .. } => {}
            _ => return Err(StoreError::InvalidBatch),
        }
        tx.commit()?;
        Ok(batch)
    }
    /// Record a deterministic integration failure only if the batch is still current.
    pub fn fail_integration(&mut self, id: &str, reason: &str) -> Result<Batch, StoreError> {
        if reason.is_empty() {
            return Err(StoreError::InvalidBatch);
        }
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let (mut batch, version) = checked_batch(&tx, id)?;
        if !matches!(batch.state, State::Building | State::Testing { .. }) {
            return Err(StoreError::InvalidBatch);
        }
        batch
            .fail(reason.into())
            .map_err(|_| StoreError::InvalidBatch)?;
        tx.execute(
            "UPDATE batches SET body=?1,version=version+1 WHERE id=?2 AND version=?3",
            params![serde_json::to_string(&batch)?, id, version],
        )?;
        enqueue_bisection(&tx, &batch)?;
        tx.commit()?;
        Ok(batch)
    }
    /// Record the exact staged commit and policy digest atomically. Repeated
    /// completion is accepted only when both values match the frozen record.
    pub fn finish_integration(
        &mut self,
        id: &str,
        sha: &str,
        policy_digest: &str,
    ) -> Result<Batch, StoreError> {
        let valid = |s: &str, n: usize| {
            s.len() == n
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if !valid(sha, 40) || !valid(policy_digest, 64) {
            return Err(StoreError::InvalidBatch);
        }
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let (mut batch, version) = checked_batch(&tx, id)?;
        match &batch.state {
            State::Building => {
                batch
                    .begin_testing(sha.into())
                    .map_err(|_| StoreError::InvalidBatch)?;
                tx.execute(
                    "INSERT INTO batch_policy(batch_id,digest) VALUES(?1,?2)",
                    params![id, policy_digest],
                )?;
                tx.execute(
                    "UPDATE batches SET body=?1,version=version+1 WHERE id=?2 AND version=?3",
                    params![serde_json::to_string(&batch)?, id, version],
                )?;
            }
            State::Testing { integration_sha } if integration_sha == sha => {
                let saved: Option<String> = tx
                    .query_row(
                        "SELECT digest FROM batch_policy WHERE batch_id=?1",
                        [id],
                        |r| r.get(0),
                    )
                    .optional()?;
                if saved.as_deref() != Some(policy_digest) {
                    return Err(StoreError::StaleBatch);
                }
            }
            _ => return Err(StoreError::InvalidBatch),
        }
        tx.commit()?;
        Ok(batch)
    }

    /// Optimistic concurrency prevents a stale worker from overwriting a newer state.
    pub fn update_batch(&mut self, batch: &Batch, version: i64) -> Result<(), StoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let current: Option<(String, i64)> = tx
            .query_row(
                "SELECT body, version FROM batches WHERE id=?1",
                [&batch.id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let (body, current_version) = current.ok_or(StoreError::Conflict)?;
        if version != current_version {
            return Err(StoreError::Conflict);
        }
        let previous: Batch = serde_json::from_str(&body)?;
        if previous.id != batch.id
            || previous.base_sha != batch.base_sha
            || previous.candidates != batch.candidates
            || !valid_transition(&previous.state, &batch.state)
        {
            return Err(StoreError::InvalidBatch);
        }
        let updated = tx.execute(
            "UPDATE batches SET version=version+1, body=?1 WHERE id=?2 AND version=?3",
            params![serde_json::to_string(batch)?, batch.id, version],
        )?;
        if updated != 1 {
            return Err(StoreError::Conflict);
        }
        tx.commit()?;
        Ok(())
    }
}

fn valid_transition(previous: &State, next: &State) -> bool {
    match (previous, next) {
        (State::Planned, State::Building | State::Failed { .. } | State::Invalidated { .. }) => {
            true
        }
        (State::Building, State::Testing { integration_sha }) => !integration_sha.is_empty(),
        (
            State::Building | State::Testing { .. },
            State::Failed { .. } | State::Invalidated { .. },
        ) => true,
        (State::Verified { .. }, State::Invalidated { .. }) => true,
        (
            State::Testing {
                integration_sha: tested,
            },
            State::Verified {
                integration_sha: verified,
            },
        ) => tested == verified && !tested.is_empty(),
        _ => false,
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

fn now_ms() -> Result<i64, StoreError> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| StoreError::Clock)?;
    i64::try_from(elapsed.as_millis()).map_err(|_| StoreError::Clock)
}

fn checked_batch(tx: &Transaction<'_>, id: &str) -> Result<(Batch, i64), StoreError> {
    let binding: Option<(i64, i64)> = tx
        .query_row(
            "SELECT installation,repository FROM repository_binding WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let (installation, repository) = binding.ok_or(StoreError::Unbound)?;
    let paused: bool = tx.query_row(
        "SELECT value != 0 FROM settings WHERE key='paused'",
        [],
        |r| r.get(0),
    )?;
    let pending: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM reconciliations WHERE installation=?1 AND repository=?2)",
        params![installation, repository],
        |r| r.get(0),
    )?;
    if paused || pending {
        return Err(StoreError::StaleBatch);
    }
    let (body, version): (String, i64) = tx
        .query_row("SELECT body,version FROM batches WHERE id=?1", [id], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .optional()?
        .ok_or(StoreError::InvalidBatch)?;
    let batch: Batch = serde_json::from_str(&body)?;
    if batch.id != id {
        return Err(StoreError::InvalidBatch);
    }
    for candidate in &batch.candidates {
        let number = i64::try_from(candidate.number).map_err(|_| StoreError::StaleBatch)?;
        let head: Option<String> = tx
            .query_row(
                "SELECT head_sha FROM candidates WHERE number=?1",
                [number],
                |r| r.get(0),
            )
            .optional()?;
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
                params![id, number],
                |r| r.get(0),
            )
            .optional()?;
        if head.as_deref() != Some(&candidate.head_sha) || current.is_none() || current != frozen {
            return Err(StoreError::StaleBatch);
        }
    }
    Ok((batch, version))
}

/// Split only failures explicitly classified by the caller as reproducible
/// integration or CI failures. Infrastructure errors must not use this path.
fn enqueue_bisection(tx: &Transaction<'_>, batch: &Batch) -> Result<(), StoreError> {
    let mut members = Vec::new();
    for candidate in &batch.candidates {
        let number = i64::try_from(candidate.number).map_err(|_| StoreError::InvalidCandidate)?;
        let evidence: Option<String> = tx
            .query_row(
                "SELECT digest FROM batch_evidence WHERE batch_id=?1 AND number=?2",
                params![batch.id, number],
                |r| r.get(0),
            )
            .optional()?;
        let digest = evidence.ok_or(StoreError::StaleBatch)?;
        members.push(AdmissionCandidate {
            candidate: candidate.clone(),
            evidence_digest: digest,
        });
    }
    if members.len() == 1 {
        let member = &members[0];
        tx.execute("INSERT INTO failed_singletons(number,head_sha,digest,batch_id) VALUES(?1,?2,?3,?4) ON CONFLICT(number) DO UPDATE SET head_sha=excluded.head_sha,digest=excluded.digest,batch_id=excluded.batch_id", params![member.candidate.number as i64,member.candidate.head_sha,member.evidence_digest,batch.id])?;
    } else {
        let (left, right) = members.split_at(members.len() / 2);
        for (side, group) in [(0, left), (1, right)] {
            tx.execute(
                "INSERT INTO split_tasks(parent_id,side,members,state) VALUES(?1,?2,?3,'pending')",
                params![batch.id, side, serde_json::to_string(group)?],
            )?;
        }
    }
    Ok(())
}

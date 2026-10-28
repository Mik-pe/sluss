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
    #[error(transparent)]
    Metrics(#[from] crate::metrics::MetricsError),
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
    #[error("pause Sluss and stop its workers before migrating selection policy")]
    MigrationNeedsPause,
    #[error(
        "selection migration requires no active batches, reservations or unresolved dispatches"
    )]
    MigrationBusy,
    #[error("pause Sluss before migrating planner settings")]
    PlanningMigrationNeedsPause,
    #[error("planner migration requires no active batches, reservations or unresolved dispatches")]
    PlanningMigrationBusy,
    #[error("a pending split is larger than the new maximum batch size")]
    PlanningMigrationSplitTooLarge,
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
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningSettings {
    pub max_batch_size: usize,
    pub max_active_batches: usize,
    pub debounce: Duration,
    pub batch_window: Duration,
}

impl PlanningSettings {
    fn validate(&self) -> Result<(), StoreError> {
        if !(1..=100).contains(&self.max_batch_size)
            || !(1..=100).contains(&self.max_active_batches)
            || self.debounce > Duration::from_secs(86400)
            || self.batch_window > Duration::from_secs(86400)
        {
            return Err(StoreError::InvalidBatch);
        }
        Ok(())
    }
    pub fn digest(&self) -> Result<String, StoreError> {
        use sha2::{Digest, Sha256};
        self.validate()?;
        let mut hash = Sha256::new();
        hash.update(b"sluss-planning-settings-v1\0");
        hash.update(serde_json::to_vec(self)?);
        Ok(hex::encode(hash.finalize()))
    }
}

/// Historical CI comparison and any durable conflict marker. This is not
/// authenticated attestation or permission to land.
#[derive(Debug)]
pub struct CiEvidence {
    pub record: crate::scheduler::CiObservationRecord,
    pub conflicted: bool,
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
            CREATE TABLE IF NOT EXISTS ci_bisection_config (id INTEGER PRIMARY KEY CHECK(id=1), enabled INTEGER NOT NULL CHECK(enabled IN (0,1)));
            CREATE TABLE IF NOT EXISTS ci_investigations (batch_id TEXT PRIMARY KEY, state TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS ci_candidate_completions (number INTEGER PRIMARY KEY, head_sha TEXT NOT NULL, digest TEXT NOT NULL, base_sha TEXT NOT NULL, batch_id TEXT NOT NULL, conclusion TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS candidate_stability (number INTEGER PRIMARY KEY, changed_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS batch_evidence (batch_id TEXT NOT NULL, number INTEGER NOT NULL, digest TEXT NOT NULL, PRIMARY KEY(batch_id, number));
            CREATE TABLE IF NOT EXISTS batch_policy (batch_id TEXT PRIMARY KEY NOT NULL, digest TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS batch_policy_paths (batch_id TEXT PRIMARY KEY NOT NULL, paths TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS batch_selection (batch_id TEXT PRIMARY KEY NOT NULL, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS observed_base (id INTEGER PRIMARY KEY CHECK(id=1), sha TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS observed_target (id INTEGER PRIMARY KEY CHECK(id=1), branch TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS planning_config (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS planning_migrations (id INTEGER PRIMARY KEY, previous_digest TEXT NOT NULL, next_digest TEXT NOT NULL, previous_body TEXT, next_body TEXT NOT NULL, migrated_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS staging_config (id INTEGER PRIMARY KEY CHECK(id=1), paths TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS staging_selection_config (id INTEGER PRIMARY KEY CHECK(id=1), mode TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS selection_migrations (id INTEGER PRIMARY KEY, previous_mode TEXT NOT NULL, next_mode TEXT NOT NULL, migrated_ms INTEGER NOT NULL, cleared_holds INTEGER NOT NULL, discarded_batches INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS split_tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, parent_id TEXT NOT NULL, side INTEGER NOT NULL, members TEXT NOT NULL, state TEXT NOT NULL, child_id TEXT, UNIQUE(parent_id,side));
            CREATE TABLE IF NOT EXISTS failed_singletons (number INTEGER PRIMARY KEY, head_sha TEXT NOT NULL, digest TEXT NOT NULL, batch_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS repository_binding (
                id INTEGER PRIMARY KEY CHECK(id = 1), installation INTEGER NOT NULL, repository INTEGER NOT NULL, policy_digest TEXT NOT NULL
            );",
        )?;
        crate::metrics::ensure_schema(&conn)?;
        Ok(Self { conn })
    }

    /// Summarize completed jobs observed by Sluss, not all repository CI.
    pub fn metrics(&self) -> Result<crate::metrics::Summary, StoreError> {
        Ok(crate::metrics::summarize(&self.conn)?)
    }

    /// Read a completed CI observation without constructing a scheduler.
    pub fn ci_evidence(&self, id: &str) -> Result<Option<CiEvidence>, StoreError> {
        let exists: bool = self.conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='ci_observations')", [], |r| r.get(0))?;
        if !exists {
            return Ok(None);
        }
        let body: Option<String> = self
            .conn
            .query_row("SELECT body FROM ci_observations WHERE id=?1", [id], |r| {
                r.get(0)
            })
            .optional()?;
        let Some(body) = body else {
            return Ok(None);
        };
        let has_conflicts: bool = self.conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='ci_observation_conflicts')", [], |r| r.get(0))?;
        let conflicted = if has_conflicts {
            self.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM ci_observation_conflicts WHERE id=?1)",
                [id],
                |r| r.get(0),
            )?
        } else {
            false
        };
        Ok(Some(CiEvidence {
            record: serde_json::from_str(&body)?,
            conflicted,
        }))
    }

    /// Admission configuration bound by successful repository reconciliation.
    pub fn repository_policy_digest(&self) -> Result<Option<String>, StoreError> {
        Ok(self
            .conn
            .query_row(
                "SELECT policy_digest FROM repository_binding WHERE id=1",
                [],
                |r| r.get(0),
            )
            .optional()?)
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
        self.apply_reconciliation_inner(job, policy_digest, candidates, None, None)
    }
    /// Apply observed review and authorization evidence with the queue update.
    pub fn apply_admission_reconciliation(
        &mut self,
        job: &crate::webhook::Reconciliation,
        policy_digest: &str,
        admitted: &[AdmissionCandidate],
    ) -> Result<(), StoreError> {
        self.apply_admission_reconciliation_inner(job, policy_digest, admitted, None)
    }

    /// Commit an observed target base together with admission and inbox acknowledgement.
    pub fn apply_admission_reconciliation_with_base(
        &mut self,
        job: &crate::webhook::Reconciliation,
        policy_digest: &str,
        admitted: &[AdmissionCandidate],
        base_sha: &str,
        target: &str,
    ) -> Result<(), StoreError> {
        self.apply_admission_reconciliation_inner(
            job,
            policy_digest,
            admitted,
            Some((base_sha, target)),
        )
    }

    fn apply_admission_reconciliation_inner(
        &mut self,
        job: &crate::webhook::Reconciliation,
        policy_digest: &str,
        admitted: &[AdmissionCandidate],
        base_sha: Option<(&str, &str)>,
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
        self.apply_reconciliation_inner(job, policy_digest, &candidates, Some(&digests), base_sha)
    }
    fn apply_reconciliation_inner(
        &mut self,
        job: &crate::webhook::Reconciliation,
        policy_digest: &str,
        candidates: &[Candidate],
        digests: Option<&[String]>,
        base_sha: Option<(&str, &str)>,
    ) -> Result<(), StoreError> {
        if base_sha.is_some_and(|(sha, target)| {
            target.is_empty()
                || target.len() > 255
                || sha.len() != 40
                || !sha
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        }) {
            return Err(StoreError::InvalidBatch);
        }
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
        let old_base: Option<String> = tx
            .query_row("SELECT sha FROM observed_base WHERE id=1", [], |r| r.get(0))
            .optional()?;
        if old_base.is_some() && base_sha.is_none() {
            return Err(StoreError::StaleBatch);
        }
        if let Some((base, target)) = base_sha {
            if old_base.as_deref() != Some(base) {
                clear_ci_candidate_heads(&tx)?;
                let batches = {
                    let mut stmt = tx.prepare("SELECT id,body FROM batches")?;
                    let rows = stmt
                        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
                    rows.collect::<Result<Vec<_>, _>>()?
                };
                for (id, body) in batches {
                    let mut batch: Batch = serde_json::from_str(&body)?;
                    if batch.base_sha != base
                        && batch.invalidate("target base changed".into()).is_ok()
                    {
                        tx.execute(
                            "UPDATE batches SET version=version+1,body=?1 WHERE id=?2",
                            params![serde_json::to_string(&batch)?, id],
                        )?;
                    }
                }
                tx.execute(
                    "UPDATE split_tasks SET state='stale' WHERE state='pending'",
                    [],
                )?;
                tx.execute("DELETE FROM failed_singletons", [])?;
                tx.execute("DELETE FROM ci_candidate_completions", [])?;
            }
            let old_target: Option<String> = tx
                .query_row("SELECT branch FROM observed_target WHERE id=1", [], |r| {
                    r.get(0)
                })
                .optional()?;
            if old_target.as_deref().is_some_and(|old| old != target) {
                return Err(StoreError::PolicyMismatch);
            }
            tx.execute("INSERT INTO observed_base(id,sha) VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET sha=excluded.sha", [base])?;
            tx.execute("INSERT INTO observed_target(id,branch) VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET branch=excluded.branch", [target])?;
        }
        let old: Vec<u64> = {
            let mut stmt = tx.prepare("SELECT number FROM candidates")?;
            let rows = stmt.query_map([], |r| Ok(r.get::<_, i64>(0)? as u64))?;
            rows.collect::<Result<_, _>>()?
        };
        for number in old {
            if !numbers.contains(&number) {
                clear_ci_candidate_heads(&tx)?;
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
                clear_ci_candidate_heads(&tx)?;
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
        invalidate_stale_ci_investigations(&tx)?;
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

    /// Bind the optional CI investigation mode across all workers.
    pub fn bind_ci_bisection(&mut self, enabled: bool) -> Result<(), StoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT OR IGNORE INTO ci_bisection_config(id,enabled) VALUES(1,?1)",
            [enabled],
        )?;
        let existing: bool = tx.query_row(
            "SELECT enabled FROM ci_bisection_config WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if existing != enabled {
            return Err(StoreError::PolicyMismatch);
        }
        tx.commit()?;
        Ok(())
    }

    /// Summarize bounded CI investigation progress for operators.
    pub fn ci_investigations(&self) -> Result<Vec<(String, String)>, StoreError> {
        let mut stmt = self
            .conn
            .prepare("SELECT batch_id,state FROM ci_investigations ORDER BY batch_id")?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Current CI-only completions are scheduling holds, not merge approvals.
    pub fn ci_only_completions(&self) -> Result<Vec<(u64, String, String)>, StoreError> {
        let mut stmt = self.conn.prepare("SELECT c.number,c.batch_id,c.conclusion FROM ci_candidate_completions c JOIN candidates p ON p.number=c.number AND p.head_sha=c.head_sha JOIN candidate_evidence e ON e.number=c.number AND e.digest=c.digest JOIN observed_base b ON b.id=1 AND b.sha=c.base_sha ORDER BY p.sequence,p.number")?;
        let rows = stmt.query_map([], |r| {
            Ok((r.get::<_, i64>(0)? as u64, r.get(1)?, r.get(2)?))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Remove holds for a completed CI-only batch; this does not free a runner.
    pub fn retry_ci_only(&mut self, batch_id: &str) -> Result<usize, StoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let body: Option<String> = tx
            .query_row("SELECT body FROM batches WHERE id=?1", [batch_id], |r| {
                r.get(0)
            })
            .optional()?;
        let batch: Batch = serde_json::from_str(&body.ok_or(StoreError::InvalidBatch)?)?;
        if !matches!(batch.state, State::CiComplete { .. }) {
            return Err(StoreError::InvalidBatch);
        }
        let removed = tx.execute(
            "DELETE FROM ci_candidate_completions WHERE batch_id=?1",
            [batch_id],
        )?;
        tx.commit()?;
        Ok(removed)
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
    /// Read the bound planning configuration and its versioned digest.
    pub fn planning_settings(&self) -> Result<Option<(PlanningSettings, String)>, StoreError> {
        let body: Option<String> = self
            .conn
            .query_row("SELECT body FROM planning_config WHERE id=1", [], |r| {
                r.get(0)
            })
            .optional()?;
        body.map(|body| {
            let settings: PlanningSettings = serde_json::from_str(&body)?;
            let digest = settings.digest()?;
            Ok((settings, digest))
        })
        .transpose()
    }

    /// Migrate settings only while paused and drained. Pending split groups are
    /// preserved and must fit the new limit. Workers must be stopped by the
    /// operator: SQLite cannot establish that an external Git process exited.
    pub fn migrate_planning_settings(
        &mut self,
        expected: &str,
        next: &PlanningSettings,
        installation: i64,
        repository: i64,
        target: &str,
    ) -> Result<bool, StoreError> {
        if expected != "unbound" && !valid_hex(expected, 64) {
            return Err(StoreError::InvalidBatch);
        }
        let next_digest = next.digest()?;
        let next_body = serde_json::to_string(next)?;
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let paused: bool = tx.query_row(
            "SELECT value != 0 FROM settings WHERE key='paused'",
            [],
            |r| r.get(0),
        )?;
        if !paused {
            return Err(StoreError::PlanningMigrationNeedsPause);
        }
        let binding: Option<(i64, i64, String)> = tx.query_row(
            "SELECT r.installation,r.repository,t.branch FROM repository_binding r JOIN observed_target t ON r.id=t.id WHERE r.id=1", [],
            |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
        ).optional()?;
        match binding {
            Some((i, r, _)) if (i, r) != (installation, repository) => {
                return Err(StoreError::RepositoryMismatch);
            }
            Some((_, _, branch)) if branch != target => return Err(StoreError::PolicyMismatch),
            None => return Err(StoreError::Unbound),
            _ => {}
        }
        let old_body: Option<String> = tx
            .query_row("SELECT body FROM planning_config WHERE id=1", [], |r| {
                r.get(0)
            })
            .optional()?;
        let old_digest = match &old_body {
            Some(body) => serde_json::from_str::<PlanningSettings>(body)?.digest()?,
            None => "unbound".to_owned(),
        };
        if old_digest != expected {
            return Err(StoreError::PolicyMismatch);
        }
        // Validate the drain inside the same write transaction that changes the
        // settings so a competing planner or dispatcher cannot interleave.
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
                    return Err(StoreError::PlanningMigrationBusy);
                }
            }
        }
        for (table, predicate) in [
            ("capacity_reservations", "1"),
            ("dispatch_intents", "state NOT IN ('finished','cancelled')"),
        ] {
            let exists: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
                [table],
                |r| r.get(0),
            )?;
            if exists
                && tx.query_row(
                    &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE {predicate})"),
                    [],
                    |r| r.get::<_, bool>(0),
                )?
            {
                return Err(StoreError::PlanningMigrationBusy);
            }
        }
        {
            let mut stmt = tx.prepare("SELECT members FROM split_tasks WHERE state='pending'")?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let members: Vec<AdmissionCandidate> =
                    serde_json::from_str(&row.get::<_, String>(0)?)?;
                if members.is_empty() {
                    return Err(StoreError::InvalidBatch);
                }
                if members.len() > next.max_batch_size {
                    return Err(StoreError::PlanningMigrationSplitTooLarge);
                }
            }
        }
        if old_digest == next_digest {
            tx.commit()?;
            return Ok(false);
        }
        tx.execute("INSERT INTO planning_config(id,body) VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET body=excluded.body", [&next_body])?;
        tx.execute("INSERT INTO planning_migrations(previous_digest,next_digest,previous_body,next_body,migrated_ms) VALUES(?1,?2,?3,?4,?5)", params![old_digest,next_digest,old_body,next_body,now_ms()?])?;
        tx.commit()?;
        Ok(true)
    }

    /// The supplied base is an observation, not proof that the target is
    /// unchanged; callers must refresh provider state before integration.
    /// Plan using only the last provider-reconciled repository and target.
    /// The planning transaction rechecks the base and outstanding webhooks.
    pub fn plan_observed_batch(
        &mut self,
        installation: i64,
        repository: i64,
        policy_digest: &str,
        target: &str,
        settings: &PlanningSettings,
    ) -> Result<Option<Batch>, StoreError> {
        let binding: Option<(i64, i64, String, String, String)> = self.conn.query_row(
            "SELECT r.installation,r.repository,r.policy_digest,b.sha,t.branch FROM repository_binding r JOIN observed_base b ON b.id=r.id JOIN observed_target t ON t.id=r.id WHERE r.id=1",
            [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        ).optional()?;
        let (known_installation, known_repository, known_policy, base, known_target) =
            binding.ok_or(StoreError::Unbound)?;
        if (known_installation, known_repository) != (installation, repository) {
            return Err(StoreError::RepositoryMismatch);
        }
        if known_policy != policy_digest || known_target != target {
            return Err(StoreError::PolicyMismatch);
        }
        if !(1..=100).contains(&settings.max_batch_size)
            || !(1..=100).contains(&settings.max_active_batches)
            || i64::try_from(settings.debounce.as_millis()).is_err()
            || i64::try_from(settings.batch_window.as_millis()).is_err()
        {
            return Err(StoreError::InvalidBatch);
        }
        let encoded = serde_json::to_string(settings)?;
        self.conn.execute(
            "INSERT OR IGNORE INTO planning_config(id,body) VALUES(1,?1)",
            [&encoded],
        )?;
        let stored: String =
            self.conn
                .query_row("SELECT body FROM planning_config WHERE id=1", [], |r| {
                    r.get(0)
                })?;
        if stored != encoded {
            return Err(StoreError::PolicyMismatch);
        }
        let random: String = self
            .conn
            .query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))?;
        self.plan_batch_with_window(
            &format!("batch-{random}"),
            &base,
            settings.max_batch_size,
            settings.max_active_batches,
            settings.debounce,
            settings.batch_window,
        )
    }

    /// Active batch identities and state for operators and restart recovery.
    pub fn active_batches(&self) -> Result<Vec<Batch>, StoreError> {
        let mut stmt = self
            .conn
            .prepare("SELECT body FROM batches ORDER BY rowid")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        let mut result = Vec::new();
        for row in rows {
            let batch: Batch = serde_json::from_str(&row?)?;
            if matches!(
                batch.state,
                State::Planned | State::Building | State::Testing { .. } | State::Verified { .. }
            ) {
                result.push(batch);
            }
        }
        Ok(result)
    }

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
        self.plan_batch_with_window(
            id,
            base_sha,
            max_batch_size,
            max_active_batches,
            debounce,
            Duration::ZERO,
        )
    }

    /// Collect eligible candidates until the window expires or the batch fills.
    /// The window starts when the oldest selected candidate finishes debouncing;
    /// persisted candidate timestamps make it survive a process restart.
    pub fn plan_batch_with_window(
        &mut self,
        id: &str,
        base_sha: &str,
        max_batch_size: usize,
        max_active_batches: usize,
        debounce: Duration,
        batch_window: Duration,
    ) -> Result<Option<Batch>, StoreError> {
        let now = now_ms()?;
        let debounce_ms =
            i64::try_from(debounce.as_millis()).map_err(|_| StoreError::InvalidBatch)?;
        let window_ms =
            i64::try_from(batch_window.as_millis()).map_err(|_| StoreError::InvalidBatch)?;
        let waiting_ms = debounce_ms
            .checked_add(window_ms)
            .ok_or(StoreError::InvalidBatch)?;
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
        let settings = PlanningSettings {
            max_batch_size,
            max_active_batches,
            debounce,
            batch_window,
        };
        let stored_settings: Option<String> = tx
            .query_row("SELECT body FROM planning_config WHERE id=1", [], |r| {
                r.get(0)
            })
            .optional()?;
        let requested_settings = serde_json::to_string(&settings)?;
        if stored_settings
            .as_deref()
            .is_some_and(|body| body != requested_settings)
        {
            return Err(StoreError::PolicyMismatch);
        }
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
        let observed_base: Option<String> = tx
            .query_row("SELECT sha FROM observed_base WHERE id=1", [], |r| r.get(0))
            .optional()?;
        if observed_base.as_deref().is_some_and(|sha| sha != base_sha) {
            return Err(StoreError::StaleBatch);
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
        let tasks: Vec<(i64, String, String)> = {
            let mut stmt = tx.prepare(
                "SELECT id,members,parent_id FROM split_tasks WHERE state='pending' ORDER BY id",
            )?;
            let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
            rows.collect::<Result<_, _>>()?
        };
        for (task_id, body, parent_id) in tasks {
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
                tx.execute(
                    "UPDATE ci_investigations SET state='stale' WHERE batch_id=?1",
                    [&parent_id],
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
                    let completed: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM ci_candidate_completions c JOIN candidate_evidence e ON e.number=c.number WHERE c.number=?1 AND c.head_sha=?2 AND c.digest=e.digest AND c.base_sha=?3)", params![number as i64, sha, base_sha], |r| r.get(0))?;
                    if completed {
                        continue;
                    }
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
        // Bisection halves should be verified promptly rather than collected
        // with unrelated candidates. A full ordinary batch is also ready now.
        if selected_task.is_none() && window_ms > 0 && selected.len() < max_batch_size {
            let mut oldest = i64::MAX;
            for candidate in &selected {
                let changed: Option<i64> = tx
                    .query_row(
                        "SELECT changed_ms FROM candidate_stability WHERE number=?1",
                        [candidate.number as i64],
                        |r| r.get(0),
                    )
                    .optional()?;
                let Some(changed) = changed.filter(|time| *time >= 0) else {
                    return Ok(None);
                };
                oldest = oldest.min(changed);
            }
            if now.saturating_sub(oldest) < waiting_ms {
                tx.commit()?;
                return Ok(None);
            }
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
        let observed_base: Option<String> = tx
            .query_row("SELECT sha FROM observed_base WHERE id=1", [], |r| r.get(0))
            .optional()?;
        if observed_base
            .as_deref()
            .is_some_and(|sha| sha != batch.base_sha)
        {
            return Err(StoreError::StaleBatch);
        }
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

    /// Whether this batch was created as a durable bisection half.
    pub fn is_split_batch(&self, id: &str) -> Result<bool, StoreError> {
        Ok(self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM split_tasks WHERE child_id=?1 AND state='planned')",
            [id],
            |r| r.get(0),
        )?)
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

    /// Return a previously frozen selective plan, if any.
    pub fn batch_selection(
        &self,
        id: &str,
    ) -> Result<Option<crate::selective::FrozenSelection>, StoreError> {
        let body: Option<String> = self
            .conn
            .query_row(
                "SELECT body FROM batch_selection WHERE batch_id=?1",
                [id],
                |r| r.get(0),
            )
            .optional()?;
        body.map(|value| serde_json::from_str(&value).map_err(StoreError::from))
            .transpose()
    }
    /// Freeze a selection only while its batch and admission evidence are current.
    pub fn record_selection(
        &mut self,
        id: &str,
        selection: &crate::selective::FrozenSelection,
    ) -> Result<(), StoreError> {
        validate_selection(selection)?;
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let (batch, _) = checked_batch(&tx, id)?;
        if batch.base_sha != selection.base_sha
            || batch.state
                != (State::Testing {
                    integration_sha: selection.integration_sha.clone(),
                })
        {
            return Err(StoreError::StaleBatch);
        }
        save_selection(&tx, id, selection)?;
        tx.commit()?;
        Ok(())
    }

    /// Freeze the operator's complete declared CI policy file set. Different
    /// workers must not prepare batches under different assumptions.
    pub fn bind_policy_paths(
        &mut self,
        batch_id: &str,
        paths: &[String],
    ) -> Result<(), StoreError> {
        let mut paths = paths.to_vec();
        paths.sort();
        if paths.is_empty()
            || paths.len() > 128
            || paths.windows(2).any(|w| w[0] == w[1])
            || paths
                .iter()
                .any(|path| !crate::integration::valid_policy_path(path))
        {
            return Err(StoreError::InvalidBatch);
        }
        let encoded = serde_json::to_string(&paths)?;
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let (batch, _) = checked_batch(&tx, batch_id)?;
        if !matches!(
            batch.state,
            State::Planned | State::Building | State::Testing { .. }
        ) {
            return Err(StoreError::StaleBatch);
        }
        tx.execute(
            "INSERT OR IGNORE INTO staging_config(id,paths) VALUES(1,?1)",
            [&encoded],
        )?;
        let existing: String =
            tx.query_row("SELECT paths FROM staging_config WHERE id=1", [], |r| {
                r.get(0)
            })?;
        if existing != encoded {
            return Err(StoreError::PolicyMismatch);
        }
        tx.commit()?;
        Ok(())
    }

    /// Bind automatic staging to full verification or one operator policy.
    pub fn bind_staging_selection(
        &mut self,
        batch_id: &str,
        policy_digest: Option<&str>,
    ) -> Result<(), StoreError> {
        let mode = policy_digest.unwrap_or("full");
        if mode != "full" && !valid_hex(mode, 64) {
            return Err(StoreError::InvalidBatch);
        }
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let (batch, _) = checked_batch(&tx, batch_id)?;
        if !matches!(
            batch.state,
            State::Planned | State::Building | State::Testing { .. }
        ) {
            return Err(StoreError::StaleBatch);
        }
        tx.execute(
            "INSERT OR IGNORE INTO staging_selection_config(id,mode) VALUES(1,?1)",
            [mode],
        )?;
        let old: String = tx.query_row(
            "SELECT mode FROM staging_selection_config WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if old != mode {
            return Err(StoreError::PolicyMismatch);
        }
        tx.commit()?;
        Ok(())
    }

    /// Current frozen staging-selection mode, displayed for explicit migration.
    pub fn staging_selection_mode(&self) -> Result<Option<String>, StoreError> {
        Ok(self
            .conn
            .query_row(
                "SELECT mode FROM staging_selection_config WHERE id=1",
                [],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Change the selection mode only while paused and without reservations or
    /// unresolved sends. Active batches require explicit discard. The caller
    /// must stop workers: the database cannot prove Git processes have exited.
    pub fn migrate_staging_selection(
        &mut self,
        expected: &str,
        next: &str,
        discard_unstarted: bool,
    ) -> Result<bool, StoreError> {
        let valid_mode = |mode: &str| mode == "full" || valid_hex(mode, 64);
        if !(expected == "unbound" || valid_mode(expected)) || !valid_mode(next) {
            return Err(StoreError::InvalidBatch);
        }
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let paused: bool = tx.query_row(
            "SELECT value != 0 FROM settings WHERE key='paused'",
            [],
            |r| r.get(0),
        )?;
        if !paused {
            return Err(StoreError::MigrationNeedsPause);
        }
        let current: Option<String> = tx
            .query_row(
                "SELECT mode FROM staging_selection_config WHERE id=1",
                [],
                |r| r.get(0),
            )
            .optional()?;
        let current = current.unwrap_or_else(|| "unbound".into());
        if current != expected {
            return Err(StoreError::PolicyMismatch);
        }
        let bodies: Vec<String> = {
            let mut stmt = tx.prepare("SELECT body FROM batches")?;
            let rows = stmt.query_map([], |r| r.get(0))?;
            rows.collect::<Result<_, _>>()?
        };
        let mut active = Vec::new();
        for body in bodies {
            let batch: Batch = serde_json::from_str(&body)?;
            if matches!(
                batch.state,
                State::Planned | State::Building | State::Testing { .. } | State::Verified { .. }
            ) {
                active.push(batch);
            }
        }
        if !active.is_empty() && !discard_unstarted {
            return Err(StoreError::MigrationBusy);
        }
        let capacity_exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='capacity_reservations')", [], |r| r.get(0))?;
        if capacity_exists {
            let reserved: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM capacity_reservations)",
                [],
                |r| r.get(0),
            )?;
            if reserved {
                return Err(StoreError::MigrationBusy);
            }
        }
        let dispatch_exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='dispatch_intents')", [], |r| r.get(0))?;
        if dispatch_exists {
            let unresolved: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM dispatch_intents WHERE state NOT IN ('finished','cancelled'))", [], |r| r.get(0))?;
            if unresolved {
                return Err(StoreError::MigrationBusy);
            }
        }
        if current == next && active.is_empty() {
            tx.commit()?;
            return Ok(false);
        }
        let discarded = i64::try_from(active.len()).map_err(|_| StoreError::InvalidBatch)?;
        for mut batch in active {
            batch
                .invalidate("selection policy migrated".into())
                .map_err(|_| StoreError::InvalidBatch)?;
            tx.execute(
                "UPDATE batches SET body=?1,version=version+1 WHERE id=?2",
                params![serde_json::to_string(&batch)?, batch.id],
            )?;
        }
        clear_ci_candidate_heads(&tx)?;
        // A prior CI investigation is tied to the previous selection policy.
        // Its pending descendants and CI-only holds cannot suppress new work.
        tx.execute("WITH RECURSIVE descendants(id) AS (SELECT batch_id FROM ci_investigations UNION SELECT t.child_id FROM split_tasks t JOIN descendants d ON t.parent_id=d.id WHERE t.child_id IS NOT NULL) UPDATE split_tasks SET state='stale' WHERE state='pending' AND parent_id IN (SELECT id FROM descendants)", [])?;
        tx.execute(
            "UPDATE ci_investigations SET state='stale' WHERE state!='stale'",
            [],
        )?;
        let cleared = tx.execute("DELETE FROM ci_candidate_completions", [])?;
        tx.execute("DELETE FROM ci_capacity_waits WHERE ended_ms IS NULL", [])?;
        tx.execute(
            "INSERT INTO staging_selection_config(id,mode) VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET mode=excluded.mode",
            [next],
        )?;
        tx.execute("INSERT INTO selection_migrations(previous_mode,next_mode,migrated_ms,cleared_holds,discarded_batches) VALUES(?1,?2,?3,?4,?5)", params![current,next,now_ms()?,i64::try_from(cleared).map_err(|_| StoreError::InvalidBatch)?,discarded])?;
        tx.commit()?;
        Ok(true)
    }

    /// Return the declared-file policy digest for a prepared batch.
    pub fn batch_policy_digest(&self, id: &str) -> Result<Option<String>, StoreError> {
        Ok(self
            .conn
            .query_row(
                "SELECT digest FROM batch_policy WHERE batch_id=?1",
                [id],
                |r| r.get(0),
            )
            .optional()?)
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
        // A subgroup that cannot be integrated has no comparable CI result.
        // Keep its separate integration-conflict bisection, but stop the CI
        // experiment and its unstarted sibling work.
        let parent: Option<String> = tx
            .query_row(
                "SELECT parent_id FROM split_tasks WHERE child_id=?1 AND state='planned'",
                [id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(parent) = parent {
            let pending: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM ci_investigations WHERE batch_id=?1 AND state='pending')",
                [&parent], |r| r.get(0),
            )?;
            if pending {
                finish_ci_investigation(&tx, &parent, "inconclusive")?;
                tx.execute(
                    "UPDATE split_tasks SET state='stale' WHERE parent_id=?1 AND state='pending'",
                    [&parent],
                )?;
            }
        }
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
        self.finish_integration_with_selection(id, sha, policy_digest, None)
    }

    /// Commit the staged SHA, policy snapshot and optional suite selection atomically.
    pub fn finish_integration_with_selection(
        &mut self,
        id: &str,
        sha: &str,
        policy_digest: &str,
        selection: Option<&crate::selective::FrozenSelection>,
    ) -> Result<Batch, StoreError> {
        if !valid_hex(sha, 40) || !valid_hex(policy_digest, 64) {
            return Err(StoreError::InvalidBatch);
        }
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let (mut batch, version) = checked_batch(&tx, id)?;
        let mode: Option<String> = tx
            .query_row(
                "SELECT mode FROM staging_selection_config WHERE id=1",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(mode) = mode {
            match (mode.as_str(), selection) {
                ("full", None) => {}
                (expected, Some(value)) if expected == value.policy_digest => {}
                _ => return Err(StoreError::PolicyMismatch),
            }
        }
        if let Some(value) = selection {
            validate_selection(value)?;
            if value.base_sha != batch.base_sha || value.integration_sha != sha {
                return Err(StoreError::StaleBatch);
            }
            save_selection(&tx, id, value)?;
        }
        match &batch.state {
            State::Building => {
                let paths: Option<String> = tx
                    .query_row("SELECT paths FROM staging_config WHERE id=1", [], |r| {
                        r.get(0)
                    })
                    .optional()?;
                if let Some(paths) = paths {
                    tx.execute(
                        "INSERT INTO batch_policy_paths(batch_id,paths) VALUES(?1,?2)",
                        params![id, paths],
                    )?;
                }
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

fn valid_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn validate_selection(selection: &crate::selective::FrozenSelection) -> Result<(), StoreError> {
    if !valid_hex(&selection.policy_digest, 64)
        || selection.selection.suites().is_empty()
        || selection.selection.suites().len() > 100
        || selection.selection.suites().iter().any(|name| {
            name.is_empty()
                || name.len() > 100
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        })
    {
        return Err(StoreError::InvalidBatch);
    }
    Ok(())
}
fn save_selection(
    tx: &Transaction<'_>,
    id: &str,
    selection: &crate::selective::FrozenSelection,
) -> Result<(), StoreError> {
    let existing: Option<String> = tx
        .query_row(
            "SELECT body FROM batch_selection WHERE batch_id=?1",
            [id],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(body) = existing {
        if serde_json::from_str::<crate::selective::FrozenSelection>(&body)? != *selection {
            return Err(StoreError::Conflict);
        }
    } else {
        tx.execute(
            "INSERT INTO batch_selection(batch_id,body) VALUES(?1,?2)",
            params![id, serde_json::to_string(selection)?],
        )?;
    }
    Ok(())
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

/// Remove only current pointers; immutable observations remain as history.
/// Older databases may not yet have the experimental candidate tables.
fn clear_ci_candidate_heads(tx: &Transaction<'_>) -> Result<(), StoreError> {
    let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='ci_evidence_candidate_heads_v1')", [], |r| r.get(0))?;
    if exists {
        tx.execute("DELETE FROM ci_evidence_candidate_heads_v1", [])?;
    }
    Ok(())
}

/// Any new scoped provider delivery can supersede a saved observation. The
/// delivery and this invalidation commit together; duplicates do not reapply.
pub(crate) fn invalidate_ci_candidate_hint(
    tx: &Transaction<'_>,
    installation: i64,
    repository: i64,
) -> Result<(), StoreError> {
    let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='repository_binding')", [], |r| r.get(0))?;
    if !exists {
        return Ok(());
    }
    let binding: Option<(i64, i64)> = tx
        .query_row(
            "SELECT installation,repository FROM repository_binding WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if binding == Some((installation, repository)) {
        clear_ci_candidate_heads(tx)?;
    }
    Ok(())
}

/// A signed provider security-change hint invalidates all active evidence in
/// this single-repository database before its wakeup can be acknowledged.
pub(crate) fn invalidate_security_event(
    tx: &Transaction<'_>,
    installation: i64,
    repository: i64,
) -> Result<(), StoreError> {
    let has_binding: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='repository_binding')", [], |r| r.get(0))?;
    if !has_binding {
        return Ok(());
    }
    let scope: Option<(i64, i64)> = tx
        .query_row(
            "SELECT installation,repository FROM repository_binding WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if scope != Some((installation, repository)) {
        return Ok(());
    }
    let rows = {
        let mut stmt = tx.prepare("SELECT id,body FROM batches")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    for (id, body) in rows {
        let mut batch: Batch = serde_json::from_str(&body)?;
        if batch
            .invalidate("provider security policy or permissions changed".into())
            .is_ok()
        {
            tx.execute(
                "UPDATE batches SET version=version+1,body=?1 WHERE id=?2",
                params![serde_json::to_string(&batch)?, id],
            )?;
        }
    }
    tx.execute(
        "UPDATE split_tasks SET state='stale' WHERE state='pending'",
        [],
    )?;
    tx.execute(
        "UPDATE ci_investigations SET state='stale' WHERE state!='stale'",
        [],
    )?;
    tx.execute("DELETE FROM ci_candidate_completions", [])?;
    tx.execute("DELETE FROM failed_singletons", [])?;
    tx.execute("DELETE FROM ci_capacity_waits WHERE ended_ms IS NULL", [])?;
    Ok(())
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

/// Propagate a terminal outcome to any still-active ancestor investigation.
pub(crate) fn finish_ci_investigation(
    tx: &Transaction<'_>,
    id: &str,
    outcome: &str,
) -> Result<(), StoreError> {
    if !matches!(outcome, "needs-review" | "inconclusive") {
        return Err(StoreError::InvalidBatch);
    }
    tx.execute(
        "WITH RECURSIVE ancestors(id) AS (SELECT ?1 UNION SELECT t.parent_id FROM split_tasks t JOIN ancestors a ON t.child_id=a.id WHERE t.state='planned') UPDATE ci_investigations SET state=?2 WHERE batch_id IN (SELECT id FROM ancestors) AND state IN ('pending','investigating')",
        params![id, outcome],
    )?;
    Ok(())
}

/// Invalidate a CI experiment and all of its descendants in the same
/// transaction as the admission snapshot. Terminal batch records remain as
/// historical observations; active descendant work cannot be revived.
fn invalidate_stale_ci_investigations(tx: &Transaction<'_>) -> Result<(), StoreError> {
    let base: Option<String> = tx
        .query_row("SELECT sha FROM observed_base WHERE id=1", [], |r| r.get(0))
        .optional()?;
    let investigations: Vec<(String, String)> = {
        let mut stmt = tx.prepare(
            "SELECT i.batch_id,b.body FROM ci_investigations i JOIN batches b ON b.id=i.batch_id WHERE i.state!='stale'",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        rows.collect::<Result<_, _>>()?
    };
    let mut stale = std::collections::BTreeSet::new();
    for (id, body) in investigations {
        let batch: Batch = serde_json::from_str(&body)?;
        let mut changed = base.as_deref() != Some(batch.base_sha.as_str());
        for candidate in &batch.candidates {
            let current: Option<(String, String)> = tx.query_row(
                "SELECT c.head_sha,e.digest FROM candidates c JOIN candidate_evidence e ON e.number=c.number WHERE c.number=?1",
                [i64::try_from(candidate.number).map_err(|_| StoreError::InvalidCandidate)?],
                |r| Ok((r.get(0)?, r.get(1)?)),
            ).optional()?;
            let frozen: Option<String> = tx
                .query_row(
                    "SELECT digest FROM batch_evidence WHERE batch_id=?1 AND number=?2",
                    params![id, candidate.number as i64],
                    |r| r.get(0),
                )
                .optional()?;
            if current.as_ref().map(|(head, _)| head.as_str()) != Some(candidate.head_sha.as_str())
                || current.as_ref().map(|(_, digest)| digest) != frozen.as_ref()
                || frozen.is_none()
            {
                changed = true;
            }
        }
        if changed {
            let mut stmt = tx.prepare(
                "WITH RECURSIVE descendants(id) AS (SELECT ?1 UNION SELECT t.child_id FROM split_tasks t JOIN descendants d ON t.parent_id=d.id WHERE t.child_id IS NOT NULL) SELECT id FROM descendants",
            )?;
            let rows = stmt.query_map([&id], |r| r.get::<_, String>(0))?;
            stale.extend(rows.collect::<Result<Vec<_>, _>>()?);
        }
    }
    for id in stale {
        tx.execute(
            "UPDATE ci_investigations SET state='stale' WHERE batch_id=?1",
            [&id],
        )?;
        tx.execute(
            "UPDATE split_tasks SET state='stale' WHERE parent_id=?1 AND state='pending'",
            [&id],
        )?;
        let body: Option<String> = tx
            .query_row("SELECT body FROM batches WHERE id=?1", [&id], |r| r.get(0))
            .optional()?;
        if let Some(body) = body {
            let mut batch: Batch = serde_json::from_str(&body)?;
            if batch
                .invalidate("CI investigation inputs changed".into())
                .is_ok()
            {
                tx.execute(
                    "UPDATE batches SET body=?1,version=version+1 WHERE id=?2",
                    params![serde_json::to_string(&batch)?, id],
                )?;
            }
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

pub(crate) fn checked_batch(tx: &Transaction<'_>, id: &str) -> Result<(Batch, i64), StoreError> {
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
    let observed_base: Option<String> = tx
        .query_row("SELECT sha FROM observed_base WHERE id=1", [], |r| r.get(0))
        .optional()?;
    if observed_base
        .as_deref()
        .is_some_and(|sha| sha != batch.base_sha)
    {
        return Err(StoreError::StaleBatch);
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
pub(crate) fn enqueue_bisection(tx: &Transaction<'_>, batch: &Batch) -> Result<(), StoreError> {
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

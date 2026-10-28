//! Optional measured CI-only batch sizing. This never grants merge authority.
use crate::{
    batch::Batch,
    scheduler::CiObservationRecord,
    store::{PlanningSettings, StoreError},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};

#[derive(Debug, PartialEq, Eq)]
pub struct AdaptiveStatus {
    pub limit: usize,
    pub success_streak: usize,
    pub increase_threshold: usize,
    pub recorded_samples: u64,
    pub invalidated_samples: u64,
}
#[derive(Debug, PartialEq, Eq)]
pub struct AdaptiveEvent {
    pub batch_id: String,
    pub epoch: i64,
    pub planning_digest: String,
    pub policy_key: String,
    pub batch_size: usize,
    pub limit_before: usize,
    pub conclusion: String,
    pub runner_ms: Option<u64>,
    pub limit_after: usize,
    pub invalid: bool,
}

pub(crate) fn history(path: &std::path::Path) -> Result<Vec<AdaptiveEvent>, StoreError> {
    let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    let exists: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='adaptive_batch_events_v1')", [], |r| r.get(0))?;
    if !exists {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare("SELECT batch_id,epoch,planning_digest,policy_key,batch_size,limit_before,conclusion,runner_ms,limit_after,invalid FROM adaptive_batch_events_v1 ORDER BY sequence DESC LIMIT 50")?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, i64>(4)?,
            r.get::<_, i64>(5)?,
            r.get::<_, String>(6)?,
            r.get::<_, i64>(7)?,
            r.get::<_, i64>(8)?,
            r.get::<_, bool>(9)?,
        ))
    })?;
    let mut result = Vec::new();
    for row in rows {
        let (
            batch_id,
            epoch,
            planning_digest,
            policy_key,
            size,
            before,
            conclusion,
            runner_ms,
            after,
            invalid,
        ) = row?;
        let valid_digest = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if batch_id.is_empty()
            || epoch <= 0
            || !valid_digest(&planning_digest)
            || !valid_digest(&policy_key)
            || !(1..=100).contains(&size)
            || !(1..=100).contains(&before)
            || !(1..=100).contains(&after)
            || !matches!(conclusion.as_str(), "success" | "failure")
            || runner_ms < 0
            || (runner_ms == 0 && conclusion == "success")
        {
            return Err(StoreError::InvalidBatch);
        }
        result.push(AdaptiveEvent {
            batch_id,
            epoch,
            planning_digest,
            policy_key,
            batch_size: size as usize,
            limit_before: before as usize,
            conclusion,
            runner_ms: (runner_ms > 0).then_some(runner_ms as u64),
            limit_after: after as usize,
            invalid,
        });
    }
    Ok(result)
}

/// Retrieve or initialize the size limit within the planner's write transaction.
pub(crate) fn limit(
    tx: &Transaction<'_>,
    settings: &PlanningSettings,
) -> Result<usize, StoreError> {
    let digest = settings.digest()?;
    let initial = settings.max_batch_size.min(2);
    let exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM adaptive_batch_state_v1 WHERE id=1)",
        [],
        |r| r.get(0),
    )?;
    if !exists {
        tx.execute(
            "INSERT INTO adaptive_batch_epochs_v1(planning_digest) VALUES(?1)",
            [&digest],
        )?;
        let epoch = tx.last_insert_rowid();
        tx.execute("INSERT INTO adaptive_batch_state_v1(id,epoch,planning_digest,batch_limit,streak,threshold,last_policy_key) VALUES(1,?1,?2,?3,0,3,'')", params![epoch,digest,initial as i64])?;
    }
    let (epoch, bound, size, streak, threshold): (i64,String,i64,i64,i64) = tx.query_row("SELECT epoch,planning_digest,batch_limit,streak,threshold FROM adaptive_batch_state_v1 WHERE id=1", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
    let bound_epoch: Option<String> = tx
        .query_row(
            "SELECT planning_digest FROM adaptive_batch_epochs_v1 WHERE id=?1",
            [epoch],
            |r| r.get(0),
        )
        .optional()?;
    if epoch <= 0
        || bound_epoch.as_deref() != Some(digest.as_str())
        || bound != digest
        || size < 1
        || size > settings.max_batch_size as i64
        || streak < 0
        || !matches!(threshold, 3 | 6)
        || streak >= threshold
    {
        return Err(StoreError::PolicyMismatch);
    }
    Ok(size as usize)
}

pub(crate) fn status(
    conn: &Connection,
    settings: &PlanningSettings,
) -> Result<Option<AdaptiveStatus>, StoreError> {
    if !settings.adaptive_batching {
        return Ok(None);
    }
    let row: Option<(i64,String,i64,i64,i64)> = conn.query_row("SELECT epoch,planning_digest,batch_limit,streak,threshold FROM adaptive_batch_state_v1 WHERE id=1", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
    let digest = settings.digest()?;
    let (epoch, size, streak, threshold) = match row {
        None => (None, settings.max_batch_size.min(2), 0, 3),
        Some((epoch, bound, size, streak, threshold))
            if epoch > 0
                && bound == digest
                && size >= 1
                && size <= settings.max_batch_size as i64
                && streak >= 0
                && matches!(threshold, 3 | 6)
                && streak < threshold =>
        {
            (
                Some(epoch),
                size as usize,
                streak as usize,
                threshold as usize,
            )
        }
        Some(_) => return Err(StoreError::PolicyMismatch),
    };
    let (count, invalid): (i64, i64) = match epoch {
        Some(epoch) => {
            let epoch_digest: Option<String> = conn
                .query_row(
                    "SELECT planning_digest FROM adaptive_batch_epochs_v1 WHERE id=?1",
                    [epoch],
                    |r| r.get(0),
                )
                .optional()?;
            if epoch_digest.as_deref() != Some(digest.as_str()) {
                return Err(StoreError::PolicyMismatch);
            }
            conn.query_row("SELECT COUNT(*),COALESCE(SUM(invalid),0) FROM adaptive_batch_events_v1 WHERE epoch=?1", [epoch], |r| Ok((r.get(0)?,r.get(1)?)))?
        }
        None => (0, 0),
    };
    Ok(Some(AdaptiveStatus {
        limit: size,
        success_streak: streak,
        increase_threshold: threshold,
        recorded_samples: u64::try_from(count).map_err(|_| StoreError::InvalidBatch)?,
        invalidated_samples: u64::try_from(invalid).map_err(|_| StoreError::InvalidBatch)?,
    }))
}

/// Only direct, completed, fully observed CI-only batches can affect sizing.
/// Failed or bisected work never becomes merge evidence or PR attribution.
pub(crate) fn record(
    tx: &Transaction<'_>,
    batch: &Batch,
    record: &CiObservationRecord,
) -> Result<(), StoreError> {
    let config: Option<String> = tx
        .query_row("SELECT body FROM planning_config WHERE id=1", [], |r| {
            r.get(0)
        })
        .optional()?;
    let Some(config) = config else {
        return Ok(());
    };
    let settings: PlanningSettings = serde_json::from_str(&config)?;
    if !settings.adaptive_batching {
        return Ok(());
    }
    let split: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM split_tasks WHERE child_id=?1)",
        [&batch.id],
        |r| r.get(0),
    )?;
    if split {
        return Ok(());
    }
    let current = limit(tx, &settings)?;
    let conclusion = match &record.observation {
        crate::verification::CiObservation::Successful => "success",
        crate::verification::CiObservation::Failed { conclusion } if conclusion == "failure" => {
            "failure"
        }
        _ => return reset_streak(tx),
    };
    let Some(snapshot) = &record.snapshot else {
        return reset_streak(tx);
    };
    let Some(runner) = record.runner_id else {
        return reset_streak(tx);
    };
    let mut executed = snapshot
        .jobs
        .iter()
        .filter(|job| job.conclusion.as_deref() != Some("skipped"));
    let Some(job) = executed.next() else {
        return reset_streak(tx);
    };
    if executed.next().is_some() || job.runner_id != Some(runner) {
        return reset_streak(tx);
    }
    let elapsed = job
        .started_at
        .as_deref()
        .and_then(|v| crate::metrics::timestamp_ms(v).ok())
        .zip(
            job.completed_at
                .as_deref()
                .and_then(|v| crate::metrics::timestamp_ms(v).ok()),
        )
        .and_then(|(start, end)| end.checked_sub(start))
        .filter(|v| *v > 0);
    let elapsed = match elapsed {
        Some(value) => value,
        None if conclusion == "failure" => 0,
        None => return reset_streak(tx),
    };
    let size = batch.candidates.len();
    if size == 0 || size > settings.max_batch_size {
        return Err(StoreError::InvalidBatch);
    }
    let already: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM adaptive_batch_events_v1 WHERE batch_id=?1)",
        [&batch.id],
        |r| r.get(0),
    )?;
    if already {
        return Ok(());
    }
    let policy = serde_json::to_vec(&(
        record.plan.installation_id,
        record.plan.repository_id,
        record.plan.workflow_id,
        &record.workflow_path,
        &record.plan.policy_digest,
        &record.plan.required_jobs,
        &record.plan.suites,
        &record.plan.selection_policy_digest,
        runner,
    ))?;
    let mut hash = Sha256::new();
    hash.update(b"sluss-adaptive-ci-policy-v1\0");
    hash.update(policy);
    let key = hex::encode(hash.finalize());
    let digest = settings.digest()?;
    let (epoch, streak, threshold, last_policy): (i64, i64, i64, String) = tx.query_row(
        "SELECT epoch,streak,threshold,last_policy_key FROM adaptive_batch_state_v1 WHERE id=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?;
    let streak = if last_policy == key { streak } else { 0 };
    tx.execute("INSERT INTO adaptive_batch_events_v1(batch_id,epoch,planning_digest,policy_key,batch_size,limit_before,conclusion,runner_ms,limit_after) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?6)", params![batch.id,epoch,digest,key,size as i64,current as i64,conclusion,elapsed])?;
    let (next, new_streak, new_threshold) = if conclusion == "failure" {
        (
            if size > 1 {
                current.min((size / 2).max(1))
            } else {
                current
            },
            0,
            6,
        )
    } else if size != current {
        (current, 0, threshold)
    } else {
        let next_streak = streak + 1;
        if next_streak < threshold {
            (current, next_streak, threshold)
        } else if current > 1 && slower_than_smaller(tx, epoch, &key, current)? {
            (current - 1, 0, 6)
        } else {
            ((current + 1).min(settings.max_batch_size), 0, 3)
        }
    };
    tx.execute("UPDATE adaptive_batch_state_v1 SET batch_limit=?1,streak=?2,threshold=?3,last_policy_key=?5 WHERE id=1 AND planning_digest=?4", params![next as i64,new_streak,new_threshold,digest,key])?;
    tx.execute(
        "UPDATE adaptive_batch_events_v1 SET limit_after=?2 WHERE batch_id=?1",
        params![batch.id, next as i64],
    )?;
    Ok(())
}

fn reset_streak(tx: &Transaction<'_>) -> Result<(), StoreError> {
    tx.execute(
        "UPDATE adaptive_batch_state_v1 SET streak=0,last_policy_key='' WHERE id=1",
        [],
    )?;
    Ok(())
}

/// A later contradictory provider receipt invalidates the sample and resets
/// the current epoch to a conservative limit. Historical rows remain visible.
pub(crate) fn invalidate(tx: &Transaction<'_>, batch_id: &str) -> Result<(), StoreError> {
    let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='adaptive_batch_events_v1')", [], |r| r.get(0))?;
    if !exists {
        return Ok(());
    }
    let epoch: Option<i64> = tx
        .query_row(
            "SELECT epoch FROM adaptive_batch_events_v1 WHERE batch_id=?1 AND invalid=0",
            [batch_id],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(epoch) = epoch {
        tx.execute(
            "UPDATE adaptive_batch_events_v1 SET invalid=1 WHERE batch_id=?1",
            [batch_id],
        )?;
        tx.execute("UPDATE adaptive_batch_state_v1 SET batch_limit=1,streak=0,threshold=6,last_policy_key='' WHERE id=1 AND epoch=?1", [epoch])?;
    }
    Ok(())
}

fn slower_than_smaller(
    tx: &Transaction<'_>,
    epoch: i64,
    key: &str,
    size: usize,
) -> Result<bool, StoreError> {
    fn median(
        tx: &Transaction<'_>,
        epoch: i64,
        key: &str,
        size: usize,
    ) -> Result<Option<i64>, StoreError> {
        let mut stmt = tx.prepare("SELECT runner_ms FROM adaptive_batch_events_v1 WHERE epoch=?1 AND policy_key=?2 AND batch_size=?3 AND limit_before=?3 AND conclusion='success' AND invalid=0 ORDER BY sequence DESC LIMIT 3")?;
        let mut durations = stmt
            .query_map(params![epoch, key, size as i64], |r| r.get::<_, i64>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        if durations.len() != 3 || durations.iter().any(|v| *v <= 0) {
            return Ok(None);
        }
        durations.sort_unstable();
        Ok(Some(durations[1]))
    }
    let (Some(current), Some(smaller)) = (
        median(tx, epoch, key, size)?,
        median(tx, epoch, key, size - 1)?,
    ) else {
        return Ok(false);
    };
    // Compare median runner time per passing candidate without float rounding.
    Ok((current as u128) * ((size - 1) as u128) * 10 > (smaller as u128) * (size as u128) * 11)
}

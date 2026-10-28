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
    pub recovery_jobs: u64,
    pub recovery_runner_ms: u64,
    pub recovery_settled: bool,
    pub observed_rerun_jobs: u64,
    pub observed_rerun_ms: u64,
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
        {
            return Err(StoreError::InvalidBatch);
        }
        let recovery = recovery_cost(&conn, &batch_id)?;
        let reruns = rerun_cost(&conn, &batch_id)?;
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
            recovery_jobs: recovery.jobs,
            recovery_runner_ms: recovery.runner_ms,
            recovery_settled: recovery.settled,
            observed_rerun_jobs: reruns
                .jobs
                .checked_add(recovery.rerun_jobs)
                .ok_or(StoreError::InvalidBatch)?,
            observed_rerun_ms: reruns
                .runner_ms
                .checked_add(recovery.rerun_ms)
                .ok_or(StoreError::InvalidBatch)?,
        });
    }
    Ok(result)
}

#[derive(Debug, Default)]
struct RecoveryCost {
    jobs: u64,
    runner_ms: u64,
    rerun_jobs: u64,
    rerun_ms: u64,
    settled: bool,
}

#[derive(Default)]
struct RerunCost {
    jobs: u64,
    runner_ms: u64,
}

/// Previously collected, host-checked job measurements remain partial: this
/// counts only known reruns and does not infer that all attempts were seen.
fn rerun_cost(conn: &Connection, batch_id: &str) -> Result<RerunCost, StoreError> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='ci_job_metrics')",
        [],
        |r| r.get(0),
    )?;
    if !exists {
        return Ok(RerunCost::default());
    }
    let mut stmt = conn.prepare("SELECT repository_id,job_id,body FROM ci_job_metrics WHERE json_valid(body) AND json_extract(body,'$.batch_id')=?1 AND json_extract(body,'$.attempt')>1")?;
    let rows = stmt.query_map([batch_id], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
        ))
    })?;
    let mut cost = RerunCost::default();
    let mut expected: Option<(crate::scheduler::DispatchPlan, String)> = None;
    for row in rows {
        let (repository, job, body) = row?;
        let metric: crate::metrics::JobMeasurement = serde_json::from_str(&body)?;
        if metric.batch_id != batch_id
            || metric.attempt < 2
            || metric.repository_id == 0
            || metric.job_id == 0
            || metric.run_id == 0
            || metric.runner_id == 0
            || repository != metric.repository_id.to_string()
            || job != metric.job_id.to_string()
        {
            return Err(StoreError::InvalidBatch);
        }
        if expected.is_none() {
            let binding: Option<(String, String, String)> = conn.query_row("SELECT d.plan,d.run_id,s.source FROM dispatch_intents d JOIN dispatch_receipt_sources s ON s.id=d.id WHERE d.id=?1 AND d.run_id IS NOT NULL", [batch_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
            let (plan, run, source) = binding.ok_or(StoreError::InvalidBatch)?;
            if source != "direct" {
                return Err(StoreError::InvalidBatch);
            }
            expected = Some((serde_json::from_str(&plan)?, run));
        }
        let (plan, run) = expected.as_ref().ok_or(StoreError::InvalidBatch)?;
        if plan.repository_id != metric.repository_id
            || plan.integration_sha != metric.head_sha
            || *run != metric.run_id.to_string()
        {
            return Err(StoreError::InvalidBatch);
        }
        let elapsed = metric
            .completed_ms
            .checked_sub(metric.started_ms)
            .and_then(|ms| u64::try_from(ms).ok())
            .ok_or(StoreError::InvalidBatch)?;
        cost.jobs = cost.jobs.checked_add(1).ok_or(StoreError::InvalidBatch)?;
        cost.runner_ms = cost
            .runner_ms
            .checked_add(elapsed)
            .ok_or(StoreError::InvalidBatch)?;
    }
    Ok(cost)
}

/// Measure every known descendant CI-only job; missing, invalidated or pending
/// work makes the root cost unresolved rather than silently counting zero.
fn recovery_cost(conn: &Connection, root: &str) -> Result<RecoveryCost, StoreError> {
    let has_costs: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='adaptive_split_costs_v1')", [], |r| r.get(0))?;
    if !has_costs {
        return Ok(RecoveryCost::default());
    }
    let root_epoch: i64 = conn.query_row(
        "SELECT epoch FROM adaptive_batch_events_v1 WHERE batch_id=?1",
        [root],
        |r| r.get(0),
    )?;
    let investigation: Option<String> = conn
        .query_row(
            "SELECT state FROM ci_investigations WHERE batch_id=?1",
            [root],
            |r| r.get(0),
        )
        .optional()?;
    let mut cost = RecoveryCost {
        settled: !matches!(
            investigation.as_deref(),
            Some("pending" | "investigating" | "stale")
        ),
        ..RecoveryCost::default()
    };
    let mut stmt = conn.prepare("WITH RECURSIVE tree(id) AS (SELECT ?1 UNION SELECT s.child_id FROM split_tasks s JOIN tree p ON s.parent_id=p.id WHERE s.child_id IS NOT NULL) SELECT s.state,s.child_id,c.root_id,c.epoch,c.runner_ms FROM split_tasks s JOIN tree p ON s.parent_id=p.id LEFT JOIN adaptive_split_costs_v1 c ON c.child_id=s.child_id")?;
    let rows = stmt.query_map([root], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, Option<i64>>(3)?,
            r.get::<_, Option<i64>>(4)?,
        ))
    })?;
    let mut tasks = 0u64;
    for row in rows {
        let (state, child, cost_root, epoch, runner_ms) = row?;
        tasks = tasks.checked_add(1).ok_or(StoreError::InvalidBatch)?;
        if child.is_none() || cost_root.as_deref() != Some(root) || epoch != Some(root_epoch) {
            cost.settled = false;
            continue;
        }
        let reruns = rerun_cost(conn, child.as_deref().ok_or(StoreError::InvalidBatch)?)?;
        cost.rerun_jobs = cost
            .rerun_jobs
            .checked_add(reruns.jobs)
            .ok_or(StoreError::InvalidBatch)?;
        cost.rerun_ms = cost
            .rerun_ms
            .checked_add(reruns.runner_ms)
            .ok_or(StoreError::InvalidBatch)?;
        if state != "planned" {
            cost.settled = false;
        }
        let Some(ms) = runner_ms.filter(|v| *v > 0) else {
            cost.settled = false;
            continue;
        };
        cost.jobs = cost.jobs.checked_add(1).ok_or(StoreError::InvalidBatch)?;
        cost.runner_ms = cost
            .runner_ms
            .checked_add(ms as u64)
            .ok_or(StoreError::InvalidBatch)?;
    }
    if tasks > 0 && investigation.is_none() {
        cost.settled = false;
    }
    if investigation.is_some() && tasks < 2 {
        cost.settled = false;
    }
    if investigation.as_deref().is_some_and(|s| {
        !matches!(
            s,
            "pending" | "investigating" | "stale" | "needs-review" | "inconclusive"
        )
    }) {
        return Err(StoreError::InvalidBatch);
    }
    Ok(cost)
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
        record_split_cost(tx, batch, record)?;
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
    let Some(runner) = record.runner_id else {
        return reset_streak(tx);
    };
    let elapsed = measured_runner_ms(record);
    // Preserve missing timing explicitly; it must not look like zero cost or
    // disappear from later comparisons.
    let elapsed = elapsed.unwrap_or(0);
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
    } else if size != current || elapsed == 0 {
        (current, 0, threshold)
    } else {
        let next_streak = streak + 1;
        if next_streak < threshold {
            (current, next_streak, threshold)
        } else {
            match decide_after_success(tx, epoch, &key, current, settings.max_batch_size)? {
                Decision::Reduce => (current - 1, 0, 6),
                Decision::Hold => (current, 0, 6),
                Decision::Increase => ((current + 1).min(settings.max_batch_size), 0, 3),
            }
        }
    };
    tx.execute("UPDATE adaptive_batch_state_v1 SET batch_limit=?1,streak=?2,threshold=?3,last_policy_key=?5 WHERE id=1 AND planning_digest=?4", params![next as i64,new_streak,new_threshold,digest,key])?;
    tx.execute(
        "UPDATE adaptive_batch_events_v1 SET limit_after=?2 WHERE batch_id=?1",
        params![batch.id, next as i64],
    )?;
    Ok(())
}

fn measured_runner_ms(record: &CiObservationRecord) -> Option<i64> {
    let snapshot = record.snapshot.as_ref()?;
    let runner = record.runner_id?;
    let mut executed = snapshot
        .jobs
        .iter()
        .filter(|job| job.conclusion.as_deref() != Some("skipped"));
    let job = executed.next()?;
    if executed.next().is_some() || job.runner_id != Some(runner) {
        return None;
    }
    let start = crate::metrics::timestamp_ms(job.started_at.as_deref()?).ok()?;
    let end = crate::metrics::timestamp_ms(job.completed_at.as_deref()?).ok()?;
    end.checked_sub(start).filter(|v| *v > 0)
}

fn record_split_cost(
    tx: &Transaction<'_>,
    batch: &Batch,
    record: &CiObservationRecord,
) -> Result<(), StoreError> {
    let root: Option<(String,i64)> = tx.query_row(
        "WITH RECURSIVE ancestors(id) AS (SELECT parent_id FROM split_tasks WHERE child_id=?1 AND state='planned' UNION SELECT t.parent_id FROM split_tasks t JOIN ancestors a ON t.child_id=a.id WHERE t.state='planned') SELECT e.batch_id,e.epoch FROM ancestors a JOIN adaptive_batch_events_v1 e ON e.batch_id=a.id WHERE e.conclusion='failure' ORDER BY e.sequence LIMIT 1",
        [&batch.id], |r| Ok((r.get(0)?,r.get(1)?)),
    ).optional()?;
    let Some((root, epoch)) = root else {
        return Ok(());
    };
    let elapsed = measured_runner_ms(record);
    tx.execute("INSERT OR IGNORE INTO adaptive_split_costs_v1(child_id,root_id,epoch,runner_ms) VALUES(?1,?2,?3,?4)", params![batch.id,root,epoch,elapsed])?;
    let saved: (String, i64, Option<i64>) = tx.query_row(
        "SELECT root_id,epoch,runner_ms FROM adaptive_split_costs_v1 WHERE child_id=?1",
        [&batch.id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    if saved != (root, epoch, elapsed) {
        return Err(StoreError::InvalidBatch);
    }
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

#[derive(Debug)]
enum Score {
    Unknown,
    Blocked,
    Measured { runner_ms: u128, root_passes: u128 },
}
enum Decision {
    Reduce,
    Hold,
    Increase,
}

/// Conservative cost index: root and investigation runner time divided only
/// by candidates from successful root batches. Split successes are not
/// counted as delivered or passed root batches.
fn score(tx: &Transaction<'_>, epoch: i64, key: &str, size: usize) -> Result<Score, StoreError> {
    let failures: Vec<(String, i64)> = {
        let mut stmt = tx.prepare("SELECT batch_id,runner_ms FROM adaptive_batch_events_v1 WHERE epoch=?1 AND policy_key=?2 AND batch_size=?3 AND conclusion='failure' AND invalid=0")?;
        let rows = stmt.query_map(params![epoch, key, size as i64], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?;
        rows.collect::<Result<_, _>>()?
    };
    for (batch, ms) in failures {
        if ms <= 0 || !recovery_cost(tx, &batch)?.settled {
            return Ok(Score::Blocked);
        }
    }
    let events: Vec<(String, String, i64)> = {
        let mut stmt = tx.prepare("SELECT batch_id,conclusion,runner_ms FROM adaptive_batch_events_v1 WHERE epoch=?1 AND policy_key=?2 AND batch_size=?3 AND invalid=0 ORDER BY sequence DESC LIMIT 6")?;
        let rows = stmt.query_map(params![epoch, key, size as i64], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })?;
        rows.collect::<Result<_, _>>()?
    };
    if events.is_empty() {
        return Ok(Score::Unknown);
    }
    let mut elapsed = 0u128;
    let mut passed = 0u128;
    for (batch, conclusion, ms) in &events {
        if *ms <= 0 {
            return Ok(Score::Blocked);
        }
        elapsed += *ms as u128;
        elapsed += u128::from(rerun_cost(tx, batch)?.runner_ms);
        match conclusion.as_str() {
            "success" => passed += size as u128,
            "failure" => {
                let recovery = recovery_cost(tx, batch)?;
                if !recovery.settled {
                    return Ok(Score::Blocked);
                }
                elapsed += u128::from(recovery.runner_ms);
                elapsed += u128::from(recovery.rerun_ms);
            }
            _ => return Err(StoreError::InvalidBatch),
        }
    }
    if passed == 0 {
        return Ok(Score::Blocked);
    }
    if events.len() < 3 {
        return Ok(Score::Unknown);
    }
    Ok(Score::Measured {
        runner_ms: elapsed,
        root_passes: passed,
    })
}

fn exceeds_by_ten_percent(larger: &Score, smaller: &Score) -> bool {
    match (larger, smaller) {
        (
            Score::Measured {
                runner_ms: a,
                root_passes: ap,
            },
            Score::Measured {
                runner_ms: b,
                root_passes: bp,
            },
        ) => a * bp * 10 > b * ap * 11,
        _ => false,
    }
}

fn decide_after_success(
    tx: &Transaction<'_>,
    epoch: i64,
    key: &str,
    current: usize,
    maximum: usize,
) -> Result<Decision, StoreError> {
    let here = score(tx, epoch, key, current)?;
    if matches!(here, Score::Blocked) {
        return Ok(Decision::Hold);
    }
    if current > 1 {
        let smaller = score(tx, epoch, key, current - 1)?;
        if matches!(smaller, Score::Blocked) {
            return Ok(Decision::Hold);
        }
        if exceeds_by_ten_percent(&here, &smaller) {
            return Ok(Decision::Reduce);
        }
    }
    if current >= maximum {
        return Ok(Decision::Hold);
    }
    let larger = score(tx, epoch, key, current + 1)?;
    if matches!(larger, Score::Blocked) || exceeds_by_ten_percent(&larger, &here) {
        Ok(Decision::Hold)
    } else {
        Ok(Decision::Increase)
    }
}

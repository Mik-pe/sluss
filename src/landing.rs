//! Experimental durable intent for an eventual GitHub ref update.
//!
//! This module does not contact GitHub, authenticate the supplied evidence,
//! establish fast-forward ancestry, revalidate reviews or authorize landing.
//! It is not connected to the service or CLI. All callers and stored inputs
//! must be trusted. In particular, a GraphQL clientMutationId is correlation,
//! not an idempotency key or proof that a provider performed an operation.
use std::{path::Path, time::Duration};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const QUERY: &str =
    "mutation($input: UpdateRefsInput!) { updateRefs(input: $input) { clientMutationId } }";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LandingPlan {
    pub operation_id: String,
    pub batch_id: String,
    pub installation_id: i64,
    pub repository_id: i64,
    pub repository_node_id: String,
    pub target_ref: String,
    pub staging_ref: String,
    pub base_sha: String,
    pub staged_sha: String,
    pub policy_digest: String,
    pub admission_digest: String,
    pub ci_evidence_digest: String,
    /// Missing on historical or purely local intents; not a bypass proof.
    #[serde(default)]
    pub branch_rules_digest: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntentState {
    Prepared,
    Sending,
    Acknowledged,
    Cancelled,
}

#[derive(Debug, Error)]
pub enum LandingError {
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Store(#[from] crate::store::StoreError),
    #[error("invalid frozen landing plan")]
    Invalid,
    #[error("operation identifier is already bound to a different plan")]
    IdConflict,
    #[error("another landing intent for this target is unresolved")]
    TargetBusy,
    #[error("invalid landing intent transition")]
    Transition,
    #[error("provider response is ambiguous; keep the intent unresolved")]
    Uncertain,
    #[error("stored landing intent is corrupt")]
    Corrupt,
    #[error("batch or its recorded observation has changed or is incomplete")]
    StaleBatch,
    #[error("this intent must use its batch-bound claim path")]
    BatchBound,
}

fn hex(value: &str, size: usize) -> bool {
    value.len() == size
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        && value.bytes().any(|c| c != b'0')
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
}
fn node_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 200
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'+' | b'/' | b'='))
}
fn reference(value: &str) -> bool {
    value.len() <= 255
        && value.starts_with("refs/heads/")
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.' | b'/'))
        && !value.ends_with('/')
        && !value.ends_with('.')
        && !value.contains("//")
        && !value.contains("..")
        && value
            .split('/')
            .all(|part| !part.is_empty() && !part.starts_with('.') && !part.ends_with(".lock"))
}
impl LandingPlan {
    pub fn validate(&self) -> Result<(), LandingError> {
        if !identifier(&self.operation_id)
            || !identifier(&self.batch_id)
            || self.installation_id <= 0
            || self.repository_id <= 0
            || !node_id(&self.repository_node_id)
            || !reference(&self.target_ref)
            || !reference(&self.staging_ref)
            || self.target_ref.starts_with("refs/heads/sluss/")
            || !self.staging_ref.starts_with("refs/heads/sluss/")
            || self.target_ref == self.staging_ref
            || !hex(&self.base_sha, 40)
            || !hex(&self.staged_sha, 40)
            || self.base_sha == self.staged_sha
            || !hex(&self.policy_digest, 64)
            || !hex(&self.admission_digest, 64)
            || !hex(&self.ci_evidence_digest, 64)
            || self
                .branch_rules_digest
                .as_deref()
                .is_some_and(|value| !hex(value, 64))
        {
            return Err(LandingError::Invalid);
        }
        Ok(())
    }

    /// Build the documented atomic compare-and-update operation. This does
    /// not send it or attest that its inputs are authorized or current.
    pub fn graphql_request(&self) -> Result<serde_json::Value, LandingError> {
        self.validate()?;
        Ok(serde_json::json!({"query":QUERY,"variables":{"input":{
            "repositoryId":self.repository_node_id,"clientMutationId":self.operation_id,
            "refUpdates":[
                {"name":self.target_ref,"beforeOid":self.base_sha,"afterOid":self.staged_sha,"force":false},
                {"name":self.staging_ref,"beforeOid":self.staged_sha,"afterOid":self.staged_sha,"force":false}
            ]
        }}}))
    }
}

pub struct LandingJournal {
    conn: Connection,
}
pub(crate) struct RefreshTarget {
    pub plan: LandingPlan,
    pub run_id: u64,
    pub members: Vec<crate::store::AdmissionCandidate>,
    pub repository_policy_digest: String,
}
impl LandingJournal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LandingError> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS landing_intents_v1 (
            id TEXT PRIMARY KEY NOT NULL, repository_id INTEGER NOT NULL,
            target_ref TEXT NOT NULL, plan TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('prepared','sending','acknowledged','cancelled')),
            receipt_digest TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS landing_one_unresolved_target_v1
        ON landing_intents_v1(repository_id,target_ref) WHERE state IN ('prepared','sending');
        CREATE TABLE IF NOT EXISTS landing_batch_bindings_v1 (id TEXT PRIMARY KEY NOT NULL);",
        )?;
        Ok(Self { conn })
    }
    pub fn prepare(&mut self, plan: &LandingPlan) -> Result<IntentState, LandingError> {
        plan.validate()?;
        let body = serde_json::to_string(plan)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<(String, String, i64, String)> = tx
            .query_row(
                "SELECT plan,state,repository_id,target_ref FROM landing_intents_v1 WHERE id=?1",
                [&plan.operation_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        if let Some(row) = existing {
            let (old, state) = decode_row(&plan.operation_id, row)?;
            if old != *plan {
                return Err(LandingError::IdConflict);
            }
            tx.commit()?;
            return Ok(state);
        }
        let busy: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM landing_intents_v1 WHERE repository_id=?1 AND target_ref=?2 AND state IN ('prepared','sending'))", params![plan.repository_id,plan.target_ref], |r| r.get(0))?;
        if busy {
            return Err(LandingError::TargetBusy);
        }
        tx.execute("INSERT INTO landing_intents_v1(id,repository_id,target_ref,plan,state) VALUES(?1,?2,?3,?4,'prepared')", params![plan.operation_id,plan.repository_id,plan.target_ref,body])?;
        tx.commit()?;
        Ok(IntentState::Prepared)
    }
    pub fn get(&self, id: &str) -> Result<Option<(LandingPlan, IntentState)>, LandingError> {
        let row: Option<(String, String, i64, String)> = self
            .conn
            .query_row(
                "SELECT plan,state,repository_id,target_ref FROM landing_intents_v1 WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        row.map(|row| decode_row(id, row)).transpose()
    }
    /// This only claims an intent. It does not validate live inputs and is
    /// not permission to send; a crash after claim must not trigger a resend.
    pub fn claim(&mut self, id: &str) -> Result<Option<LandingPlan>, LandingError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: Option<(String, String, i64, String)> = tx
            .query_row(
                "SELECT plan,state,repository_id,target_ref FROM landing_intents_v1 WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        let Some(row) = row else {
            tx.commit()?;
            return Ok(None);
        };
        let bound: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM landing_batch_bindings_v1 WHERE id=?1)",
            [id],
            |r| r.get(0),
        )?;
        if bound {
            return Err(LandingError::BatchBound);
        }
        let (plan, state) = decode_row(id, row)?;
        if state != IntentState::Prepared {
            tx.commit()?;
            return Ok(None);
        }
        if tx.execute(
            "UPDATE landing_intents_v1 SET state='sending' WHERE id=?1 AND state='prepared'",
            [id],
        )? != 1
        {
            return Err(LandingError::Transition);
        }
        tx.commit()?;
        Ok(Some(plan))
    }

    /// Freeze a local, already recorded CI observation and current admission
    /// snapshot with the intent in one transaction. The caller must separately
    /// verify the repository node ID and all provider trust assumptions.
    /// This path does not establish trusted attestation or authorize sending.
    pub fn prepare_observed_batch(
        &mut self,
        operation_id: &str,
        batch_id: &str,
        repository_node_id: &str,
    ) -> Result<LandingPlan, LandingError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let plan = observed_plan(&tx, operation_id, batch_id, repository_node_id)?;
        let body = serde_json::to_string(&plan)?;
        let existing: Option<(String, String, i64, String)> = tx
            .query_row(
                "SELECT plan,state,repository_id,target_ref FROM landing_intents_v1 WHERE id=?1",
                [operation_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        if let Some(row) = existing {
            let (old, state) = decode_row(operation_id, row)?;
            let bound: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM landing_batch_bindings_v1 WHERE id=?1)",
                [operation_id],
                |r| r.get(0),
            )?;
            if !bound || old != plan {
                return Err(LandingError::IdConflict);
            }
            if state != IntentState::Prepared {
                return Err(LandingError::Transition);
            }
        } else {
            let busy: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM landing_intents_v1 WHERE repository_id=?1 AND target_ref=?2 AND state IN ('prepared','sending'))", params![plan.repository_id,plan.target_ref], |r| r.get(0))?;
            if busy {
                return Err(LandingError::TargetBusy);
            }
            tx.execute("INSERT INTO landing_intents_v1(id,repository_id,target_ref,plan,state) VALUES(?1,?2,?3,?4,'prepared')", params![operation_id,plan.repository_id,plan.target_ref,body])?;
            tx.execute(
                "INSERT INTO landing_batch_bindings_v1(id) VALUES(?1)",
                [operation_id],
            )?;
        }
        tx.commit()?;
        Ok(plan)
    }

    /// Recheck only local frozen inputs while claiming. This development API
    /// does not query the provider and must not be used to authorize a write.
    /// Provider changes and trusted CI policy still need independent guards.
    pub fn claim_observed_batch(
        &mut self,
        operation_id: &str,
    ) -> Result<Option<LandingPlan>, LandingError> {
        self.claim_observed_batch_inner(operation_id, None)
    }

    pub(crate) fn claim_with_fresh_snapshot(
        &mut self,
        operation_id: &str,
        snapshot: &crate::github::ActionsSnapshot,
    ) -> Result<Option<LandingPlan>, LandingError> {
        self.claim_observed_batch_inner(operation_id, Some(snapshot))
    }

    fn claim_observed_batch_inner(
        &mut self,
        operation_id: &str,
        snapshot: Option<&crate::github::ActionsSnapshot>,
    ) -> Result<Option<LandingPlan>, LandingError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: Option<(String,String,i64,String)> = tx.query_row(
            "SELECT i.plan,i.state,i.repository_id,i.target_ref FROM landing_intents_v1 i JOIN landing_batch_bindings_v1 b ON b.id=i.id WHERE i.id=?1", [operation_id],
            |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)),
        ).optional()?;
        let Some(row) = row else {
            return Err(LandingError::BatchBound);
        };
        let (plan, state) = decode_row(operation_id, row)?;
        if state != IntentState::Prepared {
            tx.commit()?;
            return Ok(None);
        }
        let now = observed_plan(&tx, operation_id, &plan.batch_id, &plan.repository_node_id)?;
        if now != plan {
            return Err(LandingError::StaleBatch);
        }
        if let Some(snapshot) = snapshot {
            let body: String = tx.query_row(
                "SELECT body FROM ci_observations WHERE id=?1",
                [&plan.batch_id],
                |r| r.get(0),
            )?;
            let saved: crate::scheduler::CiObservationRecord = serde_json::from_str(&body)?;
            let mut current = snapshot.clone();
            current.jobs.sort_by_key(|job| job.id);
            if saved.snapshot.as_ref() != Some(&current) {
                return Err(LandingError::StaleBatch);
            }
        }
        if tx.execute(
            "UPDATE landing_intents_v1 SET state='sending' WHERE id=?1 AND state='prepared'",
            [operation_id],
        )? != 1
        {
            return Err(LandingError::Transition);
        }
        tx.commit()?;
        Ok(Some(plan))
    }

    pub(crate) fn refresh_target(
        &mut self,
        operation_id: &str,
    ) -> Result<RefreshTarget, LandingError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: Option<(String,String,i64,String)> = tx.query_row(
            "SELECT i.plan,i.state,i.repository_id,i.target_ref FROM landing_intents_v1 i JOIN landing_batch_bindings_v1 b ON b.id=i.id WHERE i.id=?1", [operation_id],
            |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)),
        ).optional()?;
        let (plan, state) = decode_row(operation_id, row.ok_or(LandingError::BatchBound)?)?;
        if state != IntentState::Prepared {
            return Err(LandingError::Transition);
        }
        if observed_plan(&tx, operation_id, &plan.batch_id, &plan.repository_node_id)? != plan {
            return Err(LandingError::StaleBatch);
        }
        let body: String = tx.query_row(
            "SELECT body FROM ci_observations WHERE id=?1",
            [&plan.batch_id],
            |r| r.get(0),
        )?;
        let saved: crate::scheduler::CiObservationRecord = serde_json::from_str(&body)?;
        let (batch, _) = crate::store::checked_batch(&tx, &plan.batch_id)?;
        let mut members = Vec::with_capacity(batch.candidates.len());
        for candidate in batch.candidates {
            let digest: String = tx.query_row(
                "SELECT digest FROM batch_evidence WHERE batch_id=?1 AND number=?2",
                params![plan.batch_id, candidate.number as i64],
                |r| r.get(0),
            )?;
            members.push(crate::store::AdmissionCandidate {
                candidate,
                evidence_digest: digest,
            });
        }
        let repository_policy_digest: String = tx.query_row(
            "SELECT policy_digest FROM repository_binding WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        tx.commit()?;
        Ok(RefreshTarget {
            plan,
            run_id: saved.run_id,
            members,
            repository_policy_digest,
        })
    }
    /// Only an unclaimed intent can be cancelled without external reconciliation.
    pub fn cancel(&mut self, id: &str) -> Result<bool, LandingError> {
        Ok(self.conn.execute(
            "UPDATE landing_intents_v1 SET state='cancelled' WHERE id=?1 AND state='prepared'",
            [id],
        )? == 1)
    }
    /// Record a direct successful GraphQL response supplied by a trusted
    /// transport. Every error, malformed/partial response or mismatched ID is
    /// uncertain. A later readback is not an acknowledgement of this request.
    pub fn acknowledge(
        &mut self,
        id: &str,
        status: u16,
        response: &[u8],
    ) -> Result<(), LandingError> {
        if status != 200 || response.len() > 65536 {
            return Err(LandingError::Uncertain);
        }
        let value: serde_json::Value =
            serde_json::from_slice(response).map_err(|_| LandingError::Uncertain)?;
        if value
            .get("errors")
            .is_some_and(|v| !v.as_array().is_some_and(|a| a.is_empty()))
            || value
                .pointer("/data/updateRefs/clientMutationId")
                .and_then(|v| v.as_str())
                != Some(id)
        {
            return Err(LandingError::Uncertain);
        }
        let digest = hex::encode(Sha256::digest(response));
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: Option<(String, String, i64, String, Option<String>)> = tx.query_row(
            "SELECT plan,state,repository_id,target_ref,receipt_digest FROM landing_intents_v1 WHERE id=?1",
            [id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)),
        ).optional()?;
        let Some((body, state, repository, target, recorded)) = row else {
            return Err(LandingError::Transition);
        };
        let (_, state) = decode_row(id, (body, state, repository, target))?;
        match state {
            IntentState::Sending if recorded.is_none() => {
                if tx.execute(
                    "UPDATE landing_intents_v1 SET state='acknowledged',receipt_digest=?2 WHERE id=?1 AND state='sending'",
                    params![id, digest],
                )? != 1 { return Err(LandingError::Transition); }
            }
            IntentState::Acknowledged if recorded.as_deref() == Some(&digest) => {}
            _ => return Err(LandingError::Transition),
        }
        tx.commit()?;
        Ok(())
    }
}

// These are local scheduling observations, not a proof that the provider or
// workflow is trustworthy. Every read is inside the caller's write transaction.
fn observed_plan(
    tx: &rusqlite::Transaction<'_>,
    operation_id: &str,
    batch_id: &str,
    repository_node_id: &str,
) -> Result<LandingPlan, LandingError> {
    use crate::batch::State;
    use crate::verification::CiObservation;
    let (batch, _) = crate::store::checked_batch(tx, batch_id)?;
    let integration = match &batch.state {
        State::Testing { integration_sha } => integration_sha,
        _ => return Err(LandingError::StaleBatch),
    };
    let binding: (i64, i64, String) = tx.query_row(
        "SELECT installation,repository,policy_digest FROM repository_binding WHERE id=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    let base: Option<String> = tx
        .query_row("SELECT sha FROM observed_base WHERE id=1", [], |r| r.get(0))
        .optional()?;
    let target: Option<String> = tx
        .query_row("SELECT branch FROM observed_target WHERE id=1", [], |r| {
            r.get(0)
        })
        .optional()?;
    let target = target.ok_or(LandingError::StaleBatch)?;
    if base.as_deref() != Some(batch.base_sha.as_str()) {
        return Err(LandingError::StaleBatch);
    }
    let policy: Option<String> = tx
        .query_row(
            "SELECT digest FROM batch_policy WHERE batch_id=?1",
            [batch_id],
            |r| r.get(0),
        )
        .optional()?;
    let rules_row: Option<(String, String)> = tx
        .query_row(
            "SELECT body,digest FROM dispatch_branch_rules WHERE id=?1",
            [batch_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let (rules_body, rules_digest) = rules_row.ok_or(LandingError::StaleBatch)?;
    let rules: crate::github::BranchRulesSnapshot = serde_json::from_str(&rules_body)?;
    if i64::try_from(rules.repository_id).ok() != Some(binding.1)
        || rules.branch != target
        || rules.digest().ok().as_deref() != Some(rules_digest.as_str())
    {
        return Err(LandingError::StaleBatch);
    }
    let row: Option<(String,String,Option<String>,String,String,String)> = tx.query_row(
        "SELECT c.body,d.plan,d.run_id,w.path,s.source,d.state FROM ci_observations c JOIN dispatch_batches b ON b.id=c.id AND b.batch_id=?1 JOIN dispatch_intents d ON d.id=c.id JOIN dispatch_workflows w ON w.id=c.id JOIN dispatch_receipt_sources s ON s.id=c.id WHERE c.id=?1", [batch_id],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)),
    ).optional()?;
    let (ci_body, dispatch_body, run_id, workflow, source, state) =
        row.ok_or(LandingError::StaleBatch)?;
    let conflicted: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ci_observation_conflicts WHERE id=?1)",
        [batch_id],
        |r| r.get(0),
    )?;
    let record: crate::scheduler::CiObservationRecord = serde_json::from_str(&ci_body)?;
    let dispatch: crate::scheduler::DispatchPlan = serde_json::from_str(&dispatch_body)?;
    let snapshot = record.snapshot.as_ref().ok_or(LandingError::StaleBatch)?;
    let runner = record.runner_id.ok_or(LandingError::StaleBatch)?;
    let paths: Option<String> = tx
        .query_row(
            "SELECT paths FROM batch_policy_paths WHERE batch_id=?1",
            [batch_id],
            |r| r.get(0),
        )
        .optional()?;
    let paths: Vec<String> = serde_json::from_str(&paths.ok_or(LandingError::StaleBatch)?)?;
    let selection: Option<String> = tx
        .query_row(
            "SELECT body FROM batch_selection WHERE batch_id=?1",
            [batch_id],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(digest) = &dispatch.selection_policy_digest {
        let selected: crate::selective::FrozenSelection =
            serde_json::from_str(selection.as_deref().ok_or(LandingError::StaleBatch)?)?;
        if &selected.policy_digest != digest
            || selected.base_sha != batch.base_sha
            || selected.integration_sha != *integration
            || selected.selection.suites() != &dispatch.suites
        {
            return Err(LandingError::StaleBatch);
        }
    } else if selection.is_some() || dispatch.suites.len() != 1 || !dispatch.suites.contains("full")
    {
        return Err(LandingError::StaleBatch);
    }
    if conflicted
        || source != "direct"
        || state != "observed"
        || record.plan != dispatch
        || i64::try_from(dispatch.installation_id).ok() != Some(binding.0)
        || i64::try_from(dispatch.repository_id).ok() != Some(binding.1)
        || dispatch.integration_sha != *integration
        || dispatch.staging_ref != format!("refs/heads/sluss/{batch_id}")
        || policy.as_deref() != Some(dispatch.policy_digest.as_str())
        || record.workflow_path.as_deref() != Some(workflow.as_str())
        || !paths.contains(&workflow)
        || !crate::github::workflow_path_matches(&workflow, &snapshot.run.path)
        || run_id.as_deref().and_then(|v| v.parse::<u64>().ok()) != Some(record.run_id)
        || !snapshot.jobs.iter().any(|j| j.runner_id == Some(runner))
        || record.observation != CiObservation::Successful
        || crate::verification::observe_ci(&dispatch, record.run_id, snapshot).ok()
            != Some(CiObservation::Successful)
    {
        return Err(LandingError::StaleBatch);
    }
    let mut admission = Vec::new();
    for candidate in &batch.candidates {
        let digest: String = tx.query_row(
            "SELECT digest FROM batch_evidence WHERE batch_id=?1 AND number=?2",
            params![batch_id, candidate.number as i64],
            |r| r.get(0),
        )?;
        admission.push((candidate.number, candidate.head_sha.clone(), digest));
    }
    let admission_body = serde_json::to_vec(&(binding.2, &target, &batch.base_sha, admission))?;
    let ci_digest_body =
        serde_json::to_vec(&(ci_body, dispatch_body, workflow, source, selection, paths))?;
    let plan = LandingPlan {
        operation_id: operation_id.into(),
        batch_id: batch_id.into(),
        installation_id: binding.0,
        repository_id: binding.1,
        repository_node_id: repository_node_id.into(),
        target_ref: format!("refs/heads/{target}"),
        staging_ref: dispatch.staging_ref,
        base_sha: batch.base_sha,
        staged_sha: integration.clone(),
        policy_digest: dispatch.policy_digest,
        admission_digest: hex::encode(Sha256::digest(admission_body)),
        ci_evidence_digest: hex::encode(Sha256::digest(ci_digest_body)),
        branch_rules_digest: Some(rules_digest),
    };
    plan.validate()?;
    Ok(plan)
}

fn decode_row(
    id: &str,
    (body, state, repository, target): (String, String, i64, String),
) -> Result<(LandingPlan, IntentState), LandingError> {
    let plan: LandingPlan = serde_json::from_str(&body)?;
    plan.validate().map_err(|_| LandingError::Corrupt)?;
    if plan.operation_id != id || plan.repository_id != repository || plan.target_ref != target {
        return Err(LandingError::Corrupt);
    }
    Ok((plan, parse_state(&state)?))
}
fn parse_state(value: &str) -> Result<IntentState, LandingError> {
    match value {
        "prepared" => Ok(IntentState::Prepared),
        "sending" => Ok(IntentState::Sending),
        "acknowledged" => Ok(IntentState::Acknowledged),
        "cancelled" => Ok(IntentState::Cancelled),
        _ => Err(LandingError::Corrupt),
    }
}

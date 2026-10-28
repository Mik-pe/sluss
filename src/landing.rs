//! Experimental durable intent for an eventual GitHub ref update.
//!
//! This module does not contact GitHub, authenticate the supplied evidence,
//! establish fast-forward ancestry, revalidate reviews or authorize landing.
//! The CLI exposes read-only provider observations, but neither the CLI nor
//! service sends landing requests. All callers and stored inputs must be trusted. In particular, a GraphQL clientMutationId is correlation,
//! not an idempotency key or proof that a provider performed an operation.
use std::{collections::BTreeSet, path::Path, time::Duration};

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
    /// Present only when an independently supplied operator policy is bound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trusted_ci_policy_digest: Option<String>,
    /// Missing on historical or purely local intents; not a bypass proof.
    #[serde(default)]
    pub branch_rules_digest: Option<String>,
    /// Missing unless complete visible protection details were frozen.
    #[serde(default)]
    pub branch_security_digest: Option<String>,
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
    #[error("operator CI trust policy is missing, changed or inconsistent with the recorded run")]
    TrustPolicy,
    #[error("could not read operator CI trust policy")]
    TrustPolicyRead,
    #[error("invalid operator CI trust policy TOML")]
    TrustPolicyFormat,
}

/// An operator-reviewed allowlist. Matching it is necessary but does not prove
/// isolation, complete dependencies or trustworthy workflow semantics.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustedCiPolicy {
    pub version: u32,
    pub installation_id: u64,
    pub repository_id: u64,
    pub repository_node_id: String,
    pub target_ref: String,
    pub repository_policy_digest: String,
    pub policy_digest: String,
    pub workflow_id: u64,
    pub workflow_path: String,
    pub required_jobs: BTreeSet<String>,
    pub suites: BTreeSet<String>,
    pub selection_policy_digest: Option<String>,
    pub runner_ids: BTreeSet<u64>,
}
impl TrustedCiPolicy {
    /// Read an operator-owned file. Matching a file does not establish its trust.
    pub fn load(path: &Path) -> Result<Self, LandingError> {
        use std::io::Read;
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .map_err(|_| LandingError::TrustPolicyRead)?
            .take(65_537)
            .read_to_end(&mut bytes)
            .map_err(|_| LandingError::TrustPolicyRead)?;
        if bytes.len() > 65_536 {
            return Err(LandingError::TrustPolicyFormat);
        }
        let text = std::str::from_utf8(&bytes).map_err(|_| LandingError::TrustPolicyFormat)?;
        let policy: Self = toml::from_str(text).map_err(|_| LandingError::TrustPolicyFormat)?;
        policy.digest()?;
        Ok(policy)
    }
    pub fn digest(&self) -> Result<String, LandingError> {
        if self.version != 1
            || self.installation_id == 0
            || self.repository_id == 0
            || !node_id(&self.repository_node_id)
            || !reference(&self.target_ref)
            || !hex(&self.repository_policy_digest, 64)
            || !hex(&self.policy_digest, 64)
            || self.workflow_id == 0
            || !crate::integration::valid_policy_path(&self.workflow_path)
            || self.required_jobs.is_empty()
            || self.required_jobs.len() > 100
            || self
                .required_jobs
                .iter()
                .any(|j| j.is_empty() || j.len() > 256)
            || self.suites.is_empty()
            || self.suites.len() > 100
            || self.suites.iter().any(|s| s.is_empty() || s.len() > 128)
            || self
                .selection_policy_digest
                .as_deref()
                .is_some_and(|s| !hex(s, 64))
            || self.runner_ids.is_empty()
            || self.runner_ids.len() > 100
            || self.runner_ids.contains(&0)
        {
            return Err(LandingError::TrustPolicy);
        }
        let mut hash = Sha256::new();
        hash.update(b"sluss-trusted-ci-policy-v1\0");
        hash.update(serde_json::to_vec(self)?);
        Ok(hex::encode(hash.finalize()))
    }
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
                .trusted_ci_policy_digest
                .as_deref()
                .is_some_and(|value| !hex(value, 64))
            || self
                .branch_rules_digest
                .as_deref()
                .is_some_and(|value| !hex(value, 64))
            || self
                .branch_security_digest
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

/// Historical provider observations made after a sender became uncertain.
/// They do not acknowledge a request or authorize another write.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LandingRecoveryObservation {
    pub plan: LandingPlan,
    pub target_sha: String,
    pub staging_sha: String,
    pub observed_ms: u64,
}
impl LandingRecoveryObservation {
    fn validate(&self) -> Result<(), LandingError> {
        self.plan.validate()?;
        if !hex(&self.target_sha, 40) || !hex(&self.staging_sha, 40) || self.observed_ms == 0 {
            return Err(LandingError::Corrupt);
        }
        Ok(())
    }
    pub fn target_position(&self) -> &'static str {
        if self.target_sha == self.plan.base_sha {
            "base"
        } else if self.target_sha == self.plan.staged_sha {
            "staged"
        } else {
            "other"
        }
    }
}
fn recovery_digest(body: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"sluss-landing-recovery-v1\0");
    hash.update(body.as_bytes());
    hex::encode(hash.finalize())
}

/// A provider-observed candidate for a future CI attestation. Its stored
/// digest is an integrity check, not a signature or merge authorization.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CiEvidenceCandidate {
    pub plan: LandingPlan,
    pub policy: TrustedCiPolicy,
    pub snapshot: crate::github::ActionsSnapshot,
    pub app_id: u64,
    pub app_node_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CiEvidenceRecord {
    pub candidate: CiEvidenceCandidate,
    pub digest: String,
    pub first_observed_ms: u64,
}
fn ci_candidate_digest(body: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"sluss-ci-evidence-candidate-v1\0");
    hash.update(body.as_bytes());
    hex::encode(hash.finalize())
}
fn validate_ci_candidate(candidate: &CiEvidenceCandidate) -> Result<(), LandingError> {
    candidate.plan.validate()?;
    if candidate.app_id == 0
        || !node_id(&candidate.app_node_id)
        || candidate.plan.trusted_ci_policy_digest.as_deref()
            != Some(candidate.policy.digest()?.as_str())
        || candidate.snapshot.run.id == 0
    {
        return Err(LandingError::Corrupt);
    }
    Ok(())
}

pub struct LandingJournal {
    conn: Connection,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RefreshTarget {
    pub plan: LandingPlan,
    pub run_id: u64,
    pub members: Vec<crate::store::AdmissionCandidate>,
    pub repository_policy_digest: String,
    pub snapshot: crate::github::ActionsSnapshot,
}
impl LandingJournal {
    /// Open an existing database without creating or modifying its schema.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self, LandingError> {
        let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        Ok(Self { conn })
    }
    pub(crate) fn preview_target(
        &mut self,
        batch_id: &str,
        policy: &TrustedCiPolicy,
    ) -> Result<RefreshTarget, LandingError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        let plan = observed_plan(&tx, "preview-policy", batch_id, &policy.repository_node_id)?;
        verify_trusted_policy(&tx, &plan, policy)?;
        let target = refresh_target_details(&tx, plan)?;
        tx.commit()?;
        Ok(target)
    }
    pub(crate) fn verify_preview(
        &mut self,
        expected: &RefreshTarget,
        policy: &TrustedCiPolicy,
        snapshot: &crate::github::ActionsSnapshot,
    ) -> Result<LandingPlan, LandingError> {
        let current = self.preview_target(&expected.plan.batch_id, policy)?;
        let mut fresh = snapshot.clone();
        fresh.jobs.sort_by_key(|job| job.id);
        if current != *expected || current.snapshot != fresh {
            return Err(LandingError::StaleBatch);
        }
        Ok(current.plan)
    }
    /// Append an independently provider-refreshed candidate only after the
    /// local batch, policy, admission and recorded run still agree in one
    /// transaction. No GitHub check or landing intent is created.
    pub(crate) fn record_ci_candidate(
        &mut self,
        expected: &RefreshTarget,
        policy: &TrustedCiPolicy,
        snapshot: &crate::github::ActionsSnapshot,
        app: &crate::github::AppIdentity,
        observed_ms: u64,
    ) -> Result<CiEvidenceRecord, LandingError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let plan = observed_plan(
            &tx,
            &expected.plan.operation_id,
            &expected.plan.batch_id,
            &policy.repository_node_id,
        )?;
        verify_trusted_policy(&tx, &plan, policy)?;
        let current = refresh_target_details(&tx, plan)?;
        let mut normalized = snapshot.clone();
        normalized.jobs.sort_by_key(|job| job.id);
        if current != *expected || current.snapshot != normalized {
            return Err(LandingError::StaleBatch);
        }
        let mut bound_plan = current.plan;
        let policy_digest = policy.digest()?;
        bound_plan.trusted_ci_policy_digest = Some(policy_digest.clone());
        let candidate = CiEvidenceCandidate {
            plan: bound_plan,
            policy: policy.clone(),
            snapshot: normalized,
            app_id: app.id,
            app_node_id: app.node_id.clone(),
        };
        validate_ci_candidate(&candidate)?;
        let body = serde_json::to_string(&candidate)?;
        if body.len() > 8_000_000 || observed_ms == 0 {
            return Err(LandingError::Corrupt);
        }
        let digest = ci_candidate_digest(&body);
        let observed_ms = i64::try_from(observed_ms).map_err(|_| LandingError::Corrupt)?;
        tx.execute("INSERT OR IGNORE INTO ci_evidence_candidates_v1(batch_id,policy_digest,body,digest,first_observed_ms) VALUES(?1,?2,?3,?4,?5)", params![candidate.plan.batch_id,policy_digest,body,digest,observed_ms])?;
        let (saved, saved_policy, first): (String,String,i64) = tx.query_row("SELECT body,policy_digest,first_observed_ms FROM ci_evidence_candidates_v1 WHERE batch_id=?1 AND digest=?2", params![candidate.plan.batch_id,digest], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
        if saved != body || saved_policy != policy.digest()? || first <= 0 {
            return Err(LandingError::Corrupt);
        }
        tx.execute("INSERT INTO ci_evidence_candidate_heads_v1(batch_id,policy_digest,digest) VALUES(?1,?2,?3) ON CONFLICT(batch_id,policy_digest) DO UPDATE SET digest=excluded.digest", params![candidate.plan.batch_id,policy_digest,digest])?;
        tx.commit()?;
        Ok(CiEvidenceRecord {
            candidate,
            digest,
            first_observed_ms: first as u64,
        })
    }

    /// Inspect the saved candidate under the current local queue and policy.
    /// Provider state may have changed since it was recorded.
    pub fn inspect_ci_candidate(
        &mut self,
        batch_id: &str,
        policy: &TrustedCiPolicy,
    ) -> Result<Option<CiEvidenceRecord>, LandingError> {
        let policy_digest = policy.digest()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='ci_evidence_candidate_heads_v1')", [], |r| r.get(0))?;
        if !exists {
            return Ok(None);
        }
        let head: Option<String> = tx.query_row("SELECT digest FROM ci_evidence_candidate_heads_v1 WHERE batch_id=?1 AND policy_digest=?2", params![batch_id,policy_digest], |r| r.get(0)).optional()?;
        let Some(head) = head else { return Ok(None) };
        let row: Option<(String,String,i64)> = tx.query_row("SELECT body,digest,first_observed_ms FROM ci_evidence_candidates_v1 WHERE batch_id=?1 AND policy_digest=?2 AND digest=?3", params![batch_id,policy_digest,head], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let (body, digest, first) = row.ok_or(LandingError::Corrupt)?;
        if body.len() > 8_000_000 || ci_candidate_digest(&body) != digest || first <= 0 {
            return Err(LandingError::Corrupt);
        }
        let candidate: CiEvidenceCandidate = serde_json::from_str(&body)?;
        validate_ci_candidate(&candidate)?;
        if candidate.plan.batch_id != batch_id || candidate.policy != *policy {
            return Err(LandingError::Corrupt);
        }
        let mut plan = observed_plan(
            &tx,
            &candidate.plan.operation_id,
            batch_id,
            &policy.repository_node_id,
        )?;
        verify_trusted_policy(&tx, &plan, policy)?;
        let target = refresh_target_details(&tx, plan.clone())?;
        plan.trusted_ci_policy_digest = Some(policy_digest);
        if candidate.plan != plan || candidate.snapshot != target.snapshot {
            return Err(LandingError::StaleBatch);
        }
        tx.commit()?;
        Ok(Some(CiEvidenceRecord {
            candidate,
            digest,
            first_observed_ms: first as u64,
        }))
    }

    /// Compare a frozen local batch to an operator policy without creating a
    /// journal intent or querying the provider. The database is opened read-only.
    pub fn inspect_trusted_batch(
        path: impl AsRef<Path>,
        batch_id: &str,
        policy: &TrustedCiPolicy,
    ) -> Result<LandingPlan, LandingError> {
        let mut conn =
            Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let plan = observed_plan(&tx, "inspect-policy", batch_id, &policy.repository_node_id)?;
        verify_trusted_policy(&tx, &plan, policy)?;
        tx.commit()?;
        Ok(plan)
    }
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
        CREATE TABLE IF NOT EXISTS landing_recovery_observations_v1 (sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL, body TEXT NOT NULL, digest TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS landing_recovery_by_id_v1 ON landing_recovery_observations_v1(id,sequence);
        CREATE TABLE IF NOT EXISTS ci_evidence_candidates_v1 (batch_id TEXT NOT NULL, policy_digest TEXT NOT NULL, body TEXT NOT NULL, digest TEXT NOT NULL, first_observed_ms INTEGER NOT NULL, PRIMARY KEY(batch_id,digest));
        CREATE TABLE IF NOT EXISTS ci_evidence_candidate_heads_v1 (batch_id TEXT NOT NULL, policy_digest TEXT NOT NULL, digest TEXT NOT NULL, PRIMARY KEY(batch_id,policy_digest));
        CREATE TABLE IF NOT EXISTS landing_batch_bindings_v1 (id TEXT PRIMARY KEY NOT NULL);
        CREATE TABLE IF NOT EXISTS landing_trust_bindings_v1 (id TEXT PRIMARY KEY NOT NULL, body TEXT NOT NULL, digest TEXT NOT NULL);",
        )?;
        Ok(Self { conn })
    }
    pub fn prepare(&mut self, plan: &LandingPlan) -> Result<IntentState, LandingError> {
        plan.validate()?;
        if plan.trusted_ci_policy_digest.is_some() {
            return Err(LandingError::TrustPolicy);
        }
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
    /// Retrieve the last historical observation and detect corrupted or
    /// rebound intent data. This does not change the unresolved intent.
    pub fn last_recovery_observation(
        &self,
        id: &str,
    ) -> Result<Option<LandingRecoveryObservation>, LandingError> {
        let exists: bool = self.conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='landing_recovery_observations_v1')", [], |r| r.get(0))?;
        if !exists {
            return Ok(None);
        }
        let row: Option<(String, String)> = self.conn.query_row(
            "SELECT body,digest FROM landing_recovery_observations_v1 WHERE id=?1 ORDER BY sequence DESC LIMIT 1", [id],
            |r| Ok((r.get(0)?,r.get(1)?)),
        ).optional()?;
        let Some((body, digest)) = row else {
            return Ok(None);
        };
        if body.len() > 65536 || recovery_digest(&body) != digest {
            return Err(LandingError::Corrupt);
        }
        let observation: LandingRecoveryObservation = serde_json::from_str(&body)?;
        observation.validate()?;
        let (plan, _) = self.get(id)?.ok_or(LandingError::Corrupt)?;
        if observation.plan != plan {
            return Err(LandingError::Corrupt);
        }
        Ok(Some(observation))
    }

    /// Persist a provider-fetched snapshot only if the same intent is still
    /// sending. Keep its target lock and prohibit retransmission.
    pub(crate) fn record_recovery_observation(
        &mut self,
        observation: &LandingRecoveryObservation,
    ) -> Result<(), LandingError> {
        observation.validate()?;
        let body = serde_json::to_string(observation)?;
        if body.len() > 65536 {
            return Err(LandingError::Corrupt);
        }
        let digest = recovery_digest(&body);
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: Option<(String, String, i64, String)> = tx
            .query_row(
                "SELECT plan,state,repository_id,target_ref FROM landing_intents_v1 WHERE id=?1",
                [&observation.plan.operation_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        let (plan, state) = decode_row(
            &observation.plan.operation_id,
            row.ok_or(LandingError::Transition)?,
        )?;
        if plan != observation.plan || state != IntentState::Sending {
            return Err(LandingError::Transition);
        }
        tx.execute(
            "INSERT INTO landing_recovery_observations_v1(id,body,digest) VALUES(?1,?2,?3)",
            params![plan.operation_id, body, digest],
        )?;
        tx.commit()?;
        Ok(())
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
        let trust_bound: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM landing_trust_bindings_v1 WHERE id=?1)",
            [id],
            |r| r.get(0),
        )?;
        if trust_bound {
            return Err(LandingError::TrustPolicy);
        }
        let (plan, state) = decode_row(id, row)?;
        if plan.trusted_ci_policy_digest.is_some() {
            return Err(LandingError::TrustPolicy);
        }
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
        self.prepare_observed_batch_inner(operation_id, batch_id, repository_node_id, None)
    }
    /// Bind an independently supplied operator policy atomically with the
    /// observed batch. This is a trust prerequisite, not merge authorization.
    pub fn prepare_trusted_batch(
        &mut self,
        operation_id: &str,
        batch_id: &str,
        policy: &TrustedCiPolicy,
    ) -> Result<LandingPlan, LandingError> {
        self.prepare_observed_batch_inner(
            operation_id,
            batch_id,
            &policy.repository_node_id,
            Some(policy),
        )
    }
    fn prepare_observed_batch_inner(
        &mut self,
        operation_id: &str,
        batch_id: &str,
        repository_node_id: &str,
        policy: Option<&TrustedCiPolicy>,
    ) -> Result<LandingPlan, LandingError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut plan = observed_plan(&tx, operation_id, batch_id, repository_node_id)?;
        if let Some(policy) = policy {
            verify_trusted_policy(&tx, &plan, policy)?;
            plan.trusted_ci_policy_digest = Some(policy.digest()?);
        }
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
            if !bound {
                return Err(LandingError::IdConflict);
            }
            if old.trusted_ci_policy_digest != plan.trusted_ci_policy_digest {
                return Err(LandingError::TrustPolicy);
            }
            if old != plan {
                return Err(LandingError::IdConflict);
            }
            if state != IntentState::Prepared {
                return Err(LandingError::Transition);
            }
            verify_trust_binding(&tx, &plan, policy)?;
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
            if let Some(policy) = policy {
                tx.execute(
                    "INSERT INTO landing_trust_bindings_v1(id,body,digest) VALUES(?1,?2,?3)",
                    params![
                        operation_id,
                        serde_json::to_string(policy)?,
                        policy.digest()?
                    ],
                )?;
            }
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
        self.claim_observed_batch_inner(operation_id, None, None)
    }

    pub(crate) fn claim_with_fresh_snapshot(
        &mut self,
        operation_id: &str,
        snapshot: &crate::github::ActionsSnapshot,
        policy: &TrustedCiPolicy,
    ) -> Result<Option<LandingPlan>, LandingError> {
        self.claim_observed_batch_inner(operation_id, Some(snapshot), Some(policy))
    }

    fn claim_observed_batch_inner(
        &mut self,
        operation_id: &str,
        snapshot: Option<&crate::github::ActionsSnapshot>,
        policy: Option<&TrustedCiPolicy>,
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
        let mut now = observed_plan(&tx, operation_id, &plan.batch_id, &plan.repository_node_id)?;
        now.trusted_ci_policy_digest = plan.trusted_ci_policy_digest.clone();
        if now != plan {
            return Err(LandingError::StaleBatch);
        }
        verify_trust_binding(&tx, &plan, policy)?;
        if let Some(policy) = policy {
            verify_trusted_policy(&tx, &plan, policy)?;
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
        policy: &TrustedCiPolicy,
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
        let mut now = observed_plan(&tx, operation_id, &plan.batch_id, &plan.repository_node_id)?;
        now.trusted_ci_policy_digest = plan.trusted_ci_policy_digest.clone();
        if now != plan {
            return Err(LandingError::StaleBatch);
        }
        verify_trust_binding(&tx, &plan, Some(policy))?;
        verify_trusted_policy(&tx, &plan, policy)?;
        let target = refresh_target_details(&tx, plan)?;
        tx.commit()?;
        Ok(target)
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
        let (plan, state) = decode_row(id, (body, state, repository, target))?;
        if plan.trusted_ci_policy_digest.is_some() {
            let body: Option<String> = tx
                .query_row(
                    "SELECT body FROM landing_trust_bindings_v1 WHERE id=?1",
                    [id],
                    |r| r.get(0),
                )
                .optional()?;
            let policy: TrustedCiPolicy =
                serde_json::from_str(&body.ok_or(LandingError::TrustPolicy)?)
                    .map_err(|_| LandingError::TrustPolicy)?;
            verify_trust_binding(&tx, &plan, Some(&policy))?;
        } else {
            verify_trust_binding(&tx, &plan, None)?;
        }
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

fn refresh_target_details(
    tx: &rusqlite::Transaction<'_>,
    plan: LandingPlan,
) -> Result<RefreshTarget, LandingError> {
    let body: String = tx.query_row(
        "SELECT body FROM ci_observations WHERE id=?1",
        [&plan.batch_id],
        |r| r.get(0),
    )?;
    let saved: crate::scheduler::CiObservationRecord = serde_json::from_str(&body)?;
    let (batch, _) = crate::store::checked_batch(tx, &plan.batch_id)?;
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
    Ok(RefreshTarget {
        plan,
        run_id: saved.run_id,
        members,
        repository_policy_digest,
        snapshot: saved.snapshot.ok_or(LandingError::StaleBatch)?,
    })
}
fn verify_trust_binding(
    tx: &rusqlite::Transaction<'_>,
    plan: &LandingPlan,
    policy: Option<&TrustedCiPolicy>,
) -> Result<(), LandingError> {
    let row: Option<(String, String)> = tx
        .query_row(
            "SELECT body,digest FROM landing_trust_bindings_v1 WHERE id=?1",
            [&plan.operation_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    match (row, policy, plan.trusted_ci_policy_digest.as_deref()) {
        (None, None, None) => Ok(()),
        (Some((body, digest)), Some(policy), Some(expected)) => {
            let saved: TrustedCiPolicy =
                serde_json::from_str(&body).map_err(|_| LandingError::TrustPolicy)?;
            if saved != *policy
                || saved.digest()? != digest
                || policy.digest()? != digest
                || digest != expected
            {
                return Err(LandingError::TrustPolicy);
            }
            Ok(())
        }
        _ => Err(LandingError::TrustPolicy),
    }
}
fn verify_trusted_policy(
    tx: &rusqlite::Transaction<'_>,
    plan: &LandingPlan,
    policy: &TrustedCiPolicy,
) -> Result<(), LandingError> {
    policy.digest()?;
    let repo_policy: String = tx.query_row(
        "SELECT policy_digest FROM repository_binding WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    let body: String = tx.query_row(
        "SELECT body FROM ci_observations WHERE id=?1",
        [&plan.batch_id],
        |r| r.get(0),
    )?;
    let record: crate::scheduler::CiObservationRecord = serde_json::from_str(&body)?;
    let snapshot = record.snapshot.as_ref().ok_or(LandingError::TrustPolicy)?;
    let placement = crate::verification::check_host_placement(snapshot, &policy.runner_ids);
    if i64::try_from(policy.installation_id).ok() != Some(plan.installation_id)
        || i64::try_from(policy.repository_id).ok() != Some(plan.repository_id)
        || policy.repository_node_id != plan.repository_node_id
        || policy.target_ref != plan.target_ref
        || policy.repository_policy_digest != repo_policy
        || policy.policy_digest != plan.policy_digest
        || record.plan.installation_id != policy.installation_id
        || record.plan.repository_id != policy.repository_id
        || record.plan.workflow_id != policy.workflow_id
        || record.workflow_path.as_deref() != Some(policy.workflow_path.as_str())
        || record.plan.required_jobs != policy.required_jobs
        || record.plan.suites != policy.suites
        || record.plan.selection_policy_digest != policy.selection_policy_digest
        || !matches!(placement, Ok(crate::verification::Placement::Confirmed { runner_id }) if Some(runner_id) == record.runner_id)
    {
        return Err(LandingError::TrustPolicy);
    }
    Ok(())
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
    let (integration, completed_run) = match &batch.state {
        State::Testing { integration_sha } => (integration_sha, None),
        State::CiComplete {
            integration_sha,
            run_id,
            conclusion,
        } if conclusion == "success" => (integration_sha, Some(*run_id)),
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
    let security_row: Option<(String, String)> = tx
        .query_row(
            "SELECT body,digest FROM dispatch_branch_security WHERE id=?1",
            [batch_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let (security_body, security_digest) = security_row.ok_or(LandingError::StaleBatch)?;
    let security: crate::github::BranchSecurityObservation = serde_json::from_str(&security_body)?;
    if security.rules != rules
        || security.branch.commit.sha != batch.base_sha
        || security.digest().ok().as_deref() != Some(security_digest.as_str())
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
    let receipt_conflict: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM dispatch_receipt_conflicts WHERE id=?1)",
        [batch_id],
        |r| r.get(0),
    )?;
    let (reserved, finished): (bool, bool) = tx.query_row("SELECT EXISTS(SELECT 1 FROM capacity_reservations WHERE id=?1), EXISTS(SELECT 1 FROM capacity_finished WHERE id=?1)", [batch_id], |r| Ok((r.get(0)?,r.get(1)?)))?;
    let lifecycle_valid = match (completed_run, state.as_str(), reserved, finished) {
        (None, "observed", true, false) => true,
        (Some(run), "observed", true, false) | (Some(run), "finished", false, true) => {
            run == record.run_id
        }
        _ => false,
    };
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
        || receipt_conflict
        || !lifecycle_valid
        || source != "direct"
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
        trusted_ci_policy_digest: None,
        branch_rules_digest: Some(rules_digest),
        branch_security_digest: Some(security_digest),
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

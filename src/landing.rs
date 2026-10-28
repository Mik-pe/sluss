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
        ON landing_intents_v1(repository_id,target_ref) WHERE state IN ('prepared','sending');",
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

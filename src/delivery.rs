//! Provider-observed PR deliveries for accounting, never merge authorization.
use crate::{reconcile::RepositoryPolicy, store::Store};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DeliveryError {
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Store(#[from] crate::store::StoreError),
    #[error("repository, target or admission policy does not match the queue")]
    Scope,
    #[error("delivery observation became stale during provider reads")]
    Stale,
    #[error("invalid or inconsistent delivery observation")]
    Invalid,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryObservation {
    NotMerged,
    Merged {
        merged_ms: i64,
        head_sha: Option<String>,
    },
}
#[derive(Clone, Debug)]
pub(crate) struct DeliveryClaim {
    pub number: u64,
    revision: i64,
    event_revision: i64,
    policy_revision: i64,
}
pub(crate) struct DeliveryJournal {
    conn: Connection,
}
impl DeliveryJournal {
    pub fn open(path: &Path) -> Result<Self, DeliveryError> {
        Store::open(path)?;
        crate::webhook::Inbox::open(path)?;
        let conn = Connection::open(path)?;
        crate::database::configure_writer(&conn)?;
        Ok(Self { conn })
    }
    /// Advance the cursor before reads; a crash retries after the next sweep.
    pub fn next(
        &mut self,
        policy: &RepositoryPolicy,
        digest: &str,
    ) -> Result<Option<DeliveryClaim>, DeliveryError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let policy_revision = check_scope(&tx, policy, digest)?;
        if pending(&tx, policy)? {
            return Ok(None);
        }
        tx.execute(
            "INSERT OR IGNORE INTO ci_delivery_cursor(id,after_number) VALUES(1,0)",
            [],
        )?;
        let after: i64 = tx.query_row(
            "SELECT after_number FROM ci_delivery_cursor WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        let number: Option<i64> = tx.query_row("SELECT DISTINCT a.number FROM admission_observations_v1 a WHERE a.number>?1 AND a.ended_ms IS NOT NULL AND NOT EXISTS(SELECT 1 FROM candidates c WHERE c.number=a.number) ORDER BY a.number LIMIT 1", [after], |r| r.get(0)).optional()?;
        let Some(number) = number else {
            tx.execute(
                "UPDATE ci_delivery_cursor SET after_number=0 WHERE id=1",
                [],
            )?;
            tx.commit()?;
            return Ok(None);
        };
        if number <= 0 {
            return Err(DeliveryError::Invalid);
        }
        let old: Option<i64> = tx
            .query_row(
                "SELECT revision FROM ci_delivery_claims WHERE number=?1",
                [number],
                |r| r.get(0),
            )
            .optional()?;
        let revision = match old {
            None => 1,
            Some(v) if v > 0 => v.checked_add(1).ok_or(DeliveryError::Invalid)?,
            _ => return Err(DeliveryError::Invalid),
        };
        tx.execute("INSERT INTO ci_delivery_claims(number,revision) VALUES(?1,?2) ON CONFLICT(number) DO UPDATE SET revision=excluded.revision", params![number,revision])?;
        tx.execute(
            "UPDATE ci_delivery_cursor SET after_number=?1 WHERE id=1",
            [number],
        )?;
        let event_revision = event_revision(&tx)?;
        tx.commit()?;
        Ok(Some(DeliveryClaim {
            number: number as u64,
            revision,
            event_revision,
            policy_revision,
        }))
    }
    /// Record only a still-current observation; conflicting historical provider
    /// results are retained but excluded from aggregate delivery metrics.
    pub fn record(
        &mut self,
        claim: &DeliveryClaim,
        policy: &RepositoryPolicy,
        digest: &str,
        observation: &DeliveryObservation,
    ) -> Result<bool, DeliveryError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if check_scope(&tx, policy, digest)? != claim.policy_revision
            || event_revision(&tx)? != claim.event_revision
            || pending(&tx, policy)?
        {
            return Err(DeliveryError::Stale);
        }
        let revision: Option<i64> = tx
            .query_row(
                "SELECT revision FROM ci_delivery_claims WHERE number=?1",
                [claim.number as i64],
                |r| r.get(0),
            )
            .optional()?;
        if revision != Some(claim.revision) {
            return Err(DeliveryError::Stale);
        }
        let old: Option<(i64, Option<String>)> = tx
            .query_row(
                "SELECT merged_ms,head_sha FROM ci_delivery_records WHERE number=?1",
                [claim.number as i64],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let mut inserted = false;
        match observation {
            DeliveryObservation::NotMerged => {
                if old.is_some() {
                    tx.execute(
                        "UPDATE ci_delivery_records SET conflicted=1 WHERE number=?1",
                        [claim.number as i64],
                    )?;
                }
            }
            DeliveryObservation::Merged {
                merged_ms,
                head_sha,
            } => {
                if *merged_ms <= 0 || head_sha.as_ref().is_some_and(|s| !valid_sha(s)) {
                    return Err(DeliveryError::Invalid);
                }
                if let Some((prior, head)) = old {
                    if prior != *merged_ms || head != *head_sha {
                        tx.execute(
                            "UPDATE ci_delivery_records SET conflicted=1 WHERE number=?1",
                            [claim.number as i64],
                        )?;
                    }
                } else {
                    let previously_observed: bool = tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM admission_observations_v1 WHERE number=?1 AND clock_uncertain=0 AND first_observed_ms>=0 AND first_observed_ms<?2)",
                        params![claim.number as i64, merged_ms],
                        |r| r.get(0),
                    )?;
                    if !previously_observed {
                        tx.commit()?;
                        return Ok(false);
                    }
                    let epoch_scan: i64 = tx.query_row(
                        "SELECT COALESCE(MAX(scan_revision),0) FROM admission_policy_migrations",
                        [],
                        |r| r.get(0),
                    )?;
                    let mut observed = Vec::new();
                    if let Some(head) = head_sha {
                        let mut stmt = tx.prepare("SELECT first_observed_ms FROM admission_observations_v1 WHERE number=?1 AND head_sha=?2 AND clock_uncertain=0 AND first_revision>=?3 AND first_observed_ms<?4 AND last_observed_ms<?4 AND (ended_ms IS NULL OR ended_ms>?4) ORDER BY id DESC LIMIT 2")?;
                        let rows = stmt.query_map(
                            params![claim.number as i64, head, epoch_scan, merged_ms],
                            |r| r.get::<_, i64>(0),
                        )?;
                        observed = rows.collect::<Result<Vec<_>, _>>()?;
                    }
                    let ready = if observed.len() == 1 && observed[0] >= 0 {
                        Some(observed[0])
                    } else {
                        None
                    };
                    tx.execute("INSERT INTO ci_delivery_records(number,repository_id,target,merged_ms,head_sha,observed_ready_ms,conflicted) VALUES(?1,?2,?3,?4,?5,?6,0)", params![claim.number as i64,policy.repository,policy.target_branch,merged_ms,head_sha,ready])?;
                    inserted = true;
                }
            }
        }
        tx.commit()?;
        Ok(inserted)
    }
}
fn valid_sha(s: &str) -> bool {
    s.len() == 40
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn check_scope(
    tx: &Transaction<'_>,
    policy: &RepositoryPolicy,
    digest: &str,
) -> Result<i64, DeliveryError> {
    let row: Option<(i64,i64,String,String,i64,i64)> = tx.query_row("SELECT r.installation,r.repository,r.policy_digest,t.branch,v.revision,(SELECT COALESCE(MAX(revision),0) FROM admission_policy_migrations) FROM repository_binding r JOIN observed_target t ON t.id=r.id JOIN admission_policy_revision v ON v.id=r.id WHERE r.id=1", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
    let Some((installation, repository, known, target, revision, last)) = row else {
        return Err(DeliveryError::Scope);
    };
    if installation != policy.installation
        || repository != policy.repository
        || known != digest
        || target != policy.target_branch
        || revision < 0
        || revision != last
    {
        return Err(DeliveryError::Scope);
    }
    Ok(revision)
}
fn pending(tx: &Transaction<'_>, policy: &RepositoryPolicy) -> Result<bool, DeliveryError> {
    Ok(tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM reconciliations WHERE installation=?1 AND repository=?2)",
        params![policy.installation, policy.repository],
        |r| r.get(0),
    )?)
}
fn event_revision(tx: &Transaction<'_>) -> Result<i64, DeliveryError> {
    let seq: Option<i64> = tx
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name='webhook_deliveries'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    let seq = seq.unwrap_or(0);
    if seq < 0 {
        return Err(DeliveryError::Invalid);
    }
    Ok(seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Candidate, store::AdmissionCandidate, webhook::Inbox};
    use std::collections::BTreeSet;

    #[test]
    fn delivery_accounting_is_idempotent_stale_safe_and_excludes_conflicts() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("queue.sqlite");
        let mut inbox = Inbox::open(&db).unwrap();
        let mut store = Store::open(&db).unwrap();
        let policy = RepositoryPolicy {
            installation: 1,
            repository: 2,
            owner: "owner".into(),
            name: "repo".into(),
            target_branch: "main".into(),
            required_reviews: 1,
            block_labels: BTreeSet::new(),
        };
        let digest = "f".repeat(64);
        inbox.record("one", "pull_request", &[1; 32], 1, 2).unwrap();
        let event = inbox.pending().unwrap().remove(0);
        let candidate = AdmissionCandidate {
            candidate: Candidate {
                number: 19,
                head_sha: "a".repeat(40),
            },
            evidence_digest: "e".repeat(64),
        };
        store
            .apply_admission_reconciliation_with_base(
                &event,
                &digest,
                &[candidate],
                &"b".repeat(40),
                "main",
            )
            .unwrap();
        inbox.record("two", "pull_request", &[2; 32], 1, 2).unwrap();
        let event = inbox.pending().unwrap().remove(0);
        store
            .apply_admission_reconciliation_with_base(&event, &digest, &[], &"b".repeat(40), "main")
            .unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute("UPDATE admission_observations_v1 SET first_observed_ms=100,last_observed_ms=150,ended_ms=300 WHERE number=19", []).unwrap();
        drop(conn);
        let mut journal = DeliveryJournal::open(&db).unwrap();
        let old = journal.next(&policy, &digest).unwrap().unwrap();
        assert!(journal.next(&policy, &digest).unwrap().is_none());
        let current = journal.next(&policy, &digest).unwrap().unwrap();
        let merged = DeliveryObservation::Merged {
            merged_ms: 200,
            head_sha: Some("a".repeat(40)),
        };
        assert!(
            !journal
                .record(
                    &current,
                    &policy,
                    &digest,
                    &DeliveryObservation::Merged {
                        merged_ms: 50,
                        head_sha: Some("a".repeat(40)),
                    }
                )
                .unwrap()
        );
        assert!(
            journal
                .record(
                    &current,
                    &policy,
                    &digest,
                    &DeliveryObservation::Merged {
                        merged_ms: 200,
                        head_sha: None,
                    }
                )
                .unwrap()
        );
        assert_eq!(
            Store::open(&db)
                .unwrap()
                .metrics()
                .unwrap()
                .observed_admission_samples,
            0
        );
        Connection::open(&db)
            .unwrap()
            .execute("DELETE FROM ci_delivery_records", [])
            .unwrap();
        assert!(matches!(
            journal.record(&old, &policy, &digest, &merged),
            Err(DeliveryError::Stale)
        ));
        assert!(journal.record(&current, &policy, &digest, &merged).unwrap());
        let metrics = Store::open(&db).unwrap().metrics().unwrap();
        assert_eq!(
            (
                metrics.observed_deliveries,
                metrics.delivery_conflicts,
                metrics.observed_admission_samples
            ),
            (1, 0, 1)
        );
        assert_eq!(metrics.observed_admission_median_ms, Some(100));
        drop(journal);
        let mut journal = DeliveryJournal::open(&db).unwrap();
        assert!(journal.next(&policy, &digest).unwrap().is_none());
        let same = journal.next(&policy, &digest).unwrap().unwrap();
        assert!(!journal.record(&same, &policy, &digest, &merged).unwrap());
        assert_eq!(
            Store::open(&db)
                .unwrap()
                .metrics()
                .unwrap()
                .observed_deliveries,
            1
        );
        assert!(journal.next(&policy, &digest).unwrap().is_none());
        let stale = journal.next(&policy, &digest).unwrap().unwrap();
        inbox
            .record("three", "pull_request", &[3; 32], 1, 2)
            .unwrap();
        assert!(matches!(
            journal.record(&stale, &policy, &digest, &DeliveryObservation::NotMerged),
            Err(DeliveryError::Stale)
        ));
        let event = inbox.pending().unwrap().remove(0);
        store
            .apply_admission_reconciliation_with_base(&event, &digest, &[], &"b".repeat(40), "main")
            .unwrap();
        assert!(journal.next(&policy, &digest).unwrap().is_none());
        let contradiction = journal.next(&policy, &digest).unwrap().unwrap();
        journal
            .record(
                &contradiction,
                &policy,
                &digest,
                &DeliveryObservation::NotMerged,
            )
            .unwrap();
        let metrics = Store::open(&db).unwrap().metrics().unwrap();
        assert_eq!(
            (
                metrics.observed_deliveries,
                metrics.delivery_conflicts,
                metrics.observed_admission_samples
            ),
            (0, 1, 0)
        );
    }
}

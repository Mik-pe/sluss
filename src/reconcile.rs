//! Reconcile one operator-configured repository into CI-planning observations.
//! No CI is dispatched and no result authorizes a merge.
use crate::{
    admission::{self, AdmissionError, Policy},
    batch::Candidate,
    github::{App, GitHubError, InstallationToken},
    store::{AdmissionCandidate, Store, StoreError},
    webhook::Reconciliation,
};
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Clone, serde::Serialize)]
pub struct RepositoryPolicy {
    pub installation: i64,
    pub repository: i64,
    pub owner: String,
    pub name: String,
    pub target_branch: String,
    pub required_reviews: usize,
    pub block_labels: BTreeSet<String>,
}
#[derive(Debug, Error)]
pub enum ReconcileError {
    #[error("reconciliation does not match the configured repository")]
    Scope,
    #[error("the pull request list or admission observations changed")]
    Unstable,
    #[error(transparent)]
    GitHub(#[from] GitHubError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Bind queue observations to the App origin and the full operator policy.
pub fn policy_digest(app: &App, config: &RepositoryPolicy) -> Result<String, ReconcileError> {
    use sha2::Digest;
    let encoded = serde_json::to_vec(&(app.scope(), config)).map_err(|_| ReconcileError::Scope)?;
    Ok(hex::encode(sha2::Sha256::digest(encoded)))
}

/// Canonical local digest of an authenticated review observation. This is
/// scheduling evidence, not an attestation or an atomic provider snapshot.
pub(crate) fn admission_digest(
    observed: &crate::github::AuthorizedPullObservation,
) -> Result<String, ReconcileError> {
    use sha2::Digest;
    let decisions: Vec<_> = observed
        .snapshot
        .reviews
        .iter()
        .filter(|review| !matches!(review.state.as_str(), "PENDING" | "COMMENTED"))
        .collect();
    let input = serde_json::to_vec(&(
        "sluss-admission-v2",
        &observed.snapshot.pull,
        decisions,
        &observed.authorized_reviewers,
        &observed.reviewer_permissions,
    ))
    .map_err(|_| ReconcileError::Scope)?;
    Ok(hex::encode(sha2::Sha256::digest(input)))
}

/// Refresh a single pending delivery. A failure never acknowledges the job or
/// applies a partial queue. The caller must retry and periodically recover
/// missed events. Branch protection and CODEOWNERS remain unimplemented.
pub async fn reconcile_once(
    app: &App,
    token: &InstallationToken,
    store: &mut Store,
    job: &Reconciliation,
    config: &RepositoryPolicy,
) -> Result<(), ReconcileError> {
    if job.installation != config.installation
        || job.repository != config.repository
        || config.installation <= 0
        || config.repository <= 0
        || App::repository_id(token) != config.repository as u64
        || App::installation_id(token) != config.installation as u64
        || config.target_branch.is_empty()
        || config.required_reviews == 0
    {
        return Err(ReconcileError::Scope);
    }
    let base = app
        .observe_target_branch(token, &config.owner, &config.name, &config.target_branch)
        .await?;
    let numbers = app
        .open_pull_numbers(token, &config.owner, &config.name, &config.target_branch)
        .await?;
    let mut candidates = Vec::new();
    let observations = app
        .observe_authorized_pulls(token, &config.owner, &config.name, &numbers)
        .await?;
    for observed in observations {
        if observed.snapshot.pull.base.branch == config.target_branch
            && observed.snapshot.pull.base.repo.id == config.repository as u64
            && observed.snapshot.pull.base.sha != base.commit.sha
        {
            return Err(ReconcileError::Unstable);
        }
        let policy = Policy {
            target_branch: config.target_branch.clone(),
            required_reviews: config.required_reviews,
            block_labels: config.block_labels.clone(),
            authorized_reviewers: observed.authorized_reviewers.clone(),
        };
        match admission::evaluate(&observed.snapshot, &policy) {
            Ok(eligible) => {
                candidates.push(AdmissionCandidate {
                    candidate: Candidate {
                        number: eligible.pull_number,
                        head_sha: eligible.head_sha,
                    },
                    evidence_digest: admission_digest(&observed)?,
                });
            }
            Err(
                AdmissionError::NotReady
                | AdmissionError::BlockedLabel
                | AdmissionError::ChangesRequested
                | AdmissionError::InsufficientReviews,
            ) => {}
            Err(AdmissionError::Invalid) => return Err(ReconcileError::Unstable),
        }
    }
    if app
        .open_pull_numbers(token, &config.owner, &config.name, &config.target_branch)
        .await?
        != numbers
    {
        return Err(ReconcileError::Unstable);
    }
    if app
        .observe_target_branch(token, &config.owner, &config.name, &config.target_branch)
        .await?
        != base
    {
        return Err(ReconcileError::Unstable);
    }
    let policy_digest = policy_digest(app, config)?;
    store.apply_admission_reconciliation_with_base(
        job,
        &policy_digest,
        &candidates,
        &base.commit.sha,
        &config.target_branch,
    )?;
    Ok(())
}

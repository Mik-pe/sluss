//! Reconcile one operator-configured repository into CI-planning observations.
//! No CI is dispatched and no result authorizes a merge.
use crate::{
    admission::{self, AdmissionError, Policy},
    batch::Candidate,
    github::{App, GitHubError, InstallationToken},
    store::{Store, StoreError},
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
    let numbers = app
        .open_pull_numbers(token, &config.owner, &config.name)
        .await?;
    let mut candidates = Vec::new();
    for number in &numbers {
        let observed = app
            .observe_authorized_pull(token, &config.owner, &config.name, *number)
            .await?;
        let policy = Policy {
            target_branch: config.target_branch.clone(),
            required_reviews: config.required_reviews,
            block_labels: config.block_labels.clone(),
            authorized_reviewers: observed.authorized_reviewers,
        };
        match admission::evaluate(&observed.snapshot, &policy) {
            Ok(eligible) => candidates.push(Candidate {
                number: eligible.pull_number,
                head_sha: eligible.head_sha,
            }),
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
        .open_pull_numbers(token, &config.owner, &config.name)
        .await?
        != numbers
    {
        return Err(ReconcileError::Unstable);
    }
    use sha2::Digest;
    let encoded = serde_json::to_vec(&(app.scope(), config)).map_err(|_| ReconcileError::Scope)?;
    let policy_digest = hex::encode(sha2::Sha256::digest(encoded));
    store.apply_reconciliation(job, &policy_digest, &candidates)?;
    Ok(())
}

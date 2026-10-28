//! Experimental local end-to-end integration preparation. No CI or landing.
use crate::{
    github::{App, InstallationToken, StagingError},
    integration::{self, IntegrationError, PolicyError, StageError},
    store::{Store, StoreError},
};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PrepareError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Integration(#[from] IntegrationError),
    #[error(transparent)]
    Policy(#[from] PolicyError),
    #[error(transparent)]
    Stage(#[from] StageError),
    #[error(transparent)]
    GitHubStage(#[from] StagingError),
    #[error("integration worker stopped unexpectedly")]
    Worker,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedIntegration {
    pub batch_id: String,
    pub sha: String,
    pub reference: String,
    pub policy_digest: String,
}
/// Prepare a batch using an explicit local source and local staging repository.
/// Repeating after a crash is safe if the integration and policy are unchanged.
/// The caller must establish that the source, target base, policy and remote
/// are trusted. This method cannot authorize a merge.
pub fn prepare_local(
    store: &mut Store,
    batch_id: &str,
    source: &Path,
    staging_remote: &Path,
    policy_paths: &[String],
) -> Result<PreparedIntegration, PrepareError> {
    let batch = store.start_integration(batch_id)?;
    let heads: Vec<String> = batch
        .candidates
        .iter()
        .map(|c| c.head_sha.clone())
        .collect();
    let integration = match integration::build(source, &batch.base_sha, &heads) {
        Ok(integration) => integration,
        Err(error @ (IntegrationError::Conflict | IntegrationError::AlreadyIntegrated)) => {
            store.fail_integration(batch_id, &error.to_string())?;
            return Err(error.into());
        }
        Err(error) => return Err(error.into()),
    };
    let policy = integration.check_policy(&batch.base_sha, policy_paths)?;
    // Recheck after the potentially long Git operation. If an earlier attempt
    // already completed, validate both frozen values before touching the ref.
    let current = store.start_integration(batch_id)?;
    if matches!(current.state, crate::batch::State::Testing { .. }) {
        store.finish_integration(batch_id, integration.sha(), &policy.digest)?;
    }
    let staged = integration.stage_local(staging_remote, batch_id)?;
    store.finish_integration(batch_id, &staged.sha, &policy.digest)?;
    Ok(PreparedIntegration {
        batch_id: batch_id.into(),
        sha: staged.sha,
        reference: staged.reference,
        policy_digest: policy.digest,
    })
}

pub struct GitHubRepository<'a> {
    pub owner: &'a str,
    pub repository: &'a str,
}

/// Prepare and publish a staging branch to GitHub.com with a scoped App token.
/// This remains experimental: workflow trust, staging rules and host isolation
/// must be established before CI is enabled or any landing is attempted.
pub async fn prepare_github(
    store: &mut Store,
    app: &App,
    token: &InstallationToken,
    target: GitHubRepository<'_>,
    batch_id: &str,
    source: &Path,
    policy_paths: &[String],
) -> Result<PreparedIntegration, PrepareError> {
    if store.repository_scope()? != (App::installation_id(token), App::repository_id(token)) {
        return Err(StoreError::RepositoryMismatch.into());
    }
    let batch = store.start_integration(batch_id)?;
    let heads: Vec<String> = batch
        .candidates
        .iter()
        .map(|c| c.head_sha.clone())
        .collect();
    let base = batch.base_sha.clone();
    let source = source.to_owned();
    let paths = policy_paths.to_vec();
    let built = tokio::task::spawn_blocking(move || -> Result<_, PrepareError> {
        let integration = integration::build(&source, &base, &heads)?;
        let policy = integration.check_policy(&base, &paths)?;
        Ok((integration, policy))
    })
    .await
    .map_err(|_| PrepareError::Worker)?;
    let (integration, policy) = match built {
        Ok(result) => result,
        Err(PrepareError::Integration(
            error @ (IntegrationError::Conflict | IntegrationError::AlreadyIntegrated),
        )) => {
            store.fail_integration(batch_id, &error.to_string())?;
            return Err(error.into());
        }
        Err(error) => return Err(error),
    };
    let current = store.start_integration(batch_id)?;
    if matches!(current.state, crate::batch::State::Testing { .. }) {
        store.finish_integration(batch_id, integration.sha(), &policy.digest)?;
    }
    let staged = app
        .stage_integration(
            token,
            target.owner,
            target.repository,
            integration,
            batch_id,
        )
        .await?;
    store.finish_integration(batch_id, &staged.sha, &policy.digest)?;
    Ok(PreparedIntegration {
        batch_id: batch_id.into(),
        sha: staged.sha,
        reference: staged.reference,
        policy_digest: policy.digest,
    })
}

//! Experimental local end-to-end integration preparation. No CI or landing.
use crate::{
    github::{App, InstallationToken, StagingError},
    integration::{self, IntegrationError, PolicyError, StageError, StagingWorkflowError},
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
    Selection(#[from] crate::selective::SelectionError),
    #[error(transparent)]
    Stage(#[from] StageError),
    #[error(transparent)]
    GitHubStage(#[from] StagingError),
    #[error("base workflow configuration cannot safely stage: {0}")]
    BaseWorkflow(#[source] StagingWorkflowError),
    #[error("integration changes introduced an unsafe or ambiguous staging workflow: {0}")]
    ChangedWorkflow(#[source] StagingWorkflowError),
    #[error("staging workflow inspection could not finish: {0}")]
    WorkflowInspection(#[source] StagingWorkflowError),
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
pub struct StagingOptions<'a> {
    pub policy_paths: &'a [String],
    pub verification_policy: Option<&'a crate::selective::Policy>,
}
fn selection_for(
    integration: &integration::Integration,
    base: &str,
    policy: Option<&crate::selective::Policy>,
    force_full: bool,
) -> Result<Option<crate::selective::FrozenSelection>, PrepareError> {
    policy
        .map(|policy| {
            Ok(crate::selective::FrozenSelection {
                base_sha: base.into(),
                integration_sha: integration.sha().into(),
                policy_digest: policy.digest()?,
                selection: if force_full {
                    crate::selective::Selection::Full {
                        suites: policy.full_suites.iter().cloned().collect(),
                        reason: "bisection requires comparable full suites".into(),
                    }
                } else {
                    policy.select_git(&integration.repository(), base, integration.sha())?
                },
            })
        })
        .transpose()
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
    prepare_local_with_options(
        store,
        batch_id,
        source,
        staging_remote,
        StagingOptions {
            policy_paths,
            verification_policy: None,
        },
    )
}

pub fn prepare_local_with_options(
    store: &mut Store,
    batch_id: &str,
    source: &Path,
    staging_remote: &Path,
    options: StagingOptions<'_>,
) -> Result<PreparedIntegration, PrepareError> {
    let policy_paths = options.policy_paths;
    let digest = options
        .verification_policy
        .map(|policy| policy.digest())
        .transpose()?;
    store.bind_policy_paths(batch_id, policy_paths)?;
    store.bind_staging_selection(batch_id, digest.as_deref())?;
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
    let policy = match integration.check_policy(&batch.base_sha, policy_paths) {
        Ok(policy) => policy,
        Err(error @ PolicyError::Changed) => {
            store.fail_integration(batch_id, &error.to_string())?;
            return Err(error.into());
        }
        Err(error) => return Err(error.into()),
    };
    let selection = selection_for(
        &integration,
        &batch.base_sha,
        options.verification_policy,
        store.is_split_batch(batch_id)?,
    )?;
    // Recheck after the potentially long Git operation. If an earlier attempt
    // already completed, validate both frozen values before touching the ref.
    let current = store.start_integration(batch_id)?;
    if matches!(current.state, crate::batch::State::Testing { .. }) {
        store.finish_integration_with_selection(
            batch_id,
            integration.sha(),
            &policy.digest,
            selection.as_ref(),
        )?;
    }
    let staged = integration.stage_local(staging_remote, batch_id)?;
    store.finish_integration_with_selection(
        batch_id,
        &staged.sha,
        &policy.digest,
        selection.as_ref(),
    )?;
    Ok(PreparedIntegration {
        batch_id: batch_id.into(),
        sha: staged.sha,
        reference: staged.reference,
        policy_digest: policy.digest,
    })
}

#[derive(Debug, Error)]
pub enum LocalPrepareError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Fetch(#[from] crate::integration::FetchError),
    #[error(transparent)]
    Prepare(#[from] PrepareError),
}

/// Exercise the same frozen-ref fetching and crash recovery against a local remote.
pub fn prepare_local_from_remote(
    store: &mut Store,
    batch_id: &str,
    remote: &Path,
    target: &str,
    policy_paths: &[String],
) -> Result<PreparedIntegration, LocalPrepareError> {
    prepare_local_from_remote_with_options(
        store,
        batch_id,
        remote,
        target,
        StagingOptions {
            policy_paths,
            verification_policy: None,
        },
    )
}

pub fn prepare_local_from_remote_with_options(
    store: &mut Store,
    batch_id: &str,
    remote: &Path,
    target: &str,
    options: StagingOptions<'_>,
) -> Result<PreparedIntegration, LocalPrepareError> {
    let digest = options
        .verification_policy
        .map(|policy| policy.digest())
        .transpose()
        .map_err(PrepareError::from)?;
    store.bind_policy_paths(batch_id, options.policy_paths)?;
    store.bind_staging_selection(batch_id, digest.as_deref())?;
    let batch = store.start_integration(batch_id)?;
    let inputs = crate::integration::fetch_local(remote, target, &batch)?;
    prepare_local_with_options(store, batch_id, &inputs.repository(), remote, options)
        .map_err(Into::into)
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
    prepare_github_with_options(
        store,
        app,
        token,
        target,
        batch_id,
        source,
        StagingOptions {
            policy_paths,
            verification_policy: None,
        },
    )
    .await
}

pub async fn prepare_github_with_options(
    store: &mut Store,
    app: &App,
    token: &InstallationToken,
    target: GitHubRepository<'_>,
    batch_id: &str,
    source: &Path,
    options: StagingOptions<'_>,
) -> Result<PreparedIntegration, PrepareError> {
    let policy_paths = options.policy_paths;
    if store.repository_scope()? != (App::installation_id(token), App::repository_id(token)) {
        return Err(StoreError::RepositoryMismatch.into());
    }
    let digest = options
        .verification_policy
        .map(|policy| policy.digest())
        .transpose()?;
    store.bind_policy_paths(batch_id, policy_paths)?;
    store.bind_staging_selection(batch_id, digest.as_deref())?;
    let batch = store.start_integration(batch_id)?;
    let heads: Vec<String> = batch
        .candidates
        .iter()
        .map(|c| c.head_sha.clone())
        .collect();
    let base = batch.base_sha.clone();
    let source = source.to_owned();
    let paths = policy_paths.to_vec();
    let verification_policy = options.verification_policy.cloned();
    let force_full = store.is_split_batch(batch_id)?;
    let frozen_id = batch_id.to_owned();
    let built = tokio::task::spawn_blocking(move || -> Result<_, PrepareError> {
        let integration = integration::build(&source, &base, &heads)?;
        // A bad base is operator configuration, not a PR failure. Only a
        // deterministic problem introduced on top of a safe base is split.
        integration
            .check_staging_workflows_at(&base, &frozen_id)
            .map_err(PrepareError::BaseWorkflow)?;
        let policy = integration.check_policy(&base, &paths)?;
        integration
            .check_staging_workflows(&frozen_id)
            .map_err(|error| match error {
                StagingWorkflowError::PushTrigger(_)
                | StagingWorkflowError::CreateTrigger(_)
                | StagingWorkflowError::Ambiguous(_) => PrepareError::ChangedWorkflow(error),
                _ => PrepareError::WorkflowInspection(error),
            })?;
        let selection = selection_for(
            &integration,
            &base,
            verification_policy.as_ref(),
            force_full,
        )?;
        Ok((integration, policy, selection))
    })
    .await
    .map_err(|_| PrepareError::Worker)?;
    let (integration, policy, selection) = match built {
        Ok(result) => result,
        Err(PrepareError::Integration(
            error @ (IntegrationError::Conflict | IntegrationError::AlreadyIntegrated),
        )) => {
            store.fail_integration(batch_id, &error.to_string())?;
            return Err(error.into());
        }
        Err(PrepareError::Policy(error @ PolicyError::Changed)) => {
            store.fail_integration(batch_id, &error.to_string())?;
            return Err(error.into());
        }
        Err(PrepareError::ChangedWorkflow(error)) => {
            store.fail_integration(batch_id, &error.to_string())?;
            return Err(PrepareError::ChangedWorkflow(error));
        }
        Err(error) => return Err(error),
    };
    let current = store.start_integration(batch_id)?;
    if matches!(current.state, crate::batch::State::Testing { .. }) {
        store.finish_integration_with_selection(
            batch_id,
            integration.sha(),
            &policy.digest,
            selection.as_ref(),
        )?;
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
    store.finish_integration_with_selection(
        batch_id,
        &staged.sha,
        &policy.digest,
        selection.as_ref(),
    )?;
    Ok(PreparedIntegration {
        batch_id: batch_id.into(),
        sha: staged.sha,
        reference: staged.reference,
        policy_digest: policy.digest,
    })
}

#[derive(Debug, Error)]
pub enum ProviderPrepareError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Inbox(#[from] rusqlite::Error),
    #[error(transparent)]
    Reconcile(#[from] crate::reconcile::ReconcileError),
    #[error(transparent)]
    GitHub(#[from] crate::github::GitHubError),
    #[error(transparent)]
    Fetch(#[from] crate::github::InputFetchError),
    #[error(transparent)]
    Prepare(#[from] PrepareError),
    #[error("reconciliation was consumed concurrently; retry preparation")]
    ReconciliationRace,
    #[error("automatic Git staging requires the GitHub.com API")]
    UnsupportedProvider,
    #[error("no usable policy files configured")]
    MissingPolicy,
}

/// Refresh admission, fetch exact input refs into a disposable repository,
/// then build and publish an isolated staging ref. This does not start CI or
/// authorize landing. Ambiguous failures are reconciled by the existing
/// deterministic integration and staging-ref readback on the next pass.
pub async fn prepare_github_from_provider(
    db: &Path,
    app: &App,
    policy: &crate::reconcile::RepositoryPolicy,
    batch_id: &str,
    policy_paths: &[String],
    verification_policy: Option<&Path>,
) -> Result<PreparedIntegration, ProviderPrepareError> {
    if policy_paths.is_empty() {
        return Err(ProviderPrepareError::MissingPolicy);
    }
    if !app.supports_github_git() {
        return Err(ProviderPrepareError::UnsupportedProvider);
    }
    let mut store = Store::open(db)?;
    if store.repository_scope()? != (policy.installation as u64, policy.repository as u64) {
        return Err(StoreError::RepositoryMismatch.into());
    }
    let mut inbox = crate::webhook::Inbox::open(db)?;
    inbox.schedule_if_idle(policy.installation, policy.repository)?;
    let job = inbox
        .pending_for(policy.installation, policy.repository)?
        .ok_or(ProviderPrepareError::ReconciliationRace)?;
    let read_token = app
        .read_token(policy.installation as u64, policy.repository as u64)
        .await?;
    crate::reconcile::reconcile_once(app, &read_token, &mut store, &job, policy).await?;
    let selection_policy = verification_policy
        .map(crate::selective::Policy::load)
        .transpose()
        .map_err(PrepareError::from)?;
    let digest = selection_policy
        .as_ref()
        .map(|p| p.digest())
        .transpose()
        .map_err(PrepareError::from)?;
    store.bind_policy_paths(batch_id, policy_paths)?;
    store.bind_staging_selection(batch_id, digest.as_deref())?;
    let batch = store.start_integration(batch_id)?;
    let inputs = match app
        .fetch_batch_inputs(
            &read_token,
            &policy.owner,
            &policy.name,
            &policy.target_branch,
            &batch,
        )
        .await
    {
        Ok(inputs) => inputs,
        Err(
            error @ crate::github::InputFetchError::Fetch(crate::integration::FetchError::Stale),
        ) => {
            inbox.schedule_if_idle(policy.installation, policy.repository)?;
            return Err(error.into());
        }
        Err(error) => return Err(error.into()),
    };
    let write_token = app
        .staging_token(policy.installation as u64, policy.repository as u64)
        .await?;
    prepare_github_with_options(
        &mut store,
        app,
        &write_token,
        GitHubRepository {
            owner: &policy.owner,
            repository: &policy.name,
        },
        batch_id,
        &inputs.repository(),
        StagingOptions {
            policy_paths,
            verification_policy: selection_policy.as_ref(),
        },
    )
    .await
    .map_err(Into::into)
}

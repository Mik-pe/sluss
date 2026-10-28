//! Experimental local end-to-end integration preparation. No CI or landing.
use crate::{
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

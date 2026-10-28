//! Shared guarded batch dispatch for the CLI and automatic worker.
use crate::{
    batch::State,
    github::{App, DispatchError, GitHubError},
    reconcile::{self, ReconcileError, RepositoryPolicy},
    scheduler::{Capacity, DispatchPlan, DispatchState, Request, Scheduler, SchedulerError},
    selective::{Policy, SelectionError},
    store::{Store, StoreError},
    verification::{self, InventoryError},
    webhook::Inbox,
};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct Settings {
    pub workflow_id: u64,
    pub required_jobs: BTreeSet<String>,
    pub request: Request,
    pub verification_policy: Option<PathBuf>,
}
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Sent { run: u64, host: String },
    NoCapacity,
    AlreadyStarted,
}
#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(&'static str),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Reconcile(#[from] ReconcileError),
    #[error(transparent)]
    GitHub(#[from] GitHubError),
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),
    #[error(transparent)]
    Inventory(#[from] InventoryError),
    #[error(transparent)]
    Selection(#[from] SelectionError),
}

/// Refresh all admission data, reserve physical capacity and send at most one
/// request. A sending or accepted journal entry is never automatically retried.
pub async fn batch(
    db: &Path,
    app: &App,
    policy: &RepositoryPolicy,
    capacity: &Capacity,
    settings: &Settings,
    batch_id: &str,
) -> Result<Outcome, Error> {
    let mut store = Store::open(db)?;
    if policy.installation <= 0
        || policy.repository <= 0
        || store.repository_scope()? != (policy.installation as u64, policy.repository as u64)
    {
        return Err(Error::Invalid(
            "repository does not match the reconciled queue",
        ));
    }
    if capacity
        .hosts
        .keys()
        .any(|name| !crate::github::valid_runner_label(name))
    {
        return Err(Error::Invalid(
            "each host must use a unique sluss-host-* runner label",
        ));
    }
    let mut scheduler = Scheduler::open(db, capacity.clone())?;
    if scheduler
        .pending_dispatches()?
        .iter()
        .any(|p| p.reservation.id == batch_id && !matches!(p.state, DispatchState::Prepared))
    {
        return Ok(Outcome::AlreadyStarted);
    }
    let batch = store
        .batch(batch_id)?
        .ok_or(Error::Invalid("unknown batch"))?
        .0;
    let sha = match batch.state {
        State::Testing { integration_sha } => integration_sha,
        _ => return Err(Error::Invalid("batch has not been staged for testing")),
    };
    let policy_digest = store
        .batch_policy_digest(batch_id)?
        .ok_or(Error::Invalid("batch has no policy snapshot"))?;
    let (suites, selection_policy_digest) = match (
        store.batch_selection(batch_id)?,
        &settings.verification_policy,
    ) {
        (Some(frozen), Some(path)) => {
            if Policy::load(path)?.digest()? != frozen.policy_digest {
                return Err(Error::Invalid(
                    "verification policy changed after selection",
                ));
            }
            (
                frozen.selection.suites().clone(),
                Some(frozen.policy_digest),
            )
        }
        (Some(_), None) => {
            return Err(Error::Invalid(
                "the frozen selection requires --verification-policy",
            ));
        }
        (None, Some(_)) => {
            return Err(Error::Invalid(
                "run select-batch before supplying a verification policy",
            ));
        }
        (None, None) => (BTreeSet::from(["full".into()]), None),
    };
    let plan = DispatchPlan {
        installation_id: policy.installation as u64,
        repository_id: policy.repository as u64,
        workflow_id: settings.workflow_id,
        integration_sha: sha,
        staging_ref: format!("refs/heads/sluss/{batch_id}"),
        policy_digest,
        required_jobs: settings.required_jobs.clone(),
        suites,
        selection_policy_digest,
    };
    let mut inbox = Inbox::open(db)?;
    inbox.schedule_if_idle(policy.installation, policy.repository)?;
    let job = inbox
        .pending_for(policy.installation, policy.repository)?
        .ok_or(Error::Invalid(
            "reconciliation was consumed concurrently; retry dispatch",
        ))?;
    let read_token = app
        .read_token(plan.installation_id, plan.repository_id)
        .await?;
    reconcile::reconcile_once(app, &read_token, &mut store, &job, policy).await?;
    let current = store
        .batch(batch_id)?
        .ok_or(Error::Invalid("unknown batch"))?
        .0;
    if current.base_sha != batch.base_sha
        || current.state
            != (State::Testing {
                integration_sha: plan.integration_sha.clone(),
            })
    {
        return Err(Error::Invalid(
            "batch changed during pre-dispatch reconciliation",
        ));
    }
    let inventory_token = app
        .runner_inventory_token(plan.installation_id, plan.repository_id)
        .await?;
    let runners = app
        .observe_repository_runners(&inventory_token, &policy.owner, &policy.name)
        .await?;
    let eligible_hosts = verification::available_repository_hosts(capacity, &runners)?;
    let token = app
        .dispatch_token(plan.installation_id, plan.repository_id)
        .await?;
    let Some(reservation) =
        scheduler.reserve_batch_dispatch_on(batch_id, &settings.request, &plan, &eligible_hosts)?
    else {
        return Ok(Outcome::NoCapacity);
    };
    match app
        .issue_batch_dispatch(
            &mut scheduler,
            &token,
            &policy.owner,
            &policy.name,
            batch_id,
            (&policy.target_branch, &batch.base_sha),
        )
        .await
    {
        Ok(run) => Ok(Outcome::Sent {
            run,
            host: reservation.host,
        }),
        Err(DispatchError::AlreadyClaimed) => Ok(Outcome::AlreadyStarted),
        Err(error) => Err(error.into()),
    }
}

//! Experimental webhook reconciliation loop. No CI or landing is performed.
use crate::{
    github::App,
    reconcile::{RepositoryPolicy, reconcile_once},
    store::Store,
    webhook::Inbox,
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::time::{Instant, sleep};

pub async fn supervise(
    app: Arc<App>,
    db: PathBuf,
    policy: RepositoryPolicy,
    poll: Duration,
    rescan: Duration,
) {
    loop {
        let result = tokio::spawn(run(app.clone(), db.clone(), policy.clone(), poll, rescan)).await;
        if result.is_err() {
            eprintln!("reconciliation worker stopped unexpectedly; restarting");
        }
        sleep(poll).await;
    }
}

pub async fn run(
    app: Arc<App>,
    db: PathBuf,
    policy: RepositoryPolicy,
    poll: Duration,
    rescan: Duration,
) {
    let mut next_scan = Instant::now();
    let mut next_attempt = Instant::now();
    let mut backoff = poll;
    loop {
        let now = Instant::now();
        if now >= next_scan {
            let path = db.clone();
            let installation = policy.installation;
            let repository = policy.repository;
            let result = tokio::task::spawn_blocking(move || {
                Inbox::open(path)?.schedule_if_idle(installation, repository)
            })
            .await;
            if !matches!(result, Ok(Ok(_))) {
                eprintln!("periodic reconciliation scheduling failed");
            }
            next_scan = now + rescan;
        }
        if now >= next_attempt {
            let path = db.clone();
            let installation = policy.installation;
            let repository = policy.repository;
            let pending = tokio::task::spawn_blocking(move || {
                Inbox::open(path)?.pending_for(installation, repository)
            })
            .await;
            match pending {
                Ok(Ok(Some(job))) => {
                    let result = match app.read_token(installation as u64, repository as u64).await
                    {
                        Ok(token) => {
                            let app = app.clone();
                            let path = db.clone();
                            let config = policy.clone();
                            let handle = tokio::runtime::Handle::current();
                            match tokio::task::spawn_blocking(move || {
                                let mut store = Store::open(path).map_err(|e| e.to_string())?;
                                handle
                                    .block_on(reconcile_once(
                                        &app, &token, &mut store, &job, &config,
                                    ))
                                    .map_err(|e| e.to_string())
                            })
                            .await
                            {
                                Ok(result) => result,
                                Err(_) => Err("worker task failed".to_string()),
                            }
                        }
                        Err(e) => Err(e.to_string()),
                    };
                    if let Err(error) = result {
                        eprintln!(
                            "repository reconciliation failed: {error}; pending work will be retried"
                        );
                        next_attempt = Instant::now() + backoff;
                        backoff = backoff.saturating_mul(2).min(Duration::from_secs(300));
                    } else {
                        backoff = poll;
                        next_attempt = Instant::now();
                    }
                }
                Ok(Ok(None)) => {
                    backoff = poll;
                }
                _ => {
                    eprintln!("reconciliation queue could not be read");
                    next_attempt = Instant::now() + backoff;
                    backoff = backoff.saturating_mul(2).min(Duration::from_secs(300));
                }
            }
        }
        sleep(poll).await;
    }
}

/// Optional stale-run cancellation loop. It cannot release capacity and never
/// retries an ambiguous cancellation POST. Provider failures back off.
pub async fn supervise_stale_cancellation(
    app: Arc<App>,
    db: PathBuf,
    capacity: crate::scheduler::Capacity,
    policy: RepositoryPolicy,
    poll: Duration,
) {
    let minimum = poll.max(Duration::from_secs(60));
    let mut delay = minimum;
    loop {
        let path = db.clone();
        let limits = capacity.clone();
        let scheduler =
            tokio::task::spawn_blocking(move || crate::scheduler::Scheduler::open(path, limits))
                .await;
        let result = match scheduler {
            Ok(Ok(mut scheduler)) => match scheduler
                .cancel_invalidated_prepared()
                .and_then(|_| scheduler.stale_dispatches())
            {
                Ok(ids) if ids.is_empty() => Ok(()),
                Ok(ids) => match app
                    .dispatch_token(policy.installation as u64, policy.repository as u64)
                    .await
                {
                    Ok(token) => {
                        let mut result = Ok(());
                        for id in ids {
                            match app
                                .cancel_stale_run(
                                    &mut scheduler,
                                    &token,
                                    &policy.owner,
                                    &policy.name,
                                    &id,
                                )
                                .await
                            {
                                Ok(crate::github::StaleCancellation::Uncertain) => {
                                    eprintln!(
                                        "stale-run cancellation outcome is uncertain; capacity remains reserved"
                                    );
                                    result = Err(());
                                }
                                Err(_) => result = Err(()),
                                _ => {}
                            }
                        }
                        result
                    }
                    Err(_) => Err(()),
                },
                Err(_) => Err(()),
            },
            _ => Err(()),
        };
        if result.is_err() {
            eprintln!("stale-run cancellation failed; it will be checked again");
            delay = delay.saturating_mul(2).min(Duration::from_secs(300));
        } else {
            delay = minimum;
        }
        sleep(delay).await;
    }
}

/// Observe known dispatch receipts without starting work or authorizing landing.
/// A failed provider observation is retried on a later pass.
pub async fn observe_dispatches_once(
    app: &App,
    db: &std::path::Path,
    capacity: &crate::scheduler::Capacity,
    policy: &RepositoryPolicy,
) -> Result<(), String> {
    observe_dispatches_with_completion(app, db, capacity, policy, false).await
}

/// CI-only completion retires the scheduling batch while retaining its reservation.
pub async fn observe_dispatches_with_completion(
    app: &App,
    db: &std::path::Path,
    capacity: &crate::scheduler::Capacity,
    policy: &RepositoryPolicy,
    complete_ci_only: bool,
) -> Result<(), String> {
    use crate::{batch::State, scheduler::DispatchState};
    let store = Store::open(db).map_err(|e| e.to_string())?;
    if store.repository_scope().map_err(|e| e.to_string())?
        != (policy.installation as u64, policy.repository as u64)
    {
        return Err("repository does not match the reconciled queue".into());
    }
    let mut scheduler =
        crate::scheduler::Scheduler::open(db, capacity.clone()).map_err(|e| e.to_string())?;
    let pending = scheduler.pending_dispatches().map_err(|e| e.to_string())?;
    let mut token = None;
    let mut failed = false;
    for dispatch in pending {
        if !matches!(
            dispatch.state,
            DispatchState::Accepted { .. } | DispatchState::Observed { .. }
        ) {
            continue;
        }
        let id = &dispatch.reservation.id;
        let Some((batch, _)) = store.batch(id).map_err(|e| e.to_string())? else {
            failed = true;
            continue;
        };
        if matches!(
            batch.state,
            State::Invalidated { .. } | State::CiComplete { .. }
        ) {
            continue;
        }
        let Some(host) = capacity.hosts.get(&dispatch.reservation.host) else {
            failed = true;
            continue;
        };
        if host.runner_ids.is_empty() {
            failed = true;
            continue;
        }
        if token.is_none() {
            token = Some(
                app.read_token(policy.installation as u64, policy.repository as u64)
                    .await
                    .map_err(|e| e.to_string())?,
            );
        }
        let result = async {
            let snapshot = app
                .reconcile_dispatch(
                    &mut scheduler,
                    token.as_ref().expect("token initialized"),
                    &policy.owner,
                    &policy.name,
                    id,
                )
                .await
                .map_err(|e| e.to_string())?;
            crate::verification::check_host_placement(&snapshot, &host.runner_ids)
                .map_err(|e| e.to_string())?;
            scheduler
                .record_job_metrics(id, &snapshot)
                .map_err(|e| e.to_string())?;
            let observation = scheduler
                .record_ci_observation(id, &snapshot)
                .map_err(|e| e.to_string())?;
            if complete_ci_only
                && !matches!(observation, crate::verification::CiObservation::Pending)
                && scheduler
                    .receipt_source(id)
                    .map_err(|e| e.to_string())?
                    .as_deref()
                    == Some("direct")
            {
                scheduler.complete_ci_only(id).map_err(|e| e.to_string())?;
            }
            Ok::<_, String>(())
        }
        .await;
        if let Err(error) = result {
            eprintln!("dispatch observation failed for {id}: {error}");
            failed = true;
        }
    }
    if failed {
        Err("one or more dispatch observations failed".into())
    } else {
        Ok(())
    }
}

/// Poll conservatively; an observation never releases its reservation.
pub async fn supervise_dispatch_observation(
    app: Arc<App>,
    db: PathBuf,
    capacity: crate::scheduler::Capacity,
    policy: RepositoryPolicy,
    poll: Duration,
    complete_ci_only: bool,
) {
    let minimum = poll.max(Duration::from_secs(60));
    let mut delay = minimum;
    loop {
        if let Err(error) =
            observe_dispatches_with_completion(&app, &db, &capacity, &policy, complete_ci_only)
                .await
        {
            eprintln!("dispatch observation pass failed: {error}");
            delay = delay.saturating_mul(2).min(Duration::from_secs(300));
        } else {
            delay = minimum;
        }
        sleep(delay).await;
    }
}

/// Make one planning decision from the currently reconciled queue. This does
/// not fetch Git objects, stage a commit, start CI, or authorize a merge.
pub fn plan_once(
    app: &App,
    db: &std::path::Path,
    policy: &RepositoryPolicy,
    settings: &crate::store::PlanningSettings,
) -> Result<Option<crate::batch::Batch>, String> {
    let digest = crate::reconcile::policy_digest(app, policy).map_err(|e| e.to_string())?;
    Store::open(db)
        .map_err(|e| e.to_string())?
        .plan_observed_batch(
            policy.installation,
            policy.repository,
            &digest,
            &policy.target_branch,
            settings,
        )
        .map_err(|e| e.to_string())
}

/// Optional planning loop. The planner is transactionally safe across workers
/// and simply finds active plans again after a restart.
pub async fn supervise_planning(
    app: Arc<App>,
    db: PathBuf,
    policy: RepositoryPolicy,
    settings: crate::store::PlanningSettings,
    poll: Duration,
) {
    let mut delay = poll;
    loop {
        let app = app.clone();
        let db = db.clone();
        let policy = policy.clone();
        let settings = settings.clone();
        let result =
            tokio::task::spawn_blocking(move || plan_once(&app, &db, &policy, &settings)).await;
        match result {
            Ok(Ok(Some(batch))) => {
                eprintln!(
                    "planned batch {} with {} pull requests",
                    batch.id,
                    batch.candidates.len()
                );
                delay = poll;
            }
            Ok(Ok(None)) => delay = poll,
            Ok(Err(error)) => {
                eprintln!("automatic batch planning failed: {error}");
                delay = delay.saturating_mul(2).min(Duration::from_secs(300));
            }
            Err(_) => {
                eprintln!("automatic batch planner stopped unexpectedly");
                delay = delay.saturating_mul(2).min(Duration::from_secs(300));
            }
        }
        sleep(delay).await;
    }
}

/// Process one planned or interrupted integration. Provider and Git failures
/// leave the batch available for a later pass; classified merge conflicts use
/// the durable bisection path.
pub async fn prepare_once(
    app: &App,
    db: &std::path::Path,
    policy: &RepositoryPolicy,
    policy_paths: &[String],
) -> Result<Option<crate::coordinator::PreparedIntegration>, String> {
    let store = Store::open(db).map_err(|e| e.to_string())?;
    let batch = store
        .active_batches()
        .map_err(|e| e.to_string())?
        .into_iter()
        .find(|batch| {
            matches!(
                batch.state,
                crate::batch::State::Planned | crate::batch::State::Building
            )
        });
    let Some(batch) = batch else {
        return Ok(None);
    };
    crate::coordinator::prepare_github_from_provider(db, app, policy, &batch.id, policy_paths)
        .await
        .map(Some)
        .map_err(|e| e.to_string())
}

pub async fn supervise_preparation(
    app: Arc<App>,
    db: PathBuf,
    policy: RepositoryPolicy,
    policy_paths: Vec<String>,
    poll: Duration,
) {
    let mut delay = poll;
    loop {
        match prepare_once(&app, &db, &policy, &policy_paths).await {
            Ok(Some(batch)) => {
                eprintln!(
                    "prepared staging ref {} for batch {}",
                    batch.reference, batch.batch_id
                );
                delay = poll;
            }
            Ok(None) => delay = poll,
            Err(error) => {
                eprintln!("automatic staging failed: {error}");
                delay = delay.saturating_mul(2).min(Duration::from_secs(300));
            }
        }
        sleep(delay).await;
    }
}

/// Find one staged batch whose dispatch is absent or still safely prepared.
/// Claimed, accepted and observed intents are never sent again.
pub async fn dispatch_once(
    app: &App,
    db: &std::path::Path,
    policy: &RepositoryPolicy,
    capacity: &crate::scheduler::Capacity,
    settings: &crate::dispatch::Settings,
) -> Result<Option<crate::dispatch::Outcome>, String> {
    use crate::{batch::State, scheduler::DispatchState};
    let store = Store::open(db).map_err(|e| e.to_string())?;
    if store.paused().map_err(|e| e.to_string())? {
        return Ok(None);
    }
    let inbox = Inbox::open(db).map_err(|e| e.to_string())?;
    if inbox
        .pending_for(policy.installation, policy.repository)
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Ok(None);
    }
    let scheduler =
        crate::scheduler::Scheduler::open(db, capacity.clone()).map_err(|e| e.to_string())?;
    let intents = scheduler.pending_dispatches().map_err(|e| e.to_string())?;
    let batch = store
        .active_batches()
        .map_err(|e| e.to_string())?
        .into_iter()
        .find(|batch| {
            matches!(batch.state, State::Testing { .. })
                && !intents.iter().any(|intent| {
                    intent.reservation.id == batch.id
                        && !matches!(intent.state, DispatchState::Prepared)
                })
        });
    let Some(batch) = batch else {
        return Ok(None);
    };
    crate::dispatch::batch(db, app, policy, capacity, settings, &batch.id)
        .await
        .map(Some)
        .map_err(|e| e.to_string())
}

/// Automatic dispatch remains explicit opt-in and never authorizes landing.
pub async fn supervise_dispatch(
    app: Arc<App>,
    db: PathBuf,
    policy: RepositoryPolicy,
    capacity: crate::scheduler::Capacity,
    settings: crate::dispatch::Settings,
    poll: Duration,
) {
    let minimum = poll.max(Duration::from_secs(10));
    let mut delay = minimum;
    loop {
        match dispatch_once(&app, &db, &policy, &capacity, &settings).await {
            Ok(Some(crate::dispatch::Outcome::Sent { run, host })) => {
                eprintln!("dispatched run {run} to reserved host {host}");
                delay = minimum;
            }
            Ok(_) => delay = minimum,
            Err(error) => {
                eprintln!("automatic dispatch failed: {error}");
                delay = delay.saturating_mul(2).min(Duration::from_secs(300));
            }
        }
        sleep(delay).await;
    }
}

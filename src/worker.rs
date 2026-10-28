//! Experimental webhook reconciliation loop. No CI or landing is performed.
use crate::{
    github::App,
    reconcile::{RepositoryPolicy, reconcile_once},
    store::Store,
    webhook::Inbox,
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::time::{Instant, sleep};

// Startup is not an error while the first provider reconciliation is still
// binding a fresh database. Other errors and a mismatched binding must continue
// through the worker's normal fail-closed path.
async fn awaiting_repository_binding(db: &std::path::Path) -> bool {
    let db = db.to_owned();
    matches!(
        tokio::task::spawn_blocking(
            move || Store::open(db).and_then(|store| store.repository_scope())
        )
        .await,
        Ok(Err(crate::store::StoreError::Unbound))
    )
}

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
                            match scheduler.receipt_conflict(&id) {
                                Ok(Some((recovered, direct))) => {
                                    for run_id in [recovered, direct] {
                                        match app
                                            .cancel_conflicting_run(
                                                &mut scheduler,
                                                &token,
                                                &policy.owner,
                                                &policy.name,
                                                &id,
                                                run_id,
                                            )
                                            .await
                                        {
                                            Ok(crate::github::StaleCancellation::Uncertain)
                                            | Err(_) => result = Err(()),
                                            _ => {}
                                        }
                                    }
                                    continue;
                                }
                                Ok(None) => {}
                                Err(_) => {
                                    result = Err(());
                                    continue;
                                }
                            }
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

/// Cancel obsolete nonpassing checks or read back uncertain cancellations.
/// Only the journal's one-shot claim can send an update; recovery only reads.
pub async fn cancel_stale_checks_once(
    app: &App,
    db: &std::path::Path,
    policy: &RepositoryPolicy,
) -> Result<(), String> {
    use crate::{
        github::PendingCheckError,
        landing::{LandingError, LandingJournal, StaleCheckTask},
    };
    let store = Store::open(db).map_err(|e| e.to_string())?;
    if store.repository_scope().map_err(|e| e.to_string())?
        != (policy.installation as u64, policy.repository as u64)
    {
        return Err("repository does not match the reconciled queue".into());
    }
    let mut journal = LandingJournal::open(db).map_err(|e| e.to_string())?;
    let tasks = journal.stale_check_tasks().map_err(|e| e.to_string())?;
    if tasks.is_empty() {
        return Ok(());
    }
    let read = app
        .read_token(policy.installation as u64, policy.repository as u64)
        .await
        .map_err(|e| e.to_string())?;
    let mut write = None;
    let mut failed = false;
    for task in tasks {
        let result = match task {
            StaleCheckTask::Cancel(id) => {
                if write.is_none() {
                    match app
                        .checks_write_token(policy.installation as u64, policy.repository as u64)
                        .await
                    {
                        Ok(token) => write = Some(token),
                        Err(error) => {
                            eprintln!("stale staging-check write token failed: {error}");
                            failed = true;
                            continue;
                        }
                    }
                }
                app.cancel_stale_staged_check(
                    &mut journal,
                    (&read, write.as_ref().expect("initialized")),
                    (&policy.owner, &policy.name),
                    &id,
                )
                .await
            }
            StaleCheckTask::Recover(id) => {
                app.recover_cancelled_staged_check(
                    &mut journal,
                    &read,
                    (&policy.owner, &policy.name),
                    &id,
                )
                .await
            }
        };
        match result {
            Ok(_)
            | Err(PendingCheckError::Journal(
                LandingError::PendingCheckNotStale | LandingError::StaleBatch,
            )) => {}
            Err(error) => {
                eprintln!("stale staging-check cancellation or recovery failed: {error}");
                failed = true;
            }
        }
    }
    if failed {
        Err("one or more staging checks need another observation".into())
    } else {
        Ok(())
    }
}

/// Optional worker; ambiguous writes are subsequently observed, never resent.
pub async fn supervise_stale_check_cancellation(
    app: Arc<App>,
    db: PathBuf,
    policy: RepositoryPolicy,
    poll: Duration,
) {
    let minimum = poll.max(Duration::from_secs(60));
    let mut delay = minimum;
    loop {
        if awaiting_repository_binding(&db).await {
            sleep(poll.max(Duration::from_secs(1))).await;
            continue;
        }
        match cancel_stale_checks_once(&app, &db, &policy).await {
            Ok(()) => delay = minimum,
            Err(error) => {
                eprintln!("stale staging-check worker: {error}");
                delay = delay.saturating_mul(2).min(Duration::from_secs(300));
            }
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
    supervise_dispatch_observation_with_wakeup(
        app,
        db,
        capacity,
        policy,
        poll,
        complete_ci_only,
        Arc::new(tokio::sync::Notify::new()),
    )
    .await;
}

/// CI hints wake the observer after a debounce, while failed passes retain
/// their backoff. Lost or cross-process notifications fall back to polling.
pub async fn supervise_dispatch_observation_with_wakeup(
    app: Arc<App>,
    db: PathBuf,
    capacity: crate::scheduler::Capacity,
    policy: RepositoryPolicy,
    poll: Duration,
    complete_ci_only: bool,
    wakeup: Arc<tokio::sync::Notify>,
) {
    supervise_dispatch_observation_with_signals(
        app,
        db,
        capacity,
        policy,
        poll,
        complete_ci_only,
        ObservationWakeups {
            ci: wakeup,
            release: None,
        },
    )
    .await;
}

/// Process-local hints; durable state and periodic scans remain authoritative.
pub struct ObservationWakeups {
    pub ci: Arc<tokio::sync::Notify>,
    pub release: Option<Arc<tokio::sync::Notify>>,
}

pub async fn supervise_dispatch_observation_with_signals(
    app: Arc<App>,
    db: PathBuf,
    capacity: crate::scheduler::Capacity,
    policy: RepositoryPolicy,
    poll: Duration,
    complete_ci_only: bool,
    wakeups: ObservationWakeups,
) {
    let minimum = poll.max(Duration::from_secs(60));
    let wake_delay = poll.max(Duration::from_secs(5)).min(minimum);
    let mut delay = minimum;
    loop {
        if awaiting_repository_binding(&db).await {
            sleep(poll.max(Duration::from_secs(1))).await;
            continue;
        }
        if let Err(error) =
            observe_dispatches_with_completion(&app, &db, &capacity, &policy, complete_ci_only)
                .await
        {
            eprintln!("dispatch observation pass failed: {error}");
            delay = delay.saturating_mul(2).min(Duration::from_secs(300));
            sleep(delay).await;
        } else {
            if let Some(release) = &wakeups.release {
                release.notify_one();
            }
            delay = minimum;
            tokio::select! {
                _ = sleep(minimum) => {}
                _ = wakeups.ci.notified() => sleep(wake_delay).await,
            }
        }
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
        if awaiting_repository_binding(&db).await {
            sleep(poll.max(Duration::from_secs(1))).await;
            continue;
        }
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
    verification_policy: Option<&std::path::Path>,
) -> Result<Option<crate::coordinator::PreparedIntegration>, String> {
    let store = Store::open(db).map_err(|e| e.to_string())?;
    let batches = store
        .active_batches()
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter(|batch| {
            matches!(
                batch.state,
                crate::batch::State::Planned | crate::batch::State::Building
            )
        });
    prepare_candidates(batches, |batch| async move {
        crate::coordinator::prepare_github_from_provider(
            db,
            app,
            policy,
            &batch.id,
            policy_paths,
            verification_policy,
        )
        .await
    })
    .await
}

// Only skip errors known to belong to this batch. Global or uncertain errors
// stop the pass so a provider outage cannot cause a burst of repeated calls.
async fn prepare_candidates<I, F, Fut>(
    batches: I,
    mut prepare: F,
) -> Result<Option<crate::coordinator::PreparedIntegration>, String>
where
    I: IntoIterator<Item = crate::batch::Batch>,
    F: FnMut(crate::batch::Batch) -> Fut,
    Fut: std::future::Future<
            Output = Result<
                crate::coordinator::PreparedIntegration,
                crate::coordinator::ProviderPrepareError,
            >,
        >,
{
    use crate::{
        coordinator::{PrepareError, ProviderPrepareError},
        github::StagingError,
        integration::{IntegrationError, PolicyError, StageError},
    };
    let mut first_error = None;
    for batch in batches {
        let id = batch.id.clone();
        match prepare(batch).await {
            Ok(prepared) => return Ok(Some(prepared)),
            Err(
                error @ ProviderPrepareError::Prepare(
                    PrepareError::Integration(
                        IntegrationError::Conflict | IntegrationError::AlreadyIntegrated,
                    )
                    | PrepareError::Policy(PolicyError::Changed)
                    | PrepareError::ChangedWorkflow(_)
                    | PrepareError::GitHubStage(StagingError::Git(StageError::Conflict)),
                ),
            ) => {
                let message = format!("batch {id}: {error}");
                eprintln!("automatic staging skipped: {message}");
                first_error.get_or_insert(message);
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(None),
    }
}

pub async fn supervise_preparation(
    app: Arc<App>,
    db: PathBuf,
    policy: RepositoryPolicy,
    policy_paths: Vec<String>,
    verification_policy: Option<PathBuf>,
    poll: Duration,
) {
    let mut delay = poll;
    loop {
        match prepare_once(
            &app,
            &db,
            &policy,
            &policy_paths,
            verification_policy.as_deref(),
        )
        .await
        {
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

/// Try staged batches in queue order until one dispatches. A prepared batch
/// pinned to a busy host must not block another batch on an independent host.
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
    let batches = store
        .active_batches()
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter(|batch| {
            matches!(batch.state, State::Testing { .. })
                && !intents.iter().any(|intent| {
                    intent.reservation.id == batch.id
                        && !matches!(intent.state, DispatchState::Prepared)
                })
        });
    let mut no_capacity = false;
    let mut stale_ref = None;
    for batch in batches {
        match crate::dispatch::batch(db, app, policy, capacity, settings, &batch.id).await {
            Ok(sent @ crate::dispatch::Outcome::Sent { .. }) => return Ok(Some(sent)),
            Ok(crate::dispatch::Outcome::NoCapacity) => no_capacity = true,
            // Another worker claimed this batch between selection and dispatch.
            Ok(crate::dispatch::Outcome::AlreadyStarted) => {}
            Err(crate::dispatch::Error::Dispatch(crate::github::DispatchError::StaleRef)) => {
                // The mismatch belongs to this staging ref, not the repository or
                // another batch. Its prepared reservation remains held for repair.
                let error = format!("batch {} has a changed staging ref", batch.id);
                eprintln!("automatic dispatch skipped: {error}");
                stale_ref.get_or_insert(error);
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    if let Some(error) = stale_ref {
        return Err(error);
    }
    Ok(no_capacity.then_some(crate::dispatch::Outcome::NoCapacity))
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
    supervise_dispatch_with_wakeup(
        app,
        db,
        policy,
        capacity,
        settings,
        poll,
        Arc::new(tokio::sync::Notify::new()),
    )
    .await;
}

/// Notify the local observer only after the dispatch receipt is durably saved.
pub async fn supervise_dispatch_with_wakeup(
    app: Arc<App>,
    db: PathBuf,
    policy: RepositoryPolicy,
    capacity: crate::scheduler::Capacity,
    settings: crate::dispatch::Settings,
    poll: Duration,
    ci_wakeup: Arc<tokio::sync::Notify>,
) {
    let minimum = poll.max(Duration::from_secs(10));
    let mut delay = minimum;
    loop {
        match dispatch_once(&app, &db, &policy, &capacity, &settings).await {
            Ok(Some(crate::dispatch::Outcome::Sent { run, host })) => {
                eprintln!("dispatched run {run} to reserved host {host}");
                ci_wakeup.notify_one();
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

/// Revalidate completed or invalidated runs and runner inventory before
/// releasing CI-only reservations under the conservative topology.
pub async fn release_ci_only_once(
    app: &App,
    db: &std::path::Path,
    capacity: &crate::scheduler::Capacity,
    policy: &RepositoryPolicy,
) -> Result<usize, String> {
    use crate::{batch::State, scheduler::DispatchState, verification::Placement};
    crate::verification::check_release_topology(capacity).map_err(|e| e.to_string())?;
    let store = Store::open(db).map_err(|e| e.to_string())?;
    if store.repository_scope().map_err(|e| e.to_string())?
        != (policy.installation as u64, policy.repository as u64)
    {
        return Err("repository does not match the reconciled queue".into());
    }
    let mut scheduler =
        crate::scheduler::Scheduler::open(db, capacity.clone()).map_err(|e| e.to_string())?;
    let mut released = 0;
    let mut busy_host = None;
    for pending in scheduler.pending_dispatches().map_err(|e| e.to_string())? {
        let run_id = match pending.state {
            DispatchState::Accepted { run_id } | DispatchState::Observed { run_id } => run_id,
            _ => continue,
        };
        let Some((batch, _)) = store
            .batch(&pending.reservation.id)
            .map_err(|e| e.to_string())?
        else {
            continue;
        };
        let expected_conclusion = match &batch.state {
            State::CiComplete {
                run_id: completed_run,
                conclusion,
                ..
            } if *completed_run == run_id => Some(conclusion.as_str()),
            State::Invalidated { .. } => None,
            _ => continue,
        };
        if scheduler
            .receipt_source(&pending.reservation.id)
            .map_err(|e| e.to_string())?
            .as_deref()
            == Some("conflict")
        {
            if expected_conclusion.is_some() {
                continue;
            }
            let read = app
                .read_token(policy.installation as u64, policy.repository as u64)
                .await
                .map_err(|e| e.to_string())?;
            let inventory = app
                .runner_inventory_token(policy.installation as u64, policy.repository as u64)
                .await
                .map_err(|e| e.to_string())?;
            if matches!(
                app.resolve_dispatch_conflict(
                    &mut scheduler,
                    &read,
                    &inventory,
                    (&policy.owner, &policy.name),
                    &pending.reservation.id,
                    capacity
                )
                .await
                .map_err(|e| e.to_string())?,
                crate::github::ConflictResolution::Resolved
            ) {
                released += 1;
            }
            continue;
        }
        if scheduler
            .receipt_source(&pending.reservation.id)
            .map_err(|e| e.to_string())?
            .as_deref()
            != Some("direct")
        {
            continue;
        }
        let host = capacity
            .hosts
            .get(&pending.reservation.host)
            .ok_or("reserved host is missing")?;
        let token = app
            .read_token(policy.installation as u64, policy.repository as u64)
            .await
            .map_err(|e| e.to_string())?;
        let first = app
            .reconcile_dispatch(
                &mut scheduler,
                &token,
                &policy.owner,
                &policy.name,
                &pending.reservation.id,
            )
            .await
            .map_err(|e| e.to_string())?;
        let conclusion = first
            .run
            .conclusion
            .as_deref()
            .filter(|value| !value.is_empty());
        let placement = crate::verification::check_host_placement(&first, &host.runner_ids)
            .map_err(|e| e.to_string())?;
        let placement_valid = matches!(placement, Placement::Confirmed { .. })
            || (expected_conclusion.is_none()
                && matches!(placement, Placement::Waiting)
                && first.jobs.iter().all(|job| {
                    job.status == "completed" && job.conclusion.as_deref() == Some("skipped")
                }));
        if first.run.status != "completed"
            || conclusion.is_none()
            || expected_conclusion.is_some_and(|expected| conclusion != Some(expected))
            || !placement_valid
        {
            return Err("completed run or runner placement changed".into());
        }
        if matches!(placement, Placement::Confirmed { .. }) {
            scheduler
                .record_job_metrics(&pending.reservation.id, &first)
                .map_err(|e| e.to_string())?;
        }
        let inventory_token = app
            .runner_inventory_token(policy.installation as u64, policy.repository as u64)
            .await
            .map_err(|e| e.to_string())?;
        let runners = app
            .observe_repository_runners(&inventory_token, &policy.owner, &policy.name)
            .await
            .map_err(|e| e.to_string())?;
        match crate::verification::check_repository_host_idle(
            capacity,
            &runners,
            &pending.reservation.host,
        ) {
            Ok(()) => {}
            Err(crate::verification::InventoryError::InsufficientCapacity) => {
                let host = pending.reservation.host.clone();
                eprintln!("CI-only release is waiting for host {host} to become idle");
                busy_host.get_or_insert(host);
                continue;
            }
            Err(error) => return Err(error.to_string()),
        }
        let last = app
            .reconcile_dispatch(
                &mut scheduler,
                &token,
                &policy.owner,
                &policy.name,
                &pending.reservation.id,
            )
            .await
            .map_err(|e| e.to_string())?;
        if first != last {
            return Err("run changed during capacity revalidation".into());
        }
        if expected_conclusion.is_some() {
            scheduler
                .release_completed_ci_only(&pending.reservation.id, run_id)
                .map_err(|e| e.to_string())?;
        } else {
            scheduler
                .release_invalidated_ci_only(&pending.reservation.id, run_id)
                .map_err(|e| e.to_string())?;
        }
        released += 1;
    }
    if released == 0
        && let Some(host) = busy_host
    {
        return Err(format!("reserved host {host} is not idle and online"));
    }
    Ok(released)
}

pub async fn supervise_ci_release(
    app: Arc<App>,
    db: PathBuf,
    capacity: crate::scheduler::Capacity,
    policy: RepositoryPolicy,
    poll: Duration,
) {
    supervise_ci_release_with_wakeup(
        app,
        db,
        capacity,
        policy,
        poll,
        Arc::new(tokio::sync::Notify::new()),
    )
    .await;
}

/// Completion hints are advisory; release always repeats all provider and
/// topology checks. A missed hint is recovered by the normal periodic scan.
pub async fn supervise_ci_release_with_wakeup(
    app: Arc<App>,
    db: PathBuf,
    capacity: crate::scheduler::Capacity,
    policy: RepositoryPolicy,
    poll: Duration,
    wakeup: Arc<tokio::sync::Notify>,
) {
    let minimum = poll.max(Duration::from_secs(60));
    let wake_delay = poll.max(Duration::from_secs(5)).min(minimum);
    let mut delay = minimum;
    loop {
        if awaiting_repository_binding(&db).await {
            sleep(poll.max(Duration::from_secs(1))).await;
            continue;
        }
        match release_ci_only_once(&app, &db, &capacity, &policy).await {
            Ok(count) => {
                if count > 0 {
                    eprintln!("released {count} completed CI-only reservations");
                }
                delay = minimum;
                tokio::select! {
                    _ = sleep(minimum) => {}
                    _ = wakeup.notified() => sleep(wake_delay).await,
                }
            }
            Err(error) => {
                eprintln!("CI-only capacity revalidation failed: {error}");
                delay = delay.saturating_mul(2).min(Duration::from_secs(300));
                sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod preparation_tests {
    use super::prepare_candidates;
    use crate::{
        batch::{Batch, Candidate},
        coordinator::{PrepareError, PreparedIntegration, ProviderPrepareError},
        github::StagingError,
        integration::{StageError, StagingWorkflowError},
        store::Store,
    };
    use std::sync::{Arc, Mutex};

    fn queued_batches() -> (tempfile::TempDir, Vec<Batch>) {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path().join("queue.sqlite")).unwrap();
        for number in 1..=3 {
            let candidate = Candidate {
                number,
                head_sha: format!("head-{number}"),
            };
            store.observe_candidate(&candidate).unwrap();
            store
                .insert_batch(
                    &Batch::new(format!("batch-{number}"), "base".into(), vec![candidate]).unwrap(),
                )
                .unwrap();
        }
        (dir, store.active_batches().unwrap())
    }

    #[tokio::test]
    async fn batch_specific_failures_do_not_starve_later_preparations() {
        let (_dir, batches) = queued_batches();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let result = prepare_candidates(batches, |batch| {
            let seen = Arc::clone(&seen);
            async move {
                seen.lock().unwrap().push(batch.id.clone());
                match batch.id.as_str() {
                    "batch-1" => Err(ProviderPrepareError::Prepare(PrepareError::GitHubStage(
                        StagingError::Git(StageError::Conflict),
                    ))),
                    "batch-2" => Err(ProviderPrepareError::Prepare(
                        PrepareError::ChangedWorkflow(StagingWorkflowError::CreateTrigger(
                            ".github/workflows/unsafe.yml".into(),
                        )),
                    )),
                    _ => Ok(PreparedIntegration {
                        batch_id: batch.id,
                        sha: "staged-later".into(),
                        reference: "refs/heads/sluss/batch-3".into(),
                        policy_digest: "policy".into(),
                    }),
                }
            }
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.batch_id, "batch-3");
        assert_eq!(*seen.lock().unwrap(), ["batch-1", "batch-2", "batch-3"]);
    }

    #[tokio::test]
    async fn global_or_uncertain_errors_stop_the_pass_and_skipped_errors_remain_visible() {
        let (_dir, batches) = queued_batches();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let error = prepare_candidates(batches.clone(), |batch| {
            let seen = Arc::clone(&seen);
            async move {
                seen.lock().unwrap().push(batch.id.clone());
                if batch.id == "batch-1" {
                    Err(ProviderPrepareError::Prepare(PrepareError::GitHubStage(
                        StagingError::Git(StageError::Conflict),
                    )))
                } else {
                    Err(ProviderPrepareError::MissingPolicy)
                }
            }
        })
        .await
        .unwrap_err();
        assert!(error.contains("no usable policy files"));
        assert_eq!(*seen.lock().unwrap(), ["batch-1", "batch-2"]);

        let uncertain_seen = Arc::new(Mutex::new(Vec::new()));
        let error = prepare_candidates(batches.clone(), |batch| {
            let seen = Arc::clone(&uncertain_seen);
            async move {
                seen.lock().unwrap().push(batch.id);
                Err(ProviderPrepareError::Prepare(PrepareError::GitHubStage(
                    StagingError::Git(StageError::Uncertain),
                )))
            }
        })
        .await
        .unwrap_err();
        assert!(error.contains("result is uncertain"));
        assert_eq!(*uncertain_seen.lock().unwrap(), ["batch-1"]);

        let error = prepare_candidates(batches.into_iter().take(1), |_| async {
            Err(ProviderPrepareError::Prepare(PrepareError::GitHubStage(
                StagingError::Git(StageError::Conflict),
            )))
        })
        .await
        .unwrap_err();
        assert!(error.contains("batch batch-1"));
        assert!(error.contains("staging ref already points to a different commit"));
    }
}

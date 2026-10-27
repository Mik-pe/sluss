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

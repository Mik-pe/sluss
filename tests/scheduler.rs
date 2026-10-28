use sluss::scheduler::{Capacity, Host, Request, Scheduler, SchedulerError};
use std::{
    collections::BTreeMap,
    sync::{Arc, Barrier},
};

fn pools(entries: &[(&str, u32)]) -> BTreeMap<String, u32> {
    entries.iter().map(|(k, v)| ((*k).into(), *v)).collect()
}
fn capacity() -> Capacity {
    Capacity {
        pools: pools(&[("rust", 2), ("macos", 1), ("android", 1)]),
        hosts: BTreeMap::from([
            (
                "linux".into(),
                Host {
                    runner_ids: Default::default(),
                    slots: 1,
                    pools: pools(&[("rust", 2)]),
                },
            ),
            (
                "mac".into(),
                Host {
                    runner_ids: Default::default(),
                    slots: 1,
                    pools: pools(&[("rust", 1), ("macos", 1), ("android", 1)]),
                },
            ),
        ]),
    }
}
fn request(entries: &[(&str, u32)]) -> Request {
    Request {
        pools: pools(entries),
    }
}

#[test]
fn overlapping_pools_respect_physical_machine_and_global_budgets() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("scheduler.sqlite");
    let mut s = Scheduler::open(&db, capacity()).unwrap();
    let android = request(&[("android", 1)]);
    assert_eq!(s.reserve("a", &android).unwrap().unwrap().host, "mac");
    assert!(s.reserve("b", &request(&[("macos", 1)])).unwrap().is_none());
    assert_eq!(
        s.reserve("c", &request(&[("rust", 1)]))
            .unwrap()
            .unwrap()
            .host,
        "linux"
    );
    assert_eq!(s.reserve("a", &android).unwrap().unwrap().host, "mac");
    assert!(matches!(
        s.reserve("a", &request(&[("macos", 1)])),
        Err(SchedulerError::IdConflict)
    ));
    drop(s);
    let mut s = Scheduler::open(&db, capacity()).unwrap();
    assert!(s.reserve("b", &request(&[("macos", 1)])).unwrap().is_none());
    assert!(s.release("a").unwrap());
    assert!(matches!(
        s.reserve("a", &android),
        Err(SchedulerError::IdFinished)
    ));
    assert!(!s.release("a").unwrap());
    assert_eq!(
        s.reserve("b", &request(&[("macos", 1), ("rust", 1)]))
            .unwrap()
            .unwrap()
            .host,
        "mac"
    );
    assert!(s.reserve("d", &request(&[("rust", 1)])).unwrap().is_none());
}

#[test]
fn different_workers_cannot_oversubscribe_the_same_host() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("scheduler.sqlite");
    Scheduler::open(&db, capacity()).unwrap();
    let barrier = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|n| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut scheduler = Scheduler::open(db, capacity()).unwrap();
                barrier.wait();
                scheduler
                    .reserve(&format!("worker-{n}"), &request(&[("macos", 1)]))
                    .unwrap()
                    .is_some()
            })
        })
        .collect();
    assert_eq!(
        threads
            .into_iter()
            .map(|t| usize::from(t.join().unwrap()))
            .sum::<usize>(),
        1
    );
}

#[test]
fn config_changes_wait_for_live_reservations_and_invalidate_old_handles() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("scheduler.sqlite");
    let mut old = Scheduler::open(&db, capacity()).unwrap();
    old.reserve("job", &request(&[("rust", 1)]))
        .unwrap()
        .unwrap();
    let mut changed = capacity();
    changed.pools.insert("rust".into(), 1);
    assert!(matches!(
        Scheduler::open(&db, changed.clone()),
        Err(SchedulerError::ConfigBusy)
    ));
    old.release("job").unwrap();
    let mut new = Scheduler::open(&db, changed).unwrap();
    assert!(matches!(
        old.reserve("old", &request(&[("rust", 1)])),
        Err(SchedulerError::ConfigChanged)
    ));
    assert!(
        new.reserve("new", &request(&[("rust", 1)]))
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        new.reserve("impossible", &request(&[("rust", 2)])),
        Err(SchedulerError::Invalid)
    ));
}

fn dispatch_plan() -> sluss::scheduler::DispatchPlan {
    sluss::scheduler::DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
        integration_sha: "a".repeat(40),
        staging_ref: "refs/heads/sluss/batch-0123456789abcdef".into(),
        policy_digest: "b".repeat(64),
        required_jobs: std::collections::BTreeSet::from(["job-1-0".into()]),
        suites: std::collections::BTreeSet::from(["full".into()]),
        selection_policy_digest: None,
    }
}

#[test]
fn dispatch_intent_survives_crashes_and_cannot_release_capacity_early() {
    use sluss::scheduler::DispatchState;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("dispatch.sqlite");
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    let req = request(&[("macos", 1)]);
    let plan = dispatch_plan();
    assert!(
        scheduler
            .reserve_dispatch("job", &req, &plan)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        scheduler.pending_dispatches().unwrap()[0].state,
        DispatchState::Prepared
    );
    assert!(matches!(
        scheduler.release("job"),
        Err(SchedulerError::DispatchManaged)
    ));
    let mut changed = plan.clone();
    changed.integration_sha = "c".repeat(40);
    assert!(matches!(
        scheduler.reserve_dispatch("job", &req, &changed),
        Err(SchedulerError::IdConflict)
    ));
    assert!(scheduler.claim_dispatch("job").unwrap());
    drop(scheduler);
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    assert!(!scheduler.claim_dispatch("job").unwrap());
    assert_eq!(
        scheduler.pending_dispatches().unwrap()[0].state,
        DispatchState::Sending
    );
    assert!(scheduler.reserve("other", &req).unwrap().is_none());
    assert!(matches!(
        scheduler.finish_dispatch("job", 50),
        Err(SchedulerError::DispatchTransition)
    ));
    scheduler.observe_dispatch("job", 50).unwrap();
    scheduler.observe_dispatch("job", 50).unwrap();
    assert!(matches!(
        scheduler.observe_dispatch("job", 51),
        Err(SchedulerError::DispatchTransition)
    ));
    drop(scheduler);
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    assert_eq!(
        scheduler.pending_dispatches().unwrap()[0].state,
        DispatchState::Observed { run_id: 50 }
    );
    assert!(matches!(
        scheduler.finish_dispatch("job", 51),
        Err(SchedulerError::DispatchTransition)
    ));
    assert!(matches!(
        scheduler.finish_dispatch("job", 50),
        Err(SchedulerError::DispatchTransition)
    ));
    scheduler.record_receipt("job", 50).unwrap();
    scheduler.finish_dispatch("job", 50).unwrap();
    scheduler.finish_dispatch("job", 50).unwrap();
    assert!(scheduler.pending_dispatches().unwrap().is_empty());
    assert!(matches!(
        scheduler.reserve_dispatch("job", &req, &plan),
        Err(SchedulerError::IdFinished)
    ));
    assert!(scheduler.reserve("other", &req).unwrap().is_some());
}

#[test]
fn only_one_worker_claims_a_dispatch_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("dispatch.sqlite");
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    scheduler
        .reserve_dispatch("job", &request(&[("macos", 1)]), &dispatch_plan())
        .unwrap();
    drop(scheduler);
    let barrier = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut scheduler = Scheduler::open(db, capacity()).unwrap();
                barrier.wait();
                scheduler.claim_dispatch("job").unwrap()
            })
        })
        .collect();
    assert_eq!(
        threads
            .into_iter()
            .map(|t| usize::from(t.join().unwrap()))
            .sum::<usize>(),
        1
    );
}

#[test]
fn completed_job_metrics_are_idempotent_and_reject_conflicting_or_invalid_evidence() {
    use sluss::{
        github::{ActionsJob, ActionsRepository, ActionsRun, ActionsSnapshot},
        metrics::MetricsError,
        store::Store,
    };
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("metrics.sqlite");
    let mut configured = capacity();
    configured
        .hosts
        .get_mut("mac")
        .unwrap()
        .runner_ids
        .insert(41);
    let mut scheduler = Scheduler::open(&db, configured).unwrap();
    let plan = dispatch_plan();
    scheduler
        .reserve_dispatch("job", &request(&[("macos", 1)]), &plan)
        .unwrap()
        .unwrap();
    scheduler.claim_dispatch("job").unwrap();
    scheduler.record_receipt("job", 50).unwrap();
    scheduler.observe_dispatch("job", 50).unwrap();
    let mut snapshot = ActionsSnapshot {
        run: ActionsRun {
            id: 50,
            run_attempt: 1,
            workflow_id: 3,
            head_sha: "a".repeat(40),
            head_branch: Some("sluss/batch-0123456789abcdef".into()),
            path: ".github/workflows/ci.yml".into(),
            event: "workflow_dispatch".into(),
            status: "completed".into(),
            conclusion: Some("failure".into()),
            repository: ActionsRepository { id: 2 },
            head_repository: Some(ActionsRepository { id: 2 }),
        },
        jobs: vec![ActionsJob {
            id: 99,
            run_id: 50,
            head_sha: "a".repeat(40),
            name: "tests".into(),
            status: "completed".into(),
            conclusion: Some("failure".into()),
            started_at: Some("2026-01-01T00:00:30Z".into()),
            completed_at: Some("2026-01-01T00:02:00Z".into()),
            runner_id: Some(41),
        }],
    };
    assert_eq!(scheduler.record_job_metrics("job", &snapshot).unwrap(), 1);
    assert_eq!(scheduler.record_job_metrics("job", &snapshot).unwrap(), 0);
    snapshot.jobs[0].completed_at = Some("2026-01-01T00:03:00Z".into());
    assert!(matches!(
        scheduler.record_job_metrics("job", &snapshot),
        Err(SchedulerError::Metrics(MetricsError::ConflictingJob))
    ));
    snapshot.jobs[0].id = 100;
    snapshot.jobs[0].completed_at = Some("2026-01-01T00:00:00Z".into());
    assert!(matches!(
        scheduler.record_job_metrics("job", &snapshot),
        Err(SchedulerError::Metrics(MetricsError::InvalidJob))
    ));
    snapshot.jobs[0].completed_at = Some("bad timestamp".into());
    assert!(matches!(
        scheduler.record_job_metrics("job", &snapshot),
        Err(SchedulerError::Metrics(MetricsError::InvalidJob))
    ));
    snapshot.jobs[0].runner_id = Some(42);
    assert!(matches!(
        scheduler.record_job_metrics("job", &snapshot),
        Err(SchedulerError::Metrics(MetricsError::InvalidJob))
    ));
    let totals = Store::open(&db).unwrap().metrics().unwrap();
    assert_eq!(totals.completed_jobs, 1);
    assert_eq!(totals.runner_milliseconds, 90_000);
}

#[test]
fn conflicting_recovery_and_direct_receipt_stay_visible_and_reserve_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("dispatch-conflict.sqlite");
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    let req = request(&[("macos", 1)]);
    scheduler
        .reserve_dispatch("job", &req, &dispatch_plan())
        .unwrap()
        .unwrap();
    assert!(scheduler.claim_dispatch("job").unwrap());
    scheduler.observe_dispatch("job", 50).unwrap();
    assert!(matches!(
        scheduler.record_receipt("job", 51),
        Err(SchedulerError::DispatchReceiptConflict)
    ));
    assert_eq!(scheduler.receipt_conflict("job").unwrap(), Some((50, 51)));
    assert_eq!(
        scheduler.receipt_source("job").unwrap().as_deref(),
        Some("conflict")
    );
    drop(scheduler);
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    assert_eq!(scheduler.receipt_conflict("job").unwrap(), Some((50, 51)));
    assert!(matches!(
        scheduler.observe_dispatch("job", 50),
        Err(SchedulerError::DispatchReceiptConflict)
    ));
    assert!(scheduler.finish_dispatch("job", 50).is_err());
    assert!(scheduler.finish_dispatch("job", 51).is_err());
    assert!(scheduler.reserve("other", &req).unwrap().is_none());
}

#[test]
fn one_provider_run_cannot_be_recovered_into_two_intents() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("dispatch-unique.sqlite");
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    scheduler
        .reserve_dispatch("first", &request(&[("rust", 1)]), &dispatch_plan())
        .unwrap()
        .unwrap();
    scheduler
        .reserve_dispatch("second", &request(&[("macos", 1)]), &dispatch_plan())
        .unwrap()
        .unwrap();
    assert!(scheduler.claim_dispatch("first").unwrap());
    assert!(scheduler.claim_dispatch("second").unwrap());
    scheduler.observe_dispatch("first", 50).unwrap();
    assert!(scheduler.observe_dispatch("second", 50).is_err());
    let second = scheduler
        .pending_dispatches()
        .unwrap()
        .into_iter()
        .find(|item| item.reservation.id == "second")
        .unwrap();
    assert_eq!(second.state, sluss::scheduler::DispatchState::Sending);
}

#[test]
fn legacy_finished_recovery_conflict_is_not_reported_as_an_active_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("legacy-conflict.sqlite");
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    scheduler
        .reserve_dispatch("job", &request(&[("macos", 1)]), &dispatch_plan())
        .unwrap()
        .unwrap();
    scheduler.claim_dispatch("job").unwrap();
    scheduler.observe_dispatch("job", 50).unwrap();
    // Simulate a preview database written by the former permissive finish API.
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute("DELETE FROM capacity_reservations WHERE id='job'", [])
        .unwrap();
    conn.execute(
        "UPDATE dispatch_intents SET state='finished' WHERE id='job'",
        [],
    )
    .unwrap();
    assert!(matches!(
        scheduler.record_receipt("job", 51),
        Err(SchedulerError::DispatchReceiptConflict)
    ));
    assert_eq!(scheduler.receipt_conflict("job").unwrap(), Some((50, 51)));
    assert!(scheduler.pending_dispatches().unwrap().is_empty());
}

#[test]
fn legacy_duplicate_run_receipts_block_scheduler_without_deleting_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("duplicates.sqlite");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch("CREATE TABLE dispatch_intents (id TEXT PRIMARY KEY NOT NULL, plan TEXT NOT NULL, state TEXT NOT NULL, run_id TEXT); INSERT INTO dispatch_intents VALUES('one','{}','observed','50'),('two','{}','observed','50');").unwrap();
    assert!(Scheduler::open(&db, capacity()).is_err());
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM dispatch_intents WHERE run_id='50'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);
}

#[test]
fn dispatch_overview_shows_uncertain_and_orphaned_work_without_writing() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.sqlite");
    assert!(Scheduler::dispatch_overview(&missing).is_err());
    assert!(Scheduler::rerun_scan_issues(&missing).is_err());
    assert!(!missing.exists());
    let fresh = dir.path().join("fresh.sqlite");
    sluss::store::Store::open(&fresh).unwrap();
    assert!(Scheduler::dispatch_overview(&fresh).unwrap().is_empty());
    assert!(Scheduler::rerun_scan_issues(&fresh).unwrap().is_empty());
    let partial = dir.path().join("partial.sqlite");
    let partial_db = rusqlite::Connection::open(&partial).unwrap();
    partial_db.execute_batch("CREATE TABLE dispatch_receipt_conflicts(id TEXT); INSERT INTO dispatch_receipt_conflicts VALUES ('orphan');").unwrap();
    drop(partial_db);
    assert!(matches!(
        Scheduler::dispatch_overview(&partial),
        Err(SchedulerError::Corrupt)
    ));
    let db = dir.path().join("queue.sqlite");
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    scheduler
        .reserve("standalone", &request(&[("macos", 1)]))
        .unwrap()
        .unwrap();
    drop(scheduler);
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute("INSERT INTO dispatch_intents(id,plan,state,run_id) VALUES('uncertain','{}','sending',NULL)", []).unwrap();
    conn.execute(
        "INSERT INTO capacity_reservations(id,host,request) VALUES('uncertain','linux','{}')",
        [],
    )
    .unwrap();
    conn.execute("INSERT INTO dispatch_intents(id,plan,state,run_id) VALUES('legacy-conflict','{}','finished','41')", []).unwrap();
    conn.execute("INSERT INTO dispatch_receipt_conflicts(id,recovered_run,direct_run) VALUES('legacy-conflict','41','42')", []).unwrap();
    conn.execute("INSERT INTO dispatch_receipt_conflicts(id,recovered_run,direct_run) VALUES('orphan-conflict','51','52')", []).unwrap();
    conn.execute("INSERT INTO dispatch_intents(id,plan,state,run_id) VALUES('terminal','{}','finished','90')", []).unwrap();
    conn.execute(
        "INSERT INTO capacity_reservations(id,host,request) VALUES('terminal','linux','{}')",
        [],
    )
    .unwrap();
    let schema_before: i64 = conn
        .query_row("PRAGMA schema_version", [], |r| r.get(0))
        .unwrap();
    drop(conn);
    let rows = Scheduler::dispatch_overview(&db).unwrap();
    assert_eq!(rows.len(), 5);
    let uncertain = rows.iter().find(|row| row.id == "uncertain").unwrap();
    assert_eq!(uncertain.state.as_deref(), Some("sending"));
    assert_eq!(uncertain.host.as_deref(), Some("linux"));
    assert!(uncertain.run_id.is_none());
    let conflict = rows.iter().find(|row| row.id == "legacy-conflict").unwrap();
    assert_eq!(
        conflict.conflicting_runs.as_ref(),
        Some(&("41".into(), "42".into()))
    );
    assert!(conflict.host.is_none());
    let orphan = rows.iter().find(|row| row.id == "orphan-conflict").unwrap();
    assert!(orphan.state.is_none());
    assert_eq!(
        orphan.conflicting_runs.as_ref(),
        Some(&("51".into(), "52".into()))
    );
    let standalone = rows.iter().find(|row| row.id == "standalone").unwrap();
    assert!(standalone.state.is_none());
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .args(["--db", db.to_str().unwrap(), "dispatches"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("uncertain") && text.contains("send may have occurred"));
    assert!(text.contains("legacy-conflict") && text.contains("conflicting run receipts"));
    assert!(text.contains("standalone") && text.contains("record is missing"));
    assert!(text.contains("terminal dispatch still holds a reservation"));
    let conn = rusqlite::Connection::open(&db).unwrap();
    let schema_after: i64 = conn
        .query_row("PRAGMA schema_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(schema_before, schema_after);
}

#[test]
fn rerun_metrics_use_direct_run_and_original_host_even_after_release() {
    use sluss::{
        github::{ActionsJob, ActionsRepository, ActionsRun, ActionsSnapshot},
        store::Store,
    };
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("rerun.sqlite");
    let mut configured = capacity();
    configured
        .hosts
        .get_mut("mac")
        .unwrap()
        .runner_ids
        .insert(41);
    let mut scheduler = Scheduler::open(&db, configured).unwrap();
    scheduler
        .reserve_dispatch("job", &request(&[("macos", 1)]), &dispatch_plan())
        .unwrap()
        .unwrap();
    scheduler.claim_dispatch("job").unwrap();
    scheduler.record_receipt("job", 50).unwrap();
    scheduler.observe_dispatch("job", 50).unwrap();
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute(
        "INSERT INTO dispatch_workflows(id,path) VALUES('job','.github/workflows/ci.yml')",
        [],
    )
    .unwrap();
    drop(conn);
    let mut snapshot = ActionsSnapshot {
        run: ActionsRun {
            id: 50,
            run_attempt: 1,
            workflow_id: 3,
            head_sha: "a".repeat(40),
            head_branch: Some("sluss/batch-0123456789abcdef".into()),
            path: ".github/workflows/ci.yml".into(),
            event: "workflow_dispatch".into(),
            status: "completed".into(),
            conclusion: Some("failure".into()),
            repository: ActionsRepository { id: 2 },
            head_repository: Some(ActionsRepository { id: 2 }),
        },
        jobs: vec![ActionsJob {
            id: 99,
            run_id: 50,
            head_sha: "a".repeat(40),
            name: "tests".into(),
            status: "completed".into(),
            conclusion: Some("failure".into()),
            started_at: Some("2026-01-01T00:00:30Z".into()),
            completed_at: Some("2026-01-01T00:02:00Z".into()),
            runner_id: Some(41),
        }],
    };
    assert_eq!(scheduler.record_job_metrics("job", &snapshot).unwrap(), 1);
    scheduler.finish_dispatch("job", 50).unwrap();
    drop(scheduler);
    let mut scheduler = Scheduler::open(&db, capacity_with_runner()).unwrap();
    assert_eq!(scheduler.rerun_metric_run("job").unwrap(), 50);
    snapshot.run.run_attempt = 2;
    snapshot.run.conclusion = Some("success".into());
    snapshot.jobs[0].id = 100;
    snapshot.jobs[0].conclusion = Some("success".into());
    snapshot.jobs[0].started_at = Some("2026-01-01T00:03:00Z".into());
    snapshot.jobs[0].completed_at = Some("2026-01-01T00:05:00Z".into());
    snapshot.jobs[0].runner_id = Some(42);
    assert!(scheduler.record_rerun_metrics("job", &snapshot).is_err());
    snapshot.jobs[0].runner_id = Some(41);
    snapshot.run.path = ".github/workflows/other.yml".into();
    assert!(scheduler.record_rerun_metrics("job", &snapshot).is_err());
    snapshot.run.path = ".github/workflows/ci.yml".into();
    snapshot.run.status = "in_progress".into();
    assert!(scheduler.record_rerun_metrics("job", &snapshot).is_err());
    snapshot.run.status = "completed".into();
    snapshot.jobs[0].status = "in_progress".into();
    assert!(scheduler.record_rerun_metrics("job", &snapshot).is_err());
    snapshot.jobs[0].status = "completed".into();
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute(
        "UPDATE dispatch_receipt_sources SET source='recovered' WHERE id='job'",
        [],
    )
    .unwrap();
    assert!(scheduler.rerun_metric_run("job").is_err());
    assert!(scheduler.record_rerun_metrics("job", &snapshot).is_err());
    conn.execute(
        "UPDATE dispatch_receipt_sources SET source='direct' WHERE id='job'",
        [],
    )
    .unwrap();
    drop(conn);
    assert_eq!(scheduler.record_rerun_metrics("job", &snapshot).unwrap(), 1);
    assert_eq!(scheduler.record_rerun_metrics("job", &snapshot).unwrap(), 0);
    snapshot.jobs[0].completed_at = Some("2026-01-01T00:06:00Z".into());
    assert!(scheduler.record_rerun_metrics("job", &snapshot).is_err());
    let totals = Store::open(&db).unwrap().metrics().unwrap();
    assert_eq!(
        (totals.completed_jobs, totals.runner_milliseconds),
        (2, 210_000)
    );
    assert_eq!((totals.rerun_jobs, totals.rerun_milliseconds), (1, 120_000));
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .args(["--db", db.to_str().unwrap(), "metrics"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("observed rerun jobs: 1"));
    assert!(text.contains("observed rerun runner-minutes: 2.00"));
}

fn capacity_with_runner() -> Capacity {
    let mut configured = capacity();
    configured
        .hosts
        .get_mut("mac")
        .unwrap()
        .runner_ids
        .insert(41);
    configured
}

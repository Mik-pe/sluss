use sluss::{
    batch::{Batch, Candidate},
    github::{ActionsJob, ActionsRepository, ActionsRun, ActionsSnapshot},
    scheduler::{Capacity, DispatchPlan, Host, Request, Scheduler},
    store::{AdmissionCandidate, PlanningSettings, Store},
    webhook::Inbox,
};
use std::{collections::BTreeMap, time::Duration};

fn settings() -> PlanningSettings {
    PlanningSettings {
        max_batch_size: 4,
        adaptive_batching: true,
        max_active_batches: 1,
        debounce: Duration::ZERO,
        batch_window: Duration::ZERO,
    }
}
fn capacity() -> Capacity {
    Capacity {
        pools: [("rust".into(), 1)].into(),
        hosts: [(
            "sluss-host-test".into(),
            Host {
                slots: 1,
                runner_ids: [7].into(),
                pools: [("rust".into(), 1)].into(),
            },
        )]
        .into(),
    }
}
fn queue(store: &mut Store, inbox: &mut Inbox, serial: u64, count: u64) {
    inbox
        .record(
            &format!("delivery-{serial}"),
            "pull_request",
            &[serial as u8; 32],
            1,
            2,
        )
        .unwrap();
    let job = inbox.pending().unwrap().remove(0);
    let candidates: Vec<_> = (1..=count)
        .map(|n| AdmissionCandidate {
            candidate: Candidate {
                number: serial * 100 + n,
                head_sha: format!("{:040x}", serial * 100 + n),
            },
            evidence_digest: format!("{:064x}", serial * 100 + n),
        })
        .collect();
    store
        .apply_admission_reconciliation_with_base(
            &job,
            &"f".repeat(64),
            &candidates,
            &"a".repeat(40),
            "main",
        )
        .unwrap();
}
fn plan(store: &mut Store) -> Batch {
    store
        .plan_observed_batch(1, 2, &"f".repeat(64), "main", &settings())
        .unwrap()
        .unwrap()
}
fn finish(
    store: &mut Store,
    scheduler: &mut Scheduler,
    batch: &Batch,
    run: u64,
    conclusion: &str,
    seconds: u32,
) {
    finish_with_release(store, scheduler, batch, run, conclusion, seconds, true);
}
fn finish_with_release(
    store: &mut Store,
    scheduler: &mut Scheduler,
    batch: &Batch,
    run: u64,
    conclusion: &str,
    seconds: u32,
    release: bool,
) {
    finish_with_workflow(
        store, scheduler, batch, run, conclusion, seconds, release, 3,
    );
}
#[allow(clippy::too_many_arguments)]
fn finish_with_workflow(
    store: &mut Store,
    scheduler: &mut Scheduler,
    batch: &Batch,
    run: u64,
    conclusion: &str,
    seconds: u32,
    release: bool,
    workflow_id: u64,
) {
    let id = &batch.id;
    store
        .bind_policy_paths(id, &[".github/workflows/ci.yml".into()])
        .unwrap();
    store.start_integration(id).unwrap();
    store
        .finish_integration(id, &"b".repeat(40), &"c".repeat(64))
        .unwrap();
    let plan = DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id,
        integration_sha: "b".repeat(40),
        staging_ref: format!("refs/heads/sluss/{id}"),
        policy_digest: "c".repeat(64),
        required_jobs: ["test".into()].into(),
        suites: ["full".into()].into(),
        selection_policy_digest: None,
    };
    scheduler
        .reserve_batch_dispatch(
            id,
            &Request {
                pools: BTreeMap::from([("rust".into(), 1)]),
            },
            &plan,
        )
        .unwrap()
        .unwrap();
    scheduler
        .claim_batch_dispatch(id, ".github/workflows/ci.yml")
        .unwrap();
    scheduler.record_receipt(id, run).unwrap();
    scheduler.observe_dispatch(id, run).unwrap();
    let snapshot = ActionsSnapshot {
        run: ActionsRun {
            id: run,
            run_attempt: 1,
            workflow_id,
            head_sha: "b".repeat(40),
            head_branch: Some(format!("sluss/{id}")),
            path: ".github/workflows/ci.yml".into(),
            event: "workflow_dispatch".into(),
            status: "completed".into(),
            conclusion: Some(conclusion.into()),
            repository: ActionsRepository { id: 2 },
            head_repository: Some(ActionsRepository { id: 2 }),
        },
        jobs: vec![ActionsJob {
            id: run + 1000,
            run_id: run,
            head_sha: "b".repeat(40),
            name: "test".into(),
            status: "completed".into(),
            conclusion: Some(conclusion.into()),
            started_at: Some("2026-01-01T00:00:00Z".into()),
            completed_at: Some(format!("2026-01-01T00:00:{seconds:02}Z")),
            runner_id: Some(7),
        }],
    };
    scheduler.record_ci_observation(id, &snapshot).unwrap();
    scheduler.record_job_metrics(id, &snapshot).unwrap();
    scheduler.complete_ci_only(id).unwrap();
    scheduler.complete_ci_only(id).unwrap(); // repeated completion cannot double-count
    if release {
        scheduler.release_completed_ci_only(id, run).unwrap();
    }
}

fn finish_lanes(
    store: &mut Store,
    scheduler: &mut Scheduler,
    batch: &Batch,
    run_base: u64,
    conclusion: &str,
    seconds: [Option<u32>; 2],
) -> (String, ActionsSnapshot) {
    finish_lanes_with_workflow(store, scheduler, batch, run_base, conclusion, seconds, 3)
}

#[allow(clippy::too_many_arguments)]
fn finish_lanes_with_workflow(
    store: &mut Store,
    scheduler: &mut Scheduler,
    batch: &Batch,
    run_base: u64,
    conclusion: &str,
    seconds: [Option<u32>; 2],
    workflow_id: u64,
) -> (String, ActionsSnapshot) {
    use sluss::lanes::{Lane, Policy};
    let id = &batch.id;
    store
        .bind_policy_paths(id, &[".github/workflows/ci.yml".into()])
        .unwrap();
    store.start_integration(id).unwrap();
    store
        .finish_integration(id, &"b".repeat(40), &"c".repeat(64))
        .unwrap();
    let suites = ["compile".into(), "test".into()].into();
    store
        .record_selection(
            id,
            &sluss::selective::FrozenSelection {
                base_sha: batch.base_sha.clone(),
                integration_sha: "b".repeat(40),
                policy_digest: "d".repeat(64),
                selection: sluss::selective::Selection::Selected {
                    suites,
                    components: ["all".into()].into(),
                },
            },
        )
        .unwrap();
    let request = Request {
        pools: [("rust".into(), 1)].into(),
    };
    let lane = |name: &str| Lane {
        workflow_id,
        suites: [name.into()].into(),
        required_jobs: [format!("{name}-job")].into(),
        request: request.clone(),
    };
    let policy = Policy {
        version: 1,
        lanes: [
            ("compile".into(), lane("compile")),
            ("test".into(), lane("test")),
        ]
        .into(),
    };
    let template = DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 0,
        integration_sha: "b".repeat(40),
        staging_ref: format!("refs/heads/sluss/{id}"),
        policy_digest: "c".repeat(64),
        required_jobs: Default::default(),
        suites: ["compile".into(), "test".into()].into(),
        selection_policy_digest: Some("d".repeat(64)),
    };
    let plan = policy
        .plan(
            template,
            &["compile".into(), "test".into()].into(),
            &capacity(),
        )
        .unwrap();
    scheduler.freeze_lane_plan(id, &plan).unwrap();
    let eligible = ["sluss-host-test".into()].into();
    let mut first = None;
    for (n, name) in ["compile", "test"].into_iter().enumerate() {
        let run = run_base + n as u64;
        let result = if n == 0 { conclusion } else { "success" };
        let reservation = scheduler
            .reserve_lane_dispatch_on(id, name, &eligible)
            .unwrap()
            .unwrap();
        scheduler
            .claim_batch_dispatch(&reservation.id, ".github/workflows/ci.yml")
            .unwrap();
        scheduler.record_receipt(&reservation.id, run).unwrap();
        scheduler.observe_dispatch(&reservation.id, run).unwrap();
        let snapshot = ActionsSnapshot {
            run: ActionsRun {
                id: run,
                run_attempt: 1,
                workflow_id,
                head_sha: "b".repeat(40),
                head_branch: Some(format!("sluss/{id}")),
                path: ".github/workflows/ci.yml".into(),
                event: "workflow_dispatch".into(),
                status: "completed".into(),
                conclusion: Some(result.into()),
                repository: ActionsRepository { id: 2 },
                head_repository: Some(ActionsRepository { id: 2 }),
            },
            jobs: vec![ActionsJob {
                id: run + 1000,
                run_id: run,
                head_sha: "b".repeat(40),
                name: format!("{name}-job"),
                status: "completed".into(),
                conclusion: Some(result.into()),
                started_at: seconds[n].map(|_| "2026-01-01T00:00:00Z".into()),
                completed_at: seconds[n].map(|value| format!("2026-01-01T00:00:{value:02}Z")),
                runner_id: Some(7),
            }],
        };
        scheduler
            .record_ci_observation(&reservation.id, &snapshot)
            .unwrap();
        if seconds[n].is_some() {
            scheduler
                .record_job_metrics(&reservation.id, &snapshot)
                .unwrap();
        }
        scheduler
            .release_completed_lane(&reservation.id, run)
            .unwrap();
        if n == 0 {
            first = Some((reservation.id, snapshot));
        }
    }
    assert!(scheduler.complete_lanes_ci_only(id).unwrap());
    first.unwrap()
}

#[test]
fn adaptive_lanes_sum_runner_time_and_observed_reruns_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lanes-adaptive.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    let mut first = None;
    for serial in 1..=3 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        let result = finish_lanes(
            &mut store,
            &mut scheduler,
            &batch,
            serial * 10,
            "success",
            [Some(10), Some(20)],
        );
        if first.is_none() {
            first = Some(result);
        }
    }
    let history = Store::adaptive_history(&db).unwrap();
    assert_eq!(history.len(), 3);
    assert!(history.iter().all(|event| event.runner_ms == Some(30_000)));
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 3);
    let (lane_id, mut rerun) = first.unwrap();
    rerun.run.run_attempt = 2;
    rerun.jobs[0].id += 10_000;
    rerun.jobs[0].completed_at = Some("2026-01-01T00:00:50Z".into());
    assert_eq!(scheduler.record_rerun_metrics(&lane_id, &rerun).unwrap(), 1);
    assert_eq!(scheduler.record_rerun_metrics(&lane_id, &rerun).unwrap(), 0);
    drop(scheduler);
    let history = Store::adaptive_history(&db).unwrap();
    assert_eq!(
        history
            .iter()
            .map(|event| event.observed_rerun_ms)
            .sum::<u64>(),
        50_000
    );
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    queue(&mut store, &mut inbox, 4, 4);
    let batch = plan(&mut store);
    finish_lanes(
        &mut store,
        &mut scheduler,
        &batch,
        40,
        "success",
        [Some(10), None],
    );
    assert_eq!(Store::adaptive_history(&db).unwrap()[0].runner_ms, None);
}

#[test]
fn expensive_lane_reruns_change_the_next_batch_size() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lane-rerun-cost.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    for serial in 1..=3 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        assert_eq!(batch.candidates.len(), 2);
        finish_lanes(
            &mut store,
            &mut scheduler,
            &batch,
            serial * 10,
            "success",
            [Some(10), Some(2)],
        );
    }
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 3);
    for serial in 4..=6 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        assert_eq!(batch.candidates.len(), 3);
        let (id, mut snapshot) = finish_lanes(
            &mut store,
            &mut scheduler,
            &batch,
            serial * 10,
            "success",
            [Some(10), Some(5)],
        );
        if serial == 4 {
            snapshot.run.run_attempt = 2;
            snapshot.jobs[0].id += 10_000;
            snapshot.jobs[0].completed_at = Some("2026-01-01T00:00:30Z".into());
            scheduler.record_rerun_metrics(&id, &snapshot).unwrap();
        }
    }
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 2);
    assert_eq!(
        Store::adaptive_history(&db)
            .unwrap()
            .iter()
            .map(|event| event.observed_rerun_ms)
            .sum::<u64>(),
        30_000
    );
}

#[test]
fn adaptive_lane_samples_do_not_mix_changed_workflow_policies() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lane-policy-change.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    for serial in 1..=3 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        finish_lanes_with_workflow(
            &mut store,
            &mut scheduler,
            &batch,
            serial * 10,
            "success",
            [Some(10), Some(10)],
            if serial == 3 { 4 } else { 3 },
        );
    }
    let status = store.adaptive_status().unwrap().unwrap();
    assert_eq!(status.limit, 2);
    assert_eq!(status.success_streak, 1);
    let history = Store::adaptive_history(&db).unwrap();
    assert_ne!(history[0].policy_key, history[1].policy_key);
}

#[test]
fn adaptive_lane_bisection_counts_all_child_jobs_and_runner_time() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lanes-bisection.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    store.bind_ci_bisection(true).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    queue(&mut store, &mut inbox, 1, 2);
    let root = plan(&mut store);
    finish_lanes(
        &mut store,
        &mut scheduler,
        &root,
        10,
        "failure",
        [Some(10), Some(20)],
    );
    let left = plan(&mut store);
    let (lane_id, mut rerun) = finish_lanes(
        &mut store,
        &mut scheduler,
        &left,
        20,
        "failure",
        [Some(10), Some(20)],
    );
    rerun.run.run_attempt = 2;
    rerun.jobs[0].id += 10_000;
    rerun.jobs[0].completed_at = Some("2026-01-01T00:00:40Z".into());
    scheduler.record_rerun_metrics(&lane_id, &rerun).unwrap();
    let right = plan(&mut store);
    finish_lanes(
        &mut store,
        &mut scheduler,
        &right,
        30,
        "success",
        [Some(10), Some(20)],
    );
    let history = Store::adaptive_history(&db).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].runner_ms, Some(30_000));
    assert_eq!(history[0].recovery_jobs, 4);
    assert_eq!(history[0].recovery_runner_ms, 60_000);
    assert_eq!(history[0].observed_rerun_ms, 40_000);
    assert!(history[0].recovery_settled);
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute("DROP TABLE adaptive_split_job_counts_v1", [])
        .unwrap();
    let incomplete = Store::adaptive_history(&db).unwrap();
    assert!(!incomplete[0].recovery_settled);
    assert_eq!(incomplete[0].recovery_jobs, 0);
}

#[test]
fn adapts_measured_cost_and_failures_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("adaptive.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    for serial in 1..=3 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        assert_eq!(batch.candidates.len(), 2);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 12);
    }
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 3);
    drop(store);
    drop(scheduler);
    let mut store = Store::open(&db).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    for serial in 4..=6 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        assert_eq!(batch.candidates.len(), 3);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 30);
    }
    let status = store.adaptive_status().unwrap().unwrap();
    assert_eq!(
        (
            status.limit,
            status.increase_threshold,
            status.recorded_samples
        ),
        (2, 6, 6)
    );
    for serial in 7..=12 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        assert_eq!(batch.candidates.len(), 2);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 12);
    }
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 2);
    queue(&mut store, &mut inbox, 13, 4);
    let batch = plan(&mut store);
    assert_eq!(batch.candidates.len(), 2);
    finish(&mut store, &mut scheduler, &batch, 13, "failure", 30);
    let status = store.adaptive_status().unwrap().unwrap();
    assert_eq!(
        (
            status.limit,
            status.increase_threshold,
            status.recorded_samples
        ),
        (1, 6, 13)
    );
    let events = Store::adaptive_history(&db).unwrap();
    assert_eq!(events.len(), 13);
    assert_eq!(
        (
            events[0].conclusion.as_str(),
            events[0].limit_before,
            events[0].limit_after,
            events[0].runner_ms
        ),
        ("failure", 2, 1, Some(30_000))
    );
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&db)
        .arg("adaptive-history")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("limit 2 -> 1"));
    queue(&mut store, &mut inbox, 14, 4);
    assert_eq!(plan(&mut store).candidates.len(), 1);
}

fn rerun_snapshot(batch: &Batch, run: u64) -> ActionsSnapshot {
    ActionsSnapshot {
        run: ActionsRun {
            id: run,
            run_attempt: 2,
            workflow_id: 3,
            head_sha: "b".repeat(40),
            head_branch: Some(format!("sluss/{}", batch.id)),
            path: ".github/workflows/ci.yml".into(),
            event: "workflow_dispatch".into(),
            status: "completed".into(),
            conclusion: Some("success".into()),
            repository: ActionsRepository { id: 2 },
            head_repository: Some(ActionsRepository { id: 2 }),
        },
        jobs: vec![ActionsJob {
            id: 2000 + run,
            run_id: run,
            head_sha: "b".repeat(40),
            name: "test".into(),
            status: "completed".into(),
            conclusion: Some("success".into()),
            started_at: Some("2026-01-01T00:01:00Z".into()),
            completed_at: Some("2026-01-01T00:01:30Z".into()),
            runner_id: Some(7),
        }],
    }
}

#[test]
fn observed_reruns_contribute_to_batch_size_comparisons() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("adaptive-rerun.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    for serial in 1..=3 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 12);
    }
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 3);
    let mut rerun_batch = String::new();
    for serial in 4..=6 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        assert_eq!(batch.candidates.len(), 3);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 15);
        if serial == 4 {
            rerun_batch = batch.id.clone();
            let snapshot = rerun_snapshot(&batch, serial);
            assert_eq!(
                scheduler
                    .record_rerun_metrics(&batch.id, &snapshot)
                    .unwrap(),
                1
            );
            assert_eq!(
                scheduler
                    .record_rerun_metrics(&batch.id, &snapshot)
                    .unwrap(),
                0
            );
            drop(scheduler);
            drop(store);
            store = Store::open(&db).unwrap();
            scheduler = Scheduler::open(&db, capacity()).unwrap();
        }
    }
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 2);
    let event = Store::adaptive_history(&db)
        .unwrap()
        .into_iter()
        .find(|event| event.batch_id == rerun_batch)
        .unwrap();
    assert_eq!(
        (event.observed_rerun_jobs, event.observed_rerun_ms),
        (1, 30_000)
    );
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&db)
        .arg("adaptive-history")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("observed reruns: 1 job, 0.50 runner-minutes")
    );
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute(
        "UPDATE ci_job_metrics SET body=json_set(body,'$.head_sha',?1) WHERE job_id='2004'",
        ["e".repeat(40)],
    )
    .unwrap();
    assert!(Store::adaptive_history(&db).is_err());
}

#[test]
fn disabled_adaptation_preserves_legacy_configuration_digest() {
    let mut disabled = settings();
    disabled.adaptive_batching = false;
    let serialized = serde_json::to_string(&disabled).unwrap();
    assert!(!serialized.contains("adaptive_batching"));
    let restored: PlanningSettings = serde_json::from_str(&serialized).unwrap();
    assert_eq!(disabled, restored);
    assert_eq!(disabled.digest().unwrap(), restored.digest().unwrap());
    let mut bad = settings();
    bad.max_active_batches = 2;
    assert!(bad.digest().is_err());
}

#[test]
fn adaptive_history_is_read_only_on_missing_or_legacy_databases() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.sqlite");
    assert!(Store::adaptive_history(&missing).is_err());
    assert!(!missing.exists());
    let legacy = dir.path().join("legacy.sqlite");
    let conn = rusqlite::Connection::open(&legacy).unwrap();
    conn.execute("CREATE TABLE untouched(id INTEGER)", [])
        .unwrap();
    assert!(Store::adaptive_history(&legacy).unwrap().is_empty());
    let tables: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(tables, 1);
}

#[test]
fn old_adaptive_history_does_not_invent_investigation_costs() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("old.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    store.bind_ci_bisection(true).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    queue(&mut store, &mut inbox, 1, 4);
    let batch = plan(&mut store);
    finish(&mut store, &mut scheduler, &batch, 1, "failure", 12);
    drop(scheduler);
    drop(store);
    drop(inbox);
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute("DROP TABLE adaptive_split_costs_v1", [])
        .unwrap();
    let history = Store::adaptive_history(&db).unwrap();
    assert_eq!(history.len(), 1);
    assert!(!history[0].recovery_settled);
    let table_exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='adaptive_split_costs_v1')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!table_exists);
}

#[test]
fn bisection_mode_migration_drains_work_preserves_history_and_rejects_stale_revision() {
    use sluss::store::StoreError;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("migration.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    queue(&mut store, &mut inbox, 1, 4);
    store.bind_ci_bisection(true).unwrap();
    assert_eq!(store.ci_bisection_mode().unwrap(), Some((true, 0)));
    assert!(matches!(
        store.migrate_ci_bisection(0, false, 1, 2, "main"),
        Err(StoreError::BisectionMigrationNeedsPause)
    ));
    let root = plan(&mut store);
    store.set_paused(true).unwrap();
    assert!(matches!(
        store.migrate_ci_bisection(0, false, 1, 2, "main"),
        Err(StoreError::BisectionMigrationBusy)
    ));
    store.set_paused(false).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure", 12);
    store.set_paused(true).unwrap();
    assert!(matches!(
        store.migrate_ci_bisection(0, false, 1, 2, "main"),
        Err(StoreError::BisectionMigrationBusy)
    ));
    store.set_paused(false).unwrap();
    for run in 2..=3 {
        let child = plan(&mut store);
        finish(&mut store, &mut scheduler, &child, run, "success", 6);
    }
    let history = store.ci_investigations().unwrap();
    let holds = store.ci_only_completions().unwrap();
    store.set_paused(true).unwrap();
    assert!(matches!(
        store.migrate_ci_bisection(0, false, 1, 3, "main"),
        Err(StoreError::RepositoryMismatch)
    ));
    assert!(matches!(
        store.migrate_ci_bisection(0, false, 1, 2, "other"),
        Err(StoreError::PolicyMismatch)
    ));
    assert!(store.migrate_ci_bisection(0, false, 1, 2, "main").unwrap());
    assert_eq!(store.ci_bisection_mode().unwrap(), Some((false, 1)));
    assert_eq!(store.ci_investigations().unwrap(), history);
    assert_eq!(store.ci_only_completions().unwrap(), holds);
    assert!(matches!(
        store.bind_ci_bisection(true),
        Err(StoreError::PolicyMismatch)
    ));
    drop(store);
    let mut store = Store::open(&db).unwrap();
    assert_eq!(store.ci_bisection_mode().unwrap(), Some((false, 1)));
    assert!(!store.migrate_ci_bisection(1, false, 1, 2, "main").unwrap());
    assert!(store.migrate_ci_bisection(1, true, 1, 2, "main").unwrap());
    assert_eq!(store.ci_bisection_mode().unwrap(), Some((true, 2)));
    assert!(matches!(
        store.migrate_ci_bisection(0, false, 1, 2, "main"),
        Err(StoreError::PolicyMismatch)
    ));
    let conn = rusqlite::Connection::open(&db).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM ci_bisection_migrations", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 2);
    conn.execute("DELETE FROM ci_bisection_revision", [])
        .unwrap();
    drop(conn);
    drop(store);
    let mut reopened = Store::open(&db).unwrap();
    assert!(matches!(
        reopened.ci_bisection_mode(),
        Err(StoreError::PolicyMismatch)
    ));
    assert!(matches!(
        reopened.bind_ci_bisection(true),
        Err(StoreError::PolicyMismatch)
    ));
    assert!(matches!(
        reopened.migrate_ci_bisection(0, false, 1, 2, "main"),
        Err(StoreError::Unbound)
    ));
}

#[test]
fn concurrent_bisection_migrations_cannot_reuse_a_revision() {
    use std::sync::{Arc, Barrier};
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("migration.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    queue(&mut store, &mut inbox, 1, 4);
    store.bind_ci_bisection(true).unwrap();
    store.set_paused(true).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let db = db.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            let mut store = Store::open(db).unwrap();
            barrier.wait();
            store.migrate_ci_bisection(0, false, 1, 2, "main")
        }));
    }
    let results: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| matches!(r, Ok(true))).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(sluss::store::StoreError::PolicyMismatch)))
            .count(),
        1
    );
    assert_eq!(store.ci_bisection_mode().unwrap(), Some((false, 1)));
}

#[test]
fn bisection_migration_cli_uses_bound_repository_and_revision() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("migration.sqlite");
    let config = dir.path().join("sluss.toml");
    std::fs::write(&config, "version=1\n[github]\napp_client_id='test'\ninstallation_id=1\nrepository_id=2\nowner='owner'\nrepository='repo'\n[queue]\ntarget='main'\nmax_batch_size=2\nmax_active_batches=1\ndebounce_seconds=0\nbatch_window_seconds=0\n[checks]\nrequired_reviews=1\n").unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    queue(&mut store, &mut inbox, 1, 4);
    store.bind_ci_bisection(false).unwrap();
    store.set_paused(true).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&db)
        .args([
            "migrate-ci-bisection",
            "--expected-revision",
            "0",
            "--enable",
            "--config",
        ])
        .arg(&config)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&db)
        .arg("status")
        .output()
        .unwrap();
    assert!(status.status.success());
    assert!(String::from_utf8_lossy(&status.stdout).contains("CI bisection: enabled; revision: 1"));
    assert!(store.paused().unwrap());
}

#[test]
fn missing_runtime_and_bisection_children_do_not_change_the_controller() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("adaptive.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    store.bind_ci_bisection(true).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    queue(&mut store, &mut inbox, 1, 4);
    let batch = plan(&mut store);
    finish(&mut store, &mut scheduler, &batch, 1, "success", 12);
    assert_eq!(store.adaptive_status().unwrap().unwrap().success_streak, 1);
    queue(&mut store, &mut inbox, 2, 4);
    let batch = plan(&mut store);
    finish(&mut store, &mut scheduler, &batch, 2, "success", 0);
    assert_eq!(store.adaptive_status().unwrap().unwrap().success_streak, 0);
    let history = Store::adaptive_history(&db).unwrap();
    assert_eq!(
        (history[0].conclusion.as_str(), history[0].runner_ms),
        ("success", None)
    );
    queue(&mut store, &mut inbox, 3, 4);
    let batch = plan(&mut store);
    finish(&mut store, &mut scheduler, &batch, 3, "failure", 0);
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 1);
    assert_eq!(Store::adaptive_history(&db).unwrap()[0].runner_ms, None);
    for run in 4..=5 {
        let child = plan(&mut store);
        assert_eq!(child.candidates.len(), 1);
        finish(&mut store, &mut scheduler, &child, run, "success", 12);
    }
    let status = store.adaptive_status().unwrap().unwrap();
    assert_eq!(
        (status.limit, status.recorded_samples, status.success_streak),
        (1, 3, 0)
    );
    for serial in 10..=15 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        assert_eq!(batch.candidates.len(), 1);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 6);
    }
    // The failed size-two root lacked timing; its cost cannot be assumed zero.
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 1);
}

#[test]
fn untimed_success_blocks_comparison_until_it_leaves_the_window() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("adaptive.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    for serial in 1..=4 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        finish(
            &mut store,
            &mut scheduler,
            &batch,
            serial,
            "success",
            if serial == 1 { 0 } else { 12 },
        );
    }
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 2);
    for serial in 5..=10 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 12);
    }
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 3);
}

#[test]
fn planning_migration_starts_a_new_adaptive_epoch_even_if_settings_return() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("adaptive.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    for serial in 1..=3 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 12);
    }
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 3);
    store.set_paused(true).unwrap();
    let old = settings();
    let mut disabled = old.clone();
    disabled.adaptive_batching = false;
    store
        .migrate_planning_settings(&old.digest().unwrap(), &disabled, 1, 2, "main")
        .unwrap();
    assert!(store.adaptive_status().unwrap().is_none());
    store
        .migrate_planning_settings(&disabled.digest().unwrap(), &old, 1, 2, "main")
        .unwrap();
    let status = store.adaptive_status().unwrap().unwrap();
    assert_eq!((status.limit, status.recorded_samples), (2, 0));
    store.set_paused(false).unwrap();
    queue(&mut store, &mut inbox, 4, 4);
    let batch = plan(&mut store);
    assert_eq!(batch.candidates.len(), 2);
    finish(&mut store, &mut scheduler, &batch, 4, "success", 12);
    assert_eq!(
        store.adaptive_status().unwrap().unwrap().recorded_samples,
        1
    );
    let conn = rusqlite::Connection::open(&db).unwrap();
    let epochs: i64 = conn
        .query_row("SELECT COUNT(*) FROM adaptive_batch_epochs_v1", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(epochs, 2);
}

#[test]
fn contradictory_ci_observation_invalidates_sample_and_resets_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("adaptive.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    for serial in 1..=2 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 12);
    }
    queue(&mut store, &mut inbox, 3, 4);
    let batch = plan(&mut store);
    finish_with_release(&mut store, &mut scheduler, &batch, 3, "success", 12, false);
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 3);
    let mut conflicting = store
        .ci_evidence(&batch.id)
        .unwrap()
        .unwrap()
        .record
        .snapshot
        .unwrap();
    conflicting.jobs[0].id += 1_000;
    assert!(matches!(
        scheduler.record_job_metrics(&batch.id, &conflicting),
        Err(sluss::scheduler::SchedulerError::CiConflict)
    ));
    let status = store.adaptive_status().unwrap().unwrap();
    assert_eq!(status.invalidated_samples, 1);
    assert!(Store::adaptive_history(&db).unwrap()[0].invalid);
    assert_eq!(
        (
            status.limit,
            status.success_streak,
            status.increase_threshold
        ),
        (1, 0, 6)
    );
    let conn = rusqlite::Connection::open(&db).unwrap();
    let invalid: bool = conn
        .query_row(
            "SELECT invalid FROM adaptive_batch_events_v1 WHERE batch_id=?1",
            [&batch.id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(invalid);
}

#[test]
fn different_workflows_do_not_share_streaks_or_cost_baselines() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("adaptive.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    for serial in 1..=3 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 12);
    }
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 3);
    for serial in 4..=5 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        finish_with_workflow(
            &mut store,
            &mut scheduler,
            &batch,
            serial,
            "success",
            30,
            true,
            4,
        );
    }
    assert_eq!(store.adaptive_status().unwrap().unwrap().success_streak, 2);
    queue(&mut store, &mut inbox, 6, 4);
    let batch = plan(&mut store);
    finish(&mut store, &mut scheduler, &batch, 6, "success", 30);
    assert_eq!(store.adaptive_status().unwrap().unwrap().success_streak, 1);
    for serial in 7..=9 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        finish_with_workflow(
            &mut store,
            &mut scheduler,
            &batch,
            serial,
            "success",
            30,
            true,
            4,
        );
    }
    // Workflow 4 has no comparable size-two baseline, so workflow 3's
    // smaller jobs must not be used to report a cost regression.
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 4);
}

#[test]
fn concurrent_adaptive_planners_create_one_bounded_batch() {
    use std::sync::{Arc, Barrier};
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("adaptive.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    queue(&mut store, &mut inbox, 1, 4);
    let barrier = Arc::new(Barrier::new(4));
    let mut workers = Vec::new();
    for _ in 0..4 {
        let path = db.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            let mut store = Store::open(path).unwrap();
            barrier.wait();
            store.plan_observed_batch(1, 2, &"f".repeat(64), "main", &settings())
        }));
    }
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().unwrap())
        .collect();
    let batches: Vec<_> = results.iter().flatten().collect();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].candidates.len(), 2);
    let state = Store::open(&db)
        .unwrap()
        .adaptive_status()
        .unwrap()
        .unwrap();
    assert_eq!((state.limit, state.recorded_samples), (2, 0));
}

#[test]
fn investigation_cost_prevents_return_to_an_expensive_batch_size() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("adaptive.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    store.bind_ci_bisection(true).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    for serial in 1..=3 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        assert_eq!(batch.candidates.len(), 2);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 12);
    }
    for serial in 4..=6 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        assert_eq!(batch.candidates.len(), 3);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 18);
    }
    for serial in 7..=9 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        assert_eq!(batch.candidates.len(), 4);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 24);
    }
    queue(&mut store, &mut inbox, 10, 4);
    let root = plan(&mut store);
    assert_eq!(root.candidates.len(), 4);
    finish(&mut store, &mut scheduler, &root, 10, "failure", 2);
    let root_event = Store::adaptive_history(&db)
        .unwrap()
        .into_iter()
        .find(|e| e.batch_id == root.id)
        .unwrap();
    assert!(!root_event.recovery_settled);
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 2);
    for run in 11..=12 {
        let child = plan(&mut store);
        assert_eq!(child.candidates.len(), 2);
        finish(&mut store, &mut scheduler, &child, run, "success", 2);
        if run == 11 {
            assert_eq!(
                scheduler
                    .record_rerun_metrics(&child.id, &rerun_snapshot(&child, run))
                    .unwrap(),
                1
            );
        }
    }
    let root_event = Store::adaptive_history(&db)
        .unwrap()
        .into_iter()
        .find(|e| e.batch_id == root.id)
        .unwrap();
    assert!(root_event.recovery_settled);
    assert_eq!(
        (root_event.recovery_jobs, root_event.recovery_runner_ms),
        (2, 4_000)
    );
    assert_eq!(
        (root_event.observed_rerun_jobs, root_event.observed_rerun_ms),
        (1, 30_000)
    );
    // Six successful size-two batches end the cooldown and allow size three.
    for serial in 13..=18 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        assert_eq!(batch.candidates.len(), 2);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 12);
    }
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 3);
    for serial in 19..=21 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        assert_eq!(batch.candidates.len(), 3);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 18);
    }
    // The failed parent and initial investigation cost alone are below the
    // regression threshold. Its collected rerun makes size four expensive.
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 3);
}

#[test]
fn stale_investigation_cost_blocks_reentry_to_the_failed_size() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("adaptive.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    store.bind_ci_bisection(true).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    for serial in 1..=3 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 12);
    }
    queue(&mut store, &mut inbox, 4, 4);
    let root = plan(&mut store);
    assert_eq!(root.candidates.len(), 3);
    finish(&mut store, &mut scheduler, &root, 4, "failure", 18);
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 1);
    // Changed inputs invalidate the split before it can establish full cost.
    for serial in 5..=10 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        assert_eq!(batch.candidates.len(), 1);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 6);
    }
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 2);
    for serial in 11..=13 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        assert_eq!(batch.candidates.len(), 2);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 12);
    }
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 2);
    let root_event = Store::adaptive_history(&db)
        .unwrap()
        .into_iter()
        .find(|e| e.batch_id == root.id)
        .unwrap();
    assert!(!root_event.recovery_settled);
}

#[test]
fn nested_bisection_costs_survive_restart_without_counting_passes_as_root_success() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("adaptive.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    store.bind_ci_bisection(true).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    for serial in 1..=3 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 12);
    }
    for serial in 4..=6 {
        queue(&mut store, &mut inbox, serial, 4);
        let batch = plan(&mut store);
        finish(&mut store, &mut scheduler, &batch, serial, "success", 18);
    }
    assert_eq!(store.adaptive_status().unwrap().unwrap().limit, 4);
    queue(&mut store, &mut inbox, 7, 4);
    let root = plan(&mut store);
    finish(&mut store, &mut scheduler, &root, 7, "failure", 24);
    let first = plan(&mut store);
    assert_eq!(first.candidates.len(), 2);
    finish(&mut store, &mut scheduler, &first, 8, "failure", 12);
    let partial = Store::adaptive_history(&db)
        .unwrap()
        .into_iter()
        .find(|e| e.batch_id == root.id)
        .unwrap();
    assert_eq!(
        (
            partial.recovery_jobs,
            partial.recovery_runner_ms,
            partial.recovery_settled
        ),
        (1, 12_000, false)
    );
    drop(store);
    drop(scheduler);
    let mut store = Store::open(&db).unwrap();
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    let sibling = plan(&mut store);
    assert_eq!(sibling.candidates.len(), 2);
    finish(&mut store, &mut scheduler, &sibling, 9, "success", 12);
    let grandchild = plan(&mut store);
    assert_eq!(grandchild.candidates.len(), 1);
    finish(&mut store, &mut scheduler, &grandchild, 10, "success", 6);
    let other = plan(&mut store);
    assert_eq!(other.candidates.len(), 1);
    finish(&mut store, &mut scheduler, &other, 11, "failure", 6);
    let final_event = Store::adaptive_history(&db)
        .unwrap()
        .into_iter()
        .find(|e| e.batch_id == root.id)
        .unwrap();
    assert_eq!(
        (
            final_event.recovery_jobs,
            final_event.recovery_runner_ms,
            final_event.recovery_settled
        ),
        (4, 36_000, true)
    );
    assert_eq!(
        (
            store.adaptive_status().unwrap().unwrap().limit,
            store.adaptive_status().unwrap().unwrap().success_streak
        ),
        (2, 0)
    );
}

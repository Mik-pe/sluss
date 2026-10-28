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
    scheduler.complete_ci_only(id).unwrap();
    scheduler.complete_ci_only(id).unwrap(); // repeated completion cannot double-count
    if release {
        scheduler.release_completed_ci_only(id, run).unwrap();
    }
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
    finish(&mut store, &mut scheduler, &root, 10, "failure", 24);
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
        finish(&mut store, &mut scheduler, &child, run, "success", 12);
    }
    let root_event = Store::adaptive_history(&db)
        .unwrap()
        .into_iter()
        .find(|e| e.batch_id == root.id)
        .unwrap();
    assert!(root_event.recovery_settled);
    assert_eq!(
        (root_event.recovery_jobs, root_event.recovery_runner_ms),
        (2, 24_000)
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
    // At size four, 24 s of failed parent work and 24 s of investigation
    // are charged to the three earlier successful root batches.
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

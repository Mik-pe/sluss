use sluss::{
    batch::{Candidate, State},
    github::{ActionsJob, ActionsRepository, ActionsRun, ActionsSnapshot},
    scheduler::{Capacity, DispatchPlan, Host, Request, Scheduler},
    store::{AdmissionCandidate, Store, StoreError},
    webhook::Inbox,
};
use std::collections::{BTreeMap, BTreeSet};

fn setup() -> (tempfile::TempDir, Store, Inbox, Scheduler) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    store.bind_ci_bisection(true).unwrap();
    assert!(matches!(
        store.bind_ci_bisection(false),
        Err(StoreError::PolicyMismatch)
    ));
    inbox
        .record("initial", "pull_request", &[1; 32], 1, 2)
        .unwrap();
    let job = inbox.pending().unwrap().remove(0);
    let candidates: Vec<_> = (1..=4)
        .map(|number| AdmissionCandidate {
            candidate: Candidate {
                number,
                head_sha: format!("{number:040x}"),
            },
            evidence_digest: format!("{number:064x}"),
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
    let pools = BTreeMap::from([("rust".into(), 1)]);
    let scheduler = Scheduler::open(
        &db,
        Capacity {
            pools: pools.clone(),
            hosts: BTreeMap::from([(
                "sluss-host-test".into(),
                Host {
                    slots: 1,
                    runner_ids: BTreeSet::from([7]),
                    pools,
                },
            )]),
        },
    )
    .unwrap();
    (dir, store, inbox, scheduler)
}

fn plan(store: &mut Store, n: u64) -> Option<sluss::batch::Batch> {
    store
        .plan_batch(&format!("batch-{n:016x}"), &"a".repeat(40), 4, 1)
        .unwrap()
}
fn numbers(batch: &sluss::batch::Batch) -> Vec<u64> {
    batch.candidates.iter().map(|c| c.number).collect()
}

#[test]
fn ci_policy_file_migration_requires_drain_and_rejects_stale_revisions() {
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let db = dir.path().join("queue.sqlite");
    let root = plan(&mut store, 1).unwrap();
    let old = vec![
        ".github/workflows/ci.yml".to_string(),
        ".github/workflows/other.yml".to_string(),
    ];
    let next = vec![
        ".github/workflows/ci.yml".to_string(),
        "ci/check.sh".to_string(),
    ];
    store.bind_policy_paths(&root.id, &old).unwrap();
    assert!(matches!(
        store.migrate_staging_policy_paths(0, &next, 1, 2, "main"),
        Err(StoreError::StagingPolicyMigrationNeedsPause)
    ));
    store.set_paused(true).unwrap();
    assert!(matches!(
        store.migrate_staging_policy_paths(0, &next, 1, 2, "main"),
        Err(StoreError::StagingPolicyMigrationBusy)
    ));
    store.set_paused(false).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "success");
    store.set_paused(true).unwrap();
    assert!(matches!(
        store.migrate_staging_policy_paths(0, &next, 1, 3, "main"),
        Err(StoreError::RepositoryMismatch)
    ));
    assert!(matches!(
        store.migrate_staging_policy_paths(0, &next, 1, 2, "other"),
        Err(StoreError::PolicyMismatch)
    ));
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE landing_intents_v1(state TEXT)", [])
        .unwrap();
    conn.execute("INSERT INTO landing_intents_v1 VALUES('sending')", [])
        .unwrap();
    assert!(matches!(
        store.migrate_staging_policy_paths(0, &next, 1, 2, "main"),
        Err(StoreError::StagingPolicyMigrationBusy)
    ));
    conn.execute("UPDATE landing_intents_v1 SET state='cancelled'", [])
        .unwrap();
    assert!(
        store
            .migrate_staging_policy_paths(0, &next, 1, 2, "main")
            .unwrap()
    );
    assert_eq!(
        store.staging_policy_paths().unwrap(),
        Some((next.clone(), 1))
    );
    assert!(
        !store
            .migrate_staging_policy_paths(1, &next, 1, 2, "main")
            .unwrap()
    );
    assert!(
        store
            .migrate_staging_policy_paths(1, &old, 1, 2, "main")
            .unwrap()
    );
    assert!(matches!(
        store.migrate_staging_policy_paths(0, &next, 1, 2, "main"),
        Err(StoreError::PolicyMismatch)
    ));
    assert!(matches!(
        store.migrate_staging_policy_paths(2, &["../bad".into()], 1, 2, "main"),
        Err(StoreError::InvalidBatch)
    ));
    conn.execute("DELETE FROM staging_policy_revision", [])
        .unwrap();
    drop(conn);
    drop(store);
    let mut reopened = Store::open(&db).unwrap();
    assert!(matches!(
        reopened.staging_policy_paths(),
        Err(StoreError::PolicyMismatch)
    ));
    assert!(matches!(
        reopened.migrate_staging_policy_paths(0, &next, 1, 2, "main"),
        Err(StoreError::PolicyMismatch)
    ));
}

#[test]
fn ci_policy_file_migration_cli_reports_the_new_revision() {
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let db = dir.path().join("queue.sqlite");
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "success");
    store.set_paused(true).unwrap();
    let config = dir.path().join("sluss.toml");
    std::fs::write(&config, "version=1\n[github]\napp_client_id='test'\ninstallation_id=1\nrepository_id=2\nowner='owner'\nrepository='repo'\n[queue]\ntarget='main'\nmax_batch_size=4\nmax_active_batches=1\ndebounce_seconds=0\nbatch_window_seconds=0\n[checks]\nrequired_reviews=1\n").unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&db)
        .args([
            "migrate-policy-files",
            "--expected-revision",
            "0",
            "--policy-file",
            ".github/workflows/ci.yml",
            "--policy-file",
            "ci/check.sh",
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
    assert!(
        String::from_utf8_lossy(&status.stdout)
            .contains("CI policy files: .github/workflows/ci.yml, ci/check.sh; revision: 1")
    );
    assert!(store.paused().unwrap());
}

#[test]
fn concurrent_ci_policy_file_migrations_have_one_winner() {
    use std::sync::{Arc, Barrier};
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let db = dir.path().join("queue.sqlite");
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "success");
    store.set_paused(true).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let workers: Vec<_> = ["ci/one.sh", "ci/two.sh"]
        .into_iter()
        .map(|path| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut store = Store::open(db).unwrap();
                barrier.wait();
                store.migrate_staging_policy_paths(
                    0,
                    &[".github/workflows/ci.yml".into(), path.into()],
                    1,
                    2,
                    "main",
                )
            })
        })
        .collect();
    let results: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| matches!(r, Ok(true))).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(StoreError::PolicyMismatch)))
            .count(),
        1
    );
    assert_eq!(store.staging_policy_paths().unwrap().unwrap().1, 1);
}

#[test]
fn released_capacity_can_only_expand_while_paused_and_drained() {
    use sluss::scheduler::SchedulerError;
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let db = dir.path().join("queue.sqlite");
    let old_digest = Scheduler::capacity_digest(&db).unwrap().unwrap();
    let mut next = Capacity {
        pools: [("rust".into(), 2)].into(),
        hosts: [
            (
                "sluss-host-test".into(),
                Host {
                    slots: 1,
                    runner_ids: [7].into(),
                    pools: [("rust".into(), 1)].into(),
                },
            ),
            (
                "sluss-host-new".into(),
                Host {
                    slots: 1,
                    runner_ids: [8].into(),
                    pools: [("rust".into(), 1)].into(),
                },
            ),
        ]
        .into(),
    };
    assert!(matches!(
        Scheduler::migrate_capacity(&db, &old_digest, &next, 1, 2, "main"),
        Err(SchedulerError::MigrationBusy)
    ));
    let root = plan(&mut store, 1).unwrap();
    store.set_paused(true).unwrap();
    assert!(matches!(
        Scheduler::migrate_capacity(&db, &old_digest, &next, 1, 2, "main"),
        Err(SchedulerError::MigrationBusy)
    ));
    store.set_paused(false).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "success");
    store.set_paused(true).unwrap();
    scheduler
        .reserve(
            "migration-probe",
            &Request {
                pools: [("rust".into(), 1)].into(),
            },
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        Scheduler::migrate_capacity(&db, &old_digest, &next, 1, 2, "main"),
        Err(SchedulerError::MigrationBusy)
    ));
    assert!(scheduler.release("migration-probe").unwrap());
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute("INSERT INTO dispatch_intents(id,plan,state,run_id) VALUES('uncertain-probe','{}','sending',NULL)", []).unwrap();
    assert!(matches!(
        Scheduler::migrate_capacity(&db, &old_digest, &next, 1, 2, "main"),
        Err(SchedulerError::MigrationBusy)
    ));
    conn.execute(
        "DELETE FROM dispatch_intents WHERE id='uncertain-probe'",
        [],
    )
    .unwrap();
    drop(conn);
    assert!(matches!(
        Scheduler::migrate_capacity(&db, &"f".repeat(64), &next, 1, 2, "main"),
        Err(SchedulerError::ConfigChanged)
    ));
    assert!(matches!(
        Scheduler::migrate_capacity(&db, &old_digest, &next, 1, 3, "main"),
        Err(SchedulerError::MigrationBusy)
    ));
    let old_host = next.hosts.remove("sluss-host-test").unwrap();
    assert!(matches!(
        Scheduler::migrate_capacity(&db, &old_digest, &next, 1, 2, "main"),
        Err(SchedulerError::MigrationUnsafe)
    ));
    next.hosts.insert(
        "sluss-host-test".into(),
        Host {
            runner_ids: [9].into(),
            ..old_host.clone()
        },
    );
    assert!(matches!(
        Scheduler::migrate_capacity(&db, &old_digest, &next, 1, 2, "main"),
        Err(SchedulerError::MigrationUnsafe)
    ));
    next.hosts.insert("sluss-host-test".into(), old_host);
    assert!(Scheduler::migrate_capacity(&db, &old_digest, &next, 1, 2, "main").unwrap());
    assert!(store.paused().unwrap());
    assert!(matches!(
        scheduler.reserve(
            "old-worker",
            &Request {
                pools: [("rust".into(), 1)].into()
            }
        ),
        Err(SchedulerError::ConfigChanged)
    ));
    let new_digest = Scheduler::capacity_digest(&db).unwrap().unwrap();
    assert_ne!(old_digest, new_digest);
    assert!(!Scheduler::migrate_capacity(&db, &new_digest, &next, 1, 2, "main").unwrap());
    let conn = rusqlite::Connection::open(&db).unwrap();
    let count: i64 = conn
        .query_row("SELECT count(*) FROM capacity_migrations", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
    let history: i64 = conn
        .query_row(
            "SELECT count(*) FROM capacity_release_topology_history",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(history, 2);
    drop(scheduler);
    assert!(Scheduler::open(&db, next).is_ok());
}

#[test]
fn concurrent_capacity_migrations_compare_the_same_digest() {
    use std::sync::{Arc, Barrier};
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let db = dir.path().join("queue.sqlite");
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "success");
    store.set_paused(true).unwrap();
    let digest = Scheduler::capacity_digest(&db).unwrap().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut threads = Vec::new();
    for budget in [2, 3] {
        let db = db.clone();
        let digest = digest.clone();
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            let next = Capacity {
                pools: [("rust".into(), budget)].into(),
                hosts: [(
                    "sluss-host-test".into(),
                    Host {
                        slots: 1,
                        runner_ids: [7].into(),
                        pools: [("rust".into(), 1)].into(),
                    },
                )]
                .into(),
            };
            barrier.wait();
            Scheduler::migrate_capacity(db, &digest, &next, 1, 2, "main")
        }));
    }
    let results: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Ok(true)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(sluss::scheduler::SchedulerError::ConfigChanged)))
            .count(),
        1
    );
}
fn finish(
    store: &mut Store,
    scheduler: &mut Scheduler,
    batch: &sluss::batch::Batch,
    run_id: u64,
    conclusion: &str,
) {
    finish_selected(store, scheduler, batch, run_id, conclusion, None);
}

fn finish_selected(
    store: &mut Store,
    scheduler: &mut Scheduler,
    batch: &sluss::batch::Batch,
    run_id: u64,
    conclusion: &str,
    suite: Option<&str>,
) {
    finish_with_workflow(
        store,
        scheduler,
        batch,
        run_id,
        conclusion,
        suite,
        ".github/workflows/ci.yml",
        None,
        false,
    );
}

#[allow(clippy::too_many_arguments)]
fn finish_with_workflow(
    store: &mut Store,
    scheduler: &mut Scheduler,
    batch: &sluss::batch::Batch,
    run_id: u64,
    conclusion: &str,
    suite: Option<&str>,
    workflow: &str,
    legacy_db: Option<&std::path::Path>,
    retain_reservation: bool,
) {
    let id = &batch.id;
    store
        .bind_policy_paths(
            id,
            &[
                ".github/workflows/ci.yml".into(),
                ".github/workflows/other.yml".into(),
            ],
        )
        .unwrap();
    store.start_integration(id).unwrap();
    store
        .finish_integration(id, &"b".repeat(40), &"c".repeat(64))
        .unwrap();
    if let Some(suite) = suite {
        store
            .record_selection(
                id,
                &sluss::selective::FrozenSelection {
                    base_sha: batch.base_sha.clone(),
                    integration_sha: "b".repeat(40),
                    policy_digest: "d".repeat(64),
                    selection: sluss::selective::Selection::Selected {
                        suites: [suite.into()].into(),
                        components: ["component".into()].into(),
                    },
                },
            )
            .unwrap();
    }
    let plan = DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
        integration_sha: "b".repeat(40),
        staging_ref: format!("refs/heads/sluss/{id}"),
        policy_digest: "c".repeat(64),
        required_jobs: ["test".into()].into(),
        suites: [suite.unwrap_or("full").into()].into(),
        selection_policy_digest: suite.map(|_| "d".repeat(64)),
    };
    scheduler
        .reserve_batch_dispatch(
            id,
            &Request {
                pools: [("rust".into(), 1)].into(),
            },
            &plan,
        )
        .unwrap()
        .unwrap();
    assert!(scheduler.claim_batch_dispatch(id, workflow).unwrap());
    if let Some(db) = legacy_db {
        let conn = rusqlite::Connection::open(db).unwrap();
        conn.execute("DELETE FROM dispatch_workflows WHERE id=?1", [id])
            .unwrap();
    }
    scheduler.record_receipt(id, run_id).unwrap();
    scheduler.observe_dispatch(id, run_id).unwrap();
    let snapshot = ActionsSnapshot {
        run: ActionsRun {
            id: run_id,
            run_attempt: 1,
            workflow_id: 3,
            head_sha: "b".repeat(40),
            head_branch: Some(format!("sluss/{id}")),
            path: workflow.into(),
            event: "workflow_dispatch".into(),
            status: "completed".into(),
            conclusion: Some(conclusion.into()),
            repository: ActionsRepository { id: 2 },
            head_repository: Some(ActionsRepository { id: 2 }),
        },
        jobs: vec![ActionsJob {
            id: run_id + 100,
            run_id,
            head_sha: "b".repeat(40),
            name: "test".into(),
            status: "completed".into(),
            conclusion: Some(
                if conclusion == "success" {
                    "success"
                } else {
                    "failure"
                }
                .into(),
            ),
            started_at: None,
            completed_at: None,
            runner_id: Some(7),
        }],
    };
    scheduler.record_ci_observation(id, &snapshot).unwrap();
    scheduler.complete_ci_only(id).unwrap();
    scheduler.complete_ci_only(id).unwrap(); // repeated recovery is idempotent
    if !retain_reservation {
        scheduler.release_completed_ci_only(id, run_id).unwrap(); // local test assumes the runner is no longer consuming capacity
    }
}

#[test]
fn splits_only_after_an_independent_successful_sibling() {
    let (_dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    assert_eq!(numbers(&root), [1, 2, 3, 4]);
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let left = plan(&mut store, 2).unwrap();
    assert_eq!(numbers(&left), [1, 2]);
    finish(&mut store, &mut scheduler, &left, 2, "failure");
    let right = plan(&mut store, 3).unwrap();
    assert_eq!(
        numbers(&right),
        [3, 4],
        "the other half runs before deeper isolation"
    );
    finish(&mut store, &mut scheduler, &right, 3, "success");
    let first = plan(&mut store, 4).unwrap();
    assert_eq!(numbers(&first), [1]);
    finish(&mut store, &mut scheduler, &first, 4, "success");
    let second = plan(&mut store, 5).unwrap();
    assert_eq!(numbers(&second), [2]);
    finish(&mut store, &mut scheduler, &second, 5, "failure");
    assert!(plan(&mut store, 6).is_none());
    assert!(
        store.failed_candidates().unwrap().is_empty(),
        "CI observations never label a PR as a confirmed culprit"
    );
    assert!(store.active_batches().unwrap().is_empty());
    assert!(matches!(
        store.batch(&second.id).unwrap().unwrap().0.state,
        State::CiComplete { .. }
    ));
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .iter()
            .any(|(id, state)| id == &root.id && state == "needs-review")
    );
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .iter()
            .any(|(id, state)| id == &left.id && state == "needs-review")
    );
}

#[test]
fn shared_failures_and_timeouts_stop_without_blaming_individual_prs() {
    let (_dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let left = plan(&mut store, 2).unwrap();
    finish(&mut store, &mut scheduler, &left, 2, "failure");
    let right = plan(&mut store, 3).unwrap();
    finish(&mut store, &mut scheduler, &right, 3, "failure");
    assert!(plan(&mut store, 4).is_none());
    assert!(store.failed_candidates().unwrap().is_empty());
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .contains(&(root.id, "inconclusive".into()))
    );

    let (_dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "timed_out");
    assert!(plan(&mut store, 2).is_none());
    assert!(store.ci_investigations().unwrap().is_empty());
}

#[test]
fn changed_sibling_inputs_prevent_deeper_isolation() {
    let (_dir, mut store, mut inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let left = plan(&mut store, 2).unwrap();
    finish(&mut store, &mut scheduler, &left, 2, "failure");
    inbox
        .record("new-head", "pull_request", &[2; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let mut candidates: Vec<_> = store
        .candidates()
        .unwrap()
        .into_iter()
        .map(|candidate| AdmissionCandidate {
            evidence_digest: format!("{:064x}", candidate.number),
            candidate,
        })
        .collect();
    candidates[0].candidate.head_sha = "e".repeat(40);
    store
        .apply_admission_reconciliation_with_base(
            &event,
            &"f".repeat(64),
            &candidates,
            &"a".repeat(40),
            "main",
        )
        .unwrap();
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .contains(&(root.id, "stale".into()))
    );
    let new_work = plan(&mut store, 3).unwrap();
    assert_eq!(numbers(&new_work), [1]);
}

#[test]
fn different_suite_sets_are_inconclusive() {
    let (_dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let left = plan(&mut store, 2).unwrap();
    finish_selected(
        &mut store,
        &mut scheduler,
        &left,
        2,
        "failure",
        Some("fast"),
    );
    let right = plan(&mut store, 3).unwrap();
    finish_selected(
        &mut store,
        &mut scheduler,
        &right,
        3,
        "success",
        Some("other"),
    );
    assert!(plan(&mut store, 4).is_none());
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .contains(&(root.id, "inconclusive".into()))
    );
    assert!(store.failed_candidates().unwrap().is_empty());
}

#[test]
fn changed_inputs_atomically_invalidate_planned_siblings_and_survive_restart() {
    let (dir, mut store, mut inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let left = store
        .plan_batch("batch-0000000000000002", &"a".repeat(40), 4, 2)
        .unwrap()
        .unwrap();
    let right = store
        .plan_batch("batch-0000000000000003", &"a".repeat(40), 4, 2)
        .unwrap()
        .unwrap();
    assert_eq!(numbers(&left), [1, 2]);
    assert_eq!(numbers(&right), [3, 4]);
    inbox
        .record("changed", "pull_request", &[3; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let mut candidates: Vec<_> = store
        .candidates()
        .unwrap()
        .into_iter()
        .map(|candidate| AdmissionCandidate {
            evidence_digest: format!("{:064x}", candidate.number),
            candidate,
        })
        .collect();
    candidates[0].candidate.head_sha = "e".repeat(40);
    store
        .apply_admission_reconciliation_with_base(
            &event,
            &"f".repeat(64),
            &candidates,
            &"a".repeat(40),
            "main",
        )
        .unwrap();
    drop(store);
    let mut store = Store::open(dir.path().join("queue.sqlite")).unwrap();
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .contains(&(root.id, "stale".into()))
    );
    for batch in [left, right] {
        assert!(matches!(
            store.batch(&batch.id).unwrap().unwrap().0.state,
            State::Invalidated { .. }
        ));
        assert!(matches!(
            store.start_integration(&batch.id),
            Err(StoreError::StaleBatch | StoreError::InvalidBatch)
        ));
    }
    assert_eq!(numbers(&plan(&mut store, 4).unwrap()), [1]);
}

#[test]
fn integration_conflict_stops_ci_comparison_but_keeps_conflict_bisection() {
    let (_dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let left = plan(&mut store, 2).unwrap();
    store.start_integration(&left.id).unwrap();
    store.fail_integration(&left.id, "merge conflict").unwrap();
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .contains(&(root.id.clone(), "inconclusive".into()))
    );
    let first = plan(&mut store, 3).unwrap();
    assert_eq!(numbers(&first), [1]);
    finish(&mut store, &mut scheduler, &first, 3, "failure");
    let second = plan(&mut store, 4).unwrap();
    assert_eq!(numbers(&second), [2]);
    finish(&mut store, &mut scheduler, &second, 4, "success");
    assert!(plan(&mut store, 5).is_none());
    assert_eq!(
        store.ci_investigations().unwrap(),
        vec![(root.id, "inconclusive".into())]
    );
    assert!(store.failed_candidates().unwrap().is_empty());
}

#[test]
fn changed_successful_sibling_invalidates_nested_investigation() {
    let (_dir, mut store, mut inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let left = plan(&mut store, 2).unwrap();
    finish(&mut store, &mut scheduler, &left, 2, "failure");
    let right = plan(&mut store, 3).unwrap();
    finish(&mut store, &mut scheduler, &right, 3, "success");
    let nested = plan(&mut store, 4).unwrap();
    assert_eq!(numbers(&nested), [1]);
    inbox
        .record("review-change", "pull_request_review", &[4; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let mut candidates: Vec<_> = store
        .candidates()
        .unwrap()
        .into_iter()
        .map(|candidate| AdmissionCandidate {
            evidence_digest: format!("{:064x}", candidate.number),
            candidate,
        })
        .collect();
    candidates[3].evidence_digest = "9".repeat(64);
    store
        .apply_admission_reconciliation_with_base(
            &event,
            &"f".repeat(64),
            &candidates,
            &"a".repeat(40),
            "main",
        )
        .unwrap();
    let investigations = store.ci_investigations().unwrap();
    assert!(investigations.contains(&(root.id, "stale".into())));
    assert!(investigations.contains(&(left.id, "stale".into())));
    assert!(matches!(
        store.batch(&nested.id).unwrap().unwrap().0.state,
        State::Invalidated { .. }
    ));
    assert_eq!(numbers(&plan(&mut store, 5).unwrap()), [4]);
}

#[test]
fn nested_inconclusive_result_finishes_the_root_investigation() {
    let (_dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let left = plan(&mut store, 2).unwrap();
    finish(&mut store, &mut scheduler, &left, 2, "failure");
    let right = plan(&mut store, 3).unwrap();
    finish(&mut store, &mut scheduler, &right, 3, "success");
    let first = plan(&mut store, 4).unwrap();
    finish(&mut store, &mut scheduler, &first, 4, "failure");
    let second = plan(&mut store, 5).unwrap();
    finish(&mut store, &mut scheduler, &second, 5, "failure");
    let investigations = store.ci_investigations().unwrap();
    assert!(investigations.contains(&(root.id, "inconclusive".into())));
    assert!(investigations.contains(&(left.id, "inconclusive".into())));
    assert!(plan(&mut store, 6).is_none());
}

#[test]
fn target_change_invalidates_ci_investigation_and_planned_child() {
    let (_dir, mut store, mut inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let child = plan(&mut store, 2).unwrap();
    inbox.record("target", "push", &[5; 32], 1, 2).unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let candidates: Vec<_> = store
        .candidates()
        .unwrap()
        .into_iter()
        .map(|candidate| AdmissionCandidate {
            evidence_digest: format!("{:064x}", candidate.number),
            candidate,
        })
        .collect();
    store
        .apply_admission_reconciliation_with_base(
            &event,
            &"f".repeat(64),
            &candidates,
            &"c".repeat(40),
            "main",
        )
        .unwrap();
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .contains(&(root.id, "stale".into()))
    );
    assert!(matches!(
        store.batch(&child.id).unwrap().unwrap().0.state,
        State::Invalidated { .. }
    ));
    let next = store
        .plan_batch("batch-0000000000000003", &"c".repeat(40), 4, 1)
        .unwrap()
        .unwrap();
    assert_eq!(numbers(&next), [1, 2, 3, 4]);
}

#[test]
fn selection_migration_requires_a_drained_pause_and_invalidates_old_ci_holds() {
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    store.bind_staging_selection(&root.id, None).unwrap();
    let policy_path = dir.path().join("verification.toml");
    std::fs::write(&policy_path, include_str!("../examples/verification.toml")).unwrap();
    let next = sluss::selective::Policy::load(&policy_path)
        .unwrap()
        .digest()
        .unwrap();
    assert!(matches!(
        store.migrate_staging_selection("full", &next, false),
        Err(StoreError::MigrationNeedsPause)
    ));
    store.set_paused(true).unwrap();
    assert!(matches!(
        store.migrate_staging_selection("full", &next, false),
        Err(StoreError::MigrationBusy)
    ));
    store.set_paused(false).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    assert_eq!(store.ci_only_completions().unwrap().len(), 4);
    let reservation = Request {
        pools: [("rust".into(), 1)].into(),
    };
    scheduler
        .reserve("other-work", &reservation)
        .unwrap()
        .unwrap();
    store.set_paused(true).unwrap();
    assert!(matches!(
        store.migrate_staging_selection("full", &next, false),
        Err(StoreError::MigrationBusy)
    ));
    assert!(scheduler.release("other-work").unwrap());
    let conn = rusqlite::Connection::open(dir.path().join("queue.sqlite")).unwrap();
    conn.execute(
        "INSERT INTO dispatch_intents(id,plan,state) VALUES('uncertain','{}','sending')",
        [],
    )
    .unwrap();
    assert!(matches!(
        store.migrate_staging_selection("full", &next, false),
        Err(StoreError::MigrationBusy)
    ));
    conn.execute("DELETE FROM dispatch_intents WHERE id='uncertain'", [])
        .unwrap();
    assert!(matches!(
        store.migrate_staging_selection(&"1".repeat(64), &next, false),
        Err(StoreError::PolicyMismatch)
    ));
    assert_eq!(
        store.staging_selection_mode().unwrap().as_deref(),
        Some("full")
    );
    assert_eq!(store.ci_only_completions().unwrap().len(), 4);
    // Exercise the user-facing command and operator-supplied policy file.
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(dir.path().join("queue.sqlite"))
        .args(["migrate-selection", "--expected-mode", "full", "--policy"])
        .arg(&policy_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("Sluss remains paused"));
    assert!(store.paused().unwrap());
    assert_eq!(
        store.staging_selection_mode().unwrap().as_deref(),
        Some(next.as_str())
    );
    assert!(store.ci_only_completions().unwrap().is_empty());
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .contains(&(root.id.clone(), "stale".into()))
    );
    assert!(matches!(
        store.batch(&root.id).unwrap().unwrap().0.state,
        State::CiComplete { .. }
    ));
    assert_eq!(
        conn.query_row("SELECT cleared_holds FROM selection_migrations", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap(),
        4
    );
    drop(store);
    let mut store = Store::open(dir.path().join("queue.sqlite")).unwrap();
    assert!(
        !store
            .migrate_staging_selection(&next, &next, false)
            .unwrap()
    );
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM selection_migrations", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    store.set_paused(false).unwrap();
    let next_batch = plan(&mut store, 2).unwrap();
    assert_eq!(
        numbers(&next_batch),
        [1, 2, 3, 4],
        "the old CI split must not survive the migration"
    );
    store
        .bind_staging_selection(&next_batch.id, Some(&next))
        .unwrap();
}

#[test]
fn selection_migration_preserves_pending_integration_conflict_splits() {
    let (dir, mut store, _inbox, _scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    store.bind_staging_selection(&root.id, None).unwrap();
    store.start_integration(&root.id).unwrap();
    store.fail_integration(&root.id, "merge conflict").unwrap();
    store.set_paused(true).unwrap();
    assert!(
        store
            .migrate_staging_selection("full", &"d".repeat(64), false)
            .unwrap()
    );
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(dir.path().join("queue.sqlite"))
        .args([
            "migrate-selection",
            "--expected-mode",
            &"d".repeat(64),
            "--full",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        store.staging_selection_mode().unwrap().as_deref(),
        Some("full")
    );
    store.set_paused(false).unwrap();
    assert_eq!(numbers(&plan(&mut store, 2).unwrap()), [1, 2]);
}

#[test]
fn competing_selection_migrations_compare_the_bound_mode_atomically() {
    use std::sync::{Arc, Barrier};
    let (dir, mut store, _inbox, _scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    store.bind_staging_selection(&root.id, None).unwrap();
    store.start_integration(&root.id).unwrap();
    store.fail_integration(&root.id, "merge conflict").unwrap();
    store.set_paused(true).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let workers: Vec<_> = ["d".repeat(64), "e".repeat(64)]
        .into_iter()
        .map(|next| {
            let path = dir.path().join("queue.sqlite");
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut store = Store::open(path).unwrap();
                barrier.wait();
                let result = store.migrate_staging_selection("full", &next, false);
                (next, result)
            })
        })
        .collect();
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(
        results
            .iter()
            .filter(|(_, result)| matches!(result, Ok(true)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|(_, result)| matches!(result, Err(StoreError::PolicyMismatch)))
            .count(),
        1
    );
    let winner = &results
        .iter()
        .find(|(_, result)| matches!(result, Ok(true)))
        .unwrap()
        .0;
    assert_eq!(
        store.staging_selection_mode().unwrap().as_ref(),
        Some(winner)
    );
    let conn = rusqlite::Connection::open(dir.path().join("queue.sqlite")).unwrap();
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM selection_migrations", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn migration_can_discard_unstarted_legacy_batch_but_not_a_reservation() {
    use sha2::{Digest, Sha256};
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let policy_path = dir.path().join("verification.toml");
    std::fs::write(&policy_path, include_str!("../examples/verification.toml")).unwrap();
    let policy = sluss::selective::Policy::load(&policy_path).unwrap();
    let mut hash = Sha256::new();
    hash.update(b"sluss-selective-policy-v1\0");
    hash.update(serde_json::to_vec(&policy).unwrap());
    let legacy = hex::encode(hash.finalize());
    let current = policy.digest().unwrap();
    let root = plan(&mut store, 1).unwrap();
    store
        .bind_staging_selection(&root.id, Some(&legacy))
        .unwrap();
    store.start_integration(&root.id).unwrap();
    let frozen = sluss::selective::FrozenSelection {
        base_sha: root.base_sha.clone(),
        integration_sha: "b".repeat(40),
        policy_digest: legacy.clone(),
        selection: sluss::selective::Selection::Selected {
            suites: ["core-tests".into()].into(),
            components: ["core".into()].into(),
        },
    };
    store
        .finish_integration_with_selection(
            &root.id,
            &"b".repeat(40),
            &"c".repeat(64),
            Some(&frozen),
        )
        .unwrap();
    store.set_paused(true).unwrap();
    assert!(matches!(
        store.migrate_staging_selection(&legacy, &current, false),
        Err(StoreError::MigrationBusy)
    ));
    scheduler
        .reserve(
            "other-work",
            &Request {
                pools: [("rust".into(), 1)].into(),
            },
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        store.migrate_staging_selection(&legacy, &current, true),
        Err(StoreError::MigrationBusy)
    ));
    assert!(matches!(
        store.batch(&root.id).unwrap().unwrap().0.state,
        State::Testing { .. }
    ));
    assert!(scheduler.release("other-work").unwrap());
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(dir.path().join("queue.sqlite"))
        .args(["migrate-selection", "--expected-mode", &legacy, "--policy"])
        .arg(&policy_path)
        .arg("--discard-unstarted")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(matches!(
        store.batch(&root.id).unwrap().unwrap().0.state,
        State::Invalidated { .. }
    ));
    assert_eq!(
        store.staging_selection_mode().unwrap().as_deref(),
        Some(current.as_str())
    );
    let conn = rusqlite::Connection::open(dir.path().join("queue.sqlite")).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT discarded_batches FROM selection_migrations",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    store.set_paused(false).unwrap();
    let retried = plan(&mut store, 2).unwrap();
    assert_eq!(numbers(&retried), [1, 2, 3, 4]);
    store
        .bind_staging_selection(&retried.id, Some(&current))
        .unwrap();
}

#[test]
fn migration_handles_an_unbound_legacy_manual_selection() {
    let (_dir, mut store, _inbox, _scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    store.start_integration(&root.id).unwrap();
    store
        .finish_integration(&root.id, &"b".repeat(40), &"c".repeat(64))
        .unwrap();
    store
        .record_selection(
            &root.id,
            &sluss::selective::FrozenSelection {
                base_sha: root.base_sha.clone(),
                integration_sha: "b".repeat(40),
                policy_digest: "d".repeat(64),
                selection: sluss::selective::Selection::Selected {
                    suites: ["core-tests".into()].into(),
                    components: ["core".into()].into(),
                },
            },
        )
        .unwrap();
    assert!(store.staging_selection_mode().unwrap().is_none());
    store.set_paused(true).unwrap();
    assert!(
        store
            .migrate_staging_selection("unbound", &"e".repeat(64), true)
            .unwrap()
    );
    assert!(matches!(
        store.batch(&root.id).unwrap().unwrap().0.state,
        State::Invalidated { .. }
    ));
    assert_eq!(
        store.staging_selection_mode().unwrap().as_deref(),
        Some("e".repeat(64).as_str())
    );
    store.set_paused(false).unwrap();
    let next = plan(&mut store, 2).unwrap();
    store
        .bind_staging_selection(&next.id, Some(&"e".repeat(64)))
        .unwrap();
}

#[test]
fn ci_investigation_requires_comparable_frozen_workflow_paths() {
    let (_dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let left = plan(&mut store, 2).unwrap();
    finish(&mut store, &mut scheduler, &left, 2, "failure");
    let right = plan(&mut store, 3).unwrap();
    finish_with_workflow(
        &mut store,
        &mut scheduler,
        &right,
        3,
        "success",
        None,
        ".github/workflows/other.yml",
        None,
        false,
    );
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .contains(&(root.id, "inconclusive".into()))
    );
    assert!(plan(&mut store, 4).is_none());
}

#[test]
fn matching_children_with_a_different_parent_workflow_are_inconclusive() {
    let (_dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let left = plan(&mut store, 2).unwrap();
    finish_with_workflow(
        &mut store,
        &mut scheduler,
        &left,
        2,
        "failure",
        None,
        ".github/workflows/other.yml",
        None,
        false,
    );
    let right = plan(&mut store, 3).unwrap();
    finish_with_workflow(
        &mut store,
        &mut scheduler,
        &right,
        3,
        "success",
        None,
        ".github/workflows/other.yml",
        None,
        false,
    );
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .contains(&(root.id, "inconclusive".into()))
    );
    assert!(plan(&mut store, 4).is_none());
}

#[test]
fn legacy_run_without_a_workflow_binding_remains_observable_but_is_not_bisected() {
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish_with_workflow(
        &mut store,
        &mut scheduler,
        &root,
        1,
        "failure",
        None,
        ".github/workflows/ci.yml",
        Some(&dir.path().join("queue.sqlite")),
        false,
    );
    assert!(store.ci_investigations().unwrap().is_empty());
    assert!(plan(&mut store, 2).is_none());
    assert!(matches!(
        store.batch(&root.id).unwrap().unwrap().0.state,
        State::CiComplete { .. }
    ));
}

#[test]
fn competing_batch_claims_atomically_freeze_one_workflow_across_restart() {
    use std::sync::{Arc, Barrier};
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    store
        .bind_policy_paths(&root.id, &[".github/workflows/ci.yml".into()])
        .unwrap();
    store.start_integration(&root.id).unwrap();
    store
        .finish_integration(&root.id, &"b".repeat(40), &"c".repeat(64))
        .unwrap();
    let plan = DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
        integration_sha: "b".repeat(40),
        staging_ref: format!("refs/heads/sluss/{}", root.id),
        policy_digest: "c".repeat(64),
        required_jobs: ["test".into()].into(),
        suites: ["full".into()].into(),
        selection_policy_digest: None,
    };
    let request = Request {
        pools: [("rust".into(), 1)].into(),
    };
    scheduler
        .reserve_batch_dispatch(&root.id, &request, &plan)
        .unwrap()
        .unwrap();
    let db = dir.path().join("queue.sqlite");
    let pools = BTreeMap::from([("rust".into(), 1)]);
    let capacity = Capacity {
        pools: pools.clone(),
        hosts: BTreeMap::from([(
            "sluss-host-test".into(),
            Host {
                slots: 1,
                runner_ids: BTreeSet::from([7]),
                pools,
            },
        )]),
    };
    let barrier = Arc::new(Barrier::new(6));
    let handles: Vec<_> = (0..6)
        .map(|_| {
            let db = db.clone();
            let capacity = capacity.clone();
            let barrier = barrier.clone();
            let id = root.id.clone();
            std::thread::spawn(move || {
                let mut scheduler = Scheduler::open(db, capacity).unwrap();
                barrier.wait();
                scheduler
                    .claim_batch_dispatch(&id, ".github/workflows/ci.yml")
                    .unwrap()
            })
        })
        .collect();
    assert_eq!(
        handles
            .into_iter()
            .map(|handle| usize::from(handle.join().unwrap()))
            .sum::<usize>(),
        1
    );
    drop(scheduler);
    let scheduler = Scheduler::open(&db, capacity).unwrap();
    assert_eq!(
        scheduler.dispatch_workflow(&root.id).unwrap().as_deref(),
        Some(".github/workflows/ci.yml")
    );
    assert!(matches!(
        scheduler.pending_dispatches().unwrap()[0].state,
        sluss::scheduler::DispatchState::Sending
    ));
}

#[test]
fn ci_evidence_command_shows_preserved_jobs_without_claiming_attestation() {
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "success");
    let saved = store.ci_evidence(&root.id).unwrap().unwrap();
    assert!(!saved.conflicted);
    assert_eq!(saved.record.runner_id, Some(7));
    assert_eq!(saved.record.snapshot.unwrap().jobs[0].id, 101);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(dir.path().join("queue.sqlite"))
        .args(["ci-evidence", "--batch-id", &root.id])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = String::from_utf8_lossy(&output.stdout);
    assert!(report.contains("job 101"));
    assert!(report.contains("not a trusted attestation"));
    let conn = rusqlite::Connection::open(dir.path().join("queue.sqlite")).unwrap();
    let body: String = conn
        .query_row(
            "SELECT body FROM ci_observations WHERE id=?1",
            [&root.id],
            |r| r.get(0),
        )
        .unwrap();
    let mut legacy: serde_json::Value = serde_json::from_str(&body).unwrap();
    legacy.as_object_mut().unwrap().remove("snapshot");
    legacy.as_object_mut().unwrap().remove("runner_id");
    conn.execute(
        "UPDATE ci_observations SET body=?2 WHERE id=?1",
        rusqlite::params![root.id, legacy.to_string()],
    )
    .unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(dir.path().join("queue.sqlite"))
        .args(["ci-evidence", "--batch-id", &root.id])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("historical record has no original job snapshot")
    );
}

#[test]
fn legacy_parent_without_original_job_snapshot_cannot_drive_bisection() {
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let conn = rusqlite::Connection::open(dir.path().join("queue.sqlite")).unwrap();
    let body: String = conn
        .query_row(
            "SELECT body FROM ci_observations WHERE id=?1",
            [&root.id],
            |r| r.get(0),
        )
        .unwrap();
    let mut old: serde_json::Value = serde_json::from_str(&body).unwrap();
    old.as_object_mut().unwrap().remove("snapshot");
    old.as_object_mut().unwrap().remove("runner_id");
    conn.execute(
        "UPDATE ci_observations SET body=?2 WHERE id=?1",
        rusqlite::params![root.id, old.to_string()],
    )
    .unwrap();
    let left = plan(&mut store, 2).unwrap();
    finish(&mut store, &mut scheduler, &left, 2, "failure");
    let right = plan(&mut store, 3).unwrap();
    finish(&mut store, &mut scheduler, &right, 3, "success");
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .contains(&(root.id, "inconclusive".into()))
    );
    assert!(plan(&mut store, 4).is_none());
}

#[test]
fn older_contradictory_success_snapshot_cannot_drive_bisection() {
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let left = plan(&mut store, 2).unwrap();
    finish(&mut store, &mut scheduler, &left, 2, "success");
    let conn = rusqlite::Connection::open(dir.path().join("queue.sqlite")).unwrap();
    let body: String = conn
        .query_row(
            "SELECT body FROM ci_observations WHERE id=?1",
            [&left.id],
            |r| r.get(0),
        )
        .unwrap();
    let mut old: sluss::scheduler::CiObservationRecord = serde_json::from_str(&body).unwrap();
    let mut optional = old.snapshot.as_ref().unwrap().jobs[0].clone();
    optional.id = 999;
    optional.name = "optional".into();
    optional.status = "in_progress".into();
    optional.conclusion = None;
    old.snapshot.as_mut().unwrap().jobs.push(optional);
    conn.execute(
        "UPDATE ci_observations SET body=?2 WHERE id=?1",
        [&left.id, &serde_json::to_string(&old).unwrap()],
    )
    .unwrap();
    let right = plan(&mut store, 3).unwrap();
    finish(&mut store, &mut scheduler, &right, 3, "failure");
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .contains(&(root.id, "inconclusive".into()))
    );
    assert!(plan(&mut store, 4).is_none());
}

#[test]
fn contradictory_completed_run_invalidates_its_pending_investigation() {
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish_with_workflow(
        &mut store,
        &mut scheduler,
        &root,
        1,
        "failure",
        None,
        ".github/workflows/ci.yml",
        None,
        true,
    );
    let child = plan(&mut store, 2).unwrap();
    assert!(matches!(child.state, State::Planned));
    let mut changed = store
        .ci_evidence(&root.id)
        .unwrap()
        .unwrap()
        .record
        .snapshot
        .unwrap();
    changed.jobs[0].id += 1000;
    assert!(matches!(
        scheduler.record_job_metrics(&root.id, &changed),
        Err(sluss::scheduler::SchedulerError::CiConflict)
    ));
    assert!(store.ci_evidence(&root.id).unwrap().unwrap().conflicted);
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .contains(&(root.id.clone(), "stale".into()))
    );
    assert!(matches!(
        store.batch(&child.id).unwrap().unwrap().0.state,
        State::Invalidated { .. }
    ));
    assert!(store.ci_only_completions().unwrap().is_empty());
    assert!(matches!(
        scheduler.release_completed_ci_only(&root.id, 1),
        Err(sluss::scheduler::SchedulerError::CiConflict)
    ));
    drop(store);
    let mut store = Store::open(dir.path().join("queue.sqlite")).unwrap();
    assert!(store.ci_evidence(&root.id).unwrap().unwrap().conflicted);
    assert_eq!(numbers(&plan(&mut store, 3).unwrap()), [1, 2, 3, 4]);
}

#[test]
fn missing_previously_observed_job_creates_a_durable_conflict() {
    use sluss::{scheduler::SchedulerError, verification::CiObservationError};
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    store
        .bind_policy_paths(&root.id, &[".github/workflows/ci.yml".into()])
        .unwrap();
    store.start_integration(&root.id).unwrap();
    store
        .finish_integration(&root.id, &"b".repeat(40), &"c".repeat(64))
        .unwrap();
    let plan = DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
        integration_sha: "b".repeat(40),
        staging_ref: format!("refs/heads/sluss/{}", root.id),
        policy_digest: "c".repeat(64),
        required_jobs: ["test".into()].into(),
        suites: ["full".into()].into(),
        selection_policy_digest: None,
    };
    scheduler
        .reserve_batch_dispatch(
            &root.id,
            &Request {
                pools: [("rust".into(), 1)].into(),
            },
            &plan,
        )
        .unwrap()
        .unwrap();
    scheduler
        .claim_batch_dispatch(&root.id, ".github/workflows/ci.yml")
        .unwrap();
    scheduler.record_receipt(&root.id, 1).unwrap();
    scheduler.observe_dispatch(&root.id, 1).unwrap();
    let mut snapshot = ActionsSnapshot {
        run: ActionsRun {
            id: 1,
            run_attempt: 1,
            workflow_id: 3,
            head_sha: "b".repeat(40),
            head_branch: Some(format!("sluss/{}", root.id)),
            path: ".github/workflows/ci.yml".into(),
            event: "workflow_dispatch".into(),
            status: "completed".into(),
            conclusion: Some("success".into()),
            repository: ActionsRepository { id: 2 },
            head_repository: Some(ActionsRepository { id: 2 }),
        },
        jobs: vec![ActionsJob {
            id: 101,
            run_id: 1,
            head_sha: "b".repeat(40),
            name: "test".into(),
            status: "completed".into(),
            conclusion: Some("success".into()),
            started_at: None,
            completed_at: None,
            runner_id: Some(7),
        }],
    };
    // A terminal successful run cannot establish evidence while even an
    // optional job remains unfinished or has no final conclusion.
    let mut extra = snapshot.jobs[0].clone();
    extra.id = 102;
    extra.name = "optional".into();
    extra.status = "in_progress".into();
    extra.conclusion = None;
    snapshot.jobs.push(extra);
    assert!(matches!(
        scheduler.record_ci_observation(&root.id, &snapshot),
        Err(SchedulerError::CiObservation(CiObservationError::Ambiguous))
    ));
    assert!(store.ci_evidence(&root.id).unwrap().is_none());
    assert!(scheduler.complete_ci_only(&root.id).is_err());
    drop(scheduler);
    let mut scheduler = Scheduler::open(
        dir.path().join("queue.sqlite"),
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
        },
    )
    .unwrap();
    assert!(store.ci_evidence(&root.id).unwrap().is_none());
    snapshot.jobs.pop();
    scheduler
        .record_ci_observation(&root.id, &snapshot)
        .unwrap();
    // Simulate a summary saved by an older version: completion must
    // re-evaluate the saved snapshot rather than trust its old success flag.
    let conn = rusqlite::Connection::open(dir.path().join("queue.sqlite")).unwrap();
    let saved: String = conn
        .query_row(
            "SELECT body FROM ci_observations WHERE id=?1",
            [&root.id],
            |r| r.get(0),
        )
        .unwrap();
    let mut legacy: sluss::scheduler::CiObservationRecord = serde_json::from_str(&saved).unwrap();
    let mut optional = legacy.snapshot.as_ref().unwrap().jobs[0].clone();
    optional.id = 102;
    optional.name = "optional".into();
    optional.status = "queued".into();
    optional.conclusion = None;
    legacy.snapshot.as_mut().unwrap().jobs.push(optional);
    conn.execute(
        "UPDATE ci_observations SET body=?2 WHERE id=?1",
        [&root.id, &serde_json::to_string(&legacy).unwrap()],
    )
    .unwrap();
    assert!(matches!(
        scheduler.complete_ci_only(&root.id),
        Err(SchedulerError::CiObservation(CiObservationError::Ambiguous))
    ));
    assert!(matches!(
        store.batch(&root.id).unwrap().unwrap().0.state,
        State::Testing { .. }
    ));
    conn.execute(
        "UPDATE ci_observations SET body=?2 WHERE id=?1",
        [&root.id, &saved],
    )
    .unwrap();
    snapshot.run.id = 99;
    assert!(matches!(
        scheduler.record_ci_observation(&root.id, &snapshot),
        Err(SchedulerError::CiObservation(CiObservationError::Identity))
    ));
    assert!(!store.ci_evidence(&root.id).unwrap().unwrap().conflicted);
    snapshot.run.id = 1;
    snapshot.jobs.clear();
    assert!(matches!(
        scheduler.record_ci_observation(&root.id, &snapshot),
        Err(SchedulerError::CiConflict)
    ));
    assert!(matches!(
        store.batch(&root.id).unwrap().unwrap().0.state,
        State::Invalidated { .. }
    ));
    drop(store);
    let store = Store::open(dir.path().join("queue.sqlite")).unwrap();
    assert!(store.ci_evidence(&root.id).unwrap().unwrap().conflicted);
    assert!(matches!(
        scheduler.complete_ci_only(&root.id),
        Err(SchedulerError::CiConflict)
    ));
}

#[test]
fn late_direct_receipt_conflict_invalidates_batch_and_journals_each_cancellation() {
    use sluss::{
        batch::State,
        scheduler::{CancellationClaim, SchedulerError},
    };
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let batch = plan(&mut store, 1).unwrap();
    let id = &batch.id;
    store
        .bind_policy_paths(id, &[".github/workflows/ci.yml".into()])
        .unwrap();
    store.start_integration(id).unwrap();
    store
        .finish_integration(id, &"b".repeat(40), &"c".repeat(64))
        .unwrap();
    let dispatch = DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
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
                pools: [("rust".into(), 1)].into(),
            },
            &dispatch,
        )
        .unwrap()
        .unwrap();
    assert!(
        scheduler
            .claim_batch_dispatch(id, ".github/workflows/ci.yml")
            .unwrap()
    );
    scheduler.observe_dispatch(id, 41).unwrap();
    assert!(matches!(
        scheduler.record_receipt(id, 42),
        Err(SchedulerError::DispatchReceiptConflict)
    ));
    assert!(matches!(
        store.batch(id).unwrap().unwrap().0.state,
        State::Invalidated { .. }
    ));
    assert_eq!(scheduler.receipt_conflict(id).unwrap(), Some((41, 42)));
    assert!(matches!(
        scheduler.claim_stale_cancellation(id),
        Err(SchedulerError::DispatchReceiptConflict)
    ));
    assert!(matches!(
        scheduler.claim_conflict_cancellation(id, 99),
        Err(SchedulerError::DispatchReceiptConflict)
    ));
    assert!(matches!(
        scheduler.claim_conflict_cancellation(id, 41),
        Ok(CancellationClaim::New(_))
    ));
    assert!(matches!(
        scheduler.claim_conflict_cancellation(id, 41),
        Ok(CancellationClaim::Uncertain)
    ));
    assert!(matches!(
        scheduler.claim_conflict_cancellation(id, 42),
        Ok(CancellationClaim::New(_))
    ));
    scheduler.record_conflict_cancellation(id, 42).unwrap();
    assert!(matches!(
        scheduler.claim_conflict_cancellation(id, 42),
        Ok(CancellationClaim::AlreadyRequested)
    ));
    let conn = rusqlite::Connection::open(dir.path().join("queue.sqlite")).unwrap();
    conn.execute(
        "UPDATE dispatch_receipt_sources SET source='direct' WHERE id=?1",
        [id],
    )
    .unwrap();
    assert!(matches!(
        scheduler.release_invalidated_ci_only(id, 41),
        Err(SchedulerError::DispatchReceiptConflict)
    ));
    assert_eq!(scheduler.pending_dispatches().unwrap().len(), 1);
}

#[test]
fn cancellation_claim_survives_a_late_receipt_conflict_without_resending() {
    use sluss::scheduler::{CancellationClaim, SchedulerError};
    let (dir, mut store, _inbox, mut scheduler) = setup();
    let batch = plan(&mut store, 1).unwrap();
    let id = &batch.id;
    store
        .bind_policy_paths(id, &[".github/workflows/ci.yml".into()])
        .unwrap();
    store.start_integration(id).unwrap();
    store
        .finish_integration(id, &"b".repeat(40), &"c".repeat(64))
        .unwrap();
    let dispatch = DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
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
                pools: [("rust".into(), 1)].into(),
            },
            &dispatch,
        )
        .unwrap()
        .unwrap();
    scheduler
        .claim_batch_dispatch(id, ".github/workflows/ci.yml")
        .unwrap();
    scheduler.observe_dispatch(id, 41).unwrap();
    store.remove_candidate(batch.candidates[0].number).unwrap();
    assert!(matches!(
        scheduler.claim_stale_cancellation(id),
        Ok(CancellationClaim::New(_))
    ));
    assert!(matches!(
        scheduler.record_receipt(id, 42),
        Err(SchedulerError::DispatchReceiptConflict)
    ));
    assert!(matches!(
        scheduler.claim_conflict_cancellation(id, 41),
        Ok(CancellationClaim::Uncertain)
    ));
    scheduler.record_stale_cancellation(id, 41).unwrap();
    assert!(matches!(
        scheduler.claim_conflict_cancellation(id, 41),
        Ok(CancellationClaim::AlreadyRequested)
    ));
    drop(scheduler);
    let pools = BTreeMap::from([("rust".into(), 1)]);
    let mut restarted = Scheduler::open(
        dir.path().join("queue.sqlite"),
        Capacity {
            pools: pools.clone(),
            hosts: BTreeMap::from([(
                "sluss-host-test".into(),
                Host {
                    slots: 1,
                    runner_ids: [7].into(),
                    pools,
                },
            )]),
        },
    )
    .unwrap();
    assert!(matches!(
        restarted.claim_conflict_cancellation(id, 41),
        Ok(CancellationClaim::AlreadyRequested)
    ));
}

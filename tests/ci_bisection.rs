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
    let id = &batch.id;
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
    assert!(scheduler.claim_dispatch(id).unwrap());
    scheduler.record_receipt(id, run_id).unwrap();
    scheduler.observe_dispatch(id, run_id).unwrap();
    let snapshot = ActionsSnapshot {
        run: ActionsRun {
            id: run_id,
            run_attempt: 1,
            workflow_id: 3,
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
    scheduler.finish_dispatch(id, run_id).unwrap(); // local test assumes the runner is no longer consuming capacity
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

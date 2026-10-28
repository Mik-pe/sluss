use sluss::{
    batch::{Candidate, State},
    store::{AdmissionCandidate, Store, StoreError},
    webhook::Inbox,
};
use std::sync::{Arc, Barrier};

fn with_evidence(candidates: &[Candidate]) -> Vec<AdmissionCandidate> {
    candidates
        .iter()
        .map(|c| AdmissionCandidate {
            candidate: c.clone(),
            evidence_digest: format!("{:064x}", c.number),
        })
        .collect()
}
fn ready_queue(path: &std::path::Path) {
    let mut inbox = Inbox::open(path).unwrap();
    let mut store = Store::open(path).unwrap();
    inbox
        .record("initial", "pull_request", &[1; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let candidates: Vec<_> = (1..=4)
        .map(|number| Candidate {
            number,
            head_sha: format!("{number:040x}"),
        })
        .collect();
    store
        .apply_admission_reconciliation(&event, &"f".repeat(64), &with_evidence(&candidates))
        .unwrap();
}
#[test]
fn batch_planning_respects_pause_pending_events_capacity_and_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    let mut unbound = Store::open(&db).unwrap();
    assert!(matches!(
        unbound.plan_batch("batch-0123456789abcdef", &"a".repeat(40), 2, 1),
        Err(StoreError::Unbound)
    ));
    drop(unbound);
    ready_queue(&db);
    let mut store = Store::open(&db).unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    store.set_paused(true).unwrap();
    assert!(
        store
            .plan_batch("batch-0123456789abcdef", &"a".repeat(40), 2, 1)
            .unwrap()
            .is_none()
    );
    store.set_paused(false).unwrap();
    inbox
        .record("new-event", "pull_request_review", &[2; 32], 1, 2)
        .unwrap();
    assert!(
        store
            .plan_batch("batch-0123456789abcdef", &"a".repeat(40), 2, 1)
            .unwrap()
            .is_none()
    );
    let event = inbox.pending().unwrap().remove(0);
    let candidates = store.candidates().unwrap();
    store
        .apply_admission_reconciliation(&event, &"f".repeat(64), &with_evidence(&candidates))
        .unwrap();
    let batch = store
        .plan_batch("batch-0123456789abcdef", &"a".repeat(40), 2, 1)
        .unwrap()
        .unwrap();
    assert_eq!(
        batch
            .candidates
            .iter()
            .map(|c| c.number)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert!(
        store
            .plan_batch("batch-1123456789abcdef", &"a".repeat(40), 2, 1)
            .unwrap()
            .is_none()
    );
    let second = store
        .plan_batch("batch-1123456789abcdef", &"a".repeat(40), 2, 2)
        .unwrap()
        .unwrap();
    assert_eq!(
        second
            .candidates
            .iter()
            .map(|c| c.number)
            .collect::<Vec<_>>(),
        [3, 4]
    );
    assert!(matches!(
        store.plan_batch("batch-0123456789abcdef", &"a".repeat(40), 2, 3),
        Err(StoreError::Conflict)
    ));
    let (mut first, version) = store.batch(&batch.id).unwrap().unwrap();
    first.fail("test failure".into()).unwrap();
    store.update_batch(&first, version).unwrap();
    let retried = store
        .plan_batch("batch-2123456789abcdef", &"a".repeat(40), 2, 2)
        .unwrap()
        .unwrap();
    assert_eq!(
        retried
            .candidates
            .iter()
            .map(|c| c.number)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(retried.state, State::Planned);
}
#[test]
fn concurrent_planners_cannot_duplicate_prs_or_exceed_active_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let barrier = Arc::new(Barrier::new(8));
    let handles: Vec<_> = (0..8)
        .map(|i| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut store = Store::open(db).unwrap();
                barrier.wait();
                store
                    .plan_batch(&format!("batch-{i:016}"), &"a".repeat(40), 4, 1)
                    .unwrap()
            })
        })
        .collect();
    let batches: Vec<_> = handles
        .into_iter()
        .filter_map(|h| h.join().unwrap())
        .collect();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].candidates.len(), 4);
}
#[test]
fn direct_batch_insertion_cannot_duplicate_a_live_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let mut store = Store::open(&db).unwrap();
    let batch = store
        .plan_batch("batch-0123456789abcdef", &"a".repeat(40), 2, 2)
        .unwrap()
        .unwrap();
    let duplicate = sluss::batch::Batch::new(
        "manual".into(),
        "base".into(),
        vec![batch.candidates[0].clone()],
    )
    .unwrap();
    assert!(matches!(
        store.insert_batch(&duplicate),
        Err(StoreError::Conflict)
    ));
}

#[test]
fn batch_dispatch_rechecks_queue_before_claim_and_can_cancel_unstarted_work() {
    use sluss::scheduler::{Capacity, DispatchPlan, Host, Request, Scheduler, SchedulerError};
    use std::collections::BTreeMap;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let mut store = Store::open(&db).unwrap();
    let id = "batch-0123456789abcdef";
    store
        .plan_batch(id, &"a".repeat(40), 2, 1)
        .unwrap()
        .unwrap();
    store.start_integration(id).unwrap();
    store
        .finish_integration(id, &"b".repeat(40), &"c".repeat(64))
        .unwrap();
    let pools = BTreeMap::from([("rust".into(), 1)]);
    let mut scheduler = Scheduler::open(
        &db,
        Capacity {
            pools: pools.clone(),
            hosts: BTreeMap::from([(
                "host".into(),
                Host {
                    runner_ids: Default::default(),
                    slots: 1,
                    pools: pools.clone(),
                },
            )]),
        },
    )
    .unwrap();
    let req = Request { pools };
    let plan = DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
        integration_sha: "b".repeat(40),
        staging_ref: format!("refs/heads/sluss/{id}"),
        policy_digest: "c".repeat(64),
        required_jobs: std::collections::BTreeSet::from(["job-1-0".into()]),
        suites: std::collections::BTreeSet::from(["full".into()]),
        selection_policy_digest: None,
    };
    assert!(
        scheduler
            .reserve_batch_dispatch(id, &req, &plan)
            .unwrap()
            .is_some()
    );
    store.set_paused(true).unwrap();
    assert!(matches!(
        scheduler.claim_dispatch(id),
        Err(SchedulerError::StaleBatch)
    ));
    store.set_paused(false).unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    inbox
        .record("review-changed", "pull_request_review", &[9; 32], 1, 2)
        .unwrap();
    assert!(matches!(
        scheduler.claim_dispatch(id),
        Err(SchedulerError::StaleBatch)
    ));
    let event = inbox.pending().unwrap().remove(0);
    let mut candidates = store.candidates().unwrap();
    candidates[0].head_sha = "f".repeat(40);
    store
        .apply_admission_reconciliation(&event, &"f".repeat(64), &with_evidence(&candidates))
        .unwrap();
    assert!(matches!(
        scheduler.claim_dispatch(id),
        Err(SchedulerError::StaleBatch)
    ));
    assert_eq!(scheduler.cancel_invalidated_prepared().unwrap(), 1);
    assert_eq!(scheduler.cancel_invalidated_prepared().unwrap(), 0);
    assert!(!scheduler.claim_dispatch(id).unwrap());
    assert!(!scheduler.cancel_prepared(id).unwrap());
    assert!(scheduler.pending_dispatches().unwrap().is_empty());
    assert!(scheduler.reserve("next", &req).unwrap().is_some());
}

#[test]
fn changed_review_evidence_invalidates_batch_without_changing_head() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let mut store = Store::open(&db).unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    let batch = store
        .plan_batch("batch-0123456789abcdef", &"a".repeat(40), 2, 1)
        .unwrap()
        .unwrap();
    let (mut stale, version) = store.batch(&batch.id).unwrap().unwrap();
    inbox
        .record("changed-review", "pull_request_review", &[5; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let candidates = store.candidates().unwrap();
    let mut updated = with_evidence(&candidates);
    updated[0].evidence_digest = "e".repeat(64);
    store
        .apply_admission_reconciliation(&event, &"f".repeat(64), &updated)
        .unwrap();
    assert_eq!(store.candidates().unwrap(), candidates);
    assert!(matches!(
        store.batch(&batch.id).unwrap().unwrap().0.state,
        State::Invalidated { .. }
    ));
    stale.begin_build().unwrap();
    assert!(matches!(
        store.update_batch(&stale, version),
        Err(StoreError::Conflict)
    ));
}

#[test]
fn debounce_survives_restart_and_resets_only_for_changed_candidates() {
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let raw = rusqlite::Connection::open(&db).unwrap();
    raw.execute("UPDATE candidate_stability SET changed_ms=1", [])
        .unwrap();
    let mut store = Store::open(&db).unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    inbox
        .record("unchanged", "pull_request", &[8; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let candidates = store.candidates().unwrap();
    store
        .apply_admission_reconciliation(&event, &"f".repeat(64), &with_evidence(&candidates))
        .unwrap();
    let timestamp: i64 = raw
        .query_row(
            "SELECT changed_ms FROM candidate_stability WHERE number=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(timestamp, 1);
    inbox
        .record("new-push", "pull_request", &[9; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let mut candidates = store.candidates().unwrap();
    candidates[0].head_sha = "f".repeat(40);
    store
        .apply_admission_reconciliation(&event, &"f".repeat(64), &with_evidence(&candidates))
        .unwrap();
    drop(store);
    let mut store = Store::open(&db).unwrap();
    let batch = store
        .plan_batch_debounced(
            "batch-0123456789abcdef",
            &"a".repeat(40),
            4,
            2,
            Duration::from_secs(60),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        batch
            .candidates
            .iter()
            .map(|c| c.number)
            .collect::<Vec<_>>(),
        [2, 3, 4]
    );
    assert!(
        store
            .plan_batch_debounced(
                "batch-1123456789abcdef",
                &"a".repeat(40),
                4,
                2,
                Duration::from_secs(60)
            )
            .unwrap()
            .is_none()
    );
    raw.execute(
        "UPDATE candidate_stability SET changed_ms=1 WHERE number=1",
        [],
    )
    .unwrap();
    let batch = store
        .plan_batch_debounced(
            "batch-1123456789abcdef",
            &"a".repeat(40),
            4,
            2,
            Duration::from_secs(60),
        )
        .unwrap()
        .unwrap();
    assert_eq!(batch.candidates[0].number, 1);
    assert_eq!(batch.candidates[0].head_sha, "f".repeat(40));
}

#[test]
fn failed_batches_split_durably_and_failed_singletons_wait_for_change() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let mut store = Store::open(&db).unwrap();
    let base = "a".repeat(40);
    let parent = "batch-0000000000000000";
    let batch = store.plan_batch(parent, &base, 4, 1).unwrap().unwrap();
    assert_eq!(batch.candidates.len(), 4);
    store.start_integration(parent).unwrap();
    store
        .fail_integration(parent, "reproducible failure")
        .unwrap();
    drop(store);
    let mut store = Store::open(&db).unwrap();
    let first = store
        .plan_batch("batch-0000000000000001", &base, 4, 1)
        .unwrap()
        .unwrap();
    assert_eq!(
        first
            .candidates
            .iter()
            .map(|c| c.number)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    store.start_integration(&first.id).unwrap();
    store
        .fail_integration(&first.id, "reproducible failure")
        .unwrap();
    let second = store
        .plan_batch("batch-0000000000000002", &base, 4, 1)
        .unwrap()
        .unwrap();
    assert_eq!(
        second
            .candidates
            .iter()
            .map(|c| c.number)
            .collect::<Vec<_>>(),
        [3, 4]
    );
    store.start_integration(&second.id).unwrap();
    store
        .fail_integration(&second.id, "reproducible failure")
        .unwrap();
    for (n, expected) in [(3, 1), (4, 2), (5, 3), (6, 4)] {
        let batch = store
            .plan_batch(&format!("batch-{n:016}"), &base, 4, 1)
            .unwrap()
            .unwrap();
        assert_eq!(batch.candidates.len(), 1);
        assert_eq!(batch.candidates[0].number, expected);
        store.start_integration(&batch.id).unwrap();
        store
            .fail_integration(&batch.id, "reproducible failure")
            .unwrap();
    }
    assert!(
        store
            .plan_batch("batch-0000000000000007", &base, 4, 1)
            .unwrap()
            .is_none()
    );
    assert_eq!(store.failed_candidates().unwrap().len(), 4);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&db)
        .arg("queue")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("awaiting change"));
    let mut inbox = Inbox::open(&db).unwrap();
    inbox
        .record("new-head", "pull_request", &[9; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let mut candidates = store.candidates().unwrap();
    candidates[0].head_sha = "f".repeat(40);
    store
        .apply_admission_reconciliation(&event, &"f".repeat(64), &with_evidence(&candidates))
        .unwrap();
    let retry = store
        .plan_batch("batch-0000000000000007", &base, 4, 1)
        .unwrap()
        .unwrap();
    assert_eq!(retry.candidates.len(), 1);
    assert_eq!(retry.candidates[0].number, 1);
    assert_eq!(retry.candidates[0].head_sha, "f".repeat(40));
    assert_eq!(store.failed_candidates().unwrap().len(), 3);
}

#[test]
fn stale_split_is_discarded_and_does_not_block_other_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let mut store = Store::open(&db).unwrap();
    let base = "a".repeat(40);
    let parent = "batch-0000000000000000";
    store.plan_batch(parent, &base, 4, 2).unwrap().unwrap();
    store.start_integration(parent).unwrap();
    store
        .fail_integration(parent, "reproducible failure")
        .unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    inbox
        .record("updated", "pull_request", &[8; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let mut candidates = store.candidates().unwrap();
    candidates[0].head_sha = "f".repeat(40);
    store
        .apply_admission_reconciliation(&event, &"f".repeat(64), &with_evidence(&candidates))
        .unwrap();
    let right = store
        .plan_batch("batch-0000000000000001", &base, 4, 2)
        .unwrap()
        .unwrap();
    assert_eq!(
        right
            .candidates
            .iter()
            .map(|c| c.number)
            .collect::<Vec<_>>(),
        [3, 4]
    );
    let new = store
        .plan_batch("batch-0000000000000002", &base, 4, 2)
        .unwrap()
        .unwrap();
    assert_eq!(
        new.candidates.iter().map(|c| c.number).collect::<Vec<_>>(),
        [2, 1]
    );
}

#[test]
fn ci_observation_is_immutable_and_rejected_when_review_changes() {
    use sluss::{
        github::{ActionsJob, ActionsRepository, ActionsRun, ActionsSnapshot},
        scheduler::{Capacity, DispatchPlan, Host, Request, Scheduler, SchedulerError},
        verification::CiObservation,
    };
    use std::collections::{BTreeMap, BTreeSet};
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let mut store = Store::open(&db).unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    let id = "batch-0123456789abcdef";
    store
        .plan_batch(id, &"a".repeat(40), 1, 1)
        .unwrap()
        .unwrap();
    store.start_integration(id).unwrap();
    store
        .finish_integration(id, &"b".repeat(40), &"c".repeat(64))
        .unwrap();
    let pools = BTreeMap::from([("rust".into(), 1)]);
    let capacity = Capacity {
        pools: pools.clone(),
        hosts: BTreeMap::from([(
            "sluss-host-test".into(),
            Host {
                slots: 1,
                runner_ids: BTreeSet::from([7]),
                pools: pools.clone(),
            },
        )]),
    };
    let mut scheduler = Scheduler::open(&db, capacity.clone()).unwrap();
    let plan = DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
        integration_sha: "b".repeat(40),
        staging_ref: format!("refs/heads/sluss/{id}"),
        policy_digest: "c".repeat(64),
        required_jobs: BTreeSet::from(["test".into()]),
        suites: std::collections::BTreeSet::from(["full".into()]),
        selection_policy_digest: None,
    };
    scheduler
        .reserve_batch_dispatch(id, &Request { pools }, &plan)
        .unwrap()
        .unwrap();
    scheduler.claim_dispatch(id).unwrap();
    scheduler.record_receipt(id, 9).unwrap();
    scheduler.observe_dispatch(id, 9).unwrap();
    let mut snapshot = ActionsSnapshot {
        run: ActionsRun {
            id: 9,
            run_attempt: 1,
            workflow_id: 3,
            head_sha: "b".repeat(40),
            head_branch: Some(format!("sluss/{id}")),
            path: ".github/workflows/ci.yml".into(),
            event: "workflow_dispatch".into(),
            status: "completed".into(),
            conclusion: Some("success".into()),
            repository: ActionsRepository { id: 2 },
            head_repository: Some(ActionsRepository { id: 2 }),
        },
        jobs: vec![ActionsJob {
            id: 11,
            run_id: 9,
            head_sha: "b".repeat(40),
            name: "test".into(),
            status: "completed".into(),
            conclusion: Some("success".into()),
            started_at: None,
            completed_at: None,
            runner_id: Some(7),
        }],
    };
    assert_eq!(
        scheduler.record_ci_observation(id, &snapshot).unwrap(),
        CiObservation::Successful
    );
    assert_eq!(
        scheduler.record_ci_observation(id, &snapshot).unwrap(),
        CiObservation::Successful
    );
    snapshot.run.conclusion = Some("failure".into());
    assert!(matches!(
        scheduler.record_ci_observation(id, &snapshot),
        Err(SchedulerError::CiConflict)
    ));
    snapshot.run.conclusion = Some("success".into());
    inbox
        .record("review", "pull_request_review", &[5; 32], 1, 2)
        .unwrap();
    assert!(matches!(
        scheduler.record_ci_observation(id, &snapshot),
        Err(SchedulerError::StaleBatch)
    ));
    let event = inbox.pending().unwrap().remove(0);
    let mut evidence = with_evidence(&store.candidates().unwrap());
    evidence[0].evidence_digest = "f".repeat(64);
    store
        .apply_admission_reconciliation(&event, &"f".repeat(64), &evidence)
        .unwrap();
    assert!(matches!(
        scheduler.record_ci_observation(id, &snapshot),
        Err(SchedulerError::StaleBatch)
    ));
    assert!(matches!(
        store.batch(id).unwrap().unwrap().0.state,
        State::Invalidated { .. }
    ));
    assert_eq!(scheduler.stale_dispatches().unwrap(), vec![id.to_string()]);
    assert!(matches!(
        scheduler.claim_stale_cancellation(id).unwrap(),
        sluss::scheduler::CancellationClaim::New(_)
    ));
    drop(scheduler);
    let mut scheduler = Scheduler::open(&db, capacity).unwrap();
    assert_eq!(
        scheduler.claim_stale_cancellation(id).unwrap(),
        sluss::scheduler::CancellationClaim::Uncertain
    );
    assert_eq!(scheduler.pending_dispatches().unwrap().len(), 1);
}

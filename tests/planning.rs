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
fn observed_planners_share_limits_and_reject_unbound_policy() {
    use sluss::store::PlanningSettings;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let mut store = Store::open(&db).unwrap();
    let settings = PlanningSettings {
        max_batch_size: 4,
        max_active_batches: 1,
        debounce: Duration::ZERO,
        batch_window: Duration::ZERO,
    };
    assert!(matches!(
        store.plan_observed_batch(1, 2, &"f".repeat(64), "main", &settings),
        Err(StoreError::Unbound)
    ));
    let mut inbox = Inbox::open(&db).unwrap();
    inbox
        .record("target-observed", "push", &[3; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let candidates = store.candidates().unwrap();
    store
        .apply_admission_reconciliation_with_base(
            &event,
            &"f".repeat(64),
            &with_evidence(&candidates),
            &"a".repeat(40),
            "main",
        )
        .unwrap();
    assert!(matches!(
        store.plan_observed_batch(1, 2, &"e".repeat(64), "main", &settings),
        Err(StoreError::PolicyMismatch)
    ));
    assert!(matches!(
        store.plan_observed_batch(1, 2, &"f".repeat(64), "other", &settings),
        Err(StoreError::PolicyMismatch)
    ));
    store.set_paused(true).unwrap();
    assert!(
        store
            .plan_observed_batch(1, 2, &"f".repeat(64), "main", &settings)
            .unwrap()
            .is_none()
    );
    store.set_paused(false).unwrap();
    let barrier = Arc::new(Barrier::new(8));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();
            let settings = settings.clone();
            std::thread::spawn(move || {
                let mut store = Store::open(db).unwrap();
                barrier.wait();
                store
                    .plan_observed_batch(1, 2, &"f".repeat(64), "main", &settings)
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
    assert_eq!(Store::open(&db).unwrap().active_batches().unwrap(), batches);
    let mut different = settings.clone();
    different.max_active_batches = 2;
    assert!(matches!(
        store.plan_observed_batch(1, 2, &"f".repeat(64), "main", &different),
        Err(StoreError::PolicyMismatch)
    ));
    assert!(matches!(
        store.plan_batch("batch-3123456789abcdef", &"a".repeat(40), 4, 2),
        Err(StoreError::PolicyMismatch)
    ));
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
        scheduler.claim_batch_dispatch(id, ".github/workflows/ci.yml"),
        Err(SchedulerError::StaleBatch)
    ));
    store.set_paused(false).unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    inbox
        .record("review-changed", "pull_request_review", &[9; 32], 1, 2)
        .unwrap();
    assert!(matches!(
        scheduler.claim_batch_dispatch(id, ".github/workflows/ci.yml"),
        Err(SchedulerError::StaleBatch)
    ));
    let event = inbox.pending().unwrap().remove(0);
    let mut candidates = store.candidates().unwrap();
    candidates[0].head_sha = "f".repeat(40);
    store
        .apply_admission_reconciliation(&event, &"f".repeat(64), &with_evidence(&candidates))
        .unwrap();
    assert!(matches!(
        scheduler.claim_batch_dispatch(id, ".github/workflows/ci.yml"),
        Err(SchedulerError::StaleBatch)
    ));
    assert_eq!(scheduler.cancel_invalidated_prepared().unwrap(), 1);
    assert_eq!(scheduler.cancel_invalidated_prepared().unwrap(), 0);
    assert!(
        !scheduler
            .claim_batch_dispatch(id, ".github/workflows/ci.yml")
            .unwrap()
    );
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
fn changing_target_base_invalidates_dispatch_and_stale_splits_atomically() {
    use sluss::scheduler::{Capacity, DispatchPlan, Host, Request, Scheduler, SchedulerError};
    use std::collections::{BTreeMap, BTreeSet};
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let mut store = Store::open(&db).unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    let base = "a".repeat(40);
    let changed = "d".repeat(40);
    let candidates = store.candidates().unwrap();
    inbox.record("bind-base", "push", &[10; 32], 1, 2).unwrap();
    let bind = inbox.pending().unwrap().remove(0);
    store
        .apply_admission_reconciliation_with_base(
            &bind,
            &"f".repeat(64),
            &with_evidence(&candidates),
            &base,
            "main",
        )
        .unwrap();
    let id = "batch-0123456789abcdef";
    let wrong_base = sluss::batch::Batch::new(
        "batch-3123456789abcdef".into(),
        changed.clone(),
        candidates.clone(),
    )
    .unwrap();
    assert!(matches!(
        store.insert_batch(&wrong_base),
        Err(StoreError::StaleBatch)
    ));
    store.plan_batch(id, &base, 4, 1).unwrap().unwrap();
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
                    runner_ids: BTreeSet::new(),
                    slots: 1,
                    pools: pools.clone(),
                },
            )]),
        },
    )
    .unwrap();
    let plan = DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
        integration_sha: "b".repeat(40),
        staging_ref: format!("refs/heads/sluss/{id}"),
        policy_digest: "c".repeat(64),
        required_jobs: BTreeSet::from(["test".into()]),
        suites: BTreeSet::from(["full".into()]),
        selection_policy_digest: None,
    };
    assert!(
        scheduler
            .reserve_batch_dispatch(id, &Request { pools }, &plan)
            .unwrap()
            .is_some()
    );
    assert!(scheduler.validate_dispatch_target(id, None).is_err());
    assert!(
        scheduler
            .validate_dispatch_target(id, Some(("other", &base)))
            .is_err()
    );
    assert!(
        scheduler
            .validate_dispatch_target(id, Some(("main", &changed)))
            .is_err()
    );
    scheduler
        .validate_dispatch_target(id, Some(("main", &base)))
        .unwrap();
    inbox
        .record("base-update", "push", &[11; 32], 1, 2)
        .unwrap();
    let update = inbox.pending().unwrap().remove(0);
    assert!(matches!(
        store.apply_admission_reconciliation(&update, &"f".repeat(64), &with_evidence(&candidates)),
        Err(StoreError::StaleBatch)
    ));
    store
        .apply_admission_reconciliation_with_base(
            &update,
            &"f".repeat(64),
            &with_evidence(&candidates),
            &changed,
            "main",
        )
        .unwrap();
    assert!(matches!(
        store.batch(id).unwrap().unwrap().0.state,
        State::Invalidated { .. }
    ));
    assert!(matches!(
        scheduler.claim_batch_dispatch(id, ".github/workflows/ci.yml"),
        Err(SchedulerError::StaleBatch)
    ));
    assert_eq!(scheduler.cancel_invalidated_prepared().unwrap(), 1);
    assert!(matches!(
        store.plan_batch("batch-1123456789abcdef", &base, 4, 1),
        Err(StoreError::StaleBatch)
    ));
    let split_parent = "batch-1123456789abcdef";
    store
        .plan_batch(split_parent, &changed, 4, 1)
        .unwrap()
        .unwrap();
    store.start_integration(split_parent).unwrap();
    store.fail_integration(split_parent, "conflict").unwrap();
    inbox.record("base-again", "push", &[12; 32], 1, 2).unwrap();
    let next = inbox.pending().unwrap().remove(0);
    store
        .apply_admission_reconciliation_with_base(
            &next,
            &"f".repeat(64),
            &with_evidence(&candidates),
            &base,
            "main",
        )
        .unwrap();
    let new_batch = store
        .plan_batch("batch-2123456789abcdef", &base, 4, 1)
        .unwrap()
        .unwrap();
    assert_eq!(
        new_batch.candidates.len(),
        4,
        "old-base split work must not constrain the new batch"
    );
}

#[test]
fn batch_window_survives_restart_and_full_batches_do_not_wait() {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let raw = rusqlite::Connection::open(&db).unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    raw.execute(
        "UPDATE candidate_stability SET changed_ms=?1",
        [now - 61_000],
    )
    .unwrap();
    let id = "batch-0123456789abcdef";
    let base = "a".repeat(40);
    let mut store = Store::open(&db).unwrap();
    assert!(
        store
            .plan_batch_with_window(
                id,
                &base,
                5,
                1,
                Duration::from_secs(60),
                Duration::from_secs(60)
            )
            .unwrap()
            .is_none()
    );
    drop(store);
    let mut store = Store::open(&db).unwrap();
    assert!(
        store
            .plan_batch_with_window(
                id,
                &base,
                5,
                1,
                Duration::from_secs(60),
                Duration::from_secs(60)
            )
            .unwrap()
            .is_none()
    );
    raw.execute(
        "UPDATE candidate_stability SET changed_ms=?1 WHERE number=1",
        [now - 121_000],
    )
    .unwrap();
    let batch = store
        .plan_batch_with_window(
            id,
            &base,
            5,
            1,
            Duration::from_secs(60),
            Duration::from_secs(60),
        )
        .unwrap()
        .unwrap();
    assert_eq!(batch.candidates.len(), 4);

    let other = tempfile::tempdir().unwrap();
    let other_db = other.path().join("queue.sqlite");
    ready_queue(&other_db);
    let mut other_store = Store::open(&other_db).unwrap();
    let full = other_store
        .plan_batch_with_window(id, &base, 4, 1, Duration::ZERO, Duration::from_secs(3600))
        .unwrap()
        .unwrap();
    assert_eq!(full.candidates.len(), 4);
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
        .plan_batch_with_window(
            "batch-0000000000000001",
            &base,
            4,
            1,
            std::time::Duration::ZERO,
            std::time::Duration::from_secs(3600),
        )
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
    store
        .bind_policy_paths(id, &[".github/workflows/ci.yml".into()])
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
    scheduler
        .claim_batch_dispatch(id, ".github/workflows/ci.yml")
        .unwrap();
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
    snapshot.jobs.push(ActionsJob {
        id: 12,
        run_id: 9,
        head_sha: "b".repeat(40),
        name: "skipped".into(),
        status: "completed".into(),
        conclusion: Some("skipped".into()),
        started_at: None,
        completed_at: None,
        runner_id: None,
    });
    let mut wrong_workflow = snapshot.clone();
    wrong_workflow.run.path = ".github/workflows/other.yml@refs/heads/sluss/example".into();
    assert!(matches!(
        scheduler.record_ci_observation(id, &wrong_workflow),
        Err(SchedulerError::CiObservation(
            sluss::verification::CiObservationError::Identity
        ))
    ));
    assert_eq!(
        scheduler.record_ci_observation(id, &snapshot).unwrap(),
        CiObservation::Successful
    );
    snapshot.jobs.swap(0, 1);
    assert_eq!(
        scheduler.record_ci_observation(id, &snapshot).unwrap(),
        CiObservation::Successful
    );
    snapshot.jobs.swap(0, 1);
    let conn = rusqlite::Connection::open(&db).unwrap();
    let original: String = conn
        .query_row("SELECT body FROM ci_observations WHERE id=?1", [id], |r| {
            r.get(0)
        })
        .unwrap();
    let mut legacy: serde_json::Value = serde_json::from_str(&original).unwrap();
    legacy.as_object_mut().unwrap().remove("snapshot");
    legacy.as_object_mut().unwrap().remove("runner_id");
    conn.execute(
        "UPDATE ci_observations SET body=?2 WHERE id=?1",
        rusqlite::params![id, legacy.to_string()],
    )
    .unwrap();
    assert_eq!(
        scheduler.record_ci_observation(id, &snapshot).unwrap(),
        CiObservation::Successful
    );
    assert!(
        store
            .ci_evidence(id)
            .unwrap()
            .unwrap()
            .record
            .snapshot
            .is_none(),
        "a later read must not invent historical details"
    );
    conn.execute(
        "UPDATE ci_observations SET body=?2 WHERE id=?1",
        rusqlite::params![id, original],
    )
    .unwrap();
    snapshot.jobs[0].id = 13;
    assert!(
        matches!(
            scheduler.record_job_metrics(id, &snapshot),
            Err(SchedulerError::CiConflict)
        ),
        "metrics must not record contradictory jobs first"
    );
    assert!(
        matches!(
            scheduler.record_ci_observation(id, &snapshot),
            Err(SchedulerError::StaleBatch)
        ),
        "a contradictory job ID invalidates the active batch"
    );
    let evidence = store.ci_evidence(id).unwrap().unwrap();
    assert!(evidence.conflicted);
    assert_eq!(evidence.record.snapshot.unwrap().jobs[0].id, 11);
    let report = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&db)
        .args(["ci-evidence", "--batch-id", id])
        .output()
        .unwrap();
    assert!(report.status.success());
    assert!(String::from_utf8_lossy(&report.stdout).contains("conflict: true"));
    snapshot.jobs[0].id = 11;
    drop(scheduler);
    let mut scheduler = Scheduler::open(&db, capacity.clone()).unwrap();
    assert!(
        matches!(
            scheduler.record_ci_observation(id, &snapshot),
            Err(SchedulerError::StaleBatch)
        ),
        "the invalidation survives restart even if later reads match"
    );
    assert!(matches!(
        scheduler.complete_ci_only(id),
        Err(SchedulerError::CiConflict)
    ));
    assert!(matches!(
        scheduler.record_job_metrics(id, &snapshot),
        Err(SchedulerError::CiConflict)
    ));
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

#[test]
fn busy_host_does_not_block_an_independent_eligible_host() {
    use sluss::{
        github::{RepositoryRunner, RunnerLabel},
        scheduler::{Capacity, DispatchPlan, Host, Request, Scheduler, SchedulerError},
        verification::{InventoryError, available_repository_hosts},
    };
    use std::collections::{BTreeMap, BTreeSet};
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let mut store = Store::open(&db).unwrap();
    let ids = ["batch-0000000000000001", "batch-0000000000000002"];
    for id in ids {
        store
            .plan_batch(id, &"a".repeat(40), 2, 2)
            .unwrap()
            .unwrap();
        store.start_integration(id).unwrap();
        store
            .finish_integration(id, &"b".repeat(40), &"c".repeat(64))
            .unwrap();
    }
    let pools = BTreeMap::from([("rust".into(), 2)]);
    let host = |runner_id| Host {
        slots: 1,
        runner_ids: BTreeSet::from([runner_id]),
        pools: BTreeMap::from([("rust".into(), 1)]),
    };
    let capacity = Capacity {
        pools,
        hosts: BTreeMap::from([
            ("sluss-host-a".into(), host(1)),
            ("sluss-host-b".into(), host(2)),
        ]),
    };
    let runner = |id, label: &str, busy| RepositoryRunner {
        id,
        name: format!("runner-{id}"),
        status: "online".into(),
        busy,
        labels: vec![
            RunnerLabel {
                name: "self-hosted".into(),
            },
            RunnerLabel { name: label.into() },
        ],
    };
    let mut inventory = vec![
        runner(1, "sluss-host-a", true),
        runner(2, "sluss-host-b", false),
    ];
    let eligible = available_repository_hosts(&capacity, &inventory).unwrap();
    assert_eq!(eligible, BTreeSet::from(["sluss-host-b".into()]));
    let mut scheduler = Scheduler::open(&db, capacity.clone()).unwrap();
    let request = Request {
        pools: BTreeMap::from([("rust".into(), 1)]),
    };
    let make_plan = |id| DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
        integration_sha: "b".repeat(40),
        staging_ref: format!("refs/heads/sluss/{id}"),
        policy_digest: "c".repeat(64),
        required_jobs: BTreeSet::from(["test".into()]),
        suites: BTreeSet::from(["full".into()]),
        selection_policy_digest: None,
    };
    let first = scheduler
        .reserve_batch_dispatch_on(ids[0], &request, &make_plan(ids[0]), &eligible)
        .unwrap()
        .unwrap();
    assert_eq!(first.host, "sluss-host-b");
    assert!(
        scheduler
            .reserve_batch_dispatch_on(ids[1], &request, &make_plan(ids[1]), &eligible)
            .unwrap()
            .is_none()
    );
    inventory[1].busy = true;
    let none = available_repository_hosts(&capacity, &inventory).unwrap();
    assert!(none.is_empty());
    assert!(
        scheduler
            .reserve_batch_dispatch_on(ids[0], &request, &make_plan(ids[0]), &none)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        scheduler
            .reserve_batch_dispatch_on(ids[0], &request, &make_plan(ids[0]), &eligible)
            .unwrap()
            .unwrap(),
        first
    );
    assert!(matches!(
        scheduler.reserve_batch_dispatch_on(
            ids[1],
            &request,
            &make_plan(ids[1]),
            &BTreeSet::from(["unknown".into()])
        ),
        Err(SchedulerError::Invalid)
    ));
    inventory[0].labels.pop();
    assert!(matches!(
        available_repository_hosts(&capacity, &inventory),
        Err(InventoryError::RunnerMismatch)
    ));
}

#[test]
fn capacity_wait_measurements_survive_restart_and_do_not_double_count() {
    use sluss::scheduler::{Capacity, DispatchPlan, Host, Request, Scheduler};
    use std::collections::{BTreeMap, BTreeSet};
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let mut store = Store::open(&db).unwrap();
    let id = "batch-0123456789abcdef";
    store
        .plan_batch(id, &"a".repeat(40), 1, 1)
        .unwrap()
        .unwrap();
    store.start_integration(id).unwrap();
    store
        .finish_integration(id, &"b".repeat(40), &"c".repeat(64))
        .unwrap();
    let pools = BTreeMap::from([("macos".into(), 1), ("rust".into(), 1)]);
    let capacity = Capacity {
        pools: pools.clone(),
        hosts: BTreeMap::from([(
            "sluss-host-mac".into(),
            Host {
                slots: 1,
                runner_ids: BTreeSet::from([1]),
                pools: pools.clone(),
            },
        )]),
    };
    let request = Request { pools };
    let plan = DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
        integration_sha: "b".repeat(40),
        staging_ref: format!("refs/heads/sluss/{id}"),
        policy_digest: "c".repeat(64),
        required_jobs: BTreeSet::from(["test".into()]),
        suites: BTreeSet::from(["full".into()]),
        selection_policy_digest: None,
    };
    let unavailable = BTreeSet::new();
    let available = BTreeSet::from(["sluss-host-mac".into()]);
    let mut scheduler = Scheduler::open(&db, capacity.clone()).unwrap();
    assert!(
        scheduler
            .reserve_batch_dispatch_on(id, &request, &plan, &unavailable)
            .unwrap()
            .is_none()
    );
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute(
        "UPDATE ci_capacity_waits SET started_ms=1 WHERE batch_id=?1",
        [id],
    )
    .unwrap();
    let altered_request = Request {
        pools: BTreeMap::from([("rust".into(), 1)]),
    };
    assert!(
        scheduler
            .reserve_batch_dispatch_on(id, &altered_request, &plan, &unavailable)
            .unwrap()
            .is_none()
    );
    let restarted_at: i64 = conn
        .query_row(
            "SELECT started_ms FROM ci_capacity_waits WHERE batch_id=?1 AND ended_ms IS NULL",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        restarted_at > 1,
        "a changed request begins a fresh observation"
    );
    assert!(
        scheduler
            .reserve_batch_dispatch_on(id, &request, &plan, &unavailable)
            .unwrap()
            .is_none()
    );
    conn.execute(
        "UPDATE ci_capacity_waits SET started_ms=started_ms-60000 WHERE batch_id=?1",
        [id],
    )
    .unwrap();
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let db = db.clone();
            let capacity = capacity.clone();
            let request = request.clone();
            let plan = plan.clone();
            std::thread::spawn(move || {
                Scheduler::open(db, capacity)
                    .unwrap()
                    .reserve_batch_dispatch_on(id, &request, &plan, &BTreeSet::new())
                    .unwrap()
            })
        })
        .collect();
    assert!(
        handles
            .into_iter()
            .all(|handle| handle.join().unwrap().is_none())
    );
    let open_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM ci_capacity_waits WHERE batch_id=?1 AND ended_ms IS NULL",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(open_count, 1);
    assert!(
        scheduler
            .reserve_batch_dispatch_on(id, &request, &plan, &unavailable)
            .unwrap()
            .is_none()
    );
    assert!(
        store.metrics().unwrap().capacity_waits.is_empty(),
        "incomplete intervals are not counted"
    );
    drop(scheduler);
    let mut scheduler = Scheduler::open(&db, capacity).unwrap();
    assert!(
        scheduler
            .reserve_batch_dispatch_on(id, &request, &plan, &available)
            .unwrap()
            .is_some()
    );
    assert!(
        scheduler
            .reserve_batch_dispatch_on(id, &request, &plan, &available)
            .unwrap()
            .is_some()
    );
    let summary = store.metrics().unwrap();
    assert_eq!(summary.capacity_waits.len(), 2);
    let mac = &summary.capacity_waits["macos"];
    let rust = &summary.capacity_waits["rust"];
    assert_eq!(mac.intervals, 1);
    assert_eq!(
        mac, rust,
        "multi-pool intervals appear under each requested pool"
    );
    assert!(mac.elapsed_milliseconds >= 60_000);
    assert_eq!(
        summary.runner_milliseconds, 0,
        "waiting is never runner time"
    );
    // A later prepared reservation becomes unavailable; a backward wall clock
    // invalidates that sample without preventing the reservation retry.
    assert!(
        scheduler
            .reserve_batch_dispatch_on(id, &request, &plan, &unavailable)
            .unwrap()
            .is_none()
    );
    conn.execute(
        "UPDATE ci_capacity_waits SET started_ms=?2 WHERE batch_id=?1 AND ended_ms IS NULL",
        rusqlite::params![id, i64::MAX],
    )
    .unwrap();
    assert!(
        scheduler
            .reserve_batch_dispatch_on(id, &request, &plan, &available)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store.metrics().unwrap().capacity_waits,
        summary.capacity_waits
    );
}

#[test]
fn workflow_policy_paths_must_have_been_frozen_before_staging() {
    use sluss::scheduler::{Capacity, DispatchPlan, Host, Request, Scheduler, SchedulerError};
    use std::collections::{BTreeMap, BTreeSet};
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let mut store = Store::open(&db).unwrap();
    let id = "batch-0123456789abcdef";
    store
        .plan_batch(id, &"a".repeat(40), 2, 1)
        .unwrap()
        .unwrap();
    store.bind_staging_selection(id, None).unwrap();
    store.start_integration(id).unwrap();
    // Simulate a legacy/manual batch completed without a frozen file list.
    store
        .finish_integration(id, &"b".repeat(40), &"c".repeat(64))
        .unwrap();
    store
        .bind_policy_paths(id, &[".github/workflows/ci.yml".into()])
        .unwrap();
    let pools = BTreeMap::from([("rust".into(), 1)]);
    let mut scheduler = Scheduler::open(
        &db,
        Capacity {
            pools: pools.clone(),
            hosts: BTreeMap::from([(
                "sluss-host-test".into(),
                Host {
                    slots: 1,
                    runner_ids: BTreeSet::from([7]),
                    pools: pools.clone(),
                },
            )]),
        },
    )
    .unwrap();
    let request = Request { pools };
    let plan = |batch_id: &str| DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
        integration_sha: "b".repeat(40),
        staging_ref: format!("refs/heads/sluss/{batch_id}"),
        policy_digest: "c".repeat(64),
        required_jobs: ["test".into()].into(),
        suites: ["full".into()].into(),
        selection_policy_digest: None,
    };
    scheduler
        .reserve_batch_dispatch(id, &request, &plan(id))
        .unwrap()
        .unwrap();
    assert!(matches!(
        scheduler.claim_batch_dispatch(id, ".github/workflows/ci.yml"),
        Err(SchedulerError::WorkflowPolicyMismatch)
    ));
    assert!(scheduler.cancel_prepared(id).unwrap());
    store.set_paused(true).unwrap();
    assert!(
        store
            .migrate_staging_selection("full", "full", true)
            .unwrap()
    );
    assert!(matches!(
        store.batch(id).unwrap().unwrap().0.state,
        State::Invalidated { .. }
    ));
    store.set_paused(false).unwrap();
    let next = "batch-1123456789abcdef";
    store
        .plan_batch(next, &"a".repeat(40), 2, 1)
        .unwrap()
        .unwrap();
    store
        .bind_policy_paths(next, &[".github/workflows/ci.yml".into()])
        .unwrap();
    store.start_integration(next).unwrap();
    store
        .finish_integration(next, &"b".repeat(40), &"c".repeat(64))
        .unwrap();
    scheduler
        .reserve_batch_dispatch(next, &request, &plan(next))
        .unwrap()
        .unwrap();
    assert!(matches!(
        scheduler.claim_dispatch(next),
        Err(SchedulerError::WorkflowPolicyMismatch)
    ));
    assert!(matches!(
        scheduler.claim_batch_dispatch(next, ".github/workflows/other.yml"),
        Err(SchedulerError::WorkflowPolicyMismatch)
    ));
    assert!(
        scheduler
            .claim_batch_dispatch(next, ".github/workflows/ci.yml")
            .unwrap()
    );
    assert_eq!(
        scheduler.dispatch_workflow(next).unwrap().as_deref(),
        Some(".github/workflows/ci.yml")
    );
}

#[test]
fn planner_migration_preserves_pending_split_and_rejects_unsafe_changes() {
    use sluss::store::PlanningSettings;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let mut store = Store::open(&db).unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    inbox.record("target", "push", &[3; 32], 1, 2).unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let candidates = store.candidates().unwrap();
    store
        .apply_admission_reconciliation_with_base(
            &event,
            &"f".repeat(64),
            &with_evidence(&candidates),
            &"a".repeat(40),
            "main",
        )
        .unwrap();
    let old = PlanningSettings {
        max_batch_size: 4,
        max_active_batches: 1,
        debounce: Duration::ZERO,
        batch_window: Duration::ZERO,
    };
    let mut next = old.clone();
    next.max_batch_size = 2;
    assert!(matches!(
        store.migrate_planning_settings("unbound", &next, 1, 2, "main"),
        Err(StoreError::PlanningMigrationNeedsPause)
    ));
    let parent = store
        .plan_observed_batch(1, 2, &"f".repeat(64), "main", &old)
        .unwrap()
        .unwrap();
    let digest = store.planning_settings().unwrap().unwrap().1;
    store.set_paused(true).unwrap();
    assert!(matches!(
        store.migrate_planning_settings(&digest, &next, 1, 2, "main"),
        Err(StoreError::PlanningMigrationBusy)
    ));
    store.set_paused(false).unwrap();
    store.start_integration(&parent.id).unwrap();
    store.fail_integration(&parent.id, "conflict").unwrap();
    store.set_paused(true).unwrap();
    let mut too_small = next.clone();
    too_small.max_batch_size = 1;
    assert!(matches!(
        store.migrate_planning_settings(&digest, &too_small, 1, 2, "main"),
        Err(StoreError::PlanningMigrationSplitTooLarge)
    ));
    assert!(matches!(
        store.migrate_planning_settings(&"f".repeat(64), &next, 1, 2, "main"),
        Err(StoreError::PolicyMismatch)
    ));
    assert!(matches!(
        store.migrate_planning_settings(&digest, &next, 1, 3, "main"),
        Err(StoreError::RepositoryMismatch)
    ));
    assert!(matches!(
        store.migrate_planning_settings(&digest, &next, 1, 2, "other"),
        Err(StoreError::PolicyMismatch)
    ));
    assert_eq!(
        store.planning_settings().unwrap().unwrap(),
        (old.clone(), digest.clone())
    );
    let budgets = std::collections::BTreeMap::from([("rust".to_owned(), 1)]);
    let capacity = sluss::scheduler::Capacity {
        pools: budgets.clone(),
        hosts: std::collections::BTreeMap::from([(
            "host".to_owned(),
            sluss::scheduler::Host {
                slots: 1,
                runner_ids: Default::default(),
                pools: budgets.clone(),
            },
        )]),
    };
    let mut scheduler = sluss::scheduler::Scheduler::open(&db, capacity).unwrap();
    scheduler
        .reserve("other-work", &sluss::scheduler::Request { pools: budgets })
        .unwrap()
        .unwrap();
    assert!(matches!(
        store.migrate_planning_settings(&digest, &next, 1, 2, "main"),
        Err(StoreError::PlanningMigrationBusy)
    ));
    scheduler.release("other-work").unwrap();
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute(
        "INSERT INTO dispatch_intents(id,plan,state) VALUES('uncertain','{}','sending')",
        [],
    )
    .unwrap();
    assert!(matches!(
        store.migrate_planning_settings(&digest, &next, 1, 2, "main"),
        Err(StoreError::PlanningMigrationBusy)
    ));
    conn.execute("DELETE FROM dispatch_intents WHERE id='uncertain'", [])
        .unwrap();
    conn.execute("INSERT INTO ci_candidate_completions(number,head_sha,digest,base_sha,batch_id,conclusion) VALUES(99,?1,?2,?3,'history','success')", rusqlite::params!["f".repeat(40),"e".repeat(64),"a".repeat(40)]).unwrap();
    let config = dir.path().join("sluss.toml");
    std::fs::write(&config, "version=1\n[github]\napp_client_id='test'\ninstallation_id=1\nrepository_id=2\nowner='owner'\nrepository='repo'\n[queue]\ntarget='main'\nmax_batch_size=2\nmax_active_batches=1\ndebounce_seconds=0\nbatch_window_seconds=0\n[checks]\nrequired_reviews=1\n").unwrap();
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&db)
        .arg("status")
        .output()
        .unwrap();
    assert!(status.status.success());
    assert!(String::from_utf8_lossy(&status.stdout).contains(&digest));
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&db)
        .args(["migrate-planning", "--expected-digest", &digest, "--config"])
        .arg(&config)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(store.paused().unwrap());
    let (current, new_digest) = store.planning_settings().unwrap().unwrap();
    assert_eq!(current, next);
    assert_ne!(digest, new_digest);
    assert!(
        !store
            .migrate_planning_settings(&new_digest, &next, 1, 2, "main")
            .unwrap()
    );
    let conn = rusqlite::Connection::open(&db).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM planning_migrations", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
    let holds: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM ci_candidate_completions WHERE batch_id='history'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(holds, 1);
    drop(store);
    let mut store = Store::open(&db).unwrap();
    assert_eq!(store.planning_settings().unwrap().unwrap().1, new_digest);
    store.set_paused(false).unwrap();
    assert!(matches!(
        store.plan_observed_batch(1, 2, &"f".repeat(64), "main", &old),
        Err(StoreError::PolicyMismatch)
    ));
    let first = store
        .plan_observed_batch(1, 2, &"f".repeat(64), "main", &next)
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
}

#[test]
fn concurrent_planner_migrations_compare_the_same_frozen_version() {
    use sluss::store::PlanningSettings;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    ready_queue(&db);
    let mut store = Store::open(&db).unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    inbox.record("target", "push", &[3; 32], 1, 2).unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let candidates = store.candidates().unwrap();
    store
        .apply_admission_reconciliation_with_base(
            &event,
            &"f".repeat(64),
            &with_evidence(&candidates),
            &"a".repeat(40),
            "main",
        )
        .unwrap();
    store.set_paused(true).unwrap();
    let initial = PlanningSettings {
        max_batch_size: 4,
        max_active_batches: 1,
        debounce: Duration::ZERO,
        batch_window: Duration::ZERO,
    };
    assert!(
        store
            .migrate_planning_settings("unbound", &initial, 1, 2, "main")
            .unwrap()
    );
    let digest = initial.digest().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [2, 3]
        .into_iter()
        .map(|size| {
            let db = db.clone();
            let digest = digest.clone();
            let barrier = barrier.clone();
            let mut next = initial.clone();
            next.max_batch_size = size;
            std::thread::spawn(move || {
                let mut store = Store::open(db).unwrap();
                barrier.wait();
                store.migrate_planning_settings(&digest, &next, 1, 2, "main")
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| matches!(r, Ok(true))).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(StoreError::PolicyMismatch)))
            .count(),
        1
    );
    assert!(store.paused().unwrap());
    let conn = rusqlite::Connection::open(&db).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM planning_migrations", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);
    // Unknown future fields must never be silently discarded by an older binary.
    let body: String = conn
        .query_row("SELECT body FROM planning_config WHERE id=1", [], |r| {
            r.get(0)
        })
        .unwrap();
    let mut value: serde_json::Value = serde_json::from_str(&body).unwrap();
    value["future_limit"] = serde_json::json!(1);
    conn.execute(
        "UPDATE planning_config SET body=?1 WHERE id=1",
        [serde_json::to_string(&value).unwrap()],
    )
    .unwrap();
    assert!(store.planning_settings().is_err());
    assert!(
        store
            .migrate_planning_settings(&digest, &initial, 1, 2, "main")
            .is_err()
    );
}

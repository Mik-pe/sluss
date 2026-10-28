use sluss::{
    batch::{Candidate, State},
    store::{Store, StoreError},
    webhook::Inbox,
};
use std::sync::{Arc, Barrier};

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
        .apply_reconciliation(&event, &"f".repeat(64), &candidates)
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
        .apply_reconciliation(&event, &"f".repeat(64), &candidates)
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

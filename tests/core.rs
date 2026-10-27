use sluss::{
    batch::{Batch, BatchError, Candidate, State, bisect},
    store::{Store, StoreError},
};

fn pr(number: u64, sha: &str) -> Candidate {
    Candidate {
        number,
        head_sha: sha.into(),
    }
}
fn batch() -> Batch {
    Batch::new(
        "batch-1".into(),
        "base".into(),
        vec![pr(1, "head-1"), pr(2, "head-2")],
    )
    .unwrap()
}

#[test]
fn transitions_are_ordered_and_invalidation_is_terminal() {
    let mut b = batch();
    assert_eq!(b.record_success(), Err(BatchError::InvalidTransition));
    assert_eq!(
        b.begin_testing("integrated".into()),
        Err(BatchError::InvalidTransition)
    );
    b.begin_build().unwrap();
    assert_eq!(
        b.begin_testing(String::new()),
        Err(BatchError::EmptyIntegration)
    );
    b.begin_testing("integrated".into()).unwrap();
    b.record_success().unwrap();
    assert_eq!(
        b.state,
        State::Verified {
            integration_sha: "integrated".into()
        }
    );
    b.invalidate("PR head changed".into()).unwrap();
    assert_eq!(b.record_success(), Err(BatchError::InvalidTransition));
    assert_eq!(b.begin_build(), Err(BatchError::InvalidTransition));
}

#[test]
fn failed_batch_is_terminal_and_bisection_preserves_order() {
    let mut b = batch();
    b.begin_build().unwrap();
    b.fail("conflict".into()).unwrap();
    assert_eq!(b.begin_build(), Err(BatchError::InvalidTransition));
    assert_eq!(
        bisect(&b.candidates),
        Some((&b.candidates[..1], &b.candidates[1..]))
    );
    assert!(bisect(&b.candidates[..1]).is_none());
    let items = vec![pr(1, "a"), pr(2, "b"), pr(3, "c"), pr(4, "d"), pr(5, "e")];
    let (left, right) = bisect(&items).unwrap();
    assert_eq!((left.len(), right.len()), (2, 3));
    assert_eq!([left, right].concat(), items);
}

#[test]
fn rejects_empty_and_duplicate_batch_inputs() {
    assert_eq!(
        Batch::new("b".into(), "base".into(), vec![]),
        Err(BatchError::Empty)
    );
    assert_eq!(
        Batch::new("b".into(), "base".into(), vec![pr(1, "a"), pr(1, "b")]),
        Err(BatchError::DuplicateCandidate)
    );
    assert_eq!(
        Batch::new("b".into(), "base".into(), vec![pr(0, "a")]),
        Err(BatchError::InvalidCandidate)
    );
}

#[test]
fn queue_and_pause_survive_restart_and_updated_heads_move_to_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite");
    {
        let mut s = Store::open(&path).unwrap();
        s.observe_candidate(&pr(1, "a")).unwrap();
        s.observe_candidate(&pr(2, "b")).unwrap();
        s.observe_candidate(&pr(1, "a")).unwrap();
        assert_eq!(s.candidates().unwrap(), vec![pr(1, "a"), pr(2, "b")]);
        s.observe_candidate(&pr(1, "c")).unwrap();
        s.set_paused(true).unwrap();
    }
    let mut s = Store::open(&path).unwrap();
    assert!(s.paused().unwrap());
    assert_eq!(s.candidates().unwrap(), vec![pr(2, "b"), pr(1, "c")]);
    assert!(s.remove_candidate(1).unwrap());
    assert!(!s.remove_candidate(1).unwrap());
}

#[test]
fn optimistic_batch_update_rejects_stale_worker_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite");
    let mut first = Store::open(&path).unwrap();
    first.observe_candidate(&pr(1, "head-1")).unwrap();
    first.observe_candidate(&pr(2, "head-2")).unwrap();
    first.insert_batch(&batch()).unwrap();
    assert!(matches!(
        first.insert_batch(&batch()),
        Err(StoreError::Conflict)
    ));
    let second = Store::open(&path).unwrap();
    let (mut fresh, version) = first.batch("batch-1").unwrap().unwrap();
    let (mut stale, stale_version) = second.batch("batch-1").unwrap().unwrap();
    fresh.begin_build().unwrap();
    first.update_batch(&fresh, version).unwrap();
    stale.invalidate("old observation".into()).unwrap();
    assert!(matches!(
        second.update_batch(&stale, stale_version),
        Err(StoreError::Conflict)
    ));
    drop(first);
    drop(second);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened.batch("batch-1").unwrap().unwrap().0.state,
        State::Building
    );
}

#[test]
fn changed_head_invalidates_batch_atomically_and_rejects_stale_workers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite");
    let mut store = Store::open(&path).unwrap();
    store.observe_candidate(&pr(1, "head-1")).unwrap();
    store.observe_candidate(&pr(2, "head-2")).unwrap();
    store.insert_batch(&batch()).unwrap();
    let (mut old, version) = store.batch("batch-1").unwrap().unwrap();
    old.begin_build().unwrap();
    store.observe_candidate(&pr(1, "new-head")).unwrap();
    assert!(matches!(
        store.update_batch(&old, version),
        Err(StoreError::Conflict)
    ));
    assert!(matches!(
        store.batch("batch-1").unwrap().unwrap().0.state,
        State::Invalidated { .. }
    ));
    assert!(matches!(
        store.insert_batch(
            &Batch::new("new-batch".into(), "base".into(), batch().candidates).unwrap()
        ),
        Err(StoreError::StaleCandidate)
    ));
}

#[test]
fn removing_a_candidate_invalidates_its_verified_batch() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("queue.sqlite")).unwrap();
    store.observe_candidate(&pr(1, "head-1")).unwrap();
    store.observe_candidate(&pr(2, "head-2")).unwrap();
    let mut b = batch();
    b.begin_build().unwrap();
    b.begin_testing("integrated".into()).unwrap();
    b.record_success().unwrap();
    store.insert_batch(&b).unwrap();
    store.remove_candidate(2).unwrap();
    assert!(matches!(
        store.batch("batch-1").unwrap().unwrap().0.state,
        State::Invalidated { .. }
    ));
}

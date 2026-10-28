use sluss::{
    batch::Candidate,
    store::{AdmissionCandidate, Store, StoreError},
    webhook::Inbox,
};
use std::process::Command;

fn candidate(head: char, evidence: char) -> AdmissionCandidate {
    AdmissionCandidate {
        candidate: Candidate {
            number: 19,
            head_sha: head.to_string().repeat(40),
        },
        evidence_digest: evidence.to_string().repeat(64),
    }
}
fn event(inbox: &mut Inbox, delivery: &str, hash: u8) -> sluss::webhook::Reconciliation {
    inbox
        .record(delivery, "pull_request", &[hash; 32], 42, 7)
        .unwrap();
    inbox.pending().unwrap().remove(0)
}
fn apply(
    store: &mut Store,
    job: &sluss::webhook::Reconciliation,
    candidates: &[AdmissionCandidate],
) {
    store
        .apply_admission_reconciliation_with_base(
            job,
            &"f".repeat(64),
            candidates,
            &"a".repeat(40),
            "main",
        )
        .unwrap();
}

#[test]
fn observations_follow_reconciled_versions_reviews_and_readmission() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite");
    let mut inbox = Inbox::open(&path).unwrap();
    let mut store = Store::open(&path).unwrap();
    let first = candidate('b', 'c');
    let job = event(&mut inbox, "first", 1);
    apply(&mut store, &job, std::slice::from_ref(&first));
    let original = Store::admission_history(&path, 19).unwrap();
    assert_eq!(original.len(), 1);
    assert_eq!(original[0].head_sha, first.candidate.head_sha);
    assert_eq!(original[0].evidence_digest, first.evidence_digest);
    assert_eq!(original[0].first_revision, job.revision);
    assert!(original[0].ended_ms.is_none());
    let next = event(&mut inbox, "refresh", 2);
    apply(&mut store, &next, std::slice::from_ref(&first));
    let observed = Store::admission_history(&path, 19).unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].first_observed_ms, original[0].first_observed_ms);
    assert_eq!(observed[0].last_revision, next.revision);
    assert!(observed[0].last_observed_ms >= observed[0].first_observed_ms);
    let changed_review = candidate('b', 'd');
    let job = event(&mut inbox, "review", 3);
    apply(&mut store, &job, &[changed_review]);
    let observed = Store::admission_history(&path, 19).unwrap();
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0].evidence_digest, "d".repeat(64));
    assert!(observed[0].ended_ms.is_none());
    assert!(observed[1].ended_ms.is_some());
    let job = event(&mut inbox, "head", 4);
    apply(&mut store, &job, &[candidate('e', 'd')]);
    let job = event(&mut inbox, "not-ready", 5);
    apply(&mut store, &job, &[]);
    drop(store);
    let mut store = Store::open(&path).unwrap();
    assert!(
        Store::admission_history(&path, 19)
            .unwrap()
            .iter()
            .all(|o| o.ended_ms.is_some())
    );
    let job = event(&mut inbox, "readmitted", 6);
    apply(&mut store, &job, &[first]);
    let observed = Store::admission_history(&path, 19).unwrap();
    assert_eq!(observed.len(), 4);
    assert!(observed[0].ended_ms.is_none());
    assert_ne!(observed[0].first_revision, observed[3].first_revision);
    assert!(observed.iter().all(|o| !o.clock_uncertain));
    let output = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&path)
        .args(["admission-history", "--pr", "19"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("4 local admission observations"));
    assert!(text.contains("not the exact time"));
}

#[test]
fn stale_reconciliation_never_records_admission_and_manual_changes_close_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite");
    let mut inbox = Inbox::open(&path).unwrap();
    let mut store = Store::open(&path).unwrap();
    let original = candidate('b', 'c');
    let initial = event(&mut inbox, "first", 10);
    apply(&mut store, &initial, std::slice::from_ref(&original));
    let stale = event(&mut inbox, "old", 11);
    inbox
        .record("new", "pull_request", &[12; 32], 42, 7)
        .unwrap();
    assert!(matches!(
        store.apply_admission_reconciliation_with_base(
            &stale,
            &"f".repeat(64),
            &[candidate('d', 'e')],
            &"a".repeat(40),
            "main"
        ),
        Err(StoreError::Conflict)
    ));
    assert_eq!(Store::admission_history(&path, 19).unwrap().len(), 1);
    let job = inbox.pending().unwrap().remove(0);
    apply(&mut store, &job, std::slice::from_ref(&original));
    store.observe_candidate(&original.candidate).unwrap();
    assert!(
        Store::admission_history(&path, 19).unwrap()[0]
            .ended_ms
            .is_some()
    );
    assert_eq!(Store::admission_history(&path, 19).unwrap().len(), 1);
    let job = event(&mut inbox, "fresh", 13);
    apply(&mut store, &job, &[original]);
    assert!(store.remove_candidate(19).unwrap());
    assert!(
        Store::admission_history(&path, 19)
            .unwrap()
            .iter()
            .all(|o| o.ended_ms.is_some())
    );
}

#[test]
fn backwards_clock_marks_observations_uncertain() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite");
    let mut inbox = Inbox::open(&path).unwrap();
    let mut store = Store::open(&path).unwrap();
    let original = candidate('b', 'c');
    let job = event(&mut inbox, "first", 20);
    apply(&mut store, &job, std::slice::from_ref(&original));
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute("UPDATE admission_observations_v1 SET first_observed_ms=first_observed_ms+3600000,last_observed_ms=last_observed_ms+3600000", []).unwrap();
    let job = event(&mut inbox, "clock-back", 21);
    apply(&mut store, &job, &[original]);
    let observations = Store::admission_history(&path, 19).unwrap();
    assert!(observations[0].clock_uncertain);
    let job = event(&mut inbox, "changed", 22);
    apply(&mut store, &job, &[candidate('d', 'e')]);
    let observations = Store::admission_history(&path, 19).unwrap();
    assert!(observations[0].clock_uncertain && observations[1].clock_uncertain);
}

#[test]
fn offline_history_does_not_create_or_migrate_database() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.sqlite");
    assert!(Store::admission_history(&missing, 19).is_err());
    assert!(!missing.exists());
    let legacy = dir.path().join("legacy.sqlite");
    let conn = rusqlite::Connection::open(&legacy).unwrap();
    conn.execute("CREATE TABLE untouched(id INTEGER)", [])
        .unwrap();
    assert!(Store::admission_history(&legacy, 19).unwrap().is_empty());
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn concurrent_reconciliation_records_one_observation() {
    use std::sync::{Arc, Barrier};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite");
    let mut inbox = Inbox::open(&path).unwrap();
    Store::open(&path).unwrap();
    let job = event(&mut inbox, "race", 30);
    let barrier = Arc::new(Barrier::new(2));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let path = path.clone();
        let barrier = barrier.clone();
        let revision = job.revision;
        workers.push(std::thread::spawn(move || {
            let mut store = Store::open(path).unwrap();
            let job = sluss::webhook::Reconciliation {
                installation: 42,
                repository: 7,
                revision,
            };
            barrier.wait();
            store.apply_admission_reconciliation_with_base(
                &job,
                &"f".repeat(64),
                &[candidate('b', 'c')],
                &"a".repeat(40),
                "main",
            )
        }));
    }
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(StoreError::Conflict)))
            .count(),
        1
    );
    assert_eq!(Store::admission_history(&path, 19).unwrap().len(), 1);
}

#[test]
fn target_base_change_starts_a_new_local_observation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite");
    let mut inbox = Inbox::open(&path).unwrap();
    let mut store = Store::open(&path).unwrap();
    let admitted = candidate('b', 'c');
    let first = event(&mut inbox, "first", 31);
    apply(&mut store, &first, std::slice::from_ref(&admitted));
    let second = event(&mut inbox, "base", 32);
    store
        .apply_admission_reconciliation_with_base(
            &second,
            &"f".repeat(64),
            &[admitted],
            &"d".repeat(40),
            "main",
        )
        .unwrap();
    let history = Store::admission_history(&path, 19).unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].base_sha, "d".repeat(40));
    assert!(history[0].ended_ms.is_none());
    assert!(history[1].ended_ms.is_some());
}

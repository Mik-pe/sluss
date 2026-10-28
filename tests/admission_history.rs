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
fn admission_migration_invalidates_queue_and_inflight_scans_until_reconciled() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite");
    let mut inbox = Inbox::open(&path).unwrap();
    let mut store = Store::open(&path).unwrap();
    let original = candidate('b', 'c');
    let job = event(&mut inbox, "first", 30);
    apply(&mut store, &job, std::slice::from_ref(&original));
    let stale = event(&mut inbox, "in-flight", 31);
    assert!(matches!(
        store.migrate_admission_policy(&"f".repeat(64), 0, &"e".repeat(64), 42, 7, "main"),
        Err(StoreError::AdmissionMigrationNeedsPause)
    ));
    store.set_paused(true).unwrap();
    assert!(matches!(
        store.migrate_admission_policy(&"f".repeat(64), 0, &"e".repeat(64), 42, 8, "main"),
        Err(StoreError::RepositoryMismatch)
    ));
    assert!(matches!(
        store.migrate_admission_policy(&"f".repeat(64), 0, &"e".repeat(64), 42, 7, "other"),
        Err(StoreError::PolicyMismatch)
    ));
    assert!(
        store
            .migrate_admission_policy(&"f".repeat(64), 0, &"e".repeat(64), 42, 7, "main")
            .unwrap()
    );
    assert_eq!(store.admission_policy_revision().unwrap(), 1);
    assert!(store.candidates().unwrap().is_empty());
    assert!(
        Store::admission_history(&path, 19).unwrap()[0]
            .ended_ms
            .is_some()
    );
    let fresh = inbox.pending().unwrap().remove(0);
    assert!(fresh.revision > stale.revision);
    assert_eq!(
        Store::admission_history_policy_epoch(&path).unwrap(),
        (1, fresh.revision)
    );
    assert!(matches!(
        store.apply_admission_reconciliation_with_base(
            &stale,
            &"f".repeat(64),
            std::slice::from_ref(&original),
            &"a".repeat(40),
            "main"
        ),
        Err(StoreError::PolicyMismatch)
    ));
    assert!(matches!(
        store.apply_admission_reconciliation_with_base(
            &stale,
            &"e".repeat(64),
            std::slice::from_ref(&original),
            &"a".repeat(40),
            "main"
        ),
        Err(StoreError::Conflict)
    ));
    drop(store);
    let mut store = Store::open(&path).unwrap();
    assert_eq!(
        store.repository_policy_digest().unwrap(),
        Some("e".repeat(64))
    );
    assert!(store.paused().unwrap());
    store
        .apply_admission_reconciliation_with_base(
            &fresh,
            &"e".repeat(64),
            std::slice::from_ref(&original),
            &"a".repeat(40),
            "main",
        )
        .unwrap();
    assert_eq!(store.candidates().unwrap(), vec![original.candidate]);
    let history = Store::admission_history(&path, 19).unwrap();
    assert_eq!(history.len(), 2);
    assert!(history[0].ended_ms.is_none() && history[1].ended_ms.is_some());
    assert!(history[0].first_revision >= fresh.revision);
    assert!(history[1].first_revision < fresh.revision);
    assert!(
        store
            .migrate_admission_policy(&"e".repeat(64), 1, &"f".repeat(64), 42, 7, "main")
            .unwrap()
    );
    assert!(matches!(
        store.migrate_admission_policy(&"f".repeat(64), 0, &"e".repeat(64), 42, 7, "main"),
        Err(StoreError::PolicyMismatch)
    ));
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute("DELETE FROM admission_policy_revision", [])
        .unwrap();
    drop(conn);
    drop(store);
    let mut reopened = Store::open(&path).unwrap();
    assert!(matches!(
        reopened.admission_policy_revision(),
        Err(StoreError::PolicyMismatch)
    ));
    assert!(matches!(
        Store::admission_history_policy_epoch(&path),
        Err(StoreError::PolicyMismatch)
    ));
    assert!(matches!(
        reopened.migrate_admission_policy(&"f".repeat(64), 0, &"e".repeat(64), 42, 7, "main"),
        Err(StoreError::PolicyMismatch)
    ));
}

#[test]
fn admission_migration_requires_drained_batches_and_dispatches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite");
    let mut inbox = Inbox::open(&path).unwrap();
    let mut store = Store::open(&path).unwrap();
    let job = event(&mut inbox, "first", 40);
    apply(&mut store, &job, &[candidate('b', 'c')]);
    let batch = store
        .plan_batch("batch-0123456789abcdef", &"a".repeat(40), 1, 1)
        .unwrap()
        .unwrap();
    store.set_paused(true).unwrap();
    assert!(matches!(
        store.migrate_admission_policy(&"f".repeat(64), 0, &"e".repeat(64), 42, 7, "main"),
        Err(StoreError::AdmissionMigrationBusy)
    ));
    assert_eq!(store.candidates().unwrap().len(), 1);
    store.set_paused(false).unwrap();
    store.start_integration(&batch.id).unwrap();
    store.fail_integration(&batch.id, "conflict").unwrap();
    store.set_paused(true).unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute("CREATE TABLE capacity_reservations(id TEXT)", [])
        .unwrap();
    conn.execute("INSERT INTO capacity_reservations VALUES('busy')", [])
        .unwrap();
    assert!(matches!(
        store.migrate_admission_policy(&"f".repeat(64), 0, &"e".repeat(64), 42, 7, "main"),
        Err(StoreError::AdmissionMigrationBusy)
    ));
    conn.execute("DELETE FROM capacity_reservations", [])
        .unwrap();
    conn.execute("CREATE TABLE dispatch_intents(state TEXT)", [])
        .unwrap();
    conn.execute("INSERT INTO dispatch_intents VALUES('sending')", [])
        .unwrap();
    assert!(matches!(
        store.migrate_admission_policy(&"f".repeat(64), 0, &"e".repeat(64), 42, 7, "main"),
        Err(StoreError::AdmissionMigrationBusy)
    ));
    conn.execute("UPDATE dispatch_intents SET state='finished'", [])
        .unwrap();
    assert!(
        store
            .migrate_admission_policy(&"f".repeat(64), 0, &"e".repeat(64), 42, 7, "main")
            .unwrap()
    );
}

#[test]
fn concurrent_admission_migrations_cannot_both_change_the_same_revision() {
    use std::sync::{Arc, Barrier};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite");
    let mut inbox = Inbox::open(&path).unwrap();
    let mut store = Store::open(&path).unwrap();
    let job = event(&mut inbox, "first", 50);
    apply(&mut store, &job, &[candidate('b', 'c')]);
    store.set_paused(true).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut workers = Vec::new();
    for next in ['d', 'e'] {
        let path = path.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            let mut store = Store::open(path).unwrap();
            barrier.wait();
            store.migrate_admission_policy(
                &"f".repeat(64),
                0,
                &next.to_string().repeat(64),
                42,
                7,
                "main",
            )
        }));
    }
    let results: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| matches!(r, Ok(true))).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(StoreError::PolicyMismatch)))
            .count(),
        1
    );
    assert_eq!(store.admission_policy_revision().unwrap(), 1);
    assert_eq!(inbox.pending().unwrap().len(), 1);
}

#[test]
fn admission_migration_cli_computes_the_policy_and_schedules_reconciliation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite");
    let config = dir.path().join("sluss.toml");
    let key = dir.path().join("app.pem");
    let generated = Command::new("openssl")
        .args([
            "genpkey",
            "-algorithm",
            "RSA",
            "-pkeyopt",
            "rsa_keygen_bits:2048",
            "-out",
        ])
        .arg(&key)
        .output()
        .unwrap();
    assert!(generated.status.success());
    std::fs::write(&config, "version=1\n[github]\napp_client_id='test'\ninstallation_id=42\nrepository_id=7\nowner='owner'\nrepository='repo'\n[queue]\ntarget='main'\nmax_batch_size=2\nmax_active_batches=1\ndebounce_seconds=0\nbatch_window_seconds=0\n[checks]\nrequired_reviews=2\n").unwrap();
    let mut inbox = Inbox::open(&path).unwrap();
    let mut store = Store::open(&path).unwrap();
    let job = event(&mut inbox, "first", 60);
    apply(&mut store, &job, &[candidate('b', 'c')]);
    store.set_paused(true).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&path)
        .args([
            "migrate-admission",
            "--expected-digest",
            &"f".repeat(64),
            "--expected-revision",
            "0",
            "--config",
        ])
        .arg(&config)
        .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", &key)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(store.paused().unwrap());
    assert!(store.candidates().unwrap().is_empty());
    assert_eq!(inbox.pending().unwrap().len(), 1);
    let status = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&path)
        .arg("status")
        .output()
        .unwrap();
    assert!(status.status.success());
    assert!(String::from_utf8_lossy(&status.stdout).contains("revision: 1"));
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

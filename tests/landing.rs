use sluss::landing::{IntentState, LandingError, LandingJournal, LandingPlan};
use std::sync::{Arc, Barrier};

fn plan(id: &str) -> LandingPlan {
    LandingPlan {
        operation_id: id.into(),
        batch_id: "batch-123".into(),
        installation_id: 12,
        repository_id: 34,
        repository_node_id: "R_kgDOVZy43Q".into(),
        target_ref: "refs/heads/main".into(),
        staging_ref: "refs/heads/sluss/batch-123".into(),
        base_sha: "a".repeat(40),
        staged_sha: "b".repeat(40),
        policy_digest: "c".repeat(64),
        admission_digest: "d".repeat(64),
        ci_evidence_digest: "e".repeat(64),
    }
}
fn ack(id: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"data":{"updateRefs":{"clientMutationId":id}}})).unwrap()
}

#[test]
fn request_checks_both_frozen_refs_without_force() {
    let p = plan("land-123");
    let body = p.graphql_request().unwrap();
    let input = &body["variables"]["input"];
    assert_eq!(input["repositoryId"], p.repository_node_id);
    assert_eq!(input["clientMutationId"], p.operation_id);
    let refs = input["refUpdates"].as_array().unwrap();
    assert_eq!(refs.len(), 2);
    assert_eq!(refs[0]["name"], p.target_ref);
    assert_eq!(refs[0]["beforeOid"], p.base_sha);
    assert_eq!(refs[0]["afterOid"], p.staged_sha);
    assert_eq!(refs[1]["name"], p.staging_ref);
    assert_eq!(refs[1]["beforeOid"], p.staged_sha);
    assert_eq!(refs[1]["afterOid"], p.staged_sha);
    assert!(refs.iter().all(|r| r["force"] == false));
}

#[test]
fn invalid_plans_cannot_be_prepared_or_encoded() {
    let dir = tempfile::tempdir().unwrap();
    let mut journal = LandingJournal::open(dir.path().join("journal.sqlite")).unwrap();
    let mut cases = Vec::new();
    let mut p = plan("invalid");
    p.staged_sha = "0".repeat(40);
    cases.push(p);
    let mut p = plan("invalid");
    p.staged_sha = p.base_sha.clone();
    cases.push(p);
    let mut p = plan("invalid");
    p.target_ref = "refs/heads/main\ncommit".into();
    cases.push(p);
    let mut p = plan("invalid");
    p.staging_ref = "refs/heads/other".into();
    cases.push(p);
    let mut p = plan("invalid");
    p.target_ref = "refs/heads/sluss/main".into();
    cases.push(p);
    let mut p = plan("invalid");
    p.target_ref = "refs/heads/main.lock".into();
    cases.push(p);
    let mut p = plan("invalid");
    p.ci_evidence_digest = "x".repeat(64);
    cases.push(p);
    for p in cases {
        assert!(matches!(p.graphql_request(), Err(LandingError::Invalid)));
        assert!(matches!(journal.prepare(&p), Err(LandingError::Invalid)));
    }
    assert!(journal.get("invalid").unwrap().is_none());
}

#[test]
fn intent_claim_survives_restart_without_a_retry_or_unlock() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("journal.sqlite");
    let p = plan("land-one");
    let mut journal = LandingJournal::open(&db).unwrap();
    assert_eq!(journal.prepare(&p).unwrap(), IntentState::Prepared);
    assert_eq!(journal.prepare(&p).unwrap(), IntentState::Prepared);
    let mut changed = p.clone();
    changed.policy_digest = "f".repeat(64);
    assert!(matches!(
        journal.prepare(&changed),
        Err(LandingError::IdConflict)
    ));
    assert_eq!(journal.claim(&p.operation_id).unwrap(), Some(p.clone()));
    drop(journal);
    let mut journal = LandingJournal::open(&db).unwrap();
    assert_eq!(
        journal.get(&p.operation_id).unwrap(),
        Some((p.clone(), IntentState::Sending))
    );
    assert_eq!(journal.prepare(&p).unwrap(), IntentState::Sending);
    assert!(journal.claim(&p.operation_id).unwrap().is_none());
    assert!(!journal.cancel(&p.operation_id).unwrap());
    assert!(matches!(
        journal.prepare(&plan("land-two")),
        Err(LandingError::TargetBusy)
    ));
}

#[test]
fn failed_partial_and_wrong_responses_never_release_the_target() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("journal.sqlite");
    let mut journal = LandingJournal::open(&db).unwrap();
    let p = plan("land-one");
    journal.prepare(&p).unwrap();
    journal.claim(&p.operation_id).unwrap();
    let good = ack(&p.operation_id);
    let partial = serde_json::to_vec(&serde_json::json!({"data":{"updateRefs":{"clientMutationId":p.operation_id}},"errors":[{"message":"failure"}]})).unwrap();
    for (status, body) in [
        (500, good.as_slice()),
        (200, b"not json".as_slice()),
        (200, b"{\"data\":null}".as_slice()),
        (200, ack("other").as_slice()),
        (200, partial.as_slice()),
    ] {
        assert!(matches!(
            journal.acknowledge("land-one", status, body),
            Err(LandingError::Uncertain)
        ));
        assert_eq!(
            journal.get("land-one").unwrap().unwrap().1,
            IntentState::Sending
        );
        assert!(matches!(
            journal.prepare(&plan("land-two")),
            Err(LandingError::TargetBusy)
        ));
    }
    assert!(journal.acknowledge("land-one", 200, &good).is_ok());
    assert!(journal.acknowledge("land-one", 200, &good).is_ok());
    assert_eq!(
        journal.get("land-one").unwrap().unwrap().1,
        IntentState::Acknowledged
    );
    assert_eq!(
        journal.prepare(&plan("land-two")).unwrap(),
        IntentState::Prepared
    );
}

#[test]
fn only_one_concurrent_claim_can_send_and_cancellation_is_before_claim() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("journal.sqlite");
    let mut journal = LandingJournal::open(&db).unwrap();
    journal.prepare(&plan("land-one")).unwrap();
    let barrier = Arc::new(Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut journal = LandingJournal::open(db).unwrap();
                barrier.wait();
                journal.claim("land-one").unwrap().is_some()
            })
        })
        .collect();
    assert_eq!(
        workers
            .into_iter()
            .map(|w| usize::from(w.join().unwrap()))
            .sum::<usize>(),
        1
    );
    assert!(!journal.cancel("land-one").unwrap());

    let mut other = plan("land-other");
    other.target_ref = "refs/heads/release".into();
    journal.prepare(&other).unwrap();
    assert!(journal.cancel("land-other").unwrap());
    assert!(journal.claim("land-other").unwrap().is_none());
    assert!(matches!(
        journal.acknowledge("land-other", 200, &ack("land-other")),
        Err(LandingError::Transition)
    ));
    assert_eq!(journal.prepare(&other).unwrap(), IntentState::Cancelled);
}

// The child is a separate test process, so the parent can kill it after the
// SQLite claim is committed without running destructors in the child.
#[test]
fn child_process_claim_for_crash_test() {
    let Some(db) = std::env::var_os("SLUSS_LANDING_CRASH_TEST_DB") else {
        return;
    };
    let marker = std::env::var_os("SLUSS_LANDING_CRASH_TEST_MARKER").unwrap();
    let mut journal = LandingJournal::open(db).unwrap();
    assert!(journal.claim("land-crash").unwrap().is_some());
    std::fs::write(marker, "claimed").unwrap();
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

#[test]
fn killed_sender_remains_uncertain_after_process_restart() {
    use std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("journal.sqlite");
    let marker = dir.path().join("claimed");
    let mut journal = LandingJournal::open(&db).unwrap();
    journal.prepare(&plan("land-crash")).unwrap();
    drop(journal);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_process_claim_for_crash_test",
            "--nocapture",
        ])
        .env("SLUSS_LANDING_CRASH_TEST_DB", &db)
        .env("SLUSS_LANDING_CRASH_TEST_MARKER", &marker)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !marker.exists() && Instant::now() < deadline {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let claimed = marker.exists();
    let _ = child.kill();
    child.wait().unwrap();
    assert!(claimed, "child did not commit the claim");
    let mut recovered = LandingJournal::open(&db).unwrap();
    assert_eq!(
        recovered.get("land-crash").unwrap().unwrap().1,
        IntentState::Sending
    );
    assert!(recovered.claim("land-crash").unwrap().is_none());
    assert!(!recovered.cancel("land-crash").unwrap());
    assert!(matches!(
        recovered.prepare(&plan("another")),
        Err(LandingError::TargetBusy)
    ));
}

#[test]
fn concurrent_intents_cannot_share_an_unresolved_target() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("journal.sqlite");
    LandingJournal::open(&db).unwrap();
    let barrier = Arc::new(Barrier::new(6));
    let tasks: Vec<_> = (0..6)
        .map(|n| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut journal = LandingJournal::open(db).unwrap();
                barrier.wait();
                journal.prepare(&plan(&format!("land-{n}")))
            })
        })
        .collect();
    let results: Vec<_> = tasks.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Ok(IntentState::Prepared)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(LandingError::TargetBusy)))
            .count(),
        5
    );
}

#[test]
fn inconsistent_stored_target_cannot_be_claimed() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("journal.sqlite");
    let mut journal = LandingJournal::open(&db).unwrap();
    let p = plan("land-one");
    journal.prepare(&p).unwrap();
    let external = rusqlite::Connection::open(&db).unwrap();
    external
        .execute(
            "UPDATE landing_intents_v1 SET target_ref='refs/heads/other' WHERE id='land-one'",
            [],
        )
        .unwrap();
    assert!(matches!(
        journal.claim("land-one"),
        Err(LandingError::Corrupt)
    ));
    assert!(matches!(journal.prepare(&p), Err(LandingError::Corrupt)));
}

#[test]
fn corrupted_claimed_intent_cannot_be_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("journal.sqlite");
    let mut journal = LandingJournal::open(&db).unwrap();
    let p = plan("land-one");
    journal.prepare(&p).unwrap();
    assert!(journal.claim(&p.operation_id).unwrap().is_some());
    let external = rusqlite::Connection::open(&db).unwrap();
    external
        .execute(
            "UPDATE landing_intents_v1 SET target_ref='refs/heads/other' WHERE id='land-one'",
            [],
        )
        .unwrap();
    assert!(matches!(
        journal.acknowledge("land-one", 200, &ack("land-one")),
        Err(LandingError::Corrupt)
    ));
    let state: String = external
        .query_row(
            "SELECT state FROM landing_intents_v1 WHERE id='land-one'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(state, "sending");
}

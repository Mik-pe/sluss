use sluss::{
    batch::Candidate,
    github::{ActionsJob, ActionsRepository, ActionsRun, ActionsSnapshot},
    landing::{IntentState, LandingError, LandingJournal},
    scheduler::{Capacity, DispatchPlan, Host, Request, Scheduler},
    store::{AdmissionCandidate, Store, StoreError},
    webhook::Inbox,
};
use std::collections::{BTreeMap, BTreeSet};

const BATCH: &str = "batch-0000000000000001";
fn candidate(head: &str, review: &str) -> AdmissionCandidate {
    AdmissionCandidate {
        candidate: Candidate {
            number: 1,
            head_sha: head.repeat(40),
        },
        evidence_digest: review.repeat(64),
    }
}
struct Fixture {
    _dir: tempfile::TempDir,
    db: std::path::PathBuf,
    store: Store,
    inbox: Inbox,
    journal: LandingJournal,
}
fn setup() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sluss.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    inbox
        .record("initial", "pull_request", &[1; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    store
        .apply_admission_reconciliation_with_base(
            &event,
            &"f".repeat(64),
            &[candidate("1", "2")],
            &"a".repeat(40),
            "main",
        )
        .unwrap();
    store
        .plan_batch(BATCH, &"a".repeat(40), 1, 1)
        .unwrap()
        .unwrap();
    store
        .bind_policy_paths(BATCH, &[".github/workflows/ci.yml".into()])
        .unwrap();
    store.start_integration(BATCH).unwrap();
    store
        .finish_integration(BATCH, &"b".repeat(40), &"c".repeat(64))
        .unwrap();
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
    let mut scheduler = Scheduler::open(&db, capacity).unwrap();
    let dispatch = DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
        integration_sha: "b".repeat(40),
        staging_ref: format!("refs/heads/sluss/{BATCH}"),
        policy_digest: "c".repeat(64),
        required_jobs: BTreeSet::from(["test".into()]),
        suites: BTreeSet::from(["full".into()]),
        selection_policy_digest: None,
    };
    scheduler
        .reserve_batch_dispatch(
            BATCH,
            &Request {
                pools: BTreeMap::from([("rust".into(), 1)]),
            },
            &dispatch,
        )
        .unwrap()
        .unwrap();
    assert!(
        scheduler
            .claim_batch_dispatch_with_rules(
                BATCH,
                ".github/workflows/ci.yml",
                &sluss::github::BranchRulesSnapshot {
                    repository_id: 2,
                    branch: "main".into(),
                    rules: vec![serde_json::json!({"type":"pull_request","ruleset_id":1})]
                }
            )
            .unwrap()
    );
    scheduler.record_receipt(BATCH, 42).unwrap();
    scheduler.observe_dispatch(BATCH, 42).unwrap();
    let snapshot = ActionsSnapshot {
        run: ActionsRun {
            id: 42,
            run_attempt: 1,
            workflow_id: 3,
            head_sha: "b".repeat(40),
            head_branch: Some(format!("sluss/{BATCH}")),
            path: ".github/workflows/ci.yml".into(),
            event: "workflow_dispatch".into(),
            status: "completed".into(),
            conclusion: Some("success".into()),
            repository: ActionsRepository { id: 2 },
            head_repository: Some(ActionsRepository { id: 2 }),
        },
        jobs: vec![ActionsJob {
            id: 142,
            run_id: 42,
            head_sha: "b".repeat(40),
            name: "test".into(),
            status: "completed".into(),
            conclusion: Some("success".into()),
            started_at: None,
            completed_at: None,
            runner_id: Some(7),
        }],
    };
    scheduler.record_ci_observation(BATCH, &snapshot).unwrap();
    let journal = LandingJournal::open(&db).unwrap();
    Fixture {
        _dir: dir,
        db,
        store,
        inbox,
        journal,
    }
}
fn prepare(f: &mut Fixture) -> sluss::landing::LandingPlan {
    f.journal
        .prepare_observed_batch("land-one", BATCH, "R_repo")
        .unwrap()
}
#[test]
fn bound_claim_uses_frozen_observation_and_cannot_use_unchecked_claim() {
    let mut f = setup();
    let plan = prepare(&mut f);
    assert_eq!(plan.base_sha, "a".repeat(40));
    assert_eq!(plan.staged_sha, "b".repeat(40));
    assert_eq!(plan.staging_ref, format!("refs/heads/sluss/{BATCH}"));
    assert!(matches!(
        f.journal.claim("land-one"),
        Err(LandingError::BatchBound)
    ));
    assert_eq!(
        f.journal.claim_observed_batch("land-one").unwrap(),
        Some(plan)
    );
    assert!(
        f.journal
            .claim_observed_batch("land-one")
            .unwrap()
            .is_none()
    );
}
#[test]
fn changed_review_head_or_target_prevents_claim_after_freeze() {
    for (head, review, base) in [("1", "3", "a"), ("3", "2", "a"), ("1", "2", "d")] {
        let mut f = setup();
        prepare(&mut f);
        f.inbox
            .record("changed", "pull_request_review", &[2; 32], 1, 2)
            .unwrap();
        let event = f.inbox.pending().unwrap().remove(0);
        f.store
            .apply_admission_reconciliation_with_base(
                &event,
                &"f".repeat(64),
                &[candidate(head, review)],
                &base.repeat(40),
                "main",
            )
            .unwrap();
        assert!(f.journal.claim_observed_batch("land-one").is_err());
        assert_eq!(
            f.journal.get("land-one").unwrap().unwrap().1,
            IntentState::Prepared
        );
    }
}
#[test]
fn pending_events_pause_or_ci_conflict_prevent_claim() {
    for mode in ["event", "pause", "conflict"] {
        let mut f = setup();
        prepare(&mut f);
        match mode {
            "event" => {
                f.inbox.record("new", "push", &[3; 32], 1, 2).unwrap();
            }
            "pause" => f.store.set_paused(true).unwrap(),
            _ => {
                let conn = rusqlite::Connection::open(&f.db).unwrap();
                conn.execute(
                    "INSERT INTO ci_observation_conflicts(id) VALUES(?1)",
                    [BATCH],
                )
                .unwrap();
            }
        }
        assert!(matches!(
            f.journal.claim_observed_batch("land-one"),
            Err(LandingError::StaleBatch) | Err(LandingError::Store(StoreError::StaleBatch))
        ));
        assert_eq!(
            f.journal.get("land-one").unwrap().unwrap().1,
            IntentState::Prepared
        );
    }
}
#[test]
fn missing_receipt_or_frozen_workflow_and_failed_ci_cannot_prepare() {
    for update in [
        "DELETE FROM dispatch_receipt_sources",
        "DELETE FROM dispatch_workflows",
        "DELETE FROM dispatch_branch_rules",
        "UPDATE ci_observations SET body=json_set(body,'$.observation',json('{\"Failed\":{\"conclusion\":\"failure\"}}'))",
    ] {
        let mut f = setup();
        let conn = rusqlite::Connection::open(&f.db).unwrap();
        conn.execute(update, []).unwrap();
        assert!(
            f.journal
                .prepare_observed_batch("land-one", BATCH, "R_repo")
                .is_err()
        );
        assert!(f.journal.get("land-one").unwrap().is_none());
    }
}

#[test]
fn changed_frozen_branch_rules_cannot_claim() {
    let mut f = setup();
    prepare(&mut f);
    let replacement = sluss::github::BranchRulesSnapshot {
        repository_id: 2,
        branch: "main".into(),
        rules: vec![serde_json::json!({"type":"required_linear_history","ruleset_id":1})],
    };
    let conn = rusqlite::Connection::open(&f.db).unwrap();
    conn.execute(
        "UPDATE dispatch_branch_rules SET body=?1,digest=?2 WHERE id=?3",
        rusqlite::params![
            serde_json::to_string(&replacement).unwrap(),
            replacement.digest().unwrap(),
            BATCH
        ],
    )
    .unwrap();
    assert!(matches!(
        f.journal.claim_observed_batch("land-one"),
        Err(LandingError::StaleBatch)
    ));
    assert_eq!(
        f.journal.get("land-one").unwrap().unwrap().1,
        IntentState::Prepared
    );
}

#[test]
fn changed_ci_snapshot_after_freeze_does_not_claim() {
    let mut f = setup();
    prepare(&mut f);
    let conn = rusqlite::Connection::open(&f.db).unwrap();
    conn.execute(
        "UPDATE ci_observations SET body=json_set(body,'$.snapshot.run.workflow_id',99)",
        [],
    )
    .unwrap();
    assert!(matches!(
        f.journal.claim_observed_batch("land-one"),
        Err(LandingError::StaleBatch)
    ));
    assert_eq!(
        f.journal.get("land-one").unwrap().unwrap().1,
        IntentState::Prepared
    );
}

#[test]
fn changed_policy_file_list_after_freeze_does_not_claim() {
    let mut f = setup();
    prepare(&mut f);
    let conn = rusqlite::Connection::open(&f.db).unwrap();
    conn.execute(
        "UPDATE batch_policy_paths SET paths=?1 WHERE batch_id=?2",
        [r#"[".github/workflows/ci.yml","policy.toml"]"#, BATCH],
    )
    .unwrap();
    assert!(matches!(
        f.journal.claim_observed_batch("land-one"),
        Err(LandingError::StaleBatch)
    ));
    assert_eq!(
        f.journal.get("land-one").unwrap().unwrap().1,
        IntentState::Prepared
    );
}

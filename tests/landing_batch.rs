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
            .claim_batch_dispatch_with_security(
                BATCH,
                ".github/workflows/ci.yml",
                &sluss::github::BranchSecurityObservation {
                    branch: sluss::github::BranchInfo { name:"main".into(),protected:true,commit:sluss::github::BranchCommit {sha:"a".repeat(40)} },
                    rules:sluss::github::BranchRulesSnapshot { repository_id: 2, branch:"main".into(), rules:vec![serde_json::json!({"type":"pull_request","ruleset_id":1,"ruleset_source_type":"Repository","ruleset_source":"owner/repo"})] },
                    protection:sluss::github::ProtectionObservation::Present(serde_json::json!({"enforce_admins":{"enabled":true},"allow_force_pushes":{"enabled":false},"allow_deletions":{"enabled":false},"required_linear_history":{"enabled":false},"required_status_checks":null,"required_pull_request_reviews":null,"restrictions":null})),
                    rulesets:vec![sluss::github::RulesetSecurityObservation {id:1, bypass_visible:true, detail:serde_json::json!({"id":1,"target":"branch","enforcement":"active","source_type":"Repository","source":"owner/repo","rules":[{"type":"pull_request"}],"bypass_actors":[]})}],
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
        "DELETE FROM dispatch_branch_security",
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

#[test]
fn changed_detailed_protection_cannot_claim() {
    let mut f = setup();
    prepare(&mut f);
    let conn = rusqlite::Connection::open(&f.db).unwrap();
    let body: String = conn
        .query_row(
            "SELECT body FROM dispatch_branch_security WHERE id=?1",
            [BATCH],
            |r| r.get(0),
        )
        .unwrap();
    let mut security: sluss::github::BranchSecurityObservation =
        serde_json::from_str(&body).unwrap();
    match &mut security.protection {
        sluss::github::ProtectionObservation::Present(value) => {
            value["allow_force_pushes"]["enabled"] = serde_json::json!(true)
        }
        _ => panic!("expected visible protection"),
    }
    conn.execute(
        "UPDATE dispatch_branch_security SET body=?1,digest=?2 WHERE id=?3",
        rusqlite::params![
            serde_json::to_string(&security).unwrap(),
            security.digest().unwrap(),
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
fn hidden_or_incomplete_protection_is_not_a_complete_snapshot() {
    let f = setup();
    let conn = rusqlite::Connection::open(&f.db).unwrap();
    let body: String = conn
        .query_row(
            "SELECT body FROM dispatch_branch_security WHERE id=?1",
            [BATCH],
            |r| r.get(0),
        )
        .unwrap();
    let security: sluss::github::BranchSecurityObservation = serde_json::from_str(&body).unwrap();
    let mut hidden = security.clone();
    hidden.rulesets[0].bypass_visible = false;
    hidden.rulesets[0]
        .detail
        .as_object_mut()
        .unwrap()
        .remove("bypass_actors");
    assert!(hidden.digest().is_err());
    let mut unavailable = security.clone();
    unavailable.protection = sluss::github::ProtectionObservation::Unavailable(404);
    assert!(unavailable.digest().is_err());
    let mut malformed = security.clone();
    if let sluss::github::ProtectionObservation::Present(value) = &mut malformed.protection {
        value["restrictions"] = serde_json::json!("not an object");
    }
    assert!(malformed.digest().is_err());
    let mut incomplete = security;
    if let sluss::github::ProtectionObservation::Present(value) = &mut incomplete.protection {
        value.as_object_mut().unwrap().remove("restrictions");
    }
    assert!(incomplete.digest().is_err());
}

fn trusted_policy() -> sluss::landing::TrustedCiPolicy {
    sluss::landing::TrustedCiPolicy {
        version: 1,
        installation_id: 1,
        repository_id: 2,
        repository_node_id: "R_repo".into(),
        target_ref: "refs/heads/main".into(),
        repository_policy_digest: "f".repeat(64),
        policy_digest: "c".repeat(64),
        workflow_id: 3,
        workflow_path: ".github/workflows/ci.yml".into(),
        required_jobs: BTreeSet::from(["test".into()]),
        suites: BTreeSet::from(["full".into()]),
        selection_policy_digest: None,
        runner_ids: BTreeSet::from([7]),
    }
}
#[test]
fn operator_trust_is_bound_to_the_observed_workflow_and_cannot_be_replaced() {
    let mut f = setup();
    let trusted = trusted_policy();
    let mut variants = Vec::new();
    let mut v = trusted.clone();
    v.repository_policy_digest = "a".repeat(64);
    variants.push(v);
    let mut v = trusted.clone();
    v.policy_digest = "a".repeat(64);
    variants.push(v);
    let mut v = trusted.clone();
    v.workflow_id = 4;
    variants.push(v);
    let mut v = trusted.clone();
    v.workflow_path = ".github/workflows/other.yml".into();
    variants.push(v);
    let mut v = trusted.clone();
    v.required_jobs = BTreeSet::from(["other".into()]);
    variants.push(v);
    let mut v = trusted.clone();
    v.suites = BTreeSet::from(["other".into()]);
    variants.push(v);
    let mut v = trusted.clone();
    v.selection_policy_digest = Some("a".repeat(64));
    variants.push(v);
    let mut v = trusted.clone();
    v.runner_ids = BTreeSet::from([8]);
    variants.push(v);
    let mut v = trusted.clone();
    v.repository_id = 3;
    variants.push(v);
    let mut v = trusted.clone();
    v.target_ref = "refs/heads/other".into();
    variants.push(v);
    for variant in &variants {
        assert!(matches!(
            f.journal
                .prepare_trusted_batch("land-trusted", BATCH, variant),
            Err(LandingError::TrustPolicy)
        ));
        assert!(f.journal.get("land-trusted").unwrap().is_none());
    }
    let plan = f
        .journal
        .prepare_trusted_batch("land-trusted", BATCH, &trusted)
        .unwrap();
    f.journal = LandingJournal::open(&f.db).unwrap();
    assert_eq!(
        f.journal
            .prepare_trusted_batch("land-trusted", BATCH, &trusted)
            .unwrap(),
        plan
    );
    let mut changed_allowlist = trusted.clone();
    changed_allowlist.runner_ids.insert(8);
    assert!(matches!(
        f.journal
            .prepare_trusted_batch("land-trusted", BATCH, &changed_allowlist),
        Err(LandingError::TrustPolicy)
    ));
    assert!(matches!(
        f.journal.claim_observed_batch("land-trusted"),
        Err(LandingError::TrustPolicy)
    ));
    assert!(matches!(
        f.journal
            .prepare_observed_batch("land-trusted", BATCH, "R_repo"),
        Err(LandingError::TrustPolicy)
    ));
    let conn = rusqlite::Connection::open(&f.db).unwrap();
    conn.execute(
        "UPDATE landing_trust_bindings_v1 SET digest=?1 WHERE id='land-trusted'",
        ["a".repeat(64)],
    )
    .unwrap();
    assert!(matches!(
        f.journal
            .prepare_trusted_batch("land-trusted", BATCH, &trusted),
        Err(LandingError::TrustPolicy)
    ));
    assert_eq!(
        f.journal.get("land-trusted").unwrap().unwrap().1,
        IntentState::Prepared
    );
}

#[test]
fn concurrent_operators_cannot_freeze_different_trust_policies_for_one_intent() {
    use std::sync::{Arc, Barrier};
    let fixture = setup();
    let mut second = trusted_policy();
    second.runner_ids.insert(8);
    let barrier = Arc::new(Barrier::new(2));
    let workers: Vec<_> = [trusted_policy(), second]
        .into_iter()
        .map(|policy| {
            let path = fixture.db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut journal = LandingJournal::open(path).unwrap();
                barrier.wait();
                journal.prepare_trusted_batch("race-trusted", BATCH, &policy)
            })
        })
        .collect();
    let outcomes: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Err(LandingError::TrustPolicy)))
            .count(),
        1
    );
}

#[test]
fn cli_inspects_trust_policy_without_creating_an_intent_or_database() {
    use std::process::Command;
    let fixture = setup();
    let policy = trusted_policy();
    let policy_file = fixture._dir.path().join("trust.toml");
    std::fs::write(&policy_file, toml::to_string(&policy).unwrap()).unwrap();
    let run = |db: &std::path::Path| {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(db)
            .args(["inspect-ci-policy", "--batch-id", BATCH, "--policy"])
            .arg(&policy_file)
            .output()
            .unwrap()
    };
    let output = run(&fixture.db);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains(&policy.digest().unwrap()));
    assert!(text.contains("no provider refresh"));
    let conn = rusqlite::Connection::open(&fixture.db).unwrap();
    let count: i64 = conn
        .query_row("SELECT count(*) FROM landing_intents_v1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
    let absent = fixture._dir.path().join("nonexistent.sqlite");
    assert!(!run(&absent).status.success());
    assert!(!absent.exists());
    let mut wrong = policy.clone();
    wrong.workflow_id = 4;
    std::fs::write(&policy_file, toml::to_string(&wrong).unwrap()).unwrap();
    assert!(!run(&fixture.db).status.success());
    std::fs::write(
        &policy_file,
        format!("{}\nunknown = true\n", toml::to_string(&policy).unwrap()),
    )
    .unwrap();
    assert!(!run(&fixture.db).status.success());
}

#[test]
fn published_trust_policy_example_parses_without_establishing_trust() {
    let policy = sluss::landing::TrustedCiPolicy::load(std::path::Path::new(
        "examples/trusted-ci-policy.toml",
    ))
    .unwrap();
    assert_eq!(policy.workflow_path, ".github/workflows/sluss-ci.yml");
    assert_eq!(policy.digest().unwrap().len(), 64);
}

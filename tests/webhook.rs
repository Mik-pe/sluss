use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use sluss::webhook::{Inbox, Record, router, verify_signature};
use tower::ServiceExt;

fn signature(body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}
fn request(id: &str, event: &str, body: &str, signature: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/webhook")
        .header("x-github-delivery", id)
        .header("x-github-event", event)
        .header("x-hub-signature-256", signature)
        .body(Body::from(body.to_owned()))
        .unwrap()
}

#[test]
fn github_documented_hmac_vector_and_malformed_signatures() {
    let value = "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17";
    assert!(verify_signature(
        b"It's a Secret to Everybody",
        value,
        b"Hello, World!"
    ));
    assert!(!verify_signature(b"wrong", value, b"Hello, World!"));
    assert!(!verify_signature(
        b"It's a Secret to Everybody",
        value,
        b"Hello, world!"
    ));
    assert!(!verify_signature(b"secret", "sha256=oops", b"message"));
}

#[tokio::test]
async fn rejects_forgery_and_deduplicates_signed_retries() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("test.sqlite");
    let app = router(b"secret".to_vec(), db.clone()).unwrap();
    let body = r#"{"installation":{"id":17},"repository":{"id":25}}"#;
    assert_eq!(
        app.clone()
            .oneshot(request("a", "pull_request", body, "sha256=00"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert!(Inbox::open(&db).unwrap().pending().unwrap().is_empty());
    let sig = signature(body.as_bytes());
    assert_eq!(
        app.clone()
            .oneshot(request("a", "pull_request", body, &sig))
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    assert_eq!(
        app.clone()
            .oneshot(request("a", "pull_request", body, &sig))
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    assert_eq!(Inbox::open(&db).unwrap().pending().unwrap().len(), 1);
    assert_eq!(
        app.clone()
            .oneshot(request("a", "push", body, &sig))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        app.oneshot(request("b", "push", "{}", &signature(b"{}")))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn configured_router_ignores_other_installations_and_repositories() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("test.sqlite");
    let app =
        sluss::webhook::router_with_scope(b"secret".to_vec(), db.clone(), Some((17, 25))).unwrap();
    let foreign = r#"{"installation":{"id":17},"repository":{"id":26}}"#;
    let response = app
        .clone()
        .oneshot(request(
            "foreign",
            "push",
            foreign,
            &signature(foreign.as_bytes()),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert!(Inbox::open(&db).unwrap().pending().unwrap().is_empty());
    let local = r#"{"installation":{"id":17},"repository":{"id":25}}"#;
    let response = app
        .oneshot(request(
            "local",
            "push",
            local,
            &signature(local.as_bytes()),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(Inbox::open(&db).unwrap().pending().unwrap().len(), 1);
}

#[tokio::test]
async fn concurrent_redeliveries_create_one_wakeup_and_large_bodies_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("test.sqlite");
    // Initialize the schema before concurrent requests as the server does at startup.
    Inbox::open(&db).unwrap();
    let app = router(b"secret".to_vec(), db.clone()).unwrap();
    let body = r#"{"installation":{"id":17},"repository":{"id":25}}"#;
    let sig = signature(body.as_bytes());
    let (a, b) = tokio::join!(
        app.clone()
            .oneshot(request("same", "pull_request", body, &sig)),
        app.clone()
            .oneshot(request("same", "pull_request", body, &sig)),
    );
    assert_eq!(a.unwrap().status(), StatusCode::ACCEPTED);
    assert_eq!(b.unwrap().status(), StatusCode::ACCEPTED);
    assert_eq!(Inbox::open(&db).unwrap().pending().unwrap().len(), 1);
    let oversized = "a".repeat(1024 * 1024 + 1);
    let sig = signature(oversized.as_bytes());
    assert_eq!(
        app.oneshot(request("large", "push", &oversized, &sig))
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[test]
fn revision_survives_restart_and_prevents_lost_or_aba_wakeups() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("test.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    assert_eq!(
        inbox.record("1", "push", &[1; 32], 1, 2).unwrap(),
        Record::New
    );
    let old = inbox.pending().unwrap().remove(0);
    assert_eq!(
        inbox.record("2", "push", &[2; 32], 1, 2).unwrap(),
        Record::New
    );
    assert!(!inbox.acknowledge(&old).unwrap());
    let current = inbox.pending().unwrap().remove(0);
    assert!(inbox.acknowledge(&current).unwrap());
    drop(inbox);
    let mut inbox = Inbox::open(&db).unwrap();
    assert_eq!(
        inbox.record("2", "push", &[2; 32], 1, 2).unwrap(),
        Record::Duplicate
    );
    assert!(inbox.pending().unwrap().is_empty());
    assert_eq!(
        inbox.record("3", "push", &[3; 32], 1, 2).unwrap(),
        Record::New
    );
    assert!(!inbox.acknowledge(&current).unwrap());
    assert_eq!(inbox.pending().unwrap().len(), 1);
}

#[test]
fn reconciliation_commits_queue_and_ack_together_without_losing_newer_events() {
    use sluss::{
        batch::Candidate,
        store::{Store, StoreError},
    };
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("test.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    inbox.record("d1", "push", &[1; 32], 1, 2).unwrap();
    let first = inbox.pending().unwrap().remove(0);
    let candidate = Candidate {
        number: 5,
        head_sha: "old".into(),
    };
    store
        .apply_reconciliation(&first, &"f".repeat(64), std::slice::from_ref(&candidate))
        .unwrap();
    assert_eq!(store.candidates().unwrap(), vec![candidate.clone()]);
    assert!(inbox.pending().unwrap().is_empty());
    inbox.record("d2", "push", &[2; 32], 1, 2).unwrap();
    let stale = inbox.pending().unwrap().remove(0);
    inbox.record("d3", "push", &[3; 32], 1, 2).unwrap();
    assert!(matches!(
        store.apply_reconciliation(&stale, &"f".repeat(64), &[]),
        Err(StoreError::Conflict)
    ));
    assert_eq!(store.candidates().unwrap(), vec![candidate]);
    let newer = inbox.pending().unwrap().remove(0);
    assert!(matches!(
        store.apply_reconciliation(&newer, &"e".repeat(64), &[]),
        Err(StoreError::PolicyMismatch)
    ));
    store
        .apply_reconciliation(&newer, &"f".repeat(64), &[])
        .unwrap();
    assert!(store.candidates().unwrap().is_empty());
    inbox.record("other", "push", &[4; 32], 2, 3).unwrap();
    let other = inbox.pending().unwrap().remove(0);
    assert!(matches!(
        store.apply_reconciliation(&other, &"f".repeat(64), &[]),
        Err(StoreError::RepositoryMismatch)
    ));
    assert_eq!(inbox.pending().unwrap().len(), 1);
}

#[test]
fn reconciliation_does_not_adopt_an_unbound_nonempty_database() {
    use sluss::{
        batch::Candidate,
        store::{Store, StoreError},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.sqlite");
    let mut inbox = Inbox::open(&path).unwrap();
    let mut store = Store::open(&path).unwrap();
    store
        .observe_candidate(&Candidate {
            number: 1,
            head_sha: "existing".into(),
        })
        .unwrap();
    inbox.record("new", "push", &[1; 32], 1, 2).unwrap();
    let job = inbox.pending().unwrap().remove(0);
    assert!(matches!(
        store.apply_reconciliation(&job, &"f".repeat(64), &[]),
        Err(StoreError::RepositoryMismatch)
    ));
    assert_eq!(store.candidates().unwrap().len(), 1);
    assert_eq!(inbox.pending().unwrap().len(), 1);
}

#[test]
fn periodic_scan_is_coalesced_and_revisions_are_never_reused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("inbox.sqlite");
    let mut inbox = Inbox::open(&path).unwrap();
    assert!(inbox.schedule_if_idle(1, 2).unwrap());
    let first = inbox.pending_for(1, 2).unwrap().unwrap();
    assert!(!inbox.schedule_if_idle(1, 2).unwrap());
    assert_eq!(inbox.pending_for(1, 2).unwrap().unwrap(), first);
    assert!(inbox.acknowledge(&first).unwrap());
    assert!(inbox.schedule_if_idle(1, 2).unwrap());
    let second = inbox.pending_for(1, 2).unwrap().unwrap();
    assert!(second.revision > first.revision);
    assert!(!inbox.acknowledge(&first).unwrap());
    inbox.record("webhook", "push", &[1; 32], 1, 2).unwrap();
    assert!(!inbox.acknowledge(&second).unwrap());
}

fn security_fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    sluss::store::Store,
    Inbox,
) {
    use sluss::{
        batch::Candidate,
        store::{AdmissionCandidate, Store},
    };
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("security.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    inbox.record("initial", "push", &[1; 32], 17, 25).unwrap();
    let event = inbox.pending().unwrap().remove(0);
    store
        .apply_admission_reconciliation_with_base(
            &event,
            &"f".repeat(64),
            &[AdmissionCandidate {
                candidate: Candidate {
                    number: 5,
                    head_sha: "a".repeat(40),
                },
                evidence_digest: "e".repeat(64),
            }],
            &"b".repeat(40),
            "main",
        )
        .unwrap();
    store
        .plan_batch("batch-0000000000000001", &"b".repeat(40), 1, 1)
        .unwrap()
        .unwrap();
    (dir, db, store, inbox)
}

#[tokio::test]
async fn security_webhooks_atomically_invalidate_batches_and_scope_organization_events() {
    use sluss::{batch::State, store::StoreError, webhook::router_with_scope};
    let cases = [
        ("repository", true),
        ("repository_ruleset", false),
        ("repository_ruleset", true),
        ("branch_protection_rule", true),
        ("installation", false),
        ("installation_repositories", false),
        ("member", true),
        ("membership", false),
        ("organization", false),
        ("team", false),
        ("team_add", true),
    ];
    for (index, (event, has_repo)) in cases.into_iter().enumerate() {
        let (_dir, db, mut store, inbox) = security_fixture();
        let (old, version) = store.batch("batch-0000000000000001").unwrap().unwrap();
        let mut stale_worker = old.clone();
        stale_worker.begin_build().unwrap();
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute("INSERT INTO ci_investigations(batch_id,state) VALUES('batch-0000000000000001','pending')", []).unwrap();
        conn.execute("INSERT INTO split_tasks(parent_id,side,members,state) VALUES('batch-0000000000000001',0,'[]','pending')", []).unwrap();
        conn.execute("INSERT INTO ci_candidate_completions(number,head_sha,digest,base_sha,batch_id,conclusion) VALUES(5,?1,?2,?3,'batch-0000000000000001','success')", rusqlite::params!["a".repeat(40),"e".repeat(64),"b".repeat(40)]).unwrap();
        let body = if has_repo {
            r#"{"installation":{"id":17},"repository":{"id":25}}"#
        } else {
            r#"{"installation":{"id":17}}"#
        };
        let app = router_with_scope(b"secret".to_vec(), db.clone(), Some((17, 25))).unwrap();
        let delivery = format!("security-{index}");
        let sig = signature(body.as_bytes());
        let (a, b) = tokio::join!(
            app.clone().oneshot(request(&delivery, event, body, &sig)),
            app.oneshot(request(&delivery, event, body, &sig))
        );
        assert_eq!(a.unwrap().status(), StatusCode::ACCEPTED);
        assert_eq!(b.unwrap().status(), StatusCode::ACCEPTED);
        let (batch, current_version) = store.batch("batch-0000000000000001").unwrap().unwrap();
        assert!(matches!(batch.state, State::Invalidated { .. }), "{event}");
        assert_eq!(
            current_version,
            version + 1,
            "redelivery must not invalidate twice"
        );
        assert!(matches!(
            store.update_batch(&stale_worker, version),
            Err(StoreError::Conflict)
        ));
        assert_eq!(inbox.pending_for(17, 25).unwrap().unwrap().repository, 25);
        let stale: String = conn
            .query_row("SELECT state FROM ci_investigations LIMIT 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        let split: String = conn
            .query_row("SELECT state FROM split_tasks LIMIT 1", [], |r| r.get(0))
            .unwrap();
        let holds: i64 = conn
            .query_row("SELECT count(*) FROM ci_candidate_completions", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            (stale.as_str(), split.as_str(), holds),
            ("stale", "stale", 0)
        );
        let wake = inbox.pending().unwrap().remove(0);
        assert!(inbox.acknowledge(&wake).unwrap());
        drop(store);
        drop(inbox);
        let store = sluss::store::Store::open(&db).unwrap();
        assert!(matches!(
            store
                .batch("batch-0000000000000001")
                .unwrap()
                .unwrap()
                .0
                .state,
            State::Invalidated { .. }
        ));
    }
}

#[tokio::test]
async fn unscoped_and_foreign_security_events_do_not_invalidate_the_repository() {
    use sluss::{batch::State, webhook::router_with_scope};
    let (_dir, db, store, inbox) = security_fixture();
    let app = router_with_scope(b"secret".to_vec(), db.clone(), Some((17, 25))).unwrap();
    let cases = [
        ("installation", r#"{"installation":{"id":18}}"#),
        (
            "repository_ruleset",
            r#"{"installation":{"id":17},"repository":{"id":26}}"#,
        ),
        (
            "member",
            r#"{"installation":{"id":17},"repository":{"id":26}}"#,
        ),
    ];
    for (i, (event, body)) in cases.into_iter().enumerate() {
        assert_eq!(
            app.clone()
                .oneshot(request(
                    &format!("other-{i}"),
                    event,
                    body,
                    &signature(body.as_bytes())
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::ACCEPTED
        );
    }
    assert!(inbox.pending().unwrap().is_empty());
    assert!(matches!(
        store
            .batch("batch-0000000000000001")
            .unwrap()
            .unwrap()
            .0
            .state,
        State::Planned
    ));
    let unscoped = router(b"secret".to_vec(), db.clone()).unwrap();
    let body = r#"{"installation":{"id":17}}"#;
    assert_eq!(
        unscoped
            .oneshot(request(
                "unroutable",
                "repository_ruleset",
                body,
                &signature(body.as_bytes())
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert!(inbox.pending().unwrap().is_empty());
}

#[tokio::test]
async fn failed_security_invalidation_does_not_commit_a_partial_delivery() {
    let (_dir, db, _store, inbox) = security_fixture();
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute(
        "UPDATE batches SET body='not-json' WHERE id='batch-0000000000000001'",
        [],
    )
    .unwrap();
    let app =
        sluss::webhook::router_with_scope(b"secret".to_vec(), db.clone(), Some((17, 25))).unwrap();
    let body = r#"{"installation":{"id":17},"repository":{"id":25}}"#;
    let response = app
        .oneshot(request(
            "bad-security",
            "branch_protection_rule",
            body,
            &signature(body.as_bytes()),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(inbox.pending().unwrap().is_empty());
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM webhook_deliveries WHERE delivery_id='bad-security'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
}

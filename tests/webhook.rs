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

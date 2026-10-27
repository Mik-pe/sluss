use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::Deserialize;
use serde_json::{Value, json};
use sluss::github::{App, GitHubError};
use std::{
    collections::BTreeMap,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
};

#[derive(Deserialize)]
struct Claims {
    iss: String,
    iat: u64,
    exp: u64,
}
#[derive(Clone)]
struct ServerState {
    public_key: Arc<Vec<u8>>,
    mode: Arc<AtomicU8>,
    pulls: Arc<AtomicUsize>,
    permissions: Arc<AtomicUsize>,
}
async fn permission(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let count = state.permissions.fetch_add(1, Ordering::SeqCst);
    let access = if mode == 7 || (mode == 9 && count > 0) {
        "read"
    } else {
        "write"
    };
    let id = if mode == 8 { 10 } else { 9 };
    Json(json!({"permission":access,"user":{"id":id,"login":"reviewer","type":"User"}}))
}
async fn pull(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let count = state.pulls.fetch_add(1, Ordering::SeqCst);
    let sha = if state.mode.load(Ordering::SeqCst) == 5 && count > 0 {
        "c".repeat(40)
    } else {
        "a".repeat(40)
    };
    Json(
        json!({"number":19,"state":"open","draft":false,"user":{"id":4,"login":"author","type":"User"},"head":{"sha":sha},"base":{"sha":"b".repeat(40),"ref":"main","repo":{"id":7}},"labels":[{"name":"needs-work"}]}),
    )
}
async fn reviews(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    assert_eq!(query.get("per_page").map(String::as_str), Some("100"));
    let page = query["page"].parse::<u64>().unwrap();
    let count = if state.mode.load(Ordering::SeqCst) == 6 {
        if page == 1 { 100 } else { 1 }
    } else {
        1
    };
    let items: Vec<Value> = (0..count).map(|n| json!({"id":(page-1)*100+n+1,"user":{"id":9,"login":"reviewer","type":"User"},"state":"APPROVED","commit_id":"a".repeat(40),"submitted_at":"2026-01-01T00:00:00Z"})).collect();
    Json(json!(items))
}
async fn token(
    State(state): State<ServerState>,
    Path(id): Path<u64>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(id, 42);
    assert_eq!(body["repository_ids"], json!([7]));
    assert_eq!(
        body["permissions"],
        json!({"actions":"read","checks":"read","contents":"read","pull_requests":"read"})
    );
    let jwt = headers["authorization"]
        .to_str()
        .unwrap()
        .strip_prefix("Bearer ")
        .unwrap();
    let validation = Validation::new(Algorithm::RS256);
    let claims = decode::<Claims>(
        jwt,
        &DecodingKey::from_rsa_pem(&state.public_key).unwrap(),
        &validation,
    )
    .unwrap()
    .claims;
    let now = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
    assert_eq!(claims.iss, "app-client-id");
    assert!(claims.iat <= now && now - claims.iat <= 65);
    assert!(claims.exp > now && claims.exp - now <= 540);
    let mode = state.mode.load(Ordering::SeqCst);
    if mode == 1 {
        return (
            StatusCode::TEMPORARY_REDIRECT,
            Json(json!({"error":"do not follow"})),
        );
    }
    let expiry = if mode == 2 {
        time::OffsetDateTime::now_utc() - time::Duration::hours(1)
    } else {
        time::OffsetDateTime::now_utc() + time::Duration::minutes(59)
    };
    let mut permissions = BTreeMap::from([
        ("actions", "read"),
        ("checks", "read"),
        ("contents", "read"),
        ("pull_requests", "read"),
        ("metadata", "read"),
    ]);
    if mode == 3 {
        permissions.insert("contents", "write");
    }
    let repo = if mode == 4 { 8 } else { 7 };
    (
        StatusCode::CREATED,
        Json(
            json!({"token":"fake-installation-token", "expires_at":expiry.format(&time::format_description::well_known::Rfc3339).unwrap(), "permissions":permissions, "repositories":[{"id":repo}]}),
        ),
    )
}
fn keys() -> (tempfile::TempDir, Vec<u8>, Vec<u8>) {
    let dir = tempfile::tempdir().unwrap();
    let private = dir.path().join("test-key.pem");
    let public = dir.path().join("test-public.pem");
    let output = Command::new("openssl")
        .args([
            "genpkey",
            "-algorithm",
            "RSA",
            "-pkeyopt",
            "rsa_keygen_bits:2048",
            "-out",
        ])
        .arg(&private)
        .output()
        .unwrap();
    assert!(output.status.success());
    let output = Command::new("openssl")
        .args(["pkey", "-pubout", "-in"])
        .arg(&private)
        .arg("-out")
        .arg(&public)
        .output()
        .unwrap();
    assert!(output.status.success());
    let a = std::fs::read(private).unwrap();
    let b = std::fs::read(public).unwrap();
    (dir, a, b)
}
#[tokio::test]
async fn creates_signed_scoped_tokens_and_rejects_unsafe_responses() {
    let (_dir, private, public) = keys();
    let mode = Arc::new(AtomicU8::new(0));
    let pulls = Arc::new(AtomicUsize::new(0));
    let permissions = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .route("/repos/owner/repo/pulls/19", get(pull))
        .route("/repos/owner/repo/pulls/19/reviews", get(reviews))
        .route(
            "/repos/owner/repo/collaborators/reviewer/permission",
            get(permission),
        )
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
            pulls: pulls.clone(),
            permissions: permissions.clone(),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    let token = client.read_token(42, 7).await.unwrap();
    assert_eq!(token.secret(), "fake-installation-token");
    assert!(!format!("{token:?}").contains(token.secret()));
    let snapshot = client
        .observe_pull(&token, "owner", "repo", 19)
        .await
        .unwrap();
    assert_eq!(snapshot.pull.head.sha, "a".repeat(40));
    assert_eq!(snapshot.reviews.len(), 1);
    mode.store(6, Ordering::SeqCst);
    assert_eq!(
        client
            .observe_pull(&token, "owner", "repo", 19)
            .await
            .unwrap()
            .reviews
            .len(),
        101
    );
    mode.store(5, Ordering::SeqCst);
    pulls.store(0, Ordering::SeqCst);
    assert!(matches!(
        client.observe_pull(&token, "owner", "repo", 19).await,
        Err(GitHubError::UnstableObservation)
    ));
    assert!(matches!(
        client.observe_pull(&token, "../owner", "repo", 19).await,
        Err(GitHubError::InvalidConfiguration)
    ));
    mode.store(0, Ordering::SeqCst);
    let authorized = client
        .observe_authorized_pull(&token, "owner", "repo", 19)
        .await
        .unwrap();
    assert_eq!(
        authorized.authorized_reviewers,
        std::collections::BTreeSet::from([9])
    );
    let policy = sluss::admission::Policy {
        target_branch: "main".into(),
        required_reviews: 1,
        block_labels: Default::default(),
        authorized_reviewers: authorized.authorized_reviewers,
    };
    assert!(sluss::admission::evaluate(&authorized.snapshot, &policy).is_ok());
    mode.store(7, Ordering::SeqCst);
    assert!(
        client
            .observe_authorized_pull(&token, "owner", "repo", 19)
            .await
            .unwrap()
            .authorized_reviewers
            .is_empty()
    );
    mode.store(8, Ordering::SeqCst);
    assert!(matches!(
        client
            .observe_authorized_pull(&token, "owner", "repo", 19)
            .await,
        Err(GitHubError::UnstableObservation)
    ));
    permissions.store(0, Ordering::SeqCst);
    mode.store(9, Ordering::SeqCst);
    assert!(matches!(
        client
            .observe_authorized_pull(&token, "owner", "repo", 19)
            .await,
        Err(GitHubError::UnstableObservation)
    ));
    mode.store(1, Ordering::SeqCst);
    assert!(matches!(
        client.read_token(42, 7).await,
        Err(GitHubError::Status(307))
    ));
    for value in [2, 3, 4] {
        mode.store(value, Ordering::SeqCst);
        assert!(matches!(
            client.read_token(42, 7).await,
            Err(GitHubError::InvalidResponse)
        ));
    }
    assert!(matches!(
        client.read_token(0, 7).await,
        Err(GitHubError::InvalidConfiguration)
    ));
    server.abort();
}
#[test]
fn rejects_insecure_and_malformed_configuration() {
    let (_dir, private, _public) = keys();
    for url in [
        "http://example.com/",
        "http://localhost/",
        "https://user:pass@example.com/",
        "https://example.com/path",
        "https://example.com/?q=1",
    ] {
        assert!(matches!(
            App::new("id".into(), &private, url),
            Err(GitHubError::InvalidConfiguration)
        ));
    }
    assert!(matches!(
        App::new("id".into(), b"bad key", "https://api.github.com/"),
        Err(GitHubError::InvalidConfiguration)
    ));
}

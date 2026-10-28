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
    token_calls: Arc<AtomicUsize>,
}
async fn permission(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let count = state.permissions.fetch_add(1, Ordering::SeqCst);
    let access = if mode == 7 || (mode == 9 && count > 0) {
        "read"
    } else if mode == 25 || mode == 26 {
        "admin"
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
    let label = if state.mode.load(Ordering::SeqCst) == 10 {
        "do-not-merge"
    } else {
        "needs-work"
    };
    Json(
        json!({"number":19,"state":"open","draft":false,"user":{"id":4,"login":"author","type":"User"},"head":{"sha":sha},"base":{"sha":"b".repeat(40),"ref":"main","repo":{"id":7}},"labels":[{"name":label}]}),
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
    let mut items: Vec<Value> = (0..count).map(|n| json!({"id":(page-1)*100+n+1,"user":{"id":9,"login":"reviewer","type":"User"},"state":"APPROVED","commit_id":"a".repeat(40),"submitted_at":"2026-01-01T00:00:00Z"})).collect();
    if matches!(state.mode.load(Ordering::SeqCst), 24 | 26) {
        items.push(json!({"id":2,"user":{"id":9,"login":"reviewer","type":"User"},"state":"COMMENTED","commit_id":"a".repeat(40),"submitted_at":"2026-01-02T00:00:00Z"}));
    }
    Json(json!(items))
}
async fn open_pulls(
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    assert_eq!(query.get("page").map(String::as_str), Some("1"));
    Json(json!([{"number":19}]))
}
async fn actions_run(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let count = state.pulls.fetch_add(1, Ordering::SeqCst);
    let sha = if mode == 12 && count > 0 { "b" } else { "a" };
    Json(
        json!({"id":123,"run_attempt":2,"workflow_id":17,"head_sha":sha.repeat(40),"head_branch":"sluss/test","path":".github/workflows/ci.yml@refs/heads/sluss/test","event":"push","status":"completed","conclusion":"success","repository":{"id":if mode == 11 { 8 } else { 7 }},"head_repository":{"id":7}}),
    )
}
async fn actions_jobs(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    assert_eq!(query.get("per_page").map(String::as_str), Some("100"));
    let page: u64 = query["page"].parse().unwrap();
    let mode = state.mode.load(Ordering::SeqCst);
    let count = if mode == 15 && page == 1 { 100 } else { 1 };
    let total = if mode == 15 {
        101
    } else if mode == 14 {
        2
    } else {
        1
    };
    let jobs: Vec<Value> = (0..count).map(|n| json!({"id":(page-1)*100+n+1,"run_id":if mode == 13 { 124 } else { 123 },"head_sha":"a".repeat(40),"name":format!("job-{page}-{n}"),"status":"completed","conclusion":"success","started_at":"2026-01-01T00:00:00Z","completed_at":"2026-01-01T00:01:00Z","runner_id":1})).collect();
    Json(json!({"total_count":total,"jobs":jobs}))
}
async fn dispatch_repo(headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    Json(json!({"id": 7}))
}
async fn dispatch_ref(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let sha = if state.mode.load(Ordering::SeqCst) == 16 {
        "b"
    } else {
        "a"
    };
    Json(
        json!({"ref":"refs/heads/sluss/batch-0123456789abcdef","object":{"sha":sha.repeat(40),"type":"commit"}}),
    )
}
async fn dispatched_run(
    State(state): State<ServerState>,
    headers: HeaderMap,
    path: axum::extract::OriginalUri,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let count = state.pulls.fetch_add(1, Ordering::SeqCst);
    let latest = !path.0.path().contains("/attempts/");
    let branch = if mode == 18 {
        "sluss/other"
    } else {
        "sluss/batch-0123456789abcdef"
    };
    let event = if mode == 20 {
        "push"
    } else {
        "workflow_dispatch"
    };
    let attempt = if mode == 21 && latest { 2 } else { 1 };
    let workflow_id = if mode == 23 { 18 } else { 17 };
    let sha = if mode == 22 && count >= 3 { "b" } else { "a" };
    let status = if mode == 31 {
        "in_progress"
    } else {
        "completed"
    };
    let conclusion = if mode == 31 { None } else { Some("success") };
    Json(
        json!({"id":123,"run_attempt":attempt,"workflow_id":workflow_id,"head_sha":sha.repeat(40),"head_branch":branch,"path":".github/workflows/ci.yml@refs/heads/sluss/batch-0123456789abcdef","event":event,"status":status,"conclusion":conclusion,"repository":{"id":7},"head_repository":{"id":if mode == 19 { 8 } else { 7 }}}),
    )
}
async fn cancel_run(State(state): State<ServerState>, headers: HeaderMap) -> StatusCode {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    state.permissions.fetch_add(1, Ordering::SeqCst);
    if state.mode.load(Ordering::SeqCst) == 32 {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::ACCEPTED
    }
}
async fn doctor_branch(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let count = state.pulls.fetch_add(1, Ordering::SeqCst);
    let sha = if state.mode.load(Ordering::SeqCst) == 30 && count > 0 {
        "c"
    } else {
        "b"
    };
    Json(json!({"name":"main","protected":true,"commit":{"sha":sha.repeat(40)}}))
}
async fn doctor_workflow(headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    Json(json!({"id":17,"path":".github/workflows/ci.yml","state":"active"}))
}
async fn doctor_rules(
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    assert_eq!(query["page"], "1");
    Json(json!([{"type":"pull_request","ruleset_id":10}]))
}
async fn repository_runners(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    assert_eq!(query["page"], "1");
    let mode = state.mode.load(Ordering::SeqCst);
    let count = state.pulls.fetch_add(1, Ordering::SeqCst);
    let id = if mode == 28 || (mode == 29 && count > 0) {
        2
    } else {
        1
    };
    Json(
        json!({"total_count":1,"runners":[{"id":id,"name":"runner","status":"online","busy":false,"labels":[{"name":"self-hosted"},{"name":"sluss-host-test"}]}]}),
    )
}
async fn dispatch_post(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    assert_eq!(headers["x-github-api-version"], "2026-03-10");
    assert_eq!(
        body,
        json!({"ref":"refs/heads/sluss/batch-0123456789abcdef", "inputs":{"sluss_host":"sluss-host-test"}})
    );
    state.permissions.fetch_add(1, Ordering::SeqCst);
    if state.mode.load(Ordering::SeqCst) == 17 {
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({})))
    } else {
        (StatusCode::OK, Json(json!({"workflow_run_id":123})))
    }
}
async fn token(
    State(state): State<ServerState>,
    Path(id): Path<u64>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    state.token_calls.fetch_add(1, Ordering::SeqCst);
    assert_eq!(id, 42);
    assert_eq!(body["repository_ids"], json!([7]));
    let write = body["permissions"]["actions"] == "write";
    let staging = body["permissions"]["contents"] == "write";
    let inventory = body["permissions"]["administration"] == "read";
    if inventory {
        assert_eq!(body["permissions"], json!({"administration":"read"}));
    } else if staging {
        assert_eq!(body["permissions"], json!({"contents":"write"}));
    } else if write {
        assert_eq!(
            body["permissions"],
            json!({"actions":"write","contents":"read"})
        );
    } else {
        assert_eq!(
            body["permissions"],
            json!({"actions":"read","checks":"read","contents":"read","pull_requests":"read"})
        );
    }
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
    assert!(matches!(
        claims.iss.as_str(),
        "app-client-id" | "different-app"
    ));
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
    if write {
        permissions = BTreeMap::from([
            ("actions", "write"),
            ("contents", "read"),
            ("metadata", "read"),
        ]);
    }
    if staging {
        permissions = BTreeMap::from([("contents", "write"), ("metadata", "read")]);
    }
    if inventory {
        permissions = BTreeMap::from([("administration", "read"), ("metadata", "read")]);
    }
    if mode == 3 {
        permissions.insert("contents", "write");
    }
    if mode == 27 {
        permissions.insert("actions", "write");
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
    let (key_dir, private, public) = keys();
    let mode = Arc::new(AtomicU8::new(0));
    let pulls = Arc::new(AtomicUsize::new(0));
    let permissions = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .route("/repos/owner/repo/pulls/19", get(pull))
        .route("/repos/owner/repo/pulls/19/reviews", get(reviews))
        .route("/repos/owner/repo/pulls", get(open_pulls))
        .route(
            "/repos/owner/repo/actions/runs/123/attempts/2",
            get(actions_run),
        )
        .route(
            "/repos/owner/repo/actions/runs/123/attempts/2/jobs",
            get(actions_jobs),
        )
        .route(
            "/repos/owner/repo/collaborators/reviewer/permission",
            get(permission),
        )
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
            pulls: pulls.clone(),
            permissions: permissions.clone(),
            token_calls: Arc::new(AtomicUsize::new(0)),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    let token = client.read_token(42, 7).await.unwrap();
    assert_eq!(token.secret(), "fake-installation-token");
    assert!(!format!("{token:?}").contains(token.secret()));
    let actions = client
        .observe_actions_attempt(&token, "owner", "repo", 123, 2)
        .await
        .unwrap();
    assert_eq!(actions.run.head_sha, "a".repeat(40));
    assert_eq!(actions.jobs.len(), 1);
    mode.store(15, Ordering::SeqCst);
    assert_eq!(
        client
            .observe_actions_attempt(&token, "owner", "repo", 123, 2)
            .await
            .unwrap()
            .jobs
            .len(),
        101
    );
    for failure in [11, 12, 13, 14] {
        mode.store(failure, Ordering::SeqCst);
        pulls.store(0, Ordering::SeqCst);
        assert!(
            matches!(
                client
                    .observe_actions_attempt(&token, "owner", "repo", 123, 2)
                    .await,
                Err(GitHubError::UnstableObservation)
            ),
            "mode {failure}"
        );
    }
    mode.store(0, Ordering::SeqCst);
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
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite");
    let mut inbox = sluss::webhook::Inbox::open(&path).unwrap();
    let mut store = sluss::store::Store::open(&path).unwrap();
    inbox
        .record("reconcile-1", "pull_request", &[1; 32], 42, 7)
        .unwrap();
    let job = inbox.pending().unwrap().remove(0);
    let config = sluss::reconcile::RepositoryPolicy {
        installation: 42,
        repository: 7,
        owner: "owner".into(),
        name: "repo".into(),
        target_branch: "main".into(),
        required_reviews: 1,
        block_labels: std::collections::BTreeSet::from(["do-not-merge".into()]),
    };
    sluss::reconcile::reconcile_once(&client, &token, &mut store, &job, &config)
        .await
        .unwrap();
    assert_eq!(store.candidates().unwrap()[0].head_sha, "a".repeat(40));
    assert!(inbox.pending().unwrap().is_empty());
    let frozen = store
        .plan_batch("batch-0123456789abcdef", &"b".repeat(40), 4, 1)
        .unwrap()
        .unwrap();
    inbox
        .record("reconcile-other", "pull_request", &[3; 32], 42, 7)
        .unwrap();
    let other_job = inbox.pending().unwrap().remove(0);
    let other_app = App::new("different-app".into(), &private, &format!("http://{addr}/")).unwrap();
    let other_token = other_app.read_token(42, 7).await.unwrap();
    assert!(matches!(
        sluss::reconcile::reconcile_once(&other_app, &other_token, &mut store, &other_job, &config)
            .await,
        Err(sluss::reconcile::ReconcileError::Store(
            sluss::store::StoreError::PolicyMismatch
        ))
    ));
    sluss::reconcile::reconcile_once(&client, &token, &mut store, &other_job, &config)
        .await
        .unwrap();
    assert_eq!(
        store.batch(&frozen.id).unwrap().unwrap().0.state,
        sluss::batch::State::Planned
    );
    inbox
        .record("permission-changed", "repository", &[6; 32], 42, 7)
        .unwrap();
    let permission_job = inbox.pending().unwrap().remove(0);
    mode.store(25, Ordering::SeqCst);
    sluss::reconcile::reconcile_once(&client, &token, &mut store, &permission_job, &config)
        .await
        .unwrap();
    assert!(matches!(
        store.batch(&frozen.id).unwrap().unwrap().0.state,
        sluss::batch::State::Invalidated { .. }
    ));
    let frozen = store
        .plan_batch("batch-1123456789abcdef", &"b".repeat(40), 4, 1)
        .unwrap()
        .unwrap();
    mode.store(0, Ordering::SeqCst);
    inbox
        .record("review-comment", "pull_request_review", &[7; 32], 42, 7)
        .unwrap();
    let review_job = inbox.pending().unwrap().remove(0);
    mode.store(26, Ordering::SeqCst);
    sluss::reconcile::reconcile_once(&client, &token, &mut store, &review_job, &config)
        .await
        .unwrap();
    assert_eq!(store.candidates().unwrap()[0].head_sha, "a".repeat(40));
    assert!(matches!(
        store.batch(&frozen.id).unwrap().unwrap().0.state,
        sluss::batch::State::Invalidated { .. }
    ));
    mode.store(0, Ordering::SeqCst);
    inbox
        .record("reconcile-2", "pull_request", &[2; 32], 42, 7)
        .unwrap();
    let job = inbox.pending().unwrap().remove(0);
    mode.store(8, Ordering::SeqCst);
    assert!(
        sluss::reconcile::reconcile_once(&client, &token, &mut store, &job, &config)
            .await
            .is_err()
    );
    assert_eq!(store.candidates().unwrap().len(), 1);
    assert_eq!(inbox.pending().unwrap().len(), 1);
    mode.store(10, Ordering::SeqCst);
    sluss::reconcile::reconcile_once(&client, &token, &mut store, &job, &config)
        .await
        .unwrap();
    assert!(store.candidates().unwrap().is_empty());
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
    mode.store(0, Ordering::SeqCst);
    let cli_dir = tempfile::tempdir().unwrap();
    let cli_db = cli_dir.path().join("cli.sqlite");
    let mut inbox = sluss::webhook::Inbox::open(&cli_db).unwrap();
    inbox.record("cli", "push", &[9; 32], 42, 7).unwrap();
    let path = cli_db.clone();
    let key_path = key_dir.path().join("test-key.pem");
    let output = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(path)
            .args([
                "reconcile",
                "--owner",
                "owner",
                "--repository",
                "repo",
                "--app-client-id",
                "app-client-id",
                "--installation-id",
                "42",
                "--repository-id",
                "7",
                "--api",
                &format!("http://{addr}/"),
            ])
            .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key_path)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        sluss::store::Store::open(&cli_db)
            .unwrap()
            .candidates()
            .unwrap()
            .len(),
        1
    );
    assert!(inbox.pending().unwrap().is_empty());
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

struct ChildGuard(std::process::Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn spawn_service(
    db: &std::path::Path,
    config: &std::path::Path,
    key_dir: &std::path::Path,
    port: u16,
) -> ChildGuard {
    let child = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(db)
        .args([
            "serve",
            "--listen",
            &format!("127.0.0.1:{port}"),
            "--config",
        ])
        .arg(config)
        .env("SLUSS_WEBHOOK_SECRET", "test-secret")
        .env(
            "SLUSS_GITHUB_PRIVATE_KEY_PATH",
            key_dir.join("test-key.pem"),
        )
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    ChildGuard(child)
}
#[tokio::test]
async fn configured_service_recovers_by_scanning_and_processes_webhooks() {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let (key_dir, _private, public) = keys();
    let mode = Arc::new(AtomicU8::new(1));
    let token_calls = Arc::new(AtomicUsize::new(0));
    let api = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .route("/repos/owner/repo/pulls/19", get(pull))
        .route("/repos/owner/repo/pulls/19/reviews", get(reviews))
        .route("/repos/owner/repo/pulls", get(open_pulls))
        .route(
            "/repos/owner/repo/collaborators/reviewer/permission",
            get(permission),
        )
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
            pulls: Arc::new(AtomicUsize::new(0)),
            permissions: Arc::new(AtomicUsize::new(0)),
            token_calls: token_calls.clone(),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_addr = listener.local_addr().unwrap();
    let api_task = tokio::spawn(async move { axum::serve(listener, api).await.unwrap() });
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    let config = dir.path().join("sluss.toml");
    std::fs::write(&config,format!("version = 1\n[github]\napp_client_id = \"app-client-id\"\ninstallation_id = 42\nrepository_id = 7\nowner = \"owner\"\nrepository = \"repo\"\napi = \"http://{api_addr}/\"\n[queue]\ntarget = \"main\"\npoll_seconds = 1\nrescan_seconds = 30\n[checks]\nrequired_reviews = 1\nblock_labels = [\"do-not-merge\"]\n")).unwrap();
    let mut child = spawn_service(&db, &config, key_dir.path(), port);
    let client = reqwest::Client::new();
    let mut failed_pending = false;
    for _ in 0..150 {
        if token_calls.load(Ordering::SeqCst) > 0
            && sluss::webhook::Inbox::open(&db)
                .ok()
                .and_then(|i| i.pending_for(42, 7).ok().flatten())
                .is_some()
        {
            failed_pending = true;
            break;
        }
        if child.0.try_wait().unwrap().is_some() {
            panic!("service exited before failed reconciliation");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(failed_pending, "failed work was not retained");
    drop(child);
    mode.store(0, Ordering::SeqCst);
    let mut child = spawn_service(&db, &config, key_dir.path(), port);
    let mut ready = false;
    for _ in 0..150 {
        if let Ok(store) = sluss::store::Store::open(&db)
            && store.candidates().is_ok_and(|v| v.len() == 1)
        {
            ready = true;
            break;
        }
        if child.0.try_wait().unwrap().is_some() {
            panic!("service exited before startup reconciliation");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(ready, "startup rescan did not populate the queue");
    mode.store(10, Ordering::SeqCst);
    let body = br#"{"installation":{"id":42},"repository":{"id":7}}"#;
    let mut mac = Hmac::<Sha256>::new_from_slice(b"test-secret").unwrap();
    mac.update(body);
    let signature = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
    let response = client
        .post(format!("http://127.0.0.1:{port}/webhook"))
        .header("X-Hub-Signature-256", signature)
        .header("X-GitHub-Delivery", "service-test")
        .header("X-GitHub-Event", "pull_request")
        .body(body.as_slice())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let mut cleared = false;
    for _ in 0..150 {
        if let Ok(store) = sluss::store::Store::open(&db)
            && store.candidates().is_ok_and(|v| v.is_empty())
        {
            cleared = true;
            break;
        }
        if child.0.try_wait().unwrap().is_some() {
            panic!("service exited before webhook reconciliation");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(cleared, "webhook reconciliation did not remove blocked PR");
    api_task.abort();
}

#[tokio::test]
async fn dispatch_is_scoped_claimed_once_and_uncertain_responses_are_not_retried() {
    use sluss::github::DispatchError;
    use sluss::scheduler::{Capacity, DispatchPlan, DispatchState, Host, Request, Scheduler};
    let (_key_dir, private, public) = keys();
    let mode = Arc::new(AtomicU8::new(0));
    let posts = Arc::new(AtomicUsize::new(0));
    let run_reads = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .route("/repos/owner/repo", get(dispatch_repo))
        .route(
            "/repos/owner/repo/git/ref/heads/sluss/batch-0123456789abcdef",
            get(dispatch_ref),
        )
        .route(
            "/repos/owner/repo/actions/workflows/17/dispatches",
            post(dispatch_post),
        )
        .route("/repos/owner/repo/actions/runs/123", get(dispatched_run))
        .route(
            "/repos/owner/repo/actions/runs/123/attempts/1",
            get(dispatched_run),
        )
        .route(
            "/repos/owner/repo/actions/runs/123/attempts/1/jobs",
            get(actions_jobs),
        )
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
            pulls: run_reads.clone(),
            permissions: posts.clone(),
            token_calls: Arc::new(AtomicUsize::new(0)),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    let read_token = client.read_token(42, 7).await.unwrap();
    let write_token = client.dispatch_token(42, 7).await.unwrap();
    let db = tempfile::tempdir().unwrap();
    let pools = BTreeMap::from([("rust".into(), 3)]);
    let mut scheduler = Scheduler::open(
        db.path().join("db"),
        Capacity {
            pools: pools.clone(),
            hosts: BTreeMap::from([(
                "sluss-host-test".into(),
                Host {
                    slots: 3,
                    runner_ids: Default::default(),
                    pools,
                },
            )]),
        },
    )
    .unwrap();
    let plan = DispatchPlan {
        installation_id: 42,
        repository_id: 7,
        workflow_id: 17,
        integration_sha: "a".repeat(40),
        staging_ref: "refs/heads/sluss/batch-0123456789abcdef".into(),
        policy_digest: "b".repeat(64),
        required_jobs: std::collections::BTreeSet::from(["job-1-0".into()]),
    };
    let req = Request {
        pools: BTreeMap::from([("rust".into(), 1)]),
    };
    scheduler
        .reserve_dispatch("good", &req, &plan)
        .unwrap()
        .unwrap();
    assert!(matches!(
        client
            .issue_dispatch(&mut scheduler, &read_token, "owner", "repo", "good")
            .await,
        Err(DispatchError::Invalid)
    ));
    let other = App::new("different-app".into(), &private, &format!("http://{addr}/")).unwrap();
    assert!(matches!(
        other
            .issue_dispatch(&mut scheduler, &write_token, "owner", "repo", "good")
            .await,
        Err(DispatchError::GitHub(GitHubError::InvalidConfiguration))
    ));
    assert_eq!(
        client
            .issue_dispatch(&mut scheduler, &write_token, "owner", "repo", "good")
            .await
            .unwrap(),
        123
    );
    assert_eq!(
        scheduler.pending_dispatches().unwrap()[0].state,
        DispatchState::Accepted { run_id: 123 }
    );
    assert!(matches!(
        scheduler.observe_dispatch("good", 124),
        Err(sluss::scheduler::SchedulerError::DispatchTransition)
    ));
    for bad_mode in [18, 19, 20, 21, 22, 23] {
        mode.store(bad_mode, Ordering::SeqCst);
        run_reads.store(0, Ordering::SeqCst);
        assert!(
            client
                .reconcile_dispatch(&mut scheduler, &read_token, "owner", "repo", "good")
                .await
                .is_err(),
            "mode {bad_mode}"
        );
        assert_eq!(
            scheduler.pending_dispatches().unwrap()[0].state,
            DispatchState::Accepted { run_id: 123 }
        );
    }
    mode.store(0, Ordering::SeqCst);
    let snapshot = client
        .reconcile_dispatch(&mut scheduler, &read_token, "owner", "repo", "good")
        .await
        .unwrap();
    assert_eq!(snapshot.run.head_sha, plan.integration_sha);
    assert_eq!(
        scheduler.pending_dispatches().unwrap()[0].state,
        DispatchState::Observed { run_id: 123 }
    );
    assert!(matches!(
        client
            .issue_dispatch(&mut scheduler, &write_token, "owner", "repo", "good")
            .await,
        Err(DispatchError::AlreadyClaimed)
    ));
    scheduler
        .reserve_dispatch("lost", &req, &plan)
        .unwrap()
        .unwrap();
    mode.store(17, Ordering::SeqCst);
    assert!(matches!(
        client
            .issue_dispatch(&mut scheduler, &write_token, "owner", "repo", "lost")
            .await,
        Err(DispatchError::Uncertain)
    ));
    assert!(matches!(
        client
            .issue_dispatch(&mut scheduler, &write_token, "owner", "repo", "lost")
            .await,
        Err(DispatchError::AlreadyClaimed)
    ));
    assert_eq!(posts.load(Ordering::SeqCst), 2);
    scheduler
        .reserve_dispatch("stale", &req, &plan)
        .unwrap()
        .unwrap();
    mode.store(16, Ordering::SeqCst);
    assert!(matches!(
        client
            .issue_dispatch(&mut scheduler, &write_token, "owner", "repo", "stale")
            .await,
        Err(DispatchError::StaleRef)
    ));
    let pending = scheduler.pending_dispatches().unwrap();
    assert_eq!(
        pending
            .iter()
            .find(|p| p.reservation.id == "lost")
            .unwrap()
            .state,
        DispatchState::Sending
    );
    assert_eq!(
        pending
            .iter()
            .find(|p| p.reservation.id == "stale")
            .unwrap()
            .state,
        DispatchState::Prepared
    );
    assert_eq!(posts.load(Ordering::SeqCst), 2);
    server.abort();
}

#[tokio::test]
async fn staging_tokens_are_scoped_and_never_sent_from_a_non_github_origin() {
    use sluss::github::StagingError;
    let (_key_dir, private, public) = keys();
    let mode = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
            pulls: Arc::new(AtomicUsize::new(0)),
            permissions: Arc::new(AtomicUsize::new(0)),
            token_calls: Arc::new(AtomicUsize::new(0)),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    let stage_token = client.staging_token(42, 7).await.unwrap();
    assert!(!format!("{stage_token:?}").contains(stage_token.secret()));
    mode.store(27, Ordering::SeqCst);
    assert!(matches!(
        client.staging_token(42, 7).await,
        Err(GitHubError::InvalidResponse)
    ));
    mode.store(0, Ordering::SeqCst);
    let source = tempfile::tempdir().unwrap();
    let git = |args: &[&str]| -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(source.path())
            .args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().into()
    };
    git(&["init", "-b", "main"]);
    std::fs::write(source.path().join("base"), "base").unwrap();
    git(&["add", "."]);
    git(&["commit", "-m", "base"]);
    let base = git(&["rev-parse", "HEAD"]);
    std::fs::write(source.path().join("next"), "next").unwrap();
    git(&["add", "."]);
    git(&["commit", "-m", "next"]);
    let head = git(&["rev-parse", "HEAD"]);
    let integration =
        sluss::integration::build(source.path(), &base, std::slice::from_ref(&head)).unwrap();
    assert!(matches!(
        client
            .stage_integration(
                &stage_token,
                "owner",
                "repo",
                integration,
                "batch-0123456789abcdef"
            )
            .await,
        Err(StagingError::InvalidScope)
    ));
    let data = tempfile::tempdir().unwrap();
    let db = data.path().join("queue.sqlite");
    let mut inbox = sluss::webhook::Inbox::open(&db).unwrap();
    let mut store = sluss::store::Store::open(&db).unwrap();
    inbox
        .record("initial", "pull_request", &[1; 32], 42, 7)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let admitted = sluss::store::AdmissionCandidate {
        candidate: sluss::batch::Candidate {
            number: 1,
            head_sha: head,
        },
        evidence_digest: "f".repeat(64),
    };
    store
        .apply_admission_reconciliation(&event, &"e".repeat(64), &[admitted])
        .unwrap();
    let id = "batch-0123456789abcdef";
    store.plan_batch(id, &base, 1, 1).unwrap().unwrap();
    assert!(matches!(
        sluss::coordinator::prepare_github(
            &mut store,
            &client,
            &stage_token,
            sluss::coordinator::GitHubRepository {
                owner: "owner",
                repository: "repo"
            },
            id,
            source.path(),
            &["base".into()]
        )
        .await,
        Err(sluss::coordinator::PrepareError::GitHubStage(
            StagingError::InvalidScope
        ))
    ));
    assert_eq!(
        store.batch(id).unwrap().unwrap().0.state,
        sluss::batch::State::Building
    );
    server.abort();
}

#[tokio::test]
async fn cli_dispatches_and_observes_a_reserved_host_against_stub_github() {
    use sluss::{
        batch::Candidate,
        store::{AdmissionCandidate, Store},
        webhook::Inbox,
    };
    let (key_dir, private, public) = keys();
    let posts = Arc::new(AtomicUsize::new(0));
    let mode = Arc::new(AtomicU8::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .route("/repos/owner/repo", get(dispatch_repo))
        .route("/repos/owner/repo/actions/runners", get(repository_runners))
        .route("/repos/owner/repo/branches/main", get(doctor_branch))
        .route(
            "/repos/owner/repo/actions/workflows/17",
            get(doctor_workflow),
        )
        .route("/repos/owner/repo/rules/branches/main", get(doctor_rules))
        .route(
            "/repos/owner/repo/git/ref/heads/sluss/batch-0123456789abcdef",
            get(dispatch_ref),
        )
        .route(
            "/repos/owner/repo/actions/workflows/17/dispatches",
            post(dispatch_post),
        )
        .route("/repos/owner/repo/actions/runs/123", get(dispatched_run))
        .route(
            "/repos/owner/repo/actions/runs/123/cancel",
            post(cancel_run),
        )
        .route(
            "/repos/owner/repo/actions/runs/123/attempts/1",
            get(dispatched_run),
        )
        .route(
            "/repos/owner/repo/actions/runs/123/attempts/1/jobs",
            get(actions_jobs),
        )
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
            pulls: reads.clone(),
            permissions: posts.clone(),
            token_calls: Arc::new(AtomicUsize::new(0)),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let data = tempfile::tempdir().unwrap();
    let db = data.path().join("queue.sqlite");
    let config = data.path().join("sluss.toml");
    let capacity = data.path().join("capacity.toml");
    std::fs::write(&config, format!("version=1\n[github]\napp_client_id='app-client-id'\ninstallation_id=42\nrepository_id=7\nowner='owner'\nrepository='repo'\napi='http://{addr}/'\n[queue]\ntarget='main'\n[checks]\nrequired_reviews=1\n")).unwrap();
    std::fs::write(&capacity, "[pools]\nrust=1\n[hosts.sluss-host-test]\nslots=1\nrunner_ids=[1]\n[hosts.sluss-host-test.pools]\nrust=1\n").unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    inbox
        .record("initial", "pull_request", &[1; 32], 42, 7)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    store
        .apply_admission_reconciliation(
            &event,
            &"f".repeat(64),
            &[AdmissionCandidate {
                candidate: Candidate {
                    number: 1,
                    head_sha: "c".repeat(40),
                },
                evidence_digest: "d".repeat(64),
            }],
        )
        .unwrap();
    let id = "batch-0123456789abcdef";
    store
        .plan_batch(id, &"b".repeat(40), 1, 1)
        .unwrap()
        .unwrap();
    store.start_integration(id).unwrap();
    store
        .finish_integration(id, &"a".repeat(40), &"e".repeat(64))
        .unwrap();
    drop(store);
    let launch = |args: Vec<String>| {
        let db = db.clone();
        let key = key_dir.path().join("test-key.pem");
        tokio::task::spawn_blocking(move || {
            Command::new(env!("CARGO_BIN_EXE_sluss"))
                .arg("--db")
                .arg(db)
                .args(args)
                .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key)
                .output()
                .unwrap()
        })
    };
    mode.store(29, Ordering::SeqCst);
    let unstable = launch(vec![
        "dispatch-github".into(),
        "--batch-id".into(),
        id.into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
        "--pool".into(),
        "rust=1".into(),
        "--workflow-id".into(),
        "17".into(),
        "--required-job".into(),
        "job-1-0".into(),
    ])
    .await
    .unwrap();
    assert!(!unstable.status.success());
    assert!(String::from_utf8_lossy(&unstable.stderr).contains("UnstableObservation"));
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    reads.store(0, Ordering::SeqCst);
    mode.store(28, Ordering::SeqCst);
    let rejected = launch(vec![
        "dispatch-github".into(),
        "--batch-id".into(),
        id.into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
        "--pool".into(),
        "rust=1".into(),
        "--workflow-id".into(),
        "17".into(),
        "--required-job".into(),
        "job-1-0".into(),
    ])
    .await
    .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("RunnerMismatch"));
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    mode.store(0, Ordering::SeqCst);
    let output = launch(vec![
        "dispatch-github".into(),
        "--batch-id".into(),
        id.into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
        "--pool".into(),
        "rust=1".into(),
        "--workflow-id".into(),
        "17".into(),
        "--required-job".into(),
        "job-1-0".into(),
    ])
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("dispatched run 123 to reserved host sluss-host-test")
    );
    let output = launch(vec![
        "observe-dispatch".into(),
        "--batch-id".into(),
        id.into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
    ])
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("Confirmed { runner_id: 1 }"));
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("provider CI observation: Successful (not an attestation)")
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 new completed job measurements"));
    let again = launch(vec![
        "observe-dispatch".into(),
        "--batch-id".into(),
        id.into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
    ])
    .await
    .unwrap();
    assert!(
        again.status.success(),
        "{}",
        String::from_utf8_lossy(&again.stderr)
    );
    assert!(String::from_utf8_lossy(&again.stdout).contains("0 new completed job measurements"));
    let metrics = launch(vec!["metrics".into()]).await.unwrap();
    assert!(metrics.status.success());
    let metrics_text = String::from_utf8_lossy(&metrics.stdout);
    assert!(metrics_text.contains("observed completed jobs: 1"));
    assert!(metrics_text.contains("observed runner-minutes: 1.00"));
    let output = launch(vec![
        "cancel-dispatch".into(),
        "--batch-id".into(),
        id.into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
    ])
    .await
    .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("already claimed"));
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    let cancel_args = vec![
        "cancel-stale".into(),
        "--batch-id".into(),
        id.into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
    ];
    let before_stale = launch(cancel_args.clone()).await.unwrap();
    assert!(!before_stale.status.success());
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    let mut store = Store::open(&db).unwrap();
    store.remove_candidate(1).unwrap();
    mode.store(31, Ordering::SeqCst);
    let worker_app =
        Arc::new(App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap());
    let limits: sluss::scheduler::Capacity =
        toml::from_str(&std::fs::read_to_string(&capacity).unwrap()).unwrap();
    let policy = sluss::config::Config::load(&config).unwrap().policy();
    let worker = tokio::spawn(sluss::worker::supervise_stale_cancellation(
        worker_app,
        db.clone(),
        limits,
        policy,
        std::time::Duration::from_millis(10),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while posts.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    worker.abort();
    let cancelled = launch(cancel_args.clone()).await.unwrap();
    assert!(
        cancelled.status.success(),
        "{}",
        String::from_utf8_lossy(&cancelled.stderr)
    );
    assert!(String::from_utf8_lossy(&cancelled.stdout).contains("AlreadyRequested"));
    let repeated = launch(cancel_args.clone()).await.unwrap();
    assert!(repeated.status.success());
    assert!(String::from_utf8_lossy(&repeated.stdout).contains("AlreadyRequested"));
    assert_eq!(posts.load(Ordering::SeqCst), 2);
    mode.store(0, Ordering::SeqCst);
    let completed = launch(cancel_args).await.unwrap();
    assert!(completed.status.success());
    assert!(String::from_utf8_lossy(&completed.stdout).contains("Completed"));
    assert_eq!(posts.load(Ordering::SeqCst), 2);
    let stale_observation = launch(vec![
        "observe-dispatch".into(),
        "--batch-id".into(),
        id.into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
    ])
    .await
    .unwrap();
    assert!(
        stale_observation.status.success(),
        "{}",
        String::from_utf8_lossy(&stale_observation.stderr)
    );
    assert!(
        String::from_utf8_lossy(&stale_observation.stdout)
            .contains("batch is stale; no CI result is accepted")
    );
    let doctor = launch(vec![
        "doctor".into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
        "--workflow-id".into(),
        "17".into(),
    ])
    .await
    .unwrap();
    assert!(!doctor.status.success());
    let info = String::from_utf8_lossy(&doctor.stdout);
    assert!(info.contains("target: main"));
    assert!(info.contains("active branch rules returned: 1"));
    assert!(info.contains("repository runner inventory matches configured hosts: 1 runners"));
    assert!(info.contains("repository-scoped read, staging and Actions credentials were issued"));
    assert!(String::from_utf8_lossy(&doctor.stderr).contains("NOT READY"));
    assert!(!info.contains("fake-installation-token"));
    reads.store(0, Ordering::SeqCst);
    mode.store(30, Ordering::SeqCst);
    let unstable_doctor = launch(vec![
        "doctor".into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
        "--workflow-id".into(),
        "17".into(),
    ])
    .await
    .unwrap();
    assert!(!unstable_doctor.status.success());
    assert!(String::from_utf8_lossy(&unstable_doctor.stderr).contains("UnstableObservation"));
    assert_eq!(posts.load(Ordering::SeqCst), 2);
    server.abort();
}

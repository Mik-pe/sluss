use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::post,
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
        atomic::{AtomicU8, Ordering},
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
    let app = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    let token = client.read_token(42, 7).await.unwrap();
    assert_eq!(token.secret(), "fake-installation-token");
    assert!(!format!("{token:?}").contains(token.secret()));
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

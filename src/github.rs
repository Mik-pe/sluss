//! GitHub App authentication. No token or private key is persisted or logged.
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GitHubError {
    #[error("invalid GitHub App configuration")]
    InvalidConfiguration,
    #[error("the system clock is invalid")]
    Clock,
    #[error("could not construct the app JWT")]
    Sign,
    #[error("GitHub authentication request failed")]
    Request,
    #[error("GitHub authentication returned HTTP {0}")]
    Status(u16),
    #[error("GitHub rate limit is active; request deferred")]
    RateLimited,
    #[error("the shared GitHub rate-limit state is unavailable")]
    RateLimitStore,
    #[error("GitHub returned an invalid or unexpectedly scoped installation token")]
    InvalidResponse,
    #[error("GitHub data is invalid, incomplete, or changed during observation")]
    UnstableObservation,
    #[error("the workflow file is absent or not a regular file in the inspected commit")]
    MissingDefaultWorkflow,
    #[error(
        "the workflow file is invalid, unsupported or does not declare compatible workflow_dispatch inputs"
    )]
    MissingWorkflowDispatch,
}

pub struct App {
    client_id: String,
    key: EncodingKey,
    client: reqwest::Client,
    api: reqwest::Url,
    rate_limit: Mutex<RateLimitGate>,
    shared_rate_limit: OnceLock<std::path::PathBuf>,
}

#[derive(Default)]
enum RateLimitGate {
    #[default]
    Open,
    Until(Instant),
    Indefinite,
}

fn rate_limit_delay(
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
    message_rate_limited: bool,
    now: u64,
) -> Option<Duration> {
    let exhausted = headers
        .get("x-ratelimit-remaining")
        .and_then(|v| v.to_str().ok())
        == Some("0");
    let retry = headers.get(reqwest::header::RETRY_AFTER);
    let limited = status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || exhausted
        || (status == reqwest::StatusCode::FORBIDDEN && (retry.is_some() || message_rate_limited));
    if !limited {
        return None;
    }
    let mut seconds = 0u64;
    if let Some(value) = retry {
        let parsed = value.to_str().ok().and_then(|value| {
            if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
                return Some(value.parse::<u64>().unwrap_or(u64::MAX));
            }
            value.parse::<u64>().ok().or_else(|| {
                time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc2822)
                    .ok()
                    .and_then(|date| u64::try_from(date.unix_timestamp()).ok())
                    .map(|timestamp| timestamp.saturating_sub(now))
            })
        });
        seconds = seconds.max(parsed.unwrap_or(60).max(1));
    }
    if exhausted {
        let reset = headers
            .get("x-ratelimit-reset")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        seconds = seconds.max(
            reset
                .filter(|value| *value > now)
                .map(|value| value - now)
                .unwrap_or(60),
        );
    }
    if seconds == 0 {
        seconds = 60;
    }
    Some(Duration::from_secs(seconds))
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AppIdentity {
    pub id: u64,
    pub client_id: String,
    pub slug: String,
    pub node_id: String,
}
/// The token is deliberately not printable or serializable.
pub struct InstallationToken {
    secret: String,
    pub expires_at: String,
    repository_id: u64,
    installation_id: u64,
    actions_write: bool,
    contents_write: bool,
    runner_inventory_read: bool,
    checks_read: bool,
    checks_write: bool,
    app_client_id: String,
    api_origin: String,
}
impl InstallationToken {
    pub fn secret(&self) -> &str {
        &self.secret
    }
}
impl fmt::Debug for InstallationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstallationToken")
            .field("secret", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}
#[derive(Clone, Copy)]
enum TokenPurpose {
    Read,
    Dispatch,
    Staging,
    RunnerInventory,
    FailedCheck,
}
#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    iat: u64,
    exp: u64,
}
#[derive(Serialize)]
struct RequestBody {
    repository_ids: [u64; 1],
    permissions: BTreeMap<&'static str, &'static str>,
}
#[derive(Deserialize)]
struct Repository {
    id: u64,
}
#[derive(Deserialize)]
struct TokenResponse {
    token: String,
    expires_at: String,
    permissions: BTreeMap<String, String>,
    repositories: Vec<Repository>,
}

impl App {
    /// Identify the configured App using its signed JWT. No installation
    /// token can substitute for this identity, and the response is not proof
    /// that its installation may bypass any repository rules.
    pub async fn observe_app_identity(&self) -> Result<AppIdentity, GitHubError> {
        async fn once(app: &App) -> Result<AppIdentity, GitHubError> {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| GitHubError::Clock)?
                .as_secs();
            let jwt = app.jwt(now)?;
            let url = app
                .api
                .join("app")
                .map_err(|_| GitHubError::InvalidConfiguration)?;
            let mut response = app
                .send(
                    app.client
                        .get(url)
                        .header("Accept", "application/vnd.github+json")
                        .header("X-GitHub-Api-Version", "2022-11-28")
                        .bearer_auth(jwt),
                )
                .await?;
            if !response.status().is_success() {
                return Err(GitHubError::Status(response.status().as_u16()));
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| GitHubError::Request)? {
                if bytes.len().saturating_add(chunk.len()) > 65536 {
                    return Err(GitHubError::InvalidResponse);
                }
                bytes.extend_from_slice(&chunk);
            }
            let identity: AppIdentity =
                serde_json::from_slice(&bytes).map_err(|_| GitHubError::InvalidResponse)?;
            if identity.id == 0
                || identity.client_id != app.client_id
                || !valid_name(&identity.slug)
                || identity.node_id.is_empty()
                || identity.node_id.len() > 200
                || !identity.node_id.bytes().all(|c| {
                    c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'+' | b'/' | b'=')
                })
            {
                return Err(GitHubError::InvalidResponse);
            }
            Ok(identity)
        }
        let first = once(self).await?;
        let last = once(self).await?;
        if first != last {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(last)
    }
    /// Read-only schema discovery. A positive result does not prove that the
    /// installation can update protected refs or that provider semantics are
    /// safe for landing; it only checks the presence of the documented fields.
    pub async fn inspect_ref_update_schema(
        &self,
        token: &InstallationToken,
    ) -> Result<bool, GitHubError> {
        if token.app_client_id != self.client_id || token.api_origin != self.api.as_str() {
            return Err(GitHubError::InvalidConfiguration);
        }
        const QUERY: &str = r#"query { __schema { mutationType { fields { name } } } refUpdate: __type(name: "RefUpdate") { inputFields { name } } updateInput: __type(name: "UpdateRefsInput") { inputFields { name } } }"#;
        let url = self
            .api
            .join("graphql")
            .map_err(|_| GitHubError::InvalidConfiguration)?;
        let mut response = self
            .send(
                self.client
                    .post(url)
                    .header("Accept", "application/vnd.github+json")
                    .header("X-GitHub-Api-Version", "2022-11-28")
                    .bearer_auth(token.secret())
                    .json(&serde_json::json!({"query": QUERY})),
            )
            .await?;
        if !response.status().is_success() {
            return Err(GitHubError::Status(response.status().as_u16()));
        }
        let headers = response.headers().clone();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| GitHubError::Request)? {
            if bytes.len().saturating_add(chunk.len()) > 1024 * 1024 {
                return Err(GitHubError::UnstableObservation);
            }
            bytes.extend_from_slice(&chunk);
        }
        let result: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| GitHubError::UnstableObservation)?;
        self.check_graphql_errors(result.get("errors"), &headers)
            .await?;
        let names = |path: &str| -> Option<std::collections::BTreeSet<&str>> {
            let entries = result.pointer(path)?.as_array()?;
            let mut names = std::collections::BTreeSet::new();
            for entry in entries {
                let name = entry.get("name")?.as_str()?;
                if name.is_empty() || !names.insert(name) {
                    return None;
                }
            }
            Some(names)
        };
        let mutations =
            names("/data/__schema/mutationType/fields").ok_or(GitHubError::UnstableObservation)?;
        let refs = names("/data/refUpdate/inputFields").ok_or(GitHubError::UnstableObservation)?;
        let input =
            names("/data/updateInput/inputFields").ok_or(GitHubError::UnstableObservation)?;
        Ok(mutations.contains("updateRefs")
            && ["name", "beforeOid", "afterOid", "force"]
                .iter()
                .all(|name| refs.contains(name))
            && ["repositoryId", "refUpdates", "clientMutationId"]
                .iter()
                .all(|name| input.contains(name)))
    }

    pub fn supports_github_git(&self) -> bool {
        self.api.as_str() == "https://api.github.com/"
    }
    pub(crate) fn scope(&self) -> (&str, &str) {
        (&self.client_id, self.api.as_str())
    }
    /// The API endpoint is an operator-controlled HTTPS origin. HTTP is allowed
    /// only for loopback tests. Redirects are disabled to avoid leaking JWTs.
    pub fn new(client_id: String, private_pem: &[u8], api: &str) -> Result<Self, GitHubError> {
        if client_id.is_empty() || client_id.len() > 128 {
            return Err(GitHubError::InvalidConfiguration);
        }
        let api = reqwest::Url::parse(api).map_err(|_| GitHubError::InvalidConfiguration)?;
        let loopback = api
            .host_str()
            .is_some_and(|h| h == "127.0.0.1" || h == "[::1]");
        if !(api.scheme() == "https" || api.scheme() == "http" && loopback)
            || api.host_str().is_none()
            || !api.username().is_empty()
            || api.password().is_some()
            || api.query().is_some()
            || api.fragment().is_some()
            || api.path() != "/"
        {
            return Err(GitHubError::InvalidConfiguration);
        }
        let key = EncodingKey::from_rsa_pem(private_pem)
            .map_err(|_| GitHubError::InvalidConfiguration)?;
        let client = reqwest::Client::builder()
            .user_agent("sluss/0.1")
            .redirect(reqwest::redirect::Policy::none())
            // Mutating requests are journaled as a single attempt. Disable
            // even reqwest's default retry of protocol-level NACKs.
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| GitHubError::InvalidConfiguration)?;
        Ok(Self {
            client_id,
            key,
            client,
            api,
            rate_limit: Mutex::new(RateLimitGate::Open),
            shared_rate_limit: OnceLock::new(),
        })
    }

    /// Coordinate API backoff with service processes using the same database.
    /// Call once during startup before any worker or provider request.
    pub fn enable_shared_rate_limits(&self, database: &std::path::Path) -> Result<(), GitHubError> {
        let path = database
            .canonicalize()
            .map_err(|_| GitHubError::RateLimitStore)?;
        if let Some(existing) = self.shared_rate_limit.get() {
            return if *existing == path {
                Ok(())
            } else {
                Err(GitHubError::InvalidConfiguration)
            };
        }
        crate::rate_limit::initialize(&path).map_err(|_| GitHubError::RateLimitStore)?;
        self.shared_rate_limit
            .set(path)
            .map_err(|_| GitHubError::InvalidConfiguration)
    }

    /// Defer new API requests across workers after an observed rate limit.
    /// In-flight requests and independent processes are not covered.
    async fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, GitHubError> {
        self.check_rate_limit().await?;
        let mut response = request.send().await.map_err(|_| GitHubError::Request)?;
        let status = response.status();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|value| value.as_secs())
            .unwrap_or(0);
        let mut delay = rate_limit_delay(status, response.headers(), false, now);
        if status == reqwest::StatusCode::FORBIDDEN && delay.is_none() {
            let mut bytes = Vec::new();
            while let Ok(Some(chunk)) = response.chunk().await {
                if bytes.len().saturating_add(chunk.len()) > 65536 {
                    break;
                }
                bytes.extend_from_slice(&chunk);
            }
            let limited = serde_json::from_slice::<serde_json::Value>(&bytes)
                .ok()
                .and_then(|body| {
                    body.get("message")
                        .and_then(|value| value.as_str())
                        .map(str::to_ascii_lowercase)
                })
                .is_some_and(|message| message.contains("rate limit"));
            if limited {
                delay = rate_limit_delay(status, response.headers(), true, now);
            }
            if let Some(delay) = delay {
                self.defer_requests(delay).await?;
            }
            return Err(GitHubError::Status(status.as_u16()));
        }
        if let Some(delay) = delay {
            self.defer_requests(delay).await?;
        }
        Ok(response)
    }

    async fn check_rate_limit(&self) -> Result<(), GitHubError> {
        {
            let mut gate = self
                .rate_limit
                .lock()
                .map_err(|_| GitHubError::RateLimited)?;
            match &*gate {
                RateLimitGate::Until(deadline) if Instant::now() >= *deadline => {
                    *gate = RateLimitGate::Open
                }
                RateLimitGate::Open => {}
                _ => return Err(GitHubError::RateLimited),
            }
        }
        if let Some(path) = self.shared_rate_limit.get() {
            let path = path.clone();
            let blocked = tokio::task::spawn_blocking(move || crate::rate_limit::blocked(&path))
                .await
                .map_err(|_| GitHubError::RateLimitStore)?
                .map_err(|_| GitHubError::RateLimitStore)?;
            if blocked {
                return Err(GitHubError::RateLimited);
            }
        }
        Ok(())
    }

    async fn spawn_github_git<F, T>(
        &self,
        operation: F,
    ) -> Result<Result<T, tokio::task::JoinError>, GitHubError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.check_rate_limit().await?;
        Ok(tokio::task::spawn_blocking(operation).await)
    }

    async fn defer_requests(&self, delay: Duration) -> Result<(), GitHubError> {
        {
            let mut gate = self
                .rate_limit
                .lock()
                .map_err(|_| GitHubError::RateLimited)?;
            match Instant::now().checked_add(delay) {
                None => *gate = RateLimitGate::Indefinite,
                Some(deadline) => match &*gate {
                    RateLimitGate::Indefinite => {}
                    RateLimitGate::Until(old) if *old >= deadline => {}
                    _ => *gate = RateLimitGate::Until(deadline),
                },
            }
        }
        if let Some(path) = self.shared_rate_limit.get() {
            let path = path.clone();
            tokio::task::spawn_blocking(move || crate::rate_limit::defer(&path, delay))
                .await
                .map_err(|_| GitHubError::RateLimitStore)?
                .map_err(|_| GitHubError::RateLimitStore)?;
        }
        Ok(())
    }

    async fn check_graphql_errors(
        &self,
        errors: Option<&serde_json::Value>,
        headers: &reqwest::header::HeaderMap,
    ) -> Result<(), GitHubError> {
        let Some(errors) = errors else {
            return Ok(());
        };
        let Some(errors) = errors.as_array() else {
            return Err(GitHubError::UnstableObservation);
        };
        if errors.is_empty() {
            return Ok(());
        }
        let limited = errors.iter().any(|error| {
            error
                .get("message")
                .and_then(|value| value.as_str())
                .is_some_and(|message| message.to_ascii_lowercase().contains("rate limit"))
                || error.get("type").and_then(|value| value.as_str()) == Some("RATE_LIMITED")
                || error
                    .pointer("/extensions/code")
                    .and_then(|value| value.as_str())
                    == Some("RATE_LIMITED")
        });
        if limited {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|value| value.as_secs())
                .unwrap_or(0);
            let delay = rate_limit_delay(reqwest::StatusCode::FORBIDDEN, headers, true, now)
                .ok_or(GitHubError::UnstableObservation)?;
            self.defer_requests(delay).await?;
            return Err(GitHubError::RateLimited);
        }
        Err(GitHubError::UnstableObservation)
    }
    fn jwt(&self, now: u64) -> Result<String, GitHubError> {
        let claims = Claims {
            iss: &self.client_id,
            iat: now.saturating_sub(60),
            exp: now.checked_add(540).ok_or(GitHubError::Clock)?,
        };
        encode(&Header::new(Algorithm::RS256), &claims, &self.key).map_err(|_| GitHubError::Sign)
    }
    /// Mint a read-only token for exactly one repository. A webhook alone must
    /// not be treated as authorization to request or use this token.
    pub async fn read_token(
        &self,
        installation_id: u64,
        repository_id: u64,
    ) -> Result<InstallationToken, GitHubError> {
        self.mint_token(installation_id, repository_id, TokenPurpose::Read)
            .await
    }
    /// Separate, repository-scoped credential for explicit workflow dispatch.
    pub async fn dispatch_token(
        &self,
        installation_id: u64,
        repository_id: u64,
    ) -> Result<InstallationToken, GitHubError> {
        self.mint_token(installation_id, repository_id, TokenPurpose::Dispatch)
            .await
    }
    /// Explicit write credential scoped to staging one repository. Repository
    /// rules must independently prevent writes to protected target branches.
    pub async fn staging_token(
        &self,
        installation_id: u64,
        repository_id: u64,
    ) -> Result<InstallationToken, GitHubError> {
        self.mint_token(installation_id, repository_id, TokenPurpose::Staging)
            .await
    }
    /// Read-only repository administration token for runner inventory.
    pub async fn runner_inventory_token(
        &self,
        installation_id: u64,
        repository_id: u64,
    ) -> Result<InstallationToken, GitHubError> {
        self.mint_token(
            installation_id,
            repository_id,
            TokenPurpose::RunnerInventory,
        )
        .await
    }
    /// A separate repository-scoped Checks-write credential. Current publishers
    /// only create failure or nonpassing in-progress checks, never success.
    pub async fn checks_write_token(
        &self,
        installation_id: u64,
        repository_id: u64,
    ) -> Result<InstallationToken, GitHubError> {
        self.mint_token(installation_id, repository_id, TokenPurpose::FailedCheck)
            .await
    }
    /// Compatibility alias for the failure-only reporting path.
    pub async fn failed_check_token(
        &self,
        installation_id: u64,
        repository_id: u64,
    ) -> Result<InstallationToken, GitHubError> {
        self.checks_write_token(installation_id, repository_id)
            .await
    }
    async fn mint_token(
        &self,
        installation_id: u64,
        repository_id: u64,
        purpose: TokenPurpose,
    ) -> Result<InstallationToken, GitHubError> {
        if installation_id == 0 || repository_id == 0 {
            return Err(GitHubError::InvalidConfiguration);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| GitHubError::Clock)?
            .as_secs();
        let jwt = self.jwt(now)?;
        let permissions = match purpose {
            TokenPurpose::Dispatch => BTreeMap::from([("actions", "write"), ("contents", "read")]),
            TokenPurpose::Staging => BTreeMap::from([("contents", "write")]),
            TokenPurpose::RunnerInventory => BTreeMap::from([("administration", "read")]),
            TokenPurpose::FailedCheck => BTreeMap::from([("checks", "write")]),
            TokenPurpose::Read => BTreeMap::from([
                ("actions", "read"),
                ("checks", "read"),
                ("contents", "read"),
                ("pull_requests", "read"),
            ]),
        };
        let body = RequestBody {
            repository_ids: [repository_id],
            permissions,
        };
        let url = self
            .api
            .join(&format!(
                "app/installations/{installation_id}/access_tokens"
            ))
            .map_err(|_| GitHubError::InvalidConfiguration)?;
        let mut response = self
            .send(
                self.client
                    .post(url)
                    .header("Accept", "application/vnd.github+json")
                    .header("X-GitHub-Api-Version", "2022-11-28")
                    .bearer_auth(jwt)
                    .json(&body),
            )
            .await?;
        if !response.status().is_success() {
            return Err(GitHubError::Status(response.status().as_u16()));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| GitHubError::Request)? {
            if bytes.len().saturating_add(chunk.len()) > 65536 {
                return Err(GitHubError::InvalidResponse);
            }
            bytes.extend_from_slice(&chunk);
        }
        let response: TokenResponse =
            serde_json::from_slice(&bytes).map_err(|_| GitHubError::InvalidResponse)?;
        let expires = time::OffsetDateTime::parse(
            &response.expires_at,
            &time::format_description::well_known::Rfc3339,
        )
        .map_err(|_| GitHubError::InvalidResponse)?
        .unix_timestamp();
        if expires <= now as i64 || expires > now.saturating_add(3660) as i64 {
            return Err(GitHubError::InvalidResponse);
        }
        if response.token.is_empty()
            || response.expires_at.is_empty()
            || response.repositories.len() != 1
            || response.repositories[0].id != repository_id
            || response
                .permissions
                .iter()
                .any(|(k, v)| match body.permissions.get(k.as_str()) {
                    Some(expected) => *expected != v,
                    // GitHub grants metadata read implicitly. Any other extra
                    // permission, including read access, exceeds this token's scope.
                    None => k != "metadata" || v != "read",
                })
            || body.permissions.iter().any(|(k, expected)| {
                response.permissions.get(*k).map(String::as_str) != Some(*expected)
            })
        {
            return Err(GitHubError::InvalidResponse);
        }
        Ok(InstallationToken {
            secret: response.token,
            expires_at: response.expires_at,
            repository_id,
            installation_id,
            actions_write: matches!(purpose, TokenPurpose::Dispatch),
            contents_write: matches!(purpose, TokenPurpose::Staging),
            runner_inventory_read: matches!(purpose, TokenPurpose::RunnerInventory),
            checks_read: matches!(purpose, TokenPurpose::Read),
            checks_write: matches!(purpose, TokenPurpose::FailedCheck),
            app_client_id: self.client_id.clone(),
            api_origin: self.api.as_str().to_owned(),
        })
    }
}

/// An observation is not an admission decision: reviewer permissions, branch
/// protection and concurrent provider changes must still be validated.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PullObservation {
    pub number: u64,
    pub state: String,
    pub draft: bool,
    pub user: PullUser,
    pub head: PullHead,
    pub base: PullBase,
    pub labels: Vec<PullLabel>,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PullUser {
    pub id: u64,
    pub login: String,
    #[serde(rename = "type")]
    pub kind: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PullHead {
    pub sha: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PullBase {
    pub sha: String,
    #[serde(rename = "ref")]
    pub branch: String,
    pub repo: PullRepository,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PullRepository {
    pub id: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PullLabel {
    pub name: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ReviewObservation {
    pub id: u64,
    pub user: Option<PullUser>,
    pub state: String,
    pub commit_id: Option<String>,
    pub submitted_at: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PullSnapshot {
    pub pull: PullObservation,
    pub reviews: Vec<ReviewObservation>,
}

/// Lives only within one authenticated observation or reconciliation scan.
#[derive(Default)]
struct PullReadCache {
    entries: BTreeMap<String, (reqwest::header::HeaderValue, Vec<u8>)>,
    bytes: usize,
}
impl PullReadCache {
    fn replace(&mut self, key: String, etag: Option<reqwest::header::HeaderValue>, body: Vec<u8>) {
        const MAX_CACHE_BYTES: usize = 8 * 1024 * 1024;
        if let Some((_, old)) = self.entries.remove(&key) {
            self.bytes -= old.len();
        }
        let Some(etag) =
            etag.filter(|value| value.len() <= 256 && value.to_str().is_ok_and(valid_entity_tag))
        else {
            return;
        };
        if body.len() <= MAX_CACHE_BYTES.saturating_sub(self.bytes) {
            self.bytes += body.len();
            self.entries.insert(key, (etag, body));
        }
    }
}

fn valid_entity_tag(value: &str) -> bool {
    let quoted = value.strip_prefix("W/").unwrap_or(value);
    let Some(inner) = quoted.strip_prefix('"').and_then(|v| v.strip_suffix('"')) else {
        return false;
    };
    inner
        .bytes()
        .all(|byte| byte == b'!' || (b'#'..=b'~').contains(&byte))
}

impl App {
    /// Read a bounded, double-checked snapshot using a repository-scoped token.
    /// GitHub does not provide an atomic PR/review snapshot; matching reads do
    /// not prove that values remain current after this method returns.
    pub async fn observe_pull(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        number: u64,
    ) -> Result<PullSnapshot, GitHubError> {
        let mut cache = PullReadCache::default();
        self.observe_pull_cached(token, owner, repository, number, &mut cache)
            .await
    }

    async fn observe_pull_cached(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        number: u64,
        cache: &mut PullReadCache,
    ) -> Result<PullSnapshot, GitHubError> {
        if !valid_name(owner) || !valid_name(repository) || number == 0 {
            return Err(GitHubError::InvalidConfiguration);
        }
        let path = format!("repos/{owner}/{repository}/pulls/{number}");
        let first: PullObservation = self
            .get_json_cached(token, &path, None, Some(cache))
            .await?;
        let reviews1 = self.get_reviews(token, &path, cache).await?;
        let middle: PullObservation = self
            .get_json_cached(token, &path, None, Some(cache))
            .await?;
        let reviews2 = self.get_reviews(token, &path, cache).await?;
        let last: PullObservation = self
            .get_json_cached(token, &path, None, Some(cache))
            .await?;
        if first != middle
            || middle != last
            || reviews1 != reviews2
            || last.number != number
            || last.base.repo.id != token.repository_id
            || last.user.id == 0
            || !commit_sha(&last.head.sha)
            || !commit_sha(&last.base.sha)
            || last.base.branch.is_empty()
            || last.labels.iter().any(|l| l.name.is_empty())
            || reviews2.iter().any(|r| {
                r.id == 0 || r.state.is_empty() || r.user.as_ref().is_some_and(|u| u.id == 0)
            })
        {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(PullSnapshot {
            pull: last,
            reviews: reviews2,
        })
    }
    async fn get_reviews(
        &self,
        token: &InstallationToken,
        path: &str,
        cache: &mut PullReadCache,
    ) -> Result<Vec<ReviewObservation>, GitHubError> {
        let mut result = Vec::new();
        for page in 1..=100u32 {
            let rows: Vec<ReviewObservation> = self
                .get_json_cached(token, &format!("{path}/reviews"), Some(page), Some(cache))
                .await?;
            if rows.len() > 100 {
                return Err(GitHubError::UnstableObservation);
            }
            let done = rows.len() < 100;
            result.extend(rows);
            if done {
                let mut ids = std::collections::BTreeSet::new();
                if result.iter().any(|review| !ids.insert(review.id)) {
                    return Err(GitHubError::UnstableObservation);
                }
                return Ok(result);
            }
        }
        Err(GitHubError::UnstableObservation)
    }
    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        token: &InstallationToken,
        path: &str,
        page: Option<u32>,
    ) -> Result<T, GitHubError> {
        self.get_json_cached(token, path, page, None).await
    }

    async fn get_json_cached<T: serde::de::DeserializeOwned>(
        &self,
        token: &InstallationToken,
        path: &str,
        page: Option<u32>,
        mut cache: Option<&mut PullReadCache>,
    ) -> Result<T, GitHubError> {
        if token.app_client_id != self.client_id || token.api_origin != self.api.as_str() {
            return Err(GitHubError::InvalidConfiguration);
        }
        let mut url = self
            .api
            .join(path)
            .map_err(|_| GitHubError::InvalidConfiguration)?;
        if let Some(page) = page {
            url.query_pairs_mut()
                .append_pair("per_page", "100")
                .append_pair("page", &page.to_string());
        }
        let cache_key = url.as_str().to_owned();
        let cached_etag = cache
            .as_ref()
            .and_then(|cache| cache.entries.get(&cache_key))
            .map(|(etag, _)| etag.clone());
        let mut request = self
            .client
            .get(url)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .bearer_auth(token.secret());
        if let Some(etag) = &cached_etag {
            request = request.header(reqwest::header::IF_NONE_MATCH, etag.clone());
        }
        let mut response = self.send(request).await?;
        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            if cached_etag.is_none() {
                return Err(GitHubError::UnstableObservation);
            }
            let body = cache
                .as_ref()
                .and_then(|cache| cache.entries.get(&cache_key))
                .map(|(_, body)| body)
                .ok_or(GitHubError::UnstableObservation)?;
            return serde_json::from_slice(body).map_err(|_| GitHubError::UnstableObservation);
        }
        if !response.status().is_success() {
            return Err(GitHubError::Status(response.status().as_u16()));
        }
        let etag = response.headers().get(reqwest::header::ETAG).cloned();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| GitHubError::Request)? {
            if bytes.len().saturating_add(chunk.len()) > 1024 * 1024 {
                return Err(GitHubError::UnstableObservation);
            }
            bytes.extend_from_slice(&chunk);
        }
        let value = serde_json::from_slice(&bytes).map_err(|_| GitHubError::UnstableObservation)?;
        if let Some(cache) = cache.as_mut() {
            cache.replace(cache_key, etag, bytes);
        }
        Ok(value)
    }
}
pub(crate) fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
}
fn commit_sha(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedPullObservation {
    pub snapshot: PullSnapshot,
    /// IDs with verified write or admin base permission at observation time.
    pub authorized_reviewers: std::collections::BTreeSet<u64>,
    /// Stable per-reviewer permission values observed during this scan.
    pub reviewer_permissions: BTreeMap<u64, String>,
}
#[derive(Deserialize, PartialEq, Eq)]
struct PermissionObservation {
    permission: String,
    user: PullUser,
}

fn decision_reviewers(snapshot: &PullSnapshot) -> Result<BTreeMap<u64, String>, GitHubError> {
    let mut reviewers = BTreeMap::new();
    for review in &snapshot.reviews {
        match review.state.as_str() {
            "PENDING" | "COMMENTED" => continue,
            "APPROVED" | "CHANGES_REQUESTED" | "DISMISSED" => {}
            _ => return Err(GitHubError::UnstableObservation),
        }
        let user = review
            .user
            .as_ref()
            .ok_or(GitHubError::UnstableObservation)?;
        if user.id == 0 || !valid_name(&user.login) {
            return Err(GitHubError::UnstableObservation);
        }
        if user.kind != "User" {
            continue;
        }
        if let Some(old) = reviewers.insert(user.id, user.login.clone())
            && !old.eq_ignore_ascii_case(&user.login)
        {
            return Err(GitHubError::UnstableObservation);
        }
    }
    if reviewers.len() > 100 {
        return Err(GitHubError::UnstableObservation);
    }
    Ok(reviewers)
}

impl App {
    /// Recheck each human reviewer's effective GitHub repository permission,
    /// then reread the PR. This is still not an atomic authorization snapshot.
    pub async fn observe_authorized_pull(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        number: u64,
    ) -> Result<AuthorizedPullObservation, GitHubError> {
        let mut cache = PullReadCache::default();
        let first = self
            .observe_pull_cached(token, owner, repository, number, &mut cache)
            .await?;
        let reviewers = decision_reviewers(&first)?;
        let mut first_permissions = BTreeMap::new();
        for (id, login) in &reviewers {
            first_permissions.insert(
                *id,
                self.permission(token, owner, repository, *id, login)
                    .await?,
            );
        }
        let mut authorized = std::collections::BTreeSet::new();
        let mut reviewer_permissions = BTreeMap::new();
        for (id, login) in &reviewers {
            let current = self
                .permission(token, owner, repository, *id, login)
                .await?;
            if first_permissions.get(id) != Some(&current) {
                return Err(GitHubError::UnstableObservation);
            }
            reviewer_permissions.insert(*id, current.permission.clone());
            match current.permission.as_str() {
                "admin" | "write" => {
                    authorized.insert(*id);
                }
                "read" | "none" => {}
                _ => return Err(GitHubError::UnstableObservation),
            }
        }
        let last = self
            .observe_pull_cached(token, owner, repository, number, &mut cache)
            .await?;
        if first != last {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(AuthorizedPullObservation {
            snapshot: last,
            authorized_reviewers: authorized,
            reviewer_permissions,
        })
    }

    /// Read all PRs around a shared, double-checked reviewer permission scan.
    /// Each PR retains the same independently repeated snapshot as the
    /// single-PR path. This is a non-atomic observation, not merge authority.
    pub async fn observe_authorized_pulls(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        numbers: &[u64],
    ) -> Result<Vec<AuthorizedPullObservation>, GitHubError> {
        if numbers.len() > 10_000 {
            return Err(GitHubError::UnstableObservation);
        }
        let mut unique = std::collections::BTreeSet::new();
        let mut all_reviewers: BTreeMap<u64, String> = BTreeMap::new();
        let mut first = Vec::with_capacity(numbers.len());
        let mut total_reviews = 0usize;
        let mut cache = PullReadCache::default();
        for number in numbers {
            if !unique.insert(*number) {
                return Err(GitHubError::UnstableObservation);
            }
            let snapshot = self
                .observe_pull_cached(token, owner, repository, *number, &mut cache)
                .await?;
            total_reviews = total_reviews
                .checked_add(snapshot.reviews.len())
                .ok_or(GitHubError::UnstableObservation)?;
            if total_reviews > 50_000 {
                return Err(GitHubError::UnstableObservation);
            }
            let reviewers = decision_reviewers(&snapshot)?;
            for (id, login) in &reviewers {
                if let Some(old) = all_reviewers.insert(*id, login.clone())
                    && !old.eq_ignore_ascii_case(login)
                {
                    return Err(GitHubError::UnstableObservation);
                }
            }
            if all_reviewers.len() > 10_000 {
                return Err(GitHubError::UnstableObservation);
            }
            first.push((snapshot, reviewers));
        }
        let mut permissions = BTreeMap::new();
        for (id, login) in &all_reviewers {
            permissions.insert(
                *id,
                self.permission(token, owner, repository, *id, login)
                    .await?,
            );
        }
        for (id, login) in &all_reviewers {
            let current = self
                .permission(token, owner, repository, *id, login)
                .await?;
            if permissions.get(id) != Some(&current)
                || !matches!(
                    current.permission.as_str(),
                    "admin" | "write" | "read" | "none"
                )
            {
                return Err(GitHubError::UnstableObservation);
            }
        }
        let mut observations = Vec::with_capacity(numbers.len());
        for (number, (snapshot, reviewers)) in numbers.iter().zip(first) {
            let last = self
                .observe_pull_cached(token, owner, repository, *number, &mut cache)
                .await?;
            if last != snapshot {
                return Err(GitHubError::UnstableObservation);
            }
            let mut authorized_reviewers = std::collections::BTreeSet::new();
            let mut reviewer_permissions = BTreeMap::new();
            for id in reviewers.keys() {
                let permission = &permissions
                    .get(id)
                    .ok_or(GitHubError::UnstableObservation)?
                    .permission;
                if matches!(permission.as_str(), "admin" | "write") {
                    authorized_reviewers.insert(*id);
                }
                reviewer_permissions.insert(*id, permission.clone());
            }
            observations.push(AuthorizedPullObservation {
                snapshot: last,
                authorized_reviewers,
                reviewer_permissions,
            });
        }
        Ok(observations)
    }
    async fn permission(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        id: u64,
        login: &str,
    ) -> Result<PermissionObservation, GitHubError> {
        let path = format!("repos/{owner}/{repository}/collaborators/{login}/permission");
        let permission: PermissionObservation = self.get_json(token, &path, None).await?;
        if permission.user.id != id
            || !permission.user.login.eq_ignore_ascii_case(login)
            || permission.user.kind != "User"
        {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(permission)
    }
}

#[derive(Deserialize)]
struct PullNumber {
    number: u64,
    state: String,
    base: PullListBase,
}
#[derive(Deserialize)]
struct PullListBase {
    #[serde(rename = "ref")]
    branch: String,
    repo: PullRepository,
}
impl App {
    /// Enumerate open pull requests targeting one branch twice. A changed,
    /// inconsistent or truncated listing is not usable.
    pub async fn open_pull_numbers(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        target: &str,
    ) -> Result<Vec<u64>, GitHubError> {
        if !valid_name(owner) || !valid_name(repository) {
            return Err(GitHubError::InvalidConfiguration);
        }
        let first = self
            .fetch_open_pulls(token, owner, repository, target)
            .await?;
        let second = self
            .fetch_open_pulls(token, owner, repository, target)
            .await?;
        if first != second {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(first)
    }
    async fn fetch_open_pulls(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        target: &str,
    ) -> Result<Vec<u64>, GitHubError> {
        let target_encoded =
            encoded_branch_segment(target).ok_or(GitHubError::InvalidConfiguration)?;
        let path = format!("repos/{owner}/{repository}/pulls?state=open&base={target_encoded}");
        let mut result = Vec::new();
        let mut unique = std::collections::BTreeSet::new();
        for page in 1..=100u32 {
            let pulls: Vec<PullNumber> = self.get_json(token, &path, Some(page)).await?;
            if pulls.len() > 100 {
                return Err(GitHubError::UnstableObservation);
            }
            let done = pulls.len() < 100;
            for pull in pulls {
                if pull.number == 0
                    || pull.state != "open"
                    || pull.base.branch != target
                    || pull.base.repo.id != token.repository_id
                    || !unique.insert(pull.number)
                {
                    return Err(GitHubError::UnstableObservation);
                }
                result.push(pull.number);
            }
            if done {
                return Ok(result);
            }
        }
        Err(GitHubError::UnstableObservation)
    }
    pub fn repository_id(token: &InstallationToken) -> u64 {
        token.repository_id
    }
    pub fn installation_id(token: &InstallationToken) -> u64 {
        token.installation_id
    }
}

/// Provider data only. This is not an attestation or evidence that workflow
/// code, runner environment, or a later merge is trusted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionsRun {
    pub id: u64,
    pub run_attempt: u32,
    pub workflow_id: u64,
    pub head_sha: String,
    pub head_branch: Option<String>,
    pub path: String,
    pub event: String,
    pub status: String,
    pub conclusion: Option<String>,
    pub repository: ActionsRepository,
    pub head_repository: Option<ActionsRepository>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionsRepository {
    pub id: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionsJob {
    pub id: u64,
    pub run_id: u64,
    pub head_sha: String,
    pub name: String,
    pub status: String,
    pub conclusion: Option<String>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub runner_id: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionsSnapshot {
    pub run: ActionsRun,
    pub jobs: Vec<ActionsJob>,
}
/// Partial accounting of explicitly observed rerun attempts, not CI evidence.
#[derive(Debug, PartialEq, Eq)]
pub struct RerunMetricsReport {
    pub run_id: u64,
    pub latest_attempt: Option<u32>,
    pub completed_attempts: usize,
    pub pending_attempts: usize,
    pub new_jobs: usize,
}
#[derive(Debug, Error)]
pub enum RerunMetricsError {
    #[error("attempt must be between 2 and 100")]
    InvalidAttempt,
    #[error("rerun collection exceeds 10000 observed jobs")]
    Limit,
    #[error(transparent)]
    GitHub(#[from] GitHubError),
    #[error(transparent)]
    Scheduler(#[from] crate::scheduler::SchedulerError),
}

#[derive(Deserialize)]
struct ActionsJobsPage {
    total_count: usize,
    jobs: Vec<ActionsJob>,
}
#[derive(Deserialize)]
struct ActionsRunsPage {
    total_count: usize,
    workflow_runs: Vec<ActionsRun>,
}
impl App {
    /// Find possible identities for an uncertain send. A matching run does not
    /// prove request origin, input values, CI trust or permission to release capacity.
    pub async fn find_uncertain_dispatch_runs(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        intent: &crate::scheduler::UncertainDispatch,
    ) -> Result<Vec<ActionsRun>, DispatchError> {
        let pending = &intent.pending;
        if !valid_name(owner)
            || !valid_name(repository)
            || token.repository_id != pending.plan.repository_id
            || token.installation_id != pending.plan.installation_id
            || pending.state != crate::scheduler::DispatchState::Sending
        {
            return Err(DispatchError::Invalid);
        }
        let branch = pending
            .plan
            .staging_ref
            .strip_prefix("refs/heads/")
            .ok_or(DispatchError::Invalid)?;
        if branch != format!("sluss/{}", pending.reservation.id)
            || !commit_sha(&pending.plan.integration_sha)
            || pending.plan.workflow_id == 0
        {
            return Err(DispatchError::Invalid);
        }
        let root = format!("repos/{owner}/{repository}");
        let first_repo: Repository = self.get_json(token, &root, None).await?;
        if first_repo.id != pending.plan.repository_id {
            return Err(DispatchError::Invalid);
        }
        // Branch and SHA are already validated and contain no query delimiters.
        let path = format!(
            "{root}/actions/workflows/{}/runs?branch={branch}&event=workflow_dispatch&head_sha={}",
            pending.plan.workflow_id, pending.plan.integration_sha
        );
        let first = self.fetch_workflow_runs(token, &path).await?;
        let last = self.fetch_workflow_runs(token, &path).await?;
        let last_repo: Repository = self.get_json(token, &root, None).await?;
        if first_repo.id != last_repo.id || first.len() != last.len() {
            return Err(DispatchError::GitHub(GitHubError::UnstableObservation));
        }
        for (a, b) in first.iter().zip(&last) {
            let mut comparable = a.clone();
            comparable.status = b.status.clone();
            comparable.conclusion = b.conclusion.clone();
            if comparable != *b {
                return Err(DispatchError::GitHub(GitHubError::UnstableObservation));
            }
        }
        Ok(last
            .into_iter()
            .filter(|run| {
                run.workflow_id == pending.plan.workflow_id
                    && run.repository.id == pending.plan.repository_id
                    && run.head_repository.as_ref().map(|r| r.id)
                        == Some(pending.plan.repository_id)
                    && run.head_sha == pending.plan.integration_sha
                    && run.head_branch.as_deref() == Some(branch)
                    && run.event == "workflow_dispatch"
                    && intent.workflow_path.as_deref().is_none_or(|p| {
                        workflow_path_matches(
                            p,
                            &run.path,
                            &pending.plan.staging_ref,
                            &pending.plan.integration_sha,
                        )
                    })
            })
            .collect())
    }

    async fn fetch_workflow_runs(
        &self,
        token: &InstallationToken,
        path: &str,
    ) -> Result<Vec<ActionsRun>, GitHubError> {
        let mut runs = Vec::new();
        let mut ids = std::collections::BTreeSet::new();
        let mut total = None;
        for page in 1..=10 {
            let response: ActionsRunsPage = self.get_json(token, path, Some(page)).await?;
            if response.total_count > 1000
                || response.workflow_runs.len() > 100
                || total.is_some_and(|count| count != response.total_count)
            {
                return Err(GitHubError::UnstableObservation);
            }
            total = Some(response.total_count);
            for run in response.workflow_runs {
                let safe_status = |s: &str| {
                    !s.is_empty()
                        && s.len() <= 64
                        && s.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
                };
                if run.id == 0
                    || run.run_attempt == 0
                    || !ids.insert(run.id)
                    || run.repository.id != token.repository_id
                    || !commit_sha(&run.head_sha)
                    || !safe_status(&run.status)
                    || run.conclusion.as_deref().is_some_and(|s| !safe_status(s))
                {
                    return Err(GitHubError::UnstableObservation);
                }
                runs.push(run);
            }
            if runs.len() > response.total_count {
                return Err(GitHubError::UnstableObservation);
            }
            if runs.len() == response.total_count {
                runs.sort_by_key(|r| r.id);
                return Ok(runs);
            }
            if runs.len() != page as usize * 100 {
                return Err(GitHubError::UnstableObservation);
            }
        }
        Err(GitHubError::UnstableObservation)
    }

    /// Collect one attempt or discover all observed attempts on a direct run.
    /// Each completed attempt is committed separately and is safe to re-read.
    pub async fn collect_rerun_metrics(
        &self,
        scheduler: &mut crate::scheduler::Scheduler,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        id: &str,
        attempt: Option<u32>,
    ) -> Result<RerunMetricsReport, RerunMetricsError> {
        let (installation, repository_id, run_id) = scheduler.rerun_metric_scope(id)?;
        if token.installation_id != installation || token.repository_id != repository_id {
            return Err(GitHubError::InvalidConfiguration.into());
        }
        let mut result = RerunMetricsReport {
            run_id,
            latest_attempt: None,
            completed_attempts: 0,
            pending_attempts: 0,
            new_jobs: 0,
        };
        if let Some(attempt) = attempt {
            if !(2..=100).contains(&attempt) {
                return Err(RerunMetricsError::InvalidAttempt);
            }
            let snapshot = self
                .observe_actions_attempt(token, owner, repository, run_id, attempt)
                .await?;
            result.new_jobs = scheduler.record_rerun_metrics(id, &snapshot)?;
            result.completed_attempts = 1;
            return Ok(result);
        }
        let first = self
            .observe_latest_actions_run(token, owner, repository, run_id)
            .await?;
        scheduler.validate_rerun_metric_run(id, &first)?;
        result.latest_attempt = Some(first.run_attempt);
        let mut seen_jobs = 0usize;
        for attempt in 2..=first.run_attempt {
            let snapshot = self
                .observe_actions_attempt(token, owner, repository, run_id, attempt)
                .await?;
            scheduler.validate_rerun_metric_run(id, &snapshot.run)?;
            seen_jobs = seen_jobs
                .checked_add(snapshot.jobs.len())
                .ok_or(RerunMetricsError::Limit)?;
            if seen_jobs > 10_000 {
                return Err(RerunMetricsError::Limit);
            }
            match snapshot.run.status.as_str() {
                "completed" => {}
                "queued" | "in_progress" | "waiting" | "requested" | "pending" => {
                    result.pending_attempts += 1;
                    continue;
                }
                _ => return Err(GitHubError::UnstableObservation.into()),
            }
            result.new_jobs += scheduler.record_rerun_metrics(id, &snapshot)?;
            result.completed_attempts += 1;
        }
        let last = self
            .observe_latest_actions_run(token, owner, repository, run_id)
            .await?;
        if first != last {
            return Err(GitHubError::UnstableObservation.into());
        }
        scheduler.validate_rerun_metric_run(id, &last)?;
        Ok(result)
    }

    async fn observe_latest_actions_run(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        run_id: u64,
    ) -> Result<ActionsRun, GitHubError> {
        if !valid_name(owner) || !valid_name(repository) || run_id == 0 {
            return Err(GitHubError::InvalidConfiguration);
        }
        let path = format!("repos/{owner}/{repository}/actions/runs/{run_id}");
        let first: ActionsRun = self.get_json(token, &path, None).await?;
        let last: ActionsRun = self.get_json(token, &path, None).await?;
        if first != last
            || first.id != run_id
            || first.repository.id != token.repository_id
            || !(1..=100).contains(&first.run_attempt)
        {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(first)
    }

    /// Obtain two matching snapshots of one exact attempt, with bounded and
    /// complete job pagination. Values can still change immediately afterward.
    pub async fn observe_actions_attempt(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        run_id: u64,
        attempt: u32,
    ) -> Result<ActionsSnapshot, GitHubError> {
        if !valid_name(owner) || !valid_name(repository) || run_id == 0 || attempt == 0 {
            return Err(GitHubError::InvalidConfiguration);
        }
        let path = format!("repos/{owner}/{repository}/actions/runs/{run_id}/attempts/{attempt}");
        let first = self
            .fetch_actions_attempt(token, &path, run_id, attempt)
            .await?;
        let second = self
            .fetch_actions_attempt(token, &path, run_id, attempt)
            .await?;
        if first != second {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(second)
    }
    async fn fetch_actions_attempt(
        &self,
        token: &InstallationToken,
        path: &str,
        run_id: u64,
        attempt: u32,
    ) -> Result<ActionsSnapshot, GitHubError> {
        let run: ActionsRun = self.get_json(token, path, None).await?;
        if run.id != run_id
            || run.run_attempt != attempt
            || run.workflow_id == 0
            || run.repository.id != token.repository_id
            || !commit_sha(&run.head_sha)
            || run.path.is_empty()
            || run.event.is_empty()
            || run.status.is_empty()
        {
            return Err(GitHubError::UnstableObservation);
        }
        let mut jobs = Vec::new();
        let mut ids = std::collections::BTreeSet::new();
        let mut expected_total = None;
        for page in 1..=100u32 {
            let response: ActionsJobsPage = self
                .get_json(token, &format!("{path}/jobs"), Some(page))
                .await?;
            if response.jobs.len() > 100
                || response.total_count > 10000
                || expected_total.is_some_and(|n| n != response.total_count)
            {
                return Err(GitHubError::UnstableObservation);
            }
            expected_total = Some(response.total_count);
            for job in response.jobs {
                if job.id == 0
                    || job.run_id != run_id
                    || job.head_sha != run.head_sha
                    || job.name.is_empty()
                    || job.status.is_empty()
                    || !ids.insert(job.id)
                {
                    return Err(GitHubError::UnstableObservation);
                }
                jobs.push(job);
            }
            if jobs.len() > response.total_count {
                return Err(GitHubError::UnstableObservation);
            }
            if jobs.len() == response.total_count {
                return Ok(ActionsSnapshot { run, jobs });
            }
            if jobs.len() != page as usize * 100 {
                return Err(GitHubError::UnstableObservation);
            }
        }
        Err(GitHubError::UnstableObservation)
    }
}

#[derive(Debug, Default)]
pub struct BenchmarkCollectionNotes {
    /// Jobs lacking a runner or both timestamps cannot be measured.
    pub jobs_without_timing: usize,
    pub jobs_outside_interval: usize,
    pub boundary_crossing_jobs: usize,
    pub excluded_jobs: Vec<BenchmarkExcludedJob>,
    pub merged_heads: BTreeMap<u64, String>,
    attempts_seen: usize,
    job_ids_seen: std::collections::BTreeSet<u64>,
}
#[derive(Debug)]
pub struct BenchmarkExcludedJob {
    pub run_id: u64,
    pub attempt: u32,
    pub job_id: u64,
    pub reason: &'static str,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
}
impl BenchmarkCollectionNotes {
    fn exclude(&mut self, run_id: u64, attempt: u32, job: &ActionsJob, reason: &'static str) {
        self.excluded_jobs.push(BenchmarkExcludedJob {
            run_id,
            attempt,
            job_id: job.id,
            reason,
            started_at: job.started_at.clone(),
            completed_at: job.completed_at.clone(),
        });
    }
}
#[derive(Debug, Error)]
pub enum BenchmarkCollectionError {
    #[error(transparent)]
    GitHub(#[from] GitHubError),
    #[error(transparent)]
    Benchmark(#[from] crate::benchmark::BenchmarkError),
    #[error("selected run, attempt, PR or interval does not match the repository")]
    Mismatch,
    #[error("collection exceeds 200 attempts or 10,000 distinct jobs")]
    Limit,
}
#[derive(Deserialize, PartialEq, Eq)]
struct BenchmarkPull {
    number: u64,
    state: String,
    merged: bool,
    merged_at: Option<String>,
    base: BenchmarkPullBase,
    #[serde(default)]
    head: Option<BenchmarkPullHead>,
}
#[derive(Deserialize, PartialEq, Eq)]
struct BenchmarkPullHead {
    sha: String,
}
#[derive(Deserialize, PartialEq, Eq)]
struct BenchmarkPullBase {
    #[serde(rename = "ref")]
    branch: String,
    repo: ActionsRepository,
}

impl App {
    /// Collect explicitly selected run attempts and merged PRs through the App.
    /// Selection and population completeness are deliberately not asserted.
    pub async fn collect_benchmark(
        &self,
        token: &InstallationToken,
        policy: &crate::reconcile::RepositoryPolicy,
        request: &crate::benchmark::CollectionRequest,
    ) -> Result<(crate::benchmark::Input, BenchmarkCollectionNotes), BenchmarkCollectionError> {
        request.validate()?;
        if !token.checks_read
            || token.repository_id != request.repository_id
            || i64::try_from(token.repository_id).ok() != Some(policy.repository)
            || i64::try_from(token.installation_id).ok() != Some(policy.installation)
            || !valid_name(&policy.owner)
            || !valid_name(&policy.name)
        {
            return Err(BenchmarkCollectionError::Mismatch);
        }
        let path = format!("repos/{}/{}", policy.owner, policy.name);
        #[derive(Deserialize, PartialEq, Eq)]
        struct Identity {
            id: u64,
            node_id: String,
        }
        let before: Identity = self.get_json(token, &path, None).await?;
        if before.id != request.repository_id || before.node_id.is_empty() {
            return Err(BenchmarkCollectionError::Mismatch);
        }
        let mut notes = BenchmarkCollectionNotes::default();
        let baseline = self
            .collect_benchmark_cohort(token, policy, &request.baseline, &mut notes)
            .await?;
        let sluss = self
            .collect_benchmark_cohort(token, policy, &request.sluss, &mut notes)
            .await?;
        let after: Identity = self.get_json(token, &path, None).await?;
        if before != after {
            return Err(BenchmarkCollectionError::Mismatch);
        }
        let result = crate::benchmark::Input {
            version: 1,
            repository_id: request.repository_id,
            baseline,
            sluss,
        };
        result.report()?;
        Ok((result, notes))
    }

    async fn collect_benchmark_cohort(
        &self,
        token: &InstallationToken,
        policy: &crate::reconcile::RepositoryPolicy,
        selected: &crate::benchmark::CollectionCohort,
        notes: &mut BenchmarkCollectionNotes,
    ) -> Result<crate::benchmark::Cohort, BenchmarkCollectionError> {
        use crate::benchmark::{Cohort, Delivery, Job};
        let start = crate::metrics::timestamp_ms(&selected.start)
            .map_err(|_| BenchmarkCollectionError::Mismatch)?;
        let end = crate::metrics::timestamp_ms(&selected.end)
            .map_err(|_| BenchmarkCollectionError::Mismatch)?;
        let root = format!("repos/{}/{}", policy.owner, policy.name);
        let mut jobs = Vec::new();
        for run_id in &selected.run_ids {
            let path = format!("{root}/actions/runs/{run_id}");
            let first: ActionsRun = self.get_json(token, &path, None).await?;
            if first.id != *run_id
                || first.repository.id != token.repository_id
                || first.run_attempt == 0
                || first.run_attempt > 100
                || first.status != "completed"
                || first.conclusion.as_deref().is_none_or(str::is_empty)
                || !commit_sha(&first.head_sha)
            {
                return Err(BenchmarkCollectionError::Mismatch);
            }
            notes.attempts_seen = notes
                .attempts_seen
                .checked_add(first.run_attempt as usize)
                .ok_or(BenchmarkCollectionError::Limit)?;
            if notes.attempts_seen > 200 {
                return Err(BenchmarkCollectionError::Limit);
            }
            for attempt in 1..=first.run_attempt {
                let snapshot = self
                    .observe_actions_attempt(token, &policy.owner, &policy.name, *run_id, attempt)
                    .await?;
                let run = &snapshot.run;
                if run.status != "completed"
                    || run.conclusion.as_deref().is_none_or(str::is_empty)
                    || run.workflow_id != first.workflow_id
                    || run.head_sha != first.head_sha
                    || run.repository != first.repository
                    || run.head_repository != first.head_repository
                    || run.event != first.event
                    || run.path != first.path
                {
                    return Err(BenchmarkCollectionError::Mismatch);
                }
                for job in snapshot.jobs {
                    if notes.job_ids_seen.len() >= 10_000 {
                        return Err(BenchmarkCollectionError::Limit);
                    }
                    if !notes.job_ids_seen.insert(job.id) {
                        return Err(BenchmarkCollectionError::Mismatch);
                    }
                    if job.status != "completed"
                        || job.conclusion.as_deref().is_none_or(str::is_empty)
                    {
                        return Err(BenchmarkCollectionError::Mismatch);
                    }
                    let (Some(runner), Some(begin), Some(finish)) = (
                        job.runner_id,
                        job.started_at.clone(),
                        job.completed_at.clone(),
                    ) else {
                        notes.jobs_without_timing += 1;
                        notes.exclude(*run_id, attempt, &job, "missing-runner-or-timing");
                        continue;
                    };
                    if runner == 0 {
                        return Err(BenchmarkCollectionError::Mismatch);
                    }
                    let begin_ms = crate::metrics::timestamp_ms(&begin)
                        .map_err(|_| BenchmarkCollectionError::Mismatch)?;
                    let finish_ms = crate::metrics::timestamp_ms(&finish)
                        .map_err(|_| BenchmarkCollectionError::Mismatch)?;
                    if finish_ms < begin_ms {
                        return Err(BenchmarkCollectionError::Mismatch);
                    }
                    if (finish_ms <= start && begin_ms < start) || begin_ms >= end {
                        notes.jobs_outside_interval += 1;
                        notes.exclude(*run_id, attempt, &job, "outside-interval");
                        continue;
                    }
                    if begin_ms < start || finish_ms >= end {
                        notes.boundary_crossing_jobs += 1;
                        notes.exclude(*run_id, attempt, &job, "crosses-boundary");
                        continue;
                    }
                    jobs.push(Job {
                        job_id: job.id,
                        run_id: *run_id,
                        attempt,
                        runner_id: runner,
                        started_at: begin,
                        completed_at: finish,
                    });
                }
            }
            let last: ActionsRun = self.get_json(token, &path, None).await?;
            if first != last {
                return Err(BenchmarkCollectionError::Mismatch);
            }
        }
        let mut deliveries = Vec::new();
        for number in &selected.pull_numbers {
            let path = format!("{root}/pulls/{number}");
            let first: BenchmarkPull = self.get_json(token, &path, None).await?;
            let last: BenchmarkPull = self.get_json(token, &path, None).await?;
            if first != last
                || first.number != *number
                || first.state != "closed"
                || !first.merged
                || first.base.repo.id != token.repository_id
                || first.base.branch != policy.target_branch
            {
                return Err(BenchmarkCollectionError::Mismatch);
            }
            let merged_at = first.merged_at.ok_or(BenchmarkCollectionError::Mismatch)?;
            let merged_ms = crate::metrics::timestamp_ms(&merged_at)
                .map_err(|_| BenchmarkCollectionError::Mismatch)?;
            if merged_ms < start || merged_ms >= end {
                return Err(BenchmarkCollectionError::Mismatch);
            }
            if let Some(head) = first.head.filter(|head| commit_sha(&head.sha)) {
                notes.merged_heads.insert(*number, head.sha);
            }
            deliveries.push(Delivery {
                pr: *number,
                ready_at: None,
                observed_ready_at: None,
                merged_at,
                run_id: None,
                attempt: None,
            });
        }
        Ok(Cohort {
            start: selected.start.clone(),
            end: selected.end.clone(),
            jobs_complete: false,
            deliveries_complete: false,
            jobs,
            deliveries,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictResolution {
    Resolved,
    AlreadyResolved,
    Waiting,
}

#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("invalid dispatch request or scoped credential")]
    Invalid,
    #[error("staging ref no longer identifies the frozen integration commit")]
    StaleRef,
    #[error("the target branch no longer matches the frozen base")]
    StaleBase,
    #[error("the dispatch was already claimed or is not prepared")]
    AlreadyClaimed,
    #[error("the provider may have accepted the dispatch; reconcile without retrying")]
    Uncertain,
    #[error("the observed workflow run does not match the frozen dispatch")]
    RunMismatch,
    #[error(transparent)]
    GitHub(#[from] GitHubError),
    #[error(transparent)]
    Scheduler(#[from] crate::scheduler::SchedulerError),
}
#[derive(Deserialize, PartialEq, Eq)]
struct DispatchRef {
    #[serde(rename = "ref")]
    reference: String,
    object: DispatchObject,
}
#[derive(Deserialize, PartialEq, Eq)]
struct DispatchObject {
    sha: String,
    #[serde(rename = "type")]
    kind: String,
}
#[derive(Deserialize)]
struct DispatchReceipt {
    workflow_run_id: u64,
}
impl App {
    /// Explicit experimental workflow dispatch. Claiming is durable before
    /// the HTTP POST. No automatic retry occurs after any ambiguous result.
    /// The caller must separately prove workflow trust and host binding.
    pub async fn issue_dispatch(
        &self,
        scheduler: &mut crate::scheduler::Scheduler,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        id: &str,
    ) -> Result<u64, DispatchError> {
        self.issue_dispatch_inner(scheduler, (token, None), owner, repository, id, None)
            .await
    }

    /// Recheck the target around the staging-ref read before claiming a batch dispatch.
    /// The caller supplies the batch's frozen base and a freshly reconciled target.
    pub async fn issue_batch_dispatch(
        &self,
        scheduler: &mut crate::scheduler::Scheduler,
        tokens: (&InstallationToken, &InstallationToken),
        owner: &str,
        repository: &str,
        id: &str,
        target_base: (&str, &str),
    ) -> Result<u64, DispatchError> {
        if !commit_sha(target_base.1) {
            return Err(DispatchError::Invalid);
        }
        self.issue_dispatch_inner(
            scheduler,
            (tokens.0, Some(tokens.1)),
            owner,
            repository,
            id,
            Some(target_base),
        )
        .await
    }

    async fn issue_dispatch_inner(
        &self,
        scheduler: &mut crate::scheduler::Scheduler,
        tokens: (&InstallationToken, Option<&InstallationToken>),
        owner: &str,
        repository: &str,
        id: &str,
        target_base: Option<(&str, &str)>,
    ) -> Result<u64, DispatchError> {
        use crate::scheduler::DispatchState;
        let (token, security_token) = tokens;
        if !token.actions_write || !valid_name(owner) || !valid_name(repository) {
            return Err(DispatchError::Invalid);
        }
        let pending = scheduler
            .pending_dispatches()?
            .into_iter()
            .find(|p| p.reservation.id == id)
            .ok_or(DispatchError::Invalid)?;
        if pending.plan.repository_id != token.repository_id
            || pending.plan.installation_id != token.installation_id
        {
            return Err(DispatchError::Invalid);
        }
        if pending.state != DispatchState::Prepared {
            return Err(DispatchError::AlreadyClaimed);
        }
        scheduler.validate_dispatch_target(id, target_base)?;
        let host_label = &pending.reservation.host;
        if !valid_runner_label(host_label) {
            return Err(DispatchError::Invalid);
        }
        if let Some((target, base)) = target_base {
            let branch = self
                .observe_target_branch(token, owner, repository, target)
                .await?;
            if branch.commit.sha != base {
                return Err(DispatchError::StaleBase);
            }
        }
        let repo_path = format!("repos/{owner}/{repository}");
        let observed_repo: Repository = self.get_json(token, &repo_path, None).await?;
        if observed_repo.id != pending.plan.repository_id {
            return Err(DispatchError::Invalid);
        }
        let short_ref = pending
            .plan
            .staging_ref
            .strip_prefix("refs/")
            .ok_or(DispatchError::Invalid)?;
        let ref_path = format!("{repo_path}/git/ref/{short_ref}");
        let observed_ref: DispatchRef = self.get_json(token, &ref_path, None).await?;
        if observed_ref.reference != pending.plan.staging_ref
            || observed_ref.object.kind != "commit"
            || observed_ref.object.sha != pending.plan.integration_sha
        {
            return Err(DispatchError::StaleRef);
        }
        if let Some((target, base)) = target_base {
            let branch = self
                .observe_target_branch(token, owner, repository, target)
                .await?;
            if branch.commit.sha != base {
                return Err(DispatchError::StaleBase);
            }
        }
        let workflow = self
            .observe_registered_workflow(token, owner, repository, pending.plan.workflow_id)
            .await?;
        let default_workflow = self
            .observe_default_workflow(token, owner, repository, &workflow)
            .await?;
        let staged_blob = self
            .workflow_blob_at_commit(
                token,
                owner,
                repository,
                &pending.plan.integration_sha,
                &workflow.path,
            )
            .await?;
        self.verify_dispatch_trigger(token, owner, repository, &staged_blob)
            .await?;
        let claimed = if let Some((target, base)) = target_base {
            let rules = match self
                .observe_branch_rules(token, owner, repository, target)
                .await
            {
                Ok(rules) => Some(rules),
                Err(_) => {
                    eprintln!(
                        "branch rules unavailable; this dispatch is CI-only and cannot support an experimental landing claim"
                    );
                    None
                }
            };
            let security = if let Some(security_token) = security_token {
                if security_token.repository_id != token.repository_id
                    || security_token.installation_id != token.installation_id
                {
                    return Err(DispatchError::Invalid);
                }
                match self
                    .observe_branch_security(security_token, owner, repository, target)
                    .await
                {
                    Ok(security)
                        if security.branch.commit.sha == base
                            && rules.as_ref() == Some(&security.rules)
                            && security.digest().is_ok() =>
                    {
                        Some(security)
                    }
                    Ok(security) if security.branch.commit.sha != base => {
                        return Err(DispatchError::StaleBase);
                    }
                    _ => {
                        eprintln!(
                            "complete branch protection details unavailable; this dispatch remains CI-only"
                        );
                        None
                    }
                }
            } else {
                None
            };
            if self
                .observe_target_branch(token, owner, repository, target)
                .await?
                .commit
                .sha
                != base
            {
                return Err(DispatchError::StaleBase);
            }
            self.recheck_default_workflow(token, owner, repository, &workflow, &default_workflow)
                .await?;
            let last_ref: DispatchRef = self.get_json(token, &ref_path, None).await?;
            if last_ref.reference != pending.plan.staging_ref
                || last_ref.object.kind != "commit"
                || last_ref.object.sha != pending.plan.integration_sha
            {
                return Err(DispatchError::StaleRef);
            }
            self.check_rate_limit().await?;
            if let Some(security) = security {
                scheduler.claim_batch_dispatch_with_security(id, &workflow.path, &security)?
            } else if let Some(rules) = rules {
                scheduler.claim_batch_dispatch_with_rules(id, &workflow.path, &rules)?
            } else {
                scheduler.claim_batch_dispatch(id, &workflow.path)?
            }
        } else {
            self.recheck_default_workflow(token, owner, repository, &workflow, &default_workflow)
                .await?;
            let last_ref: DispatchRef = self.get_json(token, &ref_path, None).await?;
            if last_ref.reference != pending.plan.staging_ref
                || last_ref.object.kind != "commit"
                || last_ref.object.sha != pending.plan.integration_sha
            {
                return Err(DispatchError::StaleRef);
            }
            self.check_rate_limit().await?;
            scheduler.claim_dispatch(id)?
        };
        if !claimed {
            return Err(DispatchError::AlreadyClaimed);
        }
        let path = format!(
            "{repo_path}/actions/workflows/{}/dispatches",
            pending.plan.workflow_id
        );
        let url = self.api.join(&path).map_err(|_| DispatchError::Uncertain)?;
        let mut response = self.send(self.client.post(url)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2026-03-10")
            .bearer_auth(token.secret())
            .json(&serde_json::json!({"ref": pending.plan.staging_ref, "return_run_details": true, "inputs": {"sluss_host": host_label, "sluss_suites": pending.plan.suites.iter().cloned().collect::<Vec<_>>().join(",")}})))
            .await
            .map_err(|_| DispatchError::Uncertain)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(DispatchError::Uncertain);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| DispatchError::Uncertain)?
        {
            if bytes.len().saturating_add(chunk.len()) > 65536 {
                return Err(DispatchError::Uncertain);
            }
            bytes.extend_from_slice(&chunk);
        }
        let receipt: DispatchReceipt =
            serde_json::from_slice(&bytes).map_err(|_| DispatchError::Uncertain)?;
        if receipt.workflow_run_id == 0 {
            return Err(DispatchError::Uncertain);
        }
        scheduler.record_receipt(id, receipt.workflow_run_id)?;
        Ok(receipt.workflow_run_id)
    }
}

impl App {
    /// Validate a saved receipt against the provider's latest run and complete
    /// first-attempt job list before binding it to the durable intent. This
    /// is observation only: it does not prove the CI policy or free capacity.
    pub async fn reconcile_dispatch(
        &self,
        scheduler: &mut crate::scheduler::Scheduler,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        id: &str,
    ) -> Result<ActionsSnapshot, DispatchError> {
        use crate::scheduler::DispatchState;
        if !valid_name(owner) || !valid_name(repository) {
            return Err(DispatchError::Invalid);
        }
        let pending = scheduler
            .pending_dispatches()?
            .into_iter()
            .find(|p| p.reservation.id == id)
            .ok_or(DispatchError::Invalid)?;
        if pending.plan.repository_id != token.repository_id
            || pending.plan.installation_id != token.installation_id
        {
            return Err(DispatchError::Invalid);
        }
        let run_id = match pending.state {
            DispatchState::Accepted { run_id } | DispatchState::Observed { run_id } => run_id,
            _ => return Err(DispatchError::Invalid),
        };
        let snapshot = self
            .verify_dispatch_run(token, owner, repository, &pending, run_id)
            .await?;
        if scheduler
            .dispatch_workflow(id)?
            .as_deref()
            .is_some_and(|path| {
                !workflow_path_matches(
                    path,
                    &snapshot.run.path,
                    &pending.plan.staging_ref,
                    &pending.plan.integration_sha,
                )
            })
        {
            return Err(DispatchError::RunMismatch);
        }
        scheduler.observe_dispatch(id, run_id)?;
        Ok(snapshot)
    }

    /// Reassociate an operator-supplied run ID with an uncertain send after
    /// checking its provider identity. This is recovery, not proof that this
    /// request created the run and not an attestation.
    pub async fn recover_uncertain_dispatch(
        &self,
        scheduler: &mut crate::scheduler::Scheduler,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        id: &str,
        run_id: u64,
    ) -> Result<ActionsSnapshot, DispatchError> {
        if !valid_name(owner) || !valid_name(repository) || run_id == 0 {
            return Err(DispatchError::Invalid);
        }
        let pending = scheduler
            .pending_dispatches()?
            .into_iter()
            .find(|p| p.reservation.id == id)
            .ok_or(DispatchError::Invalid)?;
        if pending.plan.repository_id != token.repository_id
            || pending.plan.installation_id != token.installation_id
            || pending.state != crate::scheduler::DispatchState::Sending
        {
            return Err(DispatchError::Invalid);
        }
        let snapshot = self
            .verify_dispatch_run(token, owner, repository, &pending, run_id)
            .await?;
        if scheduler
            .dispatch_workflow(id)?
            .as_deref()
            .is_some_and(|path| {
                !workflow_path_matches(
                    path,
                    &snapshot.run.path,
                    &pending.plan.staging_ref,
                    &pending.plan.integration_sha,
                )
            })
        {
            return Err(DispatchError::RunMismatch);
        }
        scheduler.observe_dispatch(id, run_id)?;
        Ok(snapshot)
    }

    /// Release only the physical capacity bound to a conflicted receipt after
    /// two stable provider observations and an idle runner inventory. The
    /// conflicting identities remain invalidated and cannot authorize a merge.
    pub async fn resolve_dispatch_conflict(
        &self,
        scheduler: &mut crate::scheduler::Scheduler,
        read: &InstallationToken,
        inventory: &InstallationToken,
        repository: (&str, &str),
        id: &str,
        capacity: &crate::scheduler::Capacity,
    ) -> Result<ConflictResolution, DispatchError> {
        let (owner, repository) = repository;
        use crate::{
            scheduler::{ConflictReleaseEvidence, DispatchState},
            verification::Placement,
        };
        if !valid_name(owner)
            || !valid_name(repository)
            || !read.checks_read
            || !inventory.runner_inventory_read
            || read.repository_id != inventory.repository_id
            || read.installation_id != inventory.installation_id
            || read.app_client_id != self.client_id
            || inventory.app_client_id != self.client_id
            || read.api_origin != self.api.as_str()
            || inventory.api_origin != self.api.as_str()
        {
            return Err(DispatchError::Invalid);
        }
        crate::verification::check_release_topology(capacity)
            .map_err(|_| DispatchError::Invalid)?;
        if scheduler.conflict_release_recorded(id)? {
            return Ok(ConflictResolution::AlreadyResolved);
        }
        let Some(pending) = scheduler
            .pending_dispatches()?
            .into_iter()
            .find(|p| p.reservation.id == id)
        else {
            return if scheduler.conflict_release_recorded(id)? {
                Ok(ConflictResolution::AlreadyResolved)
            } else {
                Err(DispatchError::Invalid)
            };
        };
        let (recovered, direct) = scheduler
            .receipt_conflict(id)?
            .ok_or(DispatchError::Invalid)?;
        if pending.plan.repository_id != read.repository_id
            || pending.plan.installation_id != read.installation_id
            || pending.state != (DispatchState::Observed { run_id: recovered })
        {
            return Err(DispatchError::Invalid);
        }
        for run_id in [recovered, direct] {
            if let Err(error) = scheduler.conflicting_stale_run(id, run_id) {
                if scheduler.conflict_release_recorded(id)? {
                    return Ok(ConflictResolution::AlreadyResolved);
                }
                return Err(error.into());
            }
        }
        let workflow = scheduler
            .dispatch_workflow(id)?
            .ok_or(DispatchError::Invalid)?;
        let host = capacity
            .hosts
            .get(&pending.reservation.host)
            .ok_or(DispatchError::Invalid)?;
        let first_recovered = self
            .verify_dispatch_run(read, owner, repository, &pending, recovered)
            .await?;
        let first_direct = self
            .verify_dispatch_run(read, owner, repository, &pending, direct)
            .await?;
        for snapshot in [&first_recovered, &first_direct] {
            if !workflow_path_matches(
                &workflow,
                &snapshot.run.path,
                &pending.plan.staging_ref,
                &pending.plan.integration_sha,
            ) {
                return Err(DispatchError::RunMismatch);
            }
            if snapshot.run.status != "completed"
                || snapshot.run.conclusion.as_deref().is_none_or(str::is_empty)
                || snapshot.jobs.iter().any(|job| {
                    job.status != "completed" || job.conclusion.as_deref().is_none_or(str::is_empty)
                })
            {
                return Ok(ConflictResolution::Waiting);
            }
            let placement = crate::verification::check_host_placement(snapshot, &host.runner_ids)
                .map_err(|_| DispatchError::RunMismatch)?;
            if matches!(placement, Placement::Waiting)
                && !snapshot
                    .jobs
                    .iter()
                    .all(|job| job.conclusion.as_deref() == Some("skipped"))
            {
                return Err(DispatchError::RunMismatch);
            }
        }
        let runners = self
            .observe_repository_runners(inventory, owner, repository)
            .await?;
        crate::verification::check_repository_runner_inventory(capacity, &runners)
            .map_err(|_| DispatchError::Invalid)?;
        let last_recovered = self
            .verify_dispatch_run(read, owner, repository, &pending, recovered)
            .await?;
        let last_direct = self
            .verify_dispatch_run(read, owner, repository, &pending, direct)
            .await?;
        if first_recovered != last_recovered || first_direct != last_direct {
            return Err(DispatchError::RunMismatch);
        }
        let evidence = ConflictReleaseEvidence {
            id: id.into(),
            plan: pending.plan.clone(),
            host: pending.reservation.host.clone(),
            capacity: capacity.clone(),
            workflow,
            recovered: last_recovered,
            direct: last_direct,
            runners,
        };
        if scheduler.release_conflicting_ci_only(&pending, &evidence)? {
            Ok(ConflictResolution::Resolved)
        } else {
            Ok(ConflictResolution::AlreadyResolved)
        }
    }

    async fn verify_dispatch_run(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        pending: &crate::scheduler::PendingDispatch,
        run_id: u64,
    ) -> Result<ActionsSnapshot, DispatchError> {
        let path = format!("repos/{owner}/{repository}/actions/runs/{run_id}");
        let first: ActionsRun = self.get_json(token, &path, None).await?;
        let snapshot = self
            .observe_actions_attempt(token, owner, repository, run_id, 1)
            .await?;
        let last: ActionsRun = self.get_json(token, &path, None).await?;
        let branch = pending
            .plan
            .staging_ref
            .strip_prefix("refs/heads/")
            .ok_or(DispatchError::Invalid)?;
        if first != last
            || last != snapshot.run
            || last.id != run_id
            || last.run_attempt != 1
            || last.workflow_id != pending.plan.workflow_id
            || last.repository.id != pending.plan.repository_id
            || last.head_repository.as_ref().map(|r| r.id) != Some(pending.plan.repository_id)
            || last.head_sha != pending.plan.integration_sha
            || last.head_branch.as_deref() != Some(branch)
            || last.event != "workflow_dispatch"
        {
            return Err(DispatchError::RunMismatch);
        }
        Ok(snapshot)
    }
}

#[derive(Debug, Error)]
pub enum LandingRefreshError {
    #[error("repository or App token scope does not match the frozen intent")]
    Scope,
    #[error("Actions run changed while it was being observed")]
    Changed,
    #[error("visible branch settings contain contradictions or remain incomplete")]
    UnsafeBranchPolicy,
    #[error(transparent)]
    GitHub(#[from] GitHubError),
    #[error(transparent)]
    Journal(#[from] crate::landing::LandingError),
}

impl App {
    /// Read an uncertain landing's refs twice and append historical observations.
    /// This never sends a mutation, acknowledges a request or unlocks its target.
    pub async fn observe_landing_recovery(
        &self,
        journal: &mut crate::landing::LandingJournal,
        token: &InstallationToken,
        repository: (&str, &str),
        id: &str,
    ) -> Result<crate::landing::LandingRecoveryObservation, LandingRefreshError> {
        #[derive(Deserialize, PartialEq, Eq)]
        struct Identity {
            id: u64,
            node_id: String,
        }
        let (owner, name) = repository;
        if !valid_name(owner)
            || !valid_name(name)
            || !token.checks_read
            || token.app_client_id != self.client_id
            || token.api_origin != self.api.as_str()
        {
            return Err(LandingRefreshError::Scope);
        }
        let (plan, state) = journal.get(id)?.ok_or(LandingRefreshError::Scope)?;
        if state != crate::landing::IntentState::Sending
            || i64::try_from(token.repository_id).ok() != Some(plan.repository_id)
            || i64::try_from(token.installation_id).ok() != Some(plan.installation_id)
        {
            return Err(LandingRefreshError::Scope);
        }
        let root = format!("repos/{owner}/{name}");
        let target = format!(
            "{root}/git/ref/{}",
            plan.target_ref
                .strip_prefix("refs/")
                .ok_or(LandingRefreshError::Scope)?
        );
        let staging = format!(
            "{root}/git/ref/{}",
            plan.staging_ref
                .strip_prefix("refs/")
                .ok_or(LandingRefreshError::Scope)?
        );
        let marker_path = plan.marker_ref.as_ref().map(|name| {
            format!(
                "{root}/git/ref/{}",
                name.strip_prefix("refs/").expect("validated marker")
            )
        });
        let first_repo: Identity = self.get_json(token, &root, None).await?;
        let first_target: DispatchRef = self.get_json(token, &target, None).await?;
        let first_staging: DispatchRef = self.get_json(token, &staging, None).await?;
        let first_marker: Option<DispatchRef> = match &marker_path {
            Some(path) => Some(self.get_json(token, path, None).await?),
            None => None,
        };
        let last_marker: Option<DispatchRef> = match &marker_path {
            Some(path) => Some(self.get_json(token, path, None).await?),
            None => None,
        };
        let last_staging: DispatchRef = self.get_json(token, &staging, None).await?;
        let last_target: DispatchRef = self.get_json(token, &target, None).await?;
        let last_repo: Identity = self.get_json(token, &root, None).await?;
        if first_repo != last_repo
            || first_target != last_target
            || first_staging != last_staging
            || first_marker != last_marker
        {
            return Err(LandingRefreshError::Changed);
        }
        if first_repo.id != token.repository_id
            || first_repo.node_id != plan.repository_node_id
            || first_target.reference != plan.target_ref
            || first_staging.reference != plan.staging_ref
            || first_target.object.kind != "commit"
            || first_staging.object.kind != "commit"
            || first_marker.as_ref().is_some_and(|marker| {
                Some(marker.reference.as_str()) != plan.marker_ref.as_deref()
                    || marker.object.kind != "commit"
            })
        {
            return Err(LandingRefreshError::Scope);
        }
        let observed_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| GitHubError::Clock)?
            .as_millis()
            .try_into()
            .map_err(|_| GitHubError::Clock)?;
        let observation = crate::landing::LandingRecoveryObservation {
            plan,
            target_sha: first_target.object.sha,
            staging_sha: first_staging.object.sha,
            marker_sha: first_marker.map(|marker| marker.object.sha),
            observed_ms,
        };
        journal.record_recovery_observation(&observation)?;
        Ok(observation)
    }
}

#[derive(Debug, Error)]
pub enum MarkerCreationError {
    #[error("marker credentials or repository scope do not match the frozen intent")]
    Scope,
    #[error("marker creation may have succeeded; do not retry")]
    Uncertain,
    #[error(transparent)]
    GitHub(#[from] GitHubError),
    #[error(transparent)]
    Journal(#[from] crate::landing::LandingError),
}

impl App {
    /// Observe an attempted marker creation twice. A 404 means only that the
    /// ref is not visible to this token; no response proves request origin.
    pub async fn observe_marker_creation(
        &self,
        journal: &mut crate::landing::LandingJournal,
        token: &InstallationToken,
        repository: (&str, &str),
        operation_id: &str,
    ) -> Result<crate::landing::MarkerCreationObservation, MarkerCreationError> {
        #[derive(Deserialize, PartialEq, Eq)]
        struct Identity {
            id: u64,
            node_id: String,
        }
        let (owner, name) = repository;
        if !valid_name(owner)
            || !valid_name(name)
            || !token.checks_read
            || token.app_client_id != self.client_id
            || token.api_origin != self.api.as_str()
        {
            return Err(MarkerCreationError::Scope);
        }
        let (plan, intent_state) = journal
            .get(operation_id)?
            .ok_or(MarkerCreationError::Scope)?;
        let creation_state = journal.marker_creation_state(operation_id)?;
        if creation_state == crate::landing::MarkerCreationState::Prepared
            || i64::try_from(token.repository_id).ok() != Some(plan.repository_id)
            || i64::try_from(token.installation_id).ok() != Some(plan.installation_id)
        {
            return Err(MarkerCreationError::Scope);
        }
        let marker = plan
            .marker_ref
            .as_deref()
            .ok_or(MarkerCreationError::Scope)?;
        let root = format!("repos/{owner}/{name}");
        let path = format!(
            "{root}/git/ref/{}",
            marker
                .strip_prefix("refs/")
                .ok_or(MarkerCreationError::Scope)?
        );
        let first_repo: Identity = self.get_json(token, &root, None).await?;
        async fn marker_read(
            app: &App,
            token: &InstallationToken,
            path: &str,
        ) -> Result<Option<DispatchRef>, GitHubError> {
            match app.get_json(token, path, None).await {
                Ok(reference) => Ok(Some(reference)),
                Err(GitHubError::Status(404)) => Ok(None),
                Err(error) => Err(error),
            }
        }
        let first = marker_read(self, token, &path).await?;
        let last = marker_read(self, token, &path).await?;
        let last_repo: Identity = self.get_json(token, &root, None).await?;
        if first_repo != last_repo
            || first_repo.id != token.repository_id
            || first_repo.node_id != plan.repository_node_id
            || first != last
        {
            return Err(MarkerCreationError::Scope);
        }
        if first
            .as_ref()
            .is_some_and(|value| value.reference != marker || value.object.kind != "commit")
        {
            return Err(MarkerCreationError::Scope);
        }
        let observed_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| GitHubError::Clock)?
            .as_millis()
            .try_into()
            .map_err(|_| GitHubError::Clock)?;
        let observed = crate::landing::MarkerCreationObservation {
            plan,
            intent_state,
            creation_state,
            marker_sha: first.map(|value| value.object.sha),
            observed_ms,
        };
        journal.record_marker_observation(&observed)?;
        Ok(observed)
    }

    /// Create only the frozen marker ref, once. A crash or an ambiguous
    /// response remains uncertain; matching readback does not prove origin.
    /// This does not publish CI success or send a landing mutation.
    pub async fn create_landing_marker(
        &self,
        journal: &mut crate::landing::LandingJournal,
        read: &InstallationToken,
        write: &InstallationToken,
        repository: &crate::reconcile::RepositoryPolicy,
        trusted: &crate::landing::TrustedCiPolicy,
        operation_id: &str,
    ) -> Result<crate::landing::MarkerCreationState, MarkerCreationError> {
        #[derive(Deserialize, PartialEq, Eq)]
        struct Identity {
            id: u64,
            node_id: String,
        }
        if !valid_name(&repository.owner)
            || !valid_name(&repository.name)
            || !read.checks_read
            || !write.contents_write
            || read.repository_id != write.repository_id
            || read.installation_id != write.installation_id
            || read.app_client_id != self.client_id
            || write.app_client_id != self.client_id
            || read.api_origin != self.api.as_str()
            || write.api_origin != self.api.as_str()
        {
            return Err(MarkerCreationError::Scope);
        }
        let target = journal.refresh_target(operation_id, trusted)?;
        let plan = &target.plan;
        if i64::try_from(read.repository_id).ok() != Some(plan.repository_id)
            || i64::try_from(read.installation_id).ok() != Some(plan.installation_id)
            || repository.repository != plan.repository_id
            || repository.installation != plan.installation_id
            || plan.target_ref != format!("refs/heads/{}", repository.target_branch)
            || crate::reconcile::policy_digest(self, repository)
                .ok()
                .as_deref()
                != Some(target.repository_policy_digest.as_str())
            || plan.marker_ref.is_none()
        {
            return Err(MarkerCreationError::Scope);
        }
        if plan.guarded_heads.is_none() {
            return Err(crate::landing::LandingError::UnprotectedHeads.into());
        }
        let root = format!("repos/{}/{}", repository.owner, repository.name);
        let first: Identity = self.get_json(read, &root, None).await?;
        let last: Identity = self.get_json(read, &root, None).await?;
        if first != last
            || first.id != read.repository_id
            || first.node_id != plan.repository_node_id
        {
            return Err(MarkerCreationError::Scope);
        }
        self.check_rate_limit().await?;
        match journal.claim_marker_creation(&target, trusted)? {
            crate::landing::MarkerCreationState::Creating => {
                return Err(MarkerCreationError::Uncertain);
            }
            crate::landing::MarkerCreationState::Created => {
                return Ok(crate::landing::MarkerCreationState::Created);
            }
            crate::landing::MarkerCreationState::Prepared => {}
        }
        let url = self
            .api
            .join(&format!("{root}/git/refs"))
            .map_err(|_| MarkerCreationError::Scope)?;
        let body = serde_json::json!({"ref":plan.marker_ref,"sha":plan.base_sha});
        let mut response = self
            .send(
                self.client
                    .post(url)
                    .header("Accept", "application/vnd.github+json")
                    .header("X-GitHub-Api-Version", "2022-11-28")
                    .bearer_auth(write.secret())
                    .json(&body),
            )
            .await
            .map_err(|_| MarkerCreationError::Uncertain)?;
        if response.status() != reqwest::StatusCode::CREATED {
            return Err(MarkerCreationError::Uncertain);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| MarkerCreationError::Uncertain)?
        {
            if bytes.len().saturating_add(chunk.len()) > 65536 {
                return Err(MarkerCreationError::Uncertain);
            }
            bytes.extend_from_slice(&chunk);
        }
        let observed: DispatchRef =
            serde_json::from_slice(&bytes).map_err(|_| MarkerCreationError::Uncertain)?;
        if Some(observed.reference.as_str()) != plan.marker_ref.as_deref()
            || observed.object.kind != "commit"
            || observed.object.sha != plan.base_sha
        {
            return Err(MarkerCreationError::Uncertain);
        }
        journal
            .record_marker_created(plan)
            .map_err(|_| MarkerCreationError::Uncertain)?;
        Ok(crate::landing::MarkerCreationState::Created)
    }
}

/// GitHub's current review status, not a complete mergeability or CODEOWNERS proof.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderReviewDecision {
    pub number: u64,
    pub state: String,
    pub is_draft: bool,
    pub base_ref_name: String,
    pub base_ref_oid: String,
    pub head_ref_oid: String,
    pub review_decision: Option<String>,
}
impl App {
    /// Read the provider's review decision twice. A result is a transient
    /// observation only; GitHub documents no atomic review-and-ref update.
    pub async fn observe_review_decision(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        repository_node_id: &str,
        number: u64,
    ) -> Result<ProviderReviewDecision, GitHubError> {
        #[derive(Deserialize)]
        struct RepositoryResponse {
            id: String,
            #[serde(rename = "pullRequest")]
            pull: Option<ProviderReviewDecision>,
        }
        #[derive(Deserialize)]
        struct Data {
            repository: Option<RepositoryResponse>,
        }
        #[derive(Deserialize)]
        struct Response {
            data: Option<Data>,
            #[serde(default)]
            errors: Option<serde_json::Value>,
        }
        if !token.checks_read
            || token.app_client_id != self.client_id
            || token.api_origin != self.api.as_str()
            || !valid_name(owner)
            || !valid_name(repository)
            || repository_node_id.is_empty()
            || number == 0
            || number > i32::MAX as u64
        {
            return Err(GitHubError::InvalidConfiguration);
        }
        const QUERY: &str = "query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){id pullRequest(number:$number){number state isDraft baseRefName baseRefOid headRefOid reviewDecision}}}";
        async fn once(
            app: &App,
            token: &InstallationToken,
            owner: &str,
            repository: &str,
            number: u64,
            node_id: &str,
        ) -> Result<ProviderReviewDecision, GitHubError> {
            let url = app
                .api
                .join("graphql")
                .map_err(|_| GitHubError::InvalidConfiguration)?;
            let mut response = app.send(app.client.post(url).header("Accept", "application/vnd.github+json")
                .header("X-GitHub-Api-Version", "2022-11-28").bearer_auth(token.secret())
                .json(&serde_json::json!({"query":QUERY,"variables":{"owner":owner,"name":repository,"number":number}})))
                .await?;
            if !response.status().is_success() {
                return Err(GitHubError::Status(response.status().as_u16()));
            }
            let headers = response.headers().clone();
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| GitHubError::Request)? {
                if bytes.len().saturating_add(chunk.len()) > 65536 {
                    return Err(GitHubError::UnstableObservation);
                }
                bytes.extend_from_slice(&chunk);
            }
            let result: Response =
                serde_json::from_slice(&bytes).map_err(|_| GitHubError::UnstableObservation)?;
            app.check_graphql_errors(result.errors.as_ref(), &headers)
                .await?;
            let repo = result
                .data
                .and_then(|data| data.repository)
                .ok_or(GitHubError::UnstableObservation)?;
            let pull = repo.pull.ok_or(GitHubError::UnstableObservation)?;
            if repo.id != node_id
                || pull.number != number
                || !commit_sha(&pull.base_ref_oid)
                || !commit_sha(&pull.head_ref_oid)
                || pull.base_ref_name.is_empty()
            {
                return Err(GitHubError::UnstableObservation);
            }
            Ok(pull)
        }
        let first = once(self, token, owner, repository, number, repository_node_id).await?;
        let last = once(self, token, owner, repository, number, repository_node_id).await?;
        if first != last {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(last)
    }
    /// Freeze same-repository head branches observed twice around each PR ref.
    /// Forks are rejected; review/policy races remain unsolved.
    pub async fn freeze_landing_heads(
        &self,
        journal: &mut crate::landing::LandingJournal,
        token: &InstallationToken,
        policy: &crate::reconcile::RepositoryPolicy,
        trusted: &crate::landing::TrustedCiPolicy,
        operation_id: &str,
    ) -> Result<crate::landing::LandingPlan, LandingRefreshError> {
        let target = journal.refresh_target(operation_id, trusted)?;
        let plan = &target.plan;
        if !token.checks_read
            || token.app_client_id != self.client_id
            || token.api_origin != self.api.as_str()
            || i64::try_from(token.repository_id).ok() != Some(plan.repository_id)
            || i64::try_from(token.installation_id).ok() != Some(plan.installation_id)
            || policy.repository != plan.repository_id
            || policy.installation != plan.installation_id
            || plan.target_ref != format!("refs/heads/{}", policy.target_branch)
            || crate::reconcile::policy_digest(self, policy)
                .ok()
                .as_deref()
                != Some(target.repository_policy_digest.as_str())
        {
            return Err(LandingRefreshError::Scope);
        }
        let heads = Box::pin(self.observe_same_repo_heads(token, policy, &target)).await?;
        journal
            .bind_guarded_heads(&target, trusted, heads)
            .map_err(Into::into)
    }

    async fn observe_same_repo_heads(
        &self,
        token: &InstallationToken,
        policy: &crate::reconcile::RepositoryPolicy,
        target: &crate::landing::RefreshTarget,
    ) -> Result<Vec<crate::landing::GuardedHead>, LandingRefreshError> {
        #[derive(Deserialize, PartialEq, Eq)]
        struct Identity {
            id: u64,
            node_id: String,
        }
        #[derive(Deserialize, PartialEq, Eq)]
        struct Repo {
            id: u64,
        }
        #[derive(Deserialize, PartialEq, Eq)]
        struct Head {
            sha: String,
            #[serde(rename = "ref")]
            branch: String,
            repo: Option<Repo>,
        }
        #[derive(Deserialize, PartialEq, Eq)]
        struct Base {
            #[serde(rename = "ref")]
            branch: String,
            repo: Repo,
        }
        #[derive(Deserialize, PartialEq, Eq)]
        struct Pull {
            number: u64,
            state: String,
            head: Head,
            base: Base,
        }
        if !valid_name(&policy.owner)
            || !valid_name(&policy.name)
            || !token.checks_read
            || token.app_client_id != self.client_id
            || token.api_origin != self.api.as_str()
        {
            return Err(LandingRefreshError::Scope);
        }
        let root = format!("repos/{}/{}", policy.owner, policy.name);
        let repo: Identity = self.get_json(token, &root, None).await?;
        if repo.id != token.repository_id || repo.node_id != target.plan.repository_node_id {
            return Err(LandingRefreshError::Scope);
        }
        let mut heads = Vec::with_capacity(target.members.len());
        for member in &target.members {
            let pull_path = format!("{root}/pulls/{}", member.candidate.number);
            let first: Pull = self.get_json(token, &pull_path, None).await?;
            let full_ref = format!("refs/heads/{}", first.head.branch);
            if first.number != member.candidate.number
                || first.state != "open"
                || first.head.sha != member.candidate.head_sha
                || first.head.repo.as_ref().map(|r| r.id) != Some(token.repository_id)
                || first.base.repo.id != token.repository_id
                || first.base.branch != policy.target_branch
                || !crate::landing::valid_guarded_head_ref(&full_ref)
                || full_ref == target.plan.target_ref
            {
                return Err(LandingRefreshError::Changed);
            }
            let branch_path = format!(
                "{root}/git/ref/{}",
                full_ref
                    .strip_prefix("refs/")
                    .ok_or(LandingRefreshError::Scope)?
            );
            let pull_ref_name = format!("refs/pull/{}/head", member.candidate.number);
            let pull_ref_path = format!("{root}/git/ref/pull/{}/head", member.candidate.number);
            let branch: DispatchRef = self.get_json(token, &branch_path, None).await?;
            let synthetic: DispatchRef = self.get_json(token, &pull_ref_path, None).await?;
            let last: Pull = self.get_json(token, &pull_path, None).await?;
            let final_branch: DispatchRef = self.get_json(token, &branch_path, None).await?;
            let final_synthetic: DispatchRef = self.get_json(token, &pull_ref_path, None).await?;
            if first != last
                || branch != final_branch
                || synthetic != final_synthetic
                || branch.reference != full_ref
                || synthetic.reference != pull_ref_name
                || branch.object.kind != "commit"
                || synthetic.object.kind != "commit"
                || branch.object.sha != first.head.sha
                || synthetic.object.sha != first.head.sha
            {
                return Err(LandingRefreshError::Changed);
            }
            heads.push(crate::landing::GuardedHead {
                number: first.number,
                reference: full_ref,
                sha: first.head.sha,
            });
        }
        let last_repo: Identity = self.get_json(token, &root, None).await?;
        if repo != last_repo {
            return Err(LandingRefreshError::Changed);
        }
        Ok(heads)
    }

    /// Refresh the PRs, reviews, refs and recorded Actions run immediately
    /// before claiming an experimental batch-bound intent. This is not authorization:
    /// approvals, trusted policy and provider state can still change later.
    /// No GitHub mutation is sent by this method.
    pub async fn refresh_and_claim_observed_landing(
        &self,
        journal: &mut crate::landing::LandingJournal,
        token: &InstallationToken,
        security_token: &InstallationToken,
        policy: &crate::reconcile::RepositoryPolicy,
        trusted_ci: &crate::landing::TrustedCiPolicy,
        operation_id: &str,
    ) -> Result<Option<crate::landing::LandingPlan>, LandingRefreshError> {
        let target = journal.refresh_target(operation_id, trusted_ci)?;
        let snapshot =
            Box::pin(self.refresh_landing_provider(token, security_token, policy, &target, true))
                .await?;
        journal
            .claim_with_fresh_snapshot(
                operation_id,
                &snapshot,
                trusted_ci,
                target.delivery_revision,
            )
            .map_err(Into::into)
    }
    /// Compare a current provider scan with the frozen local batch. This is a
    /// transient observation, does not create a landing intent or attestation,
    /// and does not guarantee provider state stays unchanged afterward.
    pub async fn preview_trusted_batch(
        &self,
        journal: &mut crate::landing::LandingJournal,
        token: &InstallationToken,
        security_token: &InstallationToken,
        policy: &crate::reconcile::RepositoryPolicy,
        trusted_ci: &crate::landing::TrustedCiPolicy,
        batch_id: &str,
    ) -> Result<crate::landing::LandingPlan, LandingRefreshError> {
        let target = journal.preview_target(batch_id, trusted_ci)?;
        let snapshot =
            Box::pin(self.refresh_landing_provider(token, security_token, policy, &target, true))
                .await?;
        journal
            .verify_preview(&target, trusted_ci, &snapshot)
            .map_err(Into::into)
    }
    /// Persist authenticated first-attempt CI evidence under an independent
    /// operator policy. This does not create a check or authorize landing.
    /// Unlike a landing claim, it does not require the App to bypass rules.
    pub async fn verify_staged_ci(
        &self,
        journal: &mut crate::landing::LandingJournal,
        tokens: (&InstallationToken, &InstallationToken),
        policy: &crate::reconcile::RepositoryPolicy,
        trusted_ci: &crate::landing::TrustedCiPolicy,
        batch_id: &str,
    ) -> Result<crate::landing::CiEvidenceRecord, LandingRefreshError> {
        self.verify_staged_ci_inner(journal, tokens, policy, trusted_ci, batch_id)
            .await
            .map(|(record, _)| record)
    }

    async fn verify_staged_ci_inner(
        &self,
        journal: &mut crate::landing::LandingJournal,
        tokens: (&InstallationToken, &InstallationToken),
        policy: &crate::reconcile::RepositoryPolicy,
        trusted_ci: &crate::landing::TrustedCiPolicy,
        batch_id: &str,
    ) -> Result<(crate::landing::CiEvidenceRecord, i64), LandingRefreshError> {
        let app = self.observe_app_identity().await?;
        let target = journal.preview_target(batch_id, trusted_ci)?;
        let snapshot =
            Box::pin(self.refresh_landing_provider(tokens.0, tokens.1, policy, &target, false))
                .await?;
        if self.observe_app_identity().await? != app {
            return Err(LandingRefreshError::Changed);
        }
        let now: u64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| GitHubError::Clock)?
            .as_millis()
            .try_into()
            .map_err(|_| GitHubError::Clock)?;
        let record = journal.record_ci_candidate(&target, trusted_ci, &snapshot, &app, now)?;
        Ok((record, target.delivery_revision))
    }

    async fn refresh_landing_provider(
        &self,
        token: &InstallationToken,
        security_token: &InstallationToken,
        policy: &crate::reconcile::RepositoryPolicy,
        target: &crate::landing::RefreshTarget,
        require_landing_bypass: bool,
    ) -> Result<ActionsSnapshot, LandingRefreshError> {
        #[derive(Deserialize, PartialEq, Eq)]
        struct Identity {
            id: u64,
            node_id: String,
        }
        let owner = &policy.owner;
        let repository = &policy.name;
        if !valid_name(owner)
            || !valid_name(repository)
            || !token.checks_read
            || token.app_client_id != self.client_id
            || token.api_origin != self.api.as_str()
            || !security_token.runner_inventory_read
            || security_token.app_client_id != self.client_id
            || security_token.api_origin != self.api.as_str()
            || security_token.repository_id != token.repository_id
            || security_token.installation_id != token.installation_id
        {
            return Err(LandingRefreshError::Scope);
        }
        let plan = &target.plan;
        let app_identity = self.observe_app_identity().await?;
        if i64::try_from(token.repository_id).ok() != Some(plan.repository_id)
            || i64::try_from(token.installation_id).ok() != Some(plan.installation_id)
            || policy.installation != plan.installation_id
            || policy.repository != plan.repository_id
            || plan.target_ref != format!("refs/heads/{}", policy.target_branch)
            || crate::reconcile::policy_digest(self, policy)
                .ok()
                .as_deref()
                != Some(target.repository_policy_digest.as_str())
        {
            return Err(LandingRefreshError::Scope);
        }
        let root = format!("repos/{owner}/{repository}");
        let first_repo: Identity = self.get_json(token, &root, None).await?;
        if first_repo.id != token.repository_id || first_repo.node_id != plan.repository_node_id {
            return Err(LandingRefreshError::Scope);
        }
        let base = self
            .observe_target_branch(token, owner, repository, &policy.target_branch)
            .await?;
        if base.commit.sha != plan.base_sha {
            return Err(LandingRefreshError::Changed);
        }
        let rules = self
            .observe_branch_rules(token, owner, repository, &policy.target_branch)
            .await?;
        if Some(rules.digest()?) != plan.branch_rules_digest {
            return Err(LandingRefreshError::Changed);
        }
        let security = self
            .observe_branch_security(security_token, owner, repository, &policy.target_branch)
            .await?;
        if security.branch != base
            || security.rules != rules
            || Some(security.digest()?) != plan.branch_security_digest
        {
            return Err(LandingRefreshError::Changed);
        }
        if require_landing_bypass
            && !matches!(crate::verification::assess_visible_branch_policy(&security,app_identity.id),Ok(findings) if findings.is_empty())
        {
            return Err(LandingRefreshError::UnsafeBranchPolicy);
        }
        let short_ref = plan
            .staging_ref
            .strip_prefix("refs/")
            .ok_or(LandingRefreshError::Scope)?;
        let ref_path = format!("{root}/git/ref/{short_ref}");
        let stage: DispatchRef = self.get_json(token, &ref_path, None).await?;
        if stage.reference != plan.staging_ref
            || stage.object.kind != "commit"
            || stage.object.sha != plan.staged_sha
        {
            return Err(LandingRefreshError::Changed);
        }
        let marker_path = plan.marker_ref.as_ref().map(|marker| {
            format!(
                "{root}/git/ref/{}",
                marker.strip_prefix("refs/").expect("validated marker")
            )
        });
        if let Some(path) = &marker_path {
            let marker: DispatchRef = self.get_json(token, path, None).await?;
            if Some(marker.reference.as_str()) != plan.marker_ref.as_deref()
                || marker.object.kind != "commit"
                || marker.object.sha != plan.base_sha
            {
                return Err(LandingRefreshError::Changed);
            }
        }
        if let Some(expected) = &plan.guarded_heads
            && &Box::pin(self.observe_same_repo_heads(token, policy, target)).await? != expected
        {
            return Err(LandingRefreshError::Changed);
        }
        let mut provider_reviews = Vec::with_capacity(target.members.len());
        for member in &target.members {
            let review = self
                .observe_review_decision(
                    token,
                    owner,
                    repository,
                    &plan.repository_node_id,
                    member.candidate.number,
                )
                .await?;
            if review.state != "OPEN"
                || review.is_draft
                || review.review_decision.as_deref() != Some("APPROVED")
                || review.base_ref_name != policy.target_branch
                || review.base_ref_oid != plan.base_sha
                || review.head_ref_oid != member.candidate.head_sha
            {
                return Err(LandingRefreshError::Changed);
            }
            let observed = self
                .observe_authorized_pull(token, owner, repository, member.candidate.number)
                .await?;
            let admission_policy = crate::admission::Policy {
                target_branch: policy.target_branch.clone(),
                required_reviews: policy.required_reviews,
                block_labels: policy.block_labels.clone(),
                authorized_reviewers: observed.authorized_reviewers.clone(),
            };
            let eligible = crate::admission::evaluate(&observed.snapshot, &admission_policy)
                .map_err(|_| LandingRefreshError::Changed)?;
            if eligible.pull_number != member.candidate.number
                || eligible.head_sha != member.candidate.head_sha
                || eligible.base_sha != plan.base_sha
                || observed.snapshot.pull.base.repo.id != token.repository_id
                || crate::reconcile::admission_digest(&observed)
                    .ok()
                    .as_deref()
                    != Some(member.evidence_digest.as_str())
            {
                return Err(LandingRefreshError::Changed);
            }
            provider_reviews.push(review);
        }
        let run_id = target.run_id;
        let run_path = format!("{root}/actions/runs/{run_id}");
        let first: ActionsRun = self.get_json(token, &run_path, None).await?;
        let snapshot = self
            .observe_actions_attempt(token, owner, repository, run_id, 1)
            .await?;
        let last: ActionsRun = self.get_json(token, &run_path, None).await?;
        for (member, previous) in target.members.iter().zip(&provider_reviews) {
            if self
                .observe_review_decision(
                    token,
                    owner,
                    repository,
                    &plan.repository_node_id,
                    member.candidate.number,
                )
                .await?
                != *previous
            {
                return Err(LandingRefreshError::Changed);
            }
        }
        if let Some(expected) = &plan.guarded_heads
            && &Box::pin(self.observe_same_repo_heads(token, policy, target)).await? != expected
        {
            return Err(LandingRefreshError::Changed);
        }
        let last_stage: DispatchRef = self.get_json(token, &ref_path, None).await?;
        if let Some(path) = &marker_path {
            let marker: DispatchRef = self.get_json(token, path, None).await?;
            if Some(marker.reference.as_str()) != plan.marker_ref.as_deref()
                || marker.object.kind != "commit"
                || marker.object.sha != plan.base_sha
            {
                return Err(LandingRefreshError::Changed);
            }
        }
        let last_rules = self
            .observe_branch_rules(token, owner, repository, &policy.target_branch)
            .await?;
        let last_security = self
            .observe_branch_security(security_token, owner, repository, &policy.target_branch)
            .await?;
        let last_base = self
            .observe_target_branch(token, owner, repository, &policy.target_branch)
            .await?;
        let last_repo: Identity = self.get_json(token, &root, None).await?;
        let last_app = self.observe_app_identity().await?;
        if first_repo != last_repo
            || first != last
            || last != snapshot.run
            || last.run_attempt != 1
            || last_base != base
            || last_rules != rules
            || last_security != security
            || last_app != app_identity
            || last_stage.reference != plan.staging_ref
            || last_stage.object.kind != "commit"
            || last_stage.object.sha != plan.staged_sha
        {
            return Err(LandingRefreshError::Changed);
        }
        Ok(snapshot)
    }
}

/// Immutable identity expected for a previously created App check. The
/// caller must independently establish its provenance and current policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppCheckExpectation {
    pub installation_id: u64,
    pub repository_id: u64,
    pub repository_node_id: String,
    pub check_run_id: u64,
    pub integration_sha: String,
    pub batch_id: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AppCheckObservation {
    pub id: u64,
    pub name: String,
    pub head_sha: String,
    pub external_id: Option<String>,
    pub status: String,
    pub conclusion: Option<String>,
    pub app: AppCheckIssuer,
    pub check_suite: AppCheckSuite,
    #[serde(default)]
    pub output: Option<AppCheckOutput>,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AppCheckOutput {
    pub title: Option<String>,
    pub summary: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AppCheckIssuer {
    pub id: u64,
    pub node_id: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AppCheckSuite {
    pub id: u64,
}
#[derive(Debug, Error)]
pub enum PendingCheckError {
    #[error("pending check credentials or repository scope do not match")]
    Scope,
    #[error("the check request may have succeeded; reconcile without retrying")]
    Uncertain,
    #[error("the supplied check does not match the frozen nonpassing intent")]
    CandidateMismatch,
    #[error(transparent)]
    GitHub(#[from] GitHubError),
    #[error(transparent)]
    Refresh(#[from] LandingRefreshError),
    #[error(transparent)]
    Journal(#[from] crate::landing::LandingError),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingCheckOutcome {
    Created(u64),
    AlreadyCreated(u64),
    Recovered(u64),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingCancellationOutcome {
    Cancelled(u64),
    AlreadyCancelled(u64),
    Recovered(u64),
}
const CANCELLED_CHECK_TITLE: &str = "Staging CI verification cancelled";
fn cancelled_check_summary(intent: &crate::landing::PendingCiCheckIntent) -> String {
    format!(
        "CI run {} candidate {} is no longer current. This cancelled check is not an attestation and does not authorize a merge.",
        intent.candidate.snapshot.run.id, intent.candidate_digest
    )
}
fn cancelled_check_matches(
    intent: &crate::landing::PendingCiCheckIntent,
    check: &AppCheckObservation,
) -> bool {
    check.id != 0
        && check.check_suite.id != 0
        && check.name == "sluss/verified"
        && check.head_sha == intent.candidate.plan.staged_sha
        && check.external_id.as_deref() == Some(intent.candidate.plan.batch_id.as_str())
        && check.status == "completed"
        && check.conclusion.as_deref() == Some("cancelled")
        && check.app.id == intent.candidate.app_id
        && check.app.node_id == intent.candidate.app_node_id
        && check.output.as_ref().is_some_and(|o| {
            o.title.as_deref() == Some(CANCELLED_CHECK_TITLE)
                && o.summary.as_deref() == Some(cancelled_check_summary(intent).as_str())
        })
}
const PENDING_CHECK_TITLE: &str = "Staging CI verification in progress";
fn pending_check_summary(intent: &crate::landing::PendingCiCheckIntent) -> String {
    format!(
        "CI run {} matched candidate {}. This in-progress check is not a CI attestation and does not authorize a merge.",
        intent.candidate.snapshot.run.id, intent.candidate_digest
    )
}
fn pending_check_matches(
    intent: &crate::landing::PendingCiCheckIntent,
    check: &AppCheckObservation,
) -> bool {
    check.id != 0
        && check.check_suite.id != 0
        && check.name == "sluss/verified"
        && check.head_sha == intent.candidate.plan.staged_sha
        && check.external_id.as_deref() == Some(intent.candidate.plan.batch_id.as_str())
        && check.status == "in_progress"
        && check.conclusion.is_none()
        && check.app.id == intent.candidate.app_id
        && check.app.node_id == intent.candidate.app_node_id
        && check.output.as_ref().is_some_and(|o| {
            o.title.as_deref() == Some(PENDING_CHECK_TITLE)
                && o.summary.as_deref() == Some(pending_check_summary(intent).as_str())
        })
}

impl App {
    /// Create a nonpassing in-progress check after fresh provider verification.
    /// A crash or ambiguous POST remains uncertain and is never retried.
    pub async fn start_staged_check(
        &self,
        journal: &mut crate::landing::LandingJournal,
        tokens: (&InstallationToken, &InstallationToken, &InstallationToken),
        policy: &crate::reconcile::RepositoryPolicy,
        trusted_ci: &crate::landing::TrustedCiPolicy,
        batch_id: &str,
    ) -> Result<PendingCheckOutcome, PendingCheckError> {
        use crate::landing::{PendingCiCheckClaim, PendingCiCheckState};
        let (read, security, write) = tokens;
        if !write.checks_write
            || write.repository_id != read.repository_id
            || write.installation_id != read.installation_id
            || write.app_client_id != self.client_id
            || write.api_origin != self.api.as_str()
        {
            return Err(PendingCheckError::Scope);
        }
        let (record, revision) = self
            .verify_staged_ci_inner(journal, (read, security), policy, trusted_ci, batch_id)
            .await?;
        self.check_rate_limit().await?;
        let intent = match journal.claim_pending_ci_check(&record, revision)? {
            PendingCiCheckClaim::Send(intent) => intent,
            PendingCiCheckClaim::Existing(
                intent,
                PendingCiCheckState::Created {
                    check_id,
                    suite_id,
                    direct,
                    recovered,
                },
            ) => {
                let observed = self
                    .read_staged_ci_check(
                        read,
                        (&policy.owner, &policy.name),
                        &intent,
                        check_id,
                        false,
                    )
                    .await?;
                if observed.check_suite.id != suite_id {
                    return Err(PendingCheckError::CandidateMismatch);
                }
                return if !direct && recovered {
                    Ok(PendingCheckOutcome::Recovered(check_id))
                } else {
                    Ok(PendingCheckOutcome::AlreadyCreated(check_id))
                };
            }
            PendingCiCheckClaim::Existing(_, _) => return Err(PendingCheckError::Uncertain),
        };
        let url = self
            .api
            .join(&format!(
                "repos/{}/{}/check-runs",
                policy.owner, policy.name
            ))
            .map_err(|_| PendingCheckError::Scope)?;
        let body = serde_json::json!({"name":"sluss/verified","head_sha":intent.candidate.plan.staged_sha,"external_id":batch_id,"status":"in_progress","output":{"title":PENDING_CHECK_TITLE,"summary":pending_check_summary(&intent)}});
        let mut response = self
            .send(
                self.client
                    .post(url)
                    .header("Accept", "application/vnd.github+json")
                    .header("X-GitHub-Api-Version", "2022-11-28")
                    .bearer_auth(write.secret())
                    .json(&body),
            )
            .await
            .map_err(|_| PendingCheckError::Uncertain)?;
        if response.status() != reqwest::StatusCode::CREATED {
            return Err(PendingCheckError::Uncertain);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| PendingCheckError::Uncertain)?
        {
            if bytes.len().saturating_add(chunk.len()) > 65536 {
                return Err(PendingCheckError::Uncertain);
            }
            bytes.extend_from_slice(&chunk);
        }
        let observed: AppCheckObservation =
            serde_json::from_slice(&bytes).map_err(|_| PendingCheckError::Uncertain)?;
        if !pending_check_matches(&intent, &observed) {
            return Err(PendingCheckError::Uncertain);
        }
        journal
            .record_pending_ci_receipt(batch_id, observed.id, observed.check_suite.id, true)
            .map_err(|_| PendingCheckError::Uncertain)?;
        Ok(PendingCheckOutcome::Created(observed.id))
    }

    async fn read_staged_ci_check(
        &self,
        token: &InstallationToken,
        repository: (&str, &str),
        intent: &crate::landing::PendingCiCheckIntent,
        check_id: u64,
        cancelled: bool,
    ) -> Result<AppCheckObservation, PendingCheckError> {
        #[derive(Deserialize, PartialEq, Eq)]
        struct Identity {
            id: u64,
            node_id: String,
        }
        let (owner, name) = repository;
        if !token.checks_read
            || !valid_name(owner)
            || !valid_name(name)
            || check_id == 0
            || token.app_client_id != self.client_id
            || token.api_origin != self.api.as_str()
            || i64::try_from(token.repository_id).ok() != Some(intent.candidate.plan.repository_id)
            || i64::try_from(token.installation_id).ok()
                != Some(intent.candidate.plan.installation_id)
        {
            return Err(PendingCheckError::Scope);
        }
        let app = self.observe_app_identity().await?;
        let root = format!("repos/{owner}/{name}");
        let first_repo: Identity = self.get_json(token, &root, None).await?;
        let path = format!("{root}/check-runs/{check_id}");
        let first: AppCheckObservation = self.get_json(token, &path, None).await?;
        let last: AppCheckObservation = self.get_json(token, &path, None).await?;
        let last_repo: Identity = self.get_json(token, &root, None).await?;
        let last_app = self.observe_app_identity().await?;
        if first_repo != last_repo
            || first_repo.id != token.repository_id
            || first_repo.node_id != intent.candidate.plan.repository_node_id
            || app != last_app
            || app.id != intent.candidate.app_id
            || app.node_id != intent.candidate.app_node_id
            || first != last
            || first.id != check_id
            || if cancelled {
                !cancelled_check_matches(intent, &first)
            } else {
                !pending_check_matches(intent, &first)
            }
        {
            return Err(PendingCheckError::CandidateMismatch);
        }
        Ok(first)
    }

    /// Cancel a directly received, locally stale nonpassing check at most once.
    /// An ambiguous PATCH remains journaled and is never sent again blindly.
    pub async fn cancel_stale_staged_check(
        &self,
        journal: &mut crate::landing::LandingJournal,
        tokens: (&InstallationToken, &InstallationToken),
        repository: (&str, &str),
        batch_id: &str,
    ) -> Result<PendingCancellationOutcome, PendingCheckError> {
        use crate::landing::PendingCiCancellationState;
        let (read, write) = tokens;
        if !write.checks_write
            || write.repository_id != read.repository_id
            || write.installation_id != read.installation_id
            || write.app_client_id != self.client_id
            || write.api_origin != self.api.as_str()
        {
            return Err(PendingCheckError::Scope);
        }
        if let Some(state) = journal.pending_ci_cancellation(batch_id)? {
            match state {
                PendingCiCancellationState::Sending { .. } => {
                    return Err(PendingCheckError::Uncertain);
                }
                PendingCiCancellationState::Cancelled {
                    check_id, suite_id, ..
                } => {
                    let (intent, _) = journal
                        .pending_ci_check(batch_id)?
                        .ok_or(PendingCheckError::Uncertain)?;
                    let observed = self
                        .read_staged_ci_check(read, repository, &intent, check_id, true)
                        .await?;
                    if observed.check_suite.id != suite_id {
                        return Err(PendingCheckError::CandidateMismatch);
                    }
                    return Ok(PendingCancellationOutcome::AlreadyCancelled(check_id));
                }
            }
        }
        let target = journal.stale_pending_ci_check(batch_id)?;
        let before = self
            .read_staged_ci_check(read, repository, &target.intent, target.check_id, false)
            .await?;
        if before.check_suite.id != target.suite_id {
            return Err(PendingCheckError::CandidateMismatch);
        }
        self.check_rate_limit().await?;
        if !journal.claim_pending_ci_cancellation(&target)? {
            return Err(PendingCheckError::Uncertain);
        }
        let (owner, name) = repository;
        let url = self
            .api
            .join(&format!(
                "repos/{owner}/{name}/check-runs/{}",
                target.check_id
            ))
            .map_err(|_| PendingCheckError::Scope)?;
        let body = serde_json::json!({"status":"completed","conclusion":"cancelled","output":{"title":CANCELLED_CHECK_TITLE,"summary":cancelled_check_summary(&target.intent)}});
        let mut response = self
            .send(
                self.client
                    .patch(url)
                    .header("Accept", "application/vnd.github+json")
                    .header("X-GitHub-Api-Version", "2022-11-28")
                    .bearer_auth(write.secret())
                    .json(&body),
            )
            .await
            .map_err(|_| PendingCheckError::Uncertain)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(PendingCheckError::Uncertain);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| PendingCheckError::Uncertain)?
        {
            if bytes.len().saturating_add(chunk.len()) > 65536 {
                return Err(PendingCheckError::Uncertain);
            }
            bytes.extend_from_slice(&chunk);
        }
        let observed: AppCheckObservation =
            serde_json::from_slice(&bytes).map_err(|_| PendingCheckError::Uncertain)?;
        if observed.id != target.check_id
            || observed.check_suite.id != target.suite_id
            || !cancelled_check_matches(&target.intent, &observed)
        {
            return Err(PendingCheckError::Uncertain);
        }
        journal
            .record_pending_ci_cancellation(batch_id, target.check_id, target.suite_id, true)
            .map_err(|_| PendingCheckError::Uncertain)?;
        Ok(PendingCancellationOutcome::Cancelled(target.check_id))
    }

    /// Read back a cancelled check after an uncertain PATCH, without resending.
    pub async fn recover_cancelled_staged_check(
        &self,
        journal: &mut crate::landing::LandingJournal,
        token: &InstallationToken,
        repository: (&str, &str),
        batch_id: &str,
    ) -> Result<PendingCancellationOutcome, PendingCheckError> {
        use crate::landing::PendingCiCancellationState;
        let state = journal
            .pending_ci_cancellation(batch_id)?
            .ok_or(PendingCheckError::Uncertain)?;
        let (check_id, suite_id) = match state {
            PendingCiCancellationState::Sending { check_id, suite_id }
            | PendingCiCancellationState::Cancelled {
                check_id, suite_id, ..
            } => (check_id, suite_id),
        };
        let (intent, _) = journal
            .pending_ci_check(batch_id)?
            .ok_or(PendingCheckError::Uncertain)?;
        let observed = self
            .read_staged_ci_check(token, repository, &intent, check_id, true)
            .await?;
        if observed.check_suite.id != suite_id {
            return Err(PendingCheckError::CandidateMismatch);
        }
        let recorded =
            journal.record_pending_ci_cancellation(batch_id, check_id, suite_id, false)?;
        match recorded {
            PendingCiCancellationState::Cancelled { direct: true, .. } => {
                Ok(PendingCancellationOutcome::AlreadyCancelled(check_id))
            }
            PendingCiCancellationState::Cancelled { .. } => {
                Ok(PendingCancellationOutcome::Recovered(check_id))
            }
            _ => Err(PendingCheckError::Uncertain),
        }
    }

    /// Associate an explicitly identified historical in-progress check without
    /// reissuing the POST; metadata equality does not establish request origin.
    pub async fn recover_staged_check(
        &self,
        journal: &mut crate::landing::LandingJournal,
        token: &InstallationToken,
        repository: (&str, &str),
        batch_id: &str,
        check_id: u64,
    ) -> Result<PendingCheckOutcome, PendingCheckError> {
        use crate::landing::PendingCiCheckState;
        let (intent, state) = journal
            .pending_ci_check(batch_id)?
            .ok_or(PendingCheckError::Uncertain)?;
        let known_suite = match state {
            PendingCiCheckState::Sending => None,
            PendingCiCheckState::Created {
                check_id: known,
                suite_id,
                ..
            } if known == check_id => Some(suite_id),
            _ => return Err(PendingCheckError::Uncertain),
        };
        let observed = self
            .read_staged_ci_check(token, repository, &intent, check_id, false)
            .await?;
        if known_suite.is_some_and(|suite| suite != observed.check_suite.id) {
            return Err(PendingCheckError::CandidateMismatch);
        }
        let state = journal.record_pending_ci_receipt(
            batch_id,
            observed.id,
            observed.check_suite.id,
            false,
        )?;
        match state {
            PendingCiCheckState::Created { direct: true, .. } => {
                Ok(PendingCheckOutcome::AlreadyCreated(check_id))
            }
            PendingCiCheckState::Created { .. } => Ok(PendingCheckOutcome::Recovered(check_id)),
            _ => Err(PendingCheckError::Uncertain),
        }
    }
}

#[derive(Debug, Error)]
pub enum FailureCheckError {
    #[error("check publisher credentials or repository scope do not match")]
    Scope,
    #[error("the check request may have succeeded; reconcile without retrying")]
    Uncertain,
    #[error("the supplied check does not match the recorded failure")]
    CandidateMismatch,
    #[error(transparent)]
    GitHub(#[from] GitHubError),
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
    #[error(transparent)]
    Scheduler(#[from] crate::scheduler::SchedulerError),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureCheckOutcome {
    Created(u64),
    AlreadyCreated(u64),
    Recovered(u64),
}
impl App {
    /// Publish only a failed CI observation to the exact staged commit. The
    /// intent is durably claimed before one POST; no ambiguous outcome is
    /// retried. This does not issue a positive attestation or authorize merge.
    pub async fn publish_failed_check(
        &self,
        scheduler: &mut crate::scheduler::Scheduler,
        tokens: (&InstallationToken, &InstallationToken),
        owner: &str,
        repository: &str,
        batch_id: &str,
    ) -> Result<FailureCheckOutcome, FailureCheckError> {
        use crate::scheduler::FailureCheckState;
        let (read, write) = tokens;
        if !read.checks_read
            || !write.checks_write
            || !valid_name(owner)
            || !valid_name(repository)
            || !valid_name(batch_id)
            || read.installation_id != write.installation_id
            || read.repository_id != write.repository_id
            || read.app_client_id != self.client_id
            || write.app_client_id != self.client_id
            || read.api_origin != self.api.as_str()
            || write.api_origin != self.api.as_str()
        {
            return Err(FailureCheckError::Scope);
        }
        let app = self.observe_app_identity().await?;
        let (intent, state) = scheduler.prepare_failure_check(batch_id)?;
        if intent.installation_id != read.installation_id
            || intent.repository_id != read.repository_id
        {
            return Err(FailureCheckError::Scope);
        }
        match state {
            FailureCheckState::Sending { .. } => return Err(FailureCheckError::Uncertain),
            FailureCheckState::Created { app_id, check_id } if app_id == app.id => {
                if scheduler
                    .failure_check_source(batch_id)?
                    .is_some_and(|(direct, recovered)| !direct && recovered)
                {
                    return Ok(FailureCheckOutcome::Recovered(check_id));
                }
                return Ok(FailureCheckOutcome::AlreadyCreated(check_id));
            }
            FailureCheckState::Created { .. } => return Err(FailureCheckError::Scope),
            FailureCheckState::Conflict { .. } => return Err(FailureCheckError::Uncertain),
            FailureCheckState::Prepared => {}
        }
        let root = format!("repos/{owner}/{repository}");
        let repo: Repository = self.get_json(read, &root, None).await?;
        if repo.id != read.repository_id {
            return Err(FailureCheckError::Scope);
        }
        let snapshot = self
            .reconcile_dispatch(scheduler, read, owner, repository, batch_id)
            .await?;
        if snapshot.run.id != intent.run_id || snapshot.run.head_sha != intent.integration_sha {
            return Err(FailureCheckError::Scope);
        }
        self.check_rate_limit().await?;
        if !scheduler.claim_failure_check(batch_id, app.id, &snapshot)? {
            return Err(FailureCheckError::Uncertain);
        }
        let url = self
            .api
            .join(&format!("{root}/check-runs"))
            .map_err(|_| FailureCheckError::Uncertain)?;
        let body = serde_json::json!({"name":"sluss/verified","head_sha":intent.integration_sha,"external_id":batch_id,
            "status":"completed","conclusion":"failure","output":{"title":"Staging CI failed","summary":format!("Recorded CI run {} failed. This check is not an attestation and does not authorize a merge.",intent.run_id)}});
        let mut response = self
            .send(
                self.client
                    .post(url)
                    .header("Accept", "application/vnd.github+json")
                    .header("X-GitHub-Api-Version", "2022-11-28")
                    .bearer_auth(write.secret())
                    .json(&body),
            )
            .await
            .map_err(|_| FailureCheckError::Uncertain)?;
        if response.status() != reqwest::StatusCode::CREATED {
            return Err(FailureCheckError::Uncertain);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| FailureCheckError::Uncertain)?
        {
            if bytes.len().saturating_add(chunk.len()) > 65536 {
                return Err(FailureCheckError::Uncertain);
            }
            bytes.extend_from_slice(&chunk);
        }
        let result: AppCheckObservation =
            serde_json::from_slice(&bytes).map_err(|_| FailureCheckError::Uncertain)?;
        if result.id == 0
            || result.name != "sluss/verified"
            || result.head_sha != intent.integration_sha
            || result.external_id.as_deref() != Some(batch_id)
            || result.status != "completed"
            || result.conclusion.as_deref() != Some("failure")
            || result.app.id != app.id
            || result.app.node_id != app.node_id
            || result.check_suite.id == 0
        {
            return Err(FailureCheckError::Uncertain);
        }
        scheduler
            .record_failure_check(batch_id, app.id, result.id)
            .map_err(|_| FailureCheckError::Uncertain)?;
        Ok(FailureCheckOutcome::Created(result.id))
    }
    /// Associate a matching historical failure check without retrying the POST.
    /// Matching fields do not prove which request created the candidate.
    pub async fn recover_failed_check(
        &self,
        scheduler: &mut crate::scheduler::Scheduler,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        batch_id: &str,
        check_id: u64,
    ) -> Result<FailureCheckOutcome, FailureCheckError> {
        use crate::scheduler::FailureCheckState;
        if !token.checks_read
            || !valid_name(owner)
            || !valid_name(repository)
            || !valid_name(batch_id)
            || check_id == 0
            || token.app_client_id != self.client_id
            || token.api_origin != self.api.as_str()
        {
            return Err(FailureCheckError::Scope);
        }
        let (intent, state) = scheduler
            .failure_check(batch_id)?
            .ok_or(FailureCheckError::Uncertain)?;
        if intent.installation_id != token.installation_id
            || intent.repository_id != token.repository_id
        {
            return Err(FailureCheckError::Scope);
        }
        let app_id = match state {
            FailureCheckState::Sending { app_id } | FailureCheckState::Created { app_id, .. } => {
                app_id
            }
            _ => return Err(FailureCheckError::Uncertain),
        };
        let app = self.observe_app_identity().await?;
        let root = format!("repos/{owner}/{repository}");
        let first_repo: Repository = self.get_json(token, &root, None).await?;
        let path = format!("{root}/check-runs/{check_id}");
        let first: AppCheckObservation = self.get_json(token, &path, None).await?;
        let last: AppCheckObservation = self.get_json(token, &path, None).await?;
        let last_repo: Repository = self.get_json(token, &root, None).await?;
        let last_app = self.observe_app_identity().await?;
        let summary = format!(
            "Recorded CI run {} failed. This check is not an attestation and does not authorize a merge.",
            intent.run_id
        );
        if app != last_app
            || app.id != app_id
            || first_repo.id != intent.repository_id
            || last_repo.id != intent.repository_id
            || first != last
            || last.id != check_id
            || last.name != "sluss/verified"
            || last.head_sha != intent.integration_sha
            || last.external_id.as_deref() != Some(batch_id)
            || last.status != "completed"
            || last.conclusion.as_deref() != Some("failure")
            || last.app.id != app.id
            || last.app.node_id != app.node_id
            || last.check_suite.id == 0
            || !last.output.as_ref().is_some_and(|o| {
                o.title.as_deref() == Some("Staging CI failed")
                    && o.summary.as_deref() == Some(summary.as_str())
            })
        {
            return Err(FailureCheckError::CandidateMismatch);
        }
        scheduler.record_recovered_failure_check(batch_id, app.id, check_id)?;
        let source = scheduler.failure_check_source(batch_id)?;
        if source.is_some_and(|(direct, _)| direct) {
            Ok(FailureCheckOutcome::AlreadyCreated(check_id))
        } else {
            Ok(FailureCheckOutcome::Recovered(check_id))
        }
    }
    /// Read an already existing successful `sluss/verified` check twice and
    /// compare GitHub's issuing App and exact SHA with independently scoped
    /// identity observations. This does not create a check, prove its CI
    /// policy, establish its provenance, or authorize a merge. GitHub state
    /// may change immediately after these reads.
    pub async fn observe_own_verified_check(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        expected: &AppCheckExpectation,
    ) -> Result<AppCheckObservation, GitHubError> {
        #[derive(Deserialize, PartialEq, Eq)]
        struct Identity {
            id: u64,
            node_id: String,
        }
        if !token.checks_read
            || !valid_name(owner)
            || !valid_name(repository)
            || expected.installation_id == 0
            || expected.repository_id == 0
            || expected.check_run_id == 0
            || !valid_name(&expected.batch_id)
            || !commit_sha(&expected.integration_sha)
            || expected.integration_sha.bytes().all(|c| c == b'0')
            || expected.installation_id != token.installation_id
            || expected.repository_id != token.repository_id
            || token.app_client_id != self.client_id
            || token.api_origin != self.api.as_str()
        {
            return Err(GitHubError::InvalidConfiguration);
        }
        let app = self.observe_app_identity().await?;
        let root = format!("repos/{owner}/{repository}");
        let first_repo: Identity = self.get_json(token, &root, None).await?;
        if first_repo.id != expected.repository_id
            || first_repo.node_id != expected.repository_node_id
        {
            return Err(GitHubError::UnstableObservation);
        }
        let path = format!("{root}/check-runs/{}", expected.check_run_id);
        let first: AppCheckObservation = self.get_json(token, &path, None).await?;
        let last: AppCheckObservation = self.get_json(token, &path, None).await?;
        let last_repo: Identity = self.get_json(token, &root, None).await?;
        let last_app = self.observe_app_identity().await?;
        if first != last
            || first_repo != last_repo
            || app != last_app
            || last.id != expected.check_run_id
            || last.name != "sluss/verified"
            || last.head_sha != expected.integration_sha
            || last.external_id.as_deref() != Some(expected.batch_id.as_str())
            || last.status != "completed"
            || last.conclusion.as_deref() != Some("success")
            || last.app.id != app.id
            || last.app.node_id != app.node_id
            || last.check_suite.id == 0
        {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(last)
    }
}

#[derive(Debug, Error)]
pub enum InputFetchError {
    #[error("GitHub input fetch is not authorized for this token or repository")]
    InvalidScope,
    #[error("GitHub input-fetch worker stopped unexpectedly")]
    Worker,
    #[error(transparent)]
    GitHub(#[from] GitHubError),
    #[error(transparent)]
    Fetch(#[from] crate::integration::FetchError),
}
impl App {
    /// Fetch provider refs only from GitHub.com with the scoped installation token.
    pub async fn fetch_batch_inputs(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        target: &str,
        batch: &crate::batch::Batch,
    ) -> Result<crate::integration::FetchedInputs, InputFetchError> {
        if token.app_client_id != self.client_id
            || token.api_origin != self.api.as_str()
            || self.api.as_str() != "https://api.github.com/"
            || !valid_name(owner)
            || !valid_name(repository)
        {
            return Err(InputFetchError::InvalidScope);
        }
        let repo: Repository = self
            .get_json(token, &format!("repos/{owner}/{repository}"), None)
            .await?;
        if repo.id != token.repository_id {
            return Err(InputFetchError::InvalidScope);
        }
        let owner = owner.to_owned();
        let repository = repository.to_owned();
        let target = target.to_owned();
        let batch = batch.clone();
        let secret = token.secret().to_owned();
        self.spawn_github_git(move || {
            crate::integration::fetch_github(&owner, &repository, &target, &batch, &secret)
        })
        .await?
        .map_err(|_| InputFetchError::Worker)?
        .map_err(InputFetchError::Fetch)
    }
}

#[derive(Debug, Error)]
pub enum StagingError {
    #[error("staging token or GitHub origin is not authorized for this operation")]
    InvalidScope,
    #[error("staging worker stopped; publication outcome may be uncertain")]
    Worker,
    #[error(transparent)]
    GitHub(#[from] GitHubError),
    #[error(transparent)]
    Git(#[from] crate::integration::StageError),
}
impl App {
    /// Publish with a repository-scoped Contents-write App token after checking
    /// repository identity. Only GitHub.com is supported because the Git remote
    /// constructed by the integration builder is fixed to github.com.
    /// Branch rules must deny this token permission to change the target branch.
    pub async fn stage_integration(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        integration: crate::integration::Integration,
        id: &str,
    ) -> Result<crate::integration::Staged, StagingError> {
        if !token.contents_write
            || token.app_client_id != self.client_id
            || token.api_origin != self.api.as_str()
            || self.api.as_str() != "https://api.github.com/"
            || !valid_name(owner)
            || !valid_name(repository)
        {
            return Err(StagingError::InvalidScope);
        }
        let observed: Repository = self
            .get_json(token, &format!("repos/{owner}/{repository}"), None)
            .await?;
        if observed.id != token.repository_id {
            return Err(StagingError::InvalidScope);
        }
        let owner = owner.to_owned();
        let repository = repository.to_owned();
        let id = id.to_owned();
        let secret = token.secret().to_owned();
        self.spawn_github_git(move || integration.stage_github(&owner, &repository, &secret, &id))
            .await?
            .map_err(|_| StagingError::Worker)?
            .map_err(StagingError::Git)
    }
}

/// A reserved host must use a dedicated, operator-managed runner label.
pub fn valid_runner_label(label: &str) -> bool {
    label.starts_with("sluss-host-")
        && (12..=100).contains(&label.len())
        && label
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Repository-level runners only; organization and enterprise runners require
/// separate inventory and are not included by this GitHub endpoint.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct RepositoryRunner {
    pub id: u64,
    pub name: String,
    pub status: String,
    pub busy: bool,
    pub labels: Vec<RunnerLabel>,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct RunnerLabel {
    pub name: String,
}
#[derive(Deserialize)]
struct RunnersPage {
    total_count: usize,
    runners: Vec<RepositoryRunner>,
}
impl App {
    /// Fetch a bounded and double-checked repository runner inventory.
    /// A valid result does not include organization or enterprise runners.
    pub async fn observe_repository_runners(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
    ) -> Result<Vec<RepositoryRunner>, GitHubError> {
        if !token.runner_inventory_read || !valid_name(owner) || !valid_name(repository) {
            return Err(GitHubError::InvalidConfiguration);
        }
        let root = format!("repos/{owner}/{repository}");
        let observed: Repository = self.get_json(token, &root, None).await?;
        if observed.id != token.repository_id {
            return Err(GitHubError::UnstableObservation);
        }
        let path = format!("{root}/actions/runners");
        let first = self.fetch_runners(token, &path).await?;
        let second = self.fetch_runners(token, &path).await?;
        if first != second {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(second)
    }
    async fn fetch_runners(
        &self,
        token: &InstallationToken,
        path: &str,
    ) -> Result<Vec<RepositoryRunner>, GitHubError> {
        let mut result = Vec::new();
        let mut ids = std::collections::BTreeSet::new();
        let mut expected_total = None;
        for page in 1..=100u32 {
            let data: RunnersPage = self.get_json(token, path, Some(page)).await?;
            if data.runners.len() > 100
                || data.total_count > 10000
                || expected_total.is_some_and(|count| count != data.total_count)
            {
                return Err(GitHubError::UnstableObservation);
            }
            expected_total = Some(data.total_count);
            for runner in data.runners {
                let mut labels = std::collections::BTreeSet::new();
                if runner.id == 0
                    || runner.name.is_empty()
                    || !matches!(runner.status.as_str(), "online" | "offline")
                    || !ids.insert(runner.id)
                    || runner.labels.iter().any(|label| {
                        label.name.is_empty() || !labels.insert(label.name.to_ascii_lowercase())
                    })
                {
                    return Err(GitHubError::UnstableObservation);
                }
                result.push(runner);
            }
            if result.len() > data.total_count {
                return Err(GitHubError::UnstableObservation);
            }
            if result.len() == data.total_count {
                return Ok(result);
            }
            if result.len() != page as usize * 100 {
                return Err(GitHubError::UnstableObservation);
            }
        }
        Err(GitHubError::UnstableObservation)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct BranchInfo {
    pub name: String,
    pub protected: bool,
    pub commit: BranchCommit,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct BranchCommit {
    pub sha: String,
}
pub(crate) fn valid_workflow_path(path: &str) -> bool {
    let filename = path.strip_prefix(".github/workflows/");
    crate::integration::valid_policy_path(path)
        && filename.is_some_and(|name| {
            !name.contains('/') && (name.ends_with(".yml") || name.ends_with(".yaml"))
        })
}

/// A reported workflow ref must not contradict the frozen integration.
/// A path without a suffix has no ref information; the caller must separately
/// verify run identity and cannot infer workflow trust from that absence.
pub(crate) fn workflow_path_matches(
    expected: &str,
    reported: &str,
    staging_ref: &str,
    integration_sha: &str,
) -> bool {
    let Some(branch) = staging_ref
        .strip_prefix("refs/heads/")
        .filter(|value| !value.is_empty())
    else {
        return false;
    };
    match reported.split_once('@') {
        Some((path, reference)) => {
            path == expected
                && (reference == staging_ref || reference == branch || reference == integration_sha)
        }
        None => reported == expected,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct WorkflowInfo {
    pub id: u64,
    pub path: String,
    pub state: String,
}
#[derive(Deserialize, PartialEq, Eq)]
struct DefaultRepository {
    id: u64,
    default_branch: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultWorkflowPresence {
    pub branch: String,
    pub commit_sha: String,
    pub blob_sha: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CiRepositoryPreflight {
    pub branch: BranchInfo,
    pub workflow: WorkflowInfo,
    pub default_workflow: DefaultWorkflowPresence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryPreflight {
    pub branch: BranchInfo,
    pub workflow: WorkflowInfo,
    pub default_workflow: DefaultWorkflowPresence,
    /// Active rules returned by GitHub, not a complete bypass-policy proof.
    pub rules: Vec<serde_json::Value>,
}

/// A 403/404 is deliberately not classified as absence of protection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtectionObservation {
    Present(serde_json::Value),
    Unavailable(u16),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RulesetSecurityObservation {
    pub id: u64,
    pub detail: serde_json::Value,
    /// False means GitHub omitted or withheld the bypass list.
    pub bypass_visible: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchSecurityObservation {
    pub branch: BranchInfo,
    pub rules: BranchRulesSnapshot,
    pub protection: ProtectionObservation,
    pub rulesets: Vec<RulesetSecurityObservation>,
}
impl BranchSecurityObservation {
    /// Hash only a complete visible observation. This does not establish
    /// that the visible permissions or bypass actors are safe for landing.
    pub fn digest(&self) -> Result<String, GitHubError> {
        use sha2::{Digest, Sha256};
        self.rules.digest()?;
        if !self.branch.protected
            || self.branch.name != self.rules.branch
            || !commit_sha(&self.branch.commit.sha)
            || !matches!(&self.protection, ProtectionObservation::Present(value) if valid_legacy_protection(value))
            || self.rulesets.len() > 100
        {
            return Err(GitHubError::UnstableObservation);
        }
        let mut ids = std::collections::BTreeSet::new();
        for ruleset in &self.rulesets {
            let detail = &ruleset.detail;
            let actors = detail
                .get("bypass_actors")
                .and_then(|value| value.as_array())
                .ok_or(GitHubError::UnstableObservation)?;
            if ruleset.id == 0
                || !ids.insert(ruleset.id)
                || !ruleset.bypass_visible
                || detail.get("id").and_then(|v| v.as_u64()) != Some(ruleset.id)
                || detail.get("target").and_then(|v| v.as_str()) != Some("branch")
                || detail.get("enforcement").and_then(|v| v.as_str()) != Some("active")
                || detail.get("rules").and_then(|v| v.as_array()).is_none()
                || actors.iter().any(|actor| !valid_bypass_actor(actor))
            {
                return Err(GitHubError::UnstableObservation);
            }
        }
        let mut expected_ids = std::collections::BTreeSet::new();
        for rule in &self.rules.rules {
            let id = rule
                .get("ruleset_id")
                .and_then(|value| value.as_u64())
                .filter(|id| *id > 0)
                .ok_or(GitHubError::UnstableObservation)?;
            expected_ids.insert(id);
            let detail = &self
                .rulesets
                .iter()
                .find(|entry| entry.id == id)
                .ok_or(GitHubError::UnstableObservation)?
                .detail;
            let src = rule
                .get("ruleset_source")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty());
            let dst = detail
                .get("source")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty());
            let kind = rule
                .get("ruleset_source_type")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty());
            if src.zip(dst).is_none_or(|(a, b)| !a.eq_ignore_ascii_case(b))
                || kind.is_none()
                || kind != detail.get("source_type").and_then(|v| v.as_str())
                || !detail["rules"].as_array().is_some_and(|list| {
                    list.iter().any(|entry| {
                        entry.get("type") == rule.get("type")
                            && entry.get("parameters") == rule.get("parameters")
                    })
                })
            {
                return Err(GitHubError::UnstableObservation);
            }
        }
        if ids != expected_ids {
            return Err(GitHubError::UnstableObservation);
        }
        let body = serde_json::to_vec(self).map_err(|_| GitHubError::UnstableObservation)?;
        if body.len() > 1024 * 1024 {
            return Err(GitHubError::UnstableObservation);
        }
        let mut hash = Sha256::new();
        hash.update(b"sluss-branch-security-v1\0");
        hash.update(body);
        Ok(hex::encode(hash.finalize()))
    }
}
fn valid_legacy_protection(value: &serde_json::Value) -> bool {
    [
        "/enforce_admins/enabled",
        "/allow_force_pushes/enabled",
        "/allow_deletions/enabled",
        "/required_linear_history/enabled",
    ]
    .iter()
    .all(|path| value.pointer(path).and_then(|v| v.as_bool()).is_some())
        && [
            "required_status_checks",
            "required_pull_request_reviews",
            "restrictions",
        ]
        .iter()
        .all(|key| {
            value
                .get(key)
                .is_some_and(|field| field.is_null() || field.is_object())
        })
}
fn valid_bypass_actor(actor: &serde_json::Value) -> bool {
    let kind = actor.get("actor_type").and_then(|v| v.as_str());
    let mode = actor.get("bypass_mode").and_then(|v| v.as_str());
    if !matches!(mode, Some("always" | "pull_request" | "exempt")) {
        return false;
    }
    match kind {
        Some("Integration" | "RepositoryRole" | "Team" | "User") => actor
            .get("actor_id")
            .and_then(|v| v.as_u64())
            .is_some_and(|id| id > 0),
        Some("DeployKey") => {
            actor.get("actor_id").is_some_and(|v| v.is_null()) && mode != Some("pull_request")
        }
        Some("OrganizationAdmin") => true,
        _ => false,
    }
}

/// Provider-visible active branch rules only. Bypass actors and legacy branch
/// protection may require other permissions and endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchRulesSnapshot {
    pub repository_id: u64,
    pub branch: String,
    pub rules: Vec<serde_json::Value>,
}
impl BranchRulesSnapshot {
    pub fn digest(&self) -> Result<String, GitHubError> {
        use sha2::{Digest, Sha256};
        if self.repository_id == 0
            || encoded_branch_segment(&self.branch).is_none()
            || self.rules.len() > 1000
            || self.rules.iter().any(|rule| {
                rule.get("type")
                    .and_then(|v| v.as_str())
                    .is_none_or(str::is_empty)
            })
        {
            return Err(GitHubError::UnstableObservation);
        }
        let body = serde_json::to_vec(self).map_err(|_| GitHubError::UnstableObservation)?;
        if body.len() > 1024 * 1024 {
            return Err(GitHubError::UnstableObservation);
        }
        let mut hash = Sha256::new();
        hash.update(b"sluss-branch-rules-v1\0");
        hash.update(body);
        Ok(hex::encode(hash.finalize()))
    }
}
impl App {
    /// Inspect legacy branch protection and each active ruleset's visible
    /// bypass list using a repository-scoped Administration-read token. This
    /// is a sampled diagnostic, not a proof that a bypass is safe or exclusive.
    pub async fn observe_branch_security(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        branch: &str,
    ) -> Result<BranchSecurityObservation, GitHubError> {
        if !token.runner_inventory_read || !valid_name(owner) || !valid_name(repository) {
            return Err(GitHubError::InvalidConfiguration);
        }
        let first = self
            .fetch_branch_security(token, owner, repository, branch)
            .await?;
        let second = self
            .fetch_branch_security(token, owner, repository, branch)
            .await?;
        if first != second {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(second)
    }

    async fn fetch_branch_security(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        branch: &str,
    ) -> Result<BranchSecurityObservation, GitHubError> {
        let first_branch = self
            .observe_target_branch(token, owner, repository, branch)
            .await?;
        let rules = self
            .observe_branch_rules(token, owner, repository, branch)
            .await?;
        let segment = encoded_branch_segment(branch).ok_or(GitHubError::InvalidConfiguration)?;
        let root = format!("repos/{owner}/{repository}");
        let protection = match self
            .get_json::<serde_json::Value>(
                token,
                &format!("{root}/branches/{segment}/protection"),
                None,
            )
            .await
        {
            Ok(value) if value.is_object() => ProtectionObservation::Present(value),
            Ok(_) => return Err(GitHubError::UnstableObservation),
            Err(GitHubError::Status(status @ (403 | 404))) => {
                ProtectionObservation::Unavailable(status)
            }
            Err(error) => return Err(error),
        };
        let mut ids = std::collections::BTreeSet::new();
        for rule in &rules.rules {
            let id = rule
                .get("ruleset_id")
                .and_then(|v| v.as_u64())
                .filter(|id| *id > 0)
                .ok_or(GitHubError::UnstableObservation)?;
            ids.insert(id);
        }
        if ids.len() > 100 {
            return Err(GitHubError::UnstableObservation);
        }
        let mut rulesets = Vec::new();
        let mut total = 0usize;
        for id in ids {
            let detail: serde_json::Value = self
                .get_json(token, &format!("{root}/rulesets/{id}"), None)
                .await?;
            total = total.saturating_add(
                serde_json::to_vec(&detail)
                    .map_err(|_| GitHubError::UnstableObservation)?
                    .len(),
            );
            let active_rules = detail
                .get("rules")
                .and_then(|v| v.as_array())
                .ok_or(GitHubError::UnstableObservation)?;
            if total > 1024 * 1024
                || detail.get("id").and_then(|v| v.as_u64()) != Some(id)
                || detail.get("target").and_then(|v| v.as_str()) != Some("branch")
                || detail.get("enforcement").and_then(|v| v.as_str()) != Some("active")
                || rules
                    .rules
                    .iter()
                    .filter(|rule| rule.get("ruleset_id").and_then(|v| v.as_u64()) == Some(id))
                    .any(|rule| {
                        let source = rule
                            .get("ruleset_source")
                            .and_then(|v| v.as_str())
                            .filter(|value| !value.is_empty());
                        let detail_source = detail
                            .get("source")
                            .and_then(|v| v.as_str())
                            .filter(|value| !value.is_empty());
                        let source_type = rule
                            .get("ruleset_source_type")
                            .and_then(|v| v.as_str())
                            .filter(|value| !value.is_empty());
                        let detail_type = detail
                            .get("source_type")
                            .and_then(|v| v.as_str())
                            .filter(|value| !value.is_empty());
                        source
                            .zip(detail_source)
                            .is_none_or(|(left, right)| !left.eq_ignore_ascii_case(right))
                            || source_type.is_none()
                            || source_type != detail_type
                            || !active_rules.iter().any(|entry| {
                                entry.get("type") == rule.get("type")
                                    && entry.get("parameters") == rule.get("parameters")
                            })
                    })
            {
                return Err(GitHubError::UnstableObservation);
            }
            let bypass_visible = match detail.get("bypass_actors") {
                None | Some(serde_json::Value::Null) => false,
                Some(serde_json::Value::Array(actors)) => {
                    if actors.iter().any(|actor| !valid_bypass_actor(actor)) {
                        return Err(GitHubError::UnstableObservation);
                    }
                    true
                }
                Some(_) => return Err(GitHubError::UnstableObservation),
            };
            rulesets.push(RulesetSecurityObservation {
                id,
                detail,
                bypass_visible,
            });
        }
        let last_branch = self
            .observe_target_branch(token, owner, repository, branch)
            .await?;
        if first_branch != last_branch {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(BranchSecurityObservation {
            branch: last_branch,
            rules,
            protection,
            rulesets,
        })
    }
    /// Compare two paginated reads of active rules, independently of rule
    /// order. This does not inspect bypass actors or authorize a merge.
    pub async fn observe_branch_rules(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        branch: &str,
    ) -> Result<BranchRulesSnapshot, GitHubError> {
        if !valid_name(owner) || !valid_name(repository) {
            return Err(GitHubError::InvalidConfiguration);
        }
        let segment = encoded_branch_segment(branch).ok_or(GitHubError::InvalidConfiguration)?;
        let root = format!("repos/{owner}/{repository}");
        let repo: Repository = self.get_json(token, &root, None).await?;
        if repo.id != token.repository_id {
            return Err(GitHubError::UnstableObservation);
        }
        let path = format!("{root}/rules/branches/{segment}");
        let mut first = self.fetch_branch_rules(token, &path).await?;
        let mut second = self.fetch_branch_rules(token, &path).await?;
        first.sort_by_cached_key(|rule| rule.to_string());
        second.sort_by_cached_key(|rule| rule.to_string());
        if first != second {
            return Err(GitHubError::UnstableObservation);
        }
        let value = BranchRulesSnapshot {
            repository_id: repo.id,
            branch: branch.into(),
            rules: second,
        };
        value.digest()?;
        Ok(value)
    }
    /// Read a target branch within the installation's repository scope.
    /// Callers must compare observations around the rest of a provider scan.
    pub async fn observe_target_branch(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        branch: &str,
    ) -> Result<BranchInfo, GitHubError> {
        if !valid_name(owner) || !valid_name(repository) {
            return Err(GitHubError::InvalidConfiguration);
        }
        let segment = encoded_branch_segment(branch).ok_or(GitHubError::InvalidConfiguration)?;
        let root = format!("repos/{owner}/{repository}");
        let repo: Repository = self.get_json(token, &root, None).await?;
        let info: BranchInfo = self
            .get_json(token, &format!("{root}/branches/{segment}"), None)
            .await?;
        if repo.id != token.repository_id || info.name != branch || !commit_sha(&info.commit.sha) {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(info)
    }

    /// Read the registered workflow twice before batch dispatch. This does not
    /// establish workflow contents, immutable configuration or execution trust.
    pub async fn observe_registered_workflow(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        workflow_id: u64,
    ) -> Result<WorkflowInfo, GitHubError> {
        if !valid_name(owner) || !valid_name(repository) || workflow_id == 0 {
            return Err(GitHubError::InvalidConfiguration);
        }
        let path = format!("repos/{owner}/{repository}/actions/workflows/{workflow_id}");
        let first: WorkflowInfo = self.get_json(token, &path, None).await?;
        let last: WorkflowInfo = self.get_json(token, &path, None).await?;
        if first != last
            || last.id != workflow_id
            || last.state != "active"
            || !valid_workflow_path(&last.path)
        {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(last)
    }

    /// Verify the active workflow's file is a regular blob at a stable default
    /// branch commit and declares compatible dispatch inputs. This does not prove trust.
    pub async fn observe_default_workflow(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        workflow: &WorkflowInfo,
    ) -> Result<DefaultWorkflowPresence, GitHubError> {
        if !valid_name(owner)
            || !valid_name(repository)
            || workflow.state != "active"
            || workflow.id == 0
            || !valid_workflow_path(&workflow.path)
            || !(token.checks_read || token.actions_write)
        {
            return Err(GitHubError::InvalidConfiguration);
        }
        let root = format!("repos/{owner}/{repository}");
        let repo1: DefaultRepository = self.get_json(token, &root, None).await?;
        if repo1.id != token.repository_id {
            return Err(GitHubError::UnstableObservation);
        }
        let branch1 = self
            .observe_target_branch(token, owner, repository, &repo1.default_branch)
            .await?;
        let tree_sha = self
            .workflow_blob_at_commit(
                token,
                owner,
                repository,
                &branch1.commit.sha,
                &workflow.path,
            )
            .await?;
        self.verify_dispatch_trigger(token, owner, repository, &tree_sha)
            .await?;
        let branch2 = self
            .observe_target_branch(token, owner, repository, &repo1.default_branch)
            .await?;
        let repo2: DefaultRepository = self.get_json(token, &root, None).await?;
        if repo1 != repo2 || branch1 != branch2 {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(DefaultWorkflowPresence {
            branch: repo1.default_branch,
            commit_sha: branch1.commit.sha,
            blob_sha: tree_sha,
        })
    }

    async fn workflow_blob_at_commit(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        commit: &str,
        path: &str,
    ) -> Result<String, GitHubError> {
        #[derive(Deserialize)]
        struct Tree {
            sha: String,
            truncated: bool,
            tree: Vec<Entry>,
        }
        #[derive(Deserialize)]
        struct Entry {
            path: String,
            mode: String,
            #[serde(rename = "type")]
            kind: String,
            sha: String,
        }
        if !valid_name(owner)
            || !valid_name(repository)
            || !commit_sha(commit)
            || !valid_workflow_path(path)
        {
            return Err(GitHubError::InvalidConfiguration);
        }
        let root = format!("repos/{owner}/{repository}");
        let filename = path
            .strip_prefix(".github/workflows/")
            .ok_or(GitHubError::InvalidConfiguration)?;
        let mut tree_sha = commit.to_owned();
        for (component, kind, modes) in [
            (".github", "tree", &["040000"][..]),
            ("workflows", "tree", &["040000"][..]),
            (filename, "blob", &["100644", "100755"][..]),
        ] {
            let tree: Tree = self
                .get_json(token, &format!("{root}/git/trees/{tree_sha}"), None)
                .await?;
            if tree.sha != tree_sha || tree.truncated || tree.tree.len() > 10000 {
                return Err(GitHubError::UnstableObservation);
            }
            let mut entries = tree.tree.iter().filter(|entry| entry.path == component);
            let entry = entries.next().ok_or(GitHubError::MissingDefaultWorkflow)?;
            if entries.next().is_some()
                || entry.kind != kind
                || !modes.contains(&entry.mode.as_str())
                || !commit_sha(&entry.sha)
            {
                return Err(GitHubError::MissingDefaultWorkflow);
            }
            tree_sha = entry.sha.clone();
        }
        Ok(tree_sha)
    }

    async fn verify_dispatch_trigger(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        expected_sha: &str,
    ) -> Result<(), GitHubError> {
        use base64::Engine;
        use sha1::{Digest, Sha1};
        #[derive(Deserialize)]
        struct Blob {
            sha: String,
            size: usize,
            encoding: String,
            content: String,
        }
        if !commit_sha(expected_sha) {
            return Err(GitHubError::InvalidConfiguration);
        }
        let blob: Blob = self
            .get_json(
                token,
                &format!("repos/{owner}/{repository}/git/blobs/{expected_sha}"),
                None,
            )
            .await?;
        if blob.sha != expected_sha
            || blob.encoding != "base64"
            || blob.size == 0
            || blob.size > 256 * 1024
        {
            return Err(GitHubError::MissingWorkflowDispatch);
        }
        let encoded: String = blob
            .content
            .chars()
            .filter(|character| *character != '\n' && *character != '\r')
            .collect();
        let contents = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| GitHubError::MissingWorkflowDispatch)?;
        if contents.len() != blob.size {
            return Err(GitHubError::MissingWorkflowDispatch);
        }
        let mut hash = Sha1::new();
        hash.update(format!("blob {}\0", contents.len()).as_bytes());
        hash.update(&contents);
        if hex::encode(hash.finalize()) != expected_sha
            || !crate::workflow::supports_sluss_dispatch(&contents)
                .map_err(|_| GitHubError::MissingWorkflowDispatch)?
        {
            return Err(GitHubError::MissingWorkflowDispatch);
        }
        Ok(())
    }

    async fn recheck_default_workflow(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        workflow: &WorkflowInfo,
        expected: &DefaultWorkflowPresence,
    ) -> Result<(), GitHubError> {
        let registered = self
            .observe_registered_workflow(token, owner, repository, workflow.id)
            .await?;
        if registered != *workflow {
            return Err(GitHubError::UnstableObservation);
        }
        let root = format!("repos/{owner}/{repository}");
        let first: DefaultRepository = self.get_json(token, &root, None).await?;
        let branch = self
            .observe_target_branch(token, owner, repository, &expected.branch)
            .await?;
        let last: DefaultRepository = self.get_json(token, &root, None).await?;
        if first != last
            || first.id != token.repository_id
            || first.default_branch != expected.branch
            || branch.commit.sha != expected.commit_sha
        {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(())
    }

    /// Check the provider-visible workflow needed for CI-only dispatch. These
    /// repeated reads do not establish workflow trust or authorize landing.
    pub async fn inspect_ci_repository(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        branch: &str,
        workflow_id: u64,
    ) -> Result<CiRepositoryPreflight, GitHubError> {
        let first = self
            .observe_target_branch(token, owner, repository, branch)
            .await?;
        let workflow = self
            .observe_registered_workflow(token, owner, repository, workflow_id)
            .await?;
        let default_workflow = self
            .observe_default_workflow(token, owner, repository, &workflow)
            .await?;
        let target_blob = self
            .workflow_blob_at_commit(token, owner, repository, &first.commit.sha, &workflow.path)
            .await?;
        self.verify_dispatch_trigger(token, owner, repository, &target_blob)
            .await?;
        self.recheck_default_workflow(token, owner, repository, &workflow, &default_workflow)
            .await?;
        let last = self
            .observe_target_branch(token, owner, repository, branch)
            .await?;
        if first != last {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(CiRepositoryPreflight {
            branch: last,
            workflow,
            default_workflow,
        })
    }

    /// Inspect current branch, workflow registration and active branch rules.
    /// Agreement across reads is an observation, not a landing authorization.
    pub async fn inspect_repository(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        branch: &str,
        workflow_id: u64,
    ) -> Result<RepositoryPreflight, GitHubError> {
        if !valid_name(owner) || !valid_name(repository) || workflow_id == 0 {
            return Err(GitHubError::InvalidConfiguration);
        }
        let branch_segment =
            encoded_branch_segment(branch).ok_or(GitHubError::InvalidConfiguration)?;
        let root = format!("repos/{owner}/{repository}");
        let repo: Repository = self.get_json(token, &root, None).await?;
        if repo.id != token.repository_id {
            return Err(GitHubError::UnstableObservation);
        }
        let branch_path = format!("{root}/branches/{branch_segment}");
        let workflow_path = format!("{root}/actions/workflows/{workflow_id}");
        let rules_path = format!("{root}/rules/branches/{branch_segment}");
        let branch1: BranchInfo = self.get_json(token, &branch_path, None).await?;
        let workflow1: WorkflowInfo = self.get_json(token, &workflow_path, None).await?;
        let rules1 = self.fetch_branch_rules(token, &rules_path).await?;
        let branch2: BranchInfo = self.get_json(token, &branch_path, None).await?;
        let workflow2: WorkflowInfo = self.get_json(token, &workflow_path, None).await?;
        let rules2 = self.fetch_branch_rules(token, &rules_path).await?;
        if branch1 != branch2
            || workflow1 != workflow2
            || rules1 != rules2
            || branch2.name != branch
            || !commit_sha(&branch2.commit.sha)
            || workflow2.id != workflow_id
            || !valid_workflow_path(&workflow2.path)
            || workflow2.state != "active"
        {
            return Err(GitHubError::UnstableObservation);
        }
        let default_workflow = self
            .observe_default_workflow(token, owner, repository, &workflow2)
            .await?;
        let target_blob = self
            .workflow_blob_at_commit(
                token,
                owner,
                repository,
                &branch2.commit.sha,
                &workflow2.path,
            )
            .await?;
        self.verify_dispatch_trigger(token, owner, repository, &target_blob)
            .await?;
        self.recheck_default_workflow(token, owner, repository, &workflow2, &default_workflow)
            .await?;
        Ok(RepositoryPreflight {
            branch: branch2,
            workflow: workflow2,
            default_workflow,
            rules: rules2,
        })
    }
    async fn fetch_branch_rules(
        &self,
        token: &InstallationToken,
        path: &str,
    ) -> Result<Vec<serde_json::Value>, GitHubError> {
        let mut rules = Vec::new();
        for page in 1..=100u32 {
            let rows: Vec<serde_json::Value> = self.get_json(token, path, Some(page)).await?;
            if rows.len() > 100
                || rows.iter().any(|r| {
                    r.get("type")
                        .and_then(|x| x.as_str())
                        .is_none_or(str::is_empty)
                })
            {
                return Err(GitHubError::UnstableObservation);
            }
            let done = rows.len() < 100;
            rules.extend(rows);
            if rules.len() > 1000 {
                return Err(GitHubError::UnstableObservation);
            }
            if done {
                return Ok(rules);
            }
        }
        Err(GitHubError::UnstableObservation)
    }
}
fn encoded_branch_segment(branch: &str) -> Option<String> {
    if branch.is_empty()
        || branch.len() > 255
        || branch == "@"
        || branch.ends_with('.')
        || branch.contains("..")
        || !branch
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'_' | b'-'))
        || branch
            .split('/')
            .any(|part| part.is_empty() || part.starts_with('.') || part.ends_with(".lock"))
    {
        return None;
    }
    let mut url = reqwest::Url::parse("https://example.invalid/").ok()?;
    url.path_segments_mut().ok()?.push(branch);
    Some(url.path().trim_start_matches('/').to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StaleCancellation {
    Requested,
    AlreadyRequested,
    Completed,
    /// The provider may have received the cancellation; do not blindly retry.
    Uncertain,
}
impl App {
    /// Cancel only a known run belonging to a terminally invalidated batch.
    /// A cancellation request does not free the physical-host reservation.
    pub async fn cancel_stale_run(
        &self,
        scheduler: &mut crate::scheduler::Scheduler,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        id: &str,
    ) -> Result<StaleCancellation, DispatchError> {
        self.cancel_stale_run_inner(scheduler, token, owner, repository, id, None)
            .await
    }
    /// Request cancellation for either recorded run in a dispatch conflict.
    /// This never releases capacity and never retries an uncertain request.
    pub async fn cancel_conflicting_run(
        &self,
        scheduler: &mut crate::scheduler::Scheduler,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        id: &str,
        run_id: u64,
    ) -> Result<StaleCancellation, DispatchError> {
        self.cancel_stale_run_inner(scheduler, token, owner, repository, id, Some(run_id))
            .await
    }
    async fn cancel_stale_run_inner(
        &self,
        scheduler: &mut crate::scheduler::Scheduler,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
        id: &str,
        conflicting_run: Option<u64>,
    ) -> Result<StaleCancellation, DispatchError> {
        use crate::scheduler::CancellationClaim;
        if !token.actions_write || !valid_name(owner) || !valid_name(repository) {
            return Err(DispatchError::Invalid);
        }
        let stale = if let Some(run_id) = conflicting_run {
            scheduler.conflicting_stale_run(id, run_id)?
        } else {
            scheduler.stale_run(id)?
        };
        if stale.plan.installation_id != token.installation_id
            || stale.plan.repository_id != token.repository_id
        {
            return Err(DispatchError::Invalid);
        }
        let path = format!("repos/{owner}/{repository}/actions/runs/{}", stale.run_id);
        let run: ActionsRun = self.get_json(token, &path, None).await?;
        let branch = stale
            .plan
            .staging_ref
            .strip_prefix("refs/heads/")
            .ok_or(DispatchError::Invalid)?;
        if scheduler
            .dispatch_workflow(id)?
            .as_deref()
            .is_some_and(|path| {
                !workflow_path_matches(
                    path,
                    &run.path,
                    &stale.plan.staging_ref,
                    &stale.plan.integration_sha,
                )
            })
            || run.id != stale.run_id
            || run.run_attempt == 0
            || run.repository.id != stale.plan.repository_id
            || run.head_repository.as_ref().map(|r| r.id) != Some(stale.plan.repository_id)
            || run.workflow_id != stale.plan.workflow_id
            || run.head_sha != stale.plan.integration_sha
            || run.head_branch.as_deref() != Some(branch)
            || run.event != "workflow_dispatch"
        {
            return Err(DispatchError::RunMismatch);
        }
        if run.status == "completed" {
            return Ok(StaleCancellation::Completed);
        }
        if !matches!(
            run.status.as_str(),
            "queued" | "in_progress" | "waiting" | "requested" | "pending"
        ) {
            return Ok(StaleCancellation::Uncertain);
        }
        self.check_rate_limit().await?;
        let claim = if let Some(run_id) = conflicting_run {
            scheduler.claim_conflict_cancellation(id, run_id)?
        } else {
            scheduler.claim_stale_cancellation(id)?
        };
        match claim {
            CancellationClaim::AlreadyRequested => return Ok(StaleCancellation::AlreadyRequested),
            CancellationClaim::Uncertain => return Ok(StaleCancellation::Uncertain),
            CancellationClaim::New(claimed) if claimed == stale => {}
            CancellationClaim::New(_) => return Err(DispatchError::RunMismatch),
        }
        let url = self
            .api
            .join(&format!("{path}/cancel"))
            .map_err(|_| DispatchError::Uncertain)?;
        let response = self
            .send(
                self.client
                    .post(url)
                    .header("Accept", "application/vnd.github+json")
                    .header("X-GitHub-Api-Version", "2022-11-28")
                    .bearer_auth(token.secret()),
            )
            .await;
        match response {
            Ok(response) if response.status() == reqwest::StatusCode::ACCEPTED => {
                if conflicting_run.is_some() {
                    scheduler.record_conflict_cancellation(id, stale.run_id)?;
                } else {
                    scheduler.record_stale_cancellation(id, stale.run_id)?;
                }
                Ok(StaleCancellation::Requested)
            }
            _ => Ok(StaleCancellation::Uncertain),
        }
    }
}

#[cfg(test)]
mod rate_limit_tests {
    use super::{App, GitHubError, RateLimitGate, rate_limit_delay};
    use reqwest::{
        StatusCode,
        header::{HeaderMap, HeaderValue, RETRY_AFTER},
    };
    use std::time::Duration;

    #[test]
    fn honors_retry_and_primary_reset_without_treating_ordinary_forbidden_as_rate_limited() {
        let mut headers = HeaderMap::new();
        assert_eq!(
            rate_limit_delay(StatusCode::FORBIDDEN, &headers, false, 1_000),
            None
        );
        assert_eq!(
            rate_limit_delay(StatusCode::FORBIDDEN, &headers, true, 1_000),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            rate_limit_delay(StatusCode::TOO_MANY_REQUESTS, &headers, false, 1_000),
            Some(Duration::from_secs(60))
        );
        headers.insert(RETRY_AFTER, HeaderValue::from_static("10"));
        assert_eq!(
            rate_limit_delay(StatusCode::FORBIDDEN, &headers, false, 1_000),
            Some(Duration::from_secs(10))
        );
        headers.insert("x-ratelimit-remaining", HeaderValue::from_static("0"));
        headers.insert("x-ratelimit-reset", HeaderValue::from_static("1040"));
        assert_eq!(
            rate_limit_delay(StatusCode::FORBIDDEN, &headers, false, 1_000),
            Some(Duration::from_secs(40))
        );
        headers.insert(
            RETRY_AFTER,
            HeaderValue::from_static("999999999999999999999999999"),
        );
        assert_eq!(
            rate_limit_delay(StatusCode::TOO_MANY_REQUESTS, &headers, false, 1_000),
            Some(Duration::from_secs(u64::MAX))
        );
    }

    #[tokio::test]
    async fn shared_gate_prevents_git_work_until_the_saved_deadline_clears() {
        use std::sync::{
            Arc, Mutex, OnceLock,
            atomic::{AtomicBool, Ordering},
        };
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("queue.sqlite");
        let _inbox = crate::webhook::Inbox::open(&db).unwrap();
        let app = || App {
            client_id: "test".into(),
            key: jsonwebtoken::EncodingKey::from_secret(b"unused"),
            client: reqwest::Client::new(),
            api: reqwest::Url::parse("http://127.0.0.1/").unwrap(),
            rate_limit: Mutex::new(RateLimitGate::Open),
            shared_rate_limit: OnceLock::new(),
        };
        let first = app();
        let second = app();
        first.enable_shared_rate_limits(&db).unwrap();
        second.enable_shared_rate_limits(&db).unwrap();
        first.defer_requests(Duration::from_secs(60)).await.unwrap();
        let executed = Arc::new(AtomicBool::new(false));
        let flag = executed.clone();
        assert!(matches!(
            second
                .spawn_github_git(move || flag.store(true, Ordering::SeqCst))
                .await,
            Err(GitHubError::RateLimited)
        ));
        assert!(!executed.load(Ordering::SeqCst));
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute(
            "UPDATE github_rate_limit_gate SET observed_ms=0,until_ms=0",
            [],
        )
        .unwrap();
        let flag = executed.clone();
        second
            .spawn_github_git(move || flag.store(true, Ordering::SeqCst))
            .await
            .unwrap()
            .unwrap();
        assert!(executed.load(Ordering::SeqCst));
    }
}

#[cfg(test)]
mod pull_cache_tests {
    use super::{PullReadCache, valid_entity_tag};
    use reqwest::header::HeaderValue;

    #[test]
    fn accepts_only_single_entity_tags_and_bounds_cached_bodies() {
        for tag in ["\"abc\"", "W/\"abc\"", "\"\""] {
            assert!(valid_entity_tag(tag));
        }
        for tag in ["*", "abc", "\"abc\", \"def\"", "w/\"abc\"", "\"bad space\""] {
            assert!(!valid_entity_tag(tag));
        }
        let mut cache = PullReadCache::default();
        cache.replace(
            "url".into(),
            Some(HeaderValue::from_static("\"a\"")),
            vec![1, 2],
        );
        assert_eq!(cache.bytes, 2);
        cache.replace("url".into(), Some(HeaderValue::from_static("*")), vec![3]);
        assert_eq!(cache.bytes, 0);
        assert!(cache.entries.is_empty());
        cache.replace(
            "big".into(),
            Some(HeaderValue::from_static("\"a\"")),
            vec![0; 8 * 1024 * 1024 + 1],
        );
        assert!(cache.entries.is_empty());
    }
}

#[cfg(test)]
mod workflow_path_tests {
    use super::workflow_path_matches;

    #[test]
    fn workflow_suffix_must_match_the_frozen_branch_or_commit() {
        let file = ".github/workflows/ci.yml";
        let branch = "refs/heads/sluss/batch-123";
        let sha = "a".repeat(40);
        for path in [
            file.to_string(),
            format!("{file}@{branch}"),
            format!("{file}@sluss/batch-123"),
            format!("{file}@{sha}"),
        ] {
            assert!(workflow_path_matches(file, &path, branch, &sha), "{path}");
        }
        for path in [
            format!("{file}@refs/heads/main"),
            format!("{file}@main"),
            format!("{file}@refs/tags/sluss/batch-123"),
            format!("{file}@{}", "b".repeat(40)),
            format!("{file}@"),
            format!("{file}@{branch}@extra"),
            format!(".github/workflows/other.yml@{branch}"),
        ] {
            assert!(!workflow_path_matches(file, &path, branch, &sha), "{path}");
        }
        assert!(!workflow_path_matches(
            file,
            file,
            "refs/tags/not-a-branch",
            &sha
        ));
    }
}

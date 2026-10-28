//! GitHub App authentication. No token or private key is persisted or logged.
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
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
    #[error("GitHub returned an invalid or unexpectedly scoped installation token")]
    InvalidResponse,
    #[error("GitHub PR or review data is invalid, incomplete, or changed during observation")]
    UnstableObservation,
}

pub struct App {
    client_id: String,
    key: EncodingKey,
    client: reqwest::Client,
    api: reqwest::Url,
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
                .client
                .get(url)
                .header("Accept", "application/vnd.github+json")
                .header("X-GitHub-Api-Version", "2022-11-28")
                .bearer_auth(jwt)
                .send()
                .await
                .map_err(|_| GitHubError::Request)?;
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
            .client
            .post(url)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .bearer_auth(token.secret())
            .json(&serde_json::json!({"query": QUERY}))
            .send()
            .await
            .map_err(|_| GitHubError::Request)?;
        if !response.status().is_success() {
            return Err(GitHubError::Status(response.status().as_u16()));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| GitHubError::Request)? {
            if bytes.len().saturating_add(chunk.len()) > 1024 * 1024 {
                return Err(GitHubError::UnstableObservation);
            }
            bytes.extend_from_slice(&chunk);
        }
        let result: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| GitHubError::UnstableObservation)?;
        if result
            .get("errors")
            .is_some_and(|errors| !errors.as_array().is_some_and(|v| v.is_empty()))
        {
            return Err(GitHubError::UnstableObservation);
        }
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
        })
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
    /// A separate repository-scoped Checks-write credential. It is used by
    /// the experimental failure-only publisher; it never issues success.
    pub async fn failed_check_token(
        &self,
        installation_id: u64,
        repository_id: u64,
    ) -> Result<InstallationToken, GitHubError> {
        self.mint_token(installation_id, repository_id, TokenPurpose::FailedCheck)
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
            .client
            .post(url)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .bearer_auth(jwt)
            .json(&body)
            .send()
            .await
            .map_err(|_| GitHubError::Request)?;
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
            || response.permissions.iter().any(|(k, v)| {
                v != "read" && body.permissions.get(k.as_str()).copied() != Some(v.as_str())
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
        if !valid_name(owner) || !valid_name(repository) || number == 0 {
            return Err(GitHubError::InvalidConfiguration);
        }
        let path = format!("repos/{owner}/{repository}/pulls/{number}");
        let first: PullObservation = self.get_json(token, &path, None).await?;
        let reviews1 = self.get_reviews(token, &path).await?;
        let middle: PullObservation = self.get_json(token, &path, None).await?;
        let reviews2 = self.get_reviews(token, &path).await?;
        let last: PullObservation = self.get_json(token, &path, None).await?;
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
    ) -> Result<Vec<ReviewObservation>, GitHubError> {
        let mut result = Vec::new();
        for page in 1..=100u32 {
            let rows: Vec<ReviewObservation> = self
                .get_json(token, &format!("{path}/reviews"), Some(page))
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
        let mut response = self
            .client
            .get(url)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .bearer_auth(token.secret())
            .send()
            .await
            .map_err(|_| GitHubError::Request)?;
        if !response.status().is_success() {
            return Err(GitHubError::Status(response.status().as_u16()));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| GitHubError::Request)? {
            if bytes.len().saturating_add(chunk.len()) > 1024 * 1024 {
                return Err(GitHubError::UnstableObservation);
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| GitHubError::UnstableObservation)
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
        let first = self.observe_pull(token, owner, repository, number).await?;
        let mut reviewers = BTreeMap::new();
        for review in &first.reviews {
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
        let last = self.observe_pull(token, owner, repository, number).await?;
        if first != last {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(AuthorizedPullObservation {
            snapshot: last,
            authorized_reviewers: authorized,
            reviewer_permissions,
        })
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
}
impl App {
    /// Enumerate open pull requests twice. A changed or truncated listing is not usable.
    pub async fn open_pull_numbers(
        &self,
        token: &InstallationToken,
        owner: &str,
        repository: &str,
    ) -> Result<Vec<u64>, GitHubError> {
        if !valid_name(owner) || !valid_name(repository) {
            return Err(GitHubError::InvalidConfiguration);
        }
        let first = self.fetch_open_pulls(token, owner, repository).await?;
        let second = self.fetch_open_pulls(token, owner, repository).await?;
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
    ) -> Result<Vec<u64>, GitHubError> {
        let path = format!("repos/{owner}/{repository}/pulls");
        let mut result = Vec::new();
        let mut unique = std::collections::BTreeSet::new();
        for page in 1..=100u32 {
            // GitHub defaults to state=open for this endpoint.
            let pulls: Vec<PullNumber> = self.get_json(token, &path, Some(page)).await?;
            if pulls.len() > 100 {
                return Err(GitHubError::UnstableObservation);
            }
            let done = pulls.len() < 100;
            for pull in pulls {
                if pull.number == 0 || !unique.insert(pull.number) {
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
#[derive(Deserialize)]
struct ActionsJobsPage {
    total_count: usize,
    jobs: Vec<ActionsJob>,
}
impl App {
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
#[derive(Deserialize)]
struct DispatchRef {
    #[serde(rename = "ref")]
    reference: String,
    object: DispatchObject,
}
#[derive(Deserialize)]
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
        let claimed = if let Some((target, base)) = target_base {
            let workflow = self
                .observe_registered_workflow(token, owner, repository, pending.plan.workflow_id)
                .await?;
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
            if let Some(security) = security {
                scheduler.claim_batch_dispatch_with_security(id, &workflow.path, &security)?
            } else if let Some(rules) = rules {
                scheduler.claim_batch_dispatch_with_rules(id, &workflow.path, &rules)?
            } else {
                scheduler.claim_batch_dispatch(id, &workflow.path)?
            }
        } else {
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
        let mut response = self
            .client
            .post(url)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2026-03-10")
            .bearer_auth(token.secret())
            .json(&serde_json::json!({"ref": pending.plan.staging_ref, "return_run_details": true, "inputs": {"sluss_host": host_label, "sluss_suites": pending.plan.suites.iter().cloned().collect::<Vec<_>>().join(",")}}))
            .send()
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
            .is_some_and(|path| !workflow_path_matches(path, &snapshot.run.path))
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
            .is_some_and(|path| !workflow_path_matches(path, &snapshot.run.path))
        {
            return Err(DispatchError::RunMismatch);
        }
        scheduler.observe_dispatch(id, run_id)?;
        Ok(snapshot)
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
        operation_id: &str,
    ) -> Result<Option<crate::landing::LandingPlan>, LandingRefreshError> {
        #[derive(Deserialize, PartialEq, Eq)]
        struct Identity {
            id: u64,
            node_id: String,
        }
        let owner = &policy.owner;
        let repository = &policy.name;
        if !valid_name(owner)
            || !valid_name(repository)
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
        let target = journal.refresh_target(operation_id)?;
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
        if !matches!(crate::verification::assess_visible_branch_policy(&security,app_identity.id),Ok(findings) if findings.is_empty())
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
        for member in &target.members {
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
        }
        let run_id = target.run_id;
        let run_path = format!("{root}/actions/runs/{run_id}");
        let first: ActionsRun = self.get_json(token, &run_path, None).await?;
        let snapshot = self
            .observe_actions_attempt(token, owner, repository, run_id, 1)
            .await?;
        let last: ActionsRun = self.get_json(token, &run_path, None).await?;
        let last_stage: DispatchRef = self.get_json(token, &ref_path, None).await?;
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
        journal
            .claim_with_fresh_snapshot(operation_id, &snapshot)
            .map_err(Into::into)
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
            .client
            .post(url)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .bearer_auth(write.secret())
            .json(&body)
            .send()
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
        tokio::task::spawn_blocking(move || {
            crate::integration::fetch_github(&owner, &repository, &target, &batch, &secret)
        })
        .await
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
        tokio::task::spawn_blocking(move || {
            integration.stage_github(&owner, &repository, &secret, &id)
        })
        .await
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
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct RepositoryRunner {
    pub id: u64,
    pub name: String,
    pub status: String,
    pub busy: bool,
    pub labels: Vec<RunnerLabel>,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
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

/// GitHub may append an @ref to the workflow path in a run response. The
/// SHA, repository and head branch are checked separately by the caller.
pub(crate) fn workflow_path_matches(expected: &str, reported: &str) -> bool {
    match reported.split_once('@') {
        Some((path, reference)) => path == expected && !reference.is_empty(),
        None => reported == expected,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct WorkflowInfo {
    pub id: u64,
    pub path: String,
    pub state: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryPreflight {
    pub branch: BranchInfo,
    pub workflow: WorkflowInfo,
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
            || !workflow2.path.starts_with(".github/workflows/")
            || workflow2.path.is_empty()
            || workflow2.state.is_empty()
        {
            return Err(GitHubError::UnstableObservation);
        }
        Ok(RepositoryPreflight {
            branch: branch2,
            workflow: workflow2,
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
        use crate::scheduler::CancellationClaim;
        if !token.actions_write || !valid_name(owner) || !valid_name(repository) {
            return Err(DispatchError::Invalid);
        }
        let stale = scheduler.stale_run(id)?;
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
            .is_some_and(|path| !workflow_path_matches(path, &run.path))
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
        match scheduler.claim_stale_cancellation(id)? {
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
            .client
            .post(url)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .bearer_auth(token.secret())
            .send()
            .await;
        match response {
            Ok(response) if response.status() == reqwest::StatusCode::ACCEPTED => {
                scheduler.record_stale_cancellation(id, stale.run_id)?;
                Ok(StaleCancellation::Requested)
            }
            _ => Ok(StaleCancellation::Uncertain),
        }
    }
}

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
/// The token is deliberately not printable or serializable.
pub struct InstallationToken {
    secret: String,
    pub expires_at: String,
    repository_id: u64,
    installation_id: u64,
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
        if installation_id == 0 || repository_id == 0 {
            return Err(GitHubError::InvalidConfiguration);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| GitHubError::Clock)?
            .as_secs();
        let jwt = self.jwt(now)?;
        let body = RequestBody {
            repository_ids: [repository_id],
            permissions: BTreeMap::from([
                ("actions", "read"),
                ("checks", "read"),
                ("contents", "read"),
                ("pull_requests", "read"),
            ]),
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
            || response.permissions.values().any(|v| v != "read")
            || body
                .permissions
                .keys()
                .any(|k| response.permissions.get(*k).map(String::as_str) != Some("read"))
        {
            return Err(GitHubError::InvalidResponse);
        }
        Ok(InstallationToken {
            secret: response.token,
            expires_at: response.expires_at,
            repository_id,
            installation_id,
        })
    }
}

/// An observation is not an admission decision: reviewer permissions, branch
/// protection and concurrent provider changes must still be validated.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PullObservation {
    pub number: u64,
    pub state: String,
    pub draft: bool,
    pub user: PullUser,
    pub head: PullHead,
    pub base: PullBase,
    pub labels: Vec<PullLabel>,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PullUser {
    pub id: u64,
    pub login: String,
    #[serde(rename = "type")]
    pub kind: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PullHead {
    pub sha: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PullBase {
    pub sha: String,
    #[serde(rename = "ref")]
    pub branch: String,
    pub repo: PullRepository,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PullRepository {
    pub id: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PullLabel {
    pub name: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ReviewObservation {
    pub id: u64,
    pub user: Option<PullUser>,
    pub state: String,
    pub commit_id: Option<String>,
    pub submitted_at: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
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
fn valid_name(value: &str) -> bool {
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
        for (id, login) in &reviewers {
            let current = self
                .permission(token, owner, repository, *id, login)
                .await?;
            if first_permissions.get(id) != Some(&current) {
                return Err(GitHubError::UnstableObservation);
            }
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

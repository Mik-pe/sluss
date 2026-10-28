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
    actions_write: bool,
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
        self.mint_token(installation_id, repository_id, false).await
    }
    /// Separate, repository-scoped credential for explicit workflow dispatch.
    pub async fn dispatch_token(
        &self,
        installation_id: u64,
        repository_id: u64,
    ) -> Result<InstallationToken, GitHubError> {
        self.mint_token(installation_id, repository_id, true).await
    }
    async fn mint_token(
        &self,
        installation_id: u64,
        repository_id: u64,
        actions_write: bool,
    ) -> Result<InstallationToken, GitHubError> {
        if installation_id == 0 || repository_id == 0 {
            return Err(GitHubError::InvalidConfiguration);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| GitHubError::Clock)?
            .as_secs();
        let jwt = self.jwt(now)?;
        let permissions = if actions_write {
            BTreeMap::from([("actions", "write"), ("contents", "read")])
        } else {
            BTreeMap::from([
                ("actions", "read"),
                ("checks", "read"),
                ("contents", "read"),
                ("pull_requests", "read"),
            ])
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
            || response
                .permissions
                .iter()
                .any(|(k, v)| v != "read" && !(actions_write && k == "actions" && v == "write"))
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
            actions_write,
            app_client_id: self.client_id.clone(),
            api_origin: self.api.as_str().to_owned(),
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

/// Provider data only. This is not an attestation or evidence that workflow
/// code, runner environment, or a later merge is trusted.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ActionsRepository {
    pub id: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
        use crate::scheduler::DispatchState;
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
        if !scheduler.claim_dispatch(id)? {
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
            .json(&serde_json::json!({"ref": pending.plan.staging_ref}))
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
        scheduler.observe_dispatch(id, run_id)?;
        Ok(snapshot)
    }
}

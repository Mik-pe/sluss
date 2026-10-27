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
        })
    }
}

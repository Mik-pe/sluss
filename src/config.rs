//! Operator-owned configuration for the experimental observation worker.
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    pub github: GitHub,
    pub queue: Queue,
    pub checks: Checks,
    #[serde(default)]
    pub merge: Merge,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitHub {
    pub app_client_id: String,
    pub installation_id: i64,
    pub repository_id: i64,
    pub owner: String,
    pub repository: String,
    #[serde(default = "default_api")]
    pub api: String,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Queue {
    pub target: String,
    #[serde(default = "default_poll")]
    pub poll_seconds: u64,
    #[serde(default = "default_rescan")]
    pub rescan_seconds: u64,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checks {
    pub required_reviews: usize,
    #[serde(default)]
    pub block_labels: Vec<String>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Merge {
    pub mode: String,
}
impl Default for Merge {
    fn default() -> Self {
        Self {
            mode: "disabled".into(),
        }
    }
}
fn default_api() -> String {
    "https://api.github.com/".into()
}
fn default_poll() -> u64 {
    5
}
fn default_rescan() -> u64 {
    300
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not read configuration")]
    Read,
    #[error("invalid or unsupported configuration")]
    Invalid,
}
impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self, ConfigError> {
        let bytes = std::fs::read(path).map_err(|_| ConfigError::Read)?;
        if bytes.len() > 65536 {
            return Err(ConfigError::Invalid);
        }
        let text = std::str::from_utf8(&bytes).map_err(|_| ConfigError::Invalid)?;
        let config: Self = toml::from_str(text).map_err(|_| ConfigError::Invalid)?;
        if config.version != 1
            || config.github.app_client_id.is_empty()
            || config.github.installation_id <= 0
            || config.github.repository_id <= 0
            || !crate::github::valid_name(&config.github.owner)
            || !crate::github::valid_name(&config.github.repository)
            || config.queue.target.is_empty()
            || !(1..=100).contains(&config.checks.required_reviews)
            || config.checks.block_labels.iter().any(String::is_empty)
            || !(1..=60).contains(&config.queue.poll_seconds)
            || !(30..=86400).contains(&config.queue.rescan_seconds)
            || config.queue.poll_seconds > config.queue.rescan_seconds
            || config.merge.mode != "disabled"
        {
            return Err(ConfigError::Invalid);
        }
        Ok(config)
    }
    pub fn policy(&self) -> crate::reconcile::RepositoryPolicy {
        crate::reconcile::RepositoryPolicy {
            installation: self.github.installation_id,
            repository: self.github.repository_id,
            owner: self.github.owner.clone(),
            name: self.github.repository.clone(),
            target_branch: self.queue.target.clone(),
            required_reviews: self.checks.required_reviews,
            block_labels: self.checks.block_labels.iter().cloned().collect(),
        }
    }
}

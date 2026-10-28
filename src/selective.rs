//! Conservative selection from an operator-owned verification policy.
//! Selection is not CI evidence and does not authorize landing.
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    io::Read,
    path::Path,
    process::{Command, Stdio},
};
use thiserror::Error;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub version: u32,
    pub full_suites: Vec<String>,
    #[serde(default)]
    pub always_full: Vec<String>,
    pub components: Vec<Component>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Component {
    pub name: String,
    pub paths: Vec<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub suites: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Selection {
    Full {
        suites: BTreeSet<String>,
        reason: String,
    },
    Selected {
        suites: BTreeSet<String>,
        components: BTreeSet<String>,
    },
}
#[derive(Debug, Error)]
pub enum SelectionError {
    #[error("invalid verification policy")]
    InvalidPolicy,
    #[error("invalid or unrelated Git commits")]
    InvalidCommit,
    #[error("could not read Git changes")]
    Git,
    #[error("Git change list is too large or not valid UTF-8")]
    ChangeList,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenSelection {
    pub base_sha: String,
    pub integration_sha: String,
    pub policy_digest: String,
    pub selection: Selection,
}
impl Selection {
    pub fn suites(&self) -> &BTreeSet<String> {
        match self {
            Self::Full { suites, .. } | Self::Selected { suites, .. } => suites,
        }
    }
}
impl Policy {
    /// Hash the parsed operator policy, independent of TOML formatting.
    pub fn digest(&self) -> Result<String, SelectionError> {
        use sha2::Digest;
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| SelectionError::InvalidPolicy)?;
        let mut hash = sha2::Sha256::new();
        // Bump the domain when selection semantics change so old frozen
        // decisions cannot be dispatched under a newly parsed policy.
        hash.update(b"sluss-selective-policy-v2\0");
        hash.update(bytes);
        Ok(hex::encode(hash.finalize()))
    }
    pub fn load(path: &Path) -> Result<Self, SelectionError> {
        let bytes = std::fs::read(path)?;
        if bytes.len() > 65536 {
            return Err(SelectionError::InvalidPolicy);
        }
        let policy: Self =
            toml::from_str(std::str::from_utf8(&bytes).map_err(|_| SelectionError::InvalidPolicy)?)
                .map_err(|_| SelectionError::InvalidPolicy)?;
        policy.validate()?;
        Ok(policy)
    }
    pub fn validate(&self) -> Result<(), SelectionError> {
        let valid_name = |name: &str| {
            !name.is_empty()
                && name.len() <= 100
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        };
        if self.version != 1
            || self.full_suites.is_empty()
            || self.full_suites.len() > 100
            || self.components.is_empty()
            || self.components.len() > 1000
            || self.always_full.len() > 1000
            || self.always_full.iter().any(|p| !valid_rule(p))
        {
            return Err(SelectionError::InvalidPolicy);
        }
        let suites: BTreeSet<_> = self.full_suites.iter().collect();
        if suites.len() != self.full_suites.len() || suites.iter().any(|name| !valid_name(name)) {
            return Err(SelectionError::InvalidPolicy);
        }
        let names: BTreeSet<_> = self.components.iter().map(|c| &c.name).collect();
        if names.len() != self.components.len() || names.iter().any(|name| !valid_name(name)) {
            return Err(SelectionError::InvalidPolicy);
        }
        for component in &self.components {
            if component.paths.is_empty()
                || component.paths.len() > 1000
                || component.paths.iter().any(|p| !valid_rule(p))
                || component.depends_on.iter().any(|dep| !names.contains(dep))
                || component.suites.iter().any(|suite| !suites.contains(suite))
            {
                return Err(SelectionError::InvalidPolicy);
            }
        }
        Ok(())
    }
    /// Read a frozen Git diff; unrepresentable or oversized path lists select
    /// every suite. Git errors and unrelated commits still fail the operation.
    pub fn select_git(
        &self,
        repo: &Path,
        base: &str,
        head: &str,
    ) -> Result<Selection, SelectionError> {
        self.validate()?;
        match changed_paths(repo, base, head) {
            Ok(changes) => self.select(&changes),
            Err(SelectionError::ChangeList) => Ok(Selection::Full {
                suites: self.full_suites.iter().cloned().collect(),
                reason: "change list could not be classified".into(),
            }),
            Err(error) => Err(error),
        }
    }
    pub fn select(&self, changes: &[String]) -> Result<Selection, SelectionError> {
        self.validate()?;
        let full = || self.full_suites.iter().cloned().collect::<BTreeSet<_>>();
        if changes.is_empty() {
            return Ok(Selection::Full {
                suites: full(),
                reason: "no changed paths were available".into(),
            });
        }
        let mut affected = BTreeSet::new();
        for path in changes {
            if !valid_path(path) {
                return Ok(Selection::Full {
                    suites: full(),
                    reason: "invalid changed path".into(),
                });
            }
            if critical_path(path) || self.always_full.iter().any(|rule| matches_rule(rule, path)) {
                return Ok(Selection::Full {
                    suites: full(),
                    reason: format!("policy-sensitive path: {path}"),
                });
            }
            let matches: Vec<_> = self
                .components
                .iter()
                .filter(|component| component.paths.iter().any(|rule| matches_rule(rule, path)))
                .collect();
            if matches.is_empty() {
                return Ok(Selection::Full {
                    suites: full(),
                    reason: format!("unclassified path: {path}"),
                });
            }
            affected.extend(matches.into_iter().map(|component| component.name.clone()));
        }
        loop {
            let before = affected.len();
            for component in &self.components {
                if component
                    .depends_on
                    .iter()
                    .any(|dep| affected.contains(dep))
                {
                    affected.insert(component.name.clone());
                }
            }
            if affected.len() == before {
                break;
            }
        }
        if self
            .components
            .iter()
            .any(|component| affected.contains(&component.name) && component.suites.is_empty())
        {
            return Ok(Selection::Full {
                suites: full(),
                reason: "an affected component has no configured suites".into(),
            });
        }
        let suites: BTreeSet<String> = self
            .components
            .iter()
            .filter(|component| affected.contains(&component.name))
            .flat_map(|component| component.suites.iter().cloned())
            .collect();
        if suites.is_empty() {
            return Ok(Selection::Full {
                suites: full(),
                reason: "affected components have no configured suites".into(),
            });
        }
        Ok(Selection::Selected {
            suites,
            components: affected,
        })
    }
}
fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 4096
        && !path.starts_with('/')
        && !path.contains('\\')
        && !path.chars().any(char::is_control)
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != ".." && part != ".git")
}
fn valid_rule(rule: &str) -> bool {
    let path = rule.strip_suffix('/').unwrap_or(rule);
    valid_path(path) && !path.contains('*') && !path.contains('?') && !path.contains('[')
}
fn matches_rule(rule: &str, path: &str) -> bool {
    if rule.ends_with('/') {
        path.starts_with(rule)
    } else {
        path == rule
    }
}
fn critical_path(path: &str) -> bool {
    path.starts_with(".github/")
        || matches!(
            path,
            ".gitmodules" | ".gitattributes" | "sluss.toml" | "Cargo.toml" | "Cargo.lock"
        )
        || path.ends_with("/Cargo.toml")
        || path.ends_with("/Cargo.lock")
        || path.ends_with("/.gitattributes")
        || path == ".cargo/config"
        || path == ".cargo/config.toml"
        || path.ends_with("/.cargo/config")
        || path.ends_with("/.cargo/config.toml")
        || matches!(path, "rust-toolchain" | "rust-toolchain.toml")
        || path.ends_with("/rust-toolchain")
        || path.ends_with("/rust-toolchain.toml")
}
fn git(repo: &Path) -> Command {
    let mut cmd = Command::new("git");
    cmd.env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", repo)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_NO_REPLACE_OBJECTS", "1")
        .env("LC_ALL", "C")
        .arg("-C")
        .arg(repo)
        .args(["-c", "core.hooksPath=/dev/null"]);
    cmd
}
/// List additions and deletions separately so renames classify both paths.
/// Both objects must be commits and the base must be an ancestor of the head.
pub fn changed_paths(repo: &Path, base: &str, head: &str) -> Result<Vec<String>, SelectionError> {
    let valid_sha = |sha: &str| {
        sha.len() == 40
            && sha
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    };
    if !valid_sha(base) || !valid_sha(head) {
        return Err(SelectionError::InvalidCommit);
    }
    let repo = repo.canonicalize()?;
    for sha in [base, head] {
        let object = git(&repo).args(["cat-file", "-t", sha]).output()?;
        if !object.status.success() || object.stdout != b"commit\n" {
            return Err(SelectionError::InvalidCommit);
        }
    }
    let ancestry = git(&repo)
        .args(["merge-base", "--is-ancestor", base, head])
        .status()?;
    if !ancestry.success() {
        return Err(SelectionError::InvalidCommit);
    }
    let mut child = git(&repo)
        .args([
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "--ignore-submodules=none",
            "--name-only",
            "-z",
            base,
            head,
            "--",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut output = Vec::new();
    let reader = child.stdout.take().ok_or(SelectionError::Git)?;
    reader.take(4 * 1024 * 1024 + 1).read_to_end(&mut output)?;
    if output.len() > 4 * 1024 * 1024 {
        let _ = child.kill();
        let _ = child.wait();
        return Err(SelectionError::ChangeList);
    }
    if !child.wait()?.success() {
        return Err(SelectionError::Git);
    }
    if output.is_empty() {
        return Ok(Vec::new());
    }
    if !output.ends_with(&[0]) {
        return Err(SelectionError::ChangeList);
    }
    let mut paths = Vec::new();
    for name in output[..output.len() - 1].split(|b| *b == 0) {
        paths.push(
            std::str::from_utf8(name)
                .map_err(|_| SelectionError::ChangeList)?
                .to_owned(),
        );
    }
    Ok(paths)
}

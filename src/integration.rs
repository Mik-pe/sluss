//! Local, isolated construction of deterministic merge commits.
//! This module does not push a ref, start CI, or authorize a merge.
use std::{
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};
use tempfile::TempDir;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IntegrationError {
    #[error("inputs must be nonempty, full lowercase SHA-1 commit IDs")]
    InvalidInput,
    #[error("source path is not an accessible local repository")]
    InvalidSource,
    #[error("git failed while {0}")]
    Git(&'static str),
    #[error("PR head is already contained in the integration history")]
    AlreadyIntegrated,
    #[error("git could not cleanly merge a PR head")]
    Conflict,
    #[error("git returned an invalid commit ID")]
    InvalidGitOutput,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Owns the disposable clone until the caller has consumed the result.
/// Dropping it deletes only the workspace created for this build.
pub struct Integration {
    workspace: TempDir,
    sha: String,
}
impl Integration {
    pub fn sha(&self) -> &str {
        &self.sha
    }
    pub fn repository(&self) -> PathBuf {
        self.workspace.path().join("repo")
    }
}

fn valid_sha(sha: &str) -> bool {
    sha.len() == 40
        && sha
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// Build one merge commit per PR head. Input ordering and all generated commit
/// metadata are fixed. Reproducibility additionally depends on Git's version
/// and merge behavior; consumers must pin and attest their Git environment.
/// The source must already contain the specified commit objects.
pub fn build(source: &Path, base: &str, heads: &[String]) -> Result<Integration, IntegrationError> {
    if !valid_sha(base) || heads.is_empty() || heads.iter().any(|sha| !valid_sha(sha)) {
        return Err(IntegrationError::InvalidInput);
    }
    let source = source
        .canonicalize()
        .map_err(|_| IntegrationError::InvalidSource)?;
    if !source.is_dir() {
        return Err(IntegrationError::InvalidSource);
    }
    let workspace = tempfile::Builder::new()
        .prefix("sluss-integration-")
        .tempdir()?;
    let repo = workspace.path().join("repo");
    let template = workspace.path().join("empty-template");
    std::fs::create_dir(&template)?;
    let mut clone = command(workspace.path());
    clone
        .arg("-c")
        .arg("protocol.file.allow=always")
        .arg("clone")
        .arg("--no-local")
        .arg("--no-checkout")
        .arg("--template")
        .arg(&template)
        .arg("--")
        .arg(&source)
        .arg(&repo);
    ensure(clone.output()?, "cloning the source")?;
    for sha in std::iter::once(base).chain(heads.iter().map(String::as_str)) {
        // Fetch by object ID from this local clone source. There is no network access.
        let output = git(&repo)
            .args([
                "-c",
                "protocol.file.allow=always",
                "fetch",
                "--no-tags",
                "--no-write-fetch-head",
                "--no-recurse-submodules",
                "origin",
                sha,
            ])
            .output()?;
        ensure(output, "fetching a frozen commit")?;
        let object = ensure(
            git(&repo).args(["cat-file", "-t", sha]).output()?,
            "checking a frozen commit",
        )?;
        if object.stdout != b"commit\n" {
            return Err(IntegrationError::InvalidInput);
        }
    }
    let mut current = base.to_owned();
    for head in heads {
        let ancestor = git(&repo)
            .args(["merge-base", "--is-ancestor", head, &current])
            .output()?;
        match ancestor.status.code() {
            Some(0) => return Err(IntegrationError::AlreadyIntegrated),
            Some(1) => {}
            _ => return Err(IntegrationError::Git("checking ancestry")),
        }
        let merged = git(&repo)
            .args(["merge-tree", "--write-tree", &current, head])
            .output()?;
        if !merged.status.success() {
            return if merged.status.code() == Some(1) {
                Err(IntegrationError::Conflict)
            } else {
                Err(IntegrationError::Git("merging trees"))
            };
        }
        let tree =
            String::from_utf8(merged.stdout).map_err(|_| IntegrationError::InvalidGitOutput)?;
        let tree = tree.trim();
        if !valid_sha(tree) {
            return Err(IntegrationError::InvalidGitOutput);
        }
        let mut child = git(&repo)
            .args([
                "-c",
                "commit.gpgsign=false",
                "commit-tree",
                tree,
                "-p",
                &current,
                "-p",
                head,
            ])
            .env("GIT_AUTHOR_NAME", "Sluss")
            .env("GIT_AUTHOR_EMAIL", "sluss@localhost")
            .env("GIT_COMMITTER_NAME", "Sluss")
            .env("GIT_COMMITTER_EMAIL", "sluss@localhost")
            .env("GIT_AUTHOR_DATE", "2000-01-01T00:00:00+00:00")
            .env("GIT_COMMITTER_DATE", "2000-01-01T00:00:00+00:00")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .ok_or(IntegrationError::Git("writing commit message"))?
            .write_all(b"Sluss integration\n")?;
        let commit = ensure(child.wait_with_output()?, "creating merge commit")?;
        current = String::from_utf8(commit.stdout)
            .map_err(|_| IntegrationError::InvalidGitOutput)?
            .trim()
            .to_owned();
        if !valid_sha(&current) {
            return Err(IntegrationError::InvalidGitOutput);
        }
    }
    Ok(Integration {
        workspace,
        sha: current,
    })
}

fn command(dir: &Path) -> Command {
    let mut cmd = Command::new("git");
    cmd.env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", dir)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_NO_REPLACE_OBJECTS", "1")
        .env("LC_ALL", "C");
    cmd
}
fn git(repo: &Path) -> Command {
    let mut cmd = command(repo);
    cmd.arg("-C")
        .arg(repo)
        .arg("-c")
        .arg("core.hooksPath=/dev/null");
    cmd
}
fn ensure(output: Output, context: &'static str) -> Result<Output, IntegrationError> {
    if output.status.success() {
        Ok(output)
    } else {
        Err(IntegrationError::Git(context))
    }
}

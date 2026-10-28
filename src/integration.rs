//! Local, isolated construction of deterministic merge commits.
//! Staging publication is explicit; this module does not start CI or authorize a merge.
use std::{
    io::{Read, Write},
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

fn valid_staging_id(id: &str) -> bool {
    (16..=64).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// A conservative check of push- and create-triggered Actions in the integration commit.
/// Other events, organization runners, external automation and workflow trust
/// remain independent operator responsibilities.
#[derive(Debug, Error)]
pub enum StagingWorkflowError {
    #[error("invalid staging identifier")]
    Invalid,
    #[error("could not safely inspect the integration workflow tree")]
    Tree,
    #[error("workflow {0} could run on the staging push; restrict its branch filter")]
    PushTrigger(String),
    #[error("workflow {0} could run when the staging branch is created")]
    CreateTrigger(String),
    #[error("workflow {0} has unsupported or ambiguous trigger syntax")]
    Ambiguous(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

fn bounded_git(mut cmd: Command, limit: u64) -> Result<Vec<u8>, StagingWorkflowError> {
    cmd.stdin(Stdio::null())
        .stderr(Stdio::null())
        .stdout(Stdio::piped());
    let mut child = cmd.spawn()?;
    let mut output = Vec::new();
    let stdout = child.stdout.take().ok_or(StagingWorkflowError::Tree)?;
    if let Err(error) = stdout.take(limit + 1).read_to_end(&mut output) {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error.into());
    }
    if output.len() as u64 > limit {
        let _ = child.kill();
        let _ = child.wait();
        return Err(StagingWorkflowError::Tree);
    }
    if !child.wait()?.success() {
        return Err(StagingWorkflowError::Tree);
    }
    Ok(output)
}

fn tree_entry(line: &[u8]) -> Result<(&str, &str, &str, &str), StagingWorkflowError> {
    let line = std::str::from_utf8(line).map_err(|_| StagingWorkflowError::Tree)?;
    let (metadata, name) = line.split_once('\t').ok_or(StagingWorkflowError::Tree)?;
    let mut parts = metadata.split(' ');
    let (Some(mode), Some(kind), Some(sha)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(StagingWorkflowError::Tree);
    };
    if parts.next().is_some()
        || !valid_sha(sha)
        || name.is_empty()
        || name.chars().any(char::is_control)
    {
        return Err(StagingWorkflowError::Tree);
    }
    Ok((mode, kind, sha, name))
}

impl Integration {
    /// Inspect every direct .yml/.yaml workflow file from this exact commit.
    /// Also rejects create triggers. Other events and external systems can still start CI.
    pub fn check_staging_workflows(&self, id: &str) -> Result<(), StagingWorkflowError> {
        self.check_staging_workflows_at(self.sha(), id)
    }

    /// Inspect a frozen input commit in the integration's isolated repository.
    pub(crate) fn check_staging_workflows_at(
        &self,
        commit: &str,
        id: &str,
    ) -> Result<(), StagingWorkflowError> {
        if !valid_staging_id(id) || !valid_sha(commit) {
            return Err(StagingWorkflowError::Invalid);
        }
        let branch = format!("sluss/{id}");
        let repo = self.repository();
        let mut kind = git(&repo);
        kind.args(["cat-file", "-t", commit]);
        if bounded_git(kind, 64)? != b"commit\n" {
            return Err(StagingWorkflowError::Tree);
        }
        let mut parent = git(&repo);
        parent.args([
            "--literal-pathspecs",
            "ls-tree",
            "-z",
            "--full-tree",
            commit,
            "--",
            ".github",
        ]);
        let parent = bounded_git(parent, 1024 * 1024)?;
        if parent.is_empty() {
            return Ok(());
        }
        let line = parent
            .strip_suffix(b"\0")
            .ok_or(StagingWorkflowError::Tree)?;
        let (mode, kind, _, path) = tree_entry(line)?;
        if path != ".github" {
            return Err(StagingWorkflowError::Tree);
        }
        if mode != "040000" || kind != "tree" {
            return Err(StagingWorkflowError::Ambiguous(".github".into()));
        }
        let mut root = git(&repo);
        root.args([
            "--literal-pathspecs",
            "ls-tree",
            "-z",
            "--full-tree",
            commit,
            "--",
            ".github/workflows",
        ]);
        let root = bounded_git(root, 1024 * 1024)?;
        if root.is_empty() {
            return Ok(());
        }
        let line = root.strip_suffix(b"\0").ok_or(StagingWorkflowError::Tree)?;
        let (mode, kind, tree, path) = tree_entry(line)?;
        if mode != "040000" || kind != "tree" || path != ".github/workflows" {
            return Err(StagingWorkflowError::Tree);
        }
        let mut listing = git(&repo);
        listing.args(["ls-tree", "-z", tree]);
        let listing = bounded_git(listing, 1024 * 1024)?;
        if !listing.is_empty() && !listing.ends_with(b"\0") {
            return Err(StagingWorkflowError::Tree);
        }
        if listing.is_empty() {
            return Ok(());
        }
        for (index, line) in listing[..listing.len() - 1].split(|b| *b == 0).enumerate() {
            if index >= 1000 {
                return Err(StagingWorkflowError::Tree);
            }
            let (mode, kind, sha, name) = tree_entry(line)?;
            if name.contains('/') {
                return Err(StagingWorkflowError::Tree);
            }
            let lower = name.to_ascii_lowercase();
            if !lower.ends_with(".yml") && !lower.ends_with(".yaml") {
                continue;
            }
            let path = format!(".github/workflows/{name}");
            if !matches!(mode, "100644" | "100755") || kind != "blob" {
                return Err(StagingWorkflowError::Ambiguous(path));
            }
            let mut size = git(&repo);
            size.args(["cat-file", "-s", sha]);
            let size = bounded_git(size, 64)?;
            let size = std::str::from_utf8(&size)
                .ok()
                .and_then(|v| v.trim().parse::<usize>().ok())
                .ok_or(StagingWorkflowError::Tree)?;
            if size > 256 * 1024 {
                return Err(StagingWorkflowError::Ambiguous(path));
            }
            let mut blob = git(&repo);
            blob.args(["cat-file", "blob", sha]);
            let blob = bounded_git(blob, 256 * 1024)?;
            if blob.len() != size {
                return Err(StagingWorkflowError::Tree);
            }
            match crate::workflow::inspect_staging_triggers(&blob, &branch) {
                Ok(triggers) if !triggers.create_excluded => {
                    return Err(StagingWorkflowError::CreateTrigger(path));
                }
                Ok(triggers) if !triggers.push_excluded => {
                    return Err(StagingWorkflowError::PushTrigger(path));
                }
                Ok(_) => {}
                Err(_) => return Err(StagingWorkflowError::Ambiguous(path)),
            }
        }
        Ok(())
    }
}

/// Result of publishing an integration to a temporary staging branch. This
/// identifies the commit only; it is not evidence of CI or merge permission.
#[derive(Debug, PartialEq, Eq)]
pub struct Staged {
    pub reference: String,
    pub sha: String,
}

#[derive(Debug, Error)]
pub enum StageError {
    #[error("invalid staging identifier or remote")]
    Invalid,
    #[error("staging ref already points to a different commit")]
    Conflict,
    #[error("staging publication failed or its result is uncertain")]
    Uncertain,
    #[error("authenticated Git staging is not supported on this platform")]
    Unsupported,
    #[error(transparent)]
    Workflow(#[from] StagingWorkflowError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Integration {
    /// Publish to an explicit local repository; intended for tests and local deployments.
    pub fn stage_local(&self, remote: &Path, id: &str) -> Result<Staged, StageError> {
        let remote = remote.canonicalize().map_err(|_| StageError::Invalid)?;
        if !remote.is_dir() {
            return Err(StageError::Invalid);
        }
        self.stage(remote.as_os_str(), id, None)
    }

    /// Publish to GitHub using an operator-supplied scoped token. This method
    /// does not obtain credentials and must not run in an untrusted CI job.
    #[cfg(unix)]
    pub fn stage_github(
        &self,
        owner: &str,
        repository: &str,
        token: &str,
        id: &str,
    ) -> Result<Staged, StageError> {
        if !crate::github::valid_name(owner)
            || !crate::github::valid_name(repository)
            || !valid_staging_id(id)
            || token.is_empty()
            || token.len() > 4096
            || !token.bytes().all(|b| (33..=126).contains(&b))
        {
            return Err(StageError::Invalid);
        }
        self.check_staging_workflows(id)?;
        let remote = format!("https://github.com/{owner}/{repository}.git");
        self.stage(std::ffi::OsStr::new(&remote), id, Some(token))
    }
    #[cfg(not(unix))]
    pub fn stage_github(
        &self,
        _owner: &str,
        _repository: &str,
        _token: &str,
        _id: &str,
    ) -> Result<Staged, StageError> {
        Err(StageError::Unsupported)
    }

    fn stage(
        &self,
        remote: &std::ffi::OsStr,
        id: &str,
        token: Option<&str>,
    ) -> Result<Staged, StageError> {
        if !valid_staging_id(id) {
            return Err(StageError::Invalid);
        }
        let reference = format!("refs/heads/sluss/{id}");
        let auth = auth_helper(self.workspace.path(), token)?;
        let read = || {
            remote_ref(
                &self.repository(),
                remote,
                &reference,
                token,
                auth.as_ref().map(|file| file.path()),
            )
        };
        match read()? {
            Some(actual) if actual == self.sha => {
                return Ok(Staged {
                    reference,
                    sha: self.sha.clone(),
                });
            }
            Some(_) => return Err(StageError::Conflict),
            None => {}
        }
        let lease = format!("--force-with-lease={reference}:");
        let spec = format!("{}:{reference}", self.sha);
        let mut cmd = git(&self.repository());
        cmd.args([
            "-c",
            "protocol.file.allow=always",
            "-c",
            "credential.helper=",
            "-c",
            "http.followRedirects=false",
            "push",
            "--no-verify",
            &lease,
            "--",
        ])
        .arg(remote)
        .arg(&spec);
        add_auth(&mut cmd, token, auth.as_ref().map(|file| file.path()));
        // A failed or interrupted push has an uncertain outcome until read back.
        let _push_result = run_git(cmd, false);
        match read() {
            Ok(Some(actual)) if actual == self.sha => Ok(Staged {
                reference,
                sha: self.sha.clone(),
            }),
            Ok(Some(_)) => Err(StageError::Conflict),
            _ => Err(StageError::Uncertain),
        }
    }
}

#[cfg(unix)]
fn auth_helper(
    dir: &Path,
    token: Option<&str>,
) -> Result<Option<tempfile::NamedTempFile>, StageError> {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    if token.is_none() {
        return Ok(None);
    }
    let mut file = tempfile::NamedTempFile::new_in(dir)?;
    file.as_file()
        .set_permissions(std::fs::Permissions::from_mode(0o700))?;
    file.write_all(b"#!/bin/sh\ncase \"$1\" in *sername*) printf '%s\\n' x-access-token;; *) printf '%s\\n' \"$SLUSS_GIT_TOKEN\";; esac\n")?;
    Ok(Some(file))
}
#[cfg(not(unix))]
fn auth_helper(
    _dir: &Path,
    token: Option<&str>,
) -> Result<Option<tempfile::NamedTempFile>, StageError> {
    if token.is_some() {
        Err(StageError::Unsupported)
    } else {
        Ok(None)
    }
}
fn add_auth(cmd: &mut Command, token: Option<&str>, helper: Option<&Path>) {
    if let (Some(token), Some(helper)) = (token, helper) {
        cmd.env("GIT_ASKPASS", helper).env("SLUSS_GIT_TOKEN", token);
    }
}
fn remote_ref(
    repo: &Path,
    remote: &std::ffi::OsStr,
    reference: &str,
    token: Option<&str>,
    helper: Option<&Path>,
) -> Result<Option<String>, StageError> {
    let mut cmd = git(repo);
    cmd.args([
        "-c",
        "protocol.file.allow=always",
        "-c",
        "credential.helper=",
        "-c",
        "http.followRedirects=false",
        "ls-remote",
        "--refs",
        "--",
    ])
    .arg(remote)
    .arg(reference);
    add_auth(&mut cmd, token, helper);
    let output = run_git(cmd, true)?;
    if !output.status.success() {
        return Err(StageError::Uncertain);
    }
    let text = std::str::from_utf8(&output.stdout).map_err(|_| StageError::Uncertain)?;
    if text.is_empty() {
        return Ok(None);
    }
    let mut lines = text.lines();
    let Some((sha, actual_ref)) = lines.next().and_then(|line| line.split_once('\t')) else {
        return Err(StageError::Uncertain);
    };
    if lines.next().is_some() || actual_ref != reference || !valid_sha(sha) {
        return Err(StageError::Uncertain);
    }
    Ok(Some(sha.into()))
}
/// Bound external Git calls and never surface stdout/stderr containing secrets.
fn run_git(mut cmd: Command, capture: bool) -> Result<Output, StageError> {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    cmd.stdin(Stdio::null())
        .stderr(Stdio::null())
        .stdout(if capture {
            Stdio::piped()
        } else {
            Stdio::null()
        });
    let mut child = cmd.spawn()?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if child.try_wait()?.is_some() {
            return child.wait_with_output().map_err(StageError::from);
        }
        if std::time::Instant::now() >= deadline {
            #[cfg(unix)]
            {
                // The child is the leader of a new process group. Terminate
                // Git helpers as well as the parent so credentials do not linger.
                unsafe {
                    libc::kill(-(child.id() as i32), libc::SIGKILL);
                }
            }
            let _ = child.kill();
            let _ = child.wait();
            return Err(StageError::Uncertain);
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// Digest of explicitly selected policy files, with their exact bytes and modes.
/// The caller remains responsible for selecting all relevant files, pinning
/// external actions, validating workflow behavior and protecting the base.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicySnapshot {
    pub digest: String,
    pub paths: Vec<String>,
}
#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("invalid base commit or policy paths")]
    Invalid,
    #[error("a declared CI policy file differs from the base")]
    Changed,
    #[error("a declared CI policy file in the base is missing, not regular, or too large")]
    BaseUnavailable,
    #[error("could not read a policy object")]
    Git,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
impl Integration {
    /// Verify declared policy files against a frozen base and hash the exact
    /// contents. This does not authenticate the base or validate the policy.
    pub fn check_policy(
        &self,
        base: &str,
        paths: &[String],
    ) -> Result<PolicySnapshot, PolicyError> {
        policy_snapshot(&self.repository(), base, self.sha(), paths)
    }
}
/// Hash declared policy files from one explicit commit in an operator-trusted
/// local repository. This does not authenticate the commit or review the files.
pub fn policy_snapshot_at_commit(
    repo: &Path,
    commit: &str,
    paths: &[String],
) -> Result<PolicySnapshot, PolicyError> {
    policy_snapshot(repo, commit, commit, paths)
}
fn policy_snapshot(
    repo: &Path,
    base: &str,
    staged: &str,
    paths: &[String],
) -> Result<PolicySnapshot, PolicyError> {
    use sha2::{Digest, Sha256};
    if !valid_sha(base) || !valid_sha(staged) || paths.is_empty() || paths.len() > 128 {
        return Err(PolicyError::Invalid);
    }
    let mut paths = paths.to_vec();
    paths.sort();
    if paths.windows(2).any(|pair| pair[0] == pair[1])
        || paths.iter().any(|p| !valid_policy_path(p))
    {
        return Err(PolicyError::Invalid);
    }
    let kind = git(repo).args(["cat-file", "-t", base]).output()?;
    if !kind.status.success() || kind.stdout != b"commit\n" {
        return Err(PolicyError::Invalid);
    }
    // Validate the entire base first. A missing or oversized base policy is
    // an operator configuration problem, not evidence against any PR.
    let mut base_entries = Vec::with_capacity(paths.len());
    for path in &paths {
        let (mode, object) = policy_entry(repo, base, path).map_err(|error| match error {
            PolicyError::Changed => PolicyError::BaseUnavailable,
            other => other,
        })?;
        let size = git(repo).args(["cat-file", "-s", &object]).output()?;
        if !size.status.success() {
            return Err(PolicyError::Git);
        }
        let size: usize = std::str::from_utf8(&size.stdout)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .ok_or(PolicyError::Git)?;
        if size > 1024 * 1024 {
            return Err(PolicyError::BaseUnavailable);
        }
        base_entries.push((mode, object, size));
    }
    let mut hasher = Sha256::new();
    hasher.update(b"sluss-policy-files-v1\0");
    for (path, (mode, object, size)) in paths.iter().zip(base_entries) {
        let (staged_mode, staged_object) = policy_entry(repo, staged, path)?;
        if mode != staged_mode || object != staged_object {
            return Err(PolicyError::Changed);
        }
        let content = git(repo).args(["cat-file", "blob", &object]).output()?;
        if !content.status.success() || content.stdout.len() != size {
            return Err(PolicyError::Git);
        }
        hasher.update((path.len() as u64).to_be_bytes());
        hasher.update(path.as_bytes());
        hasher.update(mode.as_bytes());
        hasher.update((size as u64).to_be_bytes());
        hasher.update(content.stdout);
    }
    Ok(PolicySnapshot {
        digest: hex::encode(hasher.finalize()),
        paths,
    })
}
/// Check a repository-relative CI policy path before preparing work.
pub fn valid_policy_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 512
        && path
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'_' | b'-'))
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != ".." && part != ".git")
}
fn policy_entry(repo: &Path, commit: &str, path: &str) -> Result<(String, String), PolicyError> {
    let output = git(repo)
        .args([
            "--literal-pathspecs",
            "ls-tree",
            "-z",
            "--full-tree",
            commit,
            "--",
            path,
        ])
        .output()?;
    if !output.status.success() {
        return Err(PolicyError::Git);
    }
    let Some(line) = output.stdout.strip_suffix(b"\0") else {
        return Err(PolicyError::Changed);
    };
    let line = std::str::from_utf8(line).map_err(|_| PolicyError::Changed)?;
    let (info, actual_path) = line.split_once('\t').ok_or(PolicyError::Changed)?;
    let mut words = info.split(' ');
    let (Some(mode), Some(kind), Some(object)) = (words.next(), words.next(), words.next()) else {
        return Err(PolicyError::Changed);
    };
    if actual_path != path
        || words.next().is_some()
        || !matches!(mode, "100644" | "100755")
        || kind != "blob"
        || !valid_sha(object)
    {
        return Err(PolicyError::Changed);
    }
    Ok((mode.into(), object.into()))
}

/// A disposable, isolated repository containing provider-observed input refs.
/// No Git credentials are stored in this repository.
pub struct FetchedInputs {
    workspace: TempDir,
}
impl FetchedInputs {
    pub fn repository(&self) -> PathBuf {
        self.workspace.path().join("inputs.git")
    }
}

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("invalid repository, ref, or frozen input")]
    Invalid,
    #[error("provider refs no longer match the frozen batch")]
    Stale,
    #[error("fetch failed or could not be verified")]
    Fetch,
    #[error(transparent)]
    Git(#[from] StageError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Read frozen refs from a local bare remote, for local deployments and tests.
pub fn fetch_local(
    remote: &Path,
    target: &str,
    batch: &crate::batch::Batch,
) -> Result<FetchedInputs, FetchError> {
    let remote = remote.canonicalize().map_err(|_| FetchError::Invalid)?;
    if !remote.is_dir() {
        return Err(FetchError::Invalid);
    }
    fetch_inputs(remote.as_os_str(), target, batch, None)
}

/// The GitHub remote is fixed to github.com, and secrets are supplied only to
/// a short-lived Git process through its environment and a temporary askpass.
#[cfg(unix)]
pub(crate) fn fetch_github(
    owner: &str,
    repository: &str,
    target: &str,
    batch: &crate::batch::Batch,
    token: &str,
) -> Result<FetchedInputs, FetchError> {
    if !crate::github::valid_name(owner)
        || !crate::github::valid_name(repository)
        || token.is_empty()
        || token.len() > 4096
        || !token.bytes().all(|b| (33..=126).contains(&b))
    {
        return Err(FetchError::Invalid);
    }
    let remote = format!("https://github.com/{owner}/{repository}.git");
    fetch_inputs(std::ffi::OsStr::new(&remote), target, batch, Some(token))
}
#[cfg(not(unix))]
pub(crate) fn fetch_github(
    _owner: &str,
    _repository: &str,
    _target: &str,
    _batch: &crate::batch::Batch,
    _token: &str,
) -> Result<FetchedInputs, FetchError> {
    Err(FetchError::Git(StageError::Unsupported))
}

fn fetch_inputs(
    remote: &std::ffi::OsStr,
    target: &str,
    batch: &crate::batch::Batch,
    token: Option<&str>,
) -> Result<FetchedInputs, FetchError> {
    use std::collections::BTreeSet;
    if !valid_sha(&batch.base_sha)
        || batch.candidates.is_empty()
        || batch.candidates.len() > 100
        || batch
            .candidates
            .iter()
            .any(|c| c.number == 0 || !valid_sha(&c.head_sha))
        || batch
            .candidates
            .iter()
            .map(|c| c.number)
            .collect::<BTreeSet<_>>()
            .len()
            != batch.candidates.len()
        || target.is_empty()
        || target.len() > 255
    {
        return Err(FetchError::Invalid);
    }
    let workspace = tempfile::Builder::new().prefix("sluss-inputs-").tempdir()?;
    let repo = workspace.path().join("inputs.git");
    let template = workspace.path().join("empty-template");
    std::fs::create_dir(&template)?;
    let mut init = command(workspace.path());
    init.arg("init")
        .arg("--bare")
        .arg("--template")
        .arg(&template)
        .arg(&repo);
    if !run_git(init, false)?.status.success() {
        return Err(FetchError::Fetch);
    }
    let branch = format!("refs/heads/{target}");
    let mut check = git(&repo);
    check.args(["check-ref-format", &branch]);
    if !run_git(check, false)?.status.success() {
        return Err(FetchError::Invalid);
    }
    let helper = auth_helper(workspace.path(), token)?;
    let mut fetch = git(&repo);
    fetch
        .args([
            "-c",
            "protocol.file.allow=always",
            "-c",
            "credential.helper=",
            "-c",
            "http.followRedirects=false",
            "fetch",
            "--no-tags",
            "--no-write-fetch-head",
            "--no-recurse-submodules",
            "--",
        ])
        .arg(remote)
        .arg(format!("{branch}:refs/sluss/inputs/base"));
    for candidate in &batch.candidates {
        fetch.arg(format!(
            "refs/pull/{}/head:refs/sluss/inputs/pr-{}",
            candidate.number, candidate.number
        ));
    }
    add_auth(&mut fetch, token, helper.as_ref().map(|file| file.path()));
    if !run_git(fetch, false)?.status.success() {
        return Err(FetchError::Fetch);
    }
    let verify = |reference: &str, expected: &str| -> Result<(), FetchError> {
        let mut read = git(&repo);
        read.args(["rev-parse", "--verify", reference]);
        let output = run_git(read, true)?;
        if !output.status.success() {
            return Err(FetchError::Fetch);
        }
        let sha = std::str::from_utf8(&output.stdout)
            .map_err(|_| FetchError::Fetch)?
            .trim();
        if sha != expected {
            return Err(FetchError::Stale);
        }
        let mut kind = git(&repo);
        kind.args(["cat-file", "-t", sha]);
        let output = run_git(kind, true)?;
        if !output.status.success() || output.stdout != b"commit\n" {
            return Err(FetchError::Invalid);
        }
        Ok(())
    };
    verify("refs/sluss/inputs/base", &batch.base_sha)?;
    for candidate in &batch.candidates {
        verify(
            &format!("refs/sluss/inputs/pr-{}", candidate.number),
            &candidate.head_sha,
        )?;
    }
    Ok(FetchedInputs { workspace })
}

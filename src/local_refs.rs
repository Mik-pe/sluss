//! Experimental local Git reference transaction.
//!
//! This is a low-level mechanism for a trusted local bare repository. It does
//! not check CI, reviews, GitHub rules or authorize a merge. It is not wired to
//! the service or to GitHub, and no ambiguous outcome may be treated as proof
//! that this process made the update.
use std::{
    io::{BufRead, BufReader, Read, Write},
    path::Path,
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};
use thiserror::Error;

const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LocalRefError {
    #[error("invalid repository, refs, or commit IDs")]
    Invalid,
    #[error("the target commit is not a strict fast-forward from the expected base")]
    NotFastForward,
    #[error("the target or staging ref is symbolic")]
    SymbolicRef,
    #[error("the expected references could not be locked and verified; no commit was sent")]
    Precondition,
    #[error("the update outcome is uncertain; reconcile before retrying")]
    Uncertain,
}

fn valid_sha(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && value.bytes().any(|b| b != b'0')
}
fn safe_ref(value: &str) -> bool {
    value.len() <= 255
        && value.starts_with("refs/heads/")
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'_' | b'-'))
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
        .env("GIT_NO_LAZY_FETCH", "1")
        .env("GIT_ALLOW_PROTOCOL", "")
        .env("GIT_COMMITTER_NAME", "Sluss")
        .env("GIT_COMMITTER_EMAIL", "sluss@localhost")
        .env("LC_ALL", "C")
        .arg("-C")
        .arg(repo)
        .args(["-c", "core.hooksPath=/dev/null"]);
    cmd
}

/// Ensure no owned Git process survives an error or a timeout.
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(Some(_))) {
            return;
        }
        #[cfg(unix)]
        unsafe {
            libc::kill(-(self.0.id() as i32), libc::SIGKILL);
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn configure(cmd: &mut Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    cmd.stderr(Stdio::null());
}
fn wait(child: &mut Child) -> Result<std::process::ExitStatus, LocalRefError> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().map_err(|_| LocalRefError::Precondition)? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(LocalRefError::Precondition);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn run(mut cmd: Command) -> Result<Output, LocalRefError> {
    configure(&mut cmd);
    cmd.stdin(Stdio::null()).stdout(Stdio::piped());
    let mut child = Process(cmd.spawn().map_err(|_| LocalRefError::Precondition)?);
    let status = wait(&mut child.0)?;
    // The commands here produce only bounded status output, never object data.
    let mut stdout = Vec::new();
    child
        .0
        .stdout
        .take()
        .ok_or(LocalRefError::Precondition)?
        .read_to_end(&mut stdout)
        .map_err(|_| LocalRefError::Precondition)?;
    Ok(Output {
        status,
        stdout,
        stderr: Vec::new(),
    })
}
fn reject_symlinks(repo: &Path, reference: &str) -> Result<(), LocalRefError> {
    let mut path = repo.to_owned();
    for component in reference.split('/') {
        path.push(component);
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(LocalRefError::SymbolicRef);
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(_) => return Err(LocalRefError::Precondition),
        }
    }
    Ok(())
}
fn direct_ref(repo: &Path, reference: &str) -> Result<(), LocalRefError> {
    reject_symlinks(repo, reference)?;
    let mut cmd = git(repo);
    cmd.args(["symbolic-ref", "--quiet", reference]);
    match run(cmd)?.status.code() {
        Some(1) => Ok(()),
        Some(0) => Err(LocalRefError::SymbolicRef),
        _ => Err(LocalRefError::Precondition),
    }
}

/// Atomically verify a staging ref and advance a target in a trusted local
/// **bare** repository. This only implements Git ref mechanics: callers must
/// separately establish all authorization, evidence, policy and writer-control
/// guarantees. This function must not be used as GitHub landing or merge proof.
///
/// A successful return means Git acknowledged the transaction. A failure after
/// the commit request is sent is `Uncertain`, even if a later read finds the
/// requested commit. This function does not journal or retry uncertain writes.
pub fn compare_and_advance(
    repository: &Path,
    target_ref: &str,
    staging_ref: &str,
    expected_base: &str,
    staged_sha: &str,
) -> Result<(), LocalRefError> {
    if !safe_ref(target_ref)
        || !safe_ref(staging_ref)
        || target_ref == staging_ref
        || target_ref.starts_with("refs/heads/sluss/")
        || !staging_ref.starts_with("refs/heads/sluss/")
        || !valid_sha(expected_base)
        || !valid_sha(staged_sha)
        || expected_base == staged_sha
    {
        return Err(LocalRefError::Invalid);
    }
    let repo = repository
        .canonicalize()
        .map_err(|_| LocalRefError::Invalid)?;
    if !repo.is_dir() {
        return Err(LocalRefError::Invalid);
    }
    for forbidden in [repo.join("info/grafts"), repo.join("commondir")] {
        match std::fs::symlink_metadata(forbidden) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => return Err(LocalRefError::Invalid),
        }
    }
    let mut bare = git(&repo);
    bare.args([
        "rev-parse",
        "--is-bare-repository",
        "--is-shallow-repository",
        "--show-object-format",
    ]);
    let output = run(bare)?;
    if !output.status.success() || output.stdout != b"true\nfalse\nsha1\n" {
        return Err(LocalRefError::Invalid);
    }
    for reference in [target_ref, staging_ref] {
        let mut check = git(&repo);
        check.args(["check-ref-format", reference]);
        if !run(check)?.status.success() {
            return Err(LocalRefError::Invalid);
        }
    }
    for sha in [expected_base, staged_sha] {
        let mut check = git(&repo);
        check.args(["cat-file", "-t", sha]);
        let result = run(check)?;
        if !result.status.success() || result.stdout != b"commit\n" {
            return Err(LocalRefError::Invalid);
        }
    }
    let mut ancestry = git(&repo);
    ancestry.args(["merge-base", "--is-ancestor", expected_base, staged_sha]);
    match run(ancestry)?.status.code() {
        Some(0) => {}
        Some(1) => return Err(LocalRefError::NotFastForward),
        _ => return Err(LocalRefError::Precondition),
    }

    // Avoid even preparing a ref lock through an existing filesystem symlink.
    reject_symlinks(&repo, target_ref)?;
    reject_symlinks(&repo, staging_ref)?;
    let mut command = git(&repo);
    configure(&mut command);
    command
        .args(["update-ref", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
    let mut child = Process(command.spawn().map_err(|_| LocalRefError::Precondition)?);
    let mut input = child.0.stdin.take().ok_or(LocalRefError::Precondition)?;
    let output = child.0.stdout.take().ok_or(LocalRefError::Precondition)?;
    let (sender, receiver) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut reader = BufReader::new(output);
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) if sender.send(line).is_err() => break,
                Ok(_) => {}
            }
        }
    });
    let receive = |expected: &str| {
        receiver
            .recv_timeout(TIMEOUT)
            .is_ok_and(|line| line == expected)
    };
    let prepare = format!(
        "start\noption no-deref\nverify {staging_ref} {staged_sha}\noption no-deref\nupdate {target_ref} {staged_sha} {expected_base}\nprepare\n"
    );
    if input.write_all(prepare.as_bytes()).is_err()
        || input.flush().is_err()
        || !receive("start: ok\n")
        || !receive("prepare: ok\n")
    {
        drop(input);
        let _ = wait(&mut child.0);
        drop(child);
        let _ = reader.join();
        return Err(LocalRefError::Precondition);
    }
    // `verify` can compare through a symbolic ref even with no-deref. Both
    // refs are now locked; refuse symbolic refs before sending commit.
    if let Err(error) = direct_ref(&repo, target_ref).and_then(|_| direct_ref(&repo, staging_ref)) {
        let aborted = input
            .write_all(b"abort\n")
            .and_then(|_| input.flush())
            .is_ok()
            && receive("abort: ok\n");
        drop(input);
        let finished = wait(&mut child.0).is_ok_and(|status| status.success());
        drop(child);
        let _ = reader.join();
        return if aborted && finished {
            Err(error)
        } else {
            Err(LocalRefError::Precondition)
        };
    }
    let sent = input
        .write_all(b"commit\n")
        .and_then(|_| input.flush())
        .is_ok();
    let acknowledged = sent && receive("commit: ok\n");
    drop(input);
    let finished = wait(&mut child.0).is_ok_and(|status| status.success());
    drop(child);
    let _ = reader.join();
    if acknowledged && finished {
        Ok(())
    } else {
        Err(LocalRefError::Uncertain)
    }
}

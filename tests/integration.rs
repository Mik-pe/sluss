use sluss::integration::{IntegrationError, build};
use std::{path::Path, process::Command};

fn git(repo: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["-c", "core.hooksPath=/dev/null"])
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "Test")
        .env("GIT_AUTHOR_EMAIL", "test@example.invalid")
        .env("GIT_COMMITTER_NAME", "Test")
        .env("GIT_COMMITTER_EMAIL", "test@example.invalid")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().into()
}
fn fixture() -> (tempfile::TempDir, String, String, String) {
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "-b", "main"]);
    std::fs::write(dir.path().join("shared"), "base\n").unwrap();
    git(dir.path(), &["add", "."]);
    git(
        dir.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "base"],
    );
    let base = git(dir.path(), &["rev-parse", "HEAD"]);
    git(dir.path(), &["checkout", "-b", "one"]);
    std::fs::write(dir.path().join("one"), "one\n").unwrap();
    git(dir.path(), &["add", "."]);
    git(
        dir.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "one"],
    );
    let one = git(dir.path(), &["rev-parse", "HEAD"]);
    git(dir.path(), &["checkout", "-b", "two", &base]);
    std::fs::write(dir.path().join("two"), "two\n").unwrap();
    git(dir.path(), &["add", "."]);
    git(
        dir.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "two"],
    );
    let two = git(dir.path(), &["rev-parse", "HEAD"]);
    git(dir.path(), &["checkout", "main"]);
    (dir, base, one, two)
}

#[test]
fn creates_deterministic_merge_chain_without_mutating_source() {
    let (dir, base, one, two) = fixture();
    let first = build(dir.path(), &base, &[one.clone(), two.clone()]).unwrap();
    let second = build(dir.path(), &base, &[one.clone(), two.clone()]).unwrap();
    assert_eq!(first.sha(), second.sha());
    assert_ne!(first.sha(), base);
    let parents = git(
        &first.repository(),
        &["show", "-s", "--format=%P", first.sha()],
    );
    let parents: Vec<_> = parents.split_whitespace().collect();
    assert_eq!(parents.len(), 2);
    assert_eq!(parents[1], two);
    assert_eq!(
        git(
            &first.repository(),
            &["rev-parse", &format!("{}^2", parents[0])]
        ),
        one
    );
    assert_eq!(
        git(
            &first.repository(),
            &["show", &format!("{}:one", first.sha())]
        ),
        "one"
    );
    assert_eq!(
        git(
            &first.repository(),
            &["show", &format!("{}:two", first.sha())]
        ),
        "two"
    );
    assert_eq!(git(dir.path(), &["rev-parse", "main"]), base);
    assert_eq!(git(dir.path(), &["status", "--porcelain"]), "");
    let workspace_repo = first.repository();
    drop(first);
    assert!(!workspace_repo.exists());
    assert!(dir.path().exists());
    let reversed = build(dir.path(), &base, &[two, one]).unwrap();
    assert_ne!(second.sha(), reversed.sha());
}

#[test]
fn conflicting_and_duplicate_heads_fail_without_touching_target() {
    let (dir, base, one, _) = fixture();
    git(dir.path(), &["checkout", "-b", "conflict-a", &base]);
    std::fs::write(dir.path().join("shared"), "a\n").unwrap();
    git(dir.path(), &["add", "."]);
    git(
        dir.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "a"],
    );
    let a = git(dir.path(), &["rev-parse", "HEAD"]);
    git(dir.path(), &["checkout", "-b", "conflict-b", &base]);
    std::fs::write(dir.path().join("shared"), "b\n").unwrap();
    git(dir.path(), &["add", "."]);
    git(
        dir.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "b"],
    );
    let b = git(dir.path(), &["rev-parse", "HEAD"]);
    git(dir.path(), &["checkout", "main"]);
    assert!(matches!(
        build(dir.path(), &base, &[a, b]),
        Err(IntegrationError::Conflict)
    ));
    assert!(matches!(
        build(dir.path(), &base, &[one.clone(), one]),
        Err(IntegrationError::AlreadyIntegrated)
    ));
    assert!(matches!(
        build(dir.path(), "--help", std::slice::from_ref(&base)),
        Err(IntegrationError::InvalidInput)
    ));
    assert_eq!(git(dir.path(), &["rev-parse", "main"]), base);
}

#[test]
fn rejects_tag_objects_even_when_they_point_to_a_commit() {
    let (dir, base, one, _) = fixture();
    git(
        dir.path(),
        &[
            "-c",
            "tag.gpgsign=false",
            "tag",
            "-a",
            "annotated",
            "-m",
            "tag",
            &one,
        ],
    );
    let tag = git(dir.path(), &["rev-parse", "refs/tags/annotated"]);
    assert!(matches!(
        build(dir.path(), &base, &[tag]),
        Err(IntegrationError::InvalidInput)
    ));
    assert_eq!(git(dir.path(), &["rev-parse", "main"]), base);
}

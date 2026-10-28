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

#[test]
fn publishes_idempotently_and_refuses_to_replace_another_commit() {
    use sluss::integration::StageError;
    let (dir, base, one, two) = fixture();
    let remote = tempfile::tempdir().unwrap();
    git(remote.path(), &["init", "--bare"]);
    git(
        dir.path(),
        &["push", remote.path().to_str().unwrap(), "main:main"],
    );
    let first = build(dir.path(), &base, std::slice::from_ref(&one)).unwrap();
    let second = build(dir.path(), &base, std::slice::from_ref(&two)).unwrap();
    let id = "batch-0123456789abcdef";
    let staged = first.stage_local(remote.path(), id).unwrap();
    assert_eq!(staged.reference, format!("refs/heads/sluss/{id}"));
    assert_eq!(staged.sha, first.sha());
    assert_eq!(first.stage_local(remote.path(), id).unwrap(), staged);
    assert!(matches!(
        second.stage_local(remote.path(), id),
        Err(StageError::Conflict)
    ));
    assert!(matches!(
        first.stage_local(remote.path(), "../main"),
        Err(StageError::Invalid)
    ));
    assert_eq!(
        git(remote.path(), &["rev-parse", &staged.reference]),
        first.sha()
    );
    assert_eq!(git(remote.path(), &["rev-parse", "refs/heads/main"]), base);
}

#[test]
fn competing_staging_publications_do_not_replace_each_other() {
    use sluss::integration::StageError;
    let (dir, base, one, two) = fixture();
    let remote = tempfile::tempdir().unwrap();
    git(remote.path(), &["init", "--bare"]);
    let first = build(dir.path(), &base, &[one]).unwrap();
    let second = build(dir.path(), &base, &[two]).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let launch = |integration: sluss::integration::Integration| {
        let barrier = barrier.clone();
        let remote = remote.path().to_owned();
        std::thread::spawn(move || {
            barrier.wait();
            integration.stage_local(&remote, "batch-competition-012345")
        })
    };
    let a = launch(first);
    let b = launch(second);
    let results = [a.join().unwrap(), b.join().unwrap()];
    assert_eq!(
        results.iter().filter(|r| r.is_ok()).count(),
        1,
        "{results:?}"
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(StageError::Conflict)))
            .count(),
        1,
        "{results:?}"
    );
    let winner = results.iter().find_map(|r| r.as_ref().ok()).unwrap();
    assert_eq!(
        git(remote.path(), &["rev-parse", &winner.reference]),
        winner.sha
    );
}

#[cfg(unix)]
#[test]
fn rejects_invalid_github_staging_inputs_before_connecting() {
    use sluss::integration::StageError;
    let (dir, base, one, _) = fixture();
    let integration = build(dir.path(), &base, &[one]).unwrap();
    for (owner, repository, token, id) in [
        ("bad/owner", "repo", "token", "batch-0123456789abcdef"),
        ("owner", "repo", "", "batch-0123456789abcdef"),
        ("owner", "repo", "token", "../main"),
    ] {
        assert!(matches!(
            integration.stage_github(owner, repository, token, id),
            Err(StageError::Invalid)
        ));
    }
}

#[test]
fn policy_snapshot_detects_policy_changes_and_rejects_missing_or_invalid_paths() {
    use sluss::integration::PolicyError;
    let (dir, _, _, _) = fixture();
    std::fs::create_dir_all(dir.path().join(".github/workflows")).unwrap();
    std::fs::write(
        dir.path().join(".github/workflows/ci.yml"),
        "trusted policy\n",
    )
    .unwrap();
    git(dir.path(), &["add", "."]);
    git(
        dir.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "policy"],
    );
    let base = git(dir.path(), &["rev-parse", "HEAD"]);
    git(dir.path(), &["checkout", "-b", "safe-change"]);
    std::fs::write(dir.path().join("safe"), "change\n").unwrap();
    git(dir.path(), &["add", "."]);
    git(
        dir.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "safe"],
    );
    let safe = git(dir.path(), &["rev-parse", "HEAD"]);
    git(dir.path(), &["checkout", "-b", "policy-change", &base]);
    std::fs::write(
        dir.path().join(".github/workflows/ci.yml"),
        "weakened policy\n",
    )
    .unwrap();
    git(dir.path(), &["add", "."]);
    git(
        dir.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "changed"],
    );
    let changed = git(dir.path(), &["rev-parse", "HEAD"]);
    let paths = vec![".github/workflows/ci.yml".to_string()];
    let good = build(dir.path(), &base, &[safe]).unwrap();
    let snapshot = good.check_policy(&base, &paths).unwrap();
    assert_eq!(snapshot.digest.len(), 64);
    assert_eq!(snapshot, good.check_policy(&base, &paths).unwrap());
    let bad = build(dir.path(), &base, &[changed]).unwrap();
    assert!(matches!(
        bad.check_policy(&base, &paths),
        Err(PolicyError::Changed)
    ));
    assert!(matches!(
        good.check_policy(&base, &["missing".into()]),
        Err(PolicyError::Changed)
    ));
    assert!(matches!(
        good.check_policy(&base, &["../main".into()]),
        Err(PolicyError::Invalid)
    ));
    assert!(matches!(
        good.check_policy(&base, &[paths[0].clone(), paths[0].clone()]),
        Err(PolicyError::Invalid)
    ));
}

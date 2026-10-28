use sluss::{
    integration::build,
    local_refs::{LocalRefError, compare_and_advance},
};
use std::{
    path::Path,
    process::Command,
    sync::{Arc, Barrier},
};

fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}
struct Fixture {
    _source: tempfile::TempDir,
    bare: tempfile::TempDir,
    base: String,
    one: String,
    two: String,
    combined: String,
}
fn fixture() -> Fixture {
    let source = tempfile::tempdir().unwrap();
    git(source.path(), &["init", "-b", "main"]);
    std::fs::write(source.path().join("base"), "base").unwrap();
    git(source.path(), &["add", "."]);
    git(source.path(), &["commit", "-m", "base"]);
    let base = git(source.path(), &["rev-parse", "HEAD"]);
    git(source.path(), &["checkout", "-b", "one"]);
    std::fs::write(source.path().join("one"), "one").unwrap();
    git(source.path(), &["add", "."]);
    git(source.path(), &["commit", "-m", "one"]);
    let head_one = git(source.path(), &["rev-parse", "HEAD"]);
    git(source.path(), &["checkout", "-b", "two", &base]);
    std::fs::write(source.path().join("two"), "two").unwrap();
    git(source.path(), &["add", "."]);
    git(source.path(), &["commit", "-m", "two"]);
    let head_two = git(source.path(), &["rev-parse", "HEAD"]);
    let bare = tempfile::tempdir().unwrap();
    git(bare.path(), &["init", "--bare"]);
    git(
        source.path(),
        &[
            "push",
            bare.path().to_str().unwrap(),
            "main:refs/heads/main",
        ],
    );
    let one = build(source.path(), &base, std::slice::from_ref(&head_one)).unwrap();
    let two = build(source.path(), &base, std::slice::from_ref(&head_two)).unwrap();
    let both = build(source.path(), &base, &[head_one, head_two]).unwrap();
    let one_sha = one
        .stage_local(bare.path(), "batch-0000000000000001")
        .unwrap()
        .sha;
    let two_sha = two
        .stage_local(bare.path(), "batch-0000000000000002")
        .unwrap()
        .sha;
    let both_sha = both
        .stage_local(bare.path(), "batch-0000000000000003")
        .unwrap()
        .sha;
    Fixture {
        _source: source,
        bare,
        base,
        one: one_sha,
        two: two_sha,
        combined: both_sha,
    }
}
const TARGET: &str = "refs/heads/main";
const ONE: &str = "refs/heads/sluss/batch-0000000000000001";
const TWO: &str = "refs/heads/sluss/batch-0000000000000002";
const BOTH: &str = "refs/heads/sluss/batch-0000000000000003";

#[test]
fn advances_to_the_exact_staging_commit_from_the_expected_base() {
    let f = fixture();
    compare_and_advance(f.bare.path(), TARGET, BOTH, &f.base, &f.combined).unwrap();
    assert_eq!(git(f.bare.path(), &["rev-parse", TARGET]), f.combined);
    assert_eq!(git(f.bare.path(), &["rev-parse", BOTH]), f.combined);
    assert_eq!(
        git(f.bare.path(), &["merge-base", &f.base, &f.combined]),
        f.base
    );
}

#[test]
fn advances_when_both_refs_are_packed() {
    let f = fixture();
    git(f.bare.path(), &["pack-refs", "--all", "--prune"]);
    assert!(!f.bare.path().join(TARGET).exists());
    assert!(!f.bare.path().join(BOTH).exists());
    compare_and_advance(f.bare.path(), TARGET, BOTH, &f.base, &f.combined).unwrap();
    assert_eq!(git(f.bare.path(), &["rev-parse", TARGET]), f.combined);
    assert_eq!(git(f.bare.path(), &["rev-parse", BOTH]), f.combined);
}

#[test]
fn refuses_a_changed_target_even_when_it_is_an_ancestor_of_staging() {
    let f = fixture();
    let intermediate = git(f.bare.path(), &["rev-parse", &format!("{}^1", f.combined)]);
    git(
        f.bare.path(),
        &["update-ref", TARGET, &intermediate, &f.base],
    );
    assert!(matches!(
        compare_and_advance(f.bare.path(), TARGET, BOTH, &f.base, &f.combined),
        Err(LocalRefError::Precondition)
    ));
    assert_eq!(git(f.bare.path(), &["rev-parse", TARGET]), intermediate);
}

#[test]
fn refuses_a_changed_staging_ref_and_non_fast_forward() {
    let f = fixture();
    git(f.bare.path(), &["update-ref", ONE, &f.two, &f.one]);
    assert!(matches!(
        compare_and_advance(f.bare.path(), TARGET, ONE, &f.base, &f.one),
        Err(LocalRefError::Precondition)
    ));
    assert_eq!(git(f.bare.path(), &["rev-parse", TARGET]), f.base);
    git(f.bare.path(), &["update-ref", TARGET, &f.two, &f.base]);
    assert_eq!(
        compare_and_advance(f.bare.path(), TARGET, BOTH, &f.two, &f.one),
        Err(LocalRefError::NotFastForward)
    );
    assert_eq!(git(f.bare.path(), &["rev-parse", TARGET]), f.two);
}

#[test]
fn refuses_symbolic_staging_and_symbolic_target_while_refs_are_locked() {
    let f = fixture();
    git(f.bare.path(), &["update-ref", "refs/heads/alias", &f.one]);
    git(f.bare.path(), &["symbolic-ref", ONE, "refs/heads/alias"]);
    assert_eq!(
        compare_and_advance(f.bare.path(), TARGET, ONE, &f.base, &f.one),
        Err(LocalRefError::SymbolicRef)
    );
    assert_eq!(git(f.bare.path(), &["rev-parse", TARGET]), f.base);
    assert!(!f.bare.path().join("refs/heads/main.lock").exists());
    assert!(
        !f.bare
            .path()
            .join("refs/heads/sluss/batch-0000000000000001.lock")
            .exists()
    );
    git(f.bare.path(), &["symbolic-ref", TARGET, "refs/heads/alias"]);
    git(
        f.bare.path(),
        &["update-ref", "refs/heads/alias", &f.base, &f.one],
    );
    assert_eq!(
        compare_and_advance(f.bare.path(), TARGET, TWO, &f.base, &f.two),
        Err(LocalRefError::SymbolicRef)
    );
    assert_eq!(
        git(f.bare.path(), &["symbolic-ref", TARGET]),
        "refs/heads/alias"
    );
    assert_eq!(
        git(f.bare.path(), &["rev-parse", "refs/heads/alias"]),
        f.base
    );
}

#[test]
fn concurrent_updates_have_exactly_one_winner() {
    let f = fixture();
    let barrier = Arc::new(Barrier::new(2));
    let tasks: Vec<_> = [(ONE, f.one.clone()), (TWO, f.two.clone())]
        .into_iter()
        .map(|(staging, sha)| {
            let barrier = barrier.clone();
            let repo = f.bare.path().to_owned();
            let base = f.base.clone();
            std::thread::spawn(move || {
                barrier.wait();
                (
                    sha.clone(),
                    compare_and_advance(&repo, TARGET, staging, &base, &sha),
                )
            })
        })
        .collect();
    let results: Vec<_> = tasks.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(
        results.iter().filter(|(_, result)| result.is_ok()).count(),
        1
    );
    assert!(
        results
            .iter()
            .any(|(_, result)| matches!(result, Err(LocalRefError::Precondition)))
    );
    let winner = &results.iter().find(|(_, result)| result.is_ok()).unwrap().0;
    assert_eq!(&git(f.bare.path(), &["rev-parse", TARGET]), winner);
}

#[test]
fn rejects_invalid_or_non_bare_inputs() {
    let f = fixture();
    assert_eq!(
        compare_and_advance(f._source.path(), TARGET, ONE, &f.base, &f.one),
        Err(LocalRefError::Invalid)
    );
    assert_eq!(
        compare_and_advance(
            f.bare.path(),
            "refs/heads/main\ncommit",
            ONE,
            &f.base,
            &f.one
        ),
        Err(LocalRefError::Invalid)
    );
    assert_eq!(
        compare_and_advance(f.bare.path(), TARGET, ONE, &f.base, &f.base),
        Err(LocalRefError::Invalid)
    );
    assert_eq!(git(f.bare.path(), &["rev-parse", TARGET]), f.base);
}

#[cfg(unix)]
#[test]
fn refuses_filesystem_symlinks_without_touching_the_link_target() {
    use std::os::unix::fs::symlink;
    let f = fixture();
    let external = tempfile::tempdir().unwrap();
    let outside = external.path().join("target");
    std::fs::write(&outside, format!("{}\n", f.base)).unwrap();
    let target_path = f.bare.path().join(TARGET);
    std::fs::remove_file(&target_path).unwrap();
    symlink(&outside, &target_path).unwrap();
    assert_eq!(
        compare_and_advance(f.bare.path(), TARGET, ONE, &f.base, &f.one),
        Err(LocalRefError::SymbolicRef)
    );
    assert_eq!(
        std::fs::read_to_string(outside).unwrap(),
        format!("{}\n", f.base)
    );
    assert!(
        std::fs::symlink_metadata(target_path)
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[test]
fn refuses_git_grafts_that_could_redefine_ancestry() {
    let f = fixture();
    std::fs::write(f.bare.path().join("info/grafts"), format!("{}\n", f.one)).unwrap();
    assert_eq!(
        compare_and_advance(f.bare.path(), TARGET, ONE, &f.base, &f.one),
        Err(LocalRefError::Invalid)
    );
    assert_eq!(git(f.bare.path(), &["rev-parse", TARGET]), f.base);
}

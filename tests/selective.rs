use sluss::selective::{Policy, Selection, SelectionError, changed_paths};
use std::{collections::BTreeSet, path::Path, process::Command};

fn sample_policy() -> Policy {
    toml::from_str(include_str!("../examples/verification.toml")).unwrap()
}
fn names(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| (*s).into()).collect()
}
#[test]
fn selects_dependents_and_unions_changes() {
    let policy = sample_policy();
    assert_eq!(
        policy.select(&["apps/ui/src/main.rs".into()]).unwrap(),
        Selection::Selected {
            suites: names(&["ui-tests"]),
            components: names(&["ui"])
        }
    );
    assert_eq!(
        policy.select(&["crates/api/src/lib.rs".into()]).unwrap(),
        Selection::Selected {
            suites: names(&["api-tests", "ui-tests"]),
            components: names(&["api", "ui"])
        }
    );
    assert_eq!(
        policy.select(&["crates/core/src/lib.rs".into()]).unwrap(),
        Selection::Selected {
            suites: names(&["core-tests", "api-tests", "ui-tests"]),
            components: names(&["core", "api", "ui"])
        }
    );
    assert_eq!(
        policy
            .select(&["crates/api/src/lib.rs".into(), "apps/ui/src/main.rs".into()])
            .unwrap(),
        Selection::Selected {
            suites: names(&["api-tests", "ui-tests"]),
            components: names(&["api", "ui"])
        }
    );
}
#[test]
fn unknown_and_policy_sensitive_changes_require_every_suite() {
    let policy = sample_policy();
    for path in [
        "README.md",
        ".github/workflows/ci.yml",
        "Cargo.lock",
        "crates/core/Cargo.toml",
        "ci/build.sh",
        "../secrets",
        "crates/api-extra/file",
    ] {
        let selected = policy.select(&[path.into()]).unwrap();
        assert!(
            matches!(selected, Selection::Full { suites, .. } if suites == names(&["core-tests","api-tests","ui-tests"])),
            "{path}"
        );
    }
    assert!(matches!(
        policy.select(&[]).unwrap(),
        Selection::Full { .. }
    ));
}
#[test]
fn rejects_invalid_dependency_graphs_and_suite_names() {
    let mut policy = sample_policy();
    policy.components[0].depends_on.push("missing".into());
    assert!(matches!(
        policy.select(&["crates/core/lib.rs".into()]),
        Err(SelectionError::InvalidPolicy)
    ));
    let mut policy = sample_policy();
    policy.components[0].suites.push("not-declared".into());
    assert!(matches!(
        policy.select(&["crates/core/lib.rs".into()]),
        Err(SelectionError::InvalidPolicy)
    ));
    let mut policy = sample_policy();
    policy.components[0].paths = vec!["../escape/".into()];
    assert!(matches!(
        policy.select(&["crates/core/lib.rs".into()]),
        Err(SelectionError::InvalidPolicy)
    ));
}
fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args([
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
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}
#[test]
fn git_diff_and_cli_classify_both_sides_of_a_rename() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path();
    git(repo, &["init", "-b", "main"]);
    std::fs::create_dir_all(repo.join("crates/api/src")).unwrap();
    std::fs::write(repo.join("crates/api/src/old.rs"), "source\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "base"]);
    let base = git(repo, &["rev-parse", "HEAD"]);
    std::fs::create_dir_all(repo.join("apps/ui/src")).unwrap();
    git(repo, &["mv", "crates/api/src/old.rs", "apps/ui/src/new.rs"]);
    git(repo, &["commit", "-m", "rename"]);
    let head = git(repo, &["rev-parse", "HEAD"]);
    let paths = changed_paths(repo, &base, &head).unwrap();
    assert_eq!(
        paths.iter().cloned().collect::<BTreeSet<_>>(),
        names(&["crates/api/src/old.rs", "apps/ui/src/new.rs"])
    );
    let config = repo.join("policy.toml");
    std::fs::write(&config, include_str!("../examples/verification.toml")).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .args(["select", "--policy"])
        .arg(config)
        .arg("--source")
        .arg(repo)
        .args(["--base-sha", &base, "--head-sha", &head])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(
        text.contains("api-tests") && text.contains("ui-tests") && !text.contains("core-tests")
    );
    assert!(matches!(
        changed_paths(repo, &head, &base),
        Err(SelectionError::InvalidCommit)
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn non_utf8_git_paths_select_full_verification() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path();
    git(repo, &["init", "-b", "main"]);
    std::fs::write(repo.join("base"), "base").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "base"]);
    let base = git(repo, &["rev-parse", "HEAD"]);
    let invalid = OsString::from_vec(b"non-utf8-\xff".to_vec());
    std::fs::write(repo.join(invalid), "change").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "change"]);
    let head = git(repo, &["rev-parse", "HEAD"]);
    assert!(matches!(
        changed_paths(repo, &base, &head),
        Err(SelectionError::ChangeList)
    ));
    assert!(matches!(
        sample_policy().select_git(repo, &base, &head).unwrap(),
        Selection::Full { .. }
    ));
}

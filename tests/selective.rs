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
        "crates/core/.gitattributes",
        "crates/core/.cargo/config.toml",
        ".cargo/config",
        "crates/core/rust-toolchain.toml",
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
fn changed_selection_semantics_invalidate_previous_policy_digests() {
    use sha2::{Digest, Sha256};
    let policy = sample_policy();
    let mut old = Sha256::new();
    old.update(b"sluss-selective-policy-v2\0");
    old.update(serde_json::to_vec(&policy).unwrap());
    assert_ne!(policy.digest().unwrap(), hex::encode(old.finalize()));
}

#[test]
fn review_policy_paths_require_full_verification_even_when_classified() {
    let mut policy = sample_policy();
    policy.components[0].paths.extend([
        "CODEOWNERS".into(),
        "docs/CODEOWNERS".into(),
        ".github".into(),
    ]);
    for path in ["CODEOWNERS", "docs/CODEOWNERS", ".github"] {
        assert!(
            matches!(
                policy.select(&[path.into()]).unwrap(),
                Selection::Full { .. }
            ),
            "{path}"
        );
    }
}

#[test]
fn a_missing_suite_on_any_affected_component_requires_full_verification() {
    let mut policy = sample_policy();
    policy.components[0].suites.clear();
    assert!(matches!(
        policy.select(&["crates/core/src/lib.rs".into()]).unwrap(),
        Selection::Full { .. }
    ));
    assert!(matches!(
        policy
            .select(&[
                "crates/core/src/lib.rs".into(),
                "apps/ui/src/main.rs".into()
            ])
            .unwrap(),
        Selection::Full { .. }
    ));
    // Missing suites in an unaffected dependency do not make an unrelated
    // selected component ambiguous.
    assert!(matches!(
        policy.select(&["apps/ui/src/main.rs".into()]).unwrap(),
        Selection::Selected { .. }
    ));
    let mut policy = sample_policy();
    policy.components[2].suites.clear();
    assert!(
        matches!(
            policy.select(&["crates/api/src/lib.rs".into()]).unwrap(),
            Selection::Full { .. }
        ),
        "an affected dependent without a suite must also trigger the full set"
    );
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
fn git_selection_uses_full_suites_for_link_and_submodule_changes() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path();
    git(repo, &["init", "-b", "main"]);
    std::fs::create_dir_all(repo.join("crates/core/src")).unwrap();
    std::fs::write(repo.join("crates/core/src/base.rs"), "base\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "base"]);
    let base = git(repo, &["rev-parse", "HEAD"]);
    let policy = sample_policy();
    std::fs::write(repo.join("link-target"), "../../api/src").unwrap();
    let blob = git(repo, &["hash-object", "-w", "link-target"]);
    git(
        repo,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("120000,{blob},crates/core/src/link"),
        ],
    );
    git(repo, &["commit", "-m", "add link"]);
    let link = git(repo, &["rev-parse", "HEAD"]);
    assert!(matches!(
        policy.select_git(repo, &base, &link).unwrap(),
        Selection::Full { .. }
    ));
    git(
        repo,
        &["update-index", "--force-remove", "crates/core/src/link"],
    );
    git(repo, &["commit", "-m", "remove link"]);
    let removed = git(repo, &["rev-parse", "HEAD"]);
    assert!(matches!(
        policy.select_git(repo, &link, &removed).unwrap(),
        Selection::Full { .. }
    ));
    git(
        repo,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("160000,{base},crates/core/dependency"),
        ],
    );
    git(repo, &["commit", "-m", "add submodule"]);
    let module = git(repo, &["rev-parse", "HEAD"]);
    assert!(matches!(
        policy.select_git(repo, &removed, &module).unwrap(),
        Selection::Full { .. }
    ));
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

#[test]
fn submodule_changes_cannot_be_hidden_by_local_diff_configuration() {
    let child = tempfile::tempdir().unwrap();
    let parent = tempfile::tempdir().unwrap();
    git(child.path(), &["init", "-b", "main"]);
    std::fs::write(child.path().join("lib"), "first").unwrap();
    git(child.path(), &["add", "."]);
    git(child.path(), &["commit", "-m", "first"]);
    let first = git(child.path(), &["rev-parse", "HEAD"]);
    std::fs::write(child.path().join("lib"), "second").unwrap();
    git(child.path(), &["add", "."]);
    git(child.path(), &["commit", "-m", "second"]);
    let second = git(child.path(), &["rev-parse", "HEAD"]);
    git(parent.path(), &["init", "-b", "main"]);
    git(
        parent.path(),
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            child.path().to_str().unwrap(),
            "crates/api/vendor",
        ],
    );
    git(
        &parent.path().join("crates/api/vendor"),
        &["checkout", "--detach", &first],
    );
    git(parent.path(), &["add", "."]);
    git(parent.path(), &["commit", "-m", "base"]);
    let base = git(parent.path(), &["rev-parse", "HEAD"]);
    git(
        &parent.path().join("crates/api/vendor"),
        &["checkout", "--detach", &second],
    );
    git(parent.path(), &["add", "crates/api/vendor"]);
    git(parent.path(), &["commit", "-m", "update submodule"]);
    let head = git(parent.path(), &["rev-parse", "HEAD"]);
    git(parent.path(), &["config", "diff.ignoreSubmodules", "all"]);
    assert!(
        git(parent.path(), &["diff", "--name-only", &base, &head]).is_empty(),
        "the fixture really hides the gitlink change by default"
    );
    assert_eq!(
        changed_paths(parent.path(), &base, &head).unwrap(),
        vec!["crates/api/vendor"]
    );
    assert!(
        matches!(sample_policy().select_git(parent.path(), &base, &head).unwrap(), Selection::Full { suites, .. } if suites == names(&["core-tests", "api-tests", "ui-tests"]))
    );
}

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
    git(dir.path(), &["checkout", "-b", "policy-delete", &base]);
    git(dir.path(), &["rm", ".github/workflows/ci.yml"]);
    git(
        dir.path(),
        &[
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "delete policy",
        ],
    );
    let deleted = git(dir.path(), &["rev-parse", "HEAD"]);
    let deletion = build(dir.path(), &base, &[deleted]).unwrap();
    assert!(matches!(
        deletion.check_policy(&base, &paths),
        Err(PolicyError::Changed)
    ));
    assert!(matches!(
        good.check_policy(&base, &["missing".into()]),
        Err(PolicyError::BaseUnavailable)
    ));
    assert!(matches!(
        bad.check_policy(&base, &[paths[0].clone(), "zzz-missing".into()]),
        Err(PolicyError::BaseUnavailable)
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

#[test]
fn changed_ci_policy_splits_batch_but_unusable_base_does_not_blame_prs() {
    use sluss::{
        batch::{Candidate, State},
        coordinator::{PrepareError, prepare_local},
        integration::PolicyError,
        store::{AdmissionCandidate, Store},
        webhook::Inbox,
    };
    let (source, base, safe, _) = fixture();
    git(source.path(), &["checkout", "-b", "policy-change", &base]);
    std::fs::write(source.path().join("shared"), "changed policy\n").unwrap();
    git(source.path(), &["add", "."]);
    git(
        source.path(),
        &[
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "change policy",
        ],
    );
    let changed = git(source.path(), &["rev-parse", "HEAD"]);
    git(source.path(), &["checkout", "main"]);
    let remote = tempfile::tempdir().unwrap();
    git(remote.path(), &["init", "--bare"]);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    inbox
        .record("initial", "pull_request", &[1; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    store
        .apply_admission_reconciliation(
            &event,
            &"f".repeat(64),
            &[
                AdmissionCandidate {
                    candidate: Candidate {
                        number: 1,
                        head_sha: safe,
                    },
                    evidence_digest: "e".repeat(64),
                },
                AdmissionCandidate {
                    candidate: Candidate {
                        number: 2,
                        head_sha: changed,
                    },
                    evidence_digest: "d".repeat(64),
                },
            ],
        )
        .unwrap();
    let paths = vec!["shared".into()];
    let root = "batch-0123456789abcdef";
    store.plan_batch(root, &base, 2, 2).unwrap().unwrap();
    assert!(matches!(
        prepare_local(&mut store, root, source.path(), remote.path(), &paths),
        Err(PrepareError::Policy(PolicyError::Changed))
    ));
    assert!(matches!(
        store.batch(root).unwrap().unwrap().0.state,
        State::Failed { .. }
    ));
    assert!(
        Command::new("git")
            .arg("-C")
            .arg(remote.path())
            .args([
                "show-ref",
                "--verify",
                "--quiet",
                &format!("refs/heads/sluss/{root}")
            ])
            .output()
            .unwrap()
            .status
            .code()
            == Some(1)
    );
    let safe_batch = "batch-1123456789abcdef";
    let safe_child = store.plan_batch(safe_batch, &base, 2, 2).unwrap().unwrap();
    assert_eq!(safe_child.candidates[0].number, 1);
    prepare_local(&mut store, safe_batch, source.path(), remote.path(), &paths).unwrap();
    let blocked_batch = "batch-2123456789abcdef";
    let blocked_child = store
        .plan_batch(blocked_batch, &base, 2, 2)
        .unwrap()
        .unwrap();
    assert_eq!(blocked_child.candidates[0].number, 2);
    assert!(matches!(
        prepare_local(
            &mut store,
            blocked_batch,
            source.path(),
            remote.path(),
            &paths
        ),
        Err(PrepareError::Policy(PolicyError::Changed))
    ));
    assert_eq!(
        store
            .failed_candidates()
            .unwrap()
            .iter()
            .map(|c| c.number)
            .collect::<Vec<_>>(),
        vec![2]
    );

    // A missing base policy file is configuration trouble even if another
    // declared file would differ in the staged tree.
    let other = tempfile::tempdir().unwrap();
    let mut other_inbox = Inbox::open(other.path().join("queue.sqlite")).unwrap();
    let mut other_store = Store::open(other.path().join("queue.sqlite")).unwrap();
    other_inbox
        .record("initial", "pull_request", &[2; 32], 1, 2)
        .unwrap();
    let event = other_inbox.pending().unwrap().remove(0);
    other_store
        .apply_admission_reconciliation(
            &event,
            &"f".repeat(64),
            &[AdmissionCandidate {
                candidate: blocked_child.candidates[0].clone(),
                evidence_digest: "d".repeat(64),
            }],
        )
        .unwrap();
    let id = "batch-3123456789abcdef";
    other_store.plan_batch(id, &base, 1, 1).unwrap().unwrap();
    assert!(matches!(
        prepare_local(
            &mut other_store,
            id,
            source.path(),
            remote.path(),
            &["shared".into(), "zzz-missing".into()]
        ),
        Err(PrepareError::Policy(PolicyError::BaseUnavailable))
    ));
    assert!(matches!(
        other_store.batch(id).unwrap().unwrap().0.state,
        State::Building
    ));
    assert!(other_store.failed_candidates().unwrap().is_empty());
}

#[test]
fn oversized_base_policy_is_not_classified_as_a_pr_change() {
    use sluss::integration::{PolicyError, policy_snapshot_at_commit};
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "-b", "main"]);
    std::fs::write(dir.path().join("large-policy"), vec![b'x'; 1024 * 1024 + 1]).unwrap();
    git(dir.path(), &["add", "."]);
    git(
        dir.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "base"],
    );
    let base = git(dir.path(), &["rev-parse", "HEAD"]);
    assert!(matches!(
        policy_snapshot_at_commit(dir.path(), &base, &["large-policy".into()]),
        Err(PolicyError::BaseUnavailable)
    ));
}

#[test]
fn prepares_and_recovers_a_reconciled_batch_through_real_git_staging() {
    use sluss::{
        batch::{Candidate, State},
        coordinator::{prepare_local, prepare_local_from_remote},
        scheduler::{Capacity, DispatchPlan, Host, Request, Scheduler, SchedulerError},
        store::{AdmissionCandidate, Store},
        webhook::Inbox,
    };
    use std::collections::BTreeMap;
    let (source, _, _, _) = fixture();
    std::fs::create_dir_all(source.path().join(".github/workflows")).unwrap();
    std::fs::write(
        source.path().join(".github/workflows/ci.yml"),
        "trusted policy\n",
    )
    .unwrap();
    git(source.path(), &["add", "."]);
    git(
        source.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "policy"],
    );
    let base = git(source.path(), &["rev-parse", "HEAD"]);
    git(source.path(), &["checkout", "-b", "feature"]);
    std::fs::write(source.path().join("feature"), "feature\n").unwrap();
    git(source.path(), &["add", "."]);
    git(
        source.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "feature"],
    );
    let head = git(source.path(), &["rev-parse", "HEAD"]);
    git(source.path(), &["checkout", "main"]);
    let remote = tempfile::tempdir().unwrap();
    git(remote.path(), &["init", "--bare"]);
    git(
        source.path(),
        &[
            "push",
            remote.path().to_str().unwrap(),
            "main:main",
            "feature:refs/pull/1/head",
        ],
    );
    let data = tempfile::tempdir().unwrap();
    let db = data.path().join("queue.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    inbox
        .record("initial", "pull_request", &[1; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let admission = AdmissionCandidate {
        candidate: Candidate {
            number: 1,
            head_sha: head,
        },
        evidence_digest: "f".repeat(64),
    };
    store
        .apply_admission_reconciliation(&event, &"e".repeat(64), &[admission])
        .unwrap();
    let id = "batch-0123456789abcdef";
    let wrong_scope = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&db)
        .args(["prepare-github", "--batch-id", id, "--source"])
        .arg(source.path())
        .args([
            "--owner",
            "owner",
            "--repository",
            "repo",
            "--app-client-id",
            "app",
            "--installation-id",
            "999",
            "--repository-id",
            "2",
            "--policy-file",
            ".github/workflows/ci.yml",
        ])
        .output()
        .unwrap();
    assert!(!wrong_scope.status.success());
    assert!(String::from_utf8_lossy(&wrong_scope.stderr).contains("repository does not match"));
    let output = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&db)
        .args([
            "plan",
            "--id",
            id,
            "--base-sha",
            &base,
            "--debounce-seconds",
            "0",
            "--batch-window-seconds",
            "0",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains(&format!("planned {id}")));
    // Simulate an interrupted process after it marked the batch Building.
    store.start_integration(id).unwrap();
    drop(store);
    let mut store = Store::open(&db).unwrap();
    let paths = vec![".github/workflows/ci.yml".into()];
    let recovered =
        prepare_local_from_remote(&mut store, id, remote.path(), "main", &paths).unwrap();
    assert_eq!(
        store.batch(id).unwrap().unwrap().0.state,
        State::Testing {
            integration_sha: recovered.sha.clone()
        }
    );
    let output = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&db)
        .args(["prepare-local", "--batch-id", id, "--source"])
        .arg(source.path())
        .arg("--staging-remote")
        .arg(remote.path())
        .args(["--policy-file", ".github/workflows/ci.yml"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("CI and landing are not authorized"));
    let prepared = prepare_local(&mut store, id, source.path(), remote.path(), &paths).unwrap();
    assert_eq!(
        git(remote.path(), &["rev-parse", &prepared.reference]),
        prepared.sha
    );
    assert_eq!(git(remote.path(), &["rev-parse", "refs/heads/main"]), base);
    assert_eq!(
        store.batch(id).unwrap().unwrap().0.state,
        State::Testing {
            integration_sha: prepared.sha.clone()
        }
    );
    assert_eq!(
        prepare_local(&mut store, id, source.path(), remote.path(), &paths).unwrap(),
        prepared
    );
    assert!(
        prepare_local(
            &mut store,
            id,
            source.path(),
            remote.path(),
            &["shared".into()]
        )
        .is_err()
    );
    assert_eq!(
        git(remote.path(), &["rev-parse", &prepared.reference]),
        prepared.sha
    );
    let policy = data.path().join("verification.toml");
    std::fs::write(
        &policy,
        "version = 1\nfull_suites = ['fast', 'all']\n[[components]]\nname = 'feature'\npaths = ['feature']\nsuites = ['fast']\n",
    )
    .unwrap();
    let select = || {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(&db)
            .args(["select-batch", "--batch-id", id, "--policy"])
            .arg(&policy)
            .arg("--source")
            .arg(remote.path())
            .output()
            .unwrap()
    };
    let selected = select();
    assert!(
        selected.status.success(),
        "{}",
        String::from_utf8_lossy(&selected.stderr)
    );
    assert!(String::from_utf8_lossy(&selected.stdout).contains("fast"));
    let frozen = store.batch_selection(id).unwrap().unwrap();
    assert_eq!(frozen.base_sha, base);
    assert_eq!(frozen.integration_sha, prepared.sha);
    assert!(select().status.success());
    std::fs::write(&policy, "version = 1\nfull_suites = ['different']\n").unwrap();
    assert!(!select().status.success());
    assert_eq!(store.batch_selection(id).unwrap().unwrap(), frozen);
    let pools = BTreeMap::from([("rust".into(), 1)]);
    let mut scheduler = Scheduler::open(
        &db,
        Capacity {
            pools: pools.clone(),
            hosts: BTreeMap::from([(
                "host".into(),
                Host {
                    runner_ids: Default::default(),
                    slots: 1,
                    pools: pools.clone(),
                },
            )]),
        },
    )
    .unwrap();
    let request = Request { pools };
    let mut plan = DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
        integration_sha: prepared.sha,
        staging_ref: prepared.reference,
        policy_digest: "c".repeat(64),
        required_jobs: std::collections::BTreeSet::from(["job-1-0".into()]),
        suites: std::collections::BTreeSet::from(["full".into()]),
        selection_policy_digest: None,
    };
    assert!(matches!(
        scheduler.reserve_batch_dispatch(id, &request, &plan),
        Err(SchedulerError::StaleBatch)
    ));
    plan.policy_digest = prepared.policy_digest;
    plan.suites = std::collections::BTreeSet::from(["fast".into()]);
    plan.selection_policy_digest = Some("d".repeat(64));
    assert!(matches!(
        scheduler.reserve_batch_dispatch(id, &request, &plan),
        Err(SchedulerError::StaleBatch)
    ));
    plan.selection_policy_digest = Some(frozen.policy_digest);
    assert!(
        scheduler
            .reserve_batch_dispatch(id, &request, &plan)
            .unwrap()
            .is_some()
    );
    inbox
        .record("new-review", "pull_request_review", &[2; 32], 1, 2)
        .unwrap();
    assert!(matches!(
        scheduler.claim_batch_dispatch(id, ".github/workflows/ci.yml"),
        Err(SchedulerError::StaleBatch)
    ));
    assert!(prepare_local(&mut store, id, source.path(), remote.path(), &paths).is_err());
}

#[test]
fn integration_conflicts_fail_the_batch_without_creating_staging_refs() {
    use sluss::{
        batch::{Candidate, State},
        coordinator::prepare_local,
        store::{AdmissionCandidate, Store},
        webhook::Inbox,
    };
    let (source, base, _, _) = fixture();
    let mut heads = Vec::new();
    for (branch, content) in [("conflict-one", "first\n"), ("conflict-two", "second\n")] {
        git(source.path(), &["checkout", "-b", branch, &base]);
        std::fs::write(source.path().join("shared"), content).unwrap();
        git(source.path(), &["add", "."]);
        git(
            source.path(),
            &["-c", "commit.gpgsign=false", "commit", "-m", branch],
        );
        heads.push(git(source.path(), &["rev-parse", "HEAD"]));
    }
    git(source.path(), &["checkout", "main"]);
    let remote = tempfile::tempdir().unwrap();
    git(remote.path(), &["init", "--bare"]);
    let data = tempfile::tempdir().unwrap();
    let db = data.path().join("queue.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    inbox
        .record("initial", "pull_request", &[1; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let admitted: Vec<_> = heads
        .into_iter()
        .enumerate()
        .map(|(n, head_sha)| AdmissionCandidate {
            candidate: Candidate {
                number: n as u64 + 1,
                head_sha,
            },
            evidence_digest: format!("{:064x}", n + 1),
        })
        .collect();
    store
        .apply_admission_reconciliation(&event, &"e".repeat(64), &admitted)
        .unwrap();
    let id = "batch-0123456789abcdef";
    store.plan_batch(id, &base, 4, 1).unwrap().unwrap();
    assert!(
        prepare_local(
            &mut store,
            id,
            source.path(),
            remote.path(),
            &["shared".into()]
        )
        .is_err()
    );
    assert!(matches!(
        store.batch(id).unwrap().unwrap().0.state,
        State::Failed { .. }
    ));
    assert_eq!(
        git(remote.path(), &["for-each-ref", "--format=%(refname)"]),
        ""
    );
    assert_eq!(git(source.path(), &["rev-parse", "main"]), base);
}

#[test]
fn fetches_frozen_provider_refs_and_rejects_moved_inputs() {
    use sluss::{
        batch::{Batch, Candidate},
        integration::{FetchError, fetch_local},
    };
    let (source, base, one, two) = fixture();
    let remote = tempfile::tempdir().unwrap();
    git(remote.path(), &["init", "--bare"]);
    git(
        source.path(),
        &[
            "push",
            remote.path().to_str().unwrap(),
            "main:refs/heads/main",
            "one:refs/pull/1/head",
            "two:refs/pull/2/head",
        ],
    );
    let batch = Batch::new(
        "batch-0123456789abcdef".into(),
        base.clone(),
        vec![
            Candidate {
                number: 1,
                head_sha: one.clone(),
            },
            Candidate {
                number: 2,
                head_sha: two.clone(),
            },
        ],
    )
    .unwrap();
    let fetched = fetch_local(remote.path(), "main", &batch).unwrap();
    let repo = fetched.repository();
    let integration = build(&repo, &base, &[one.clone(), two.clone()]).unwrap();
    let direct = build(source.path(), &base, &[one.clone(), two.clone()]).unwrap();
    assert_eq!(integration.sha(), direct.sha());
    assert!(matches!(
        fetch_local(remote.path(), "../other", &batch),
        Err(FetchError::Invalid)
    ));
    git(remote.path(), &["update-ref", "refs/pull/1/head", &two]);
    assert!(matches!(
        fetch_local(remote.path(), "main", &batch),
        Err(FetchError::Stale)
    ));
    git(remote.path(), &["update-ref", "refs/pull/1/head", &one]);
    git(remote.path(), &["update-ref", "refs/heads/main", &two]);
    assert!(matches!(
        fetch_local(remote.path(), "main", &batch),
        Err(FetchError::Stale)
    ));
    drop(fetched);
    assert!(!repo.exists());
}

#[test]
fn selective_staging_freezes_suites_atomically_and_recovers_after_publication() {
    use sluss::{
        batch::{Candidate, State},
        coordinator::{StagingOptions, prepare_local_from_remote_with_options},
        selective::{FrozenSelection, Policy, Selection},
        store::{AdmissionCandidate, Store, StoreError},
        webhook::Inbox,
    };
    let (source, base, one, _) = fixture();
    let remote = tempfile::tempdir().unwrap();
    git(remote.path(), &["init", "--bare"]);
    git(
        source.path(),
        &[
            "push",
            remote.path().to_str().unwrap(),
            "main:refs/heads/main",
            "one:refs/pull/1/head",
        ],
    );
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    let policy_path = dir.path().join("verification.toml");
    std::fs::write(&policy_path, "version=1\nfull_suites=['fast','other']\n[[components]]\nname='one'\npaths=['one']\nsuites=['fast']\n").unwrap();
    let policy = Policy::load(&policy_path).unwrap();
    let digest = policy.digest().unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    inbox
        .record("initial", "pull_request", &[5; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    store
        .apply_admission_reconciliation(
            &event,
            &"f".repeat(64),
            &[AdmissionCandidate {
                candidate: Candidate {
                    number: 1,
                    head_sha: one.clone(),
                },
                evidence_digest: "e".repeat(64),
            }],
        )
        .unwrap();
    let id = "batch-0123456789abcdef";
    store.plan_batch(id, &base, 1, 1).unwrap().unwrap();
    store.bind_policy_paths(id, &["shared".into()]).unwrap();
    store.bind_staging_selection(id, Some(&digest)).unwrap();
    store.start_integration(id).unwrap();
    let built = build(source.path(), &base, std::slice::from_ref(&one)).unwrap();
    let snapshot = built.check_policy(&base, &["shared".into()]).unwrap();
    let invalid = FrozenSelection {
        base_sha: base.clone(),
        integration_sha: built.sha().into(),
        policy_digest: digest.clone(),
        selection: Selection::Selected {
            suites: Default::default(),
            components: ["one".into()].into(),
        },
    };
    assert!(matches!(
        store.finish_integration(id, built.sha(), &snapshot.digest),
        Err(StoreError::PolicyMismatch)
    ));
    assert!(matches!(
        store.finish_integration_with_selection(id, built.sha(), &snapshot.digest, Some(&invalid)),
        Err(StoreError::InvalidBatch)
    ));
    let wrong_base = FrozenSelection {
        base_sha: "0".repeat(40),
        integration_sha: built.sha().into(),
        policy_digest: digest.clone(),
        selection: Selection::Selected {
            suites: ["fast".into()].into(),
            components: ["one".into()].into(),
        },
    };
    assert!(matches!(
        store.finish_integration_with_selection(
            id,
            built.sha(),
            &snapshot.digest,
            Some(&wrong_base)
        ),
        Err(StoreError::StaleBatch)
    ));
    assert_eq!(store.batch(id).unwrap().unwrap().0.state, State::Building);
    assert!(store.batch_selection(id).unwrap().is_none());
    assert!(store.batch_policy_digest(id).unwrap().is_none());
    let conn = rusqlite::Connection::open(&db).unwrap();
    assert!(
        !conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM batch_policy_paths WHERE batch_id=?1)",
                [id],
                |r| r.get::<_, bool>(0)
            )
            .unwrap()
    );
    // Simulate interruption after publishing the staging ref but before saving Testing.
    let published = built.stage_local(remote.path(), id).unwrap();
    drop(store);
    let mut store = Store::open(&db).unwrap();
    let prepared = prepare_local_from_remote_with_options(
        &mut store,
        id,
        remote.path(),
        "main",
        StagingOptions {
            policy_paths: &["shared".into()],
            verification_policy: Some(&policy),
        },
    )
    .unwrap();
    assert_eq!(prepared.sha, published.sha);
    let repeated = prepare_local_from_remote_with_options(
        &mut store,
        id,
        remote.path(),
        "main",
        StagingOptions {
            policy_paths: &["shared".into()],
            verification_policy: Some(&policy),
        },
    )
    .unwrap();
    assert_eq!(repeated, prepared);
    assert_eq!(
        store.batch(id).unwrap().unwrap().0.state,
        State::Testing {
            integration_sha: published.sha.clone()
        }
    );
    let frozen = store.batch_selection(id).unwrap().unwrap();
    assert_eq!(frozen.base_sha, base);
    assert_eq!(frozen.integration_sha, published.sha);
    let paths: String = conn
        .query_row(
            "SELECT paths FROM batch_policy_paths WHERE batch_id=?1",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Vec<String>>(&paths).unwrap(),
        ["shared"]
    );
    assert_eq!(frozen.policy_digest, digest);
    assert_eq!(
        frozen.selection.suites(),
        &["fast".to_string()].into_iter().collect()
    );
    let mut changed = policy.clone();
    changed.full_suites.push("new-suite".into());
    assert!(matches!(
        store.bind_staging_selection(id, Some(&changed.digest().unwrap())),
        Err(StoreError::PolicyMismatch)
    ));
    assert_eq!(store.batch_selection(id).unwrap().unwrap(), frozen);
    // Reopen a database bound to the prior algorithm digest. The new policy
    // cannot silently continue using selections frozen by that version.
    let legacy = {
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        hash.update(b"sluss-selective-policy-v1\0");
        hash.update(serde_json::to_vec(&policy).unwrap());
        hex::encode(hash.finalize())
    };
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute(
        "UPDATE staging_selection_config SET mode=?1 WHERE id=1",
        [&legacy],
    )
    .unwrap();
    drop(store);
    let mut store = Store::open(&db).unwrap();
    assert!(matches!(
        store.bind_staging_selection(id, Some(&digest)),
        Err(StoreError::PolicyMismatch)
    ));
}

#[test]
fn selective_bisection_stages_the_same_full_suite_for_comparable_halves() {
    use sluss::{
        batch::Candidate,
        coordinator::{StagingOptions, prepare_local_from_remote_with_options},
        selective::{Policy, Selection},
        store::{AdmissionCandidate, Store},
        webhook::Inbox,
    };
    let (source, base, one, two) = fixture();
    let remote = tempfile::tempdir().unwrap();
    git(remote.path(), &["init", "--bare"]);
    git(
        source.path(),
        &[
            "push",
            remote.path().to_str().unwrap(),
            "main:refs/heads/main",
            "one:refs/pull/1/head",
            "two:refs/pull/2/head",
        ],
    );
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    let policy_path = dir.path().join("verification.toml");
    std::fs::write(&policy_path, "version=1\nfull_suites=['fast','other']\n[[components]]\nname='one'\npaths=['one']\nsuites=['fast']\n[[components]]\nname='two'\npaths=['two']\nsuites=['other']\n").unwrap();
    let policy = Policy::load(&policy_path).unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    inbox
        .record("initial", "pull_request", &[6; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    store
        .apply_admission_reconciliation(
            &event,
            &"f".repeat(64),
            &[
                AdmissionCandidate {
                    candidate: Candidate {
                        number: 1,
                        head_sha: one,
                    },
                    evidence_digest: "e".repeat(64),
                },
                AdmissionCandidate {
                    candidate: Candidate {
                        number: 2,
                        head_sha: two,
                    },
                    evidence_digest: "d".repeat(64),
                },
            ],
        )
        .unwrap();
    let root = "batch-0123456789abcdef";
    store.plan_batch(root, &base, 2, 1).unwrap().unwrap();
    store.start_integration(root).unwrap();
    store
        .fail_integration(root, "reproducible conflict")
        .unwrap();
    let child = store
        .plan_batch("batch-1123456789abcdef", &base, 2, 1)
        .unwrap()
        .unwrap();
    assert!(store.is_split_batch(&child.id).unwrap());
    prepare_local_from_remote_with_options(
        &mut store,
        &child.id,
        remote.path(),
        "main",
        StagingOptions {
            policy_paths: &["shared".into()],
            verification_policy: Some(&policy),
        },
    )
    .unwrap();
    let frozen = store.batch_selection(&child.id).unwrap().unwrap();
    assert!(matches!(frozen.selection, Selection::Full { .. }));
    assert_eq!(
        frozen.selection.suites(),
        &["fast".to_string(), "other".to_string()]
            .into_iter()
            .collect()
    );
}

#[test]
fn reviewed_commit_policy_digest_matches_staging_and_ignores_worktree_changes() {
    use sluss::integration::policy_snapshot_at_commit;
    let (dir, base, one, _) = fixture();
    let integration = build(dir.path(), &base, &[one]).unwrap();
    let paths = vec!["shared".to_string()];
    let from_staging = integration.check_policy(&base, &paths).unwrap();
    std::fs::write(dir.path().join("shared"), "uncommitted change\n").unwrap();
    let reviewed = policy_snapshot_at_commit(dir.path(), &base, &paths).unwrap();
    assert_eq!(reviewed, from_staging);
    let database = dir.path().join("unused.sqlite");
    let output = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&database)
        .args(["policy-digest", "--source"])
        .arg(dir.path())
        .args(["--commit", &base, "--policy-file", "shared"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains(&reviewed.digest));
    assert!(text.contains("independently verified"));
    assert!(!database.exists());
    assert!(policy_snapshot_at_commit(dir.path(), &"0".repeat(40), &paths).is_err());
    assert!(policy_snapshot_at_commit(dir.path(), &base, &["missing".into()]).is_err());
}

#[test]
fn github_staging_inspects_all_workflows_in_the_exact_integration_commit() {
    #[cfg(unix)]
    use sluss::integration::StageError;
    use sluss::integration::StagingWorkflowError;
    let source = tempfile::tempdir().unwrap();
    git(source.path(), &["init", "-b", "main"]);
    std::fs::create_dir_all(source.path().join(".github/workflows")).unwrap();
    std::fs::write(
        source.path().join(".github/workflows/ci.yml"),
        "on:\n  push:\n    branches: [main, 'releases/**']\n  workflow_dispatch:\n",
    )
    .unwrap();
    git(source.path(), &["add", "."]);
    git(
        source.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "base"],
    );
    let base = git(source.path(), &["rev-parse", "HEAD"]);
    git(source.path(), &["checkout", "-b", "safe"]);
    std::fs::write(source.path().join("change"), "safe\n").unwrap();
    git(source.path(), &["add", "."]);
    git(
        source.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "safe"],
    );
    let safe = git(source.path(), &["rev-parse", "HEAD"]);
    let id = "batch-0123456789abcdef";
    let integration = build(source.path(), &base, &[safe]).unwrap();
    integration.check_staging_push_workflows(id).unwrap();
    git(source.path(), &["checkout", "-b", "unsafe", &base]);
    // This file was not in the base or any operator-selected policy list.
    std::fs::write(
        source.path().join(".github/workflows/new.yml"),
        "on: push\n",
    )
    .unwrap();
    git(source.path(), &["add", "."]);
    git(
        source.path(),
        &[
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "unsafe workflow",
        ],
    );
    let unsafe_head = git(source.path(), &["rev-parse", "HEAD"]);
    let integration = build(source.path(), &base, &[unsafe_head]).unwrap();
    assert!(
        matches!(integration.check_staging_push_workflows(id), Err(StagingWorkflowError::PushTrigger(path)) if path == ".github/workflows/new.yml")
    );
    #[cfg(unix)]
    assert!(matches!(
        integration.stage_github("owner", "repo", "not-a-real-token", id),
        Err(StageError::Workflow(StagingWorkflowError::PushTrigger(_)))
    ));
    git(source.path(), &["checkout", "-b", "ambiguous", &base]);
    std::fs::write(
        source.path().join(".github/workflows/new.yaml"),
        "on: {push: {branches: *unknown}}\n",
    )
    .unwrap();
    git(source.path(), &["add", "."]);
    git(
        source.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "ambiguous"],
    );
    let ambiguous = git(source.path(), &["rev-parse", "HEAD"]);
    let integration = build(source.path(), &base, &[ambiguous]).unwrap();
    assert!(
        matches!(integration.check_staging_push_workflows(id), Err(StagingWorkflowError::Ambiguous(path)) if path == ".github/workflows/new.yaml")
    );
    git(source.path(), &["checkout", "-b", "oversized", &base]);
    std::fs::write(
        source.path().join(".github/workflows/large.yml"),
        vec![b'x'; 256 * 1024 + 1],
    )
    .unwrap();
    git(source.path(), &["add", "."]);
    git(
        source.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "oversized"],
    );
    let head = git(source.path(), &["rev-parse", "HEAD"]);
    let integration = build(source.path(), &base, &[head]).unwrap();
    assert!(
        matches!(integration.check_staging_push_workflows(id), Err(StagingWorkflowError::Ambiguous(path)) if path == ".github/workflows/large.yml")
    );
    #[cfg(unix)]
    {
        git(source.path(), &["checkout", "-b", "symlink", &base]);
        std::os::unix::fs::symlink("ci.yml", source.path().join(".github/workflows/link.yml"))
            .unwrap();
        git(source.path(), &["add", "."]);
        git(
            source.path(),
            &["-c", "commit.gpgsign=false", "commit", "-m", "symlink"],
        );
        let head = git(source.path(), &["rev-parse", "HEAD"]);
        let integration = build(source.path(), &base, &[head]).unwrap();
        assert!(
            matches!(integration.check_staging_push_workflows(id), Err(StagingWorkflowError::Ambiguous(path)) if path == ".github/workflows/link.yml")
        );
        git(source.path(), &["checkout", "-b", "parent-link", &base]);
        std::fs::remove_dir_all(source.path().join(".github")).unwrap();
        std::os::unix::fs::symlink("elsewhere", source.path().join(".github")).unwrap();
        git(source.path(), &["add", "-A"]);
        git(
            source.path(),
            &["-c", "commit.gpgsign=false", "commit", "-m", "parent link"],
        );
        let head = git(source.path(), &["rev-parse", "HEAD"]);
        let integration = build(source.path(), &base, &[head]).unwrap();
        assert!(
            matches!(integration.check_staging_push_workflows(id), Err(StagingWorkflowError::Ambiguous(path)) if path == ".github")
        );
    }
}

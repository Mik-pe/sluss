use sluss::config::{Config, ConfigError};
const VALID: &str = r#"version = 1
[github]
app_client_id = "abc"
installation_id = 42
repository_id = 7
owner = "owner"
repository = "repo"
[queue]
target = "main"
[checks]
required_reviews = 1
block_labels = ["do-not-merge"]
[merge]
mode = "disabled"
"#;
#[test]
fn accepts_disabled_mode_and_rejects_unsupported_merge_or_unknown_settings() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sluss.toml");
    std::fs::write(&path, VALID).unwrap();
    let config = Config::load(&path).unwrap();
    assert_eq!(config.policy().repository, 7);
    assert_eq!(config.queue.rescan_seconds, 300);
    assert_eq!(config.queue.max_batch_size, 4);
    assert_eq!(config.queue.max_active_batches, 1);
    assert_eq!(config.queue.debounce_seconds, 60);
    assert_eq!(config.queue.batch_window_seconds, 60);
    for extra in [
        "max_batch_size = 0",
        "max_active_batches = 101",
        "batch_window_seconds = 86401",
        "debounce_seconds = 86401",
    ] {
        std::fs::write(
            &path,
            VALID.replace("target = \"main\"", &format!("target = \"main\"\n{extra}")),
        )
        .unwrap();
        assert!(
            matches!(Config::load(&path), Err(ConfigError::Invalid)),
            "{extra}"
        );
    }
    std::fs::write(&path, VALID.replace("disabled", "verified-fast-forward")).unwrap();
    assert!(matches!(Config::load(&path), Err(ConfigError::Invalid)));
    std::fs::write(&path, format!("{VALID}\nextra = true\n")).unwrap();
    assert!(matches!(Config::load(&path), Err(ConfigError::Invalid)));
}

#[test]
fn published_example_is_valid() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/sluss.toml");
    assert!(Config::load(&path).is_ok());
}

#[test]
fn example_capacity_has_valid_host_labels_and_can_open_scheduler() {
    let capacity: sluss::scheduler::Capacity =
        toml::from_str(include_str!("../examples/capacity.toml")).unwrap();
    assert!(
        capacity
            .hosts
            .keys()
            .all(|label| sluss::github::valid_runner_label(label))
    );
    let dir = tempfile::tempdir().unwrap();
    sluss::scheduler::Scheduler::open(dir.path().join("capacity.sqlite"), capacity).unwrap();
}

#[test]
fn adaptive_batching_requires_single_active_batch_and_ci_completion_worker() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("adaptive.toml");
    let adaptive = VALID.replace(
        "target = \"main\"",
        "target = \"main\"\nadaptive_batching = true",
    );
    std::fs::write(&config, &adaptive).unwrap();
    assert!(Config::load(&config).unwrap().planning().adaptive_batching);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(dir.path().join("queue.sqlite"))
        .args(["serve", "--auto-plan", "--config"])
        .arg(&config)
        .env("SLUSS_WEBHOOK_SECRET", "test")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires --complete-ci-only"));
    std::fs::write(
        &config,
        adaptive.replace(
            "adaptive_batching = true",
            "adaptive_batching = true\nmax_active_batches = 2",
        ),
    )
    .unwrap();
    assert!(matches!(Config::load(&config), Err(ConfigError::Invalid)));
}

#[test]
fn invalid_worker_configuration_fails_before_creating_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("sluss.toml");
    let capacity = dir.path().join("capacity.toml");
    let db = dir.path().join("queue.sqlite");
    std::fs::write(&config, VALID).unwrap();
    let run = |extra: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(&db)
            .args(["serve", "--config"])
            .arg(&config)
            .args(extra)
            .env("SLUSS_WEBHOOK_SECRET", "test")
            .env_remove("SLUSS_GITHUB_PRIVATE_KEY_PATH")
            .output()
            .unwrap()
    };
    let cap = capacity.to_str().unwrap();
    let flags = [
        "--auto-plan",
        "--auto-stage",
        "--policy-file",
        ".github/workflows/ci.yml",
        "--auto-dispatch",
        "--capacity",
        cap,
        "--workflow-id",
        "17",
        "--required-job",
        "test",
        "--pool",
        "rust=1",
    ];
    std::fs::write(&capacity, "not valid toml").unwrap();
    let output = run(&flags);
    assert!(!output.status.success());
    assert!(
        !db.exists(),
        "a worker or inbox touched the database before validation"
    );

    std::fs::write(
        &capacity,
        "[pools]\nrust = 1\n[hosts.sluss-host-one]\nslots = 0\npools = {rust = 1}\n",
    )
    .unwrap();
    let output = run(&flags);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("capacity configuration or request is invalid")
    );
    assert!(!db.exists());

    std::fs::write(
        &capacity,
        "[pools]\nrust = 1\n[hosts.sluss-host-one]\nslots = 1\npools = {rust = 1}\n",
    )
    .unwrap();
    let output = run(&["--auto-stage", "--policy-file", "../outside"]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("invalid or duplicate CI policy paths")
    );
    assert!(!db.exists());
    let output = run(&[
        "--auto-dispatch",
        "--capacity",
        cap,
        "--workflow-id",
        "17",
        "--required-job",
        "test",
        "--pool",
        "rust=2",
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unsatisfiable dispatch capacity"));
    assert!(!db.exists());
    let output = run(&[
        "--observe-dispatches",
        "--complete-ci-only",
        "--release-ci-only",
        "--capacity",
        cap,
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("exactly one registered runner"));
    assert!(!db.exists());
}

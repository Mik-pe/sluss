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

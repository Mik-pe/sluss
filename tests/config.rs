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

use sluss::setup::{RegistrationError, RegistrationProfile, registration_url};
use std::{collections::BTreeMap, process::Command};

fn parameters(url: &reqwest::Url) -> BTreeMap<String, Vec<String>> {
    let mut result = BTreeMap::<String, Vec<String>>::new();
    for (key, value) in url.query_pairs() {
        result
            .entry(key.into_owned())
            .or_default()
            .push(value.into_owned());
    }
    result
}
#[test]
fn observation_registration_is_private_read_only_and_webhook_free_by_default() {
    let url = registration_url("Sluss test", None, RegistrationProfile::Observe, None).unwrap();
    assert_eq!(
        url.as_str().split('?').next().unwrap(),
        "https://github.com/settings/apps/new"
    );
    let params = parameters(&url);
    for name in [
        "actions",
        "checks",
        "contents",
        "pull_requests",
        "administration",
    ] {
        assert_eq!(params[name], ["read"]);
    }
    assert_eq!(params["public"], ["false"]);
    assert_eq!(params["request_oauth_on_install"], ["false"]);
    assert_eq!(params["webhook_active"], ["false"]);
    assert!(!params.contains_key("events[]"));
}
#[test]
fn ci_registration_scopes_events_and_does_not_embed_a_secret() {
    let url = registration_url(
        "Sluss & demo",
        Some("my-org"),
        RegistrationProfile::Ci,
        Some("https://example.test/hooks/sluss"),
    )
    .unwrap();
    assert_eq!(url.path(), "/organizations/my-org/settings/apps/new");
    let params = parameters(&url);
    for name in ["actions", "checks", "contents"] {
        assert_eq!(params[name], ["write"]);
    }
    assert_eq!(params["pull_requests"], ["read"]);
    assert_eq!(params["administration"], ["read"]);
    assert_eq!(params["name"], ["Sluss & demo"]);
    assert_eq!(params["webhook_url"], ["https://example.test/hooks/sluss"]);
    for event in [
        "push",
        "pull_request",
        "pull_request_review",
        "workflow_run",
        "check_run",
        "check_suite",
        "repository",
        "repository_ruleset",
        "branch_protection_rule",
    ] {
        assert!(params["events[]"].contains(&event.to_string()));
    }
    assert!(!url.as_str().contains("secret"));
    assert!(!params.contains_key("members"));
}
#[test]
fn rejects_malformed_or_credential_bearing_urls() {
    for webhook in [
        "http://example.test/x",
        "https://user:pass@example.test/x",
        "https://example.test/x?token=secret",
        "https://example.test/x#fragment",
        "not a url",
    ] {
        assert!(matches!(
            registration_url("test", None, RegistrationProfile::Ci, Some(webhook)),
            Err(RegistrationError::InvalidWebhook)
        ));
    }
    for name in ["", " spaced", "line\nbreak"] {
        assert!(matches!(
            registration_url(name, None, RegistrationProfile::Observe, None),
            Err(RegistrationError::InvalidIdentity)
        ));
    }
    assert!(matches!(
        registration_url(
            "test",
            Some("../escape"),
            RegistrationProfile::Observe,
            None
        ),
        Err(RegistrationError::InvalidIdentity)
    ));
}
#[test]
fn cli_generates_link_without_accessing_github_or_creating_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("unused.sqlite");
    let output = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&db)
        .args(["app-registration-url", "--name", "Sluss Test"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let out = String::from_utf8_lossy(&output.stdout);
    assert!(out.contains("https://github.com/settings/apps/new?"));
    assert!(out.contains("creates no App or installation"));
    assert!(!db.exists());
}

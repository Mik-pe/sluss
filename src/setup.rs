//! Build a GitHub-hosted registration URL. This does not register or install an App.
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationProfile {
    Observe,
    Ci,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistrationError {
    #[error("invalid App name or organization")]
    InvalidIdentity,
    #[error("webhook URL must be HTTPS without credentials, query or fragment")]
    InvalidWebhook,
}

/// Construct only a github.com URL; a user must review and submit its settings.
/// The webhook secret must be set separately in GitHub's UI.
pub fn registration_url(
    name: &str,
    organization: Option<&str>,
    profile: RegistrationProfile,
    webhook: Option<&str>,
) -> Result<reqwest::Url, RegistrationError> {
    if name.trim() != name
        || name.is_empty()
        || name.len() > 100
        || name.chars().any(char::is_control)
        || organization.is_some_and(|org| !crate::github::valid_name(org))
    {
        return Err(RegistrationError::InvalidIdentity);
    }
    let webhook = webhook
        .map(|value| {
            let url = reqwest::Url::parse(value).map_err(|_| RegistrationError::InvalidWebhook)?;
            if url.scheme() != "https"
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(RegistrationError::InvalidWebhook);
            }
            Ok(url)
        })
        .transpose()?;
    let mut url = reqwest::Url::parse("https://github.com/").expect("static GitHub URL");
    {
        let mut path = url.path_segments_mut().expect("GitHub URL can be a base");
        path.clear();
        if let Some(org) = organization {
            path.push("organizations").push(org);
        }
        path.push("settings").push("apps").push("new");
    }
    {
        let mut query = url.query_pairs_mut();
        query
            .append_pair("name", name)
            .append_pair("url", "https://github.com/Mik-pe/sluss")
            .append_pair("description", "Sluss CI orchestration")
            .append_pair("public", "false")
            .append_pair("request_oauth_on_install", "false")
            .append_pair("administration", "read")
            .append_pair("pull_requests", "read");
        let level = match profile {
            RegistrationProfile::Observe => "read",
            RegistrationProfile::Ci => "write",
        };
        query
            .append_pair("actions", level)
            .append_pair("checks", level)
            .append_pair("contents", level);
        if let Some(webhook) = webhook {
            query
                .append_pair("webhook_active", "true")
                .append_pair("webhook_url", webhook.as_str());
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
                query.append_pair("events[]", event);
            }
        } else {
            query.append_pair("webhook_active", "false");
        }
    }
    Ok(url)
}

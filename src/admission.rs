//! Evaluate a PR observation for CI planning under an explicit reviewer policy.
//! This module neither authenticates its inputs nor authorizes a merge.
use crate::github::PullSnapshot;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Policy {
    pub target_branch: String,
    pub required_reviews: usize,
    pub block_labels: BTreeSet<String>,
    /// Only independently verified and currently authorized user IDs belong here.
    pub authorized_reviewers: BTreeSet<u64>,
}
#[derive(Debug, PartialEq, Eq)]
pub struct EligibleForCi {
    pub pull_number: u64,
    pub head_sha: String,
    pub base_sha: String,
    pub reviewers: BTreeSet<u64>,
}
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AdmissionError {
    #[error("admission policy or observation is incomplete")]
    Invalid,
    #[error("pull request is not open, is draft, or targets another branch")]
    NotReady,
    #[error("a blocking label is present")]
    BlockedLabel,
    #[error("an authorized reviewer has requested changes")]
    ChangesRequested,
    #[error("not enough independent current-head approvals")]
    InsufficientReviews,
}

/// Eligibility permits CI planning only. The caller must obtain current
/// reviewer authorization and re-read PRs, reviews and policy before any
/// later action; this computation is not a GitHub mergeability check.
pub fn evaluate(snapshot: &PullSnapshot, policy: &Policy) -> Result<EligibleForCi, AdmissionError> {
    let pull = &snapshot.pull;
    if policy.target_branch.is_empty()
        || policy.required_reviews == 0
        || policy.authorized_reviewers.contains(&0)
        || policy.block_labels.iter().any(|s| s.is_empty())
        || pull.number == 0
        || pull.user.id == 0
        || !valid_sha(&pull.head.sha)
        || !valid_sha(&pull.base.sha)
    {
        return Err(AdmissionError::Invalid);
    }
    if pull.state != "open" || pull.draft || pull.base.branch != policy.target_branch {
        return Err(AdmissionError::NotReady);
    }
    if pull.labels.iter().any(|label| {
        policy
            .block_labels
            .iter()
            .any(|blocked| blocked.to_lowercase() == label.name.to_lowercase())
    }) {
        return Err(AdmissionError::BlockedLabel);
    }
    let mut decisions: BTreeMap<u64, (&str, Option<&str>)> = BTreeMap::new();
    let mut last_time = None;
    let mut ids = BTreeSet::new();
    for review in &snapshot.reviews {
        if review.id == 0 || !ids.insert(review.id) {
            return Err(AdmissionError::Invalid);
        }
        match review.state.as_str() {
            "PENDING" => continue,
            "APPROVED" | "CHANGES_REQUESTED" | "DISMISSED" | "COMMENTED" => {}
            _ => return Err(AdmissionError::Invalid),
        }
        let user = review.user.as_ref().ok_or(AdmissionError::Invalid)?;
        let timestamp = review
            .submitted_at
            .as_ref()
            .ok_or(AdmissionError::Invalid)?;
        let submitted =
            time::OffsetDateTime::parse(timestamp, &time::format_description::well_known::Rfc3339)
                .map_err(|_| AdmissionError::Invalid)?;
        if user.id == 0 || last_time.is_some_and(|old| submitted < old) {
            return Err(AdmissionError::Invalid);
        }
        last_time = Some(submitted);
        if !policy.authorized_reviewers.contains(&user.id)
            || user.id == pull.user.id
            || user.kind != "User"
        {
            continue;
        }
        match review.state.as_str() {
            "APPROVED" | "CHANGES_REQUESTED" | "DISMISSED" => {
                decisions.insert(
                    user.id,
                    (review.state.as_str(), review.commit_id.as_deref()),
                );
            }
            "COMMENTED" => {}
            _ => unreachable!(),
        }
    }
    if decisions
        .values()
        .any(|(state, _)| *state == "CHANGES_REQUESTED")
    {
        return Err(AdmissionError::ChangesRequested);
    }
    let reviewers: BTreeSet<u64> = decisions
        .into_iter()
        .filter_map(|(id, (state, commit))| {
            (state == "APPROVED" && commit == Some(pull.head.sha.as_str())).then_some(id)
        })
        .collect();
    if reviewers.len() < policy.required_reviews {
        return Err(AdmissionError::InsufficientReviews);
    }
    Ok(EligibleForCi {
        pull_number: pull.number,
        head_sha: pull.head.sha.clone(),
        base_sha: pull.base.sha.clone(),
        reviewers,
    })
}
fn valid_sha(s: &str) -> bool {
    s.len() == 40
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

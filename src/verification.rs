//! Consistency checks for an eventual provider-fetched CI record.
//!
//! These checks do not authenticate their inputs, fetch GitHub, validate the
//! workflow's code or issue an attestation. They cannot authorize landing.
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedRun {
    pub repository_id: u64,
    pub run_id: u64,
    pub attempt: u32,
    pub workflow_id: u64,
    pub integration_sha: String,
    pub policy_digest: String,
    /// Names are exact and must uniquely identify all required jobs, including matrix entries.
    pub required_jobs: BTreeSet<String>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: u64,
    pub name: String,
    pub completed: bool,
    pub successful: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunObservation {
    pub repository_id: u64,
    pub run_id: u64,
    pub attempt: u32,
    pub workflow_id: u64,
    pub integration_sha: String,
    pub policy_digest: String,
    pub completed: bool,
    pub successful: bool,
    /// The provider client must establish that pagination and attempt selection are complete.
    pub jobs_complete: bool,
    pub jobs: Vec<Job>,
}
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EvidenceError {
    #[error("invalid expected identity, policy digest or required jobs")]
    InvalidExpectation,
    #[error("run identity, commit or policy differs from the frozen expectation")]
    IdentityMismatch,
    #[error("run is incomplete or unsuccessful")]
    RunNotSuccessful,
    #[error("the provider's job list is incomplete")]
    IncompleteJobs,
    #[error("job identities or names are missing or duplicated")]
    AmbiguousJobs,
    #[error("a required job is absent, incomplete or unsuccessful")]
    RequiredJobNotSuccessful,
}

/// A successful result indicates only that the supplied records agree. Callers
/// must independently establish authenticity, trusted policy and current state.
pub fn check_consistency(
    expected: &ExpectedRun,
    observed: &RunObservation,
) -> Result<(), EvidenceError> {
    if expected.repository_id == 0
        || expected.run_id == 0
        || expected.attempt == 0
        || expected.workflow_id == 0
        || !is_hex(&expected.integration_sha, 40)
        || !is_hex(&expected.policy_digest, 64)
        || expected.required_jobs.is_empty()
        || expected.required_jobs.iter().any(|j| j.is_empty())
    {
        return Err(EvidenceError::InvalidExpectation);
    }
    if expected.repository_id != observed.repository_id
        || expected.run_id != observed.run_id
        || expected.attempt != observed.attempt
        || expected.workflow_id != observed.workflow_id
        || expected.integration_sha != observed.integration_sha
        || expected.policy_digest != observed.policy_digest
    {
        return Err(EvidenceError::IdentityMismatch);
    }
    if !observed.completed || !observed.successful {
        return Err(EvidenceError::RunNotSuccessful);
    }
    if !observed.jobs_complete {
        return Err(EvidenceError::IncompleteJobs);
    }
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    for job in &observed.jobs {
        if job.id == 0 || job.name.is_empty() || !ids.insert(job.id) || !names.insert(&job.name) {
            return Err(EvidenceError::AmbiguousJobs);
        }
    }
    for required in &expected.required_jobs {
        if !observed
            .jobs
            .iter()
            .any(|job| &job.name == required && job.completed && job.successful)
        {
            return Err(EvidenceError::RequiredJobNotSuccessful);
        }
    }
    Ok(())
}
fn is_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// Placement observations do not enforce runner labels or validate policy.
/// The operator must bind every allowed runner ID to the reserved physical host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    Waiting,
    Confirmed { runner_id: u64 },
}
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PlacementError {
    #[error("the reserved host has no valid runner identities")]
    InvalidHost,
    #[error("multiple executable jobs cannot be covered by one host slot")]
    MultipleJobs,
    #[error("job ran on a runner outside the reserved physical host")]
    WrongHost,
    #[error("completed job has no runner identity")]
    MissingRunner,
}
/// Check a provider-observed snapshot for the conservative single-job routing
/// contract. This is not a guarantee that GitHub enforced the requested label.
pub fn check_host_placement(
    snapshot: &crate::github::ActionsSnapshot,
    allowed_runner_ids: &BTreeSet<u64>,
) -> Result<Placement, PlacementError> {
    if allowed_runner_ids.is_empty() || allowed_runner_ids.contains(&0) {
        return Err(PlacementError::InvalidHost);
    }
    let mut jobs = snapshot
        .jobs
        .iter()
        .filter(|job| !(job.status == "completed" && job.conclusion.as_deref() == Some("skipped")));
    let Some(job) = jobs.next() else {
        return Ok(Placement::Waiting);
    };
    if jobs.next().is_some() {
        return Err(PlacementError::MultipleJobs);
    }
    match job.runner_id {
        Some(id) if allowed_runner_ids.contains(&id) => Ok(Placement::Confirmed { runner_id: id }),
        Some(_) => Err(PlacementError::WrongHost),
        None if job.status == "completed" => Err(PlacementError::MissingRunner),
        None => Ok(Placement::Waiting),
    }
}

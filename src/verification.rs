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

#[derive(Debug, Error, PartialEq, Eq)]
pub enum InventoryError {
    #[error("physical hosts need distinct, nonzero runner IDs and valid labels")]
    InvalidConfiguration,
    #[error("registered runners do not match the configured host labels and IDs")]
    RunnerMismatch,
    #[error("a physical host has fewer idle, online runners than its slot budget")]
    InsufficientCapacity,
}
/// Compare repository-scoped runner registrations against the operator's
/// declared physical hosts. Organization/enterprise runners are not covered.
pub fn check_repository_runner_inventory(
    capacity: &crate::scheduler::Capacity,
    runners: &[crate::github::RepositoryRunner],
) -> Result<(), InventoryError> {
    let available = available_repository_hosts(capacity, runners)?;
    if available.len() != capacity.hosts.len() {
        return Err(InventoryError::InsufficientCapacity);
    }
    Ok(())
}

/// Validate the entire repository inventory and return only hosts whose full
/// configured slot budget is currently idle. Other busy hosts do not block an
/// independent reservation. Physical isolation remains an operator obligation.
pub fn available_repository_hosts(
    capacity: &crate::scheduler::Capacity,
    runners: &[crate::github::RepositoryRunner],
) -> Result<BTreeSet<String>, InventoryError> {
    let mut configured = BTreeSet::new();
    for (label, host) in &capacity.hosts {
        if !crate::github::valid_runner_label(label)
            || host.slots == 0
            || host.runner_ids.is_empty()
            || host
                .runner_ids
                .iter()
                .any(|id| *id == 0 || !configured.insert(*id))
        {
            return Err(InventoryError::InvalidConfiguration);
        }
    }
    if capacity.hosts.is_empty() {
        return Err(InventoryError::InvalidConfiguration);
    }
    let mut seen = BTreeSet::new();
    for runner in runners {
        if runner.id == 0 || !seen.insert(runner.id) {
            return Err(InventoryError::RunnerMismatch);
        }
        let matching: Vec<_> = capacity
            .hosts
            .iter()
            .filter(|(name, _)| {
                runner
                    .labels
                    .iter()
                    .any(|l| l.name.eq_ignore_ascii_case(name))
            })
            .collect();
        if matching.len() > 1 {
            return Err(InventoryError::RunnerMismatch);
        }
        if let Some((_, host)) = matching.first() {
            if !host.runner_ids.contains(&runner.id)
                || !runner
                    .labels
                    .iter()
                    .any(|l| l.name.eq_ignore_ascii_case("self-hosted"))
            {
                return Err(InventoryError::RunnerMismatch);
            }
        } else if configured.contains(&runner.id) {
            return Err(InventoryError::RunnerMismatch);
        }
    }
    let mut available_hosts = BTreeSet::new();
    for (label, host) in &capacity.hosts {
        let matches: Vec<_> = runners
            .iter()
            .filter(|runner| {
                host.runner_ids.contains(&runner.id)
                    && runner
                        .labels
                        .iter()
                        .any(|l| l.name.eq_ignore_ascii_case(label))
            })
            .collect();
        if matches.len() != host.runner_ids.len() {
            return Err(InventoryError::RunnerMismatch);
        }
        let available = matches
            .iter()
            .filter(|runner| runner.status == "online" && !runner.busy)
            .count();
        if available >= host.slots as usize {
            available_hosts.insert(label.clone());
        }
    }
    Ok(available_hosts)
}

/// A provider result comparison, not proof of trusted workflow policy.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CiObservation {
    Pending,
    Successful,
    Failed { conclusion: String },
}
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CiObservationError {
    #[error("run identity does not match the frozen dispatch")]
    Identity,
    #[error("job identities, names or conclusions are invalid or ambiguous")]
    Ambiguous,
    #[error("a required job is absent, incomplete, skipped or unsuccessful")]
    RequiredJob,
}
/// Compare exact run and job names. The caller must obtain authenticated,
/// complete provider observations and independently validate workflow policy.
pub fn observe_ci(
    plan: &crate::scheduler::DispatchPlan,
    run_id: u64,
    snapshot: &crate::github::ActionsSnapshot,
) -> Result<CiObservation, CiObservationError> {
    let run = &snapshot.run;
    let branch = plan
        .staging_ref
        .strip_prefix("refs/heads/")
        .ok_or(CiObservationError::Identity)?;
    if run_id == 0
        || plan.required_jobs.is_empty()
        || run.id != run_id
        || run.run_attempt != 1
        || run.repository.id != plan.repository_id
        || run.head_repository.as_ref().map(|r| r.id) != Some(plan.repository_id)
        || run.workflow_id != plan.workflow_id
        || run.head_sha != plan.integration_sha
        || run.head_branch.as_deref() != Some(branch)
        || run.event != "workflow_dispatch"
    {
        return Err(CiObservationError::Identity);
    }
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    for job in &snapshot.jobs {
        if job.id == 0
            || job.name.is_empty()
            || job.run_id != run_id
            || job.head_sha != run.head_sha
            || !ids.insert(job.id)
            || !names.insert(&job.name)
        {
            return Err(CiObservationError::Ambiguous);
        }
    }
    if run.status != "completed" {
        return Ok(CiObservation::Pending);
    }
    let conclusion = run
        .conclusion
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or(CiObservationError::Ambiguous)?;
    if conclusion != "success" {
        return Ok(CiObservation::Failed {
            conclusion: conclusion.into(),
        });
    }
    for required in &plan.required_jobs {
        if !snapshot.jobs.iter().any(|job| {
            &job.name == required
                && job.status == "completed"
                && job.conclusion.as_deref() == Some("success")
        }) {
            return Err(CiObservationError::RequiredJob);
        }
    }
    Ok(CiObservation::Successful)
}

/// Conservative envelope for reusing capacity when GitHub allows reruns.
/// The operator must also ensure no additional runner process shares a host
/// and that job cleanup/isolation prevents work surviving its runner job.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ReleaseTopologyError {
    #[error(
        "automatic CI-only release requires exactly one registered runner and one slot per physical host"
    )]
    Host,
    #[error("pool budgets must cover the sum of all host pool capacities")]
    Pool,
}
pub fn check_release_topology(
    capacity: &crate::scheduler::Capacity,
) -> Result<(), ReleaseTopologyError> {
    use std::collections::{BTreeMap, BTreeSet};
    let mut runners = BTreeSet::new();
    let mut totals: BTreeMap<&str, u64> = BTreeMap::new();
    if capacity.hosts.is_empty() || capacity.pools.is_empty() {
        return Err(ReleaseTopologyError::Host);
    }
    for (label, host) in &capacity.hosts {
        if !crate::github::valid_runner_label(label)
            || host.slots != 1
            || host.runner_ids.len() != 1
            || host.runner_ids.contains(&0)
            || !runners.insert(
                *host
                    .runner_ids
                    .iter()
                    .next()
                    .ok_or(ReleaseTopologyError::Host)?,
            )
            || host.pools.is_empty()
        {
            return Err(ReleaseTopologyError::Host);
        }
        for (name, units) in &host.pools {
            if *units == 0 || !capacity.pools.contains_key(name) {
                return Err(ReleaseTopologyError::Pool);
            }
            *totals.entry(name).or_default() += u64::from(*units);
        }
    }
    if totals.iter().any(|(name, used)| {
        capacity
            .pools
            .get(*name)
            .is_none_or(|limit| u64::from(*limit) < *used)
    }) {
        return Err(ReleaseTopologyError::Pool);
    }
    Ok(())
}

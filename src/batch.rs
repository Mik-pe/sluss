use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A snapshot of an admitted PR. Admission must be checked against the provider.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    pub number: u64,
    pub head_sha: String,
}

/// Commit identities are frozen before integration. A stale batch is never revived.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Batch {
    pub id: String,
    pub base_sha: String,
    pub candidates: Vec<Candidate>,
    pub state: State,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum State {
    Planned,
    Building,
    Testing {
        integration_sha: String,
    },
    /// A CI-only observation for scheduling; it is not a merge approval.
    CiComplete {
        integration_sha: String,
        run_id: u64,
        conclusion: String,
    },
    /// Aggregate scheduling result for all lanes; never a merge approval.
    CiCompleteLanes {
        integration_sha: String,
        lanes: BTreeMap<String, LaneResult>,
        conclusion: String,
    },
    /// This is a state-machine result, not an authorization to update a ref.
    Verified {
        integration_sha: String,
    },
    Failed {
        reason: String,
    },
    Invalidated {
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LaneResult {
    pub run_id: u64,
    pub conclusion: String,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum BatchError {
    #[error("batch id and base SHA must be nonempty, and the batch must contain PRs")]
    Empty,
    #[error("a PR number or head SHA is invalid")]
    InvalidCandidate,
    #[error("the batch contains a PR more than once")]
    DuplicateCandidate,
    #[error("invalid state transition")]
    InvalidTransition,
    #[error("integration SHA must be nonempty")]
    EmptyIntegration,
}

impl Batch {
    pub fn new(
        id: String,
        base_sha: String,
        candidates: Vec<Candidate>,
    ) -> Result<Self, BatchError> {
        if id.is_empty() || base_sha.is_empty() || candidates.is_empty() {
            return Err(BatchError::Empty);
        }
        let mut numbers = HashSet::new();
        for candidate in &candidates {
            if candidate.number == 0 || candidate.head_sha.is_empty() {
                return Err(BatchError::InvalidCandidate);
            }
            if !numbers.insert(candidate.number) {
                return Err(BatchError::DuplicateCandidate);
            }
        }
        Ok(Self {
            id,
            base_sha,
            candidates,
            state: State::Planned,
        })
    }

    pub fn begin_build(&mut self) -> Result<(), BatchError> {
        if self.state != State::Planned {
            return Err(BatchError::InvalidTransition);
        }
        self.state = State::Building;
        Ok(())
    }

    pub fn begin_testing(&mut self, integration_sha: String) -> Result<(), BatchError> {
        if self.state != State::Building {
            return Err(BatchError::InvalidTransition);
        }
        if integration_sha.is_empty() {
            return Err(BatchError::EmptyIntegration);
        }
        self.state = State::Testing { integration_sha };
        Ok(())
    }

    /// The caller must establish trusted CI evidence before using this transition.
    /// This method itself is deliberately not a landing authorization.
    pub fn record_success(&mut self) -> Result<(), BatchError> {
        match &self.state {
            State::Testing { integration_sha } => {
                self.state = State::Verified {
                    integration_sha: integration_sha.clone(),
                };
                Ok(())
            }
            _ => Err(BatchError::InvalidTransition),
        }
    }

    pub fn fail(&mut self, reason: String) -> Result<(), BatchError> {
        match self.state {
            State::Planned | State::Building | State::Testing { .. } => {
                self.state = State::Failed { reason };
                Ok(())
            }
            _ => Err(BatchError::InvalidTransition),
        }
    }

    pub fn invalidate(&mut self, reason: String) -> Result<(), BatchError> {
        match self.state {
            State::Planned | State::Building | State::Testing { .. } | State::Verified { .. } => {
                self.state = State::Invalidated { reason };
                Ok(())
            }
            _ => Err(BatchError::InvalidTransition),
        }
    }
}

/// Deterministic left-to-right partition. Singleton failures need investigation,
/// not further bisection; callers must independently rebuild and retest both halves.
pub fn bisect(candidates: &[Candidate]) -> Option<(&[Candidate], &[Candidate])> {
    if candidates.len() < 2 {
        return None;
    }
    Some(candidates.split_at(candidates.len() / 2))
}

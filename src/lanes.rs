//! Frozen descriptions of independent CI runs for one integration commit.
//!
//! A lane plan is only scheduling input. It does not dispatch work, prove CI,
//! or authorize a merge. Provider identity and current batch state must be
//! checked again when each lane is sent and observed.

use crate::scheduler::{Capacity, DispatchPlan, Request};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Operator-owned mapping from every selectable suite to a physical request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub version: u32,
    pub lanes: BTreeMap<String, Lane>,
}

/// The complete batch identity and its required suites and job names.
/// Each lane carries a subset of those suites and job names and may use a
/// different workflow and physical resource pool.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LanePlan {
    pub batch: DispatchPlan,
    pub lanes: BTreeMap<String, Lane>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lane {
    pub workflow_id: u64,
    pub suites: BTreeSet<String>,
    pub required_jobs: BTreeSet<String>,
    pub request: Request,
}

impl Policy {
    pub fn load(path: &std::path::Path) -> Result<Self, &'static str> {
        let bytes = std::fs::read(path).map_err(|_| "could not read lane policy")?;
        if bytes.len() > 65536 {
            return Err("lane policy is too large");
        }
        let text = std::str::from_utf8(&bytes).map_err(|_| "invalid lane policy")?;
        toml::from_str(text).map_err(|_| "invalid lane policy")
    }

    /// Resolve the current selection. The full policy must cover all suites
    /// declared by the selection policy, even if this batch selects fewer.
    pub fn plan(
        &self,
        mut batch: DispatchPlan,
        all_suites: &BTreeSet<String>,
        capacity: &Capacity,
    ) -> Result<LanePlan, &'static str> {
        if self.version != 1 || self.lanes.is_empty() || self.lanes.len() > 100 {
            return Err("invalid lane policy");
        }
        let mut declared = BTreeSet::new();
        for lane in self.lanes.values() {
            for suite in &lane.suites {
                if !declared.insert(suite.clone()) {
                    return Err("suite appears in multiple lanes");
                }
            }
        }
        if &declared != all_suites {
            return Err("lane policy does not cover the full verification policy");
        }
        // Validate even inactive lanes so an unrelated selection cannot hide
        // a malformed mapping or an impossible capacity request.
        let mut full = batch.clone();
        full.suites = declared;
        full.selection_policy_digest = Some("0".repeat(64));
        full.required_jobs = self
            .lanes
            .values()
            .flat_map(|lane| lane.required_jobs.iter().cloned())
            .collect();
        full.workflow_id = self
            .lanes
            .values()
            .next()
            .ok_or("invalid lane policy")?
            .workflow_id;
        if !(LanePlan {
            batch: full,
            lanes: self.lanes.clone(),
        })
        .validate(capacity)
        {
            return Err("invalid or unsatisfiable lane policy");
        }
        let lanes: BTreeMap<_, _> = self
            .lanes
            .iter()
            .filter_map(|(name, lane)| {
                let suites: BTreeSet<_> =
                    lane.suites.intersection(&batch.suites).cloned().collect();
                if suites.is_empty() {
                    None
                } else {
                    let mut selected = lane.clone();
                    selected.suites = suites;
                    Some((name.clone(), selected))
                }
            })
            .collect();
        batch.required_jobs = lanes
            .values()
            .flat_map(|lane| lane.required_jobs.iter().cloned())
            .collect();
        batch.workflow_id = lanes
            .values()
            .next()
            .ok_or("selection has no configured lane")?
            .workflow_id;
        let plan = LanePlan { batch, lanes };
        if !plan.validate(capacity) {
            return Err("invalid selected lane plan");
        }
        Ok(plan)
    }
}

impl LanePlan {
    pub(crate) fn dispatch(&self, name: &str) -> Option<(DispatchPlan, Request)> {
        let lane = self.lanes.get(name)?;
        let mut plan = self.batch.clone();
        plan.workflow_id = lane.workflow_id;
        plan.suites = lane.suites.clone();
        plan.required_jobs = lane.required_jobs.clone();
        Some((plan, lane.request.clone()))
    }

    pub(crate) fn dispatch_id(batch_id: &str, name: &str) -> String {
        format!("{batch_id}:lane:{name}")
    }
    /// Verify complete, non-overlapping suite coverage and that each lane can
    /// fit on at least one configured host. This says nothing about current
    /// availability or the correctness of the operator's pool declarations.
    pub fn validate(&self, capacity: &Capacity) -> bool {
        if !self.batch.valid() || self.lanes.is_empty() || self.lanes.len() > 100 {
            return false;
        }
        let mut suites = BTreeSet::new();
        let mut jobs = BTreeSet::new();
        for (name, lane) in &self.lanes {
            if name.is_empty()
                || name.len() > 100
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
                || lane.workflow_id == 0
                || lane.suites.is_empty()
                || lane.required_jobs.is_empty()
                || lane.request.pools.is_empty()
                || lane.request.pools.iter().any(|(pool, amount)| {
                    *amount == 0 || capacity.pools.get(pool).is_none_or(|limit| amount > limit)
                })
                || !capacity.hosts.values().any(|host| {
                    host.slots > 0
                        && lane.request.pools.iter().all(|(pool, amount)| {
                            host.pools.get(pool).is_some_and(|limit| amount <= limit)
                        })
                })
            {
                return false;
            }
            if lane
                .suites
                .iter()
                .any(|suite| !suites.insert(suite.clone()))
            {
                return false;
            }
            jobs.extend(lane.required_jobs.iter().cloned());
        }
        suites == self.batch.suites && jobs == self.batch.required_jobs
    }
}

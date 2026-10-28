//! Compare explicitly collected historical observations; this is not attestation.
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BenchmarkError {
    #[error("cannot read benchmark input")]
    Read,
    #[error("benchmark input is too large")]
    TooLarge,
    #[error("invalid benchmark JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid benchmark observation: {0}")]
    Invalid(&'static str),
    #[error("benchmark total exceeds supported range")]
    Overflow,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub version: u32,
    pub repository_id: u64,
    pub baseline: Cohort,
    pub sluss: Cohort,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cohort {
    pub start: String,
    pub end: String,
    /// Operator assertion that all relevant job attempts are included.
    pub jobs_complete: bool,
    /// Operator assertion that all delivered PRs are included.
    pub deliveries_complete: bool,
    pub jobs: Vec<Job>,
    pub deliveries: Vec<Delivery>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub job_id: u64,
    pub run_id: u64,
    pub attempt: u32,
    pub runner_id: u64,
    pub started_at: String,
    pub completed_at: String,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Delivery {
    pub pr: u64,
    pub ready_at: Option<String>,
    pub merged_at: String,
    /// Optional operator-supplied run association, not proof of verification.
    pub run_id: Option<u64>,
    pub attempt: Option<u32>,
}
#[derive(Debug, PartialEq)]
pub struct CohortReport {
    pub jobs: usize,
    pub attempts: usize,
    pub rerun_attempts: usize,
    pub runner_ms: u64,
    pub delivered: usize,
    pub attributed_deliveries: usize,
    pub attempts_with_delivery: usize,
    pub latency_samples: usize,
    pub median_latency_ms: Option<u64>,
    pub p90_latency_ms: Option<u64>,
    pub complete: bool,
}
#[derive(Debug, PartialEq)]
pub struct Report {
    pub baseline: CohortReport,
    pub sluss: CohortReport,
    /// Relative change in runner time per delivered PR, not a causal estimate.
    pub runner_time_change_percent: Option<f64>,
}
fn instant(value: &str) -> Result<i64, BenchmarkError> {
    crate::metrics::timestamp_ms(value).map_err(|_| BenchmarkError::Invalid("invalid timestamp"))
}
impl Input {
    pub fn load(path: &Path) -> Result<Self, BenchmarkError> {
        // Limit the bytes actually read, including if the file changes during read.
        use std::io::Read;
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .map_err(|_| BenchmarkError::Read)?
            .take(16 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| BenchmarkError::Read)?;
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(BenchmarkError::TooLarge);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }
    pub fn report(&self) -> Result<Report, BenchmarkError> {
        if self.version != 1 || self.repository_id == 0 {
            return Err(BenchmarkError::Invalid("version or repository ID"));
        }
        let a_start = instant(&self.baseline.start)?;
        let a_end = instant(&self.baseline.end)?;
        let b_start = instant(&self.sluss.start)?;
        let b_end = instant(&self.sluss.end)?;
        if a_start >= a_end || b_start >= b_end || a_end > b_start {
            return Err(BenchmarkError::Invalid(
                "cohorts must be ordered and non-overlapping",
            ));
        }
        let mut job_ids = BTreeSet::new();
        let mut runs = BTreeSet::new();
        let mut prs = BTreeSet::new();
        let baseline = cohort(
            &self.baseline,
            a_start,
            a_end,
            &mut job_ids,
            &mut runs,
            &mut prs,
        )?;
        let sluss = cohort(
            &self.sluss,
            b_start,
            b_end,
            &mut job_ids,
            &mut runs,
            &mut prs,
        )?;
        let change = if baseline.complete
            && sluss.complete
            && baseline.delivered > 0
            && sluss.delivered > 0
            && baseline.runner_ms > 0
            && sluss.runner_ms > 0
        {
            let old = baseline.runner_ms as f64 / baseline.delivered as f64;
            let new = sluss.runner_ms as f64 / sluss.delivered as f64;
            Some((new / old - 1.0) * 100.0)
        } else {
            None
        };
        Ok(Report {
            baseline,
            sluss,
            runner_time_change_percent: change,
        })
    }
}
fn cohort(
    input: &Cohort,
    start: i64,
    end: i64,
    all_jobs: &mut BTreeSet<u64>,
    previous_runs: &mut BTreeSet<u64>,
    all_prs: &mut BTreeSet<u64>,
) -> Result<CohortReport, BenchmarkError> {
    let mut attempts = BTreeSet::new();
    let mut runs = BTreeSet::new();
    let mut intervals: BTreeMap<u64, Vec<(i64, i64)>> = BTreeMap::new();
    let mut attempt_end: BTreeMap<(u64, u32), i64> = BTreeMap::new();
    let mut runner_ms = 0u64;
    for job in &input.jobs {
        if job.job_id == 0
            || job.run_id == 0
            || job.attempt == 0
            || job.runner_id == 0
            || !all_jobs.insert(job.job_id)
            || previous_runs.contains(&job.run_id)
        {
            return Err(BenchmarkError::Invalid(
                "duplicate or invalid job, run, attempt or runner",
            ));
        }
        let begin = instant(&job.started_at)?;
        let finish = instant(&job.completed_at)?;
        if begin < start || finish >= end || finish < begin {
            return Err(BenchmarkError::Invalid(
                "job outside cohort or negative duration",
            ));
        }
        let duration = u64::try_from(finish.checked_sub(begin).ok_or(BenchmarkError::Overflow)?)
            .map_err(|_| BenchmarkError::Overflow)?;
        runner_ms = runner_ms
            .checked_add(duration)
            .ok_or(BenchmarkError::Overflow)?;
        intervals
            .entry(job.runner_id)
            .or_default()
            .push((begin, finish));
        runs.insert(job.run_id);
        attempts.insert((job.run_id, job.attempt));
        attempt_end
            .entry((job.run_id, job.attempt))
            .and_modify(|old| *old = (*old).max(finish))
            .or_insert(finish);
    }
    for intervals in intervals.values_mut() {
        intervals.sort_unstable();
        if intervals.windows(2).any(|w| w[0].1 > w[1].0) {
            return Err(BenchmarkError::Invalid("overlapping jobs on one runner"));
        }
    }
    previous_runs.extend(runs);
    let mut latencies = Vec::new();
    let mut associated = BTreeSet::new();
    let mut attributed = 0;
    for delivery in &input.deliveries {
        if delivery.pr == 0 || !all_prs.insert(delivery.pr) {
            return Err(BenchmarkError::Invalid("duplicate or invalid delivered PR"));
        }
        let merged = instant(&delivery.merged_at)?;
        if merged < start || merged >= end {
            return Err(BenchmarkError::Invalid("delivery outside cohort"));
        }
        if let Some(ready) = &delivery.ready_at {
            let ready = instant(ready)?;
            let elapsed = merged
                .checked_sub(ready)
                .filter(|v| *v >= 0)
                .ok_or(BenchmarkError::Invalid("ready time follows merge"))?;
            latencies.push(elapsed as u64);
        }
        match (delivery.run_id, delivery.attempt) {
            (None, None) => {}
            (Some(run), Some(attempt))
                if attempt_end
                    .get(&(run, attempt))
                    .is_some_and(|end| *end <= merged) =>
            {
                attributed += 1;
                associated.insert((run, attempt));
            }
            _ => {
                return Err(BenchmarkError::Invalid(
                    "delivery references an unknown run attempt",
                ));
            }
        }
    }
    latencies.sort_unstable();
    let percentile = |numerator: usize| -> Option<u64> {
        if latencies.is_empty() {
            None
        } else {
            Some(latencies[(latencies.len() * numerator).div_ceil(100) - 1])
        }
    };
    Ok(CohortReport {
        jobs: input.jobs.len(),
        attempts: attempts.len(),
        rerun_attempts: attempts.iter().filter(|(_, attempt)| *attempt > 1).count(),
        runner_ms,
        delivered: input.deliveries.len(),
        attributed_deliveries: attributed,
        attempts_with_delivery: associated.len(),
        latency_samples: latencies.len(),
        median_latency_ms: percentile(50),
        p90_latency_ms: percentile(90),
        complete: input.jobs_complete && input.deliveries_complete,
    })
}

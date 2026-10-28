use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use sluss::store::Store;

#[derive(Parser)]
#[command(
    version,
    about = "CI orchestration and merge queue (development preview)"
)]
struct Cli {
    /// SQLite database path.
    #[arg(long, global = true, default_value = "sluss.sqlite")]
    db: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Receive authenticated webhooks and optionally observe GitHub.
    Serve {
        /// Listen address; expose externally only behind a trusted HTTPS proxy.
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: std::net::SocketAddr,
        /// Enable automatic observation using operator-owned configuration.
        #[arg(long)]
        config: Option<PathBuf>,
        /// Automatically freeze batches from reconciled queue observations.
        #[arg(long, requires = "config")]
        auto_plan: bool,
        /// Automatically fetch and stage planned batches on GitHub.com.
        #[arg(long, requires_all = ["config", "policy_file"])]
        auto_stage: bool,
        /// Complete operator-selected list of tracked CI policy files; repeat as needed.
        #[arg(long, requires = "auto_stage")]
        policy_file: Vec<String>,
        /// Automatically dispatch staged batches to the configured workflow.
        #[arg(long, requires_all = ["config", "capacity", "workflow_id", "required_job", "pool"])]
        auto_dispatch: bool,
        /// GitHub Actions workflow ID for automatic dispatch.
        #[arg(long, requires = "auto_dispatch")]
        workflow_id: Option<u64>,
        /// Required workflow job name; repeat as needed.
        #[arg(long, requires = "auto_dispatch")]
        required_job: Vec<String>,
        /// Resource request as pool=count; repeat as needed.
        #[arg(long, requires = "auto_dispatch")]
        pool: Vec<String>,
        /// Operator-owned selective policy for staging and dispatch.
        #[arg(long, requires = "config")]
        verification_policy: Option<PathBuf>,
        /// Cancel known runs when their batch is invalidated.
        #[arg(long, requires_all = ["config", "capacity"])]
        cancel_stale: bool,
        /// Cancel stale staging checks and recover uncertain cancellations.
        #[arg(long, requires = "config")]
        cancel_stale_checks: bool,
        /// Observe known workflow runs periodically.
        #[arg(long, requires_all = ["config", "capacity"])]
        observe_dispatches: bool,
        /// Complete observed CI-only batches without releasing their reservations.
        #[arg(long, requires = "observe_dispatches")]
        complete_ci_only: bool,
        /// Investigate CI failures by testing controlled subgroups.
        #[arg(long, requires = "complete_ci_only")]
        bisect_ci_failures: bool,
        /// Release finished or invalidated CI reservations under the single-runner contract.
        #[arg(long, requires_all = ["complete_ci_only", "capacity"])]
        release_ci_only: bool,
        /// Capacity configuration used by scheduling and observation.
        #[arg(long, requires = "config")]
        capacity: Option<PathBuf>,
    },
    /// Reconcile one pending repository wakeup without starting CI.
    Reconcile(ReconcileArgs),
    /// Select suites from an operator-controlled policy and a Git diff.
    Select {
        #[arg(long)]
        policy: PathBuf,
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        base_sha: String,
        #[arg(long)]
        head_sha: String,
    },
    /// Freeze a selective plan for an already prepared batch.
    SelectBatch {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        policy: PathBuf,
        /// Local repository containing both frozen commits.
        #[arg(long)]
        source: PathBuf,
    },
    /// Migrate the selection policy after stopping workers and draining the queue.
    MigrateSelection {
        /// Current selection mode from `sluss status` (a digest, "full" or "unbound").
        #[arg(long)]
        expected_mode: String,
        /// New operator-owned selective policy file.
        #[arg(long, required_unless_present = "full", conflicts_with = "full")]
        policy: Option<PathBuf>,
        /// Switch to full verification instead of a selective policy.
        #[arg(long, required_unless_present = "policy")]
        full: bool,
        /// Invalidate active batches after stopping workers; never releases reservations.
        #[arg(long)]
        discard_unstarted: bool,
    },
    /// Migrate planner limits while paused and drained.
    MigratePlanning {
        /// Current planner digest from `sluss status`, or "unbound".
        #[arg(long)]
        expected_digest: String,
        /// Operator-owned configuration containing the new queue settings.
        #[arg(long)]
        config: PathBuf,
    },
    /// Change the admission policy after stopping workers and draining work.
    MigrateAdmission {
        /// Current admission digest from `sluss status`.
        #[arg(long)]
        expected_digest: String,
        /// Current admission policy revision from `sluss status`.
        #[arg(long)]
        expected_revision: i64,
        /// Operator-owned configuration containing the new policy.
        #[arg(long)]
        config: PathBuf,
    },
    /// Change declared CI policy files after stopping and draining workers.
    MigratePolicyFiles {
        /// Current CI policy file revision from `sluss status`.
        #[arg(long)]
        expected_revision: i64,
        /// Configuration identifying the bound repository and target.
        #[arg(long)]
        config: PathBuf,
        /// Complete new policy-file list; repeat for each file.
        #[arg(long, required = true)]
        policy_file: Vec<String>,
    },
    /// Change CI bisection after stopping workers and draining investigations.
    MigrateCiBisection {
        /// Current CI bisection revision from `sluss status`.
        #[arg(long)]
        expected_revision: i64,
        /// Configuration identifying the bound repository and target.
        #[arg(long)]
        config: PathBuf,
        #[arg(long, required_unless_present = "disable", conflicts_with = "disable")]
        enable: bool,
        #[arg(long, required_unless_present = "enable", conflicts_with = "enable")]
        disable: bool,
    },
    /// Expand capacity while paused and drained, preserving old rerun budgets.
    MigrateCapacity {
        /// Current capacity digest from `sluss status`.
        #[arg(long)]
        expected_digest: String,
        /// Configuration identifying the bound repository and target.
        #[arg(long)]
        config: PathBuf,
        /// New operator-owned capacity configuration.
        #[arg(long)]
        capacity: PathBuf,
    },
    /// Freeze a trusted batch and generate a unique experimental landing ID.
    PrepareLandingMarker {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        policy: PathBuf,
    },
    /// Freeze currently observed same-repository PR source branches.
    FreezeLandingHeads {
        #[arg(long)]
        operation_id: String,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        policy: PathBuf,
    },
    /// Create the frozen marker once with the GitHub App; no landing is sent.
    CreateLandingMarker {
        #[arg(long)]
        operation_id: String,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        policy: PathBuf,
    },
    /// Read an attempted marker creation without retrying or resolving it.
    ObserveMarkerCreation {
        #[arg(long)]
        operation_id: String,
        #[arg(long)]
        config: PathBuf,
    },
    /// Cancel a landing intent only before its landing send is claimed.
    CancelLanding {
        #[arg(long)]
        operation_id: String,
    },
    /// List unresolved landing intents from an existing database.
    LandingIntents,
    /// Read an uncertain landing's GitHub refs without retrying the mutation.
    ObserveLandingRecovery {
        #[arg(long)]
        operation_id: String,
        #[arg(long)]
        config: PathBuf,
    },
    /// Show a landing intent and its last historical ref observation.
    LandingRecovery {
        #[arg(long)]
        operation_id: String,
    },
    /// Record provider-observed staging CI evidence under an operator policy.
    VerifyStagedCi {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        policy: PathBuf,
    },
    /// Inspect a saved CI evidence candidate against current local state.
    InspectStagedCi {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        policy: PathBuf,
    },
    /// Start a nonpassing App check for freshly verified staging CI.
    StartStagedCheck {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        policy: PathBuf,
    },
    /// Recover an explicitly identified nonpassing check after an uncertain send.
    RecoverStagedCheck {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        check_id: u64,
        #[arg(long)]
        config: PathBuf,
    },
    /// Cancel a directly received check whose local CI evidence is stale.
    CancelStaleStagedCheck {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        config: PathBuf,
    },
    /// Recover an uncertain stale-check cancellation by reading GitHub.
    RecoverCancelledStagedCheck {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        config: PathBuf,
    },
    /// Inspect the local nonpassing check journal without contacting GitHub.
    StagedCheck {
        #[arg(long)]
        batch_id: String,
    },
    /// Freeze a batch from reconciled observations for local experimentation.
    Plan {
        #[arg(long)]
        id: String,
        /// Full SHA of an independently observed target commit.
        #[arg(long)]
        base_sha: String,
        #[arg(long, default_value_t = 4)]
        max_batch_size: usize,
        #[arg(long, default_value_t = 1)]
        max_active_batches: usize,
        #[arg(long, default_value_t = 60)]
        debounce_seconds: u64,
        /// Maximum collection time after the oldest candidate finishes debouncing.
        #[arg(long, default_value_t = 60)]
        batch_window_seconds: u64,
    },
    /// Build and publish an experimental staging ref to a local repository.
    PrepareLocal {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        staging_remote: PathBuf,
        /// Tracked files whose contents must match the frozen base.
        #[arg(long, required = true)]
        policy_file: Vec<String>,
    },
    /// Publish an experimental staging ref to a GitHub repository.
    PrepareGithub {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        owner: String,
        #[arg(long)]
        repository: String,
        #[arg(long)]
        app_client_id: String,
        #[arg(long)]
        installation_id: u64,
        #[arg(long)]
        repository_id: u64,
        #[arg(long, required = true)]
        policy_file: Vec<String>,
    },
    /// Reserve capacity and dispatch one prepared batch to GitHub Actions.
    DispatchGithub {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        config: PathBuf,
        /// TOML file describing global pools and physical hosts.
        #[arg(long)]
        capacity: PathBuf,
        /// Resource request, repeat as name=count.
        #[arg(long, required = true)]
        pool: Vec<String>,
        #[arg(long)]
        workflow_id: u64,
        /// Exact GitHub job name required for this run; repeat if needed.
        #[arg(long, required = true)]
        required_job: Vec<String>,
        /// Operator-controlled selective policy used by `select-batch`, if any.
        #[arg(long)]
        verification_policy: Option<PathBuf>,
    },
    /// Reassociate an uncertain send with an operator-supplied provider run ID.
    RecoverDispatch {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        run_id: u64,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        capacity: PathBuf,
    },
    /// Find possible GitHub runs for a dispatch whose response was lost.
    FindDispatchRuns {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        config: PathBuf,
    },
    /// Observe a dispatched run and check the reserved host's runner IDs.
    ObserveDispatch {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        capacity: PathBuf,
    },
    /// Publish a failure-only check for an observed failed staging run.
    ReportFailedCheck {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        capacity: PathBuf,
    },
    /// Match an existing failure check after an uncertain publication.
    RecoverFailedCheck {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        check_run_id: u64,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        capacity: PathBuf,
    },
    /// Request cancellation of a known run for an invalidated batch.
    CancelStale {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        capacity: PathBuf,
    },
    /// Show both IDs for conflicting direct and recovered receipts.
    DispatchConflict {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        capacity: PathBuf,
    },
    /// Cancel one identified conflicting run without releasing capacity.
    CancelConflict {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        run_id: u64,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        capacity: PathBuf,
    },
    /// Revalidate both conflicting runs and release bounded CI-only capacity.
    ResolveDispatchConflict {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        capacity: PathBuf,
    },
    /// Release a reservation only if its dispatch has not been claimed.
    CancelDispatch {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        capacity: PathBuf,
    },
    /// Clear CI-only scheduling holds for a completed batch.
    RetryCi {
        #[arg(long)]
        batch_id: String,
    },
    /// Show local queue state and implementation status.
    Status,
    /// Inspect a historical CI observation; this is not an attestation.
    CiEvidence {
        #[arg(long)]
        batch_id: String,
    },
    /// Compare a local batch with an operator CI policy without creating an intent.
    InspectCiPolicy {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        policy: PathBuf,
    },
    /// Refresh a trusted-policy comparison from GitHub without creating an intent.
    PreviewTrustedBatch {
        #[arg(long)]
        batch_id: String,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        policy: PathBuf,
    },
    /// Hash declared policy files from an explicitly selected local commit.
    PolicyDigest {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        commit: String,
        #[arg(long, required = true)]
        policy_file: Vec<String>,
    },
    /// Prepare a GitHub App registration link; no App is created by this command.
    AppRegistrationUrl {
        #[arg(long)]
        name: String,
        #[arg(long)]
        organization: Option<String>,
        #[arg(long, value_enum, default_value = "observe")]
        profile: AppProfile,
        /// Public HTTPS endpoint for /webhook; omit to start with polling only.
        #[arg(long)]
        webhook_url: Option<String>,
    },
    /// Show runner time for completed jobs observed by Sluss.
    Metrics,
    /// Compare two operator-collected cohorts of jobs and delivered PRs.
    Benchmark {
        #[arg(long)]
        input: PathBuf,
    },
    /// Collect explicitly selected GitHub jobs and merged PRs for a benchmark.
    CollectBenchmark {
        /// JSON intervals and explicit run and PR IDs for each cohort.
        #[arg(long)]
        request: PathBuf,
        /// GitHub App installation and repository configuration.
        #[arg(long)]
        config: PathBuf,
        /// Optional local Sluss database for observed admission times.
        #[arg(long)]
        admission_db: Option<PathBuf>,
    },
    /// Show historical local observations that a PR version met admission.
    AdmissionHistory {
        #[arg(long)]
        pr: u64,
    },
    /// Show recent measured CI-only adaptive batch decisions.
    AdaptiveHistory,
    /// Show reconciled PR observations.
    Queue,
    /// Persist a pause for future scheduler implementations.
    Pause,
    /// Clear the local pause flag.
    Resume,
    /// Inspect GitHub prerequisites without enabling landing.
    Doctor {
        #[arg(long, requires_all = ["capacity", "workflow_id"])]
        config: Option<PathBuf>,
        #[arg(long, requires_all = ["config", "workflow_id"])]
        capacity: Option<PathBuf>,
        #[arg(long, requires_all = ["config", "capacity"])]
        workflow_id: Option<u64>,
    },
}

#[derive(Args)]
struct ReconcileArgs {
    #[arg(long)]
    owner: String,
    #[arg(long)]
    repository: String,
    #[arg(long)]
    app_client_id: String,
    #[arg(long)]
    installation_id: i64,
    #[arg(long)]
    repository_id: i64,
    #[arg(long, default_value = "main")]
    target: String,
    #[arg(long, default_value_t = 1)]
    required_reviews: usize,
    #[arg(long)]
    block_label: Vec<String>,
    #[arg(long, default_value = "https://api.github.com/")]
    api: String,
}

#[derive(Clone, Copy, ValueEnum)]
enum AppProfile {
    Observe,
    Ci,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if let Command::AppRegistrationUrl {
        name,
        organization,
        profile,
        webhook_url,
    } = &cli.command
    {
        let profile = match profile {
            AppProfile::Observe => sluss::setup::RegistrationProfile::Observe,
            AppProfile::Ci => sluss::setup::RegistrationProfile::Ci,
        };
        let url = sluss::setup::registration_url(
            name,
            organization.as_deref(),
            profile,
            webhook_url.as_deref(),
        )?;
        println!("{url}");
        println!(
            "Review and submit the registration on GitHub. This command creates no App or installation."
        );
        if webhook_url.is_some() {
            println!(
                "Set the webhook secret separately in GitHub; it is not included in this URL."
            );
        } else {
            println!(
                "Webhooks are disabled; configure and subscribe them later or use periodic polling."
            );
        }
        return Ok(());
    }
    if let Command::PreviewTrustedBatch {
        batch_id,
        config,
        policy,
    } = &cli.command
    {
        let config = sluss::config::Config::load(config)?;
        let trust = sluss::landing::TrustedCiPolicy::load(policy)?;
        let mut journal = sluss::landing::LandingJournal::open_read_only(&cli.db)?;
        let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
            .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
        let app = sluss::github::App::new(
            config.github.app_client_id.clone(),
            &std::fs::read(key_path)?,
            &config.github.api,
        )?;
        let read = app
            .read_token(
                config.github.installation_id as u64,
                config.github.repository_id as u64,
            )
            .await?;
        let security = app
            .runner_inventory_token(
                config.github.installation_id as u64,
                config.github.repository_id as u64,
            )
            .await?;
        let plan = app
            .preview_trusted_batch(
                &mut journal,
                &read,
                &security,
                &config.policy(),
                &trust,
                batch_id,
            )
            .await?;
        println!(
            "provider observations matched the frozen batch {} at staging commit {}",
            plan.batch_id, plan.staged_sha
        );
        println!(
            "This is a point-in-time comparison, not an attestation or merge authorization; no landing intent or check was created."
        );
        return Ok(());
    }
    if let Command::InspectCiPolicy { batch_id, policy } = &cli.command {
        let trust = sluss::landing::TrustedCiPolicy::load(policy)?;
        let plan =
            sluss::landing::LandingJournal::inspect_trusted_batch(&cli.db, batch_id, &trust)?;
        println!(
            "local batch {batch_id} matches operator CI policy {}",
            trust.digest()?
        );
        println!(
            "repository {} / installation {}; target {}; base {}; staging {}",
            plan.repository_id,
            plan.installation_id,
            plan.target_ref,
            plan.base_sha,
            plan.staged_sha
        );
        println!(
            "This is a local comparison only: no provider refresh, policy validation, attestation, intent, or merge authorization was performed."
        );
        return Ok(());
    }
    if let Command::PolicyDigest {
        source,
        commit,
        policy_file,
    } = &cli.command
    {
        let snapshot = sluss::integration::policy_snapshot_at_commit(source, commit, policy_file)?;
        println!("policy digest: {}", snapshot.digest);
        for path in snapshot.paths {
            println!("policy file: {path}");
        }
        println!(
            "Commit identity, file completeness and workflow trust must be independently verified."
        );
        return Ok(());
    }
    if let Command::Benchmark { input } = &cli.command {
        let report = sluss::benchmark::Input::load(input)?.report()?;
        for (label, cohort) in [("baseline", &report.baseline), ("sluss", &report.sluss)] {
            println!(
                "{label}: {} jobs, {} attempts with measured jobs ({} reruns), {:.2} runner-minutes, {} delivered PRs",
                cohort.jobs,
                cohort.attempts,
                cohort.rerun_attempts,
                cohort.runner_ms as f64 / 60_000.0,
                cohort.delivered
            );
            println!(
                "  operator-assigned deliveries: {} across {} run attempts; ready-to-merge samples: {}/{}",
                cohort.attributed_deliveries,
                cohort.attempts_with_delivery,
                cohort.latency_samples,
                cohort.delivered
            );
            if let (Some(median), Some(p90)) = (cohort.median_latency_ms, cohort.p90_latency_ms) {
                println!(
                    "  observed ready-to-merge: median {:.2} minutes, p90 {:.2} minutes",
                    median as f64 / 60_000.0,
                    p90 as f64 / 60_000.0
                );
            }
            if let (Some(median), Some(p90)) = (
                cohort.observed_median_latency_ms,
                cohort.observed_p90_latency_ms,
            ) {
                println!(
                    "  locally observed admission-to-merge: median {:.2} minutes, p90 {:.2} minutes ({}/{} deliveries); observation is not the exact ready time",
                    median as f64 / 60_000.0,
                    p90 as f64 / 60_000.0,
                    cohort.observed_latency_samples,
                    cohort.delivered
                );
            }
            if cohort.complete && cohort.delivered > 0 {
                println!(
                    "  runner-minutes per delivered PR: {:.2}",
                    cohort.runner_ms as f64 / 60_000.0 / cohort.delivered as f64
                );
            } else {
                println!("  runner-minutes per PR unavailable: incomplete cohort or no deliveries");
            }
        }
        if let Some(change) = report.runner_time_change_percent {
            println!("observed change in runner-minutes per delivered PR: {change:+.1}%");
        } else {
            println!(
                "runner-minutes comparison unavailable: incomplete cohort, no deliveries, or zero baseline time"
            );
        }
        println!(
            "Operator-supplied observations and completeness assertions are not independently verified. This comparison does not establish causation, CI trust, or merge authorization."
        );
        return Ok(());
    }
    if let Command::CollectBenchmark {
        request,
        config,
        admission_db,
    } = &cli.command
    {
        let request = sluss::benchmark::CollectionRequest::load(request)?;
        let config = sluss::config::Config::load(config)?;
        let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
            .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
        let app = sluss::github::App::new(
            config.github.app_client_id.clone(),
            &std::fs::read(key_path)?,
            &config.github.api,
        )?;
        let token = app
            .read_token(
                config.github.installation_id as u64,
                config.github.repository_id as u64,
            )
            .await?;
        let (mut input, notes) = app
            .collect_benchmark(&token, &config.policy(), &request)
            .await?;
        if let Some(path) = admission_db {
            let binding = Store::admission_history_binding(path)?;
            let epoch = Store::admission_history_policy_epoch(path)?;
            let expected = sluss::reconcile::policy_digest(&app, &config.policy())?;
            if binding
                != (
                    config.github.installation_id,
                    config.github.repository_id,
                    expected,
                )
            {
                return Err("admission database repository or policy binding differs".into());
            }
            let mut matched = 0;
            for delivery in &mut input.sluss.deliveries {
                let Some(head) = notes.merged_heads.get(&delivery.pr) else {
                    continue;
                };
                let merged = time::OffsetDateTime::parse(
                    &delivery.merged_at,
                    &time::format_description::well_known::Rfc3339,
                )?;
                let merged_ms = i64::try_from(merged.unix_timestamp_nanos().div_euclid(1_000_000))?;
                let observations = Store::admission_history(path, delivery.pr)?;
                if let Some(observed) = observations.iter().find(|o| {
                    o.head_sha == *head
                        && !o.clock_uncertain
                        && o.first_revision >= epoch.1
                        && o.first_observed_ms < merged_ms
                        && o.last_observed_ms < merged_ms
                        && o.ended_ms.is_none_or(|end| end > merged_ms)
                }) {
                    delivery.observed_ready_at = Some(format_unix_ms(observed.first_observed_ms)?);
                    matched += 1;
                }
            }
            if Store::admission_history_binding(path)? != binding
                || Store::admission_history_policy_epoch(path)? != epoch
            {
                return Err("admission database policy changed during collection".into());
            }
            eprintln!(
                "Matched {matched}/{} Sluss deliveries to local admission observations; these are not exact ready times or proof of continuous eligibility.",
                input.sluss.deliveries.len()
            );
        }
        println!("{}", serde_json::to_string_pretty(&input)?);
        eprintln!(
            "Selected GitHub observations only: {} jobs lack timing, {} are outside their cohort, {} cross its boundary. Both completeness flags are false; no run-to-PR associations or ready times were inferred.",
            notes.jobs_without_timing, notes.jobs_outside_interval, notes.boundary_crossing_jobs
        );
        for job in notes.excluded_jobs {
            eprintln!(
                "excluded job {} in run {} attempt {}: {}; started {:?}, completed {:?}",
                job.job_id, job.run_id, job.attempt, job.reason, job.started_at, job.completed_at
            );
        }
        return Ok(());
    }
    if let Command::AdmissionHistory { pr } = &cli.command {
        let observations = Store::admission_history(&cli.db, *pr)?;
        println!(
            "PR {pr}: {} local admission observations (latest 100)",
            observations.len()
        );
        for observation in observations {
            println!(
                "head {} base {} evidence {}",
                observation.head_sha, observation.base_sha, observation.evidence_digest
            );
            println!(
                "  first observed {}; last observed {}; revisions {}–{}",
                format_unix_ms(observation.first_observed_ms)?,
                format_unix_ms(observation.last_observed_ms)?,
                observation.first_revision,
                observation.last_revision
            );
            if let Some(ended) = observation.ended_ms {
                println!("  local observation ended {}", format_unix_ms(ended)?);
            } else {
                println!("  latest local observation remains open");
            }
            if observation.clock_uncertain {
                println!("  clock moved backwards; do not use these times for latency");
            }
        }
        println!(
            "Local observation times are not the exact time the PR became ready or proof that it remained eligible; no merge authorization is provided."
        );
        return Ok(());
    }
    if let Command::AdaptiveHistory = &cli.command {
        let events = Store::adaptive_history(&cli.db)?;
        println!("{} adaptive CI-only events (latest 50)", events.len());
        for event in events {
            let timing = event
                .runner_ms
                .map(|ms| format!("{:.2} runner-minutes", ms as f64 / 60_000.0))
                .unwrap_or_else(|| "runner time unavailable".into());
            println!(
                "batch {} epoch {} policy {}: {} candidates, {}, {}, limit {} -> {}{}",
                event.batch_id,
                event.epoch,
                event.policy_key,
                event.batch_size,
                event.conclusion,
                timing,
                event.limit_before,
                event.limit_after,
                if event.invalid { " (invalidated)" } else { "" }
            );
            if event.conclusion == "failure" {
                println!(
                    "  observed investigation: {} jobs, {:.2} runner-minutes; {}",
                    event.recovery_jobs,
                    event.recovery_runner_ms as f64 / 60_000.0,
                    if event.recovery_settled {
                        "settled"
                    } else {
                        "incomplete or unknown"
                    }
                );
            }
        }
        println!(
            "Samples measure observed first-attempt CI jobs, not delivered PRs, later reruns, or merge authorization."
        );
        return Ok(());
    }
    if let Command::Serve {
        listen,
        config,
        auto_plan,
        auto_stage,
        policy_file,
        auto_dispatch,
        workflow_id,
        required_job,
        pool,
        verification_policy,
        cancel_stale,
        cancel_stale_checks,
        observe_dispatches,
        complete_ci_only,
        bisect_ci_failures,
        release_ci_only,
        capacity,
    } = cli.command
    {
        if verification_policy.is_some() && !auto_stage && !auto_dispatch {
            return Err("--verification-policy requires --auto-stage or --auto-dispatch".into());
        }
        let secret = std::env::var("SLUSS_WEBHOOK_SECRET")?;
        let _inbox = sluss::webhook::Inbox::open(&cli.db)?;
        let worker = if let Some(path) = config {
            let config = sluss::config::Config::load(&path)?;
            if auto_plan && config.queue.adaptive_batching && !complete_ci_only {
                return Err("adaptive batching with automatic planning requires --complete-ci-only and dispatch observation".into());
            }
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let key = std::fs::read(key_path)?;
            let app = std::sync::Arc::new(sluss::github::App::new(
                config.github.app_client_id.clone(),
                &key,
                &config.github.api,
            )?);
            let mut initial_store = Store::open(&cli.db)?;
            if complete_ci_only {
                initial_store.bind_ci_bisection(bisect_ci_failures)?;
            }
            Some((
                app,
                config.policy(),
                config.planning(),
                std::time::Duration::from_secs(config.queue.poll_seconds),
                std::time::Duration::from_secs(config.queue.rescan_seconds),
            ))
        } else {
            None
        };
        let scope = worker
            .as_ref()
            .map(|(_, policy, _, _, _)| (policy.installation, policy.repository));
        let app = sluss::webhook::router_with_scope(secret.into_bytes(), cli.db.clone(), scope)?;
        let listener = tokio::net::TcpListener::bind(listen).await?;
        if let Some((app, policy, planning, poll, rescan)) = worker {
            if auto_plan {
                eprintln!("automatic batch planning enabled; landing remains disabled");
                tokio::spawn(sluss::worker::supervise_planning(
                    app.clone(),
                    cli.db.clone(),
                    policy.clone(),
                    planning,
                    poll,
                ));
            }
            if auto_stage {
                eprintln!("automatic staging enabled; landing is disabled");
                tokio::spawn(sluss::worker::supervise_preparation(
                    app.clone(),
                    cli.db.clone(),
                    policy.clone(),
                    policy_file,
                    verification_policy.clone(),
                    poll,
                ));
            }
            if auto_dispatch {
                let capacity = load_capacity(capacity.as_deref().ok_or("capacity is required")?)?;
                let settings = sluss::dispatch::Settings {
                    workflow_id: workflow_id.ok_or("workflow ID is required")?,
                    required_jobs: required_job.into_iter().collect(),
                    request: sluss::scheduler::Request {
                        pools: parse_pools(&pool)?,
                    },
                    verification_policy,
                };
                eprintln!("automatic dispatch enabled; landing remains disabled");
                tokio::spawn(sluss::worker::supervise_dispatch(
                    app.clone(),
                    cli.db.clone(),
                    policy.clone(),
                    capacity,
                    settings,
                    poll,
                ));
            }
            if cancel_stale {
                let capacity = load_capacity(capacity.as_deref().ok_or("capacity is required")?)?;
                eprintln!(
                    "automatic stale-run cancellation enabled; capacity is not automatically released"
                );
                tokio::spawn(sluss::worker::supervise_stale_cancellation(
                    app.clone(),
                    cli.db.clone(),
                    capacity,
                    policy.clone(),
                    poll,
                ));
            }
            if cancel_stale_checks {
                eprintln!(
                    "automatic stale staging-check cancellation enabled; landing remains disabled"
                );
                tokio::spawn(sluss::worker::supervise_stale_check_cancellation(
                    app.clone(),
                    cli.db.clone(),
                    policy.clone(),
                    poll,
                ));
            }
            if release_ci_only {
                let capacity = load_capacity(capacity.as_deref().ok_or("capacity is required")?)?;
                sluss::verification::check_release_topology(&capacity)?;
                eprintln!(
                    "CI-only capacity release enabled under the single-runner isolation contract"
                );
                tokio::spawn(sluss::worker::supervise_ci_release(
                    app.clone(),
                    cli.db.clone(),
                    capacity,
                    policy.clone(),
                    poll,
                ));
            }
            if observe_dispatches {
                let capacity = load_capacity(capacity.as_deref().ok_or("capacity is required")?)?;
                eprintln!(
                    "automatic dispatch observation enabled; observations do not authorize landing"
                );
                tokio::spawn(sluss::worker::supervise_dispatch_observation(
                    app.clone(),
                    cli.db.clone(),
                    capacity,
                    policy.clone(),
                    poll,
                    complete_ci_only,
                ));
            }
            tokio::spawn(sluss::worker::supervise(app, cli.db, policy, poll, rescan));
            eprintln!("webhook listener on {listen}; configured workers enabled, landing disabled");
        } else {
            eprintln!("webhook listener on {listen}; reconciliation worker is not enabled");
        }
        axum::serve(listener, app).await?;
        return Ok(());
    }
    if let Command::Reconcile(args) = cli.command {
        if args.installation_id <= 0 || args.repository_id <= 0 || args.required_reviews == 0 {
            return Err("invalid reconciliation configuration".into());
        }
        let inbox = sluss::webhook::Inbox::open(&cli.db)?;
        let Some(job) = inbox.pending()?.into_iter().find(|job| {
            job.installation == args.installation_id && job.repository == args.repository_id
        }) else {
            println!("no pending reconciliation for configured repository");
            return Ok(());
        };
        let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
            .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
        let key = std::fs::read(key_path)?;
        let app = sluss::github::App::new(args.app_client_id, &key, &args.api)?;
        let token = app
            .read_token(args.installation_id as u64, args.repository_id as u64)
            .await?;
        let config = sluss::reconcile::RepositoryPolicy {
            installation: args.installation_id,
            repository: args.repository_id,
            owner: args.owner,
            name: args.repository,
            target_branch: args.target,
            required_reviews: args.required_reviews,
            block_labels: args.block_label.into_iter().collect(),
        };
        let mut store = Store::open(&cli.db)?;
        sluss::reconcile::reconcile_once(&app, &token, &mut store, &job, &config).await?;
        println!(
            "repository observations updated; this command does not start CI or authorize landing"
        );
        return Ok(());
    }
    if let Command::Doctor {
        config,
        capacity,
        workflow_id,
    } = &cli.command
    {
        if let (Some(config), Some(capacity), Some(workflow_id)) = (config, capacity, workflow_id) {
            let config = sluss::config::Config::load(config)?;
            let capacity = load_capacity(capacity)?;
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let key = std::fs::read(key_path)?;
            let app =
                sluss::github::App::new(config.github.app_client_id, &key, &config.github.api)?;
            let app_identity = match app.observe_app_identity().await {
                Ok(identity) => {
                    println!(
                        "authenticated GitHub App: {} (ID {})",
                        identity.slug, identity.id
                    );
                    Some(identity)
                }
                Err(_) => {
                    eprintln!("authenticated GitHub App identity could not be verified");
                    None
                }
            };
            let installation = config.github.installation_id as u64;
            let repository_id = config.github.repository_id as u64;
            let read = app.read_token(installation, repository_id).await?;
            let inventory_token = app
                .runner_inventory_token(installation, repository_id)
                .await?;
            let preflight = app
                .inspect_repository(
                    &read,
                    &config.github.owner,
                    &config.github.repository,
                    &config.queue.target,
                    *workflow_id,
                )
                .await?;
            println!(
                "repository: {}/{} (ID {})",
                config.github.owner, config.github.repository, repository_id
            );
            println!(
                "target: {} at {} (GitHub protected: {})",
                preflight.branch.name, preflight.branch.commit.sha, preflight.branch.protected
            );
            println!("active branch rules returned: {}", preflight.rules.len());
            let mut preflight_rules = preflight.rules.clone();
            preflight_rules.sort_by_cached_key(|rule| rule.to_string());
            match app
                .observe_branch_security(
                    &inventory_token,
                    &config.github.owner,
                    &config.github.repository,
                    &config.queue.target,
                )
                .await
            {
                Ok(security)
                    if security.branch == preflight.branch
                        && security.rules.rules == preflight_rules =>
                {
                    let hidden = security
                        .rulesets
                        .iter()
                        .filter(|rule| !rule.bypass_visible)
                        .count();
                    println!(
                        "active rulesets inspected: {}; bypass lists unavailable: {}",
                        security.rulesets.len(),
                        hidden
                    );
                    let visible_actors: usize = security
                        .rulesets
                        .iter()
                        .filter_map(|rule| {
                            rule.detail
                                .get("bypass_actors")
                                .and_then(|value| value.as_array())
                        })
                        .map(Vec::len)
                        .sum();
                    println!("visible ruleset bypass actors: {visible_actors}");
                    if let Some(identity) = &app_identity {
                        match sluss::verification::assess_visible_branch_policy(
                            &security,
                            identity.id,
                        ) {
                            Ok(findings) if findings.is_empty() => println!(
                                "no contradictions found in visible branch settings; this is not authorization"
                            ),
                            Ok(findings) => {
                                for finding in findings {
                                    eprintln!("branch policy: {finding}");
                                }
                            }
                            Err(_) => eprintln!(
                                "branch policy assessment incomplete because protection or bypass visibility is missing"
                            ),
                        }
                    }
                    match security.protection {
                        sluss::github::ProtectionObservation::Present(protection) => {
                            println!("classic branch protection: readable");
                            if protection
                                .pointer("/enforce_admins/enabled")
                                .and_then(|v| v.as_bool())
                                == Some(false)
                            {
                                eprintln!(
                                    "classic protection does not enforce rules for administrators"
                                );
                            }
                            if protection
                                .pointer("/allow_force_pushes/enabled")
                                .and_then(|v| v.as_bool())
                                == Some(true)
                            {
                                eprintln!("classic protection allows force pushes");
                            }
                        }
                        sluss::github::ProtectionObservation::Unavailable(status) => eprintln!(
                            "classic branch protection unavailable (HTTP {status}); absence is not confirmed"
                        ),
                    }
                }
                Ok(_) => eprintln!("branch or rules changed between diagnostic observations"),
                Err(_) => {
                    eprintln!("branch protection and ruleset bypass details could not be inspected")
                }
            }
            match app.inspect_ref_update_schema(&read).await {
                Ok(true) => println!(
                    "GraphQL updateRefs and expected-old fields appear in the schema (semantics and permissions not verified)"
                ),
                Ok(false) => eprintln!("GraphQL expected-old ref update fields are missing"),
                Err(_) => eprintln!("GraphQL ref update schema could not be inspected"),
            }
            println!(
                "workflow: {} ({}, {})",
                preflight.workflow.id, preflight.workflow.path, preflight.workflow.state
            );
            println!(
                "workflow_dispatch declared on default branch {} at {} and on the selected target; workflow trust is not verified",
                preflight.default_workflow.branch, preflight.default_workflow.commit_sha
            );
            let runners = app
                .observe_repository_runners(
                    &inventory_token,
                    &config.github.owner,
                    &config.github.repository,
                )
                .await?;
            sluss::verification::check_repository_runner_inventory(&capacity, &runners)?;
            println!(
                "repository runner inventory matches configured hosts: {} runners",
                runners.len()
            );
            let _staging = app.staging_token(installation, repository_id).await?;
            let _dispatch = app.dispatch_token(installation, repository_id).await?;
            println!("repository-scoped read, staging and Actions credentials were issued");
        } else {
            eprintln!("To inspect GitHub, supply --config, --capacity and --workflow-id.");
        }
        eprintln!(
            "Only frozen same-repository PR head branches can be guarded; reviews, PR metadata and branch policy are not atomically guarded."
        );
        eprintln!(
            "NOT READY: workflow trust, full runner scope, bypass policy, attestation and strict landing are not verified."
        );
        std::process::exit(1);
    }
    if let Command::StagedCheck { batch_id } = &cli.command {
        let mut journal = sluss::landing::LandingJournal::open_read_only(&cli.db)?;
        if let Some((intent, state)) = journal.pending_ci_check(batch_id)? {
            println!(
                "staged check {batch_id}: {state:?}; commit {}, CI run {}, evidence {}",
                intent.candidate.plan.staged_sha,
                intent.candidate.snapshot.run.id,
                intent.candidate_digest
            );
            if let Some(cancellation) = journal.pending_ci_cancellation(batch_id)? {
                println!("cancellation: {cancellation:?}");
            }
            match journal.inspect_ci_candidate(batch_id, &intent.candidate.policy) {
                Ok(Some(current)) if current.digest == intent.candidate_digest => {
                    println!("local evidence matches; provider state may have changed")
                }
                Ok(_) => println!("no current local evidence for this check"),
                Err(error) => println!("local evidence unavailable: {error}"),
            }
        } else {
            println!("no staged check intent for {batch_id}");
        }
        println!("local journal only; no CI attestation or merge authorization");
        return Ok(());
    }
    if let Command::InspectStagedCi { batch_id, policy } = &cli.command {
        let trust = sluss::landing::TrustedCiPolicy::load(policy)?;
        let mut journal = sluss::landing::LandingJournal::open_read_only(&cli.db)?;
        match journal.inspect_ci_candidate(batch_id, &trust)? {
            Some(record) => println!(
                "CI evidence candidate {}: run {}, commit {}, digest {}, first observed {} ms",
                batch_id,
                record.candidate.snapshot.run.id,
                record.candidate.plan.staged_sha,
                record.digest,
                record.first_observed_ms
            ),
            None => println!("no recorded CI evidence candidate for this policy"),
        }
        println!(
            "local comparison only; provider state may have changed; not an attestation or merge authorization"
        );
        return Ok(());
    }
    if let Command::PrepareLandingMarker { batch_id, policy } = &cli.command {
        let trust = sluss::landing::TrustedCiPolicy::load(policy)?;
        // Open read-only first: a typo must not create an empty queue database.
        sluss::landing::LandingJournal::inspect_trusted_batch(&cli.db, batch_id, &trust)?;
        let mut random = [0_u8; 16];
        getrandom::fill(&mut random).map_err(|error| std::io::Error::other(error.to_string()))?;
        let id = hex::encode(random);
        let mut journal = sluss::landing::LandingJournal::open(&cli.db)?;
        let plan = journal.prepare_trusted_batch_with_marker(&id, batch_id, &trust)?;
        println!(
            "prepared landing {} for batch {}; marker {}",
            plan.operation_id,
            plan.batch_id,
            plan.marker_ref
                .as_deref()
                .ok_or("marker missing from plan")?
        );
        println!("Local intent only; no GitHub ref, CI attestation or merge was created.");
        return Ok(());
    }
    if let Command::FreezeLandingHeads {
        operation_id,
        config,
        policy,
    } = &cli.command
    {
        let config = sluss::config::Config::load(config)?;
        let trust = sluss::landing::TrustedCiPolicy::load(policy)?;
        let inspect = sluss::landing::LandingJournal::open_read_only(&cli.db)?;
        let (plan, _) = inspect.get(operation_id)?.ok_or("unknown landing intent")?;
        if (plan.installation_id, plan.repository_id)
            != (config.github.installation_id, config.github.repository_id)
            || plan.target_ref != format!("refs/heads/{}", config.queue.target)
            || plan.marker_ref.is_none()
        {
            return Err("repository, target or marker does not match the landing intent".into());
        }
        let key = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
            .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
        let app = sluss::github::App::new(
            config.github.app_client_id.clone(),
            &std::fs::read(key)?,
            &config.github.api,
        )?;
        let token = app
            .read_token(
                config.github.installation_id as u64,
                config.github.repository_id as u64,
            )
            .await?;
        let mut journal = sluss::landing::LandingJournal::open(&cli.db)?;
        let bound = app
            .freeze_landing_heads(&mut journal, &token, &config.policy(), &trust, operation_id)
            .await?;
        for head in bound.guarded_heads.as_deref().unwrap_or_default() {
            println!("PR {}: {} at {}", head.number, head.reference, head.sha);
        }
        println!(
            "Head refs frozen; reviews, PR metadata and branch policy are not atomically guarded. No landing was sent."
        );
        return Ok(());
    }
    if let Command::CreateLandingMarker {
        operation_id,
        config,
        policy,
    } = &cli.command
    {
        let config = sluss::config::Config::load(config)?;
        let trust = sluss::landing::TrustedCiPolicy::load(policy)?;
        let read_only = sluss::landing::LandingJournal::open_read_only(&cli.db)?;
        let (plan, _) = read_only
            .get(operation_id)?
            .ok_or("unknown landing intent")?;
        if (plan.installation_id, plan.repository_id)
            != (config.github.installation_id, config.github.repository_id)
            || plan.target_ref != format!("refs/heads/{}", config.queue.target)
            || plan.marker_ref.is_none()
        {
            return Err("repository, target or marker does not match the landing intent".into());
        }
        let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
            .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
        let app = sluss::github::App::new(
            config.github.app_client_id.clone(),
            &std::fs::read(key_path)?,
            &config.github.api,
        )?;
        let read = app
            .read_token(
                config.github.installation_id as u64,
                config.github.repository_id as u64,
            )
            .await?;
        let write = app
            .staging_token(
                config.github.installation_id as u64,
                config.github.repository_id as u64,
            )
            .await?;
        let mut journal = sluss::landing::LandingJournal::open(&cli.db)?;
        let state = app
            .create_landing_marker(
                &mut journal,
                &read,
                &write,
                &config.policy(),
                &trust,
                operation_id,
            )
            .await?;
        println!("marker creation for {operation_id}: {state:?}");
        println!("No landing mutation, CI attestation or merge authorization was issued.");
        return Ok(());
    }
    if let Command::ObserveMarkerCreation {
        operation_id,
        config,
    } = &cli.command
    {
        let config = sluss::config::Config::load(config)?;
        let inspect = sluss::landing::LandingJournal::open_read_only(&cli.db)?;
        let (plan, _) = inspect.get(operation_id)?.ok_or("unknown landing intent")?;
        if (plan.installation_id, plan.repository_id)
            != (config.github.installation_id, config.github.repository_id)
            || plan.target_ref != format!("refs/heads/{}", config.queue.target)
            || plan.marker_ref.is_none()
        {
            return Err("repository, target or marker does not match the landing intent".into());
        }
        let key = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
            .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
        let app = sluss::github::App::new(
            config.github.app_client_id.clone(),
            &std::fs::read(key)?,
            &config.github.api,
        )?;
        let token = app
            .read_token(
                config.github.installation_id as u64,
                config.github.repository_id as u64,
            )
            .await?;
        let mut journal = sluss::landing::LandingJournal::open(&cli.db)?;
        let observed = app
            .observe_marker_creation(
                &mut journal,
                &token,
                (&config.github.owner, &config.github.repository),
                operation_id,
            )
            .await?;
        match &observed.marker_sha {
            Some(sha) => println!("marker observed at {sha}"),
            None => println!("marker not visible (GitHub returned 404)"),
        }
        println!(
            "Observation saved at {} ms. This does not prove request origin or absence, resolve creation, or authorize landing.",
            observed.observed_ms
        );
        return Ok(());
    }
    if let Command::CancelLanding { operation_id } = &cli.command {
        let inspect = sluss::landing::LandingJournal::open_read_only(&cli.db)?;
        inspect.get(operation_id)?.ok_or("unknown landing intent")?;
        let mut journal = sluss::landing::LandingJournal::open(&cli.db)?;
        if !journal.cancel(operation_id)? {
            return Err("landing was already claimed or is no longer prepared".into());
        }
        println!("cancelled prepared landing {operation_id}; no landing mutation was sent");
        println!(
            "A marker-creation attempt may still need inspection or cleanup; it will not be retried."
        );
        return Ok(());
    }
    if let Command::LandingIntents = &cli.command {
        let journal = sluss::landing::LandingJournal::open_read_only(&cli.db)?;
        let (intents, more) = journal.unresolved_intents()?;
        println!("{} unresolved landing intents shown", intents.len());
        for (plan, state) in intents {
            println!(
                "{}: {:?}, batch {}, target {}, marker {}",
                plan.operation_id,
                state,
                plan.batch_id,
                plan.target_ref,
                plan.marker_ref.as_deref().unwrap_or("none")
            );
        }
        if more {
            println!("Showing the 100 most recent unresolved intents.");
        }
        println!("Local journal only; no CI attestation or merge authorization.");
        return Ok(());
    }
    if let Command::LandingRecovery { operation_id } = &cli.command {
        let journal = sluss::landing::LandingJournal::open_read_only(&cli.db)?;
        let (plan, state) = journal.get(operation_id)?.ok_or("unknown landing intent")?;
        println!(
            "landing {}: {:?}; target {}, base {}, staged {}",
            plan.operation_id, state, plan.target_ref, plan.base_sha, plan.staged_sha
        );
        if plan.marker_ref.is_some() {
            println!(
                "marker creation: {:?}",
                journal.marker_creation_state(operation_id)?
            );
        }
        if let Some(marker) = journal.last_marker_observation(operation_id)? {
            println!(
                "last marker observation: {} at {} ms (creation state then {:?})",
                marker.marker_sha.as_deref().unwrap_or("not visible (404)"),
                marker.observed_ms,
                marker.creation_state
            );
        }
        if let Some(observed) = journal.last_recovery_observation(operation_id)? {
            println!(
                "last historical observation: target {} ({}), staging {} at {} ms",
                observed.target_position(),
                observed.target_sha,
                observed.staging_sha,
                observed.observed_ms
            );
            if let Some(sha) = &observed.marker_sha {
                println!(
                    "marker {} observed at {}",
                    observed.plan.marker_ref.as_deref().unwrap_or("unknown"),
                    sha
                );
            }
        } else {
            println!("no recorded ref observation");
        }
        println!("a ref observation does not acknowledge a request or authorize a merge");
        return Ok(());
    }

    if let Command::FindDispatchRuns { batch_id, config } = &cli.command {
        let config = sluss::config::Config::load(config)?;
        let frozen = sluss::scheduler::Scheduler::inspect_uncertain_dispatch(&cli.db, batch_id)?;
        if frozen.pending.plan.installation_id != config.github.installation_id as u64
            || frozen.pending.plan.repository_id != config.github.repository_id as u64
        {
            return Err("repository does not match the uncertain dispatch".into());
        }
        let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
            .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
        let app = sluss::github::App::new(
            config.github.app_client_id.clone(),
            &std::fs::read(key_path)?,
            &config.github.api,
        )?;
        let token = app
            .read_token(
                config.github.installation_id as u64,
                config.github.repository_id as u64,
            )
            .await?;
        let runs = app
            .find_uncertain_dispatch_runs(
                &token,
                &config.github.owner,
                &config.github.repository,
                &frozen,
            )
            .await?;
        if sluss::scheduler::Scheduler::inspect_uncertain_dispatch(&cli.db, batch_id)? != frozen {
            return Err("dispatch changed during discovery; repeat the read".into());
        }
        println!("{} possible workflow runs for {batch_id}", runs.len());
        for run in runs {
            println!(
                "run {}: latest attempt {}, status {}, conclusion {}",
                run.id,
                run.run_attempt,
                run.status,
                run.conclusion.as_deref().unwrap_or("none")
            );
        }
        if frozen.workflow_path.is_none() {
            println!("workflow path was not frozen for this older dispatch");
        }
        println!(
            "Matches do not prove request origin or input values; absence does not prove the request was not sent. No recovery or capacity release was performed."
        );
        println!(
            "Recovery requires the latest run to still be attempt 1; rerun attempts need separate investigation."
        );
        return Ok(());
    }

    let store = Store::open(&cli.db)?;
    match cli.command {
        Command::Serve { .. } => unreachable!(),
        Command::Reconcile(_) => unreachable!(),
        Command::FindDispatchRuns { .. } => unreachable!(),
        Command::PrepareLandingMarker { .. } => unreachable!(),
        Command::CreateLandingMarker { .. } => unreachable!(),
        Command::FreezeLandingHeads { .. } => unreachable!(),
        Command::LandingIntents => unreachable!(),
        Command::CancelLanding { .. } => unreachable!(),
        Command::ObserveMarkerCreation { .. } => unreachable!(),
        Command::Select {
            policy,
            source,
            base_sha,
            head_sha,
        } => {
            let policy = sluss::selective::Policy::load(&policy)?;
            match policy.select_git(&source, &base_sha, &head_sha)? {
                sluss::selective::Selection::Full { suites, reason } => {
                    println!("full verification: {reason}");
                    for suite in suites {
                        println!("{suite}");
                    }
                }
                sluss::selective::Selection::Selected { suites, components } => {
                    println!(
                        "selected components: {}",
                        components.into_iter().collect::<Vec<_>>().join(", ")
                    );
                    for suite in suites {
                        println!("{suite}");
                    }
                }
            }
            println!("selection is not CI evidence or merge authorization");
        }
        Command::SelectBatch {
            batch_id,
            policy,
            source,
        } => {
            let mut store = store;
            let batch = store.batch(&batch_id)?.ok_or("unknown batch")?.0;
            let sha = match batch.state {
                sluss::batch::State::Testing { integration_sha } => integration_sha,
                _ => return Err("batch has not been staged for testing".into()),
            };
            let policy = sluss::selective::Policy::load(&policy)?;
            let selection = policy.select_git(&source, &batch.base_sha, &sha)?;
            let frozen = sluss::selective::FrozenSelection {
                base_sha: batch.base_sha,
                integration_sha: sha,
                policy_digest: policy.digest()?,
                selection,
            };
            store.record_selection(&batch_id, &frozen)?;
            println!(
                "frozen verification selection for {} (policy {})",
                batch_id, frozen.policy_digest
            );
            for suite in frozen.selection.suites() {
                println!("{suite}");
            }
            println!("selection is not CI evidence or merge authorization");
        }
        Command::Plan {
            id,
            base_sha,
            max_batch_size,
            max_active_batches,
            debounce_seconds,
            batch_window_seconds,
        } => {
            let mut store = store;
            match store.plan_batch_with_window(
                &id,
                &base_sha,
                max_batch_size,
                max_active_batches,
                std::time::Duration::from_secs(debounce_seconds),
                std::time::Duration::from_secs(batch_window_seconds),
            )? {
                Some(batch) => {
                    println!("planned {} on {}", batch.id, batch.base_sha);
                    for candidate in batch.candidates {
                        println!("#{} {}", candidate.number, candidate.head_sha);
                    }
                }
                None => println!(
                    "no batch ready (paused, pending reconciliation, debounce, collection window, or capacity)"
                ),
            }
        }
        Command::PrepareLocal {
            batch_id,
            source,
            staging_remote,
            policy_file,
        } => {
            let mut store = store;
            let result = sluss::coordinator::prepare_local(
                &mut store,
                &batch_id,
                &source,
                &staging_remote,
                &policy_file,
            )?;
            println!(
                "staged {} at {} (policy {})",
                result.sha, result.reference, result.policy_digest
            );
            println!("CI and landing are not authorized by this result");
        }
        Command::PrepareGithub {
            batch_id,
            source,
            owner,
            repository,
            app_client_id,
            installation_id,
            repository_id,
            policy_file,
        } => {
            let mut store = store;
            if store.repository_scope()? != (installation_id, repository_id) {
                return Err("repository does not match the reconciled queue".into());
            }
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let key = std::fs::read(key_path)?;
            let app = sluss::github::App::new(app_client_id, &key, "https://api.github.com/")?;
            let token = app.staging_token(installation_id, repository_id).await?;
            let result = sluss::coordinator::prepare_github(
                &mut store,
                &app,
                &token,
                sluss::coordinator::GitHubRepository {
                    owner: &owner,
                    repository: &repository,
                },
                &batch_id,
                &source,
                &policy_file,
            )
            .await?;
            println!(
                "staged {} at {} (policy {})",
                result.sha, result.reference, result.policy_digest
            );
            println!("CI and landing are not authorized by this result");
        }
        Command::DispatchGithub {
            batch_id,
            config,
            capacity,
            pool,
            workflow_id,
            required_job,
            verification_policy,
        } => {
            let config = sluss::config::Config::load(&config)?;
            let limits = load_capacity(&capacity)?;
            let settings = sluss::dispatch::Settings {
                workflow_id,
                required_jobs: required_job.into_iter().collect(),
                request: sluss::scheduler::Request {
                    pools: parse_pools(&pool)?,
                },
                verification_policy,
            };
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let key = std::fs::read(key_path)?;
            let app = sluss::github::App::new(
                config.github.app_client_id.clone(),
                &key,
                &config.github.api,
            )?;
            match sluss::dispatch::batch(
                &cli.db,
                &app,
                &config.policy(),
                &limits,
                &settings,
                &batch_id,
            )
            .await?
            {
                sluss::dispatch::Outcome::Sent { run, host } => {
                    println!("dispatched run {run} to reserved host {host}; awaiting observation")
                }
                sluss::dispatch::Outcome::NoCapacity => {
                    println!("no compatible capacity available")
                }
                sluss::dispatch::Outcome::AlreadyStarted => {
                    println!("dispatch already started or uncertain; reconcile before retrying")
                }
            }
        }
        Command::RecoverDispatch {
            batch_id,
            run_id,
            config,
            capacity,
        } => {
            let config = sluss::config::Config::load(&config)?;
            if store.repository_scope()?
                != (
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
            {
                return Err("repository does not match the reconciled queue".into());
            }
            let mut scheduler =
                sluss::scheduler::Scheduler::open(&cli.db, load_capacity(&capacity)?)?;
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let key = std::fs::read(key_path)?;
            let app =
                sluss::github::App::new(config.github.app_client_id, &key, &config.github.api)?;
            let token = app
                .read_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let snapshot = app
                .recover_uncertain_dispatch(
                    &mut scheduler,
                    &token,
                    &config.github.owner,
                    &config.github.repository,
                    &batch_id,
                    run_id,
                )
                .await?;
            let source = scheduler
                .receipt_source(&batch_id)?
                .unwrap_or_else(|| "unknown".into());
            println!(
                "recovered run {} (status {}); receipt source: {}",
                snapshot.run.id, snapshot.run.status, source
            );
            println!(
                "run origin is not proven; this is not an attestation and capacity remains reserved"
            );
        }
        Command::ObserveDispatch {
            batch_id,
            config,
            capacity,
        } => {
            let config = sluss::config::Config::load(&config)?;
            if store.repository_scope()?
                != (
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
            {
                return Err("repository does not match the reconciled queue".into());
            }
            let capacity = load_capacity(&capacity)?;
            let mut scheduler = sluss::scheduler::Scheduler::open(&cli.db, capacity.clone())?;
            let pending = scheduler
                .pending_dispatches()?
                .into_iter()
                .find(|p| p.reservation.id == batch_id)
                .ok_or("unknown or finished dispatch")?;
            let runners = &capacity
                .hosts
                .get(&pending.reservation.host)
                .ok_or("reserved host missing from capacity configuration")?
                .runner_ids;
            if runners.is_empty() {
                return Err("reserved host has no configured runner IDs".into());
            }
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let key = std::fs::read(key_path)?;
            let app =
                sluss::github::App::new(config.github.app_client_id, &key, &config.github.api)?;
            let token = app
                .read_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let snapshot = app
                .reconcile_dispatch(
                    &mut scheduler,
                    &token,
                    &config.github.owner,
                    &config.github.repository,
                    &batch_id,
                )
                .await?;
            let placement = sluss::verification::check_host_placement(&snapshot, runners)?;
            let recorded = scheduler.record_job_metrics(&batch_id, &snapshot)?;
            let batch = store.batch(&batch_id)?.ok_or("unknown batch")?.0;
            if matches!(batch.state, sluss::batch::State::Invalidated { .. }) {
                println!("batch is stale; no CI result is accepted");
            } else {
                let ci = scheduler.record_ci_observation(&batch_id, &snapshot)?;
                println!("provider CI observation: {ci:?} (not an attestation)");
            }
            println!(
                "run {}: status {}, conclusion {:?}, placement {:?}; {} new completed job measurements",
                snapshot.run.id, snapshot.run.status, snapshot.run.conclusion, placement, recorded
            );
            let source = scheduler
                .receipt_source(&batch_id)?
                .unwrap_or_else(|| "unknown".into());
            println!("receipt source: {source}");
            println!("observation does not authorize landing or release reserved capacity");
        }
        Command::ReportFailedCheck {
            batch_id,
            config,
            capacity,
        } => {
            let config = sluss::config::Config::load(&config)?;
            let installation = config.github.installation_id as u64;
            let repository = config.github.repository_id as u64;
            if store.repository_scope()? != (installation, repository) {
                return Err("repository does not match the reconciled queue".into());
            }
            let mut scheduler =
                sluss::scheduler::Scheduler::open(&cli.db, load_capacity(&capacity)?)?;
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let key = std::fs::read(key_path)?;
            let app =
                sluss::github::App::new(config.github.app_client_id, &key, &config.github.api)?;
            let read = app.read_token(installation, repository).await?;
            let write = app.failed_check_token(installation, repository).await?;
            let outcome = app
                .publish_failed_check(
                    &mut scheduler,
                    (&read, &write),
                    &config.github.owner,
                    &config.github.repository,
                    &batch_id,
                )
                .await?;
            println!("failure-only staging check: {outcome:?}; this does not authorize a merge");
            if matches!(outcome, sluss::github::FailureCheckOutcome::Recovered(_)) {
                println!("recovered check matches recorded fields; request origin is not proven");
            }
        }
        Command::RecoverFailedCheck {
            batch_id,
            check_run_id,
            config,
            capacity,
        } => {
            let config = sluss::config::Config::load(&config)?;
            let installation = config.github.installation_id as u64;
            let repository = config.github.repository_id as u64;
            if store.repository_scope()? != (installation, repository) {
                return Err("repository does not match the reconciled queue".into());
            }
            let mut scheduler =
                sluss::scheduler::Scheduler::open(&cli.db, load_capacity(&capacity)?)?;
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let app = sluss::github::App::new(
                config.github.app_client_id,
                &std::fs::read(key_path)?,
                &config.github.api,
            )?;
            let token = app.read_token(installation, repository).await?;
            let outcome = app
                .recover_failed_check(
                    &mut scheduler,
                    &token,
                    &config.github.owner,
                    &config.github.repository,
                    &batch_id,
                    check_run_id,
                )
                .await?;
            println!(
                "matched failure check: {outcome:?}; matching does not prove request origin or authorize a merge"
            );
        }
        Command::CancelStale {
            batch_id,
            config,
            capacity,
        } => {
            let config = sluss::config::Config::load(&config)?;
            if store.repository_scope()?
                != (
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
            {
                return Err("repository does not match the reconciled queue".into());
            }
            let mut scheduler =
                sluss::scheduler::Scheduler::open(&cli.db, load_capacity(&capacity)?)?;
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let key = std::fs::read(key_path)?;
            let app =
                sluss::github::App::new(config.github.app_client_id, &key, &config.github.api)?;
            let token = app
                .dispatch_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let outcome = app
                .cancel_stale_run(
                    &mut scheduler,
                    &token,
                    &config.github.owner,
                    &config.github.repository,
                    &batch_id,
                )
                .await?;
            println!(
                "stale cancellation: {outcome:?}; capacity remains reserved until separately reconciled"
            );
        }
        Command::DispatchConflict { batch_id, capacity } => {
            let scheduler = sluss::scheduler::Scheduler::open(&cli.db, load_capacity(&capacity)?)?;
            if let Some((recovered, direct)) = scheduler.receipt_conflict(&batch_id)? {
                println!(
                    "dispatch conflict for {batch_id}: recovered run {recovered}, direct receipt run {direct}"
                );
                if scheduler.conflict_release_recorded(&batch_id)? {
                    println!(
                        "capacity released after both runs were observed completed; evidence is historical and does not authorize a merge"
                    );
                } else if scheduler
                    .pending_dispatches()?
                    .iter()
                    .any(|pending| pending.reservation.id == batch_id)
                {
                    println!("capacity remains reserved");
                } else {
                    println!(
                        "no active reservation recorded; investigate legacy state before scheduling more work"
                    );
                }
            } else {
                println!("no recorded dispatch receipt conflict for {batch_id}");
            }
        }
        Command::CancelConflict {
            batch_id,
            run_id,
            config,
            capacity,
        } => {
            let config = sluss::config::Config::load(&config)?;
            if store.repository_scope()?
                != (
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
            {
                return Err("repository does not match the reconciled queue".into());
            }
            let mut scheduler =
                sluss::scheduler::Scheduler::open(&cli.db, load_capacity(&capacity)?)?;
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let app = sluss::github::App::new(
                config.github.app_client_id,
                &std::fs::read(key_path)?,
                &config.github.api,
            )?;
            let token = app
                .dispatch_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let outcome = app
                .cancel_conflicting_run(
                    &mut scheduler,
                    &token,
                    &config.github.owner,
                    &config.github.repository,
                    &batch_id,
                    run_id,
                )
                .await?;
            println!(
                "conflicting run {run_id} cancellation: {outcome:?}; capacity remains reserved"
            );
        }
        Command::ResolveDispatchConflict {
            batch_id,
            config,
            capacity,
        } => {
            let config = sluss::config::Config::load(&config)?;
            if store.repository_scope()?
                != (
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
            {
                return Err("repository does not match the reconciled queue".into());
            }
            let capacity = load_capacity(&capacity)?;
            let mut scheduler = sluss::scheduler::Scheduler::open(&cli.db, capacity.clone())?;
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let app = sluss::github::App::new(
                config.github.app_client_id,
                &std::fs::read(key_path)?,
                &config.github.api,
            )?;
            let read = app
                .read_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let inventory = app
                .runner_inventory_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let outcome = app
                .resolve_dispatch_conflict(
                    &mut scheduler,
                    &read,
                    &inventory,
                    (&config.github.owner, &config.github.repository),
                    &batch_id,
                    &capacity,
                )
                .await?;
            println!(
                "conflict capacity resolution: {outcome:?}; no CI attestation or merge authorization was issued"
            );
        }
        Command::CancelDispatch { batch_id, capacity } => {
            let mut scheduler =
                sluss::scheduler::Scheduler::open(&cli.db, load_capacity(&capacity)?)?;
            if scheduler.cancel_prepared(&batch_id)? {
                println!("cancelled unstarted dispatch");
            } else {
                println!(
                    "dispatch is absent or already claimed; reconcile before releasing capacity"
                );
            }
        }
        Command::MigrateSelection {
            expected_mode,
            policy,
            full,
            discard_unstarted,
        } => {
            let next = match (policy, full) {
                (Some(path), false) => sluss::selective::Policy::load(&path)?.digest()?,
                (None, true) => "full".to_owned(),
                _ => return Err("provide exactly one of --policy or --full".into()),
            };
            let mut store = store;
            if store.migrate_staging_selection(&expected_mode, &next, discard_unstarted)? {
                println!(
                    "selection mode migrated to {next}; old CI-only holds were cleared; Sluss remains paused"
                );
            } else {
                println!("selection mode already matches {next}; Sluss remains paused");
            }
        }
        Command::MigratePlanning {
            expected_digest,
            config,
        } => {
            let config = sluss::config::Config::load(&config)?;
            let mut store = store;
            if store.migrate_planning_settings(
                &expected_digest,
                &config.planning(),
                config.github.installation_id,
                config.github.repository_id,
                &config.queue.target,
            )? {
                println!(
                    "planning settings migrated; queue, CI holds and split tasks retained; Sluss remains paused"
                );
            } else {
                println!("planning settings already match; Sluss remains paused");
            }
        }
        Command::MigrateAdmission {
            expected_digest,
            expected_revision,
            config,
        } => {
            let config = sluss::config::Config::load(&config)?;
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let app = sluss::github::App::new(
                config.github.app_client_id.clone(),
                &std::fs::read(key_path)?,
                &config.github.api,
            )?;
            let next = sluss::reconcile::policy_digest(&app, &config.policy())?;
            let mut store = store;
            if store.migrate_admission_policy(
                &expected_digest,
                expected_revision,
                &next,
                config.github.installation_id,
                config.github.repository_id,
                &config.queue.target,
            )? {
                println!(
                    "admission policy migrated; old queue cleared and a new reconciliation scheduled; Sluss remains paused"
                );
            } else {
                println!("admission policy already matches; Sluss remains paused");
            }
        }
        Command::MigratePolicyFiles {
            expected_revision,
            config,
            policy_file,
        } => {
            let config = sluss::config::Config::load(&config)?;
            let mut store = store;
            if store.migrate_staging_policy_paths(
                expected_revision,
                &policy_file,
                config.github.installation_id,
                config.github.repository_id,
                &config.queue.target,
            )? {
                println!(
                    "CI policy files migrated; old evidence retired and CI holds cleared; Sluss remains paused"
                );
            } else {
                println!("CI policy files already match; Sluss remains paused");
            }
        }
        Command::MigrateCiBisection {
            expected_revision,
            config,
            enable,
            disable: _,
        } => {
            let config = sluss::config::Config::load(&config)?;
            let mut store = store;
            if store.migrate_ci_bisection(
                expected_revision,
                enable,
                config.github.installation_id,
                config.github.repository_id,
                &config.queue.target,
            )? {
                println!("CI bisection migrated; history and holds retained; Sluss remains paused");
            } else {
                println!("CI bisection already matches; Sluss remains paused");
            }
        }
        Command::MigrateCapacity {
            expected_digest,
            config,
            capacity,
        } => {
            let config = sluss::config::Config::load(&config)?;
            let next = load_capacity(&capacity)?;
            if sluss::scheduler::Scheduler::migrate_capacity(
                &cli.db,
                &expected_digest,
                &next,
                config.github.installation_id,
                config.github.repository_id,
                &config.queue.target,
            )? {
                println!(
                    "capacity migrated; historical release topology retained; Sluss remains paused"
                );
            } else {
                println!("capacity already matches; Sluss remains paused");
            }
        }
        Command::StartStagedCheck {
            batch_id,
            config,
            policy,
        } => {
            let config = sluss::config::Config::load(&config)?;
            let trust = sluss::landing::TrustedCiPolicy::load(&policy)?;
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let app = sluss::github::App::new(
                config.github.app_client_id.clone(),
                &std::fs::read(key_path)?,
                &config.github.api,
            )?;
            let read = app
                .read_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let security = app
                .runner_inventory_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let write = app
                .checks_write_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let mut journal = sluss::landing::LandingJournal::open(&cli.db)?;
            let outcome = app
                .start_staged_check(
                    &mut journal,
                    (&read, &security, &write),
                    &config.policy(),
                    &trust,
                    &batch_id,
                )
                .await?;
            println!(
                "nonpassing staged check: {outcome:?}; not an attestation or merge authorization"
            );
        }
        Command::RecoverStagedCheck {
            batch_id,
            check_id,
            config,
        } => {
            let config = sluss::config::Config::load(&config)?;
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let app = sluss::github::App::new(
                config.github.app_client_id.clone(),
                &std::fs::read(key_path)?,
                &config.github.api,
            )?;
            let read = app
                .read_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let mut journal = sluss::landing::LandingJournal::open(&cli.db)?;
            let outcome = app
                .recover_staged_check(
                    &mut journal,
                    &read,
                    (&config.github.owner, &config.github.repository),
                    &batch_id,
                    check_id,
                )
                .await?;
            println!(
                "nonpassing staged check recovery: {outcome:?}; a match does not prove request origin or authorize a merge"
            );
        }
        Command::CancelStaleStagedCheck { batch_id, config } => {
            let config = sluss::config::Config::load(&config)?;
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let app = sluss::github::App::new(
                config.github.app_client_id.clone(),
                &std::fs::read(key_path)?,
                &config.github.api,
            )?;
            let read = app
                .read_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let write = app
                .checks_write_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let mut journal = sluss::landing::LandingJournal::open(&cli.db)?;
            let outcome = app
                .cancel_stale_staged_check(
                    &mut journal,
                    (&read, &write),
                    (&config.github.owner, &config.github.repository),
                    &batch_id,
                )
                .await?;
            println!("stale staged check cancellation: {outcome:?}; no merge is authorized");
        }
        Command::RecoverCancelledStagedCheck { batch_id, config } => {
            let config = sluss::config::Config::load(&config)?;
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let app = sluss::github::App::new(
                config.github.app_client_id.clone(),
                &std::fs::read(key_path)?,
                &config.github.api,
            )?;
            let read = app
                .read_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let mut journal = sluss::landing::LandingJournal::open(&cli.db)?;
            let outcome = app
                .recover_cancelled_staged_check(
                    &mut journal,
                    &read,
                    (&config.github.owner, &config.github.repository),
                    &batch_id,
                )
                .await?;
            println!(
                "stale check cancellation recovery: {outcome:?}; a match does not prove request origin"
            );
        }
        Command::StagedCheck { .. } => unreachable!(),
        Command::VerifyStagedCi {
            batch_id,
            config,
            policy,
        } => {
            let config = sluss::config::Config::load(&config)?;
            let trust = sluss::landing::TrustedCiPolicy::load(&policy)?;
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let app = sluss::github::App::new(
                config.github.app_client_id.clone(),
                &std::fs::read(key_path)?,
                &config.github.api,
            )?;
            let read = app
                .read_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let security = app
                .runner_inventory_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let mut journal = sluss::landing::LandingJournal::open(&cli.db)?;
            let record = app
                .verify_staged_ci(
                    &mut journal,
                    (&read, &security),
                    &config.policy(),
                    &trust,
                    &batch_id,
                )
                .await?;
            println!(
                "CI evidence candidate {}: run {}, commit {}, digest {}, first observed {} ms",
                batch_id,
                record.candidate.snapshot.run.id,
                record.candidate.plan.staged_sha,
                record.digest,
                record.first_observed_ms
            );
            println!(
                "provider observation recorded; no GitHub check was created and no merge is authorized"
            );
        }
        Command::InspectStagedCi { .. } => unreachable!(),
        Command::ObserveLandingRecovery {
            operation_id,
            config,
        } => {
            let config = sluss::config::Config::load(&config)?;
            let mut journal = sluss::landing::LandingJournal::open(&cli.db)?;
            let (plan, _) = journal
                .get(&operation_id)?
                .ok_or("unknown landing intent")?;
            if (plan.installation_id, plan.repository_id)
                != (config.github.installation_id, config.github.repository_id)
                || plan.target_ref != format!("refs/heads/{}", config.queue.target)
            {
                return Err("repository or target does not match the landing intent".into());
            }
            let key_path = std::env::var_os("SLUSS_GITHUB_PRIVATE_KEY_PATH")
                .ok_or("SLUSS_GITHUB_PRIVATE_KEY_PATH is required")?;
            let app = sluss::github::App::new(
                config.github.app_client_id,
                &std::fs::read(key_path)?,
                &config.github.api,
            )?;
            let token = app
                .read_token(
                    config.github.installation_id as u64,
                    config.github.repository_id as u64,
                )
                .await?;
            let observed = app
                .observe_landing_recovery(
                    &mut journal,
                    &token,
                    (&config.github.owner, &config.github.repository),
                    &operation_id,
                )
                .await?;
            println!(
                "target observed at {} ({}); staging {} ({}); observation time {} ms",
                observed.target_position(),
                observed.target_sha,
                observed.staging_sha,
                if observed.staging_sha == observed.plan.staged_sha {
                    "matches frozen SHA"
                } else {
                    "changed"
                },
                observed.observed_ms
            );
            if let Some(sha) = &observed.marker_sha {
                println!(
                    "marker {} observed at {}",
                    observed.plan.marker_ref.as_deref().unwrap_or("unknown"),
                    sha
                );
            }
            println!(
                "historical observation only; operation remains uncertain and the target stays locked"
            );
        }
        Command::LandingRecovery { .. } => unreachable!(),
        Command::RetryCi { batch_id } => {
            let mut store = store;
            let count = store.retry_ci_only(&batch_id)?;
            println!(
                "cleared {count} CI-only scheduling holds; any runner reservation remains held"
            );
        }
        Command::Status => {
            println!(
                "paused: {}\nqueued observations: {}\nCI dispatch: experimental (manual or optional worker)\nlanding: disabled",
                store.paused()?,
                store.candidates()?.len()
            );
            if let Some(digest) = store.repository_policy_digest()? {
                println!(
                    "admission policy digest: {digest}; revision: {}",
                    store.admission_policy_revision()?
                );
            } else {
                println!("admission policy: unbound");
            }
            if let Some((paths, revision)) = store.staging_policy_paths()? {
                println!(
                    "CI policy files: {}; revision: {revision}",
                    paths.join(", ")
                );
            } else {
                println!("CI policy files: unbound");
            }
            println!(
                "selection mode: {}",
                store
                    .staging_selection_mode()?
                    .as_deref()
                    .unwrap_or("unbound")
            );
            println!(
                "capacity digest: {}",
                sluss::scheduler::Scheduler::capacity_digest(&cli.db)?
                    .as_deref()
                    .unwrap_or("unbound")
            );
            if let Some((settings, digest)) = store.planning_settings()? {
                println!(
                    "planning digest: {digest}\nplanning limits: batch {}, active {}, debounce {:?}, window {:?}",
                    settings.max_batch_size,
                    settings.max_active_batches,
                    settings.debounce,
                    settings.batch_window
                );
                if let Some(adaptive) = store.adaptive_status()? {
                    println!(
                        "adaptive batch limit: {}; full successes: {}/{}; recorded CI events: {} (invalidated: {})",
                        adaptive.limit,
                        adaptive.success_streak,
                        adaptive.increase_threshold,
                        adaptive.recorded_samples,
                        adaptive.invalidated_samples
                    );
                }
            } else {
                println!("planning digest: unbound");
            }
            if let Some((enabled, revision)) = store.ci_bisection_mode()? {
                println!(
                    "CI bisection: {}; revision: {revision}",
                    if enabled { "enabled" } else { "disabled" }
                );
            } else {
                println!("CI bisection: unbound");
            }
            for (batch, state) in store.ci_investigations()? {
                println!("CI investigation {batch}: {state} (not a merge approval)");
            }
            for batch in store.active_batches()? {
                println!(
                    "batch {}: {:?} ({} PRs, base {})",
                    batch.id,
                    batch.state,
                    batch.candidates.len(),
                    batch.base_sha
                );
            }
        }
        Command::CiEvidence { batch_id } => {
            if let Some(evidence) = store.ci_evidence(&batch_id)? {
                let record = evidence.record;
                println!(
                    "CI observation for batch {:?}: {:?}; conflict: {}",
                    batch_id, record.observation, evidence.conflicted
                );
                println!(
                    "repository: {}; run: {}; workflow ID: {}; commit: {}; policy digest: {}",
                    record.plan.repository_id,
                    record.run_id,
                    record.plan.workflow_id,
                    record.plan.integration_sha,
                    record.plan.policy_digest
                );
                println!(
                    "frozen workflow path: {:?}; observed runner: {:?}",
                    record.workflow_path, record.runner_id
                );
                println!(
                    "admission policy digest: {}; required jobs: {:?}; suites: {:?}; selection policy digest: {:?}",
                    store
                        .repository_policy_digest()?
                        .as_deref()
                        .unwrap_or("unbound"),
                    record.plan.required_jobs,
                    record.plan.suites,
                    record.plan.selection_policy_digest
                );
                if let Some(snapshot) = record.snapshot {
                    println!(
                        "attempt: {}; run status: {:?}; conclusion: {:?}",
                        snapshot.run.run_attempt, snapshot.run.status, snapshot.run.conclusion
                    );
                    for job in snapshot.jobs {
                        println!(
                            "job {} {:?}: status {:?}, conclusion {:?}, runner {:?}",
                            job.id, job.name, job.status, job.conclusion, job.runner_id
                        );
                    }
                } else {
                    println!("historical record has no original job snapshot");
                }
                println!(
                    "This is a provider observation, not a trusted attestation or merge approval."
                );
            } else {
                println!("no completed CI observation for batch {:?}", batch_id);
            }
        }
        Command::Metrics => {
            let metrics = store.metrics()?;
            println!(
                "observed completed jobs: {}\nobserved runner-minutes: {:.2}\nOnly completed jobs recorded by Sluss are included; PR delivery and reruns are not yet measured.",
                metrics.completed_jobs,
                metrics.runner_milliseconds as f64 / 60_000.0
            );
            for (pool, wait) in metrics.capacity_waits {
                println!(
                    "observed scheduling delay for requests including pool {pool}: {:.2} minutes ({} intervals)",
                    wait.elapsed_milliseconds as f64 / 60_000.0,
                    wait.intervals
                );
            }
            println!(
                "Scheduling delay spans first unavailable observation to a later eligible reservation; it includes polling gaps and is not runner time or proof of pool saturation."
            );
        }
        Command::Benchmark { .. } => {
            unreachable!("benchmark is handled before opening the database")
        }
        Command::CollectBenchmark { .. } => {
            unreachable!("collection is handled before opening the database")
        }
        Command::AdmissionHistory { .. } => {
            unreachable!("history is handled before opening the database")
        }
        Command::AdaptiveHistory => unreachable!("history is handled before opening the database"),
        Command::InspectCiPolicy { .. } => {
            unreachable!("policy inspection is handled before opening the database")
        }
        Command::PreviewTrustedBatch { .. } => {
            unreachable!("batch preview is handled before opening the database")
        }
        Command::PolicyDigest { .. } => {
            unreachable!("policy digest is handled before opening the database")
        }
        Command::AppRegistrationUrl { .. } => {
            unreachable!("App registration URL is handled before opening the database")
        }
        Command::Queue => {
            let failed: std::collections::BTreeMap<_, _> = store
                .failed_candidates()?
                .into_iter()
                .map(|f| (f.number, f.batch_id))
                .collect();
            let completed: std::collections::BTreeMap<_, _> = store
                .ci_only_completions()?
                .into_iter()
                .map(|(number, batch, conclusion)| (number, (batch, conclusion)))
                .collect();
            for pr in store.candidates()? {
                if let Some((batch, conclusion)) = completed.get(&pr.number) {
                    println!(
                        "#{} {} (historical CI-only first attempt {} in {}; not merge approval)",
                        pr.number, pr.head_sha, conclusion, batch
                    );
                } else if let Some(batch) = failed.get(&pr.number) {
                    println!(
                        "#{} {} (failed in {}; awaiting change)",
                        pr.number, pr.head_sha, batch
                    );
                } else {
                    println!("#{} {}", pr.number, pr.head_sha);
                }
            }
        }
        Command::Pause => {
            store.set_paused(true)?;
            println!("paused");
        }
        Command::Resume => {
            store.set_paused(false)?;
            println!("resumed (landing remains disabled)");
        }
        Command::Doctor { .. } => unreachable!(),
    }
    Ok(())
}

fn load_capacity(
    path: &std::path::Path,
) -> Result<sluss::scheduler::Capacity, Box<dyn std::error::Error>> {
    let bytes = std::fs::read(path)?;
    if bytes.len() > 65536 {
        return Err("capacity configuration is too large".into());
    }
    Ok(toml::from_str(std::str::from_utf8(&bytes)?)?)
}
fn format_unix_ms(value: i64) -> Result<String, Box<dyn std::error::Error>> {
    Ok(
        time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(value) * 1_000_000)?
            .format(&time::format_description::well_known::Rfc3339)?,
    )
}
fn parse_pools(
    values: &[String],
) -> Result<std::collections::BTreeMap<String, u32>, Box<dyn std::error::Error>> {
    let mut pools = std::collections::BTreeMap::new();
    for value in values {
        let (name, count) = value.split_once('=').ok_or("pool must be name=count")?;
        let count: u32 = count.parse()?;
        if name.is_empty() || count == 0 || pools.insert(name.into(), count).is_some() {
            return Err("invalid or duplicate pool".into());
        }
    }
    if pools.is_empty() {
        return Err("at least one pool is required".into());
    }
    Ok(pools)
}

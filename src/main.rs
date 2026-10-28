use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
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
    /// Observe a dispatched run and check the reserved host's runner IDs.
    ObserveDispatch {
        #[arg(long)]
        batch_id: String,
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
    /// Show runner time for completed jobs observed by Sluss.
    Metrics,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
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
            println!(
                "workflow: {} ({}, {})",
                preflight.workflow.id, preflight.workflow.path, preflight.workflow.state
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
            if preflight.workflow.state != "active" {
                eprintln!("workflow is not active");
            }
        } else {
            eprintln!("To inspect GitHub, supply --config, --capacity and --workflow-id.");
        }
        eprintln!(
            "NOT READY: workflow trust, full runner scope, bypass policy, attestation and strict landing are not verified."
        );
        std::process::exit(1);
    }
    let store = Store::open(&cli.db)?;
    match cli.command {
        Command::Serve { .. } => unreachable!(),
        Command::Reconcile(_) => unreachable!(),
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
            println!(
                "selection mode: {}",
                store
                    .staging_selection_mode()?
                    .as_deref()
                    .unwrap_or("unbound")
            );
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

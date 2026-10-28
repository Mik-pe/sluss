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
    },
    /// Reconcile one pending repository wakeup (CI dispatch remains disabled).
    Reconcile(ReconcileArgs),
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
    /// Show local queue state and implementation status.
    Status,
    /// Show reconciled PR observations.
    Queue,
    /// Persist a pause for future scheduler implementations.
    Pause,
    /// Clear the local pause flag.
    Resume,
    /// Explain which prerequisites are not yet implemented.
    Doctor,
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
    if let Command::Serve { listen, config } = cli.command {
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
            let _store = Store::open(&cli.db)?;
            Some((
                app,
                config.policy(),
                std::time::Duration::from_secs(config.queue.poll_seconds),
                std::time::Duration::from_secs(config.queue.rescan_seconds),
            ))
        } else {
            None
        };
        let scope = worker
            .as_ref()
            .map(|(_, policy, _, _)| (policy.installation, policy.repository));
        let app = sluss::webhook::router_with_scope(secret.into_bytes(), cli.db.clone(), scope)?;
        let listener = tokio::net::TcpListener::bind(listen).await?;
        if let Some((app, policy, poll, rescan)) = worker {
            tokio::spawn(sluss::worker::supervise(app, cli.db, policy, poll, rescan));
            eprintln!(
                "webhook listener on {listen}; automatic observations enabled, CI dispatch and landing disabled"
            );
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
        println!("repository observations updated; CI dispatch and landing remain disabled");
        return Ok(());
    }
    if matches!(cli.command, Command::Doctor) {
        eprintln!(
            "NOT READY: live GitHub permissions, branch rules, CI attestation and strict landing are not verified."
        );
        std::process::exit(1);
    }
    let store = Store::open(cli.db)?;
    match cli.command {
        Command::Serve { .. } => unreachable!(),
        Command::Reconcile(_) => unreachable!(),
        Command::Plan {
            id,
            base_sha,
            max_batch_size,
            max_active_batches,
            debounce_seconds,
        } => {
            let mut store = store;
            match store.plan_batch_debounced(
                &id,
                &base_sha,
                max_batch_size,
                max_active_batches,
                std::time::Duration::from_secs(debounce_seconds),
            )? {
                Some(batch) => {
                    println!("planned {} on {}", batch.id, batch.base_sha);
                    for candidate in batch.candidates {
                        println!("#{} {}", candidate.number, candidate.head_sha);
                    }
                }
                None => println!(
                    "no batch ready (paused, pending reconciliation, debounce, or capacity)"
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
        Command::Status => println!(
            "paused: {}\nqueued observations: {}\nCI dispatch: unavailable\nlanding: disabled",
            store.paused()?,
            store.candidates()?.len()
        ),
        Command::Queue => {
            let failed: std::collections::BTreeMap<_, _> = store
                .failed_candidates()?
                .into_iter()
                .map(|f| (f.number, f.batch_id))
                .collect();
            for pr in store.candidates()? {
                if let Some(batch) = failed.get(&pr.number) {
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
            println!("resumed (CI dispatch and landing are unavailable)");
        }
        Command::Doctor => unreachable!(),
    }
    Ok(())
}

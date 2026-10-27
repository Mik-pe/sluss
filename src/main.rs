use std::path::PathBuf;

use clap::{Parser, Subcommand};
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
    /// Receive authenticated GitHub webhook notifications (no worker yet).
    Serve {
        /// Listen address; expose externally only behind a trusted HTTPS proxy.
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: std::net::SocketAddr,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if let Command::Serve { listen } = cli.command {
        let secret = std::env::var("SLUSS_WEBHOOK_SECRET")?;
        let _inbox = sluss::webhook::Inbox::open(&cli.db)?;
        let app = sluss::webhook::router(secret.into_bytes(), cli.db)?;
        let listener = tokio::net::TcpListener::bind(listen).await?;
        eprintln!("webhook listener on {listen}; reconciliation worker is not implemented");
        axum::serve(listener, app).await?;
        return Ok(());
    }
    if matches!(cli.command, Command::Doctor) {
        eprintln!(
            "NOT READY: GitHub App, admission, CI attestation, branch rules and strict landing are not implemented."
        );
        std::process::exit(1);
    }
    let store = Store::open(cli.db)?;
    match cli.command {
        Command::Serve { .. } => unreachable!(),
        Command::Status => println!(
            "paused: {}\nqueued observations: {}\nCI dispatch: unavailable\nlanding: disabled",
            store.paused()?,
            store.candidates()?.len()
        ),
        Command::Queue => {
            for pr in store.candidates()? {
                println!("#{} {}", pr.number, pr.head_sha);
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

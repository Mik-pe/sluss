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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if matches!(cli.command, Command::Doctor) {
        eprintln!(
            "NOT READY: GitHub App, admission, CI attestation, branch rules and strict landing are not implemented."
        );
        std::process::exit(1);
    }
    let store = Store::open(cli.db)?;
    match cli.command {
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

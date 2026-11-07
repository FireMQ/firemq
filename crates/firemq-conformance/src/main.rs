//! `firemq-conformance`: the language-neutral SDK test suite. See spec/conformance.md.

mod driver;
mod runner;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(about = "FireMQ SDK conformance suite")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the scenarios against a driver command.
    Run {
        /// Shell command that starts the driver, for example "go run ./conformance/driver".
        #[arg(long)]
        driver: String,
        /// Run one scenario.
        #[arg(long)]
        only: Option<String>,
    },
    /// List the scenarios.
    List,
    /// Act as the driver for the Rust SDK.
    Driver,
}

#[tokio::main]
async fn main() {
    match Cli::parse().cmd {
        Cmd::List => {
            for (name, _) in runner::SCENARIOS {
                println!("{name}");
            }
        }
        Cmd::Driver => driver::run().await,
        Cmd::Run { driver, only } => match runner::run(&driver, only.as_deref()).await {
            Ok(0) => {}
            Ok(_) => std::process::exit(1),
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(2);
            }
        },
    }
}

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
        /// Address the broker listens on; use 0.0.0.0 when the driver runs in a container.
        #[arg(long, default_value = "127.0.0.1")]
        listen: std::net::IpAddr,
        /// Host name the driver connects to, for example host.docker.internal.
        #[arg(long, default_value = "localhost")]
        url_host: String,
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
        Cmd::Run {
            driver,
            only,
            listen,
            url_host,
        } => match runner::run(&driver, only.as_deref(), listen, &url_host).await {
            Ok(0) => {}
            Ok(_) => std::process::exit(1),
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(2);
            }
        },
    }
}

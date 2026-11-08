mod bench;
mod config;
mod ctl;
mod doctor;
mod msg;
mod serve;

use clap::{CommandFactory, Parser, Subcommand};

pub use firemq::builder;

/// FireMQ: a lean, fast, RabbitMQ-compatible message broker.
///
/// Run `firemq` with no arguments to start a broker with zero configuration.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None, args_conflicts_with_subcommands = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[command(flatten)]
    serve: serve::ServeArgs,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start the broker (the default command).
    Serve(serve::ServeArgs),
    /// Write a commented firemq.toml with every setting and its default.
    Init {
        /// Where to write the file.
        #[arg(default_value = "firemq.toml")]
        path: String,
        /// Overwrite an existing file.
        #[arg(long)]
        force: bool,
    },
    /// Manage a running broker: list, declare, delete, users, policies, definitions.
    Ctl(ctl::CtlArgs),
    /// Publish a message.
    Pub(msg::PubArgs),
    /// Consume messages and print them.
    Sub(msg::SubArgs),
    /// Measure throughput and latency.
    Bench(bench::BenchArgs),
    /// Check the environment and a running broker.
    Doctor(doctor::DoctorArgs),
    /// Print shell completions, for example: firemq completions zsh > ~/.zfunc/_firemq
    Completions { shell: clap_complete::Shell },
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        None => serve::run(cli.serve),
        Some(Command::Serve(args)) => serve::run(args),
        Some(Command::Init { path, force }) => init(&path, force),
        Some(Command::Ctl(args)) => ctl::run(args),
        Some(Command::Pub(args)) => runtime().block_on(msg::publish(args)),
        Some(Command::Sub(args)) => runtime().block_on(msg::subscribe(args)),
        Some(Command::Bench(args)) => runtime().block_on(bench::run(args)),
        Some(Command::Doctor(args)) => doctor::run(args),
        Some(Command::Completions { shell }) => {
            clap_complete::generate(shell, &mut Cli::command(), "firemq", &mut std::io::stdout());
            Ok(())
        }
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn init(path: &str, force: bool) -> Result<(), String> {
    if std::path::Path::new(path).exists() && !force {
        return Err(format!(
            "{path} already exists; use --force to overwrite it"
        ));
    }
    std::fs::write(path, config::TEMPLATE).map_err(|e| format!("cannot write {path}: {e}"))?;
    println!("wrote {path}; edit it and run `firemq`");
    Ok(())
}

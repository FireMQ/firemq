mod bench;
mod config;
mod ctl;
mod doctor;
mod msg;
mod serve;

use clap::{CommandFactory, Parser, Subcommand};

pub use firemq::builder;

// The static Linux builds use musl, whose allocator slows down sharply when the broker's
// threads allocate concurrently. Glibc, macOS and Windows builds keep the system allocator.
#[cfg(target_env = "musl")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Keeps the kernel from backing mimalloc's memory with 2 MiB transparent huge pages. Many
/// Linux hosts, Docker Desktop's VM among them, enable them for every process, which made an
/// idle broker use 17 MiB instead of 3 MiB. It must run before the runtime starts allocating.
#[cfg(all(target_env = "musl", target_os = "linux"))]
fn disable_transparent_huge_pages() {
    // SAFETY: PR_SET_THP_DISABLE only sets a flag on this process and takes no pointers.
    unsafe {
        libc::prctl(libc::PR_SET_THP_DISABLE, 1, 0, 0, 0);
    }
}

/// FireMQ: a lean, fast, RabbitMQ-compatible message broker.
///
/// Run `firemq` with no arguments to start a broker with the default settings.
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
    /// Exit 0 when a local broker answers its health probe (for container health checks).
    #[command(hide = true)]
    Health {
        #[arg(
            long,
            env = "FIREMQ_HTTP_URL",
            default_value = "http://127.0.0.1:15672"
        )]
        url: String,
    },
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn main() {
    #[cfg(all(target_env = "musl", target_os = "linux"))]
    disable_transparent_huge_pages();
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
        Some(Command::Health { url }) => health(&url),
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

fn health(url: &str) -> Result<(), String> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(3)))
        .build()
        .into();
    agent
        .get(format!("{}/healthz", url.trim_end_matches('/')))
        .call()
        .map(|_| ())
        .map_err(|e| format!("unhealthy: {e}"))
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

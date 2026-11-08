use std::net::SocketAddr;

use clap::Parser;
use firemq_server::{Server, ServerConfig};

/// FireMQ: a lean, RabbitMQ-compatible message broker.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// AMQP 0-9-1 listen address.
    #[arg(long, env = "FIREMQ_AMQP", default_value = "0.0.0.0:5672")]
    amqp: SocketAddr,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("FIREMQ_LOG")
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let server = Server::start(ServerConfig {
        amqp: Some(args.amqp),
        ..Default::default()
    })
    .await?;
    if let Some(a) = server.amqp_addr() {
        println!("FireMQ {} is running", env!("CARGO_PKG_VERSION"));
        println!("  AMQP    amqp://guest:guest@localhost:{}", a.port());
    }
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = async { server_wait_forever().await } => {}
    }
    Ok(())
}

async fn server_wait_forever() {
    std::future::pending::<()>().await
}

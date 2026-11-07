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
    /// FireMQ protocol listen address.
    #[arg(long, env = "FIREMQ_FIRE", default_value = "0.0.0.0:5673")]
    fire: SocketAddr,
    /// HTTP listen address (WebSocket, management API, UI).
    #[arg(long, env = "FIREMQ_HTTP", default_value = "0.0.0.0:15672")]
    http: SocketAddr,
    /// Users who may only connect from localhost, comma-separated. Empty allows everyone.
    #[arg(long, env = "FIREMQ_LOOPBACK_USERS", default_value = "guest")]
    loopback_users: String,
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
    let mut config = ServerConfig {
        amqp: Some(args.amqp),
        fire: Some(args.fire),
        http: Some(args.http),
        ..Default::default()
    };
    config.broker.loopback_users = args
        .loopback_users
        .split(',')
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .map(String::from)
        .collect();
    let server = Server::start(config).await?;
    println!("FireMQ {} is running", env!("CARGO_PKG_VERSION"));
    if let Some(a) = server.amqp_addr() {
        println!("  AMQP       amqp://guest:guest@localhost:{}", a.port());
    }
    if let Some(a) = server.fire_addr() {
        println!("  FireMQ     fire://guest:guest@localhost:{}", a.port());
    }
    if let Some(a) = server.http_addr() {
        println!("  WebSocket  ws://localhost:{}/fire", a.port());
    }
    tokio::signal::ctrl_c().await?;
    server.shutdown().await;
    Ok(())
}

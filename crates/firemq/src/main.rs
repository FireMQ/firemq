use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use firemq_server::{Server, ServerConfig, TlsConfig};

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
    /// AMQPS (AMQP over TLS) listen address; used when a certificate is set.
    #[arg(long, env = "FIREMQ_AMQPS", default_value = "0.0.0.0:5671")]
    amqps: SocketAddr,
    /// FireMQ protocol over TLS listen address; used when a certificate is set.
    #[arg(long, env = "FIREMQ_FIRES", default_value = "0.0.0.0:5674")]
    fires: SocketAddr,
    /// TLS certificate chain (PEM). Enables the TLS listeners.
    #[arg(long, env = "FIREMQ_TLS_CERT", requires = "tls_key")]
    tls_cert: Option<PathBuf>,
    /// TLS private key (PEM).
    #[arg(long, env = "FIREMQ_TLS_KEY", requires = "tls_cert")]
    tls_key: Option<PathBuf>,
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
    let tls = match (args.tls_cert, args.tls_key) {
        (Some(cert), Some(key)) => Some(TlsConfig { cert, key }),
        _ => None,
    };
    let mut config = ServerConfig {
        amqp: Some(args.amqp),
        amqps: Some(args.amqps),
        fire: Some(args.fire),
        fires: Some(args.fires),
        http: Some(args.http),
        tls,
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
    if let Some(a) = server.addresses().amqps {
        println!("  AMQPS      amqps://guest:guest@localhost:{}", a.port());
    }
    if let Some(a) = server.fire_addr() {
        println!("  FireMQ     fire://guest:guest@localhost:{}", a.port());
    }
    if let Some(a) = server.addresses().fires {
        println!("  FireMQ TLS fires://guest:guest@localhost:{}", a.port());
    }
    if let Some(a) = server.http_addr() {
        println!("  WebSocket  ws://localhost:{}/fire", a.port());
    }
    tokio::signal::ctrl_c().await?;
    server.shutdown().await;
    Ok(())
}

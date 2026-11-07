//! The FireMQ server: protocol listeners wired to one broker.
//!
//! ```no_run
//! # async fn run() -> std::io::Result<()> {
//! let server = firemq_server::Server::start(firemq_server::ServerConfig::default()).await?;
//! println!("AMQP on {}", server.amqp_addr().unwrap());
//! server.wait().await;
//! # Ok(())
//! # }
//! ```

pub mod convert;
mod http;
pub mod native;

use std::net::SocketAddr;
use std::sync::Arc;

use firemq_amqp::{AmqpConfig, serve_connection};
use firemq_core::{Broker, BrokerConfig, MemoryStore};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub use native::NativeConfig;

/// Server settings. The defaults match RabbitMQ's ports and credentials.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// AMQP 0-9-1 listener; `None` disables it.
    pub amqp: Option<SocketAddr>,
    /// FireMQ protocol listener.
    pub fire: Option<SocketAddr>,
    /// HTTP listener: management API, UI, metrics and the FireMQ protocol over WebSocket.
    pub http: Option<SocketAddr>,
    pub broker: BrokerConfig,
    pub amqp_config: AmqpConfig,
    pub native_config: NativeConfig,
    /// User created on first start when no users exist.
    pub default_user: String,
    pub default_pass: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            amqp: Some(SocketAddr::from(([0, 0, 0, 0], 5672))),
            fire: Some(SocketAddr::from(([0, 0, 0, 0], 5673))),
            http: Some(SocketAddr::from(([0, 0, 0, 0], 15672))),
            broker: BrokerConfig::default(),
            amqp_config: AmqpConfig::default(),
            native_config: NativeConfig::default(),
            default_user: "guest".into(),
            default_pass: "guest".into(),
        }
    }
}

/// A running server.
pub struct Server {
    broker: Broker,
    amqp_addr: Option<SocketAddr>,
    fire_addr: Option<SocketAddr>,
    http_addr: Option<SocketAddr>,
    tasks: Vec<JoinHandle<()>>,
}

impl Server {
    /// Binds every configured listener and starts serving.
    pub async fn start(config: ServerConfig) -> std::io::Result<Server> {
        let broker = Broker::new(config.broker.clone(), Arc::new(MemoryStore));
        broker.ensure_default_user(&config.default_user, &config.default_pass);
        let mut tasks = Vec::new();
        let mut amqp_addr = None;
        if let Some(addr) = config.amqp {
            let listener = TcpListener::bind(addr).await?;
            amqp_addr = Some(listener.local_addr()?);
            let amqp = Arc::new(config.amqp_config.clone());
            let b = broker.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    let (stream, peer) = match listener.accept().await {
                        Ok(x) => x,
                        Err(e) => {
                            tracing::warn!(error = %e, "AMQP accept failed");
                            continue;
                        }
                    };
                    let _ = stream.set_nodelay(true);
                    let local = stream.local_addr().unwrap_or(addr);
                    tokio::spawn(serve_connection(
                        b.clone(),
                        amqp.clone(),
                        stream,
                        peer,
                        local,
                        false,
                    ));
                }
            }));
        }
        let native = Arc::new(config.native_config.clone());
        let mut fire_addr = None;
        if let Some(addr) = config.fire {
            let listener = TcpListener::bind(addr).await?;
            fire_addr = Some(listener.local_addr()?);
            let b = broker.clone();
            let native = native.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    let (stream, peer) = match listener.accept().await {
                        Ok(x) => x,
                        Err(e) => {
                            tracing::warn!(error = %e, "FireMQ accept failed");
                            continue;
                        }
                    };
                    let _ = stream.set_nodelay(true);
                    let local = stream.local_addr().unwrap_or(addr);
                    tokio::spawn(native::serve_stream(
                        b.clone(),
                        native.clone(),
                        stream,
                        peer,
                        local,
                        false,
                    ));
                }
            }));
        }
        let mut http_addr = None;
        if let Some(addr) = config.http {
            let listener = TcpListener::bind(addr).await?;
            let local = listener.local_addr()?;
            http_addr = Some(local);
            let app = http::router(http::AppState {
                broker: broker.clone(),
                native: native.clone(),
                local,
                tls: false,
            });
            tasks.push(tokio::spawn(async move {
                let svc = app.into_make_service_with_connect_info::<SocketAddr>();
                if let Err(e) = axum::serve(listener, svc).await {
                    tracing::error!(error = %e, "HTTP server stopped");
                }
            }));
        }
        Ok(Server {
            broker,
            amqp_addr,
            fire_addr,
            http_addr,
            tasks,
        })
    }

    pub fn broker(&self) -> &Broker {
        &self.broker
    }

    /// The bound AMQP address, useful when the configured port was 0.
    pub fn amqp_addr(&self) -> Option<SocketAddr> {
        self.amqp_addr
    }

    /// The bound FireMQ protocol address.
    pub fn fire_addr(&self) -> Option<SocketAddr> {
        self.fire_addr
    }

    /// The bound HTTP address.
    pub fn http_addr(&self) -> Option<SocketAddr> {
        self.http_addr
    }

    /// Waits until every listener stops.
    pub async fn wait(self) {
        for t in self.tasks {
            let _ = t.await;
        }
    }

    /// Stops listeners and queues.
    pub async fn shutdown(self) {
        for t in &self.tasks {
            t.abort();
        }
        self.broker.shutdown().await;
    }
}

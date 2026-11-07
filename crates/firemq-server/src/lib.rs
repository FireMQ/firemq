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

use std::net::SocketAddr;
use std::sync::Arc;

use firemq_amqp::{AmqpConfig, serve_connection};
use firemq_core::{Broker, BrokerConfig, MemoryStore};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// Server settings. The defaults match RabbitMQ's ports and credentials.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// AMQP 0-9-1 listener; `None` disables it.
    pub amqp: Option<SocketAddr>,
    pub broker: BrokerConfig,
    pub amqp_config: AmqpConfig,
    /// User created on first start when no users exist.
    pub default_user: String,
    pub default_pass: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            amqp: Some(SocketAddr::from(([0, 0, 0, 0], 5672))),
            broker: BrokerConfig::default(),
            amqp_config: AmqpConfig::default(),
            default_user: "guest".into(),
            default_pass: "guest".into(),
        }
    }
}

/// A running server.
pub struct Server {
    broker: Broker,
    amqp_addr: Option<SocketAddr>,
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
        Ok(Server {
            broker,
            amqp_addr,
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

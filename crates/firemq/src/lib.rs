//! FireMQ: a lean, fast, RabbitMQ-compatible message broker.
//!
//! This crate is the `firemq` command-line tool and an embeddable broker for tests and
//! applications:
//!
//! ```no_run
//! # async fn run() -> std::io::Result<()> {
//! let server = firemq::builder().ephemeral().random_ports().start().await?;
//! println!("connect with {}", server.amqp_url().unwrap());
//! // ... run your tests against it ...
//! server.shutdown().await;
//! # Ok(())
//! # }
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;

pub use firemq_core as core;
pub use firemq_server::{
    Addresses, AlarmConfig, NativeConfig, Server, ServerConfig, StoreConfig, TlsConfig,
};

/// Starts building a broker. Defaults match the `firemq` binary: RabbitMQ's ports and a
/// `guest` user.
pub fn builder() -> Builder {
    Builder {
        config: ServerConfig::default(),
    }
}

/// Configures an embedded broker.
#[derive(Debug, Clone)]
pub struct Builder {
    config: ServerConfig,
}

impl Builder {
    /// Keeps everything in memory.
    pub fn ephemeral(mut self) -> Self {
        self.config.data_dir = None;
        self
    }

    /// Stores durable data in `dir`.
    pub fn data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.config.data_dir = Some(dir.into());
        self
    }

    /// Binds AMQP, FireMQ and HTTP on free localhost ports and disables TLS listeners and
    /// resource alarms. Ideal for tests that run in parallel.
    pub fn random_ports(mut self) -> Self {
        let any: SocketAddr = "127.0.0.1:0".parse().expect("valid address");
        self.config.amqp = Some(any);
        self.config.fire = Some(any);
        self.config.http = Some(any);
        self.config.amqps = None;
        self.config.fires = None;
        self.config.alarms = None;
        self
    }

    pub fn amqp(mut self, addr: Option<SocketAddr>) -> Self {
        self.config.amqp = addr;
        self
    }

    pub fn fire(mut self, addr: Option<SocketAddr>) -> Self {
        self.config.fire = addr;
        self
    }

    pub fn http(mut self, addr: Option<SocketAddr>) -> Self {
        self.config.http = addr;
        self
    }

    /// Sets the user created when no users exist.
    pub fn user(mut self, name: &str, pass: &str) -> Self {
        self.config.default_user = name.into();
        self.config.default_pass = pass.into();
        self
    }

    /// The full configuration, for settings without a builder method.
    pub fn config_mut(&mut self) -> &mut ServerConfig {
        &mut self.config
    }

    pub async fn start(self) -> std::io::Result<Server> {
        Server::start(self.config).await
    }
}

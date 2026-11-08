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

pub mod alarms;
pub mod convert;
mod http;
mod metrics;
pub mod mgmt;
pub mod native;
pub mod tls;

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use firemq_amqp::{AmqpConfig, serve_connection};
use firemq_core::{Broker, BrokerConfig, MemoryStore};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

pub use alarms::AlarmConfig;
pub use firemq_store::StoreConfig;
pub use native::NativeConfig;
pub use tls::{IoStream, TlsConfig};

/// Server settings. The defaults match RabbitMQ's ports and credentials.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// AMQP 0-9-1 listener; `None` disables it.
    pub amqp: Option<SocketAddr>,
    /// AMQP 0-9-1 over TLS; needs `tls`.
    pub amqps: Option<SocketAddr>,
    /// FireMQ protocol listener.
    pub fire: Option<SocketAddr>,
    /// FireMQ protocol over TLS; needs `tls`.
    pub fires: Option<SocketAddr>,
    /// HTTP listener: management API, UI, metrics and the FireMQ protocol over WebSocket.
    pub http: Option<SocketAddr>,
    /// Certificate and key for the TLS listeners.
    pub tls: Option<TlsConfig>,
    /// Where durable data lives. `None` keeps everything in memory.
    pub data_dir: Option<PathBuf>,
    pub store: StoreConfig,
    /// Memory and disk alarms; `None` disables them.
    pub alarms: Option<AlarmConfig>,
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
            amqps: Some(SocketAddr::from(([0, 0, 0, 0], 5671))),
            fire: Some(SocketAddr::from(([0, 0, 0, 0], 5673))),
            fires: Some(SocketAddr::from(([0, 0, 0, 0], 5674))),
            http: Some(SocketAddr::from(([0, 0, 0, 0], 15672))),
            tls: None,
            data_dir: None,
            store: StoreConfig::default(),
            alarms: Some(AlarmConfig::default()),
            broker: BrokerConfig::default(),
            amqp_config: AmqpConfig::default(),
            native_config: NativeConfig::default(),
            default_user: "guest".into(),
            default_pass: "guest".into(),
        }
    }
}

/// Bound addresses of a running server.
#[derive(Debug, Clone, Default)]
pub struct Addresses {
    pub amqp: Option<SocketAddr>,
    pub amqps: Option<SocketAddr>,
    pub fire: Option<SocketAddr>,
    pub fires: Option<SocketAddr>,
    pub http: Option<SocketAddr>,
}

/// A running server.
pub struct Server {
    broker: Broker,
    addrs: Addresses,
    tasks: Vec<JoinHandle<()>>,
    recovery: firemq_core::RecoveryReport,
    user: String,
    pass: String,
}

type BoxStream = Box<dyn IoStream>;

/// Accepts connections, optionally wraps them in TLS, and hands them to `handle`.
fn accept_loop<F, Fut>(
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    name: &'static str,
    handle: F,
) -> JoinHandle<()>
where
    F: Fn(BoxStream, SocketAddr, SocketAddr, bool) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let handle = Arc::new(handle);
    tokio::spawn(async move {
        loop {
            let (stream, peer): (TcpStream, SocketAddr) = match listener.accept().await {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!(listener = name, error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let _ = stream.set_nodelay(true);
            let Ok(local) = stream.local_addr() else {
                continue;
            };
            let handle = handle.clone();
            let tls = tls.clone();
            tokio::spawn(async move {
                match tls {
                    None => handle(Box::new(stream), peer, local, false).await,
                    Some(acceptor) => {
                        match tokio::time::timeout(Duration::from_secs(10), acceptor.accept(stream))
                            .await
                        {
                            Ok(Ok(s)) => handle(Box::new(s), peer, local, true).await,
                            Ok(Err(e)) => {
                                tracing::debug!(listener = name, %peer, error = %e, "TLS handshake failed")
                            }
                            Err(_) => {
                                tracing::debug!(listener = name, %peer, "TLS handshake timed out")
                            }
                        }
                    }
                }
            });
        }
    })
}

impl Server {
    /// Opens the data directory (if any), recovers durable state, binds every configured
    /// listener and starts serving.
    pub async fn start(config: ServerConfig) -> std::io::Result<Server> {
        let broker = match &config.data_dir {
            Some(dir) => {
                let store = firemq_store::DiskStore::open(dir, config.store.clone())?;
                Broker::new(config.broker.clone(), Arc::new(store))
            }
            None => Broker::new(config.broker.clone(), Arc::new(MemoryStore)),
        };
        Server::start_with(config, broker).await
    }

    /// Recovers state for an existing broker and starts listeners.
    pub async fn start_with(config: ServerConfig, broker: Broker) -> std::io::Result<Server> {
        let recovery = broker
            .recover()
            .await
            .map_err(|e| std::io::Error::other(e.text))?;
        broker.ensure_default_user(&config.default_user, &config.default_pass);
        let mut tasks = Vec::new();
        if broker.store().is_durable() {
            broker
                .save_definitions()
                .map_err(|e| std::io::Error::other(format!("cannot write definitions: {e}")))?;
            tasks.push(broker.spawn_definitions_writer());
        }
        if let Some(a) = &config.alarms {
            tasks.push(alarms::spawn(
                broker.clone(),
                a.clone(),
                config.data_dir.clone(),
            ));
        }
        let acceptor = config.tls.as_ref().map(tls::acceptor).transpose()?;
        let amqp = Arc::new(config.amqp_config.clone());
        let native = Arc::new(config.native_config.clone());
        let mut addrs = Addresses::default();

        let amqp_listeners = [
            (config.amqp, None, &mut addrs.amqp, "amqp"),
            (
                config.amqps.filter(|_| acceptor.is_some()),
                acceptor.clone(),
                &mut addrs.amqps,
                "amqps",
            ),
        ];
        for (addr, tls, slot, name) in amqp_listeners {
            let Some(addr) = addr else { continue };
            let listener = TcpListener::bind(addr).await?;
            *slot = Some(listener.local_addr()?);
            let (b, cfg) = (broker.clone(), amqp.clone());
            tasks.push(accept_loop(
                listener,
                tls,
                name,
                move |s, peer, local, tls| {
                    serve_connection(b.clone(), cfg.clone(), s, peer, local, tls)
                },
            ));
        }

        let fire_listeners = [
            (config.fire, None, &mut addrs.fire, "fire"),
            (
                config.fires.filter(|_| acceptor.is_some()),
                acceptor.clone(),
                &mut addrs.fires,
                "fires",
            ),
        ];
        for (addr, tls, slot, name) in fire_listeners {
            let Some(addr) = addr else { continue };
            let listener = TcpListener::bind(addr).await?;
            *slot = Some(listener.local_addr()?);
            let (b, cfg) = (broker.clone(), native.clone());
            tasks.push(accept_loop(
                listener,
                tls,
                name,
                move |s, peer, local, tls| {
                    native::serve_stream(b.clone(), cfg.clone(), s, peer, local, tls)
                },
            ));
        }

        if let Some(addr) = config.http {
            let listener = TcpListener::bind(addr).await?;
            let local = listener.local_addr()?;
            addrs.http = Some(local);
            let mut listeners = Vec::new();
            for (proto, a) in [
                ("amqp", addrs.amqp),
                ("amqp/ssl", addrs.amqps),
                ("fire", addrs.fire),
                ("fire/ssl", addrs.fires),
                ("http", addrs.http),
            ] {
                if let Some(a) = a {
                    listeners.push((proto.to_string(), a));
                }
            }
            let rates = Arc::new(mgmt::rates::Rates::default());
            tasks.push(rates.spawn(broker.clone(), Duration::from_secs(5)));
            let app = http::router(http::AppState {
                broker: broker.clone(),
                native: native.clone(),
                local,
                tls: false,
                rates,
                listeners: Arc::new(listeners),
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
            addrs,
            tasks,
            recovery,
            user: config.default_user.clone(),
            pass: config.default_pass.clone(),
        })
    }

    pub fn broker(&self) -> &Broker {
        &self.broker
    }

    /// What startup recovery restored from disk.
    pub fn recovery(&self) -> &firemq_core::RecoveryReport {
        &self.recovery
    }

    /// Every bound address. Useful when a configured port was 0.
    pub fn addresses(&self) -> &Addresses {
        &self.addrs
    }

    pub fn amqp_addr(&self) -> Option<SocketAddr> {
        self.addrs.amqp
    }

    pub fn fire_addr(&self) -> Option<SocketAddr> {
        self.addrs.fire
    }

    pub fn http_addr(&self) -> Option<SocketAddr> {
        self.addrs.http
    }

    fn url(&self, scheme: &str, addr: Option<SocketAddr>, path: &str) -> Option<String> {
        let a = addr?;
        let host = if a.ip().is_unspecified() || a.ip().is_loopback() {
            "localhost".to_string()
        } else {
            a.ip().to_string()
        };
        Some(format!(
            "{scheme}://{}:{}@{host}:{}{path}",
            self.user,
            self.pass,
            a.port()
        ))
    }

    /// An `amqp://` URL with the default user, for clients and tests.
    pub fn amqp_url(&self) -> Option<String> {
        self.url("amqp", self.addrs.amqp, "/%2F")
    }

    /// A `fire://` URL with the default user.
    pub fn fire_url(&self) -> Option<String> {
        self.url("fire", self.addrs.fire, "")
    }

    /// The management UI and API base URL.
    pub fn http_url(&self) -> Option<String> {
        let a = self.addrs.http?;
        Some(format!("http://localhost:{}", a.port()))
    }

    /// Waits until every listener stops.
    pub async fn wait(self) {
        for t in self.tasks {
            let _ = t.await;
        }
    }

    /// Stops listeners, asks connections to close, and stops queues without deleting data.
    pub async fn shutdown(self) {
        for t in &self.tasks {
            t.abort();
        }
        self.broker.shutdown().await;
        if self.broker.store().is_durable()
            && let Err(e) = self.broker.save_definitions()
        {
            tracing::error!(error = %e, "failed to save definitions at shutdown");
        }
    }
}

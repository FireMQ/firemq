use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedSender;

use crate::confirm::ConnEvent;
use crate::error::{BrokerError, Result};
use crate::message::now_ms;
use crate::storage::{MemoryStore, Store};
use crate::vhost::VHost;

/// Broker-wide settings.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Node name shown in the management API, `firemq@<host>` by default.
    pub node_name: String,
    /// Capacity of each queue's command inbox. A full inbox slows publishers down.
    pub queue_inbox: usize,
    /// Message bytes each disk-backed queue keeps in memory before reading from disk.
    pub queue_cache_bytes: u64,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        let host = std::env::var("HOSTNAME")
            .ok()
            .filter(|h| !h.is_empty())
            .unwrap_or_else(|| "localhost".to_string());
        Self {
            node_name: format!("firemq@{host}"),
            queue_inbox: 1024,
            queue_cache_bytes: 8 * 1024 * 1024,
        }
    }
}

pub(crate) struct Ctx {
    pub config: BrokerConfig,
    pub store: Arc<dyn Store>,
    pub ids: AtomicU64,
    pub definitions_dirty: Notify,
}

impl Ctx {
    pub fn definitions_changed(&self) {
        self.definitions_dirty.notify_one();
    }
}

/// A connection, as listed by the management API.
#[derive(Debug)]
pub struct ConnectionInfo {
    pub id: u64,
    pub name: String,
    /// `AMQP 0-9-1` or `FireMQ 1`.
    pub protocol: String,
    pub peer: String,
    pub local: String,
    pub user: String,
    pub vhost: String,
    pub tls: bool,
    pub connected_at: u64,
    pub client_properties: serde_json::Value,
    pub events: UnboundedSender<ConnEvent>,
    pub channels: AtomicU32,
    pub recv_bytes: AtomicU64,
    pub send_bytes: AtomicU64,
    pub published: AtomicU64,
    pub delivered: AtomicU64,
}

/// Keeps a connection listed until dropped.
pub struct ConnectionRegistration {
    broker: Broker,
    id: u64,
}

impl Drop for ConnectionRegistration {
    fn drop(&mut self) {
        self.broker.inner.connections.lock().remove(&self.id);
    }
}

struct Inner {
    ctx: Arc<Ctx>,
    vhosts: RwLock<BTreeMap<String, Arc<VHost>>>,
    connections: Mutex<BTreeMap<u64, Arc<ConnectionInfo>>>,
    started_at: u64,
}

/// The broker: the set of virtual hosts plus shared services. Cheap to clone.
#[derive(Clone)]
pub struct Broker {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Broker")
            .field("node", &self.inner.ctx.config.node_name)
            .finish()
    }
}

impl Broker {
    /// Creates a broker with the default vhost `/`.
    pub fn new(config: BrokerConfig, store: Arc<dyn Store>) -> Broker {
        let ctx = Arc::new(Ctx {
            config,
            store,
            ids: AtomicU64::new(1),
            definitions_dirty: Notify::new(),
        });
        let broker = Broker {
            inner: Arc::new(Inner {
                ctx,
                vhosts: RwLock::new(BTreeMap::new()),
                connections: Mutex::new(BTreeMap::new()),
                started_at: now_ms(),
            }),
        };
        broker.add_vhost("/");
        broker
    }

    /// A broker that keeps everything in memory.
    pub fn in_memory() -> Broker {
        Broker::new(BrokerConfig::default(), Arc::new(MemoryStore))
    }

    pub fn config(&self) -> &BrokerConfig {
        &self.inner.ctx.config
    }

    pub fn store(&self) -> &Arc<dyn Store> {
        &self.inner.ctx.store
    }

    /// Milliseconds since the Unix epoch when the broker started.
    pub fn started_at(&self) -> u64 {
        self.inner.started_at
    }

    /// A broker-wide unique id for connections and consumers.
    pub fn next_id(&self) -> u64 {
        self.inner.ctx.ids.fetch_add(1, Ordering::Relaxed)
    }

    pub fn vhost(&self, name: &str) -> Option<Arc<VHost>> {
        self.inner.vhosts.read().get(name).cloned()
    }

    pub fn vhosts(&self) -> Vec<Arc<VHost>> {
        self.inner.vhosts.read().values().cloned().collect()
    }

    /// Creates a vhost, or returns the existing one.
    pub fn add_vhost(&self, name: &str) -> Arc<VHost> {
        let mut vhosts = self.inner.vhosts.write();
        if let Some(v) = vhosts.get(name) {
            return v.clone();
        }
        let v = VHost::new(name, self.inner.ctx.clone());
        vhosts.insert(name.to_string(), v.clone());
        drop(vhosts);
        self.inner.ctx.definitions_changed();
        v
    }

    /// Deletes a vhost with all its queues and closes its connections.
    pub async fn delete_vhost(&self, name: &str) -> Result<()> {
        let v = self
            .inner
            .vhosts
            .write()
            .remove(name)
            .ok_or_else(|| BrokerError::not_found(format!("no vhost '{name}'")))?;
        for c in self.connections() {
            if c.vhost == name {
                let _ = c.events.send(ConnEvent::Close {
                    code: 320,
                    reason: format!("CONNECTION_FORCED - vhost '{name}' is deleted"),
                });
            }
        }
        v.shutdown(true).await;
        self.inner.ctx.definitions_changed();
        Ok(())
    }

    /// Lists a connection until the returned registration is dropped.
    pub fn register_connection(
        &self,
        info: ConnectionInfo,
    ) -> (Arc<ConnectionInfo>, ConnectionRegistration) {
        let id = info.id;
        let info = Arc::new(info);
        self.inner.connections.lock().insert(id, info.clone());
        (
            info,
            ConnectionRegistration {
                broker: self.clone(),
                id,
            },
        )
    }

    pub fn connections(&self) -> Vec<Arc<ConnectionInfo>> {
        self.inner.connections.lock().values().cloned().collect()
    }

    pub fn connection(&self, id: u64) -> Option<Arc<ConnectionInfo>> {
        self.inner.connections.lock().get(&id).cloned()
    }

    /// Resolves when durable definitions changed since the last call.
    pub async fn definitions_changed(&self) {
        self.inner.ctx.definitions_dirty.notified().await
    }

    /// Stops every queue without deleting data and asks connections to close.
    pub async fn shutdown(&self) {
        for c in self.connections() {
            let _ = c.events.send(ConnEvent::Close {
                code: 320,
                reason: "CONNECTION_FORCED - broker shutdown".into(),
            });
        }
        for v in self.vhosts() {
            v.shutdown(false).await;
        }
    }
}

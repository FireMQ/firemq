//! Queues: a handle shared by connections and an actor that owns the messages.

mod actor;
mod config;

use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use firemq_amqp_codec::FieldTable;
use tokio::sync::{mpsc, oneshot};

use crate::confirm::{ConnEvent, Ticket};
use crate::error::{BrokerError, Result};
use crate::message::Message;

pub(crate) use actor::Actor;
pub use config::{Overflow, QueueConfig};

/// Options for declaring a queue.
#[derive(Debug, Clone, Default)]
pub struct QueueOptions {
    /// Empty means the broker picks a unique `amq.gen-` name.
    pub name: String,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub passive: bool,
    pub arguments: FieldTable,
}

/// Identifies who holds unacknowledged messages: a connection and, for AMQP, a channel.
/// The FireMQ protocol uses channel 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Owner {
    pub conn: u64,
    pub channel: u16,
}

/// How many messages a consumer may receive before it is paused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Limit {
    /// AMQP prefetch: at most this many unacknowledged messages; 0 means unlimited.
    Prefetch(u32),
    /// FireMQ credit: the consumer grants credit explicitly; each delivery uses one.
    Credit(u32),
}

/// A prefetch limit shared by every consumer on an AMQP channel (`basic.qos` with
/// `global = true`).
#[derive(Debug)]
pub struct SharedLimit {
    limit: u32,
    in_flight: AtomicU32,
}

impl SharedLimit {
    pub fn new(limit: u32) -> Arc<SharedLimit> {
        Arc::new(SharedLimit {
            limit,
            in_flight: AtomicU32::new(0),
        })
    }

    fn has_room(&self) -> bool {
        self.limit == 0 || self.in_flight.load(Ordering::Acquire) < self.limit
    }

    fn try_acquire(&self) -> bool {
        if self.limit == 0 {
            return true;
        }
        self.in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.limit).then_some(n + 1)
            })
            .is_ok()
    }

    fn release(&self) {
        if self.limit != 0 {
            let _ = self
                .in_flight
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
        }
    }
}

/// Everything a queue needs to deliver to a consumer.
#[derive(Debug)]
pub struct ConsumerSpec {
    /// Broker-wide unique id, from [`crate::Broker::next_id`].
    pub id: u64,
    pub tag: String,
    pub owner: Owner,
    pub no_ack: bool,
    pub exclusive: bool,
    pub limit: Limit,
    pub shared_limit: Option<Arc<SharedLimit>>,
    pub arguments: FieldTable,
    /// The connection's delivery buffer.
    pub deliveries: mpsc::Sender<Delivery>,
    /// The connection's event channel, for cancel notifications.
    pub events: mpsc::UnboundedSender<ConnEvent>,
}

/// A message sent to a consumer's connection.
#[derive(Debug)]
pub struct Delivery {
    pub consumer_id: u64,
    pub queue: QueueHandle,
    /// Queue-local sequence number; acknowledge with [`QueueHandle::ack`].
    pub seq: u64,
    pub message: Arc<Message>,
    pub redelivered: bool,
}

/// The reply to [`QueueHandle::get`].
#[derive(Debug)]
pub struct GetResult {
    pub seq: u64,
    pub message: Arc<Message>,
    pub redelivered: bool,
    /// Messages still ready after this one.
    pub remaining: u32,
}

/// Counters the queue actor publishes for the management API.
#[derive(Debug, Default)]
pub struct QueueStats {
    pub ready: AtomicU64,
    pub unacked: AtomicU64,
    pub ready_bytes: AtomicU64,
    pub consumers: AtomicU32,
    pub published: AtomicU64,
    pub delivered: AtomicU64,
    pub acked: AtomicU64,
    pub redelivered: AtomicU64,
}

pub(crate) enum QueueCmd {
    Publish {
        msg: Arc<Message>,
        ticket: Option<Ticket>,
    },
    Consume {
        spec: ConsumerSpec,
        reply: oneshot::Sender<Result<()>>,
    },
    Cancel {
        consumer_id: u64,
        reply: Option<oneshot::Sender<()>>,
    },
    Ack {
        seqs: Vec<u64>,
    },
    Reject {
        seqs: Vec<u64>,
        requeue: bool,
    },
    Credit {
        consumer_id: u64,
        credit: u32,
    },
    Get {
        owner: Owner,
        no_ack: bool,
        reply: oneshot::Sender<Option<GetResult>>,
    },
    Purge {
        reply: oneshot::Sender<u64>,
    },
    Recover {
        owner: Owner,
    },
    OwnerClosed {
        conn: u64,
        channel: Option<u16>,
    },
    Touch,
    Wake,
    SetConfig(QueueConfig),
    Shutdown {
        reply: Option<oneshot::Sender<u64>>,
        delete_data: bool,
    },
}

/// State shared between a queue's actor and everyone holding a [`QueueHandle`].
#[derive(Debug)]
pub struct QueueShared {
    pub name: String,
    pub vhost: String,
    pub durable: bool,
    pub auto_delete: bool,
    /// The connection that owns an exclusive queue.
    pub exclusive_owner: Option<u64>,
    pub arguments: FieldTable,
    /// The configuration from arguments alone, used to check redeclarations.
    pub declared_config: QueueConfig,
    pub stats: QueueStats,
    pub(crate) tx: mpsc::Sender<QueueCmd>,
}

/// A cheap, cloneable reference to a queue.
#[derive(Debug, Clone)]
pub struct QueueHandle(pub(crate) Arc<QueueShared>);

impl Deref for QueueHandle {
    type Target = QueueShared;
    fn deref(&self) -> &QueueShared {
        &self.0
    }
}

impl PartialEq for QueueHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for QueueHandle {}

impl std::hash::Hash for QueueHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (Arc::as_ptr(&self.0) as usize).hash(state)
    }
}

fn gone(name: &str) -> BrokerError {
    BrokerError::not_found(format!("queue '{name}' was deleted"))
}

impl QueueHandle {
    pub fn name(&self) -> &str {
        &self.0.name
    }

    pub fn message_count(&self) -> u64 {
        self.0.stats.ready.load(Ordering::Relaxed)
    }

    pub fn consumer_count(&self) -> u32 {
        self.0.stats.consumers.load(Ordering::Relaxed)
    }

    pub fn is_closed(&self) -> bool {
        self.0.tx.is_closed()
    }

    async fn send(&self, cmd: QueueCmd) -> Result<()> {
        self.0.tx.send(cmd).await.map_err(|_| gone(&self.0.name))
    }

    /// Enqueues a message, waiting while the queue's inbox is full. Returns false when the
    /// queue no longer exists; the ticket is then released.
    pub async fn publish(&self, msg: Arc<Message>, ticket: Option<Ticket>) -> bool {
        self.0
            .tx
            .send(QueueCmd::Publish { msg, ticket })
            .await
            .is_ok()
    }

    /// Enqueues without waiting. Used by queues dead-lettering into other queues, which must
    /// never block on each other.
    pub fn publish_nowait(&self, msg: Arc<Message>) {
        match self.0.tx.try_send(QueueCmd::Publish { msg, ticket: None }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(cmd)) => {
                let tx = self.0.tx.clone();
                tokio::spawn(async move {
                    let _ = tx.send(cmd).await;
                });
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    /// Adds a consumer.
    pub async fn consume(&self, spec: ConsumerSpec) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.send(QueueCmd::Consume { spec, reply }).await?;
        rx.await.map_err(|_| gone(&self.0.name))?
    }

    /// Removes a consumer. Its unacknowledged messages stay with the owner.
    pub async fn cancel(&self, consumer_id: u64) {
        let (reply, rx) = oneshot::channel();
        if self
            .send(QueueCmd::Cancel {
                consumer_id,
                reply: Some(reply),
            })
            .await
            .is_ok()
        {
            let _ = rx.await;
        }
    }

    /// Acknowledges deliveries by queue sequence number.
    pub async fn ack(&self, seqs: Vec<u64>) {
        let _ = self.send(QueueCmd::Ack { seqs }).await;
    }

    /// Rejects deliveries, requeueing or dead-lettering them.
    pub async fn reject(&self, seqs: Vec<u64>, requeue: bool) {
        let _ = self.send(QueueCmd::Reject { seqs, requeue }).await;
    }

    /// Grants credit to a credit-mode consumer.
    pub async fn credit(&self, consumer_id: u64, credit: u32) {
        let _ = self
            .send(QueueCmd::Credit {
                consumer_id,
                credit,
            })
            .await;
    }

    /// Takes one message (`basic.get`).
    pub async fn get(&self, owner: Owner, no_ack: bool) -> Result<Option<GetResult>> {
        let (reply, rx) = oneshot::channel();
        self.send(QueueCmd::Get {
            owner,
            no_ack,
            reply,
        })
        .await?;
        rx.await.map_err(|_| gone(&self.0.name))
    }

    /// Removes every ready message and returns how many were removed.
    pub async fn purge(&self) -> Result<u64> {
        let (reply, rx) = oneshot::channel();
        self.send(QueueCmd::Purge { reply }).await?;
        rx.await.map_err(|_| gone(&self.0.name))
    }

    /// Requeues every unacknowledged message held by `owner` (`basic.recover`).
    pub async fn recover(&self, owner: Owner) {
        let _ = self.send(QueueCmd::Recover { owner }).await;
    }

    /// Tells the queue a channel (or, with `channel: None`, a whole connection) closed:
    /// its consumers are removed and its unacknowledged messages requeued.
    pub async fn owner_closed(&self, conn: u64, channel: Option<u16>) {
        let _ = self.send(QueueCmd::OwnerClosed { conn, channel }).await;
    }

    /// Marks the queue as used, resetting its `x-expires` timer.
    pub fn touch(&self) {
        let _ = self.0.tx.try_send(QueueCmd::Touch);
    }

    /// Asks the queue to retry deliveries, after shared credit was freed elsewhere.
    pub fn wake(&self) {
        let _ = self.0.tx.try_send(QueueCmd::Wake);
    }

    pub(crate) async fn set_config(&self, cfg: QueueConfig) {
        let _ = self.send(QueueCmd::SetConfig(cfg)).await;
    }

    /// Stops the actor and deletes the queue's data. Returns the number of messages lost.
    pub(crate) async fn shutdown(&self) -> u64 {
        let (reply, rx) = oneshot::channel();
        if self
            .send(QueueCmd::Shutdown {
                reply: Some(reply),
                delete_data: true,
            })
            .await
            .is_err()
        {
            return 0;
        }
        rx.await.unwrap_or(0)
    }

    /// Stops the actor but keeps the queue's data, for a broker shutdown.
    pub(crate) async fn stop(&self) {
        let (reply, rx) = oneshot::channel();
        if self
            .send(QueueCmd::Shutdown {
                reply: Some(reply),
                delete_data: false,
            })
            .await
            .is_ok()
        {
            let _ = rx.await;
        }
    }
}

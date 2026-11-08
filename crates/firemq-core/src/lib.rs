//! The protocol-agnostic core of the FireMQ broker.
//!
//! The core owns the messaging model: virtual hosts, exchanges, bindings, queues, consumers
//! and publisher confirms. Protocol adapters (AMQP 0-9-1 and the FireMQ protocol) translate
//! their wire commands into calls on [`VHost`] and [`QueueHandle`].
//!
//! # Concurrency
//!
//! - Routing reads a copy-on-write [`Topology`] snapshot, so publishing never takes a lock.
//! - Each queue is an actor: one task owns its messages and consumers and receives commands
//!   on a bounded channel. A full channel slows the publisher's connection, which is the
//!   broker's backpressure.
//! - Queues never wait on a consumer. When a connection's delivery buffer is full, the queue
//!   parks that consumer and keeps serving the others.
//! - A publish creates a [`Ticket`]. Every queue that stores the message holds a clone and
//!   drops it once the message is safe; the last drop sends the confirm.

pub mod auth;
mod broker;
mod confirm;
mod error;
mod exchange;
pub mod json;
mod message;
pub mod queue;
mod routing;
mod storage;
mod vhost;

pub use auth::{Access, Permission, User, Users};
pub use broker::{Broker, BrokerConfig, ConnectionInfo, ConnectionRegistration};
pub use confirm::{ConnEvent, Ticket};
pub use error::{BrokerError, Result};
pub use exchange::{Binding, Destination, Exchange, ExchangeKind, ExchangeOptions};
pub use firemq_amqp_codec::{BasicProperties, FieldTable, LongString, Value};
pub use message::{Message, now_ms};
pub use queue::{
    ConsumerSpec, Delivery, GetResult, Limit, Overflow, Owner, QueueConfig, QueueHandle,
    QueueOptions, QueueShared, QueueStats, SharedLimit,
};
pub use routing::{TopicTrie, headers_match};
pub use storage::{Loc, MemoryStore, QueueStore, Recovered, Store};
pub use vhost::{Policy, QueueDeclared, Topology, VHost};

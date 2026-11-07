use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use firemq_amqp_codec::BasicProperties;

/// Milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A published message. Queues share one `Arc<Message>` per publish.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    /// Exchange the message was published to.
    pub exchange: String,
    /// Routing key it was published with.
    pub routing_key: String,
    pub properties: BasicProperties,
    pub body: Bytes,
    /// Arrival time in milliseconds since the Unix epoch.
    pub published_at: u64,
}

impl Message {
    pub fn new(
        exchange: impl Into<String>,
        routing_key: impl Into<String>,
        properties: BasicProperties,
        body: Bytes,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            properties,
            body,
            published_at: now_ms(),
        }
    }

    /// True when the publisher asked for the message to be written to disk.
    pub fn is_persistent(&self) -> bool {
        self.properties.delivery_mode == Some(2)
    }

    /// Per-message TTL from the `expiration` property, in milliseconds.
    pub fn expiration_ms(&self) -> Option<u64> {
        self.properties
            .expiration
            .as_deref()
            .and_then(|e| e.trim().parse().ok())
    }

    /// Body size in bytes, the unit of `x-max-length-bytes`.
    pub fn size(&self) -> usize {
        self.body.len()
    }
}

use std::time::Duration;

use firemq_proto::{ConnectUrl, Map, Properties};

/// How to connect.
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    pub url: ConnectUrl,
    /// Heartbeat interval in seconds; 0 disables heartbeats.
    pub heartbeat: u32,
    /// Shown in the management UI.
    pub connection_name: String,
    pub connect_timeout: Duration,
}

impl ConnectOptions {
    pub fn from_url(url: &str) -> crate::Result<ConnectOptions> {
        Ok(ConnectOptions {
            url: ConnectUrl::parse(url)?,
            heartbeat: 30,
            connection_name: String::new(),
            connect_timeout: Duration::from_secs(10),
        })
    }
}

/// Options for [`crate::Client::declare_queue`]. Queues are durable by default.
#[derive(Debug, Clone)]
pub struct QueueOptions {
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub passive: bool,
    /// RabbitMQ queue arguments, for example `x-message-ttl`.
    pub args: Map,
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            durable: true,
            exclusive: false,
            auto_delete: false,
            passive: false,
            args: Map::new(),
        }
    }
}

impl QueueOptions {
    /// A queue that lives only as long as this connection.
    pub fn temporary() -> Self {
        Self {
            durable: false,
            exclusive: true,
            auto_delete: true,
            ..Default::default()
        }
    }

    pub fn arg(mut self, key: &str, value: impl Into<firemq_proto::Value>) -> Self {
        self.args.insert(key.to_string(), value.into());
        self
    }
}

/// Options for [`crate::Client::declare_exchange`]. Exchanges are durable by default.
#[derive(Debug, Clone)]
pub struct ExchangeOptions {
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub passive: bool,
    pub args: Map,
}

impl Default for ExchangeOptions {
    fn default() -> Self {
        Self {
            durable: true,
            auto_delete: false,
            internal: false,
            passive: false,
            args: Map::new(),
        }
    }
}

/// Options for [`crate::Client::publish_with`]. Messages are persistent and confirmed by
/// default.
#[derive(Debug, Clone)]
pub struct PublishOptions {
    pub persistent: bool,
    /// Fail with [`crate::Error::Unroutable`] when no queue receives the message.
    pub mandatory: bool,
    /// Wait for the broker's confirm. When false, `publish` returns once the frame is queued.
    pub confirm: bool,
    pub properties: Properties,
    pub headers: Map,
}

impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            persistent: true,
            mandatory: false,
            confirm: true,
            properties: Properties::default(),
            headers: Map::new(),
        }
    }
}

/// Options for [`crate::Client::subscribe`].
#[derive(Debug, Clone)]
pub struct SubscribeOptions {
    /// How many unacknowledged messages the broker may send at once.
    pub prefetch: u32,
    /// Treat messages as acknowledged when sent. Faster, but messages are lost if the
    /// consumer crashes.
    pub no_ack: bool,
    pub exclusive: bool,
    /// Consumer tag shown in the management UI.
    pub tag: String,
}

impl Default for SubscribeOptions {
    fn default() -> Self {
        Self {
            prefetch: 100,
            no_ack: false,
            exclusive: false,
            tag: String::new(),
        }
    }
}

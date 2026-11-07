//! AMQP 0-9-1 adapter for the FireMQ broker.
//!
//! [`serve_connection`] runs one client connection over any byte stream (plain TCP or TLS).
//! It maps AMQP channels, methods and content frames onto [`firemq_core`].
//!
//! Each connection runs two tasks. The connection task owns all protocol state: it reads
//! frames, talks to queues and turns deliveries into frames. The writer task only writes
//! bytes. Splitting them means a client that stops reading never stops the broker from
//! reading that client's publishes.

mod channel;
mod connection;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use firemq_core::Broker;
use tokio::io::{AsyncRead, AsyncWrite};

/// Settings for AMQP connections.
#[derive(Debug, Clone)]
pub struct AmqpConfig {
    /// Largest frame the broker accepts and sends, including frame overhead.
    pub frame_max: u32,
    /// Highest channel number a client may open.
    pub channel_max: u16,
    /// Heartbeat interval the broker suggests, in seconds. 0 disables heartbeats.
    pub heartbeat: u16,
    /// Deliveries buffered per connection before queues park its consumers.
    pub delivery_buffer: usize,
    /// Largest message body accepted, in bytes.
    pub max_message_size: u64,
    /// Time allowed from TCP accept to `connection.open-ok`.
    pub handshake_timeout: Duration,
    /// Shown to clients as `cluster_name`.
    pub cluster_name: String,
}

impl Default for AmqpConfig {
    fn default() -> Self {
        Self {
            frame_max: 131_072,
            channel_max: 2047,
            heartbeat: 60,
            delivery_buffer: 256,
            max_message_size: 128 * 1024 * 1024,
            handshake_timeout: Duration::from_secs(10),
            cluster_name: "firemq".to_string(),
        }
    }
}

/// Serves one AMQP 0-9-1 connection until the client disconnects or the broker closes it.
pub async fn serve_connection<S>(
    broker: Broker,
    config: Arc<AmqpConfig>,
    stream: S,
    peer: SocketAddr,
    local: SocketAddr,
    tls: bool,
) where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    connection::serve(broker, config, stream, peer, local, tls).await
}

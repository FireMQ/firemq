//! Rust client for [FireMQ](https://github.com/FireMQ/firemq).
//!
//! ```no_run
//! # async fn run() -> Result<(), firemq_client::Error> {
//! use firemq_client::{Client, SubscribeOptions};
//!
//! let client = Client::connect("fire://guest:guest@localhost:5673").await?;
//! client.declare_queue("jobs", Default::default()).await?;
//! client.publish("", "jobs", "hello").await?; // resolves when the broker confirms
//!
//! let mut jobs = client.subscribe("jobs", SubscribeOptions::default()).await?;
//! while let Some(msg) = jobs.next().await {
//!     println!("{}", msg.body_str().unwrap_or_default());
//!     msg.ack()?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Defaults favour safety: queues and exchanges are durable, messages are persistent, and
//! `publish` waits for the broker's confirm. The client reconnects on its own when the
//! connection drops; see [`Reconnect`].

mod client;
mod message;
mod options;
mod tls;

pub use client::{Client, QueueInfo};
pub use firemq_proto::{ConnectUrl, Map, Properties, Value};
pub use message::{Message, Subscription};
pub use options::{
    ConnectOptions, ExchangeOptions, PublishOptions, QueueOptions, Reconnect, SubscribeOptions,
};

/// Errors returned by the client.
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(String),
    #[error("invalid URL: {0}")]
    Url(#[from] firemq_proto::UrlError),
    #[error("protocol error: {0}")]
    Protocol(String),
    /// The broker refused a request. `code` is an AMQP reply code such as 404 or 406.
    #[error("{msg} (code {code})")]
    Server { code: u16, msg: String },
    /// The broker rejected a publish, for example because a queue was full.
    #[error("publish rejected: {msg} (code {code})")]
    Nacked { code: u16, msg: String },
    /// A mandatory publish matched no queue.
    #[error("message returned: no queue is bound to exchange '{exchange}' with key '{key}'")]
    Unroutable { exchange: String, key: String },
    #[error("connection closed: {0}")]
    Closed(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

impl Error {
    /// The AMQP reply code, when the broker sent one.
    pub fn code(&self) -> Option<u16> {
        match self {
            Error::Server { code, .. } | Error::Nacked { code, .. } => Some(*code),
            Error::Unroutable { .. } => Some(312),
            _ => None,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

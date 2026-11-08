//! Foreign-language bindings for the FireMQ client, [`firemq_client`].
//!
//! Two surfaces share one implementation:
//!
//! - A [UniFFI](https://mozilla.github.io/uniffi-rs/) object API, from which the Python, Kotlin,
//!   Swift and Ruby bindings in `bindings/` are generated.
//! - A C ABI in [`c`], described by `bindings/c/firemq.h`, used by C, C++ and Dart.
//!
//! Both are blocking: every call runs on one shared multi-threaded Tokio runtime and waits
//! for the result, so foreign code needs no event loop. Message headers and queue arguments
//! cross the boundary as JSON objects.

pub mod c;
mod json;

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use firemq_client as fc;

uniffi::setup_scaffolding!();

/// The runtime every binding call runs on. Connections spawn their reader and writer tasks
/// here, so they keep running between calls.
pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("firemq")
            .enable_all()
            .build()
            .expect("cannot start the FireMQ runtime")
    })
}

/// Errors returned by every binding call.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, uniffi::Error)]
pub enum FireMqError {
    /// The broker refused a request. `code` is an AMQP reply code such as 403, 404 or 406.
    #[error("{message} (code {code})")]
    Server { code: u16, message: String },
    /// A mandatory publish matched no queue.
    #[error("{message}")]
    Unroutable { message: String },
    /// The broker rejected a publish, for example because a queue was full.
    #[error("publish rejected: {message} (code {code})")]
    Nacked { code: u16, message: String },
    /// The connection or subscription is closed.
    #[error("connection closed: {message}")]
    Closed { message: String },
    /// Anything else: I/O failures, invalid URLs or JSON, protocol errors.
    #[error("{message}")]
    Other { message: String },
}

impl From<fc::Error> for FireMqError {
    fn from(e: fc::Error) -> Self {
        match e {
            fc::Error::Server { code, msg } => FireMqError::Server { code, message: msg },
            fc::Error::Nacked { code, msg } => FireMqError::Nacked { code, message: msg },
            e @ fc::Error::Unroutable { .. } => FireMqError::Unroutable {
                message: e.to_string(),
            },
            fc::Error::Closed(message) => FireMqError::Closed { message },
            e => FireMqError::Other {
                message: e.to_string(),
            },
        }
    }
}

impl FireMqError {
    /// The AMQP reply code, or 0 when there is none.
    pub fn code(&self) -> u16 {
        match self {
            FireMqError::Server { code, .. } | FireMqError::Nacked { code, .. } => *code,
            FireMqError::Unroutable { .. } => 312,
            _ => 0,
        }
    }

    fn closed(message: &str) -> FireMqError {
        FireMqError::Closed {
            message: message.into(),
        }
    }
}

type Result<T, E = FireMqError> = std::result::Result<T, E>;

/// What `declare_queue` reports.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct QueueInfo {
    /// The queue name; the broker generates one when the declared name was empty.
    pub name: String,
    /// Ready messages.
    pub messages: u64,
    pub consumers: u32,
}

/// Message properties, mirroring AMQP's basic properties.
#[derive(Debug, Clone, Default, PartialEq, Eq, uniffi::Record)]
pub struct MessageProperties {
    #[uniffi(default)]
    pub content_type: Option<String>,
    #[uniffi(default)]
    pub content_encoding: Option<String>,
    #[uniffi(default)]
    pub correlation_id: Option<String>,
    #[uniffi(default)]
    pub reply_to: Option<String>,
    /// Time to live in milliseconds.
    #[uniffi(default)]
    pub expiration: Option<u64>,
    #[uniffi(default)]
    pub message_id: Option<String>,
    /// Seconds since the Unix epoch.
    #[uniffi(default)]
    pub timestamp: Option<u64>,
    /// The `type` property.
    #[uniffi(default)]
    pub message_type: Option<String>,
    #[uniffi(default)]
    pub user_id: Option<String>,
    #[uniffi(default)]
    pub app_id: Option<String>,
    #[uniffi(default)]
    pub priority: Option<u8>,
}

impl From<MessageProperties> for fc::Properties {
    fn from(p: MessageProperties) -> Self {
        fc::Properties {
            content_type: p.content_type,
            content_encoding: p.content_encoding,
            correlation_id: p.correlation_id,
            reply_to: p.reply_to,
            expiration: p.expiration,
            message_id: p.message_id,
            timestamp: p.timestamp,
            kind: p.message_type,
            user_id: p.user_id,
            app_id: p.app_id,
            priority: p.priority,
        }
    }
}

impl From<&fc::Properties> for MessageProperties {
    fn from(p: &fc::Properties) -> Self {
        MessageProperties {
            content_type: p.content_type.clone(),
            content_encoding: p.content_encoding.clone(),
            correlation_id: p.correlation_id.clone(),
            reply_to: p.reply_to.clone(),
            expiration: p.expiration,
            message_id: p.message_id.clone(),
            timestamp: p.timestamp,
            message_type: p.kind.clone(),
            user_id: p.user_id.clone(),
            app_id: p.app_id.clone(),
            priority: p.priority,
        }
    }
}

/// How to publish. The defaults are persistent and confirmed, like the Rust client's.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct PublishOptions {
    /// Store the message on disk in durable queues.
    #[uniffi(default = true)]
    pub persistent: bool,
    /// Fail with `Unroutable` when no queue receives the message.
    #[uniffi(default = false)]
    pub mandatory: bool,
    /// Wait for the broker's confirm. When false, `publish` returns once the frame is queued.
    #[uniffi(default = true)]
    pub confirm: bool,
    #[uniffi(default)]
    pub properties: MessageProperties,
    /// Application headers as a JSON object, for example `{"trace": "abc", "attempt": 2}`.
    #[uniffi(default)]
    pub headers_json: Option<String>,
}

impl Default for PublishOptions {
    fn default() -> Self {
        PublishOptions {
            persistent: true,
            mandatory: false,
            confirm: true,
            properties: MessageProperties::default(),
            headers_json: None,
        }
    }
}

impl PublishOptions {
    pub(crate) fn into_client(self) -> Result<fc::PublishOptions> {
        Ok(fc::PublishOptions {
            persistent: self.persistent,
            mandatory: self.mandatory,
            confirm: self.confirm,
            properties: self.properties.into(),
            headers: json::map_from_json(self.headers_json.as_deref())?,
        })
    }
}

/// A connection to FireMQ. Safe to share between threads.
#[derive(uniffi::Object)]
pub struct Client {
    inner: fc::Client,
}

#[uniffi::export]
impl Client {
    /// Connects with a URL such as `fire://guest:guest@localhost:5673/vhost`, or `fires://`
    /// for TLS.
    #[uniffi::constructor]
    pub fn connect(url: String) -> Result<Arc<Client>> {
        let inner = runtime().block_on(fc::Client::connect(&url))?;
        Ok(Arc::new(Client { inner }))
    }

    /// Closes the connection gracefully. Unacknowledged messages return to their queues.
    pub fn close(&self) -> Result<()> {
        Ok(runtime().block_on(self.inner.close())?)
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Declares a queue. An empty name asks the broker to generate one. `args_json` holds
    /// RabbitMQ queue arguments, for example `{"x-max-length": 1000}`.
    #[uniffi::method(default(
        durable = true,
        exclusive = false,
        auto_delete = false,
        args_json = None
    ))]
    pub fn declare_queue(
        &self,
        name: String,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
        args_json: Option<String>,
    ) -> Result<QueueInfo> {
        let opts = fc::QueueOptions {
            durable,
            exclusive,
            auto_delete,
            passive: false,
            args: json::map_from_json(args_json.as_deref())?,
        };
        let q = runtime().block_on(self.inner.declare_queue(&name, opts))?;
        Ok(QueueInfo {
            name: q.name,
            messages: q.messages,
            consumers: q.consumers,
        })
    }

    /// Deletes a queue and returns how many messages it held.
    pub fn delete_queue(&self, name: String) -> Result<u64> {
        Ok(runtime().block_on(self.inner.delete_queue(&name))?)
    }

    /// Removes every ready message from a queue and returns how many were removed.
    pub fn purge_queue(&self, name: String) -> Result<u64> {
        Ok(runtime().block_on(self.inner.purge_queue(&name))?)
    }

    /// Declares an exchange of type `direct`, `fanout`, `topic` or `headers`.
    #[uniffi::method(default(durable = true, auto_delete = false, args_json = None))]
    pub fn declare_exchange(
        &self,
        name: String,
        kind: String,
        durable: bool,
        auto_delete: bool,
        args_json: Option<String>,
    ) -> Result<()> {
        let opts = fc::ExchangeOptions {
            durable,
            auto_delete,
            args: json::map_from_json(args_json.as_deref())?,
            ..Default::default()
        };
        Ok(runtime().block_on(self.inner.declare_exchange(&name, &kind, opts))?)
    }

    #[uniffi::method(default(if_unused = false))]
    pub fn delete_exchange(&self, name: String, if_unused: bool) -> Result<()> {
        Ok(runtime().block_on(self.inner.delete_exchange(&name, if_unused))?)
    }

    /// Binds `queue` to `exchange` with a routing key or pattern. `args_json` holds binding
    /// arguments, for example `{"x-match": "all"}` for headers exchanges.
    #[uniffi::method(default(args_json = None))]
    pub fn bind_queue(
        &self,
        queue: String,
        exchange: String,
        key: String,
        args_json: Option<String>,
    ) -> Result<()> {
        let args = json::map_from_json(args_json.as_deref())?;
        Ok(runtime().block_on(self.inner.bind_queue_with(&queue, &exchange, &key, args))?)
    }

    pub fn unbind_queue(&self, queue: String, exchange: String, key: String) -> Result<()> {
        Ok(runtime().block_on(self.inner.unbind_queue(&queue, &exchange, &key))?)
    }

    /// Binds exchange `destination` to exchange `source`.
    pub fn bind_exchange(&self, destination: String, source: String, key: String) -> Result<()> {
        Ok(runtime().block_on(self.inner.bind_exchange(&destination, &source, &key))?)
    }

    /// Publishes a message. Use exchange `""` to send straight to the queue named by `key`.
    /// With the default options this waits for the broker's confirm.
    #[uniffi::method(default(options))]
    pub fn publish(
        &self,
        exchange: String,
        key: String,
        body: Vec<u8>,
        options: PublishOptions,
    ) -> Result<()> {
        let opts = options.into_client()?;
        Ok(runtime().block_on(self.inner.publish_with(&exchange, &key, body, opts))?)
    }

    /// Starts consuming from a queue. `prefetch` caps unacknowledged deliveries; with
    /// `no_ack` messages count as acknowledged when sent.
    #[uniffi::method(default(prefetch = 100, no_ack = false))]
    pub fn subscribe(
        &self,
        queue: String,
        prefetch: u32,
        no_ack: bool,
    ) -> Result<Arc<Subscription>> {
        let opts = fc::SubscribeOptions {
            prefetch,
            no_ack,
            ..Default::default()
        };
        let sub = runtime().block_on(self.inner.subscribe(&queue, opts))?;
        Ok(Arc::new(Subscription::new(sub, no_ack)))
    }

    /// Takes one message from a queue, or returns nothing when it is empty.
    #[uniffi::method(default(no_ack = false))]
    pub fn get(&self, queue: String, no_ack: bool) -> Result<Option<Arc<Delivery>>> {
        let msg = runtime().block_on(self.inner.get(&queue, no_ack))?;
        Ok(msg.map(|m| Arc::new(Delivery::new(m, no_ack))))
    }
}

/// A stream of messages from a queue. Dropping it cancels the subscription.
#[derive(uniffi::Object)]
pub struct Subscription {
    inner: tokio::sync::Mutex<Option<fc::Subscription>>,
    no_ack: bool,
}

impl Subscription {
    pub(crate) fn new(sub: fc::Subscription, no_ack: bool) -> Subscription {
        Subscription {
            inner: tokio::sync::Mutex::new(Some(sub)),
            no_ack,
        }
    }
}

#[uniffi::export]
impl Subscription {
    /// Waits up to `timeout_ms` for the next message and returns nothing on timeout. Fails
    /// with `Closed` once the subscription has ended: it was cancelled, its queue was
    /// deleted or the connection closed.
    pub fn next(&self, timeout_ms: u64) -> Result<Option<Arc<Delivery>>> {
        runtime().block_on(async {
            let mut guard = self.inner.lock().await;
            let Some(sub) = guard.as_mut() else {
                return Err(FireMqError::closed("the subscription was cancelled"));
            };
            match tokio::time::timeout(Duration::from_millis(timeout_ms), sub.next()).await {
                Ok(Some(m)) => Ok(Some(Arc::new(Delivery::new(m, self.no_ack)))),
                Ok(None) => {
                    *guard = None;
                    Err(FireMqError::closed("the subscription ended"))
                }
                Err(_) => Ok(None),
            }
        })
    }

    /// Stops the subscription. Messages already received can still be acknowledged.
    pub fn cancel(&self) -> Result<()> {
        runtime().block_on(async {
            let sub = self.inner.lock().await.take();
            match sub {
                Some(sub) => Ok(sub.cancel().await?),
                None => Ok(()),
            }
        })
    }
}

/// A delivered message.
#[derive(uniffi::Object)]
pub struct Delivery {
    inner: fc::Message,
    // Messages consumed with `no_ack` are settled already; acknowledging them again would
    // make the broker report an unknown delivery tag.
    no_ack: bool,
    // Headers are decoded once, on first use.
    headers_json: Mutex<Option<String>>,
}

impl Delivery {
    pub(crate) fn new(inner: fc::Message, no_ack: bool) -> Delivery {
        Delivery {
            inner,
            no_ack,
            headers_json: Mutex::new(None),
        }
    }
}

#[uniffi::export]
impl Delivery {
    pub fn body(&self) -> Vec<u8> {
        self.inner.body.to_vec()
    }

    pub fn exchange(&self) -> String {
        self.inner.exchange.clone()
    }

    pub fn routing_key(&self) -> String {
        self.inner.routing_key.clone()
    }

    /// True when the message was delivered before and not acknowledged.
    pub fn redelivered(&self) -> bool {
        self.inner.redelivered
    }

    pub fn delivery_tag(&self) -> u64 {
        self.inner.delivery_tag
    }

    pub fn properties(&self) -> MessageProperties {
        (&self.inner.properties).into()
    }

    /// Application headers as a JSON object. Binary values are base64-encoded strings.
    pub fn headers_json(&self) -> String {
        let mut cached = self.headers_json.lock().unwrap_or_else(|e| e.into_inner());
        cached
            .get_or_insert_with(|| json::map_to_json(&self.inner.headers))
            .clone()
    }

    /// Acknowledges the message. Does nothing when it was already settled or consumed with
    /// `no_ack`.
    pub fn ack(&self) -> Result<()> {
        if self.no_ack {
            return Ok(());
        }
        Ok(self.inner.ack()?)
    }

    /// Rejects the message. With `requeue` it is delivered again; otherwise it is
    /// dead-lettered when the queue has a dead-letter exchange, and dropped if not.
    pub fn nack(&self, requeue: bool) -> Result<()> {
        if self.no_ack {
            return Ok(());
        }
        Ok(self.inner.nack(requeue)?)
    }
}

#[cfg(test)]
mod tests;

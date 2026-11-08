use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use bytes::Bytes;
use firemq_proto::{self as proto, Command, Map, Properties};
use tokio::sync::mpsc;

use crate::client::Conn;
use crate::{Client, Error, Result};

/// Tracks credit for a subscription on one connection and tops it up as messages are
/// handled.
pub(crate) struct Credit {
    sub: u32,
    window: u32,
    pub(crate) generation: u64,
    conn: Weak<Conn>,
    released: AtomicU32,
}

impl Credit {
    pub(crate) fn new(sub: u32, window: u32, conn: &Arc<Conn>) -> Credit {
        Credit {
            sub,
            window,
            generation: conn.generation,
            conn: Arc::downgrade(conn),
            released: AtomicU32::new(0),
        }
    }

    /// Records handled messages; grants credit back in batches of half the window.
    fn release(&self, n: u32) {
        let total = self.released.fetch_add(n, Ordering::AcqRel) + n;
        let batch = (self.window / 2).max(1);
        if total >= batch {
            let granted = self.released.swap(0, Ordering::AcqRel);
            if let (true, Some(conn)) = (granted > 0, self.conn.upgrade()) {
                let _ = conn.send(Command::Credit(proto::Credit {
                    sub: self.sub,
                    credit: granted,
                }));
            }
        }
    }
}

pub(crate) struct Acker {
    /// The connection the message arrived on. Delivery tags only mean something there.
    conn: Arc<Conn>,
    /// The client reconnects, so the broker redelivers messages from a lost connection.
    recovers: bool,
    done: AtomicBool,
}

impl Acker {
    pub(crate) fn new(conn: Arc<Conn>, recovers: bool) -> Acker {
        Acker {
            conn,
            recovers,
            done: AtomicBool::new(false),
        }
    }
}

/// A delivered message.
pub struct Message {
    pub body: Bytes,
    pub exchange: String,
    pub routing_key: String,
    pub properties: Properties,
    pub headers: Map,
    /// True when the message was delivered before and not acknowledged.
    pub redelivered: bool,
    pub delivery_tag: u64,
    pub(crate) acker: Option<Acker>,
    pub(crate) credit: Option<Arc<Credit>>,
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("exchange", &self.exchange)
            .field("routing_key", &self.routing_key)
            .field("delivery_tag", &self.delivery_tag)
            .field("redelivered", &self.redelivered)
            .field("body_len", &self.body.len())
            .finish()
    }
}

impl Message {
    /// The body as UTF-8 text.
    pub fn body_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.body).ok()
    }

    /// Decodes a JSON body.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.body).map_err(|e| Error::Protocol(e.to_string()))
    }

    fn settle(&self, cmd: Command) -> Result<()> {
        let Some(a) = &self.acker else {
            return Ok(());
        };
        if a.done.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if a.recovers && !a.conn.is_open() {
            // The connection this message came on is gone, so the broker has put the
            // message back and will deliver it again.
            tracing::debug!("ignoring an ack for a message from a lost connection");
            return Ok(());
        }
        a.conn.send(cmd)?;
        if let Some(c) = &self.credit {
            c.release(1);
        }
        Ok(())
    }

    /// True when the message came on a connection the client has since lost. The broker
    /// delivers it again on the new connection.
    fn is_stale(&self) -> bool {
        self.acker
            .as_ref()
            .is_some_and(|a| a.recovers && !a.conn.is_open())
    }

    /// Acknowledges the message. Does nothing for `no_ack` subscriptions or when already
    /// settled.
    pub fn ack(&self) -> Result<()> {
        self.settle(Command::Ack(proto::Ack {
            tag: self.delivery_tag,
            multiple: false,
        }))
    }

    /// Rejects the message. With `requeue` it is delivered again; otherwise it is
    /// dead-lettered when the queue has a dead-letter exchange, and dropped if not.
    pub fn nack(&self, requeue: bool) -> Result<()> {
        self.settle(Command::Nack(proto::Nack {
            tag: self.delivery_tag,
            multiple: false,
            requeue,
        }))
    }
}

/// A stream of messages from a queue. Dropping it cancels the subscription.
///
/// The subscription survives reconnects. Messages that arrived on a lost connection and
/// need an ack are skipped, because the broker delivers them again.
pub struct Subscription {
    client: Client,
    sub: u32,
    rx: mpsc::UnboundedReceiver<Message>,
    no_ack: bool,
    cancelled: bool,
}

impl Subscription {
    pub(crate) fn new(
        client: Client,
        sub: u32,
        rx: mpsc::UnboundedReceiver<Message>,
        no_ack: bool,
    ) -> Subscription {
        Subscription {
            client,
            sub,
            rx,
            no_ack,
            cancelled: false,
        }
    }

    /// Returns the message to hand out, or `None` to skip it.
    fn accept(&self, msg: Message) -> Option<Message> {
        if msg.is_stale() {
            return None;
        }
        if self.no_ack
            && let Some(c) = &msg.credit
        {
            c.release(1);
        }
        Some(msg)
    }

    /// Waits for the next message. Returns `None` once the subscription ends, for example
    /// because the queue was deleted or the client closed.
    pub async fn next(&mut self) -> Option<Message> {
        loop {
            let msg = self.rx.recv().await?;
            if let Some(msg) = self.accept(msg) {
                return Some(msg);
            }
        }
    }

    /// Stops the subscription. Unacknowledged messages can still be acknowledged.
    pub async fn cancel(mut self) -> Result<()> {
        self.stop();
        Ok(())
    }

    fn stop(&mut self) {
        self.cancelled = true;
        self.client.inner.subs.lock().remove(&self.sub);
        let _ = self.client.send(Command::Unsubscribe(proto::Unsubscribe {
            rid: 0,
            sub: self.sub,
        }));
    }
}

impl futures::Stream for Subscription {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Message>> {
        let this = &mut *self;
        loop {
            match this.rx.poll_recv(cx) {
                Poll::Ready(Some(msg)) => {
                    if let Some(msg) = this.accept(msg) {
                        return Poll::Ready(Some(msg));
                    }
                }
                other => return other,
            }
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if !self.cancelled {
            self.stop();
        }
    }
}

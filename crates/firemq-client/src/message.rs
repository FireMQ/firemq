use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use firemq_proto::{self as proto, Command, Map, Properties};
use tokio::sync::mpsc;

use crate::{Client, Error, Result};

/// Tracks credit for a subscription and tops it up as messages are handled.
pub(crate) struct Credit {
    sub: u32,
    window: u32,
    released: AtomicU32,
}

impl Credit {
    pub(crate) fn new(sub: u32, window: u32) -> Credit {
        Credit {
            sub,
            window,
            released: AtomicU32::new(0),
        }
    }

    /// Records handled messages; grants credit back in batches of half the window.
    fn release(&self, client: &Client, n: u32) {
        let total = self.released.fetch_add(n, Ordering::AcqRel) + n;
        let batch = (self.window / 2).max(1);
        if total >= batch {
            let granted = self.released.swap(0, Ordering::AcqRel);
            if granted > 0 {
                let _ = client.send(Command::Credit(proto::Credit {
                    sub: self.sub,
                    credit: granted,
                }));
            }
        }
    }
}

pub(crate) struct Acker {
    client: Client,
    credit: Option<Arc<Credit>>,
    done: AtomicBool,
}

impl Acker {
    pub(crate) fn new(client: Client, credit: Option<Arc<Credit>>) -> Acker {
        Acker {
            client,
            credit,
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
        a.client.send(cmd)?;
        if let Some(c) = &a.credit {
            c.release(&a.client, 1);
        }
        Ok(())
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
pub struct Subscription {
    client: Client,
    sub: u32,
    rx: mpsc::UnboundedReceiver<Message>,
    credit: Arc<Credit>,
    no_ack: bool,
    cancelled: bool,
}

impl Subscription {
    pub(crate) fn new(
        client: Client,
        sub: u32,
        rx: mpsc::UnboundedReceiver<Message>,
        credit: Arc<Credit>,
        no_ack: bool,
    ) -> Subscription {
        Subscription {
            client,
            sub,
            rx,
            credit,
            no_ack,
            cancelled: false,
        }
    }

    /// Waits for the next message. Returns `None` once the subscription ends, for example
    /// because the queue was deleted or the connection closed.
    pub async fn next(&mut self) -> Option<Message> {
        let msg = self.rx.recv().await?;
        if self.no_ack {
            self.credit.release(&self.client, 1);
        }
        Some(msg)
    }

    /// Stops the subscription. Unacknowledged messages can still be acknowledged.
    pub async fn cancel(mut self) -> Result<()> {
        self.cancelled = true;
        self.client.inner.state.lock().subs.remove(&self.sub);
        let _ = self.client.send(Command::Unsubscribe(proto::Unsubscribe {
            rid: 0,
            sub: self.sub,
        }));
        Ok(())
    }
}

impl futures::Stream for Subscription {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Message>> {
        let this = &mut *self;
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(msg)) => {
                if this.no_ack {
                    this.credit.release(&this.client, 1);
                }
                Poll::Ready(Some(msg))
            }
            other => other,
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if !self.cancelled {
            self.client.inner.state.lock().subs.remove(&self.sub);
            let _ = self.client.send(Command::Unsubscribe(proto::Unsubscribe {
                rid: 0,
                sub: self.sub,
            }));
        }
    }
}

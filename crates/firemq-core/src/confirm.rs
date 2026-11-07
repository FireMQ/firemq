use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::mpsc::UnboundedSender;

/// Events the broker sends to a connection outside the request/reply flow.
#[derive(Debug)]
pub enum ConnEvent {
    /// A publish was confirmed (`ack`) or rejected.
    Confirm { channel: u16, seq: u64, ack: bool },
    /// The broker cancelled a consumer, for example because its queue was deleted.
    ConsumerCancelled { consumer_id: u64, reason: String },
    /// The memory or disk alarm changed state; publishers should pause while `true`.
    Blocked(bool),
    /// An operator asked to close the connection.
    Close { code: u16, reason: String },
    /// Credit freed on a channel with a shared (global) prefetch limit.
    Wake,
}

struct TicketInner {
    events: UnboundedSender<ConnEvent>,
    channel: u16,
    seq: u64,
    nack: AtomicBool,
}

impl Drop for TicketInner {
    fn drop(&mut self) {
        let _ = self.events.send(ConnEvent::Confirm {
            channel: self.channel,
            seq: self.seq,
            ack: !self.nack.load(Ordering::Relaxed),
        });
    }
}

/// A pending publisher confirm.
///
/// The connection creates one ticket per confirmed publish and hands a clone to every queue
/// that receives the message. A queue drops its clone once the message is safe: on enqueue
/// for transient messages, after fsync for persistent ones. When the last clone drops, the
/// confirm is sent. Calling [`Ticket::nack`] on any clone turns the confirm into a rejection.
#[derive(Clone)]
pub struct Ticket(Arc<TicketInner>);

impl Ticket {
    pub fn new(events: UnboundedSender<ConnEvent>, channel: u16, seq: u64) -> Ticket {
        Ticket(Arc::new(TicketInner {
            events,
            channel,
            seq,
            nack: AtomicBool::new(false),
        }))
    }

    /// Marks the publish as rejected. The confirm is still sent when the last clone drops.
    pub fn nack(&self) {
        self.0.nack.store(true, Ordering::Relaxed);
    }

    pub fn seq(&self) -> u64 {
        self.0.seq
    }
}

impl std::fmt::Debug for Ticket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ticket(ch={}, seq={})", self.0.channel, self.0.seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn last_drop_sends_one_confirm() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let t = Ticket::new(tx, 1, 9);
        let a = t.clone();
        let b = t.clone();
        drop(t);
        drop(a);
        assert!(rx.try_recv().is_err());
        b.nack();
        drop(b);
        match rx.try_recv().unwrap() {
            ConnEvent::Confirm { channel, seq, ack } => {
                assert_eq!((channel, seq, ack), (1, 9, false));
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(rx.try_recv().is_err());
    }
}

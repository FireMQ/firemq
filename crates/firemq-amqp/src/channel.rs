use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use firemq_amqp_codec::BasicProperties;
use firemq_amqp_codec::methods::BasicPublish;
use firemq_core::{QueueHandle, SharedLimit};

/// Tracks publisher confirms on a channel and decides when to send cumulative acks.
#[derive(Debug, Default)]
pub struct ConfirmTracker {
    next: u64,
    pending: BTreeSet<u64>,
    acked: BTreeSet<u64>,
    emitted: u64,
}

impl ConfirmTracker {
    /// Assigns the next publish sequence number, starting at 1.
    pub fn issue(&mut self) -> u64 {
        let seq = self.next.max(1);
        self.next = seq + 1;
        self.pending.insert(seq);
        seq
    }

    /// Records a completed publish. Returns `(tag, multiple)` for a `basic.ack` when the
    /// contiguous acknowledged range grew and contains at least one positive confirm.
    pub fn complete(&mut self, seq: u64, ack: bool) -> Option<(u64, bool)> {
        if !self.pending.remove(&seq) {
            return None;
        }
        if ack {
            self.acked.insert(seq);
        }
        let watermark = match self.pending.first() {
            Some(first) => first - 1,
            None => self.next.saturating_sub(1),
        };
        if watermark <= self.emitted {
            return None;
        }
        let has_ack = self.acked.range(..=watermark).next().is_some();
        self.acked = self.acked.split_off(&(watermark + 1));
        let multiple = watermark > self.emitted + 1;
        self.emitted = watermark;
        has_ack.then_some((watermark, multiple))
    }
}

/// A publish waiting for its content header and body frames.
#[derive(Debug)]
pub struct PendingContent {
    pub method: BasicPublish,
    pub properties: Option<BasicProperties>,
    pub body_size: u64,
    pub chunks: Vec<Bytes>,
    pub received: u64,
}

impl PendingContent {
    pub fn new(method: BasicPublish) -> Self {
        Self {
            method,
            properties: None,
            body_size: 0,
            chunks: Vec::new(),
            received: 0,
        }
    }

    pub fn body(&mut self) -> Bytes {
        match self.chunks.len() {
            0 => Bytes::new(),
            1 => self.chunks.pop().expect("one chunk"),
            _ => {
                let mut b = BytesMut::with_capacity(self.received as usize);
                for c in self.chunks.drain(..) {
                    b.extend_from_slice(&c);
                }
                b.freeze()
            }
        }
    }
}

/// A delivery removed from a channel's unacked map: (delivery tag, queue, queue seq).
pub type Taken = (u64, QueueHandle, u64);

/// A buffered operation in a transactional channel.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum TxOp {
    Publish {
        method: BasicPublish,
        properties: BasicProperties,
        body: Bytes,
    },
    Ack(Vec<Taken>),
    Reject(Vec<Taken>, bool),
}

/// One AMQP channel's state.
pub struct Channel {
    /// True after the broker sent `channel.close` and is waiting for `close-ok`.
    pub closing: bool,
    pub confirm: Option<ConfirmTracker>,
    pub tx: Option<Vec<TxOp>>,
    pub prefetch: u16,
    pub global_limit: Option<Arc<SharedLimit>>,
    pub next_tag: u64,
    /// Delivery tag to (queue, queue sequence number), ordered for `multiple` acks.
    pub unacked: BTreeMap<u64, (QueueHandle, u64)>,
    /// Consumer tag to consumer id.
    pub consumers: HashMap<String, u64>,
    pub pending: Option<PendingContent>,
    /// Name of the last queue declared, used when a method names the queue as "".
    pub last_queue: String,
}

impl Channel {
    pub fn new() -> Channel {
        Channel {
            closing: false,
            confirm: None,
            tx: None,
            prefetch: 0,
            global_limit: None,
            next_tag: 1,
            unacked: BTreeMap::new(),
            consumers: HashMap::new(),
            pending: None,
            last_queue: String::new(),
        }
    }

    pub fn take_tag(&mut self) -> u64 {
        let t = self.next_tag;
        self.next_tag += 1;
        t
    }

    /// Removes deliveries for an ack, nack or reject. `tag` 0 with `multiple` means all.
    pub fn take_unacked(&mut self, tag: u64, multiple: bool) -> Option<Vec<Taken>> {
        if multiple {
            if tag >= self.next_tag {
                return None;
            }
            let rest = if tag == 0 {
                BTreeMap::new()
            } else {
                self.unacked.split_off(&(tag + 1))
            };
            let taken = std::mem::replace(&mut self.unacked, rest);
            Some(taken.into_iter().map(|(t, (q, s))| (t, q, s)).collect())
        } else {
            self.unacked.remove(&tag).map(|(q, s)| vec![(tag, q, s)])
        }
    }

    /// Puts deliveries back after a transaction rollback.
    pub fn restore_unacked(&mut self, items: Vec<Taken>) {
        for (t, q, s) in items {
            self.unacked.insert(t, (q, s));
        }
    }
}

/// Groups taken deliveries by queue, keeping order.
pub fn group_by_queue(items: Vec<Taken>) -> Vec<(QueueHandle, Vec<u64>)> {
    let mut out: Vec<(QueueHandle, Vec<u64>)> = Vec::new();
    for (_, q, seq) in items {
        match out.iter_mut().find(|(x, _)| *x == q) {
            Some((_, seqs)) => seqs.push(seq),
            None => out.push((q, vec![seq])),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confirms_are_cumulative_and_skip_nack_only_ranges() {
        let mut t = ConfirmTracker::default();
        let (a, b, c) = (t.issue(), t.issue(), t.issue());
        assert_eq!((a, b, c), (1, 2, 3));
        // 2 completes first: nothing yet, 1 is outstanding.
        assert_eq!(t.complete(2, true), None);
        // 1 completes: ack up to 2 with multiple.
        assert_eq!(t.complete(1, true), Some((2, true)));
        // 3 was nacked (sent separately); no ack for a nack-only range.
        assert_eq!(t.complete(3, false), None);
        let d = t.issue();
        assert_eq!(t.complete(d, true), Some((4, false)));
        // Unknown or repeated completions are ignored.
        assert_eq!(t.complete(4, true), None);
    }
}

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use firemq_amqp_codec::{FieldTable, Value};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self, OwnedPermit};
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::{
    ConsumerSpec, Delivery, GetResult, Limit, Owner, QueueCmd, QueueConfig, QueueHandle,
    QueueShared, SharedLimit,
};
use crate::confirm::{ConnEvent, Ticket};
use crate::error::{BrokerError, Result};
use crate::message::{Message, now_ms};
use crate::storage::{Loc, QueueStore, Recovered};
use crate::vhost::VHost;

type ParkFuture =
    Pin<Box<dyn std::future::Future<Output = (u64, Option<OwnedPermit<Delivery>>)> + Send>>;

/// Commands handled per wakeup before the actor delivers and commits.
const BATCH: usize = 512;

struct Entry {
    seq: u64,
    msg: Option<Arc<Message>>,
    loc: Loc,
    size: u64,
    expires_at: Option<u64>,
    redelivered: bool,
    priority: u8,
}

struct Unacked {
    entry: Entry,
    consumer: Option<u64>,
    owner: Owner,
}

struct Consumer {
    id: u64,
    owner: Owner,
    deliveries: mpsc::Sender<Delivery>,
    events: mpsc::UnboundedSender<ConnEvent>,
    no_ack: bool,
    exclusive: bool,
    limit: Limit,
    credit: u32,
    unacked: u32,
    parked: bool,
    shared_limit: Option<Arc<SharedLimit>>,
}

impl Consumer {
    fn can_take(&self) -> bool {
        if self.parked {
            return false;
        }
        let own = match self.limit {
            Limit::Prefetch(0) => true,
            Limit::Prefetch(n) => self.no_ack || self.unacked < n,
            Limit::Credit(_) => self.credit > 0,
        };
        own && (self.no_ack || self.shared_limit.as_ref().is_none_or(|s| s.has_room()))
    }

    fn release(&mut self) {
        self.unacked = self.unacked.saturating_sub(1);
        if let Some(s) = &self.shared_limit {
            s.release();
            let _ = self.events.send(ConnEvent::Wake);
        }
    }
}

/// Ready messages, by priority (a single level unless `x-max-priority` is set).
struct Ready {
    levels: Vec<VecDeque<Entry>>,
    len: usize,
}

impl Ready {
    fn new(max_priority: Option<u8>) -> Ready {
        let n = max_priority.map(|p| p as usize + 1).unwrap_or(1);
        Ready {
            levels: (0..n).map(|_| VecDeque::new()).collect(),
            len: 0,
        }
    }

    fn level(&self, priority: u8) -> usize {
        (priority as usize).min(self.levels.len() - 1)
    }

    fn push(&mut self, e: Entry) {
        let l = self.level(e.priority);
        self.levels[l].push_back(e);
        self.len += 1;
    }

    fn front(&self) -> Option<&Entry> {
        self.levels.iter().rev().find_map(|q| q.front())
    }

    fn pop(&mut self) -> Option<Entry> {
        let e = self.levels.iter_mut().rev().find_map(|q| q.pop_front());
        if e.is_some() {
            self.len -= 1;
        }
        e
    }

    /// Removes the oldest message of the lowest priority, for `drop-head` overflow.
    fn pop_oldest(&mut self) -> Option<Entry> {
        let e = self.levels.iter_mut().find_map(|q| q.pop_front());
        if e.is_some() {
            self.len -= 1;
        }
        e
    }

    fn drain(&mut self) -> impl Iterator<Item = Entry> + '_ {
        self.len = 0;
        self.levels.iter_mut().flat_map(|q| q.drain(..))
    }
}

pub(crate) struct Actor {
    shared: Arc<QueueShared>,
    handle: QueueHandle,
    rx: mpsc::Receiver<QueueCmd>,
    store: Box<dyn QueueStore>,
    cfg: QueueConfig,
    vhost: Weak<VHost>,
    next_seq: u64,
    ready: Ready,
    requeued: BTreeMap<u64, Entry>,
    ready_bytes: u64,
    unacked: HashMap<u64, Unacked>,
    consumers: Vec<Consumer>,
    /// Cancelled consumers that still hold unacknowledged messages.
    retired: HashMap<u64, Consumer>,
    rr: usize,
    parked: FuturesUnordered<ParkFuture>,
    tickets: Vec<Ticket>,
    keep_bodies: bool,
    cached_bytes: u64,
    cache_limit: u64,
    ever_consumed: bool,
    last_used: u64,
    stopping: bool,
    delete_data: bool,
    shutdown_reply: Option<oneshot::Sender<u64>>,
    published: u64,
    delivered: u64,
    acked: u64,
    redelivered: u64,
}

impl Actor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        shared: Arc<QueueShared>,
        rx: mpsc::Receiver<QueueCmd>,
        store: Box<dyn QueueStore>,
        cfg: QueueConfig,
        vhost: Weak<VHost>,
        cache_limit: u64,
        recovered: Vec<Recovered>,
    ) -> Actor {
        let keep_bodies = store.bodies_in_memory();
        let mut actor = Actor {
            handle: QueueHandle(shared.clone()),
            shared,
            rx,
            store,
            ready: Ready::new(cfg.max_priority),
            cfg,
            vhost,
            next_seq: 1,
            requeued: BTreeMap::new(),
            ready_bytes: 0,
            unacked: HashMap::new(),
            consumers: Vec::new(),
            retired: HashMap::new(),
            rr: 0,
            parked: FuturesUnordered::new(),
            tickets: Vec::new(),
            keep_bodies,
            cached_bytes: 0,
            cache_limit,
            ever_consumed: false,
            last_used: now_ms(),
            stopping: false,
            delete_data: true,
            shutdown_reply: None,
            published: 0,
            delivered: 0,
            acked: 0,
            redelivered: 0,
        };
        for r in recovered {
            actor.next_seq = actor.next_seq.max(r.seq + 1);
            let size = r.message.size() as u64;
            let ttl = actor.ttl_for(&r.message);
            let priority = r.message.properties.priority.unwrap_or(0);
            actor.ready_bytes += size;
            actor.ready.push(Entry {
                seq: r.seq,
                msg: None,
                loc: r.loc,
                size,
                expires_at: ttl.map(|t| r.message.published_at.saturating_add(t)),
                redelivered: true,
                priority,
            });
        }
        actor.publish_stats();
        actor
    }

    pub(crate) async fn run(mut self) {
        let sleep = tokio::time::sleep(Duration::from_secs(3600));
        tokio::pin!(sleep);
        loop {
            let deadline = self.next_deadline();
            if let Some(d) = deadline {
                sleep.as_mut().reset(d);
            }
            tokio::select! {
                cmd = self.rx.recv() => {
                    let Some(cmd) = cmd else { break };
                    self.handle(cmd);
                    let mut n = 0;
                    while n < BATCH && !self.stopping {
                        match self.rx.try_recv() {
                            Ok(cmd) => self.handle(cmd),
                            Err(_) => break,
                        }
                        n += 1;
                    }
                }
                Some((id, permit)) = self.parked.next(), if !self.parked.is_empty() => {
                    self.unpark(id, permit);
                }
                _ = &mut sleep, if deadline.is_some() => self.on_timer(),
            }
            if self.stopping {
                self.finish();
                break;
            }
            self.after_batch();
        }
    }

    fn handle(&mut self, cmd: QueueCmd) {
        match cmd {
            QueueCmd::Publish { msg, ticket } => self.publish(msg, ticket),
            QueueCmd::Consume { spec, reply } => {
                let r = self.add_consumer(spec);
                let _ = reply.send(r);
            }
            QueueCmd::Cancel { consumer_id, reply } => {
                self.remove_consumer(consumer_id, false);
                if let Some(r) = reply {
                    let _ = r.send(());
                }
            }
            QueueCmd::Ack { seqs } => {
                for s in seqs {
                    self.ack(s);
                }
            }
            QueueCmd::Reject { seqs, requeue } => {
                for s in seqs {
                    self.reject(s, requeue);
                }
            }
            QueueCmd::Credit {
                consumer_id,
                credit,
            } => {
                if let Some(c) = self.consumers.iter_mut().find(|c| c.id == consumer_id) {
                    c.credit = c.credit.saturating_add(credit);
                }
            }
            QueueCmd::Get {
                owner,
                no_ack,
                reply,
            } => {
                let r = self.get(owner, no_ack);
                let _ = reply.send(r);
            }
            QueueCmd::Purge { reply } => {
                let n = self.purge();
                let _ = reply.send(n);
            }
            QueueCmd::Recover { owner } => self.requeue_owner(owner.conn, Some(owner.channel)),
            QueueCmd::OwnerClosed { conn, channel } => {
                let ids: Vec<u64> = self
                    .consumers
                    .iter()
                    .filter(|c| {
                        c.owner.conn == conn && channel.is_none_or(|ch| ch == c.owner.channel)
                    })
                    .map(|c| c.id)
                    .collect();
                for id in ids {
                    self.remove_consumer(id, false);
                }
                self.requeue_owner(conn, channel);
            }
            QueueCmd::Touch => self.last_used = now_ms(),
            QueueCmd::Wake => {}
            QueueCmd::SetConfig(cfg) => {
                if cfg.max_priority != self.cfg.max_priority {
                    // Priority levels are fixed at declaration, as in RabbitMQ.
                    let max_priority = self.cfg.max_priority;
                    self.cfg = cfg;
                    self.cfg.max_priority = max_priority;
                } else {
                    self.cfg = cfg;
                }
                self.enforce_limits();
            }
            QueueCmd::Shutdown { reply, delete_data } => {
                self.stopping = true;
                self.delete_data = delete_data;
                self.shutdown_reply = reply;
            }
        }
    }

    fn ready_count(&self) -> usize {
        self.ready.len + self.requeued.len()
    }

    fn ttl_for(&self, msg: &Message) -> Option<u64> {
        match (self.cfg.message_ttl, msg.expiration_ms()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    fn would_overflow(&self, size: u64) -> bool {
        self.cfg
            .max_length
            .is_some_and(|max| self.ready_count() as u64 >= max)
            || self
                .cfg
                .max_length_bytes
                .is_some_and(|max| self.ready_bytes + size > max)
    }

    fn over_limit(&self) -> bool {
        self.cfg
            .max_length
            .is_some_and(|max| self.ready_count() as u64 > max)
            || self
                .cfg
                .max_length_bytes
                .is_some_and(|max| self.ready_bytes > max)
    }

    fn publish(&mut self, msg: Arc<Message>, ticket: Option<Ticket>) {
        self.published += 1;
        let size = msg.size() as u64;
        if self.cfg.overflow != super::Overflow::DropHead && self.would_overflow(size) {
            if let Some(t) = &ticket {
                t.nack();
            }
            if self.cfg.overflow == super::Overflow::RejectPublishDlx {
                self.dead_letter(&msg, "maxlen");
            }
            return;
        }
        let persistent = self.shared.durable && msg.is_persistent();
        let seq = self.next_seq;
        self.next_seq += 1;
        let loc = match self.store.append(seq, &msg, persistent) {
            Ok(loc) => loc,
            Err(e) => {
                tracing::error!(queue = %self.shared.name, error = %e, "failed to store message");
                if let Some(t) = &ticket {
                    t.nack();
                }
                return;
            }
        };
        let now = now_ms();
        let expires_at = self.ttl_for(&msg).map(|t| now.saturating_add(t));
        let keep = self.keep_bodies || self.cached_bytes + size <= self.cache_limit;
        if keep && !self.keep_bodies {
            self.cached_bytes += size;
        }
        let priority = msg.properties.priority.unwrap_or(0);
        self.ready_bytes += size;
        self.ready.push(Entry {
            seq,
            msg: keep.then_some(msg),
            loc,
            size,
            expires_at,
            redelivered: false,
            priority,
        });
        if let Some(t) = ticket {
            if persistent {
                self.tickets.push(t);
            }
        }
        self.enforce_limits();
    }

    fn enforce_limits(&mut self) {
        while self.over_limit() {
            let entry = if let Some((_, e)) = self.requeued.pop_first() {
                e
            } else if let Some(e) = self.ready.pop_oldest() {
                e
            } else {
                break;
            };
            self.ready_bytes -= entry.size;
            self.discard(entry, "maxlen");
        }
    }

    fn add_consumer(&mut self, spec: ConsumerSpec) -> Result<()> {
        if self.consumers.iter().any(|c| c.exclusive)
            || (spec.exclusive && !self.consumers.is_empty())
        {
            return Err(BrokerError::access_refused(format!(
                "queue '{}' in vhost '{}' in exclusive use",
                self.shared.name, self.shared.vhost
            )));
        }
        let credit = match spec.limit {
            Limit::Credit(n) => n,
            Limit::Prefetch(_) => 0,
        };
        self.consumers.push(Consumer {
            id: spec.id,
            owner: spec.owner,
            deliveries: spec.deliveries,
            events: spec.events,
            no_ack: spec.no_ack,
            exclusive: spec.exclusive,
            limit: spec.limit,
            credit,
            unacked: 0,
            parked: false,
            shared_limit: spec.shared_limit,
        });
        self.ever_consumed = true;
        self.last_used = now_ms();
        Ok(())
    }

    fn remove_consumer(&mut self, id: u64, notify: bool) {
        let Some(pos) = self.consumers.iter().position(|c| c.id == id) else {
            return;
        };
        let c = self.consumers.remove(pos);
        if self.rr > pos {
            self.rr -= 1;
        }
        if notify {
            let _ = c.events.send(ConnEvent::ConsumerCancelled {
                consumer_id: c.id,
                reason: format!("queue '{}' was deleted", self.shared.name),
            });
        }
        if c.unacked > 0 {
            self.retired.insert(c.id, c);
        }
        self.last_used = now_ms();
        if self.consumers.is_empty() && self.shared.auto_delete && self.ever_consumed {
            self.delete_self();
        }
    }

    fn consumer_mut(&mut self, id: u64) -> Option<&mut Consumer> {
        if let Some(c) = self.consumers.iter_mut().find(|c| c.id == id) {
            return Some(c);
        }
        self.retired.get_mut(&id)
    }

    fn release_consumer(&mut self, id: Option<u64>) {
        let Some(id) = id else { return };
        if let Some(c) = self.consumer_mut(id) {
            c.release();
            if c.unacked == 0 {
                self.retired.remove(&id);
            }
        }
    }

    fn ack(&mut self, seq: u64) {
        if let Some(u) = self.unacked.remove(&seq) {
            self.acked += 1;
            self.release_consumer(u.consumer);
            self.finish_entry(u.entry);
        }
    }

    fn reject(&mut self, seq: u64, requeue: bool) {
        let Some(u) = self.unacked.remove(&seq) else {
            return;
        };
        self.release_consumer(u.consumer);
        if requeue {
            let mut e = u.entry;
            e.redelivered = true;
            self.ready_bytes += e.size;
            self.requeued.insert(e.seq, e);
        } else {
            self.discard(u.entry, "rejected");
        }
    }

    fn requeue_owner(&mut self, conn: u64, channel: Option<u16>) {
        let seqs: Vec<u64> = self
            .unacked
            .iter()
            .filter(|(_, u)| u.owner.conn == conn && channel.is_none_or(|ch| ch == u.owner.channel))
            .map(|(s, _)| *s)
            .collect();
        for s in seqs {
            self.reject(s, true);
        }
    }

    fn peek_next(&self) -> Option<&Entry> {
        self.requeued
            .first_key_value()
            .map(|(_, e)| e)
            .or_else(|| self.ready.front())
    }

    fn pop_next(&mut self) -> Option<Entry> {
        let e = match self.requeued.pop_first() {
            Some((_, e)) => Some(e),
            None => self.ready.pop(),
        }?;
        self.ready_bytes -= e.size;
        Some(e)
    }

    fn drop_expired_head(&mut self, now: u64) {
        while let Some(e) = self.peek_next() {
            if e.expires_at.is_some_and(|t| t < now) {
                let e = self.pop_next().expect("peeked entry exists");
                self.discard(e, "expired");
            } else {
                break;
            }
        }
    }

    fn load(&mut self, entry: &Entry) -> Option<Arc<Message>> {
        if let Some(m) = &entry.msg {
            return Some(m.clone());
        }
        match self.store.read(entry.loc) {
            Ok(m) => Some(Arc::new(m)),
            Err(e) => {
                tracing::error!(queue = %self.shared.name, seq = entry.seq, error = %e, "failed to read message");
                None
            }
        }
    }

    /// Releases an entry's storage after an ack, expiry or drop.
    fn finish_entry(&mut self, entry: Entry) {
        if entry.msg.is_some() && !self.keep_bodies {
            self.cached_bytes = self.cached_bytes.saturating_sub(entry.size);
        }
        if let Err(e) = self.store.remove(entry.loc) {
            tracing::warn!(queue = %self.shared.name, error = %e, "failed to remove message from store");
        }
    }

    /// Dead-letters an entry when a dead-letter exchange is set, then releases it.
    fn discard(&mut self, entry: Entry, reason: &str) {
        if self.cfg.dead_letter_exchange.is_some() {
            if let Some(msg) = self.load(&entry) {
                self.dead_letter(&msg, reason);
            }
        }
        self.finish_entry(entry);
    }

    fn dead_letter(&mut self, msg: &Message, reason: &str) {
        let Some(dlx) = self.cfg.dead_letter_exchange.clone() else {
            return;
        };
        let Some(vhost) = self.vhost.upgrade() else {
            return;
        };
        let mut out = msg.clone();
        let routing_key = self
            .cfg
            .dead_letter_routing_key
            .clone()
            .unwrap_or_else(|| msg.routing_key.clone());
        let mut headers = out.properties.headers.take().unwrap_or_default();
        add_x_death(
            &mut headers,
            &self.shared.name,
            reason,
            &msg.exchange,
            &msg.routing_key,
        );
        let cyclic_ok = x_death_has_rejection(&headers);
        out.properties.headers = Some(headers);
        if reason == "expired" {
            out.properties.expiration = None;
        }
        out.exchange = dlx.clone();
        out.routing_key = routing_key.clone();
        out.published_at = now_ms();
        let targets = match vhost.route(&dlx, &routing_key, out.properties.headers.as_ref()) {
            Ok(t) => t,
            Err(e) => {
                tracing::debug!(queue = %self.shared.name, error = %e, "dead-letter exchange missing; dropping message");
                return;
            }
        };
        let out = Arc::new(out);
        for q in targets {
            if q.name() == self.shared.name && !cyclic_ok {
                // A message dead-lettered back into the same queue without any rejection
                // would cycle forever; RabbitMQ drops it, and so do we.
                continue;
            }
            q.publish_nowait(out.clone());
        }
    }

    fn get(&mut self, owner: Owner, no_ack: bool) -> Option<GetResult> {
        self.last_used = now_ms();
        self.drop_expired_head(now_ms());
        loop {
            let entry = self.pop_next()?;
            let Some(message) = self.load(&entry) else {
                self.finish_entry(entry);
                continue;
            };
            self.delivered += 1;
            if entry.redelivered {
                self.redelivered += 1;
            }
            let result = GetResult {
                seq: entry.seq,
                message,
                redelivered: entry.redelivered,
                remaining: self.ready_count() as u32,
            };
            if no_ack {
                self.finish_entry(entry);
            } else {
                self.unacked.insert(
                    entry.seq,
                    Unacked {
                        entry,
                        consumer: None,
                        owner,
                    },
                );
            }
            return Some(result);
        }
    }

    fn purge(&mut self) -> u64 {
        let mut entries: Vec<Entry> = self.ready.drain().collect();
        entries.extend(std::mem::take(&mut self.requeued).into_values());
        let n = entries.len() as u64;
        self.ready_bytes = 0;
        for e in entries {
            self.finish_entry(e);
        }
        n
    }

    fn pick_consumer(&mut self) -> Option<usize> {
        if self.cfg.single_active_consumer {
            return self.consumers.first().filter(|c| c.can_take()).map(|_| 0);
        }
        let n = self.consumers.len();
        for i in 0..n {
            let idx = (self.rr + i) % n;
            if self.consumers[idx].can_take() {
                self.rr = (idx + 1) % n;
                return Some(idx);
            }
        }
        None
    }

    fn park(&mut self, idx: usize, tx: mpsc::Sender<Delivery>) {
        let c = &mut self.consumers[idx];
        c.parked = true;
        let id = c.id;
        self.parked
            .push(Box::pin(async move { (id, tx.reserve_owned().await.ok()) }));
    }

    fn unpark(&mut self, id: u64, permit: Option<OwnedPermit<Delivery>>) {
        let Some(idx) = self.consumers.iter().position(|c| c.id == id) else {
            return;
        };
        self.consumers[idx].parked = false;
        let Some(permit) = permit else {
            self.remove_consumer(id, false);
            return;
        };
        self.drop_expired_head(now_ms());
        if self.ready_count() > 0 && self.consumers[idx].can_take() {
            self.deliver_one(idx, permit);
        }
    }

    fn try_deliver(&mut self) {
        if self.consumers.is_empty() {
            return;
        }
        let now = now_ms();
        loop {
            self.drop_expired_head(now);
            if self.ready_count() == 0 {
                break;
            }
            let Some(idx) = self.pick_consumer() else {
                break;
            };
            let tx = self.consumers[idx].deliveries.clone();
            match tx.try_reserve_owned() {
                Ok(permit) => self.deliver_one(idx, permit),
                Err(TrySendError::Full(tx)) => self.park(idx, tx),
                Err(TrySendError::Closed(_)) => {
                    let id = self.consumers[idx].id;
                    self.remove_consumer(id, false);
                }
            }
        }
    }

    fn deliver_one(&mut self, idx: usize, permit: OwnedPermit<Delivery>) {
        let c = &self.consumers[idx];
        if !c.no_ack {
            if let Some(s) = &c.shared_limit {
                if !s.try_acquire() {
                    return;
                }
            }
        }
        let Some(entry) = self.pop_next() else {
            if let Some(s) = &self.consumers[idx].shared_limit {
                s.release();
            }
            return;
        };
        let Some(message) = self.load(&entry) else {
            if let Some(s) = &self.consumers[idx].shared_limit {
                s.release();
            }
            self.finish_entry(entry);
            return;
        };
        let c = &mut self.consumers[idx];
        permit.send(Delivery {
            consumer_id: c.id,
            queue: self.handle.clone(),
            seq: entry.seq,
            message,
            redelivered: entry.redelivered,
        });
        if let Limit::Credit(_) = c.limit {
            c.credit = c.credit.saturating_sub(1);
        }
        let (no_ack, id, owner) = (c.no_ack, c.id, c.owner);
        if !no_ack {
            c.unacked += 1;
        }
        self.delivered += 1;
        if entry.redelivered {
            self.redelivered += 1;
        }
        if no_ack {
            self.finish_entry(entry);
        } else {
            self.unacked.insert(
                entry.seq,
                Unacked {
                    entry,
                    consumer: Some(id),
                    owner,
                },
            );
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        let now = now_ms();
        let mut at: Option<u64> = self.peek_next().and_then(|e| e.expires_at).map(|t| t + 1);
        if let Some(exp) = self.cfg.expires {
            if self.consumers.is_empty() {
                let t = self.last_used + exp;
                at = Some(at.map_or(t, |a| a.min(t)));
            }
        }
        at.map(|t| Instant::now() + Duration::from_millis(t.saturating_sub(now)))
    }

    fn on_timer(&mut self) {
        let now = now_ms();
        self.drop_expired_head(now);
        if let Some(exp) = self.cfg.expires {
            if self.consumers.is_empty() && now >= self.last_used + exp {
                tracing::debug!(queue = %self.shared.name, "queue expired");
                self.delete_self();
            }
        }
    }

    fn delete_self(&mut self) {
        if let Some(vhost) = self.vhost.upgrade() {
            vhost.unlink_queue(&self.handle);
        }
        self.stopping = true;
    }

    fn after_batch(&mut self) {
        self.try_deliver();
        if !self.tickets.is_empty() {
            let tickets = std::mem::take(&mut self.tickets);
            self.store.commit(tickets);
        }
        self.publish_stats();
    }

    fn publish_stats(&self) {
        let s = &self.shared.stats;
        s.ready.store(self.ready_count() as u64, Ordering::Relaxed);
        s.unacked
            .store(self.unacked.len() as u64, Ordering::Relaxed);
        s.ready_bytes.store(self.ready_bytes, Ordering::Relaxed);
        s.consumers
            .store(self.consumers.len() as u32, Ordering::Relaxed);
        s.published.store(self.published, Ordering::Relaxed);
        s.delivered.store(self.delivered, Ordering::Relaxed);
        s.acked.store(self.acked, Ordering::Relaxed);
        s.redelivered.store(self.redelivered, Ordering::Relaxed);
    }

    fn finish(&mut self) {
        let n = self.ready_count() as u64 + self.unacked.len() as u64;
        for c in std::mem::take(&mut self.consumers) {
            let _ = c.events.send(ConnEvent::ConsumerCancelled {
                consumer_id: c.id,
                reason: format!("queue '{}' was deleted", self.shared.name),
            });
        }
        self.parked = FuturesUnordered::new();
        if self.delete_data {
            self.tickets.clear();
            if let Err(e) = self.store.destroy() {
                tracing::warn!(queue = %self.shared.name, error = %e, "failed to delete queue data");
            }
        } else if !self.tickets.is_empty() {
            let tickets = std::mem::take(&mut self.tickets);
            self.store.commit(tickets);
        }
        self.ready = Ready::new(None);
        self.requeued.clear();
        self.unacked.clear();
        self.ready_bytes = 0;
        self.publish_stats();
        self.rx.close();
        if let Some(r) = self.shutdown_reply.take() {
            let _ = r.send(n);
        }
    }
}

/// Records a dead-lettering in the `x-death` header, as RabbitMQ does.
fn add_x_death(headers: &mut FieldTable, queue: &str, reason: &str, exchange: &str, key: &str) {
    let now = now_ms() / 1000;
    let mut deaths: Vec<Value> = headers
        .get("x-death")
        .and_then(|v| v.as_array())
        .map(|a| a.to_vec())
        .unwrap_or_default();
    let existing = deaths.iter().position(|d| {
        d.as_table().is_some_and(|t| {
            t.get_str("queue") == Some(queue) && t.get_str("reason") == Some(reason)
        })
    });
    let entry = match existing {
        Some(i) => {
            let Value::Table(mut t) = deaths.remove(i) else {
                unreachable!("position matched a table")
            };
            let count = t.get_i64("count").unwrap_or(0) + 1;
            t.insert("count", Value::I64(count));
            t.insert("time", Value::Timestamp(now));
            t
        }
        None => {
            let mut t = FieldTable::new();
            t.insert("count", Value::I64(1));
            t.insert("reason", reason);
            t.insert("queue", queue);
            t.insert("time", Value::Timestamp(now));
            t.insert("exchange", exchange);
            t.insert("routing-keys", Value::Array(vec![Value::string(key)]));
            t
        }
    };
    deaths.insert(0, Value::Table(entry));
    headers.insert("x-death", Value::Array(deaths));
    if headers.get("x-first-death-reason").is_none() {
        headers.insert("x-first-death-reason", reason);
        headers.insert("x-first-death-queue", queue);
        headers.insert("x-first-death-exchange", exchange);
    }
    headers.insert("x-last-death-reason", reason);
    headers.insert("x-last-death-queue", queue);
    headers.insert("x-last-death-exchange", exchange);
}

fn x_death_has_rejection(headers: &FieldTable) -> bool {
    headers
        .get("x-death")
        .and_then(|v| v.as_array())
        .is_some_and(|a| {
            a.iter().any(|d| {
                d.as_table()
                    .is_some_and(|t| t.get_str("reason") == Some("rejected"))
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn x_death_counts_repeat_deaths() {
        let mut h = FieldTable::new();
        add_x_death(&mut h, "work", "rejected", "ex", "k");
        add_x_death(&mut h, "work", "rejected", "ex", "k");
        add_x_death(&mut h, "retry", "expired", "dlx", "k");
        let deaths = h.get("x-death").unwrap().as_array().unwrap();
        assert_eq!(deaths.len(), 2);
        let first = deaths[0].as_table().unwrap();
        assert_eq!(first.get_str("queue"), Some("retry"));
        let second = deaths[1].as_table().unwrap();
        assert_eq!(second.get_i64("count"), Some(2));
        assert_eq!(h.get_str("x-first-death-queue"), Some("work"));
        assert_eq!(h.get_str("x-last-death-queue"), Some("retry"));
        assert!(x_death_has_rejection(&h));
    }
}

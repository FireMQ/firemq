use std::io;

use crate::confirm::Ticket;
use crate::message::Message;

/// Where a store keeps a message. Opaque to the core.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Loc {
    pub segment: u32,
    pub offset: u32,
    pub len: u32,
}

/// A message found by [`QueueStore::recover`]: enough metadata to queue it without
/// loading its body.
#[derive(Debug, Clone)]
pub struct Recovered {
    pub seq: u64,
    pub loc: Loc,
    /// Body size in bytes.
    pub size: u64,
    /// Arrival time in milliseconds since the Unix epoch.
    pub published_at: u64,
    /// The per-message TTL from the `expiration` property.
    pub expiration_ms: Option<u64>,
    pub priority: u8,
}

/// Persistence for one queue's messages. Owned by the queue's actor, so implementations
/// need no locking.
pub trait QueueStore: Send + 'static {
    /// Stores a message. `persistent` messages must survive a restart once their tickets
    /// are released by [`QueueStore::commit`].
    fn append(&mut self, seq: u64, msg: &Message, persistent: bool) -> io::Result<Loc>;

    /// Reads a message back. Only called when [`QueueStore::bodies_in_memory`] is false.
    fn read(&mut self, loc: Loc) -> io::Result<Message>;

    /// Forgets a message that was acknowledged, expired or dropped.
    fn remove(&mut self, loc: Loc) -> io::Result<()>;

    /// Ends a batch: writes buffered data and takes the confirm tickets of persistent
    /// messages appended since the last call, dropping them once those messages are durable.
    fn commit(&mut self, tickets: Vec<Ticket>);

    /// Returns the messages that survived a restart, in sequence order.
    fn recover(&mut self) -> io::Result<Vec<Recovered>>;

    /// Deletes all data. Called when the queue is deleted.
    fn destroy(&mut self) -> io::Result<()>;

    /// When true the queue keeps every body in memory and never calls `read`.
    fn bodies_in_memory(&self) -> bool {
        true
    }
}

/// Creates queue stores and persists definitions.
pub trait Store: Send + Sync + 'static {
    /// Opens (or creates) the store for a queue. `meta` describes the queue (vhost, name,
    /// flags and arguments) so a store can restore it after a crash.
    fn open_queue(
        &self,
        vhost: &str,
        queue: &str,
        durable: bool,
        meta: &serde_json::Value,
    ) -> io::Result<Box<dyn QueueStore>>;

    /// Lists the metadata of every durable queue that has data on disk.
    fn stored_queues(&self) -> io::Result<Vec<serde_json::Value>> {
        Ok(Vec::new())
    }

    /// Writes the broker definitions (users, vhosts, durable topology).
    fn save_definitions(&self, _json: &serde_json::Value) -> io::Result<()> {
        Ok(())
    }

    /// Reads the definitions written by [`Store::save_definitions`].
    fn load_definitions(&self) -> io::Result<Option<serde_json::Value>> {
        Ok(None)
    }

    /// True when data survives a restart.
    fn is_durable(&self) -> bool {
        false
    }
}

/// A store that keeps everything in memory. Used by `--ephemeral` and in tests.
#[derive(Debug, Default, Clone, Copy)]
pub struct MemoryStore;

/// A queue store that keeps bodies in the queue and confirms immediately.
#[derive(Debug, Default)]
pub struct MemoryQueueStore;

impl QueueStore for MemoryQueueStore {
    fn append(&mut self, _seq: u64, _msg: &Message, _persistent: bool) -> io::Result<Loc> {
        Ok(Loc::default())
    }

    fn read(&mut self, _loc: Loc) -> io::Result<Message> {
        Err(io::Error::other("memory store keeps bodies in the queue"))
    }

    fn remove(&mut self, _loc: Loc) -> io::Result<()> {
        Ok(())
    }

    fn commit(&mut self, tickets: Vec<Ticket>) {
        drop(tickets);
    }

    fn recover(&mut self) -> io::Result<Vec<Recovered>> {
        Ok(Vec::new())
    }

    fn destroy(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Store for MemoryStore {
    fn open_queue(
        &self,
        _vhost: &str,
        _queue: &str,
        _durable: bool,
        _meta: &serde_json::Value,
    ) -> io::Result<Box<dyn QueueStore>> {
        Ok(Box::new(MemoryQueueStore))
    }
}

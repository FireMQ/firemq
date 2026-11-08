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

/// A message found by [`QueueStore::recover`].
#[derive(Debug)]
pub struct Recovered {
    pub seq: u64,
    pub loc: Loc,
    pub message: Message,
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

    /// Takes the confirm tickets of persistent messages appended since the last call and
    /// drops them once those messages are durable.
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
    /// Opens (or creates) the store for a queue.
    fn open_queue(
        &self,
        vhost: &str,
        queue: &str,
        durable: bool,
    ) -> io::Result<Box<dyn QueueStore>>;

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

struct MemoryQueueStore;

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
    ) -> io::Result<Box<dyn QueueStore>> {
        Ok(Box::new(MemoryQueueStore))
    }
}

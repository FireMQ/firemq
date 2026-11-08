//! Disk storage for FireMQ.
//!
//! ```text
//! <data dir>/
//!   node.json                        format version
//!   definitions.json                 users, vhosts, permissions, policies, durable topology
//!   firemq.lock                      held while a broker uses the directory
//!   queues/<vhost-hash>/<queue-hash>/
//!     meta.json                      queue name, vhost and arguments
//!     0000000001.seg                 append-only records
//!     0000000001.ack                 offsets of acknowledged records
//! ```
//!
//! Only durable queues touch the disk; transient queues stay in memory. Every message of a
//! durable queue is written so large backlogs page out of memory, but only persistent ones
//! (`delivery_mode = 2`) are recovered after a restart. A segment is deleted as soon as all
//! its records are acknowledged.
//!
//! Persistent publishes are confirmed after an fsync. One thread syncs every dirty file
//! after a short window (2 ms by default), so a burst of publishes shares one fsync.

mod flusher;
mod queue;
mod record;

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use firemq_core::{MemoryQueueStore, QueueStore, Store};
use sha2::{Digest, Sha256};

pub use flusher::Flusher;

/// Version of the on-disk format written by this build.
pub const FORMAT_VERSION: u64 = 1;

/// Storage settings.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Target segment file size.
    pub segment_size: u64,
    /// Sync persistent messages to disk before confirming them. Turning this off keeps
    /// messages safe from a broker crash but not from a power loss.
    pub fsync: bool,
    /// How long the fsync thread waits to batch publishes.
    pub fsync_interval: Duration,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            segment_size: 32 * 1024 * 1024,
            fsync: true,
            fsync_interval: Duration::from_millis(2),
        }
    }
}

/// A directory-backed [`Store`].
pub struct DiskStore {
    dir: PathBuf,
    settings: queue::QueueSettings,
    _lock: File,
}

fn hash(s: &str) -> String {
    let d = Sha256::digest(s.as_bytes());
    d.iter().take(10).map(|b| format!("{b:02x}")).collect()
}

/// Writes a file atomically: temporary file, fsync, rename, directory fsync.
pub(crate) fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    #[cfg(unix)]
    if let Some(parent) = path.parent()
        && let Ok(d) = File::open(parent)
    {
        let _ = d.sync_all();
    }
    Ok(())
}

impl DiskStore {
    /// Opens or creates a data directory and locks it against other brokers.
    pub fn open(dir: impl Into<PathBuf>, config: StoreConfig) -> io::Result<DiskStore> {
        let dir = dir.into();
        fs::create_dir_all(dir.join("queues"))?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("firemq.lock"))?;
        if lock.try_lock().is_err() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "data directory {} is in use by another FireMQ process",
                    dir.display()
                ),
            ));
        }
        let node = dir.join("node.json");
        match fs::read(&node) {
            Ok(data) => {
                let v: serde_json::Value = serde_json::from_slice(&data)?;
                let format = v.get("format").and_then(|f| f.as_u64()).unwrap_or(0);
                if format > FORMAT_VERSION {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "data directory format {format} is newer than this FireMQ supports ({FORMAT_VERSION}); upgrade FireMQ"
                        ),
                    ));
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let v = serde_json::json!({"format": FORMAT_VERSION, "created_by": concat!("firemq ", env!("CARGO_PKG_VERSION"))});
                write_atomic(&node, &serde_json::to_vec_pretty(&v)?)?;
            }
            Err(e) => return Err(e),
        }
        Ok(DiskStore {
            settings: queue::QueueSettings {
                segment_size: config.segment_size.max(64 * 1024),
                fsync: config.fsync,
                flusher: Flusher::start(config.fsync_interval),
            },
            dir,
            _lock: lock,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn queue_dir(&self, vhost: &str, queue: &str) -> PathBuf {
        self.dir.join("queues").join(hash(vhost)).join(hash(queue))
    }
}

impl Store for DiskStore {
    fn open_queue(
        &self,
        vhost: &str,
        queue: &str,
        durable: bool,
        meta: &serde_json::Value,
    ) -> io::Result<Box<dyn QueueStore>> {
        if !durable {
            return Ok(Box::new(MemoryQueueStore));
        }
        Ok(Box::new(queue::DiskQueue::new(
            self.queue_dir(vhost, queue),
            meta.clone(),
            self.settings.clone(),
        )))
    }

    fn stored_queues(&self) -> io::Result<Vec<serde_json::Value>> {
        let mut out = Vec::new();
        let root = self.dir.join("queues");
        for vhost in fs::read_dir(&root)?.flatten() {
            let Ok(queues) = fs::read_dir(vhost.path()) else {
                continue;
            };
            for q in queues.flatten() {
                if let Ok(data) = fs::read(q.path().join("meta.json")) {
                    match serde_json::from_slice(&data) {
                        Ok(v) => out.push(v),
                        Err(e) => {
                            tracing::warn!(dir = %q.path().display(), error = %e, "ignoring unreadable queue metadata")
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    fn save_definitions(&self, json: &serde_json::Value) -> io::Result<()> {
        write_atomic(
            &self.dir.join("definitions.json"),
            &serde_json::to_vec_pretty(json)?,
        )
    }

    fn load_definitions(&self) -> io::Result<Option<serde_json::Value>> {
        match fs::read(self.dir.join("definitions.json")) {
            Ok(data) => Ok(Some(serde_json::from_slice(&data)?)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn is_durable(&self) -> bool {
        true
    }
}

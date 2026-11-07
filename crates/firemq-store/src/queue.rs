//! A durable queue's messages: append-only segment files plus ack files.

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use firemq_core::{Loc, Message, QueueStore, Recovered, Ticket};

use crate::flusher::Flusher;
use crate::record;

/// Write buffer size that forces a flush before the batch ends.
const MAX_BUFFER: usize = 1 << 20;

struct Segment {
    file: Arc<File>,
    /// Bytes in the file plus bytes still in the write buffer (active segment only).
    len: u64,
    records: u32,
    acked: u32,
    ack_buf: Vec<u8>,
}

/// Settings shared by every queue of a store.
#[derive(Clone)]
pub(crate) struct QueueSettings {
    pub segment_size: u64,
    pub fsync: bool,
    pub flusher: Flusher,
}

pub(crate) struct DiskQueue {
    dir: PathBuf,
    meta: serde_json::Value,
    settings: QueueSettings,
    segments: BTreeMap<u32, Segment>,
    active: u32,
    buf: Vec<u8>,
    dirty: Vec<Arc<File>>,
    created: bool,
}

fn seg_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("{id:010}.seg"))
}

fn ack_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("{id:010}.ack"))
}

#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn read_at(file: &File, buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut done = 0;
    while done < buf.len() {
        let n = file.seek_read(&mut buf[done..], offset)?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        done += n;
        offset += n as u64;
    }
    Ok(())
}

fn open_segment(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(path)
}

impl DiskQueue {
    pub(crate) fn new(dir: PathBuf, meta: serde_json::Value, settings: QueueSettings) -> DiskQueue {
        DiskQueue {
            dir,
            meta,
            settings,
            segments: BTreeMap::new(),
            active: 0,
            buf: Vec::new(),
            dirty: Vec::new(),
            created: false,
        }
    }

    fn ensure_dir(&mut self) -> io::Result<()> {
        if self.created {
            return Ok(());
        }
        fs::create_dir_all(&self.dir)?;
        let meta = serde_json::to_vec_pretty(&self.meta)?;
        crate::write_atomic(&self.dir.join("meta.json"), &meta)?;
        self.created = true;
        Ok(())
    }

    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let seg = self
            .segments
            .get(&self.active)
            .expect("active segment exists while buffering");
        (&*seg.file).write_all(&self.buf)?;
        self.buf.clear();
        Ok(())
    }

    fn flush_acks(&mut self) -> io::Result<()> {
        for (id, seg) in self.segments.iter_mut() {
            if seg.ack_buf.is_empty() {
                continue;
            }
            let mut f = OpenOptions::new()
                .append(true)
                .create(true)
                .open(ack_path(&self.dir, *id))?;
            f.write_all(&seg.ack_buf)?;
            seg.ack_buf.clear();
        }
        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.ensure_dir()?;
        self.flush_buffer()?;
        let previous = self.active;
        let id = self
            .segments
            .keys()
            .next_back()
            .copied()
            .unwrap_or(0)
            .max(self.active)
            + 1;
        let file = Arc::new(open_segment(&seg_path(&self.dir, id))?);
        self.segments.insert(
            id,
            Segment {
                file,
                len: 0,
                records: 0,
                acked: 0,
                ack_buf: Vec::new(),
            },
        );
        self.active = id;
        if previous != 0 {
            self.maybe_delete(previous)?;
        }
        Ok(())
    }

    /// Deletes a sealed segment once every record in it is acknowledged.
    fn maybe_delete(&mut self, id: u32) -> io::Result<()> {
        if id == self.active {
            return Ok(());
        }
        let done = self.segments.get(&id).is_some_and(|s| s.acked >= s.records);
        if done {
            self.segments.remove(&id);
            let _ = fs::remove_file(seg_path(&self.dir, id));
            let _ = fs::remove_file(ack_path(&self.dir, id));
        }
        Ok(())
    }
}

impl QueueStore for DiskQueue {
    fn append(&mut self, seq: u64, msg: &Message, persistent: bool) -> io::Result<Loc> {
        let props_len = msg.properties.to_bytes().len();
        let size = record::size(msg, props_len) as u64;
        let need_rotate = match self.segments.get(&self.active) {
            None => true,
            Some(s) => s.len > 0 && s.len + size > self.settings.segment_size,
        };
        if need_rotate {
            self.rotate()?;
        }
        let n = record::encode(&mut self.buf, seq, msg, persistent) as u64;
        let seg = self.segments.get_mut(&self.active).expect("active segment");
        let offset = seg.len;
        if offset + n > u32::MAX as u64 {
            return Err(io::Error::other("segment exceeds 4 GiB"));
        }
        seg.len += n;
        seg.records += 1;
        if persistent && self.settings.fsync {
            let f = seg.file.clone();
            if !self.dirty.iter().any(|d| Arc::ptr_eq(d, &f)) {
                self.dirty.push(f);
            }
        }
        let loc = Loc {
            segment: self.active,
            offset: offset as u32,
            len: n as u32,
        };
        if self.buf.len() >= MAX_BUFFER {
            self.flush_buffer()?;
        }
        Ok(loc)
    }

    fn read(&mut self, loc: Loc) -> io::Result<Message> {
        let seg = self
            .segments
            .get(&loc.segment)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "segment deleted"))?;
        let mut data = vec![0u8; loc.len as usize];
        let flushed = if loc.segment == self.active {
            seg.len - self.buf.len() as u64
        } else {
            seg.len
        };
        if loc.offset as u64 >= flushed {
            let start = (loc.offset as u64 - flushed) as usize;
            data.copy_from_slice(&self.buf[start..start + loc.len as usize]);
        } else {
            read_at(&seg.file, &mut data, loc.offset as u64)?;
        }
        let d = record::decode(&data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt record"))?;
        record::to_message(&d)
    }

    fn remove(&mut self, loc: Loc) -> io::Result<()> {
        let Some(seg) = self.segments.get_mut(&loc.segment) else {
            return Ok(());
        };
        seg.acked += 1;
        seg.ack_buf.extend_from_slice(&loc.offset.to_be_bytes());
        self.maybe_delete(loc.segment)
    }

    fn commit(&mut self, tickets: Vec<Ticket>) {
        if let Err(e) = self.flush_buffer().and_then(|_| self.flush_acks()) {
            tracing::error!(dir = %self.dir.display(), error = %e, "queue write failed");
            for t in &tickets {
                t.nack();
            }
            return;
        }
        let files = std::mem::take(&mut self.dirty);
        if tickets.is_empty() && files.is_empty() {
            return;
        }
        if self.settings.fsync && !files.is_empty() {
            self.settings.flusher.submit(files, tickets);
        } else {
            drop(tickets);
        }
    }

    fn recover(&mut self) -> io::Result<Vec<Recovered>> {
        let mut out = Vec::new();
        if !self.dir.exists() {
            return Ok(out);
        }
        self.created = true;
        let mut ids: Vec<u32> = fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                name.strip_suffix(".seg")?.parse().ok()
            })
            .collect();
        ids.sort_unstable();
        for id in ids {
            let path = seg_path(&self.dir, id);
            let acks = fs::read(ack_path(&self.dir, id)).unwrap_or_default();
            let mut acked: HashSet<u32> = acks
                .chunks_exact(4)
                .map(|c| u32::from_be_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            let data = fs::read(&path)?;
            let mut off = 0usize;
            let mut records = 0u32;
            let mut dead = Vec::new();
            while off < data.len() {
                let Some(d) = record::decode(&data[off..]) else {
                    break;
                };
                records += 1;
                let offset = off as u32;
                if acked.contains(&offset) {
                    // Already acknowledged.
                } else if d.flags & record::FLAG_PERSISTENT == 0 {
                    // Transient messages do not survive a restart.
                    dead.push(offset);
                    acked.insert(offset);
                } else {
                    let props = firemq_amqp_codec::BasicProperties::from_bytes(
                        bytes::Bytes::copy_from_slice(d.props),
                    )
                    .unwrap_or_default();
                    out.push(Recovered {
                        seq: d.seq,
                        loc: Loc {
                            segment: id,
                            offset,
                            len: d.len as u32,
                        },
                        size: d.body.len() as u64,
                        published_at: d.ts,
                        expiration_ms: props.expiration.as_deref().and_then(|e| e.parse().ok()),
                        priority: props.priority.unwrap_or(0),
                    });
                }
                off += d.len;
            }
            if off < data.len() {
                tracing::warn!(
                    segment = %path.display(),
                    valid = off,
                    total = data.len(),
                    "truncating a torn or corrupt segment tail"
                );
                OpenOptions::new()
                    .write(true)
                    .open(&path)?
                    .set_len(off as u64)?;
            }
            let acked_count = acked.iter().filter(|o| (**o as usize) < off).count() as u32;
            if records == 0 || acked_count >= records {
                let _ = fs::remove_file(&path);
                let _ = fs::remove_file(ack_path(&self.dir, id));
                continue;
            }
            if !dead.is_empty() {
                let mut f = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(ack_path(&self.dir, id))?;
                for o in &dead {
                    f.write_all(&o.to_be_bytes())?;
                }
            }
            self.segments.insert(
                id,
                Segment {
                    file: Arc::new(open_segment(&path)?),
                    len: off as u64,
                    records,
                    acked: acked_count,
                    ack_buf: Vec::new(),
                },
            );
            self.active = self.active.max(id);
        }
        out.sort_by_key(|r| r.seq);
        // New writes go to a fresh segment, never after a recovered tail.
        if self.active != 0 {
            self.rotate()?;
        }
        Ok(out)
    }

    fn destroy(&mut self) -> io::Result<()> {
        self.segments.clear();
        self.buf.clear();
        self.dirty.clear();
        match fs::remove_dir_all(&self.dir) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    fn bodies_in_memory(&self) -> bool {
        false
    }
}

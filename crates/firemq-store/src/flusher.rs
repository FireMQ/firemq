//! Group commit: one thread syncs dirty files for every queue and then releases the
//! confirm tickets waiting on them.

use std::fs::File;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

use firemq_core::Ticket;

struct Job {
    files: Vec<Arc<File>>,
    tickets: Vec<Ticket>,
}

/// Handle to the fsync thread. Cheap to clone.
#[derive(Clone)]
pub struct Flusher {
    tx: Sender<Job>,
}

impl Flusher {
    /// Starts the thread. After the first pending job it waits `interval` to collect more,
    /// so one fsync per file covers every publish in that window.
    pub fn start(interval: Duration) -> Flusher {
        let (tx, rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("firemq-fsync".into())
            .spawn(move || run(rx, interval))
            .expect("spawn fsync thread");
        Flusher { tx }
    }

    /// Syncs `files`, then drops `tickets`. Tickets are nacked if a sync fails.
    pub fn submit(&self, files: Vec<Arc<File>>, tickets: Vec<Ticket>) {
        if let Err(mpsc::SendError(job)) = self.tx.send(Job { files, tickets }) {
            for t in &job.tickets {
                t.nack();
            }
        }
    }
}

fn run(rx: Receiver<Job>, interval: Duration) {
    while let Ok(first) = rx.recv() {
        if !interval.is_zero() {
            std::thread::sleep(interval);
        }
        let mut jobs = vec![first];
        while let Ok(j) = rx.try_recv() {
            jobs.push(j);
        }
        let mut synced: Vec<(*const File, bool)> = Vec::new();
        for job in &jobs {
            let mut ok = true;
            for f in &job.files {
                let ptr = Arc::as_ptr(f);
                let result = match synced.iter().find(|(p, _)| *p == ptr) {
                    Some((_, r)) => *r,
                    None => {
                        let r = match f.sync_data() {
                            Ok(()) => true,
                            Err(e) => {
                                tracing::error!(error = %e, "fsync failed; rejecting publishes");
                                false
                            }
                        };
                        synced.push((ptr, r));
                        r
                    }
                };
                ok &= result;
            }
            if !ok {
                for t in &job.tickets {
                    t.nack();
                }
            }
        }
        // Dropping the jobs releases their tickets, which sends the confirms.
        drop(jobs);
    }
}

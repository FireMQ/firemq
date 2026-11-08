//! Message rates for the management API, sampled every few seconds.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use firemq_core::Broker;
use parking_lot::Mutex;

#[derive(Debug, Clone, Copy, Default)]
struct Sample {
    values: [u64; 4],
    at: Option<Instant>,
}

/// Rates per object key, in events per second.
#[derive(Default)]
pub struct Rates {
    samples: Mutex<HashMap<String, Sample>>,
    rates: Mutex<HashMap<String, [f64; 4]>>,
}

impl Rates {
    pub fn get(&self, key: &str) -> [f64; 4] {
        self.rates.lock().get(key).copied().unwrap_or_default()
    }

    fn record(&self, key: String, values: [u64; 4], now: Instant) {
        let mut samples = self.samples.lock();
        let prev = samples.get(&key).copied().unwrap_or_default();
        if let Some(at) = prev.at {
            let secs = now.duration_since(at).as_secs_f64().max(0.001);
            let mut r = [0.0; 4];
            for i in 0..4 {
                r[i] = values[i].saturating_sub(prev.values[i]) as f64 / secs;
            }
            self.rates.lock().insert(key.clone(), r);
        }
        samples.insert(
            key,
            Sample {
                values,
                at: Some(now),
            },
        );
    }

    /// Samples counters every `every`.
    pub fn spawn(self: &Arc<Self>, broker: Broker, every: Duration) -> tokio::task::JoinHandle<()> {
        let rates = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            loop {
                tick.tick().await;
                let now = Instant::now();
                let c = broker.counters();
                rates.record(
                    "global".into(),
                    [
                        c.published.load(Ordering::Relaxed),
                        c.delivered.load(Ordering::Relaxed),
                        c.acked.load(Ordering::Relaxed),
                        c.confirmed.load(Ordering::Relaxed),
                    ],
                    now,
                );
                for v in broker.vhosts() {
                    for q in v.topology().queues.values() {
                        let s = &q.stats;
                        rates.record(
                            format!("q:{}:{}", v.name, q.name),
                            [
                                s.published.load(Ordering::Relaxed),
                                s.delivered.load(Ordering::Relaxed),
                                s.acked.load(Ordering::Relaxed),
                                s.redelivered.load(Ordering::Relaxed),
                            ],
                            now,
                        );
                    }
                }
                for conn in broker.connections() {
                    rates.record(
                        format!("c:{}", conn.id),
                        [
                            conn.recv_bytes.load(Ordering::Relaxed),
                            conn.send_bytes.load(Ordering::Relaxed),
                            conn.published.load(Ordering::Relaxed),
                            conn.delivered.load(Ordering::Relaxed),
                        ],
                        now,
                    );
                }
            }
        })
    }
}

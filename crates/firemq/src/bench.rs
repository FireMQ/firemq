//! `firemq bench`: measure publish and consume throughput and end-to-end latency.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use clap::Args;
use firemq_client::{Client, PublishOptions, QueueOptions, SubscribeOptions};
use futures::StreamExt;
use futures::stream::FuturesUnordered;

#[derive(Args, Debug, Clone)]
pub struct BenchArgs {
    #[arg(long, env = "FIREMQ_URL", default_value = "fire://localhost")]
    pub url: String,
    /// Start an in-memory broker inside this process instead of connecting to one.
    #[arg(long)]
    pub embedded: bool,
    #[arg(long, short, default_value_t = 1)]
    pub publishers: usize,
    #[arg(long, short, default_value_t = 1)]
    pub consumers: usize,
    /// Message size in bytes.
    #[arg(long, short, default_value_t = 256)]
    pub size: usize,
    /// Seconds to run.
    #[arg(long, short, default_value_t = 10)]
    pub duration: u64,
    /// Write messages to disk (persistent messages). The queue is always durable.
    #[arg(long)]
    pub persistent: bool,
    /// Unconfirmed publishes in flight per publisher.
    #[arg(long, default_value_t = 1000)]
    pub window: usize,
    /// Consumer prefetch.
    #[arg(long, default_value_t = 1000)]
    pub prefetch: u32,
    /// Messages per second per publisher, to measure latency at a fixed load. 0 publishes as
    /// fast as confirms allow.
    #[arg(long, default_value_t = 0)]
    pub rate: u64,
    #[arg(long, default_value = "firemq-bench")]
    pub queue: String,
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i] as f64 / 1000.0
}

pub async fn run(a: BenchArgs) -> Result<(), String> {
    let _embedded;
    let url = if a.embedded {
        let server = crate::builder()
            .random_ports()
            .ephemeral()
            .start()
            .await
            .map_err(|e| e.to_string())?;
        let url = server.fire_url().expect("fire listener");
        _embedded = Some(server);
        url
    } else {
        _embedded = None;
        a.url.clone()
    };
    let admin = Client::connect(&url)
        .await
        .map_err(|e| format!("cannot connect to {url}: {e}"))?;
    let _ = admin.delete_queue(&a.queue).await;
    admin
        .declare_queue(
            &a.queue,
            QueueOptions {
                durable: true,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| e.to_string())?;
    println!(
        "FireMQ bench: {} publisher(s), {} consumer(s), {} B messages, {}, {}s{}{}",
        a.publishers,
        a.consumers,
        a.size,
        if a.persistent {
            "persistent"
        } else {
            "transient"
        },
        a.duration,
        if a.rate > 0 {
            format!(", {} msg/s per publisher", a.rate)
        } else {
            String::new()
        },
        if a.embedded { ", embedded broker" } else { "" }
    );
    let published = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let latencies = Arc::new(parking::Lat::default());
    let mut tasks = Vec::new();
    for _ in 0..a.consumers {
        let c = Client::connect(&url).await.map_err(|e| e.to_string())?;
        let mut sub = c
            .subscribe(
                &a.queue,
                SubscribeOptions {
                    prefetch: a.prefetch,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| e.to_string())?;
        let consumed = consumed.clone();
        let stop = stop.clone();
        let latencies = latencies.clone();
        tasks.push(tokio::spawn(async move {
            let _keep = c;
            while !stop.load(Ordering::Relaxed) {
                let Ok(Some(m)) =
                    tokio::time::timeout(Duration::from_millis(200), sub.next()).await
                else {
                    continue;
                };
                if m.body.len() >= 8 {
                    let sent = u64::from_be_bytes(m.body[..8].try_into().unwrap_or_default());
                    latencies.push(now_micros().saturating_sub(sent));
                }
                let _ = m.ack();
                consumed.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for _ in 0..a.publishers {
        let c = Client::connect(&url).await.map_err(|e| e.to_string())?;
        let published = published.clone();
        let stop = stop.clone();
        let (queue, size, window, persistent, rate) = (
            a.queue.clone(),
            a.size.max(8),
            a.window,
            a.persistent,
            a.rate,
        );
        tasks.push(tokio::spawn(async move {
            let filler = vec![b'x'; size - 8];
            let mut in_flight = FuturesUnordered::new();
            let started = Instant::now();
            let mut sent = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if rate > 0 {
                    let due = (started.elapsed().as_secs_f64() * rate as f64) as u64;
                    if sent >= due {
                        // Wait for the next send slot, collecting confirms meanwhile.
                        let slot = Duration::from_secs_f64((sent + 1) as f64 / rate as f64)
                            .saturating_sub(started.elapsed());
                        if in_flight.is_empty() {
                            tokio::time::sleep(slot).await;
                        } else if let Ok(Some(Ok(()))) =
                            tokio::time::timeout(slot, in_flight.next()).await
                        {
                            published.fetch_add(1, Ordering::Relaxed);
                        }
                        continue;
                    }
                }
                sent += 1;
                let mut body = BytesMut::with_capacity(size);
                body.put_u64(now_micros());
                body.put_slice(&filler);
                let c2 = c.clone();
                let q = queue.clone();
                let body: Bytes = body.freeze();
                in_flight.push(async move {
                    c2.publish_with(
                        "",
                        &q,
                        body,
                        PublishOptions {
                            persistent,
                            ..Default::default()
                        },
                    )
                    .await
                });
                while in_flight.len() >= window {
                    if let Some(Ok(())) = in_flight.next().await {
                        published.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            while let Some(r) = in_flight.next().await {
                if r.is_ok() {
                    published.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    let started = Instant::now();
    let (mut last_p, mut last_c) = (0, 0);
    for sec in 1..=a.duration {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let p = published.load(Ordering::Relaxed);
        let c = consumed.load(Ordering::Relaxed);
        println!(
            "  {sec:>3}s  published {:>9} msg/s  consumed {:>9} msg/s",
            p - last_p,
            c - last_c
        );
        last_p = p;
        last_c = c;
    }
    stop.store(true, Ordering::Relaxed);
    let elapsed = started.elapsed().as_secs_f64();
    for t in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(5), t).await;
    }
    let p = published.load(Ordering::Relaxed);
    let c = consumed.load(Ordering::Relaxed);
    let mut lat = latencies.take();
    lat.sort_unstable();
    println!(
        "\n  published {:.0} msg/s ({:.1} MB/s), consumed {:.0} msg/s",
        p as f64 / elapsed,
        p as f64 * a.size as f64 / elapsed / 1e6,
        c as f64 / elapsed
    );
    println!(
        "  end-to-end latency  p50 {:.2} ms  p95 {:.2} ms  p99 {:.2} ms  max {:.2} ms",
        percentile(&lat, 0.50),
        percentile(&lat, 0.95),
        percentile(&lat, 0.99),
        lat.last().copied().unwrap_or(0) as f64 / 1000.0
    );
    let _ = admin.delete_queue(&a.queue).await;
    admin.close().await.ok();
    Ok(())
}

mod parking {
    /// Latency samples, thinned to bound memory on long runs.
    #[derive(Default)]
    pub struct Lat {
        inner: std::sync::Mutex<(Vec<u64>, u64)>,
    }

    impl Lat {
        pub fn push(&self, v: u64) {
            let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            g.1 += 1;
            if g.0.len() < 1_000_000 || g.1.is_multiple_of(16) {
                g.0.push(v);
            }
        }

        pub fn take(&self) -> Vec<u64> {
            std::mem::take(&mut self.inner.lock().unwrap_or_else(|e| e.into_inner()).0)
        }
    }
}

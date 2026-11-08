//! An AMQP 0-9-1 load generator, used to compare FireMQ with RabbitMQ using the same client.
//! It mirrors `firemq bench`: publishers keep a window of unconfirmed messages, consumers ack
//! each message, and every body starts with its send time for end-to-end latency.
//!
//! ```text
//! cargo run --release -p firemq-amqp --example amqp_bench -- --url amqp://localhost --duration 30
//! ```
//!
//! `--count` prints how many messages a queue holds instead, read with a passive
//! `queue.declare`, which is exact on both brokers. With `--wait-for N` it waits, reconnecting
//! as needed, until the queue holds N messages: the recovery benchmark uses it after a restart.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use amqprs::callbacks::ChannelCallback;
use amqprs::channel::{
    BasicAckArguments, BasicConsumeArguments, BasicPublishArguments, BasicQosArguments, Channel,
    ConfirmSelectArguments, QueueDeclareArguments, QueueDeleteArguments,
};
use amqprs::connection::{Connection, OpenConnectionArguments};
use amqprs::{Ack, BasicProperties, Cancel, CloseChannel, Nack, Return};
use clap::Parser;
use tokio::sync::{Semaphore, mpsc};

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(long, default_value = "amqp://guest:guest@localhost:5672/%2F")]
    url: String,
    #[arg(long, short, default_value_t = 1)]
    publishers: usize,
    #[arg(long, short, default_value_t = 1)]
    consumers: usize,
    /// Message size in bytes.
    #[arg(long, short, default_value_t = 256)]
    size: usize,
    /// Seconds to run.
    #[arg(long, short, default_value_t = 10)]
    duration: u64,
    /// Publish this many messages in total, then stop, even before --duration ends.
    #[arg(long)]
    messages: Option<u64>,
    /// Persistent messages. The queue is always durable, as RabbitMQ 4 requires.
    #[arg(long)]
    persistent: bool,
    /// Unconfirmed publishes in flight per publisher.
    #[arg(long, default_value_t = 1000)]
    window: usize,
    /// Consumer prefetch.
    #[arg(long, default_value_t = 1000)]
    prefetch: u16,
    /// Messages per second per publisher; 0 publishes as fast as confirms allow.
    #[arg(long, default_value_t = 0)]
    rate: u64,
    #[arg(long, default_value = "amqp-bench")]
    queue: String,
    /// Leave the queue and its messages in place at the end, for recovery tests.
    #[arg(long)]
    keep: bool,
    /// Use the queue and the messages already in it instead of recreating it, to measure
    /// how fast consumers drain a backlog.
    #[arg(long)]
    existing: bool,
    /// Print the number of messages in the queue and exit.
    #[arg(long)]
    count: bool,
    /// With --count, wait until the queue holds exactly this many messages.
    #[arg(long, requires = "count")]
    wait_for: Option<u64>,
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

async fn connect(url: &str) -> Connection {
    Connection::open(&open_args(url)).await.expect("connect")
}

fn open_args(url: &str) -> OpenConnectionArguments {
    let u = url::Url::parse(url).expect("valid amqp:// URL");
    let vhost = match u.path().trim_start_matches('/') {
        "" => "/".to_string(),
        v => url::form_urlencoded::parse(v.as_bytes())
            .next()
            .map(|(k, _)| k.into_owned())
            .unwrap_or_else(|| "/".into()),
    };
    let user = if u.username().is_empty() {
        "guest"
    } else {
        u.username()
    };
    let mut args = OpenConnectionArguments::new(
        u.host_str().unwrap_or("localhost"),
        u.port().unwrap_or(5672),
        user,
        u.password().unwrap_or("guest"),
    );
    args.virtual_host(&vhost);
    args
}

/// The queue's message count, or `None` while the broker or the queue is unavailable.
async fn try_count(url: &str, queue: &str) -> Option<u64> {
    let conn = Connection::open(&open_args(url)).await.ok()?;
    let ch = conn.open_channel(None).await.ok()?;
    let declared = ch
        .queue_declare(QueueDeclareArguments::new(queue).passive(true).finish())
        .await;
    let _ = conn.close().await;
    declared.ok().flatten().map(|(_, n, _)| n as u64)
}

async fn count(a: &Args) {
    loop {
        if let Some(n) = try_count(&a.url, &a.queue).await
            && a.wait_for.is_none_or(|want| want == n)
        {
            println!("{n}");
            return;
        }
        if a.wait_for.is_none() {
            eprintln!("cannot read queue '{}'", a.queue);
            std::process::exit(1);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Turns broker acks into a count of newly confirmed messages.
struct Confirms {
    tx: mpsc::UnboundedSender<(u64, bool)>,
}

#[async_trait::async_trait]
impl ChannelCallback for Confirms {
    async fn close(
        &mut self,
        _: &Channel,
        close: CloseChannel,
    ) -> Result<(), amqprs::error::Error> {
        eprintln!("channel closed: {close}");
        Ok(())
    }
    async fn cancel(&mut self, _: &Channel, _: Cancel) -> Result<(), amqprs::error::Error> {
        Ok(())
    }
    async fn flow(&mut self, _: &Channel, _: bool) -> Result<bool, amqprs::error::Error> {
        Ok(true)
    }
    async fn publish_ack(&mut self, _: &Channel, ack: Ack) {
        let _ = self.tx.send((ack.delivery_tag(), ack.mutiple()));
    }
    async fn publish_nack(&mut self, _: &Channel, nack: Nack) {
        let _ = self.tx.send((nack.delivery_tag(), nack.multiple()));
    }
    async fn publish_return(&mut self, _: &Channel, _: Return, _: BasicProperties, _: Vec<u8>) {}
}

/// Counts confirms, which can be cumulative (`multiple`) or single and out of order.
#[derive(Default)]
struct Watermark {
    upto: u64,
    singles: HashSet<u64>,
}

impl Watermark {
    fn confirm(&mut self, tag: u64, multiple: bool) -> u64 {
        if tag <= self.upto {
            return 0;
        }
        if multiple {
            let before = self.singles.len();
            self.singles.retain(|&s| s > tag);
            let n = tag - self.upto - (before - self.singles.len()) as u64;
            self.upto = tag;
            n
        } else {
            self.singles.insert(tag);
            while self.singles.remove(&(self.upto + 1)) {
                self.upto += 1;
            }
            1
        }
    }
}

#[tokio::main]
async fn main() {
    let a = Args::parse();
    if a.count {
        return count(&a).await;
    }
    let admin = connect(&a.url).await;
    let ch = admin.open_channel(None).await.expect("channel");
    if !a.existing {
        let _ = ch.queue_delete(QueueDeleteArguments::new(&a.queue)).await;
    }
    let mut declare = QueueDeclareArguments::new(&a.queue);
    declare.durable(true);
    ch.queue_declare(declare).await.expect("declare queue");
    println!(
        "AMQP bench: {} publisher(s), {} consumer(s), {} B messages, {}, {}s{}",
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
        }
    );

    let published = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let sent_total = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let mut keep = Vec::new();

    for _ in 0..a.consumers {
        let conn = connect(&a.url).await;
        let ch = conn.open_channel(None).await.expect("channel");
        ch.basic_qos(BasicQosArguments::new(0, a.prefetch, false))
            .await
            .expect("qos");
        let (_, mut rx) = ch
            .basic_consume_rx(BasicConsumeArguments::new(&a.queue, ""))
            .await
            .expect("consume");
        let (consumed, latencies) = (consumed.clone(), latencies.clone());
        tokio::spawn(async move {
            let mut lat = Vec::new();
            while let Some(m) = rx.recv().await {
                let (Some(d), Some(body)) = (m.deliver, m.content) else {
                    continue;
                };
                if body.len() >= 8 {
                    let sent = u64::from_be_bytes(body[..8].try_into().unwrap_or_default());
                    lat.push(now_micros().saturating_sub(sent));
                    if lat.len() >= 1024 {
                        latencies.lock().unwrap().append(&mut lat);
                    }
                }
                let _ = ch
                    .basic_ack(BasicAckArguments::new(d.delivery_tag(), false))
                    .await;
                consumed.fetch_add(1, Ordering::Relaxed);
            }
            latencies.lock().unwrap().append(&mut lat);
        });
        keep.push(conn);
    }

    for _ in 0..a.publishers {
        let conn = connect(&a.url).await;
        let ch = conn.open_channel(None).await.expect("channel");
        let (tx, mut acks) = mpsc::unbounded_channel();
        ch.register_callback(Confirms { tx })
            .await
            .expect("callback");
        ch.confirm_select(ConfirmSelectArguments::default())
            .await
            .expect("confirm.select");
        let window = Arc::new(Semaphore::new(a.window));
        {
            let (window, published) = (window.clone(), published.clone());
            tokio::spawn(async move {
                let mut w = Watermark::default();
                while let Some((tag, multiple)) = acks.recv().await {
                    let n = w.confirm(tag, multiple);
                    published.fetch_add(n, Ordering::Relaxed);
                    window.add_permits(n as usize);
                }
            });
        }
        let (stop, sent_total, a) = (stop.clone(), sent_total.clone(), a.clone());
        tokio::spawn(async move {
            let filler = vec![b'x'; a.size.max(8) - 8];
            let mut props = BasicProperties::default();
            props.with_delivery_mode(if a.persistent { 2 } else { 1 });
            let args = BasicPublishArguments::new("", &a.queue);
            let started = Instant::now();
            let mut sent = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if a.rate > 0 {
                    let due = (started.elapsed().as_secs_f64() * a.rate as f64) as u64;
                    if sent >= due {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        continue;
                    }
                }
                let Ok(permit) = window.acquire().await else {
                    break;
                };
                if a.messages
                    .is_some_and(|max| sent_total.fetch_add(1, Ordering::Relaxed) >= max)
                {
                    break;
                }
                permit.forget();
                let mut body = Vec::with_capacity(a.size.max(8));
                body.extend_from_slice(&now_micros().to_be_bytes());
                body.extend_from_slice(&filler);
                if ch
                    .basic_publish(props.clone(), body, args.clone())
                    .await
                    .is_err()
                {
                    break;
                }
                sent += 1;
            }
        });
        keep.push(conn);
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
        (last_p, last_c) = (p, c);
        if a.messages.is_some_and(|max| p >= max) {
            break;
        }
    }
    stop.store(true, Ordering::Relaxed);
    let elapsed = started.elapsed().as_secs_f64();
    let p = published.load(Ordering::Relaxed);
    let c = consumed.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut lat = std::mem::take(&mut *latencies.lock().unwrap());
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
    for conn in keep {
        let _ = conn.close().await;
    }
    if !a.keep {
        let _ = ch.queue_delete(QueueDeleteArguments::new(&a.queue)).await;
    }
    let _ = admin.close().await;
}

#[cfg(test)]
mod tests {
    use super::Watermark;

    #[test]
    fn counts_cumulative_and_single_confirms_once() {
        let mut w = Watermark::default();
        assert_eq!(w.confirm(3, false), 1);
        assert_eq!(w.confirm(1, false), 1);
        assert_eq!(w.upto, 1);
        assert_eq!(w.confirm(5, true), 3); // 2, 4, 5
        assert_eq!(w.confirm(5, true), 0);
        assert_eq!(w.confirm(6, false), 1);
        assert_eq!(w.upto, 6);
    }
}

//! `firemq pub` and `firemq sub`: send and receive messages from the terminal.

use std::io::{IsTerminal, Read};

use bytes::Bytes;
use clap::Args;
use firemq_client::{Client, PublishOptions, SubscribeOptions};

#[derive(Args, Debug)]
pub struct PubArgs {
    /// EXCHANGE ROUTING_KEY [BODY], or just [BODY] with --queue. Use "" for the default
    /// exchange, which routes by queue name. The body is read from --file or stdin when
    /// omitted.
    #[arg(value_name = "ARGS", num_args = 0..=3)]
    pub args: Vec<String>,
    /// Publish straight to this queue (shorthand for exchange "" and key QUEUE).
    #[arg(long, short)]
    pub queue: Option<String>,
    #[arg(long, env = "FIREMQ_URL", default_value = "fire://localhost")]
    pub url: String,
    /// Read the body from a file.
    #[arg(long, short)]
    pub file: Option<String>,
    /// Publish the message this many times.
    #[arg(long, short, default_value_t = 1)]
    pub count: u64,
    /// Header such as trace=abc (repeatable).
    #[arg(long = "header", short = 'H', value_name = "KEY=VALUE")]
    pub headers: Vec<String>,
    #[arg(long)]
    pub content_type: Option<String>,
    /// Do not write the message to disk.
    #[arg(long)]
    pub transient: bool,
    /// Fail when no queue receives the message. Always on with --queue.
    #[arg(long)]
    pub mandatory: bool,
}

#[derive(Args, Debug)]
pub struct SubArgs {
    /// Queue to consume from.
    pub queue: String,
    #[arg(long, env = "FIREMQ_URL", default_value = "fire://localhost")]
    pub url: String,
    /// Stop after this many messages.
    #[arg(long, short)]
    pub count: Option<u64>,
    /// Acknowledge on delivery (messages are removed even if this process dies).
    #[arg(long)]
    pub no_ack: bool,
    /// Requeue messages instead of acknowledging them, to inspect without consuming.
    #[arg(long, conflicts_with = "no_ack")]
    pub peek: bool,
    #[arg(long, default_value_t = 100)]
    pub prefetch: u32,
    /// Print each message as a JSON line with properties and headers.
    #[arg(long)]
    pub json: bool,
}

async fn connect(url: &str) -> Result<Client, String> {
    Client::connect(url).await.map_err(|e| {
        format!("cannot connect to {url}: {e}. Is FireMQ running? Start it with `firemq`.")
    })
}

pub async fn publish(a: PubArgs) -> Result<(), String> {
    let (exchange, key, body_arg) = match (&a.queue, a.args.as_slice()) {
        (Some(q), []) => (String::new(), q.clone(), None),
        (Some(q), [b]) => (String::new(), q.clone(), Some(b.clone())),
        (Some(_), _) => return Err("with --queue, give at most one argument: the body".into()),
        (None, [e, k]) => (e.clone(), k.clone(), None),
        (None, [e, k, b]) => (e.clone(), k.clone(), Some(b.clone())),
        (None, _) => {
            return Err("give EXCHANGE ROUTING_KEY [BODY], or --queue NAME [BODY]".into());
        }
    };
    let body: Bytes = match (body_arg, &a.file) {
        (Some(b), _) => Bytes::from(b),
        (None, Some(f)) => {
            Bytes::from(std::fs::read(f).map_err(|e| format!("cannot read {f}: {e}"))?)
        }
        (None, None) => {
            if std::io::stdin().is_terminal() {
                return Err("give a message body, --file, or pipe the body to stdin".into());
            }
            let mut buf = Vec::new();
            std::io::stdin()
                .read_to_end(&mut buf)
                .map_err(|e| e.to_string())?;
            Bytes::from(buf)
        }
    };
    let client = connect(&a.url).await?;
    let mut opts = PublishOptions {
        persistent: !a.transient,
        mandatory: a.mandatory || a.queue.is_some(),
        ..Default::default()
    };
    opts.properties.content_type = a.content_type.clone();
    for kv in &a.headers {
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| format!("expected KEY=VALUE, got '{kv}'"))?;
        opts.headers.insert(k.to_string(), v.into());
    }
    let to_queue = a.queue.is_some();
    let explain = move |e: firemq_client::Error| match e {
        firemq_client::Error::Unroutable { key, .. } if to_queue => {
            format!("no queue named '{key}'; declare it first with: firemq ctl declare-queue {key}")
        }
        e => e.to_string(),
    };
    let started = std::time::Instant::now();
    let mut pending = futures::stream::FuturesUnordered::new();
    use futures::StreamExt;
    for _ in 0..a.count {
        let c = client.clone();
        let (e, k, b, o) = (exchange.clone(), key.clone(), body.clone(), opts.clone());
        pending.push(async move { c.publish_with(&e, &k, b, o).await });
        if pending.len() >= 512 {
            pending.next().await.transpose().map_err(explain)?;
        }
    }
    while let Some(r) = pending.next().await {
        r.map_err(explain)?;
    }
    let target = if exchange.is_empty() {
        format!("queue '{key}'")
    } else {
        format!("exchange '{exchange}' with key '{key}'")
    };
    if a.count == 1 {
        eprintln!("published {} bytes to {target} (confirmed)", body.len());
    } else {
        let secs = started.elapsed().as_secs_f64();
        eprintln!(
            "published {} messages to {target} in {secs:.2}s ({:.0} msg/s, all confirmed)",
            a.count,
            a.count as f64 / secs
        );
    }
    client.close().await.ok();
    Ok(())
}

pub async fn subscribe(a: SubArgs) -> Result<(), String> {
    let client = connect(&a.url).await?;
    let mut sub = client
        .subscribe(
            &a.queue,
            SubscribeOptions {
                prefetch: a.prefetch,
                no_ack: a.no_ack,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| e.to_string())?;
    if std::io::stderr().is_terminal() {
        eprintln!("consuming from '{}' (Ctrl-C to stop)", a.queue);
    }
    let mut n = 0u64;
    loop {
        let msg = tokio::select! {
            m = sub.next() => m,
            _ = tokio::signal::ctrl_c() => None,
        };
        let Some(msg) = msg else { break };
        if a.json {
            let body = match msg.body_str() {
                Some(s) => serde_json::Value::String(s.to_string()),
                None => serde_json::Value::String(base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    &msg.body,
                )),
            };
            let line = serde_json::json!({
                "exchange": msg.exchange,
                "routing_key": msg.routing_key,
                "redelivered": msg.redelivered,
                "properties": msg.properties,
                "headers": msg.headers,
                "body": body,
            });
            println!("{line}");
        } else {
            match msg.body_str() {
                Some(s) => println!("{s}"),
                None => println!("<{} bytes of binary data>", msg.body.len()),
            }
        }
        if a.peek {
            msg.nack(true).map_err(|e| e.to_string())?;
        } else {
            msg.ack().map_err(|e| e.to_string())?;
        }
        n += 1;
        if a.count.is_some_and(|c| n >= c) {
            break;
        }
    }
    client.close().await.ok();
    Ok(())
}

# firemq-client

The Rust client for [FireMQ](https://github.com/FireMQ/firemq), a lean, RabbitMQ-compatible
message broker. It speaks the FireMQ protocol over TCP, TLS or WebSocket.

```toml
[dependencies]
firemq-client = "0.1"
tokio = { version = "1", features = ["full"] }
```

```rust,no_run
use firemq_client::{Client, SubscribeOptions};

#[tokio::main]
async fn main() -> Result<(), firemq_client::Error> {
    let client = Client::connect("fire://localhost").await?;
    client.declare_queue("jobs", Default::default()).await?;
    client.publish("", "jobs", "hello").await?; // returns when the broker confirms

    let mut jobs = client.subscribe("jobs", SubscribeOptions::default()).await?;
    while let Some(msg) = jobs.next().await {
        println!("{}", msg.body_str().unwrap_or_default());
        msg.ack()?;
    }
    Ok(())
}
```

The defaults favour safety: queues and exchanges are durable, messages are persistent, and
`publish` waits for the broker's confirm. The client reconnects on its own and restores the
queues, exchanges, bindings and subscriptions it declared.

See the [client documentation](https://github.com/FireMQ/firemq/blob/main/docs/src/clients.md)
for the full guide.

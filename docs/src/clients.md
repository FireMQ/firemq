# Clients

You can connect to FireMQ in two ways:

- **With a RabbitMQ client over AMQP 0-9-1.** Use this to move existing applications, or when your language already has a good AMQP client.
- **With a FireMQ SDK over the FireMQ protocol.** Use this for new code. The SDKs share one API across languages, confirm every publish by default, and also run in browsers over WebSocket.

Both kinds of client share the same broker: a message published over one protocol can be consumed over the other, with its properties and headers.

## RabbitMQ clients

Every AMQP 0-9-1 client works. CI tests these against every change:

| Language | Library |
|---|---|
| Python | pika |
| JavaScript | amqplib |
| Go | amqp091-go (plus the library's own integration test suite) |
| Java | RabbitMQ Java client |
| .NET | RabbitMQ.Client |

Connect with the usual URL, for example `amqp://guest:guest@localhost:5672/`. See [RabbitMQ client compatibility](compatibility.md) for the details.

## FireMQ SDKs

| Language | Repository | Package |
|---|---|---|
| Rust | [`crates/firemq-client`](https://github.com/FireMQ/firemq/tree/main/crates/firemq-client) | `firemq-client` on crates.io |
| Go | [FireMQ/firemq-go](https://github.com/FireMQ/firemq-go) | `github.com/FireMQ/firemq-go` |
| TypeScript: Node.js, browsers | [FireMQ/firemq-js](https://github.com/FireMQ/firemq-js) | `@firemq/client` on npm |
| Java, Kotlin | [FireMQ/firemq-java](https://github.com/FireMQ/firemq-java) | `io.github.firemq:firemq-client` and `firemq-kotlin` on Maven Central |
| .NET | [FireMQ/firemq-dotnet](https://github.com/FireMQ/firemq-dotnet) | `FireMQ.Client` on NuGet |
| Elixir | [FireMQ/firemq-elixir](https://github.com/FireMQ/firemq-elixir) | `firemq` on Hex |
| PHP | [FireMQ/firemq-php](https://github.com/FireMQ/firemq-php) | `firemq/firemq` on Packagist |
| Python, Kotlin, Swift, Ruby, Dart, C | [`bindings/`](https://github.com/FireMQ/firemq/tree/main/bindings), over the Rust SDK | Build from source for now |

Every SDK runs the [conformance suite](conformance.md) against a real broker in its CI. Packages are published to their registries starting with FireMQ v0.1.0; until then, install from the repositories.

Track progress in the [roadmap issue](https://github.com/FireMQ/firemq/issues/23).

### Connection URLs

```text
fire://user:password@host:5673/vhost      FireMQ protocol over TCP
fires://user:password@host:5674/vhost     over TLS
ws://host:15672/fire?vhost=dev            over WebSocket (browsers)
wss://host/fire                           over secure WebSocket, through a TLS proxy
```

Missing parts default to user and password `guest`, vhost `/` and the standard ports, so `fire://localhost` is enough for local development.

### Rust

```toml
[dependencies]
firemq-client = "0.1"
tokio = { version = "1", features = ["full"] }
```

```rust
use firemq_client::{Client, SubscribeOptions};

# async fn run() -> Result<(), firemq_client::Error> {
let client = Client::connect("fire://localhost").await?;
client.declare_queue("jobs", Default::default()).await?;
client.publish("", "jobs", "hello").await?; // returns when the broker confirms

let mut jobs = client.subscribe("jobs", SubscribeOptions::default()).await?;
while let Some(msg) = jobs.next().await {
    println!("{}", msg.body_str().unwrap_or_default());
    msg.ack()?;
}
# Ok(())
# }
```

The SDK's defaults favour safety: queues and exchanges are durable, messages are persistent, and `publish` waits for the confirm. Errors are typed: `Error::Unroutable` for a mandatory message nobody received, `Error::Nacked` when a queue rejected it, and `Error::Server { code, .. }` with an AMQP reply code for refused requests.

#### Reconnection

The Rust client reconnects on its own when the connection drops, for example while the broker restarts. After reconnecting, it does the following:

- Declares again the exchanges, queues and bindings it declared. A queue declared with an empty name gets a new name from the broker, and the client moves its bindings and subscriptions to the new name.
- Resumes every subscription. Your `while let Some(msg) = sub.next().await` loop keeps running.
- Publishes again any message that was waiting for a confirm. Delivery is at least once, so a consumer can see a message twice.

Messages delivered before the drop and not yet acknowledged go back to the queue, and the broker delivers them again with `redelivered` set. The subscription skips the old copies, and acknowledging one does nothing.

While the client is reconnecting, calls wait up to 30 seconds by default and then fail with `Error::Closed`. Attempts back off from 100 ms to 5 s and continue until you call `close`. To change this, set `ConnectOptions::reconnect`:

```rust
use std::time::Duration;
use firemq_client::{Client, ConnectOptions, Reconnect};

# async fn run() -> Result<(), firemq_client::Error> {
let mut opts = ConnectOptions::from_url("fire://localhost")?;
opts.reconnect = Some(Reconnect {
    max_attempts: Some(10),
    wait: Duration::from_secs(5),
    ..Default::default()
});
let client = Client::connect_with(opts).await?;
# Ok(())
# }
```

Set `reconnect` to `None` to get an error as soon as the connection drops, and handle reconnection yourself.

### Write your own client

The [FireMQ protocol](protocol.md) is small: length-prefixed frames with a MessagePack header and a raw body. A client needs a MessagePack library and a few hundred lines. [`spec/test-vectors/frames.json`](https://github.com/FireMQ/firemq/blob/main/spec/test-vectors/frames.json) has an encoded example of every frame to test your codec against.

## Testing with an embedded broker (Rust)

The `firemq` crate runs a broker inside your tests, on free ports and in memory:

```rust
# async fn run() -> std::io::Result<()> {
let server = firemq::builder().ephemeral().random_ports().start().await?;
let amqp_url = server.amqp_url().unwrap();
let fire_url = server.fire_url().unwrap();
// ... connect your code to the broker ...
server.shutdown().await;
# Ok(())
# }
```

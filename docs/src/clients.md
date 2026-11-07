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

| Language | Package | Status |
|---|---|---|
| Rust | `firemq-client` | Available |
| Go, TypeScript (Node, Deno, Bun, browsers), Java, .NET | `firemq-go`, `@firemq/client`, `firemq-java`, `FireMQ.Client` | In progress |
| Python, Ruby, Swift, Kotlin, Dart, C | Bindings over the Rust core | In progress |
| Elixir, PHP | Native clients | In progress |

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

# Changelog

## 0.1.0 (2026-11-08)

The first release: a single-node, durable message broker that speaks AMQP 0-9-1 and the FireMQ protocol.

### Broker

- AMQP 0-9-1 with RabbitMQ's extensions: publisher confirms, consumer cancel notifications, `connection.blocked`, exchange-to-exchange bindings, `basic.nack`, per-consumer and per-channel prefetch, and transactions. PLAIN and AMQPLAIN authentication.
- Direct, fanout, topic and headers exchanges, alternate exchanges, and the default exchange.
- Queues with message TTL, queue expiry, dead-lettering with `x-death`, maximum length and size with `drop-head`, `reject-publish` and `reject-publish-dlx`, priorities, single active consumer, exclusive and auto-delete queues, and policies.
- Durable storage in a segmented log. Persistent messages are confirmed after `fsync`, batched across publishers. Recovery truncates torn writes, and a test kills the broker with `kill -9` during confirmed publishing and checks that nothing confirmed is lost.
- Definitions are stored in RabbitMQ's `definitions.json` format, so RabbitMQ exports import unchanged.
- Virtual hosts, users with RabbitMQ-compatible password hashes, and configure, write and read permissions. `guest` can connect only from loopback by default.
- TLS for AMQP and the FireMQ protocol.
- Memory and disk alarms that pause publishers.

### The FireMQ protocol and SDKs

- A small protocol: length-prefixed frames with a MessagePack header and a raw body, over TCP, TLS and WebSocket. Specification and test vectors are in `spec/`.
- A conformance suite of 19 scenarios that every SDK runs against a real broker.
- The Rust SDK, `firemq-client`. It reconnects on its own and restores the topology and subscriptions it declared.
- SDKs for Go, TypeScript, Java and Kotlin, .NET, Elixir and PHP in their own repositories.
- Bindings over the Rust SDK for Python, Ruby, Swift, Dart and C, with prebuilt native libraries: Python wheels, Ruby gems per platform, an XCFramework for macOS and iOS, and C library archives for Linux, macOS and Windows. A Kotlin binding for Android builds from source.

### Operations and developer experience

- `firemq` starts with no configuration, prints connection URLs, and accepts flags, `FIREMQ_*` environment variables, or a `firemq.toml`.
- `firemq ctl`, `pub`, `sub`, `bench`, `doctor`, `init` and `completions`.
- A management API compatible with RabbitMQ's, so `rabbitmqadmin` works, and Prometheus metrics with RabbitMQ's metric names.
- A web UI for queues, exchanges, bindings, connections and messages.
- An embeddable broker for Rust tests: `firemq::builder().ephemeral().random_ports().start()`.
- A 14 MB container image at `ghcr.io/firemq/firemq`, release binaries for Linux, macOS and Windows, an install script, a Homebrew tap, and a GitHub Action.
- A benchmark that runs FireMQ and RabbitMQ under the same limits, `bench/run.sh`. The [results](docs/src/benchmarks.md) are in the documentation.

### Compatibility

CI runs pika, amqplib, amqp091-go (including its own integration suite), the RabbitMQ Java and .NET clients, and `rabbitmqadmin` against every change. See the documentation for [what FireMQ does not support](docs/src/unsupported.md).

# FireMQ

A lean, fast, RabbitMQ-compatible message broker written in Rust. One binary, zero config.

[![CI](https://github.com/FireMQ/firemq/actions/workflows/ci.yml/badge.svg)](https://github.com/FireMQ/firemq/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

FireMQ keeps RabbitMQ's model of exchanges, queues, bindings and acknowledgements, and drops the Erlang runtime, the plugins and the setup. Your existing RabbitMQ clients connect to it unchanged.

```bash
curl -fsSL https://raw.githubusercontent.com/FireMQ/firemq/main/install.sh | sh
firemq
```

```text
FireMQ 0.1.0 is ready in 40 ms

  AMQP 0-9-1   amqp://guest:guest@localhost:5672
  FireMQ       fire://guest:guest@localhost:5673
  WebSocket    ws://localhost:15672/fire
  Management   http://localhost:15672
  Metrics      http://localhost:15672/metrics
  Data         firemq-data (0 queues, 0 messages recovered)
```

Or with Docker:

```bash
docker run -p 5672:5672 -p 5673:5673 -p 15672:15672 ghcr.io/firemq/firemq
```

## Why FireMQ

- **Drop-in for RabbitMQ clients.** FireMQ speaks AMQP 0-9-1. CI runs pika, amqplib, amqp091-go (including its own integration suite), the RabbitMQ Java and .NET clients, and `rabbitmqadmin` against every change. See the [compatibility notes](docs/src/compatibility.md).
- **Durable, and tested that way.** Persistent messages are confirmed after `fsync`, batched so one sync covers a burst of publishes. A test kills the broker with `kill -9` in the middle of confirmed publishing and checks that nothing confirmed is lost.
- **RabbitMQ's features, not a subset of a subset.** Direct, fanout, topic and headers exchanges, exchange-to-exchange bindings, alternate exchanges, publisher confirms, mandatory returns, prefetch, TTL, dead-lettering with `x-death`, max length with overflow modes, priorities, single active consumer, transactions, policies, vhosts, users and permissions, memory and disk alarms.
- **Small.** One static binary. About 20 MB of memory when idle.
- **Built for developers.** `firemq pub`, `sub`, `ctl`, `bench` and `doctor` in the terminal; a management UI and a RabbitMQ-compatible HTTP API; Prometheus metrics with RabbitMQ's names; a broker you can embed in Rust tests.
- **A native protocol and SDKs.** A small MessagePack-based protocol over TCP and WebSocket, with SDKs that share one API across languages.

## Try it

```bash
firemq ctl declare-queue jobs
firemq pub --queue jobs "hello"
firemq sub jobs --count 1
```

From Python, with any RabbitMQ client:

```python
import pika

channel = pika.BlockingConnection(pika.URLParameters("amqp://localhost")).channel()
channel.queue_declare(queue="hello")
channel.basic_publish(exchange="", routing_key="hello", body=b"Hello World!")
```

From Rust, with the FireMQ SDK:

```rust
let client = firemq_client::Client::connect("fire://localhost").await?;
client.publish("", "hello", "Hello World!").await?; // returns when confirmed
```

## Documentation

- [Install](docs/src/install.md) and [quickstart](docs/src/quickstart.md)
- [Tutorials](docs/src/tutorials.md) in Python, JavaScript and Rust, all tested in CI
- [Configuration](docs/src/configuration.md) and the [command-line tool](docs/src/cli.md)
- [Management UI, API and metrics](docs/src/management.md)
- [Durability and recovery](docs/src/durability.md), [security](docs/src/security.md) and [operations](docs/src/operations.md)
- [Migrating from RabbitMQ](docs/src/migrating.md) and [what FireMQ does not support](docs/src/unsupported.md)
- [The FireMQ protocol](spec/fire-protocol.md)

## Project layout

| Path | Contents |
|---|---|
| `crates/firemq` | The `firemq` binary and the embeddable broker |
| `crates/firemq-server` | Listeners, management API, metrics, web UI |
| `crates/firemq-core` | Exchanges, queues, routing, confirms, definitions |
| `crates/firemq-store` | Segmented message log, fsync batching, recovery |
| `crates/firemq-amqp`, `firemq-amqp-codec` | AMQP 0-9-1 |
| `crates/firemq-proto`, `firemq-client` | FireMQ protocol and the Rust SDK |
| `compat/` | RabbitMQ client suites run in CI |
| `spec/` | Protocol specification and test vectors |

## Status

FireMQ runs on a single node. Clustering, streams and MQTT are on the [roadmap](https://github.com/FireMQ/firemq/issues/23).

## License

Apache-2.0. See [LICENSE](LICENSE).

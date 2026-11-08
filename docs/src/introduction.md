# FireMQ

FireMQ is a message broker written in Rust. It speaks RabbitMQ's protocol, AMQP 0-9-1, so the RabbitMQ client you already use connects to FireMQ unchanged. It also speaks its own small protocol for FireMQ's SDKs and for browsers.

FireMQ keeps RabbitMQ's model: publishers send messages to exchanges, exchanges route them to queues through bindings, and consumers acknowledge what they process. It drops what makes RabbitMQ heavy to run: there is no Erlang runtime, no plugins to enable, and no configuration to write before the first message.

```bash
firemq
```

That one command starts a broker with:

- AMQP 0-9-1 on port 5672, for RabbitMQ clients
- the FireMQ protocol on port 5673, for FireMQ SDKs
- the management UI and a RabbitMQ-compatible HTTP API on port 15672
- Prometheus metrics at `/metrics` and WebSocket at `/fire` on the same port
- durable storage in `./firemq-data`

## What you get

- **RabbitMQ compatibility.** Exchanges (direct, fanout, topic, headers), publisher confirms, mandatory returns, prefetch, TTL, dead-lettering, maximum length, priorities, single active consumer, transactions, policies, vhosts, users and permissions behave as they do in RabbitMQ. CI runs pika, amqplib, amqp091-go, the Java client, the .NET client and `rabbitmqadmin` against every change.
- **Durability you can test.** Persistent messages are confirmed only after they are on disk. A test kills the broker with `kill -9` while it confirms publishes, restarts it, and checks that no confirmed message is missing.
- **A small footprint.** One static binary with no runtime dependencies. An idle broker uses about 8 MB of memory on Linux.
- **Tools for developers.** `firemq pub`, `firemq sub`, `firemq ctl`, `firemq bench` and `firemq doctor` work from the terminal, and the broker can run inside Rust tests as a library.

## What FireMQ is not (yet)

FireMQ runs on a single node. It does not cluster or replicate, and it does not implement AMQP 1.0, MQTT, STOMP or streams. [What FireMQ does not support](unsupported.md) lists every gap.

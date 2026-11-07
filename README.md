# FireMQ

A lean, fast, RabbitMQ-compatible message broker written in Rust. One binary, zero config.

> **Status:** under active development. Follow progress in the [roadmap tracking issue](https://github.com/FireMQ/firemq/issues?q=is%3Aissue+label%3Aroadmap).

FireMQ keeps RabbitMQ's mental model (exchanges, queues, bindings, acknowledgements) and drops the Erlang runtime, the plugin sprawl and the setup ceremony.

- **Drop-in for RabbitMQ clients.** FireMQ speaks AMQP 0-9-1, so pika, amqplib, amqp091-go and the Java and .NET clients connect unchanged.
- **A native protocol and first-party SDKs.** A small binary protocol over TCP and WebSocket, with SDKs that share one API across languages.
- **Zero config.** Run `firemq` and get AMQP on 5672, the native protocol on 5673, and the management API and UI on 15672.

## License

Apache-2.0. See [LICENSE](LICENSE).

# RabbitMQ client compatibility

FireMQ speaks AMQP 0-9-1, so RabbitMQ client libraries connect to it unchanged. CI runs every client below against the `firemq` binary on every pull request.

| Language | Library | What CI runs | Status |
|---|---|---|---|
| Python | [pika](https://github.com/pika/pika) | The six RabbitMQ tutorials, publisher confirms, mandatory returns, redeclaration errors, headers exchange, dead-lettering | Passing |
| JavaScript | [amqplib](https://github.com/amqp-node/amqplib) | Tutorials, confirm channels, returns, channel errors | Passing |
| Go | [amqp091-go](https://github.com/rabbitmq/amqp091-go) | Tutorials, confirms, returns, channel errors | Passing |
| Go | amqp091-go's own integration test suite (v1.10.0) | The library's upstream tests, unmodified | Tracked in CI |
| Java | [RabbitMQ Java client](https://github.com/rabbitmq/rabbitmq-java-client) 5.x | Tutorials, confirms, returns, consumer cancel notifications, transactions | Passing |
| .NET | [RabbitMQ.Client](https://github.com/rabbitmq/rabbitmq-dotnet-client) 7.x | Tutorials, tracked publisher confirms, returns, channel errors | Passing |

The suites live in [`compat/`](../compat). To run one locally, start `firemq` and set `AMQP_URL`:

```bash
AMQP_URL=amqp://guest:guest@localhost:5672/ uv run --with pika --with pytest pytest compat/python
```

## Behaviour that matches RabbitMQ on purpose

- `basic.publish` with `immediate=true` closes the connection with 540 NOT_IMPLEMENTED.
- `channel.flow` with `active=false` is answered with 540 NOT_IMPLEMENTED.
- `basic.qos` with a non-zero `prefetch_size` is answered with 540 NOT_IMPLEMENTED.
- Deleting a queue or exchange that does not exist succeeds.
- The `guest` user can only connect from localhost. Set `FIREMQ_LOOPBACK_USERS=` to lift the restriction, for example in containers.

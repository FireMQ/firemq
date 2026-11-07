# Quickstart

This page takes about five minutes. You start a broker, send and receive messages from the terminal and from code, and look at the management UI.

## Start the broker

```bash
firemq
```

FireMQ prints its connection strings:

```text
FireMQ 0.1.0 is ready in 40 ms

  AMQP 0-9-1   amqp://guest:guest@localhost:5672
  FireMQ       fire://guest:guest@localhost:5673
  WebSocket    ws://localhost:15672/fire
  Management   http://localhost:15672
  Metrics      http://localhost:15672/metrics
  Data         firemq-data (0 queues, 0 messages recovered)
```

Leave it running and open a second terminal.

> If port 5672 is in use, RabbitMQ is probably running. Stop it, or start FireMQ on other ports, for example `firemq --amqp 6672`.

## Send and receive from the terminal

Declare a queue, publish to it, and read it back:

```bash
firemq ctl declare-queue jobs
firemq pub --queue jobs "hello from the terminal"
firemq sub jobs --count 1
```

`firemq pub` waits for the broker's confirm, so when it returns the message is on disk. `firemq sub` prints each message and acknowledges it.

## Send and receive from code

Any RabbitMQ client works. With Python and pika:

```bash
pip install pika
```

```python
import pika

connection = pika.BlockingConnection(pika.URLParameters("amqp://guest:guest@localhost:5672"))
channel = connection.channel()
channel.queue_declare(queue="hello")
channel.basic_publish(exchange="", routing_key="hello", body=b"Hello World!")
method, properties, body = channel.basic_get("hello", auto_ack=True)
print(body)
connection.close()
```

The [tutorials](tutorials.md) cover every messaging pattern in Python, JavaScript and Rust.

## Look at the management UI

Open <http://localhost:15672> and log in as `guest` with password `guest`. The UI shows queues, exchanges, connections and rates, and lets you publish, peek at messages, and manage users and policies.

## Next steps

- Read [Configuration](configuration.md) to change ports, storage and limits.
- Read [Migrating from RabbitMQ](migrating.md) to move an existing setup.
- Run `firemq bench --embedded` to measure throughput on your machine.

# Tutorials

These tutorials follow RabbitMQ's classic tutorials. Each pattern has a complete program that sends messages and receives them, so you can run it as is. CI runs every program on this page against FireMQ, so the code always works.

Start a broker first:

```bash
firemq
```

The Python programs use [pika](https://pypi.org/project/pika/) and the JavaScript programs use [amqplib](https://www.npmjs.com/package/amqplib); both talk to FireMQ over AMQP 0-9-1 exactly as they would to RabbitMQ. The Rust programs use FireMQ's own SDK, [`firemq-client`](clients.md#firemq-sdks), over the FireMQ protocol. Set `AMQP_URL` or `FIREMQ_URL` if your broker is not on localhost.

## Hello world

A producer sends a message to a queue and a consumer receives it. The empty exchange name `""` is the default exchange, which delivers to the queue named by the routing key.

Python:

```python
{{#include ../../examples/python/hello_world.py}}
```

JavaScript:

```javascript
{{#include ../../examples/node/hello_world.mjs}}
```

Rust:

```rust
{{#include ../../crates/firemq-client/examples/hello.rs}}
```

## Work queues

Several workers share the tasks of one queue. Two settings make this safe and fair:

- Workers acknowledge a task only after finishing it, so a crashed worker's task goes to another worker.
- A prefetch of 1 gives each worker one unacknowledged task at a time, so a busy worker does not hoard tasks.

The queue is durable and the messages are persistent, so tasks survive a broker restart.

Python:

```python
{{#include ../../examples/python/work_queues.py}}
```

JavaScript:

```javascript
{{#include ../../examples/node/work_queues.mjs}}
```

Rust (the SDK makes queues durable and messages persistent by default):

```rust
{{#include ../../crates/firemq-client/examples/work_queues.rs}}
```

## Publish/subscribe

A fanout exchange copies every message to every queue bound to it. Each subscriber declares its own temporary queue: an empty name asks the broker for a unique one, and `exclusive` deletes it when the connection closes.

Python:

```python
{{#include ../../examples/python/publish_subscribe.py}}
```

JavaScript:

```javascript
{{#include ../../examples/node/publish_subscribe.mjs}}
```

Rust:

```rust
{{#include ../../crates/firemq-client/examples/publish_subscribe.rs}}
```

## Routing

A direct exchange delivers a message to the queues bound with exactly its routing key. Here only the `error` message reaches the queue.

Python:

```python
{{#include ../../examples/python/routing.py}}
```

Rust, with a queue bound to two keys:

```rust
{{#include ../../crates/firemq-client/examples/routing.rs}}
```

## Topics

A topic exchange matches routing keys made of dot-separated words against binding patterns. `*` matches exactly one word and `#` matches zero or more words, so `*.critical` matches `kern.critical` but not `kern.info`.

Python:

```python
{{#include ../../examples/python/topics.py}}
```

JavaScript:

```javascript
{{#include ../../examples/node/topics.mjs}}
```

Rust:

```rust
{{#include ../../crates/firemq-client/examples/topics.rs}}
```

## RPC

A client sends a request with two properties: `reply_to` names a queue for the answer, and `correlation_id` matches the answer to the request. The server publishes its answer to `reply_to` with the same `correlation_id`.

Python:

```python
{{#include ../../examples/python/rpc.py}}
```

Rust:

```rust
{{#include ../../crates/firemq-client/examples/rpc.rs}}
```

## Run all the examples

```bash
./examples/run.sh
```

The script runs every program above against a broker on localhost. It needs [uv](https://docs.astral.sh/uv/), Node.js and Rust.

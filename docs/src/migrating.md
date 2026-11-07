# Migrating from RabbitMQ

Most applications move to FireMQ without code changes, because FireMQ speaks AMQP 0-9-1 and serves RabbitMQ's management API.

## 1. Check that you use supported features

Read [What FireMQ does not support](unsupported.md). The main questions:

- Do you rely on clustering, quorum queue replication, streams, federation or the shovel plugin?
- Do you use plugin exchange types, such as `x-delayed-message` or `x-consistent-hash`?
- Do you connect with MQTT, STOMP or AMQP 1.0?

If not, your applications should work unchanged.

## 2. Export your definitions from RabbitMQ

```bash
rabbitmqadmin export rabbitmq-definitions.json
# or: curl -u admin:password http://rabbitmq:15672/api/definitions > rabbitmq-definitions.json
```

## 3. Import them into FireMQ

```bash
firemq ctl import rabbitmq-definitions.json
```

Users keep their passwords, because FireMQ verifies RabbitMQ's SHA-256 and SHA-512 password hashes. Vhosts, permissions, policies, exchanges, queues and bindings are created. Queues declared as `x-queue-type: quorum` become durable FireMQ queues.

## 4. Point your applications at FireMQ

Change the host in your AMQP URLs. Ports, vhosts and credentials stay the same.

## 5. Move the messages, if you need to

Definitions do not contain messages. To move messages that are still queued, either let the RabbitMQ queues drain before switching publishers, or run a small program that consumes from RabbitMQ and publishes to FireMQ with confirms.

## Differences you may notice

- **Management UI.** FireMQ's UI is different from RabbitMQ's and smaller. The API underneath is compatible.
- **Product name.** `connection.start` and `/api/overview` report the product as `FireMQ`. Code that checks for `RabbitMQ` by name, which is rare, needs a change.
- **Message TTL.** Expired messages are removed when they reach the head of the queue, as in RabbitMQ classic queues, with a timer precision of a few milliseconds.
- **Storage.** Durable queues always page to disk instead of switching between lazy and default modes; `x-queue-mode` is accepted and ignored.
- **Errors.** Reply codes and messages follow RabbitMQ's, including `PRECONDITION_FAILED - inequivalent arg ...`, so existing error handling keeps working.

# What FireMQ does not support

FireMQ aims to be honest about its gaps. If something you need is missing, open an issue on [GitHub](https://github.com/FireMQ/firemq/issues).

## Not supported

| Feature | Notes |
|---|---|
| Clustering and replication | FireMQ runs on one node. Leader-follower replication is on the [roadmap](https://github.com/FireMQ/firemq/issues/22). |
| Quorum queue semantics | `x-queue-type: quorum` is accepted and creates a durable classic queue. Delivery limits (`x-delivery-limit`) and replication are not implemented. |
| Streams | `x-queue-type: stream` is rejected. Streams are on the [roadmap](https://github.com/FireMQ/firemq/issues/20). |
| MQTT, STOMP, AMQP 1.0 | Only AMQP 0-9-1 and the FireMQ protocol. MQTT is on the [roadmap](https://github.com/FireMQ/firemq/issues/21). |
| Plugins | No plugin system. Plugin exchange types (`x-delayed-message`, `x-consistent-hash`, `x-random`) are rejected with `COMMAND_INVALID`. |
| Federation and shovel | Not supported. |
| LDAP, OAuth 2, x509 client authentication | Only username and password (PLAIN, AMQPLAIN). |
| Topic permissions | Only configure, write and read permissions per vhost. |
| `immediate` publishing | Closes the connection with 540, as in RabbitMQ 3.x. |
| `channel.flow` with `active=false` | Rejected with 540, as in RabbitMQ. |
| `basic.recover` with `requeue=false` | Rejected with 540, as in RabbitMQ. |
| `prefetch_size` (byte-based prefetch) | Rejected with 540, as in RabbitMQ. |
| Consumer priorities (`x-priority` on consumers) | Accepted and ignored. |
| Direct reply-to (`amq.rabbitmq.reply-to`) | Not supported; use a temporary exclusive queue. |
| Vhost and user limits, operator policies, parameters | Not supported. |
| HTTPS on the management listener | Use a TLS-terminating proxy. |
| Management API statistics history | Rates are current only; there is no sampling history or `?lengths_age`. |

## Supported

Everything else in RabbitMQ's core AMQP 0-9-1 model works, including:

- exchanges: direct, fanout, topic and headers (`x-match` all, any, all-with-x and any-with-x), exchange-to-exchange bindings, alternate exchanges, internal and auto-delete exchanges
- queues: durable, exclusive, auto-delete, server-named, `x-message-ttl`, per-message `expiration`, `x-expires`, `x-max-length`, `x-max-length-bytes`, `x-overflow` (`drop-head`, `reject-publish`, `reject-publish-dlx`), `x-dead-letter-exchange`, `x-dead-letter-routing-key` with `x-death` headers, `x-max-priority`, `x-single-active-consumer`
- consuming: prefetch per consumer and per channel (`global`), acks, nacks and rejects with requeue, `basic.get`, `basic.recover`, consumer cancel notifications
- publishing: publisher confirms, mandatory returns, transactions (`tx.select`, `tx.commit`, `tx.rollback`)
- policies, vhosts, users, permissions, definitions import and export, memory and disk alarms with `connection.blocked`

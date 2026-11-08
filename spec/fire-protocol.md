# The FireMQ protocol

Version 1, release candidate. The version freezes once the Rust, Go and TypeScript SDKs pass the [conformance suite](conformance.md); from then on, changes are additive only and gated by capabilities.

The FireMQ protocol is the native protocol of the FireMQ broker. It carries the same model as AMQP 0-9-1 (exchanges, queues, bindings, acknowledgements, publisher confirms) in a format that is small enough to implement in an afternoon in any language with a MessagePack library.

FireMQ also speaks AMQP 0-9-1 on port 5672. Both protocols share one broker, so a message published over one is consumed over the other.

## Design rules

- One connection is one session. There are no channels.
- Every frame has a MessagePack header and an optional raw payload. Message bodies are never MessagePack-encoded.
- Requests carry a request id (`rid`) and get exactly one reply: `ok`, `error`, or an op-specific reply.
- Omitted header fields take their zero value: `false`, `0`, empty string, empty map. Implementations ignore header keys they do not know.
- All integers in framing are big-endian.

## Transports

| Transport | Default address | Framing |
|---|---|---|
| TCP | port 5673 | Client first sends the 4 ASCII bytes `FMQ1`, then length-prefixed frames. |
| TLS | port 5674 | As TCP, inside TLS. |
| WebSocket | `ws://host:15672/fire` | One binary WebSocket message per frame, without the length prefix and without `FMQ1`. |

Connection URLs use the schemes `fire://`, `fires://` (TLS), `ws://` and `wss://`:

```
fire://user:password@host:5673/vhost
```

For `fire` and `fires`, the vhost is the URL path with the leading `/` removed and percent-decoded; an empty path means vhost `/`. For `ws` and `wss`, the path is the WebSocket endpoint (`/fire` when empty) and the vhost is the `vhost` query parameter, for example `ws://localhost:15672/fire?vhost=dev`. The default user and password are `guest` and `guest`. The default ports are 5673 (`fire`), 5674 (`fires`), 15672 (`ws`) and 15671 (`wss`).

## Frame format

On TCP and TLS:

```
+-----------+--------+------------+---------------------+------------------+
| len : u32 | op : u8| hlen : u32 | header (hlen bytes) | payload          |
+-----------+--------+------------+---------------------+------------------+
```

- `len` counts every byte after the `len` field: `1 + 4 + hlen + payload length`.
- `header` is a MessagePack map with string keys. `hlen` may be 0, meaning an empty map.
- `payload` is the rest of the frame. Only `publish`, `return`, `deliver` and `get_ok` carry a payload.

On WebSocket the frame is the same without `len`.

Before `welcome`, a frame may be at most 65 536 bytes. After `welcome`, the limit is `welcome.max_frame` (16 MiB by default). A message body must fit in one frame.

## Op codes

| Op | Name | Direction | Payload |
|---|---|---|---|
| `0x01` | `hello` | client → server | |
| `0x02` | `welcome` | server → client | |
| `0x03` | `ping` | both | |
| `0x04` | `pong` | both | |
| `0x05` | `close` | both | |
| `0x06` | `ok` | server → client | |
| `0x07` | `error` | server → client | |
| `0x08` | `flow` | server → client | |
| `0x10` | `declare_exchange` | client → server | |
| `0x11` | `delete_exchange` | client → server | |
| `0x12` | `declare_queue` | client → server | |
| `0x13` | `delete_queue` | client → server | |
| `0x14` | `purge_queue` | client → server | |
| `0x15` | `bind` | client → server | |
| `0x16` | `unbind` | client → server | |
| `0x20` | `publish` | client → server | message body |
| `0x21` | `confirm` | server → client | |
| `0x22` | `return` | server → client | message body |
| `0x30` | `subscribe` | client → server | |
| `0x31` | `unsubscribe` | client → server | |
| `0x32` | `credit` | client → server | |
| `0x33` | `deliver` | server → client | message body |
| `0x34` | `cancelled` | server → client | |
| `0x35` | `ack` | client → server | |
| `0x36` | `nack` | client → server | |
| `0x37` | `get` | client → server | |
| `0x38` | `get_ok` | server → client | message body |

A server that receives an unknown op replies `error` with code 540 and keeps the connection open.

## Connection lifecycle

```
client                                   server
  | -- FMQ1 (TCP only) ------------------> |
  | -- hello ----------------------------> |
  | <-------------------- welcome or error |
  | ... requests, publishes, deliveries ...|
  | -- close ----------------------------> |
  | <------------------------------- close |
```

### `hello`

| Field | Type | Meaning |
|---|---|---|
| `ver` | uint | Protocol major version. Must be `1`. |
| `vhost` | str | Virtual host. Empty means `/`. |
| `auth` | map | `{"mech": "plain", "user": str, "pass": str}`. |
| `heartbeat` | uint | Desired heartbeat interval in seconds. 0 disables heartbeats. |
| `client` | map | Free-form client info, for example `{"name": "firemq-go", "version": "0.1.0", "lang": "go"}`. Shown in the management UI. |
| `caps` | [str] | Capabilities the client supports. |

### `welcome`

| Field | Type | Meaning |
|---|---|---|
| `ver` | uint | Protocol version in use. |
| `server` | map | `{"name": "firemq", "version": str}`. |
| `conn_id` | str | Connection name, as shown in the management API. |
| `heartbeat` | uint | Negotiated heartbeat interval in seconds: the smaller non-zero value of the client's and the server's. |
| `max_frame` | uint | Largest frame the server accepts, in bytes. |
| `caps` | [str] | Capabilities the server supports. Version 1 servers send `confirms`, `credit`, `mandatory`, `exchange-bindings`, `dead-letter` and `ttl`. |

If authentication fails, the server replies `error` with code 403 and `fatal: true`, then closes the socket.

### Heartbeats

When the negotiated interval `h` is non-zero, each side sends `ping` when it has sent nothing for `h / 2` seconds. Each side considers the peer dead when it has received nothing for `2 × h` seconds. A peer that receives `ping` replies `pong`.

### `close`

`{"code": uint, "msg": str}`. The receiver replies with its own `close` and closes the socket. Code 200 means a normal shutdown.

## Replies

### `ok`

| Field | Type | Present for |
|---|---|---|
| `rid` | uint | always |
| `name` | str | `declare_queue` (the queue's name, useful for server-named queues) |
| `messages` | uint | `declare_queue`, `delete_queue`, `purge_queue` |
| `consumers` | uint | `declare_queue` |
| `empty` | bool | `get` when the queue is empty |

### `error`

| Field | Type | Meaning |
|---|---|---|
| `rid` | uint | The failed request, or absent for errors not tied to a request. |
| `code` | uint | An AMQP reply code (table below). |
| `msg` | str | Human-readable reason. |
| `fatal` | bool | `true` when the server closes the connection after this frame. |

| Code | Name | Typical cause |
|---|---|---|
| 320 | connection-forced | Server shutting down. |
| 403 | access-refused | Bad credentials, or no permission for the resource. |
| 404 | not-found | Queue or exchange does not exist. |
| 405 | resource-locked | Exclusive queue owned by another connection. |
| 406 | precondition-failed | Redeclared with different properties, unknown delivery tag, `if_unused` or `if_empty` failed. |
| 501 | frame-error | Malformed frame. Fatal. |
| 502 | syntax-error | Header is not a MessagePack map, or a field has the wrong type. |
| 503 | command-invalid | Request not allowed in the current state, such as a request before `hello`. |
| 506 | resource-error | Server out of a resource. |
| 530 | not-allowed | Operation not allowed, such as deleting a built-in exchange. |
| 540 | not-implemented | Unknown op or unsupported feature. |
| 541 | internal-error | Bug in the server. |

### `flow`

`{"pause": bool}`. The server sends `pause: true` when a memory or disk alarm fires. Clients stop publishing until `pause: false`. A client that keeps publishing is throttled by the server no longer reading its socket.

## Topology

### `declare_exchange`

| Field | Type | Meaning |
|---|---|---|
| `rid` | uint | Request id. |
| `name` | str | Exchange name. Names starting with `amq.` are reserved. |
| `type` | str | `direct`, `fanout`, `topic` or `headers`. |
| `durable` | bool | Survives a broker restart. |
| `auto_delete` | bool | Deleted when its last binding is removed. |
| `internal` | bool | Cannot be published to directly. |
| `passive` | bool | Only check that it exists. |
| `args` | map | Exchange arguments, for example `alternate-exchange`. |

Reply: `ok`. Redeclaring an existing exchange with different `type`, `durable`, `auto_delete` or `internal` fails with 406.

### `delete_exchange`

`{"rid", "name", "if_unused": bool}`. Reply: `ok`.

### `declare_queue`

| Field | Type | Meaning |
|---|---|---|
| `rid` | uint | Request id. |
| `name` | str | Queue name. Empty means the server picks a unique name starting with `amq.gen-`. |
| `durable` | bool | Survives a broker restart. |
| `exclusive` | bool | Used only by this connection and deleted when it closes. |
| `auto_delete` | bool | Deleted when its last subscription is removed. |
| `passive` | bool | Only check that it exists. |
| `args` | map | Queue arguments with RabbitMQ's names: `x-message-ttl`, `x-expires`, `x-max-length`, `x-max-length-bytes`, `x-overflow`, `x-dead-letter-exchange`, `x-dead-letter-routing-key`, `x-single-active-consumer`, `x-max-priority`. |

Reply: `ok` with `name`, `messages` and `consumers`.

### `delete_queue`

`{"rid", "name", "if_unused": bool, "if_empty": bool}`. Reply: `ok` with `messages`, the number of messages deleted.

### `purge_queue`

`{"rid", "name"}`. Reply: `ok` with `messages`, the number of messages purged.

### `bind` and `unbind`

| Field | Type | Meaning |
|---|---|---|
| `rid` | uint | Request id. |
| `exchange` | str | Source exchange. |
| `queue` | str | Destination queue. |
| `to_exchange` | str | Destination exchange, for exchange-to-exchange bindings. Exactly one of `queue` and `to_exchange` is set. |
| `key` | str | Binding key. |
| `args` | map | Binding arguments, for example `x-match` for headers exchanges. |

Reply: `ok`.

## Publishing

### `publish`

| Field | Type | Meaning |
|---|---|---|
| `seq` | uint | Client-assigned sequence number, starting at 1 and increasing by 1. `0` means no confirm is wanted. |
| `exchange` | str | Target exchange. Empty is the default exchange, which routes to the queue named by `key`. |
| `key` | str | Routing key. |
| `mandatory` | bool | Return the message with `return` when no queue receives it. |
| `persistent` | bool | Write the message to disk. Survives a restart when the queue is durable. |
| `props` | map | Message properties (below). |
| `headers` | map | Application headers. Values are any MessagePack type. |

The payload is the message body.

Message properties:

| Key | Type | AMQP property |
|---|---|---|
| `content_type` | str | content-type |
| `content_encoding` | str | content-encoding |
| `correlation_id` | str | correlation-id |
| `reply_to` | str | reply-to |
| `expiration` | uint | expiration, in milliseconds |
| `message_id` | str | message-id |
| `timestamp` | uint | timestamp, in seconds since the Unix epoch |
| `type` | str | type |
| `user_id` | str | user-id. When set, it must equal the connection's user. |
| `app_id` | str | app-id |
| `priority` | uint | priority, 0 to 255 |

### `confirm`

The server confirms every publish with a non-zero `seq` exactly once.

- `{"upto": n}` confirms every outstanding publish with `seq <= n`. The broker has taken responsibility for those messages: they are on disk when persistent and routed to a durable queue, and in memory otherwise.
- `{"seq": n, "nack": true, "code": uint, "msg": str}` rejects one publish, for example because a queue with `x-overflow: reject-publish` is full. The message was not enqueued in every queue that should have received it.

A `nack` for a sequence number is always sent before any `upto` that covers it.

### `return`

`{"seq", "code", "msg", "exchange", "key", "props", "headers"}` with the message body as payload. Sent for a `mandatory` publish that no queue received, with code 312 (no-route). The `return` is sent before the publish's `confirm`.

## Consuming

### `subscribe`

| Field | Type | Meaning |
|---|---|---|
| `rid` | uint | Request id. |
| `sub` | uint | Client-chosen subscription id, unique on the connection. |
| `queue` | str | Queue to consume from. |
| `credit` | uint | Initial credit: how many messages the server may send before the client grants more. |
| `no_ack` | bool | Messages count as acknowledged once sent. |
| `exclusive` | bool | Fail with 405 if the queue has other consumers, and block new ones. |
| `tag` | str | Consumer tag shown in the management API. Defaults to `fire-<sub>`. |
| `args` | map | Consumer arguments. |

Reply: `ok`.

### `credit`

`{"sub": uint, "credit": uint}` adds credit to a subscription. The server sends at most as many `deliver` frames as the credit granted so far. Clients usually grant their buffer size at `subscribe`, then top up by half that size each time they have processed half of it.

### `deliver`

| Field | Type | Meaning |
|---|---|---|
| `sub` | uint | Subscription id. |
| `tag` | uint | Delivery tag, unique and increasing on the connection, starting at 1. |
| `redelivered` | bool | The message was delivered before and not acknowledged. |
| `exchange` | str | Exchange the message was published to. |
| `key` | str | Routing key it was published with. |
| `props` | map | Message properties. |
| `headers` | map | Application headers. |

The payload is the message body.

### `ack` and `nack`

- `ack`: `{"tag": uint, "multiple": bool}`. With `multiple`, acknowledges every unacknowledged delivery with a tag up to and including `tag`.
- `nack`: `{"tag", "multiple", "requeue": bool}`. Requeued messages are delivered again with `redelivered: true`. Messages that are not requeued are dead-lettered when the queue has a dead-letter exchange, and dropped otherwise.

Neither has a reply. An unknown tag produces a non-fatal `error` with code 406.

### `unsubscribe`

`{"rid", "sub"}`. Reply: `ok`. The server sends no more deliveries for `sub`. Unacknowledged deliveries stay unacknowledged until the client acks them or the connection closes, when they are requeued.

### `cancelled`

`{"sub", "code", "msg"}`. The server ended a subscription, for example because its queue was deleted.

### `get` and `get_ok`

`get` is `{"rid", "queue", "no_ack": bool}`. When a message is ready, the server replies `get_ok`:

| Field | Type | Meaning |
|---|---|---|
| `rid` | uint | Request id. |
| `tag` | uint | Delivery tag for `ack` and `nack`. |
| `redelivered` | bool | As in `deliver`. |
| `exchange` | str | As in `deliver`. |
| `key` | str | As in `deliver`. |
| `remaining` | uint | Messages still ready in the queue. |
| `props` | map | Message properties. |
| `headers` | map | Application headers. |

When the queue is empty, the reply is `ok` with `empty: true`.

## Interoperability with AMQP 0-9-1

| FireMQ | AMQP 0-9-1 |
|---|---|
| `persistent: true` | `delivery-mode: 2` |
| `props.expiration` (uint ms) | `expiration` (decimal string) |
| MessagePack nil, bool, int, float, str, bin, array, map | field table `V`, `t`, `l`, `d`, `S`, `x`, `A`, `F` |
| AMQP `D` (decimal) and `T` (timestamp) | float and uint |

## Reserved for later versions

These names are reserved so later versions can add them without breaking version 1 clients:

- Streams: `declare_queue` with `args["x-queue-type"] = "stream"`, a `subscribe` field `offset` (`"first"`, `"last"`, `"next"`, a uint, or `{"ts": uint}`), a `deliver` field `offset`, and an op `0x39` `commit_offset`.
- Capabilities `streams`, `zstd` and `scram`.

## Test vectors

[`test-vectors/frames.json`](test-vectors/frames.json) lists one encoded frame per op with its decoded header and payload. Decoders must decode every vector to the listed header. Encoders do not have to reproduce the bytes exactly, because MessagePack allows several encodings of the same map.

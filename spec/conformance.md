# SDK conformance suite

Every FireMQ SDK runs the same conformance scenarios. The runner, `firemq-conformance`, starts an in-memory broker and drives the SDK through a small **driver**: a program, written with the SDK, that reads JSON commands on stdin and writes JSON replies on stdout. A driver is usually 150 to 300 lines.

```bash
cargo run -p firemq-conformance -- run --driver "go run ./conformance/driver"
```

The runner prints one line per scenario and exits with status 1 if any scenario fails. `--only <name>` runs one scenario and `--list` lists them.

## Driver protocol

The runner writes one JSON object per line. Each has an `id`, an `op`, and the op's fields. The driver writes exactly one reply line per command, with the same `id`, in order:

```json
{"id": 3, "op": "publish", "exchange": "", "key": "jobs", "body": "hi", "persistent": true, "mandatory": false, "properties": {}, "headers": {}}
{"id": 3, "ok": true, "result": {}}
```

A failed command replies with `ok: false` and an error:

```json
{"id": 4, "ok": false, "error": {"kind": "server", "code": 406, "message": "PRECONDITION_FAILED - ..."}}
```

| `kind` | When |
|---|---|
| `server` | The broker refused a request; `code` is the AMQP reply code. |
| `unroutable` | A mandatory publish reached no queue. |
| `nacked` | The broker rejected a publish. |
| `closed` | The connection is closed. |
| `unsupported` | The SDK does not implement the op. |
| `other` | Anything else. |

The driver must not write anything else to stdout. It may log to stderr. It exits when stdin closes.

## Ops

The runner always sends every field listed, so drivers need no defaults.

| Op | Fields | Result |
|---|---|---|
| `hello` | | `{"sdk": "firemq-go", "version": "0.1.0", "capabilities": []}` |
| `connect` | `url` | `{}` |
| `close` | | `{}` |
| `declare_queue` | `name`, `durable`, `exclusive`, `auto_delete`, `args` | `{"name", "messages", "consumers"}` |
| `delete_queue` | `name` | `{"messages"}` |
| `purge_queue` | `name` | `{"messages"}` |
| `declare_exchange` | `name`, `type`, `durable` | `{}` |
| `delete_exchange` | `name` | `{}` |
| `bind_queue` | `queue`, `exchange`, `key`, `args` | `{}` |
| `unbind_queue` | `queue`, `exchange`, `key` | `{}` |
| `bind_exchange` | `destination`, `source`, `key` | `{}` |
| `publish` | `exchange`, `key`, `body` or `body_base64`, `persistent`, `mandatory`, `properties`, `headers` | `{}` after the broker confirms |
| `subscribe` | `queue`, `prefetch`, `no_ack` | `{"sub": "<id>"}` |
| `next` | `sub`, `timeout_ms` | `{"message": <message or null>}` |
| `ack` | `tag` | `{}` |
| `nack` | `tag`, `requeue` | `{}` |
| `unsubscribe` | `sub` | `{}` |
| `get` | `queue`, `no_ack` | `{"message": <message or null>}` |

`args` and `headers` are JSON objects whose values are strings, numbers, booleans, nulls, arrays or objects. `properties` uses the keys of the protocol's message properties: `content_type`, `content_encoding`, `correlation_id`, `reply_to`, `expiration` (milliseconds), `message_id`, `timestamp`, `type`, `user_id`, `app_id` and `priority`.

A message is:

```json
{
  "body": "text, or null when the body is not UTF-8",
  "body_base64": "the body, base64-encoded",
  "exchange": "orders",
  "routing_key": "order.created",
  "redelivered": false,
  "tag": "17",
  "properties": {"content_type": "application/json"},
  "headers": {"trace": "abc"}
}
```

`tag` is an opaque string the runner passes back to `ack` and `nack`.

## Scenarios

| Scenario | Checks |
|---|---|
| `connect_and_close` | Handshake, `hello`, clean close |
| `wrong_password` | Connecting with a bad password fails with code 403 |
| `publish_and_get` | Publish with confirm; `get` returns body, exchange and key; empty queue returns null |
| `consume_and_ack` | 100 messages consumed in order and acknowledged; the queue ends empty |
| `prefetch_limits_deliveries` | With prefetch 2, a third delivery waits for an ack |
| `nack_requeue` | A nacked message comes back with `redelivered: true` |
| `nack_dead_letters` | A message nacked without requeue goes to the dead-letter exchange |
| `mandatory_unroutable` | A mandatory publish to nowhere fails with kind `unroutable` |
| `overflow_nack` | A publish to a full `reject-publish` queue fails with kind `nacked` |
| `topic_routing` | Topic patterns route as specified |
| `exchange_to_exchange` | Exchange-to-exchange bindings route |
| `properties_and_headers` | Every property and header type round-trips |
| `binary_and_large_bodies` | A non-UTF-8 body and a 1 MiB body arrive intact |
| `error_codes` | Redeclaring with other properties gives 406; binding to a missing exchange gives 404; the connection stays usable |
| `purge_and_delete_counts` | Purge and delete report message counts |
| `unacked_requeued_on_close` | Unacknowledged messages return when the connection closes |
| `exclusive_queue_deleted_on_close` | An exclusive queue disappears with its connection |
| `unsubscribe_stops_delivery` | No deliveries after `unsubscribe` |
| `amqp_interop` | A message published by the SDK reaches an AMQP consumer, and back |

# Management UI, API and metrics

FireMQ's HTTP listener, port 15672 by default, serves four things:

| Path | What |
|---|---|
| `/` | The management UI |
| `/api/...` | The management API, compatible with RabbitMQ's |
| `/metrics` | Prometheus metrics |
| `/healthz` | An unauthenticated liveness probe |
| `/fire` | The FireMQ protocol over WebSocket |

## UI

Open `http://localhost:15672` and log in with a user that has the `administrator`, `management`, `monitoring` or `policymaker` tag. The UI shows totals and rates, connections, queues and exchanges. From a queue's page you can publish, peek at or take messages, bind it, purge it and delete it. The Admin page manages users, vhosts, policies and definitions.

## API

The API follows RabbitMQ's management API: the same paths, JSON shapes and status codes. Tools written for RabbitMQ, including `rabbitmqadmin`, work with it. Authenticate with HTTP Basic:

```bash
curl -u guest:guest http://localhost:15672/api/overview
curl -u guest:guest http://localhost:15672/api/queues/%2F
curl -u guest:guest -X PUT -H 'content-type: application/json' \
  -d '{"durable": true, "arguments": {"x-max-length": 1000}}' \
  http://localhost:15672/api/queues/%2F/orders
```

The default vhost `/` is written `%2F` in URLs, and the default exchange is `amq.default`.

### Access

| User tag | Can |
|---|---|
| `administrator` | Everything |
| `monitoring` or `management` | Read everything; `management` users can also fetch messages with `/get` |
| `policymaker` | Read everything and manage policies |

A user without one of these tags cannot use the API. Users in `loopback_users` can only log in from localhost.

### Endpoints

| Area | Endpoints |
|---|---|
| Node | `GET /api/overview`, `/api/nodes`, `/api/whoami`, `/api/cluster-name` |
| Virtual hosts | `GET /api/vhosts`; `GET`, `PUT`, `DELETE /api/vhosts/{vhost}` |
| Users | `GET /api/users`; `GET`, `PUT`, `DELETE /api/users/{user}`; `GET /api/users/{user}/permissions` |
| Permissions | `GET /api/permissions`; `GET`, `PUT`, `DELETE /api/permissions/{vhost}/{user}` |
| Policies | `GET /api/policies[/{vhost}]`; `GET`, `PUT`, `DELETE /api/policies/{vhost}/{name}` |
| Exchanges | `GET /api/exchanges[/{vhost}]`; `GET`, `PUT`, `DELETE /api/exchanges/{vhost}/{name}`; `POST .../publish`; `GET .../bindings/source`, `.../bindings/destination` |
| Queues | `GET /api/queues[/{vhost}]`; `GET`, `PUT`, `DELETE /api/queues/{vhost}/{name}`; `DELETE .../contents`; `POST .../get`; `GET .../bindings` |
| Bindings | `GET /api/bindings[/{vhost}]`; `GET`, `POST /api/bindings/{vhost}/e/{exchange}/q/{queue}` (or `/e/{exchange}`); `GET`, `DELETE .../{properties_key}` |
| Connections | `GET /api/connections`, `/api/vhosts/{vhost}/connections`; `GET`, `DELETE /api/connections/{name}` |
| Channels and consumers | `GET /api/channels`, `/api/consumers[/{vhost}]` |
| Definitions | `GET`, `POST /api/definitions[/{vhost}]` |
| Health | `GET /api/aliveness-test/{vhost}`, `/api/health/checks/alarms`, `local-alarms`, `virtual-hosts`, `port-listener/{port}`, `protocol-listener/{protocol}` |

List endpoints accept `?columns=name,messages` (dotted paths such as `message_stats.publish` work) and `?name=` to filter by name.

## Prometheus metrics

`/metrics` needs no authentication. Metric names follow RabbitMQ's Prometheus plugin, so existing dashboards work:

- `rabbitmq_connections`, `rabbitmq_channels`, `rabbitmq_consumers`, `rabbitmq_queues`
- `rabbitmq_queue_messages_ready`, `rabbitmq_queue_messages_unacked`, `rabbitmq_queue_messages`
- `rabbitmq_global_messages_received_total`, `_delivered_total`, `_acknowledged_total`, `_confirmed_total`, `_unroutable_dropped_total`
- `rabbitmq_process_resident_memory_bytes`, `rabbitmq_resident_memory_limit_bytes`, `rabbitmq_disk_space_available_bytes`, `rabbitmq_alarms_memory_used_watermark`, `rabbitmq_alarms_free_disk_space_watermark`

Per-queue series carry `vhost` and `queue` labels: `firemq_queue_messages_ready`, `firemq_queue_messages_unacked`, `firemq_queue_consumers`, `firemq_queue_messages_published_total`, `firemq_queue_messages_delivered_total` and `firemq_queue_messages_acked_total`.

```yaml
# prometheus.yml
scrape_configs:
  - job_name: firemq
    static_configs:
      - targets: ["localhost:15672"]
```

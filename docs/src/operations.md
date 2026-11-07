# Operations

## Health checks

| Check | Use |
|---|---|
| `GET /healthz` | Liveness. Needs no credentials. Returns 200 while the HTTP listener runs. |
| `GET /api/health/checks/alarms` | Readiness. Returns 503 while a memory or disk alarm is active. |
| `GET /api/aliveness-test/{vhost}` | End to end: declares a queue, publishes and consumes a message. |
| `GET /api/health/checks/port-listener/5672` | Checks that a listener is up. |
| `firemq health` | Exit status 0 when `/healthz` answers. Used by the Docker image's `HEALTHCHECK`. |

## Resource alarms

FireMQ samples its memory use and the free disk space of the data directory every second. It raises an alarm when:

- resident memory exceeds the memory high watermark: 40% of system memory by default, or the absolute `--memory-limit`
- free disk space drops below `--disk-free-limit`, 50 MB by default

While an alarm is active, FireMQ stops reading from connections that publish. Consumers keep working, so queues drain and memory is freed. AMQP clients that support it receive `connection.blocked` and `connection.unblocked`. FireMQ SDKs receive a `flow` frame. The UI shows a banner, and `rabbitmq_alarms_*` metrics are 1.

## Monitoring

Scrape `/metrics` with Prometheus; see [Management UI, API and metrics](management.md#prometheus-metrics). Useful alerts:

- `rabbitmq_alarms_memory_used_watermark == 1` or `rabbitmq_alarms_free_disk_space_watermark == 1`: publishers are paused
- `firemq_queue_messages_ready` growing steadily: consumers are not keeping up
- `firemq_queue_consumers == 0` on a queue that should be consumed

## Logs

Logs go to standard error. Use `--log-format json` for log collectors, and `--log-level debug` or a filter such as `info,firemq_amqp=debug` to investigate client behaviour.

## Backups

- **Topology and users:** `firemq ctl export definitions.json`, or copy `firemq-data/definitions.json`. Restore with `firemq ctl import`.
- **Messages:** stop the broker and copy the whole data directory. Copying it while the broker runs can capture a segment in the middle of a write; recovery truncates such a segment, losing the messages after the damaged record.

## Upgrades

Stop the broker, replace the binary, and start it again. FireMQ refuses to start on a data directory written by a newer format than it understands, rather than misreading it.

## Open files

Each connection and each segment file uses a file descriptor. FireMQ raises its soft limit to the hard limit at startup; `firemq doctor` shows both. For many connections or large backlogs, set the hard limit to at least 65536 (`LimitNOFILE` in the systemd unit).

# Configuration

FireMQ needs no configuration. When you need to change something, each setting can come from three places. The first one found wins:

1. a command-line flag, such as `--amqp 6672`
2. an environment variable, such as `FIREMQ_AMQP=6672`
3. the settings file, `firemq.toml` in the working directory or the file given with `--config`

`firemq init` writes a `firemq.toml` that lists every setting with its default and a comment. FireMQ rejects unknown keys in the file, so a typo fails at startup instead of being ignored.

## Listeners

A listener value is `host:port`, a bare port (all interfaces), or `off`.

| Setting | Flag | Environment | Default |
|---|---|---|---|
| AMQP 0-9-1 | `--amqp` | `FIREMQ_AMQP` | `0.0.0.0:5672` |
| AMQP over TLS | `--amqps` | `FIREMQ_AMQPS` | `0.0.0.0:5671`, only with TLS |
| FireMQ protocol | `--fire` | `FIREMQ_FIRE` | `0.0.0.0:5673` |
| FireMQ over TLS | `--fires` | `FIREMQ_FIRES` | `0.0.0.0:5674`, only with TLS |
| HTTP: UI, API, metrics, WebSocket | `--http` | `FIREMQ_HTTP` | `0.0.0.0:15672` |
| TLS certificate chain (PEM) | `--tls-cert` | `FIREMQ_TLS_CERT` | none |
| TLS private key (PEM) | `--tls-key` | `FIREMQ_TLS_KEY` | none |

In `firemq.toml`, listeners go in `[listeners]` and the TLS files in `[tls]` as `cert` and `key`.

## Storage

| Setting | Flag | Environment | `[storage]` key | Default |
|---|---|---|---|---|
| Data directory | `--data-dir` | `FIREMQ_DATA_DIR` | `data_dir` | `firemq-data` |
| Keep everything in memory | `--ephemeral` | `FIREMQ_EPHEMERAL` | `ephemeral` | `false` |
| Sync before confirming | `--fsync` | `FIREMQ_FSYNC` | `fsync` | `true` |

With `fsync = false`, a confirmed persistent message survives a broker crash but can be lost if the machine loses power. See [Durability and recovery](durability.md).

## Limits

| Setting | Flag | Environment | `[limits]` key | Default |
|---|---|---|---|---|
| Memory high watermark (fraction of RAM) | `--memory-high-watermark` | `FIREMQ_MEMORY_HIGH_WATERMARK` | `memory_high_watermark` | `0.4` |
| Absolute memory limit | `--memory-limit` | `FIREMQ_MEMORY_LIMIT` | `memory_limit` | none |
| Free disk space limit | `--disk-free-limit` | `FIREMQ_DISK_FREE_LIMIT` | `disk_free_limit` | `50MB` |

Sizes take units: `512MB`, `2GiB`, `1000000`. When a limit is crossed, publishers are paused; see [Operations](operations.md#resource-alarms).

## Users

| Setting | Flag | Environment | `[auth]` key | Default |
|---|---|---|---|---|
| First user | `--default-user` | `FIREMQ_DEFAULT_USER` | `default_user` | `guest` |
| First user's password | `--default-pass` | `FIREMQ_DEFAULT_PASS` | `default_pass` | `guest` |
| Users limited to localhost | `--loopback-users` | `FIREMQ_LOOPBACK_USERS` | `loopback_users` | `guest` |

The first user is created only when the broker has no users, and gets the `administrator` tag and full access to the `/` vhost. An empty `FIREMQ_LOOPBACK_USERS` lets every user connect from any host.

## Logging

| Setting | Flag | Environment | `[log]` key | Default |
|---|---|---|---|---|
| Level or filter | `--log-level` | `FIREMQ_LOG` | `level` | `info` |
| Format: `text` or `json` | `--log-format` | `FIREMQ_LOG_FORMAT` | `format` | `text` |

The level also accepts per-module filters, for example `info,firemq_amqp=debug`.

## Development mode

`firemq --dev` is shorthand for an in-memory broker that accepts `guest` from any host and logs at debug level. Use it in containers for local development and in CI.

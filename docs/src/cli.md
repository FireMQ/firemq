# Command-line tool

The `firemq` binary is the broker and a set of tools for working with it. Run `firemq <command> --help` for every option.

| Command | What it does |
|---|---|
| `firemq` | Starts the broker. Same as `firemq serve`. |
| `firemq --dev` | Starts an in-memory broker that accepts `guest` from any host and logs at debug level. |
| `firemq init` | Writes a commented `firemq.toml` with every setting and its default. |
| `firemq ctl ...` | Manages a running broker through its HTTP API. |
| `firemq pub ...` | Publishes messages and waits for the confirms. |
| `firemq sub ...` | Consumes messages and prints them. |
| `firemq bench` | Measures throughput and latency. |
| `firemq doctor` | Checks file limits, ports, the data directory and a running broker. |
| `firemq completions <shell>` | Prints completions for bash, zsh, fish, PowerShell or Elvish. |

## Manage a broker with `ctl`

`ctl` talks to the management API at `http://localhost:15672` as `guest` by default. Change that with `--url`, `--user` and `--pass`, or with `FIREMQ_HTTP_URL`, `FIREMQ_USER` and `FIREMQ_PASS`. `--vhost` selects the virtual host, and `--json` prints raw JSON instead of a table.

```bash
firemq ctl overview
firemq ctl list queues            # also: exchanges, bindings, connections, consumers, users, vhosts, policies, permissions
firemq ctl declare-queue orders --arg x-message-ttl=60000 --arg x-dead-letter-exchange=dlx
firemq ctl declare-exchange events topic
firemq ctl bind events orders --key 'order.*'
firemq ctl get orders --count 5   # peek; add --take to remove the messages
firemq ctl purge orders
firemq ctl delete-queue orders
firemq ctl add-user app s3cret --tags management
firemq ctl set-permissions app --configure '^app\.' --write '^app\.' --read '^app\.'
firemq ctl set-policy ttl '^tmp\.' '{"message-ttl": 60000}'
firemq ctl export definitions.json
firemq ctl import definitions.json   # also accepts a RabbitMQ export
firemq ctl close-connection '127.0.0.1:50000 -> 127.0.0.1:5672'
```

## Publish and consume

`pub` and `sub` use the FireMQ protocol at `fire://localhost`. Set `--url` or `FIREMQ_URL` to change it.

```bash
firemq pub events order.created '{"id": 1}' --content-type application/json
firemq pub --queue jobs "one task"           # straight to a queue
firemq pub --queue jobs --count 10000 "load" # publish many, pipelined
cat payload.json | firemq pub --queue jobs   # body from stdin
firemq pub --queue jobs --file payload.bin -H trace=abc

firemq sub jobs                  # print and acknowledge until Ctrl-C
firemq sub jobs --count 10       # stop after 10
firemq sub jobs --peek --count 1 # look without removing (requeue)
firemq sub jobs --json           # JSON lines with properties and headers
```

## Benchmark

```bash
firemq bench --embedded                       # an in-process broker, nothing else needed
firemq bench --publishers 4 --consumers 4 --size 1024 --duration 30
firemq bench --persistent                     # durable queue, persistent messages
```

`bench` prints published and consumed messages per second every second, then the averages and the end-to-end latency percentiles. Publishers keep `--window` unconfirmed messages in flight.

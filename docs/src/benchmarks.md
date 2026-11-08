# Benchmarks

This page compares FireMQ with RabbitMQ on the same machine, under the same limits, using the same load generator. Run `bench/run.sh` to reproduce it on your hardware; numbers from a laptop are a comparison, not a capacity plan.

## Method

Each broker runs in a Docker container limited to 4 CPUs and 2 GiB of memory. The load generators run in a second container, also limited to 4 CPUs, on the same Docker network. Nothing crosses the macOS host's network stack.

Two load generators produce the same workload:

- `amqp_bench`, an AMQP 0-9-1 client built on [amqprs](https://crates.io/crates/amqprs), drives both brokers. Its source is [`crates/firemq-amqp/examples/amqp_bench.rs`](https://github.com/FireMQ/firemq/blob/main/crates/firemq-amqp/examples/amqp_bench.rs).
- `firemq bench` drives FireMQ over its own protocol.

Both work the same way:

- Every publisher uses publisher confirms with up to 1,000 unconfirmed messages in flight.
- Every consumer uses a prefetch of 1,000 and acknowledges each message individually.
- The queue is durable. "Persistent" scenarios publish persistent messages, which both brokers write to disk before confirming; "transient" scenarios publish non-persistent messages.
- Each message body starts with its send time, so the consumer measures end-to-end latency from publish to delivery.
- Throughput scenarios publish as fast as confirms allow. Latency scenarios publish a fixed 1,000 messages per second, so latency isn't dominated by queueing.

A fresh broker container starts for every run. Each scenario runs three times for 20 seconds, and the tables show the median run: the run with the median delivered rate. Memory is the container's peak memory use during the run, sampled every 2 seconds with `docker stats`.

Throughput is the rate at which consumers received messages. When a broker accepts messages faster than it delivers them, the queue grows, and latency then measures mostly time spent waiting in the queue. The results call out those runs.

The recovery test fills a durable queue with 2,000,000 persistent 256-byte messages, stops the broker gracefully, starts it again, and measures how long until the queue holds every message again. It counts messages with a passive `queue.declare`, which both brokers answer exactly; RabbitMQ's management API refreshes its counts only every 5 seconds. Startup and recovery times include about 0.2 seconds to start the container that measures them.

## Results

Measured on November 7, 2026, on an Apple M2 Pro with 10 cores and Docker Desktop 29.4.3. FireMQ ran at commit `bf67ac7`. RabbitMQ ran from the `rabbitmq:4-management` image: RabbitMQ 4.4.0 on Erlang/OTP 29. RabbitMQ's numbers come from a run earlier the same evening. FireMQ's runs were repeated after a change that lowered its idle memory, which doesn't affect RabbitMQ. The raw logs and CSV files are in [`bench/results/2026-11-07`](https://github.com/FireMQ/firemq/tree/main/bench/results/2026-11-07).

### Throughput

Messages delivered to consumers per second:

| Scenario | RabbitMQ | FireMQ, AMQP | FireMQ, FireMQ protocol |
|---|---|---|---|
| Transient, 1 publisher and 1 consumer, 256 B | 113,000 | 135,000 | 156,000 |
| Persistent, 1 publisher and 1 consumer, 1 KiB | 79,000 | 134,000 | 174,000 |
| Transient, 4 publishers and 4 consumers, 256 B | 123,000 | 274,000 | 331,000 |
| Persistent, 4 publishers and 4 consumers, 1 KiB | 57,000 | 261,000 | 300,000 |

Driven by the same AMQP client, FireMQ delivered 1.2 to 4.6 times as many messages per second as RabbitMQ. The gap is widest with persistent messages and several publishers, because FireMQ syncs the writes of all publishers together, at most once every 2 ms. The FireMQ protocol adds another 10 to 25 percent.

In the two AMQP scenarios with 4 publishers, FireMQ accepted messages faster than it delivered them:

- Transient messages: 313,000 published and 274,000 delivered per second.
- Persistent messages: 283,000 published and 261,000 delivered per second.

The broker was at its 4-CPU limit in both. FireMQ doesn't slow publishers down when consumers fall behind, so the queue grew for the length of the run. RabbitMQ slows publishers in that situation with its credit flow. Over the FireMQ protocol, whose acknowledgments are cumulative and cheaper to process, publishing and delivery stayed level in every scenario.

### Latency

End-to-end latency with publishers held at 1,000 messages per second, p50 / p99:

| Scenario | RabbitMQ | FireMQ, AMQP | FireMQ, FireMQ protocol |
|---|---|---|---|
| Transient, 256 B | 0.65 ms / 1.07 ms | 0.35 ms / 0.63 ms | 0.39 ms / 0.73 ms |
| Persistent, 1 KiB | 0.66 ms / 1.52 ms | 0.26 ms / 0.71 ms | 0.24 ms / 0.70 ms |

Persistent messages include the wait for the disk: neither broker confirms or delivers them before the write that holds them is synced.

End-to-end latency at full throughput, p50 / p99:

| Scenario | RabbitMQ | FireMQ, AMQP | FireMQ, FireMQ protocol |
|---|---|---|---|
| Transient, 1 publisher and 1 consumer, 256 B | 6.18 ms / 8.39 ms | 7.21 ms / 8.29 ms | 6.25 ms / 7.16 ms |
| Persistent, 1 publisher and 1 consumer, 1 KiB | 6.34 ms / 12 ms | 1.78 ms / 3.98 ms | 0.88 ms / 3.17 ms |
| Transient, 4 publishers and 4 consumers, 256 B | 35 ms / 42 ms | 1.3 s / 2.5 s | 13 ms / 26 ms |
| Persistent, 4 publishers and 4 consumers, 1 KiB | 28 ms / 64 ms | 833 ms / 1.6 s | 5.04 ms / 15 ms |

At full throughput, latency mostly measures time spent waiting. When the consumer is the bottleneck, it holds up to its prefetch of 1,000 messages that it hasn't processed yet, so latency approaches 1,000 divided by the delivery rate: about 7 ms at 140,000 messages per second. The FireMQ AMQP rows with 4 publishers show the queue that grew during those runs.

### Memory

Peak broker memory during the throughput runs:

| Scenario | RabbitMQ | FireMQ, AMQP | FireMQ, FireMQ protocol |
|---|---|---|---|
| Transient, 1 publisher and 1 consumer, 256 B | 254 MiB | 9 MiB | 9 MiB |
| Persistent, 1 publisher and 1 consumer, 1 KiB | 252 MiB | 18 MiB | 20 MiB |
| Transient, 4 publishers and 4 consumers, 256 B | 285 MiB | 204 MiB | 22 MiB |
| Persistent, 4 publishers and 4 consumers, 1 KiB | 302 MiB | 129 MiB | 48 MiB |

Most of RabbitMQ's memory is the Erlang runtime, which uses about 245 MiB before the first message. FireMQ's memory follows the messages it holds. In the AMQP runs with 4 publishers, that includes the backlog that built up during the run.

### Startup and recovery

| | RabbitMQ | FireMQ |
|---|---|---|
| Ready to accept connections after the container starts | 2.04 s | 0.36 s |
| Memory when idle | 246 MiB | 3 MiB |
| Restart with 2,000,000 queued messages, until every message is back | 2.40 s | 1.34 s |
| Memory holding those messages, before the restart | 231 MiB | 156 MiB |
| Memory holding those messages, after the restart | 255 MiB | 124 MiB |
| Container image | 456 MB | 13.8 MB |

On startup FireMQ reads each queue's segment files and verifies every record's checksum, so recovery time grows with the amount of queued data on disk. See [Durability and recovery](durability.md#crash-recovery).

## Reproduce

```bash
bench/run.sh          # 20 seconds per run, 3 runs
bench/run.sh 60 5     # longer runs
CPUS=2 MEMORY=1g bench/run.sh
```

The script builds the FireMQ image from the working tree, builds the load generators for Linux, and writes raw logs and CSV files to `bench/results/<date>/`. To turn them into the tables on this page, run:

```bash
bench/summarize.py bench/results/<date>
```

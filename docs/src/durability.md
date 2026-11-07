# Durability and recovery

This page explains what survives a restart or a crash, and what a publisher confirm promises.

## What survives a restart

A message survives a restart when both of these are true:

- the queue is **durable** (`durable: true` when declared)
- the message is **persistent** (`delivery_mode: 2` in AMQP, `persistent: true` in the FireMQ protocol)

Durable exchanges, durable queues, the bindings between them, users, permissions, vhosts and policies also survive. Transient messages and non-durable queues are kept in memory and are gone after a restart, as in RabbitMQ. Messages that were delivered but not acknowledged come back after a restart with `redelivered` set.

`--ephemeral` keeps everything in memory, including durable queues.

## What a confirm means

With publisher confirms (`confirm.select` in AMQP, always on in the FireMQ protocol), the broker acknowledges a publish when it has taken responsibility for the message:

| Message | Queue | Confirmed after |
|---|---|---|
| Persistent | Durable | The message is written and synced to disk (`fsync`) |
| Persistent | Non-durable | The message is in the queue's memory |
| Transient | Any | The message is in the queue's memory |
| Any | No queue matched | Routing finished; the message is dropped, or returned first when `mandatory` |

Syncing is batched. One thread waits about 2 ms after the first pending write, then syncs every queue file that changed and confirms everything written before the sync. Under load, one sync covers thousands of publishes.

With `--fsync=false`, confirms are sent once the message is written to the operating system. The message survives a broker crash but not a power loss.

## How FireMQ stores messages

```text
firemq-data/
  node.json              format version
  definitions.json       users, vhosts, permissions, policies, durable topology
  firemq.lock            prevents two brokers from using the directory
  queues/<vhost>/<queue>/
    meta.json            queue name, vhost and arguments
    0000000001.seg       messages, appended in order
    0000000001.ack       which messages were acknowledged
```

- Each durable queue appends messages to segment files of about 32 MiB. Every record carries a CRC32C checksum.
- When a message is acknowledged, its position is appended to the segment's `.ack` file. A segment is deleted as soon as all its messages are acknowledged, so disk use follows the backlog.
- A durable queue keeps up to 8 MiB of message bodies in memory and reads the rest from disk, so a large backlog does not fill memory.
- `definitions.json` is rewritten atomically (write, sync, rename) shortly after any durable change. It uses RabbitMQ's definitions format, so you can read it, back it up, or import it into RabbitMQ.

## Crash recovery

At startup FireMQ:

1. reads `definitions.json` and recreates users, vhosts, policies, exchanges, queues and bindings
2. restores queues that have data on disk but are missing from the definitions, which happens if the broker crashed right after a queue was declared
3. scans each queue's segments, verifies every checksum, and truncates a segment at the first damaged record, which is where a crash interrupted a write
4. skips acknowledged and transient messages and requeues the rest as redelivered

The startup banner and the `Data` line report how many queues and messages were recovered.

## Tested behaviour

A test in the repository (`crates/firemq/tests/chaos.rs`) starts the broker, publishes persistent messages with 256 confirms in flight, kills the process with `kill -9` at a random moment, restarts it, and repeats four times. It then checks that every confirmed message is present. Tens of thousands of messages are confirmed per run, and none are lost.

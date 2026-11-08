// RabbitMQ tutorial scenarios with amqplib against FireMQ.
// Run: AMQP_URL=amqp://guest:guest@localhost:5672 npm test
import { test, after } from "node:test";
import assert from "node:assert/strict";
import amqp from "amqplib";

const URL = process.env.AMQP_URL ?? "amqp://guest:guest@localhost:5672";
const conn = await amqp.connect(URL);
after(() => conn.close());
const uid = (p) => `${p}-${Math.random().toString(16).slice(2, 10)}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function drain(ch, q) {
  const out = [];
  for (;;) {
    const m = await ch.get(q, { noAck: true });
    if (!m) return out;
    out.push(m.content.toString());
  }
}

test("hello world", async () => {
  const ch = await conn.createChannel();
  const q = uid("hello");
  await ch.assertQueue(q, { durable: false });
  ch.sendToQueue(q, Buffer.from("Hello World!"));
  const got = await new Promise((resolve) => {
    ch.consume(q, (m) => resolve(m.content.toString()), { noAck: true });
  });
  assert.equal(got, "Hello World!");
  await ch.close();
});

test("work queues with prefetch and acks", async () => {
  const ch = await conn.createChannel();
  const q = uid("task_queue");
  await ch.assertQueue(q, { durable: true });
  for (let i = 0; i < 5; i++) ch.sendToQueue(q, Buffer.from(`task ${i}`), { persistent: true });
  await ch.prefetch(1);
  const got = await new Promise((resolve) => {
    const seen = [];
    ch.consume(q, (m) => {
      seen.push(m.content.toString());
      ch.ack(m);
      if (seen.length === 5) resolve(seen);
    });
  });
  assert.deepEqual(got, [0, 1, 2, 3, 4].map((i) => `task ${i}`));
  await ch.close();
});

test("publish/subscribe fanout", async () => {
  const ch = await conn.createChannel();
  const ex = uid("logs");
  await ch.assertExchange(ex, "fanout", { durable: false });
  const queues = [];
  for (let i = 0; i < 3; i++) {
    const { queue } = await ch.assertQueue("", { exclusive: true });
    await ch.bindQueue(queue, ex, "");
    queues.push(queue);
  }
  ch.publish(ex, "", Buffer.from("broadcast"));
  await sleep(100);
  for (const q of queues) assert.deepEqual(await drain(ch, q), ["broadcast"]);
  await ch.close();
});

test("routing and topics", async () => {
  const ch = await conn.createChannel();
  const direct = uid("direct_logs");
  await ch.assertExchange(direct, "direct", { durable: false });
  const { queue: errors } = await ch.assertQueue("", { exclusive: true });
  await ch.bindQueue(errors, direct, "error");
  for (const s of ["info", "error"]) ch.publish(direct, s, Buffer.from(s));
  const topic = uid("topic_logs");
  await ch.assertExchange(topic, "topic", { durable: false });
  const { queue: kern } = await ch.assertQueue("", { exclusive: true });
  await ch.bindQueue(kern, topic, "kern.*");
  for (const k of ["kern.critical", "app.critical", "kern.info.extra"]) ch.publish(topic, k, Buffer.from(k));
  await sleep(100);
  assert.deepEqual(await drain(ch, errors), ["error"]);
  assert.deepEqual(await drain(ch, kern), ["kern.critical"]);
  await ch.close();
});

test("rpc with reply_to and correlation_id", async () => {
  const server = await conn.createChannel();
  const rpcQueue = uid("rpc_queue");
  await server.assertQueue(rpcQueue, { durable: false });
  server.consume(rpcQueue, (m) => {
    const n = parseInt(m.content.toString(), 10);
    server.sendToQueue(m.properties.replyTo, Buffer.from(String(n * 2)), {
      correlationId: m.properties.correlationId,
    });
    server.ack(m);
  });
  const client = await conn.createChannel();
  const { queue: replies } = await client.assertQueue("", { exclusive: true });
  const corr = uid("corr");
  const answer = new Promise((resolve) => {
    client.consume(replies, (m) => {
      if (m.properties.correlationId === corr) resolve(m.content.toString());
    }, { noAck: true });
  });
  client.sendToQueue(rpcQueue, Buffer.from("21"), { replyTo: replies, correlationId: corr });
  assert.equal(await answer, "42");
  await client.close();
  await server.close();
});

test("publisher confirms", async () => {
  const ch = await conn.createConfirmChannel();
  const q = uid("confirmed");
  await ch.assertQueue(q, { durable: true });
  for (let i = 0; i < 100; i++) ch.sendToQueue(q, Buffer.from(`m${i}`), { persistent: true });
  await ch.waitForConfirms();
  const { messageCount } = await ch.checkQueue(q);
  assert.equal(messageCount, 100);
  await ch.close();
});

test("mandatory unroutable publish is returned", async () => {
  const ch = await conn.createChannel();
  const returned = new Promise((resolve) => ch.on("return", (m) => resolve(m.content.toString())));
  ch.publish("", uid("nowhere"), Buffer.from("lost"), { mandatory: true });
  assert.equal(await returned, "lost");
  await ch.close();
});

test("channel error does not kill the connection", async () => {
  const ch = await conn.createChannel();
  ch.on("error", () => {});
  await assert.rejects(ch.checkQueue(uid("missing")), /404/);
  const ch2 = await conn.createChannel();
  await ch2.assertQueue(uid("ok"), { exclusive: true });
  await ch2.close();
});

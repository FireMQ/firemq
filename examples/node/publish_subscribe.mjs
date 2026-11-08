// Publish/subscribe: a fanout exchange copies each message to every bound queue.
import amqp from "amqplib";

const conn = await amqp.connect(process.env.AMQP_URL ?? "amqp://localhost");
const ch = await conn.createChannel();
await ch.assertExchange("logs", "fanout", { durable: false });
const queues = [];
for (let i = 0; i < 2; i++) {
  const { queue } = await ch.assertQueue("", { exclusive: true });
  await ch.bindQueue(queue, "logs", "");
  queues.push(queue);
}
ch.publish("logs", "", Buffer.from("info: hello subscribers"));
for (const q of queues) {
  await new Promise((resolve) => ch.consume(q, (msg) => { console.log(` [${q.slice(-6)}] ${msg.content}`); resolve(); }, { noAck: true }));
}
await conn.close();

// Topics: route with word patterns; * matches one word, # matches zero or more.
import amqp from "amqplib";

const conn = await amqp.connect(process.env.AMQP_URL ?? "amqp://localhost");
const ch = await conn.createChannel();
await ch.assertExchange("topic_logs", "topic", { durable: false });
const { queue } = await ch.assertQueue("", { exclusive: true });
await ch.bindQueue(queue, "topic_logs", "*.critical");
for (const key of ["kern.critical", "kern.info", "app.critical"]) ch.publish("topic_logs", key, Buffer.from(key));

let received = 0;
await new Promise((resolve) =>
  ch.consume(queue, (msg) => {
    console.log(` [x] ${msg.content}`);
    if (++received === 2) resolve();
  }, { noAck: true }),
);
await conn.close();

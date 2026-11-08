// Work queues: share tasks between workers, one unacknowledged task each.
import amqp from "amqplib";

const conn = await amqp.connect(process.env.AMQP_URL ?? "amqp://localhost");
const ch = await conn.createChannel();
await ch.assertQueue("task_queue", { durable: true });
for (let n = 1; n <= 3; n++) ch.sendToQueue("task_queue", Buffer.from(`task ${n}`), { persistent: true });

await ch.prefetch(1); // at most one unacknowledged task per worker
let done = 0;
await new Promise((resolve) =>
  ch.consume("task_queue", (msg) => {
    console.log(` [x] Done ${msg.content}`);
    ch.ack(msg);
    if (++done === 3) resolve();
  }),
);
await conn.close();

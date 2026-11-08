// Hello world: send one message to a queue and receive it.
import amqp from "amqplib";

const conn = await amqp.connect(process.env.AMQP_URL ?? "amqp://localhost");
const ch = await conn.createChannel();
await ch.assertQueue("hello", { durable: false });

ch.sendToQueue("hello", Buffer.from("Hello World!"));
console.log(" [x] Sent 'Hello World!'");

await new Promise((resolve) =>
  ch.consume("hello", (msg) => {
    console.log(` [x] Received '${msg.content}'`);
    resolve();
  }, { noAck: true }),
);
await conn.close();

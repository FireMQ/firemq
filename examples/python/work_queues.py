"""Work queues: share tasks between workers, one unacknowledged task each."""
import os

import pika

connection = pika.BlockingConnection(pika.URLParameters(os.environ.get("AMQP_URL", "amqp://localhost")))
channel = connection.channel()
channel.queue_declare(queue="task_queue", durable=True)

for n in range(1, 4):
    channel.basic_publish(
        exchange="",
        routing_key="task_queue",
        body=f"task {n}".encode(),
        properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
    )

channel.basic_qos(prefetch_count=1)  # at most one unacknowledged task per worker
done = 0
for method, properties, body in channel.consume("task_queue", inactivity_timeout=5):
    print(f" [x] Done {body.decode()}")
    channel.basic_ack(method.delivery_tag)
    done += 1
    if done == 3:
        break
connection.close()

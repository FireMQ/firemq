"""Publish/subscribe: a fanout exchange copies each message to every bound queue."""
import os

import pika

connection = pika.BlockingConnection(pika.URLParameters(os.environ.get("AMQP_URL", "amqp://localhost")))
channel = connection.channel()
channel.exchange_declare(exchange="logs", exchange_type="fanout")

# Each subscriber gets its own temporary queue.
queues = [channel.queue_declare(queue="", exclusive=True).method.queue for _ in range(2)]
for q in queues:
    channel.queue_bind(exchange="logs", queue=q)

channel.basic_publish(exchange="logs", routing_key="", body=b"info: hello subscribers")
for q in queues:
    for method, properties, body in channel.consume(q, auto_ack=True, inactivity_timeout=5):
        print(f" [{q[-6:]}] {body.decode()}")
        break
    channel.cancel()  # stop this consumer before reading the next queue
connection.close()

"""Hello world: send one message to a queue and receive it."""
import os

import pika

connection = pika.BlockingConnection(pika.URLParameters(os.environ.get("AMQP_URL", "amqp://localhost")))
channel = connection.channel()
channel.queue_declare(queue="hello")

channel.basic_publish(exchange="", routing_key="hello", body=b"Hello World!")
print(" [x] Sent 'Hello World!'")

for method, properties, body in channel.consume("hello", auto_ack=True, inactivity_timeout=5):
    print(f" [x] Received {body.decode()!r}")
    break
connection.close()

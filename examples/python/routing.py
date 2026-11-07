"""Routing: a direct exchange delivers by exact routing key."""
import os

import pika

connection = pika.BlockingConnection(pika.URLParameters(os.environ.get("AMQP_URL", "amqp://localhost")))
channel = connection.channel()
channel.exchange_declare(exchange="direct_logs", exchange_type="direct")
errors = channel.queue_declare(queue="", exclusive=True).method.queue
channel.queue_bind(exchange="direct_logs", queue=errors, routing_key="error")

for severity in ("info", "warning", "error"):
    channel.basic_publish(exchange="direct_logs", routing_key=severity, body=f"{severity} event".encode())

for method, properties, body in channel.consume(errors, auto_ack=True, inactivity_timeout=5):
    print(f" [x] {method.routing_key}: {body.decode()}")  # only the error event arrives
    break
connection.close()

"""Topics: route with word patterns; * matches one word, # matches zero or more."""
import os

import pika

connection = pika.BlockingConnection(pika.URLParameters(os.environ.get("AMQP_URL", "amqp://localhost")))
channel = connection.channel()
channel.exchange_declare(exchange="topic_logs", exchange_type="topic")
critical = channel.queue_declare(queue="", exclusive=True).method.queue
channel.queue_bind(exchange="topic_logs", queue=critical, routing_key="*.critical")

for key in ("kern.critical", "kern.info", "app.critical"):
    channel.basic_publish(exchange="topic_logs", routing_key=key, body=key.encode())

received = 0
for method, properties, body in channel.consume(critical, auto_ack=True, inactivity_timeout=5):
    print(f" [x] {body.decode()}")
    received += 1
    if received == 2:
        break
connection.close()

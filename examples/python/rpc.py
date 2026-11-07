"""RPC: a request carries reply_to and correlation_id; the server answers there."""
import os
import threading
import uuid

import pika

URL = pika.URLParameters(os.environ.get("AMQP_URL", "amqp://localhost"))


def server():
    connection = pika.BlockingConnection(URL)
    channel = connection.channel()
    for method, props, body in channel.consume("rpc_queue", inactivity_timeout=5):
        n = int(body)
        channel.basic_publish(
            exchange="",
            routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id=props.correlation_id),
            body=str(n * n).encode(),
        )
        channel.basic_ack(method.delivery_tag)
        break
    connection.close()


connection = pika.BlockingConnection(URL)
channel = connection.channel()
channel.queue_declare(queue="rpc_queue")
threading.Thread(target=server).start()

callback_queue = channel.queue_declare(queue="", exclusive=True).method.queue
corr_id = str(uuid.uuid4())
channel.basic_publish(
    exchange="",
    routing_key="rpc_queue",
    properties=pika.BasicProperties(reply_to=callback_queue, correlation_id=corr_id),
    body=b"7",
)
for method, props, body in channel.consume(callback_queue, auto_ack=True, inactivity_timeout=5):
    if props.correlation_id == corr_id:
        print(f" [.] square(7) = {body.decode()}")
        break
connection.close()

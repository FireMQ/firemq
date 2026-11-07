"""RabbitMQ tutorial scenarios run with pika against FireMQ.

Run: AMQP_URL=amqp://guest:guest@localhost:5672/ uv run --with pika --with pytest pytest compat/python
"""

import os
import threading
import time
import uuid

import pika
import pytest

URL = os.environ.get("AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")


@pytest.fixture
def ch():
    conn = pika.BlockingConnection(pika.URLParameters(URL))
    channel = conn.channel()
    yield channel
    if conn.is_open:
        conn.close()


def name(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def drain(ch, queue, limit=100):
    out = []
    for _ in range(limit):
        method, props, body = ch.basic_get(queue, auto_ack=True)
        if method is None:
            break
        out.append(body)
    return out


def test_hello_world(ch):
    q = name("hello")
    ch.queue_declare(q)
    ch.basic_publish(exchange="", routing_key=q, body=b"Hello World!")
    got = []

    def on_msg(channel, method, props, body):
        got.append(body)
        channel.stop_consuming()

    ch.basic_consume(q, on_msg, auto_ack=True)
    ch.connection.call_later(5, ch.stop_consuming)
    ch.start_consuming()
    assert got == [b"Hello World!"]


def test_work_queues_fair_dispatch(ch):
    q = name("task_queue")
    ch.queue_declare(q, durable=True)
    for i in range(5):
        ch.basic_publish(
            "", q, f"task {i}".encode(),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
    ch.basic_qos(prefetch_count=1)
    got = []

    def on_msg(channel, method, props, body):
        got.append(body)
        channel.basic_ack(method.delivery_tag)
        if len(got) == 5:
            channel.stop_consuming()

    ch.basic_consume(q, on_msg)
    ch.connection.call_later(5, ch.stop_consuming)
    ch.start_consuming()
    assert got == [f"task {i}".encode() for i in range(5)]


def test_publish_subscribe_fanout(ch):
    ex = name("logs")
    ch.exchange_declare(ex, exchange_type="fanout")
    queues = []
    for _ in range(3):
        r = ch.queue_declare("", exclusive=True)
        queues.append(r.method.queue)
        ch.queue_bind(r.method.queue, ex)
    ch.basic_publish(ex, "", b"broadcast")
    time.sleep(0.1)
    for q in queues:
        assert drain(ch, q) == [b"broadcast"]


def test_routing_direct(ch):
    ex = name("direct_logs")
    ch.exchange_declare(ex, exchange_type="direct")
    errors = ch.queue_declare("", exclusive=True).method.queue
    everything = ch.queue_declare("", exclusive=True).method.queue
    ch.queue_bind(errors, ex, routing_key="error")
    for sev in ("info", "warning", "error"):
        ch.queue_bind(everything, ex, routing_key=sev)
    for sev in ("info", "warning", "error"):
        ch.basic_publish(ex, sev, sev.encode())
    time.sleep(0.1)
    assert drain(ch, errors) == [b"error"]
    assert drain(ch, everything) == [b"info", b"warning", b"error"]


def test_topics(ch):
    ex = name("topic_logs")
    ch.exchange_declare(ex, exchange_type="topic")
    q1 = ch.queue_declare("", exclusive=True).method.queue
    q2 = ch.queue_declare("", exclusive=True).method.queue
    ch.queue_bind(q1, ex, "*.orange.*")
    ch.queue_bind(q2, ex, "*.*.rabbit")
    ch.queue_bind(q2, ex, "lazy.#")
    for key in ("quick.orange.rabbit", "lazy.orange.elephant", "quick.orange.fox", "lazy.brown.fox", "quick.brown.fox"):
        ch.basic_publish(ex, key, key.encode())
    time.sleep(0.1)
    assert drain(ch, q1) == [b"quick.orange.rabbit", b"lazy.orange.elephant", b"quick.orange.fox"]
    assert drain(ch, q2) == [b"quick.orange.rabbit", b"lazy.orange.elephant", b"lazy.brown.fox"]


def test_rpc(ch):
    rpc_queue = name("rpc_queue")
    ch.queue_declare(rpc_queue)

    def server():
        conn = pika.BlockingConnection(pika.URLParameters(URL))
        c = conn.channel()

        def on_request(channel, method, props, body):
            n = int(body)
            channel.basic_publish(
                "", props.reply_to, str(n * 2).encode(),
                properties=pika.BasicProperties(correlation_id=props.correlation_id),
            )
            channel.basic_ack(method.delivery_tag)
            channel.stop_consuming()

        c.basic_consume(rpc_queue, on_request)
        conn.call_later(5, c.stop_consuming)
        c.start_consuming()
        conn.close()

    t = threading.Thread(target=server)
    t.start()
    callback_queue = ch.queue_declare("", exclusive=True).method.queue
    corr = str(uuid.uuid4())
    response = []

    def on_response(channel, method, props, body):
        if props.correlation_id == corr:
            response.append(body)
            channel.stop_consuming()

    ch.basic_consume(callback_queue, on_response, auto_ack=True)
    ch.basic_publish(
        "", rpc_queue, b"21",
        properties=pika.BasicProperties(reply_to=callback_queue, correlation_id=corr),
    )
    ch.connection.call_later(5, ch.stop_consuming)
    ch.start_consuming()
    t.join()
    assert response == [b"42"]


def test_publisher_confirms(ch):
    q = name("confirmed")
    ch.queue_declare(q, durable=True)
    ch.confirm_delivery()
    for i in range(50):
        ch.basic_publish("", q, f"m{i}".encode(), mandatory=True)
    assert len(drain(ch, q)) == 50


def test_mandatory_unroutable_is_returned(ch):
    ch.confirm_delivery()
    with pytest.raises(pika.exceptions.UnroutableError):
        ch.basic_publish("", name("nowhere"), b"lost", mandatory=True)


def test_redeclare_with_different_args_is_rejected(ch):
    q = name("strict")
    ch.queue_declare(q, durable=False)
    with pytest.raises(pika.exceptions.ChannelClosedByBroker) as e:
        ch.queue_declare(q, durable=True)
    assert e.value.reply_code == 406


def test_headers_exchange(ch):
    ex = name("hdr")
    ch.exchange_declare(ex, exchange_type="headers")
    q = ch.queue_declare("", exclusive=True).method.queue
    ch.queue_bind(q, ex, arguments={"x-match": "all", "format": "pdf", "type": "report"})
    ch.basic_publish(ex, "", b"yes", properties=pika.BasicProperties(headers={"format": "pdf", "type": "report"}))
    ch.basic_publish(ex, "", b"no", properties=pika.BasicProperties(headers={"format": "pdf"}))
    time.sleep(0.1)
    assert drain(ch, q) == [b"yes"]


def test_dead_letter_on_reject(ch):
    dlx = name("dlx")
    ch.exchange_declare(dlx, exchange_type="fanout")
    dead = ch.queue_declare("", exclusive=True).method.queue
    ch.queue_bind(dead, dlx)
    work = name("work")
    ch.queue_declare(work, arguments={"x-dead-letter-exchange": dlx})
    ch.basic_publish("", work, b"bad")
    time.sleep(0.1)
    method, _, _ = ch.basic_get(work)
    ch.basic_reject(method.delivery_tag, requeue=False)
    time.sleep(0.1)
    method, props, body = ch.basic_get(dead, auto_ack=True)
    assert body == b"bad"
    assert props.headers["x-death"][0]["reason"] == "rejected"

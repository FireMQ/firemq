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


def test_message_ttl_dead_letters_expired_messages(ch):
    dlx = name("dlx")
    ch.exchange_declare(dlx, exchange_type="fanout")
    dead = ch.queue_declare("", exclusive=True).method.queue
    ch.queue_bind(dead, dlx)
    q = name("ttl")
    ch.queue_declare(q, arguments={"x-message-ttl": 100, "x-dead-letter-exchange": dlx})
    ch.basic_publish("", q, b"short-lived")
    time.sleep(0.5)
    assert drain(ch, q) == []
    method, props, body = ch.basic_get(dead, auto_ack=True)
    assert body == b"short-lived"
    assert props.headers["x-death"][0]["reason"] == "expired"


def test_per_message_expiration(ch):
    q = name("expiring")
    ch.queue_declare(q)
    ch.basic_publish("", q, b"gone", properties=pika.BasicProperties(expiration="50"))
    ch.basic_publish("", q, b"kept")
    time.sleep(0.3)
    # Expired messages at the head are dropped when the queue is read.
    assert drain(ch, q) == [b"kept"]


def test_max_length_drops_oldest(ch):
    q = name("bounded")
    ch.queue_declare(q, arguments={"x-max-length": 3})
    for i in range(6):
        ch.basic_publish("", q, f"m{i}".encode())
    time.sleep(0.1)
    assert drain(ch, q) == [b"m3", b"m4", b"m5"]


def test_reject_publish_overflow_nacks(ch):
    q = name("full")
    ch.queue_declare(q, arguments={"x-max-length": 1, "x-overflow": "reject-publish"})
    ch.confirm_delivery()
    ch.basic_publish("", q, b"fits")
    with pytest.raises(pika.exceptions.NackError):
        ch.basic_publish("", q, b"rejected")


def test_priority_queue(ch):
    q = name("prio")
    ch.queue_declare(q, arguments={"x-max-priority": 10})
    for body, prio in [(b"low", 1), (b"high", 9), (b"mid", 5)]:
        ch.basic_publish("", q, body, properties=pika.BasicProperties(priority=prio))
    time.sleep(0.1)
    assert drain(ch, q) == [b"high", b"mid", b"low"]


def test_single_active_consumer(ch):
    q = name("sac")
    ch.queue_declare(q, arguments={"x-single-active-consumer": True})
    got = {"a": 0, "b": 0}

    def make(tag):
        def cb(channel, method, props, body):
            got[tag] += 1
            channel.basic_ack(method.delivery_tag)
            if got["a"] + got["b"] == 5:
                channel.stop_consuming()
        return cb

    ch.basic_consume(q, make("a"))
    ch.basic_consume(q, make("b"))
    for i in range(5):
        ch.basic_publish("", q, b"x")
    ch.connection.call_later(5, ch.stop_consuming)
    ch.start_consuming()
    assert sorted(got.values()) == [0, 5]


def test_queue_expires_when_unused(ch):
    q = name("ephemeral")
    ch.queue_declare(q, arguments={"x-expires": 200})
    time.sleep(0.6)
    with pytest.raises(pika.exceptions.ChannelClosedByBroker) as e:
        ch.queue_declare(q, passive=True)
    assert e.value.reply_code == 404


def test_alternate_exchange(ch):
    ae = name("ae")
    ch.exchange_declare(ae, exchange_type="fanout")
    unrouted = ch.queue_declare("", exclusive=True).method.queue
    ch.queue_bind(unrouted, ae)
    ex = name("main")
    ch.exchange_declare(ex, exchange_type="direct", arguments={"alternate-exchange": ae})
    ch.basic_publish(ex, "nobody-listens", b"caught")
    time.sleep(0.1)
    assert drain(ch, unrouted) == [b"caught"]


def test_exchange_to_exchange_binding(ch):
    upstream = name("upstream")
    downstream = name("downstream")
    ch.exchange_declare(upstream, exchange_type="topic")
    ch.exchange_declare(downstream, exchange_type="fanout")
    ch.exchange_bind(downstream, upstream, routing_key="audit.#")
    q = ch.queue_declare("", exclusive=True).method.queue
    ch.queue_bind(q, downstream)
    ch.basic_publish(upstream, "audit.login", b"yes")
    ch.basic_publish(upstream, "metrics.cpu", b"no")
    time.sleep(0.1)
    assert drain(ch, q) == [b"yes"]


def test_transactions(ch):
    q = name("tx")
    ch.queue_declare(q)
    ch.tx_select()
    ch.basic_publish("", q, b"rolled back")
    ch.tx_rollback()
    ch.basic_publish("", q, b"committed")
    ch.tx_commit()
    assert drain(ch, q) == [b"committed"]


def test_recover_redelivers_unacked(ch):
    q = name("recover")
    ch.queue_declare(q)
    ch.basic_publish("", q, b"again")
    time.sleep(0.05)
    method, _, _ = ch.basic_get(q)
    assert not method.redelivered
    ch.basic_recover(requeue=True)
    method, _, body = ch.basic_get(q, auto_ack=True)
    assert body == b"again" and method.redelivered


def test_consumer_cancel_notification_on_queue_delete(ch):
    q = name("doomed")
    ch.queue_declare(q)
    cancelled = []
    ch.basic_consume(q, lambda *a: None, auto_ack=True)
    ch.add_on_cancel_callback(lambda frame: cancelled.append(frame))
    other = ch.connection.channel()
    other.queue_delete(q)
    ch.connection.process_data_events(time_limit=1)
    assert len(cancelled) == 1

import threading

import pytest

import firemq


def test_hello_world(client, queue):
    client.publish("", queue, "hello")
    with client.subscribe(queue, prefetch=10) as sub:
        msg = sub.next(timeout=3)
        assert msg is not None
        assert msg.text == "hello"
        assert msg.body == b"hello"
        assert msg.routing_key == queue
        assert msg.exchange == ""
        assert not msg.redelivered
        msg.ack()
        assert sub.next(timeout=0.05) is None
    assert client.get(queue) is None


def test_iterate_in_order(client, queue):
    for i in range(20):
        client.publish("", queue, f"m{i}".encode())
    got = []
    with client.subscribe(queue, prefetch=5) as sub:
        for msg in sub:
            got.append(msg.text)
            msg.ack()
            if len(got) == 20:
                break
    assert got == [f"m{i}" for i in range(20)]


def test_iteration_stops_when_cancelled(client, queue):
    sub = client.subscribe(queue)
    timer = threading.Timer(0.2, sub.cancel)
    timer.start()
    assert list(sub) == []
    timer.join()
    with pytest.raises(firemq.ClosedError):
        sub.next(timeout=0)


def test_properties_and_headers(client, queue):
    headers = {"s": "x", "int": 42, "neg": -7, "float": 1.5, "bool": True,
               "list": [1, "two"], "map": {"k": "v"}, "nil": None}
    client.publish(
        "", queue, b"\x00\xff",
        headers=headers,
        content_type="application/octet-stream",
        correlation_id="c-1",
        type="order.created",
        priority=3,
        expiration=60000,
        timestamp=1767225600,
    )
    msg = client.get(queue, no_ack=True)
    assert msg.body == b"\x00\xff"
    assert msg.headers == headers
    assert msg.properties == {
        "content_type": "application/octet-stream",
        "correlation_id": "c-1",
        "type": "order.created",
        "priority": 3,
        "expiration": 60000,
        "timestamp": 1767225600,
    }
    with pytest.raises(TypeError):
        client.publish("", queue, "x", colour="red")


def test_publish_json(client, queue):
    client.publish_json("", queue, {"id": 7, "items": ["a"]})
    msg = client.get(queue, no_ack=True)
    assert msg.json() == {"id": 7, "items": ["a"]}
    assert msg.properties["content_type"] == "application/json"


def test_nack_requeues(client, queue):
    client.publish("", queue, "again")
    msg = client.get(queue)
    msg.nack(requeue=True)
    msg = client.get(queue)
    assert msg.redelivered
    msg.ack()
    assert client.get(queue) is None


def test_topology_and_counts(client, queue):
    ex = queue + ".ex"
    client.declare_exchange(ex, "topic", durable=False, auto_delete=False)
    client.bind_queue(queue, ex, "*.critical.#")
    for key in ["kern.critical", "kern.info", "kern.critical.disk"]:
        client.publish(ex, key, key)
    assert client.purge_queue(queue) == 2
    client.unbind_queue(queue, ex, "*.critical.#")
    client.publish(ex, "kern.critical", "x")
    assert client.get(queue) is None
    client.delete_exchange(ex)

    tmp = client.declare_queue(exclusive=True, auto_delete=True, durable=False)
    assert tmp.name.startswith("amq.gen-")
    assert tmp.messages == 0
    client.publish("", tmp.name, "x")
    assert client.delete_queue(tmp.name) == 1


def test_errors(url, client, queue):
    with pytest.raises(firemq.ServerError) as e:
        firemq.connect(url.replace("guest:guest", "guest:wrong"))
    assert e.value.code == 403

    with pytest.raises(firemq.UnroutableError):
        client.publish("", queue + ".nowhere", "lost", mandatory=True)

    with pytest.raises(firemq.ServerError) as e:
        client.declare_queue(queue, durable=False)
    assert e.value.code == 406

    with pytest.raises(firemq.ServerError) as e:
        client.bind_queue(queue, queue + ".missing")
    assert e.value.code == 404

    full = queue + ".full"
    client.declare_queue(full, arguments={"x-max-length": 1, "x-overflow": "reject-publish"})
    client.publish("", full, "fits")
    with pytest.raises(firemq.NackedError):
        client.publish("", full, "rejected")
    client.delete_queue(full)

    with pytest.raises(firemq.OtherError):
        firemq.connect("http://nope")


def test_close(url):
    client = firemq.connect(url)
    assert not client.closed
    client.close()
    assert client.closed
    with pytest.raises(firemq.ClosedError):
        client.declare_queue("py.after-close")

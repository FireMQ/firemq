"""FireMQ client for Python, built on the Rust client.

>>> import firemq
>>> with firemq.connect("fire://guest:guest@localhost:5673") as client:
...     client.declare_queue("jobs")
...     client.publish("", "jobs", "hello", headers={"attempt": 1})
...     with client.subscribe("jobs") as sub:
...         for msg in sub:
...             print(msg.text)
...             msg.ack()
...             break

Calls block until the broker answers; they release the GIL while waiting. Queues and
exchanges are durable and messages persistent by default, and ``publish`` waits for the
broker's confirm.
"""

from __future__ import annotations

import json as _json
from typing import Any, Dict, Iterator, Mapping, Optional, Union

from ._ffi import firemq_client_ffi as _ffi

__all__ = [
    "Client",
    "ClosedError",
    "FireMQError",
    "Message",
    "NackedError",
    "OtherError",
    "QueueInfo",
    "ServerError",
    "Subscription",
    "UnroutableError",
    "connect",
]

__version__ = "0.1.0"

#: Base class of every error. Each variant below is a subclass.
FireMQError = _ffi.FireMqError
#: The broker refused a request; ``code`` is an AMQP reply code such as 403, 404 or 406.
ServerError = _ffi.FireMqError.Server
#: A mandatory publish matched no queue.
UnroutableError = _ffi.FireMqError.Unroutable
#: The broker rejected a publish, for example because a queue was full.
NackedError = _ffi.FireMqError.Nacked
#: The connection or subscription is closed.
ClosedError = _ffi.FireMqError.Closed
#: Anything else: I/O failures, invalid URLs or JSON, protocol errors.
OtherError = _ffi.FireMqError.Other

#: What ``declare_queue`` returns: ``name``, ``messages`` and ``consumers``.
QueueInfo = _ffi.QueueInfo

DEFAULT_URL = "fire://guest:guest@localhost:5673"

# Property names as the protocol spells them, and as the Rust record spells them.
_PROPERTY_FIELDS = {
    "content_type": "content_type",
    "content_encoding": "content_encoding",
    "correlation_id": "correlation_id",
    "reply_to": "reply_to",
    "expiration": "expiration",
    "message_id": "message_id",
    "timestamp": "timestamp",
    "type": "message_type",
    "user_id": "user_id",
    "app_id": "app_id",
    "priority": "priority",
}

Body = Union[str, bytes, bytearray, memoryview]


def _json_or_none(value: Optional[Mapping[str, Any]]) -> Optional[str]:
    return None if value is None else _json.dumps(dict(value))


def _bytes(body: Body) -> bytes:
    if isinstance(body, str):
        return body.encode("utf-8")
    return bytes(body)


def connect(url: str = DEFAULT_URL) -> "Client":
    """Connects with a URL such as ``fire://user:pass@host:5673/vhost`` (``fires://`` for TLS)."""
    return Client(_ffi.Client.connect(url))


class Client:
    """A connection to FireMQ. Safe to share between threads; use it as a context manager to
    close it."""

    def __init__(self, inner: "_ffi.Client") -> None:
        self._inner = inner

    @classmethod
    def connect(cls, url: str = DEFAULT_URL) -> "Client":
        return connect(url)

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Closes the connection. Unacknowledged messages return to their queues."""
        self._inner.close()

    @property
    def closed(self) -> bool:
        return self._inner.is_closed()

    def declare_queue(
        self,
        name: str = "",
        *,
        durable: bool = True,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: Optional[Mapping[str, Any]] = None,
    ) -> QueueInfo:
        """Declares a queue. An empty name asks the broker to generate one. ``arguments`` are
        RabbitMQ queue arguments such as ``{"x-max-length": 1000}``."""
        return self._inner.declare_queue(
            name, durable, exclusive, auto_delete, _json_or_none(arguments)
        )

    def delete_queue(self, name: str) -> int:
        """Deletes a queue and returns how many messages it held."""
        return self._inner.delete_queue(name)

    def purge_queue(self, name: str) -> int:
        """Removes every ready message and returns how many were removed."""
        return self._inner.purge_queue(name)

    def declare_exchange(
        self,
        name: str,
        type: str = "direct",
        *,
        durable: bool = True,
        auto_delete: bool = False,
        arguments: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Declares an exchange of type ``direct``, ``fanout``, ``topic`` or ``headers``."""
        self._inner.declare_exchange(name, type, durable, auto_delete, _json_or_none(arguments))

    def delete_exchange(self, name: str, *, if_unused: bool = False) -> None:
        self._inner.delete_exchange(name, if_unused)

    def bind_queue(
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        *,
        arguments: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Binds ``queue`` to ``exchange`` with a routing key or pattern."""
        self._inner.bind_queue(queue, exchange, routing_key, _json_or_none(arguments))

    def unbind_queue(self, queue: str, exchange: str, routing_key: str = "") -> None:
        self._inner.unbind_queue(queue, exchange, routing_key)

    def bind_exchange(self, destination: str, source: str, routing_key: str = "") -> None:
        """Routes messages published to ``source`` on to ``destination``."""
        self._inner.bind_exchange(destination, source, routing_key)

    def publish(
        self,
        exchange: str,
        routing_key: str,
        body: Body,
        *,
        headers: Optional[Mapping[str, Any]] = None,
        persistent: bool = True,
        mandatory: bool = False,
        confirm: bool = True,
        properties: Optional[Mapping[str, Any]] = None,
        **property_kwargs: Any,
    ) -> None:
        """Publishes a message; ``str`` bodies are sent as UTF-8. Use exchange ``""`` to send
        straight to the queue named by ``routing_key``.

        Properties use the protocol's names and may be given as keywords or as a
        ``properties`` mapping: ``content_type``, ``content_encoding``, ``correlation_id``,
        ``reply_to``, ``expiration`` (milliseconds), ``message_id``, ``timestamp``, ``type``,
        ``user_id``, ``app_id`` and ``priority``. Header values may be any JSON value.

        Raises :class:`UnroutableError` for a ``mandatory`` message no queue accepted and
        :class:`NackedError` when the broker rejects it.
        """
        props = dict(properties or {})
        props.update(property_kwargs)
        unknown = set(props) - set(_PROPERTY_FIELDS)
        if unknown:
            raise TypeError(f"unknown message properties: {', '.join(sorted(unknown))}")
        record = _ffi.MessageProperties(
            **{_PROPERTY_FIELDS[k]: v for k, v in props.items() if v is not None}
        )
        options = _ffi.PublishOptions(
            persistent=persistent,
            mandatory=mandatory,
            confirm=confirm,
            properties=record,
            headers_json=_json_or_none(headers),
        )
        self._inner.publish(exchange, routing_key, _bytes(body), options)

    def publish_json(self, exchange: str, routing_key: str, value: Any, **kwargs: Any) -> None:
        """Publishes ``value`` encoded as JSON with content type ``application/json``."""
        kwargs.setdefault("content_type", "application/json")
        self.publish(exchange, routing_key, _json.dumps(value), **kwargs)

    def subscribe(self, queue: str, *, prefetch: int = 100, no_ack: bool = False) -> "Subscription":
        """Starts consuming. ``prefetch`` caps unacknowledged deliveries; with ``no_ack``
        messages count as acknowledged when sent."""
        return Subscription(self._inner.subscribe(queue, prefetch, no_ack))

    def get(self, queue: str, *, no_ack: bool = False) -> Optional["Message"]:
        """Takes one message from a queue, or returns ``None`` when it is empty."""
        d = self._inner.get(queue, no_ack)
        return None if d is None else Message(d)


class Subscription:
    """Messages from a queue. Iterate over it, or call :meth:`next`. Use it as a context
    manager to cancel it."""

    # How long each wait in the native library lasts, so Ctrl+C is handled promptly.
    _POLL_MS = 500

    def __init__(self, inner: "_ffi.Subscription") -> None:
        self._inner = inner

    def __enter__(self) -> "Subscription":
        return self

    def __exit__(self, *exc: object) -> None:
        self.cancel()

    def __iter__(self) -> Iterator["Message"]:
        """Yields messages until the subscription ends."""
        while True:
            try:
                msg = self.next(None)
            except ClosedError:
                return
            if msg is not None:
                yield msg

    def next(self, timeout: Optional[float] = 1.0) -> Optional["Message"]:
        """Waits up to ``timeout`` seconds for a message (forever with ``None``) and returns
        ``None`` on timeout. Raises :class:`ClosedError` once the subscription has ended."""
        if timeout is not None:
            d = self._inner.next(max(0, int(timeout * 1000)))
            return None if d is None else Message(d)
        while True:
            d = self._inner.next(self._POLL_MS)
            if d is not None:
                return Message(d)

    def cancel(self) -> None:
        """Stops the subscription. Messages already received can still be acknowledged."""
        self._inner.cancel()


class Message:
    """A delivered message."""

    __slots__ = ("_d", "_body", "_headers", "_properties")

    def __init__(self, delivery: "_ffi.Delivery") -> None:
        self._d = delivery
        self._body: Optional[bytes] = None
        self._headers: Optional[Dict[str, Any]] = None
        self._properties: Optional[Dict[str, Any]] = None

    @property
    def body(self) -> bytes:
        if self._body is None:
            self._body = self._d.body()
        return self._body

    @property
    def text(self) -> str:
        """The body decoded as UTF-8."""
        return self.body.decode("utf-8")

    def json(self) -> Any:
        """The body decoded as JSON."""
        return _json.loads(self.body)

    @property
    def exchange(self) -> str:
        return self._d.exchange()

    @property
    def routing_key(self) -> str:
        return self._d.routing_key()

    @property
    def redelivered(self) -> bool:
        """True when the message was delivered before and not acknowledged."""
        return self._d.redelivered()

    @property
    def delivery_tag(self) -> int:
        return self._d.delivery_tag()

    @property
    def headers(self) -> Dict[str, Any]:
        """Application headers. Binary values arrive base64-encoded."""
        if self._headers is None:
            self._headers = _json.loads(self._d.headers_json())
        return self._headers

    @property
    def properties(self) -> Dict[str, Any]:
        """The properties that are set, keyed by their protocol names (``content_type``,
        ``type``, ``priority``...)."""
        if self._properties is None:
            p = self._d.properties()
            self._properties = {
                name: getattr(p, field)
                for name, field in _PROPERTY_FIELDS.items()
                if getattr(p, field) is not None
            }
        return self._properties

    def ack(self) -> None:
        """Acknowledges the message. Does nothing when it is already settled."""
        self._d.ack()

    def nack(self, requeue: bool = True) -> None:
        """Rejects the message. With ``requeue`` it is delivered again; otherwise it is
        dead-lettered when the queue has a dead-letter exchange, and dropped if not."""
        self._d.nack(requeue)

    def __repr__(self) -> str:
        return (
            f"Message(exchange={self.exchange!r}, routing_key={self.routing_key!r}, "
            f"redelivered={self.redelivered}, body={len(self.body)} bytes)"
        )

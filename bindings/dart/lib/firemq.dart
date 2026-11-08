/// FireMQ client for Dart, over the C API of the Rust client.
///
/// ```dart
/// final client = Client.connect('fire://guest:guest@localhost:5673');
/// client.declareQueue('jobs');
/// client.publishString('', 'jobs', 'hello', headers: {'attempt': 1});
/// final sub = client.subscribe('jobs', prefetch: 10);
/// final msg = sub.next(timeout: const Duration(seconds: 5));
/// print(msg?.text);
/// msg?.ack();
/// client.close();
/// ```
///
/// Calls are synchronous: they block the calling isolate until the broker answers. Run a
/// consumer in its own isolate to keep an event loop responsive. Native handles are released
/// when their Dart objects are garbage collected, or at once by [Client.close],
/// [Subscription.cancel], [Message.ack] and [Message.nack].
library;

import 'dart:convert';
import 'dart:ffi';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'src/native.dart' as n;

/// The URL [Client.connect] uses by default.
const defaultUrl = 'fire://guest:guest@localhost:5673';

/// The native library's version.
String get nativeVersion => n.version().toDartString();

/// Base class of every FireMQ error.
sealed class FireMQException implements Exception {
  FireMQException(this.message, this.code);

  final String message;

  /// The AMQP reply code, for example 403, 404, 406 or 312, or 0 when there is none.
  final int code;

  @override
  String toString() => '$runtimeType: $message';
}

/// The broker refused a request.
final class ServerException extends FireMQException {
  ServerException(super.message, super.code);
}

/// A mandatory publish matched no queue.
final class UnroutableException extends FireMQException {
  UnroutableException(super.message, super.code);
}

/// The broker rejected a publish, for example because a queue was full.
final class NackedException extends FireMQException {
  NackedException(super.message, super.code);
}

/// The connection or subscription is closed.
final class ClosedException extends FireMQException {
  ClosedException(super.message, super.code);
}

/// Anything else: I/O failures, invalid URLs or JSON, protocol errors.
final class OtherException extends FireMQException {
  OtherException(super.message, super.code);
}

void _check(int status) {
  if (status == n.firemqOk) return;
  final p = n.lastError();
  final message = p == nullptr ? 'unknown error' : p.toDartString();
  final code = n.lastErrorCode();
  throw switch (status) {
    n.firemqErrServer => ServerException(message, code),
    n.firemqErrUnroutable => UnroutableException(message, code),
    n.firemqErrNacked => NackedException(message, code),
    n.firemqErrClosed => ClosedException(message, code),
    _ => OtherException(message, code),
  };
}

Pointer<Utf8> _str(String s, Arena arena) => s.toNativeUtf8(allocator: arena);

Pointer<Utf8> _json(Map<String, Object?>? value, Arena arena) =>
    value == null ? nullptr : jsonEncode(value).toNativeUtf8(allocator: arena);

/// What [Client.declareQueue] reports.
final class QueueInfo {
  const QueueInfo(this.name, this.messages, this.consumers);

  final String name;
  final int messages;
  final int consumers;
}

/// A connection to FireMQ.
final class Client implements Finalizable {
  Client._(this._handle) {
    _finalizer.attach(this, _handle.cast(), detach: this);
  }

  static final _finalizer = NativeFinalizer(n.freeClient);

  Pointer<n.FiremqClient> _handle;

  /// Connects with a URL such as `fire://user:pass@host:5673/vhost`.
  static Client connect([String url = defaultUrl]) => using((arena) {
        final out = arena<Pointer<n.FiremqClient>>();
        _check(n.connect(_str(url, arena), out));
        return Client._(out.value);
      });

  Pointer<n.FiremqClient> get _h {
    if (_handle == nullptr) throw ClosedException('the client was closed', 0);
    return _handle;
  }

  /// Closes the connection gracefully and releases the client. Unacknowledged messages
  /// return to their queues.
  void close() {
    if (_handle == nullptr) return;
    try {
      _check(n.close(_handle));
    } finally {
      _finalizer.detach(this);
      n.clientFree(_handle);
      _handle = nullptr;
    }
  }

  bool get isClosed => _handle == nullptr;

  /// Declares a queue. An empty name asks the broker to generate one.
  QueueInfo declareQueue(
    String name, {
    bool durable = true,
    bool exclusive = false,
    bool autoDelete = false,
    Map<String, Object?>? arguments,
  }) =>
      using((arena) {
        final outName = arena<Pointer<Utf8>>();
        final outMessages = arena<Uint64>();
        final outConsumers = arena<Uint32>();
        _check(n.declareQueue(_h, _str(name, arena), durable, exclusive, autoDelete,
            _json(arguments, arena), outName, outMessages, outConsumers));
        final queue = outName.value.toDartString();
        n.stringFree(outName.value);
        return QueueInfo(queue, outMessages.value, outConsumers.value);
      });

  /// Deletes a queue and returns how many messages it held.
  int deleteQueue(String name) => using((arena) {
        final out = arena<Uint64>();
        _check(n.deleteQueue(_h, _str(name, arena), out));
        return out.value;
      });

  /// Removes every ready message and returns how many were removed.
  int purgeQueue(String name) => using((arena) {
        final out = arena<Uint64>();
        _check(n.purgeQueue(_h, _str(name, arena), out));
        return out.value;
      });

  /// Declares an exchange of type `direct`, `fanout`, `topic` or `headers`.
  void declareExchange(String name, String type,
          {bool durable = true, bool autoDelete = false, Map<String, Object?>? arguments}) =>
      using((arena) => _check(n.declareExchange(
          _h, _str(name, arena), _str(type, arena), durable, autoDelete, _json(arguments, arena))));

  void deleteExchange(String name, {bool ifUnused = false}) =>
      using((arena) => _check(n.deleteExchange(_h, _str(name, arena), ifUnused)));

  /// Binds [queue] to [exchange] with a routing key or pattern.
  void bindQueue(String queue, String exchange,
          [String routingKey = '', Map<String, Object?>? arguments]) =>
      using((arena) => _check(n.bindQueue(_h, _str(queue, arena), _str(exchange, arena),
          _str(routingKey, arena), _json(arguments, arena))));

  void unbindQueue(String queue, String exchange, [String routingKey = '']) =>
      using((arena) => _check(
          n.unbindQueue(_h, _str(queue, arena), _str(exchange, arena), _str(routingKey, arena))));

  /// Routes messages published to [source] on to [destination].
  void bindExchange(String destination, String source, [String routingKey = '']) =>
      using((arena) => _check(n.bindExchange(
          _h, _str(destination, arena), _str(source, arena), _str(routingKey, arena))));

  /// Publishes [body] and, by default, waits for the broker's confirm. [properties] uses the
  /// protocol's keys: `content_type`, `correlation_id`, `type`, `priority`, `expiration`...
  void publish(
    String exchange,
    String routingKey,
    List<int> body, {
    Map<String, Object?>? headers,
    Map<String, Object?>? properties,
    bool persistent = true,
    bool mandatory = false,
    bool confirm = true,
  }) =>
      using((arena) {
        final data = arena<Uint8>(body.isEmpty ? 1 : body.length);
        data.asTypedList(body.length).setAll(0, body);
        final options = arena<n.FiremqPublishOptions>();
        options.ref
          ..persistent = persistent
          ..mandatory = mandatory
          ..confirm = confirm
          ..propertiesJson = _json(properties, arena)
          ..headersJson = _json(headers, arena);
        _check(n.publish(
            _h, _str(exchange, arena), _str(routingKey, arena), data, body.length, options));
      });

  /// Publishes UTF-8 text. See [publish].
  void publishString(
    String exchange,
    String routingKey,
    String text, {
    Map<String, Object?>? headers,
    Map<String, Object?>? properties,
    bool persistent = true,
    bool mandatory = false,
    bool confirm = true,
  }) =>
      publish(exchange, routingKey, utf8.encode(text),
          headers: headers,
          properties: properties,
          persistent: persistent,
          mandatory: mandatory,
          confirm: confirm);

  /// Starts consuming. [prefetch] caps unacknowledged deliveries.
  Subscription subscribe(String queue, {int prefetch = 100, bool noAck = false}) => using((arena) {
        final out = arena<Pointer<n.FiremqSubscription>>();
        _check(n.subscribe(_h, _str(queue, arena), prefetch, noAck, out));
        return Subscription._(out.value);
      });

  /// Takes one message from a queue, or returns null when it is empty.
  Message? get(String queue, {bool noAck = false}) => using((arena) {
        final out = arena<Pointer<n.FiremqMessage>>();
        _check(n.get(_h, _str(queue, arena), noAck, out));
        return out.value == nullptr ? null : Message._(out.value);
      });
}

/// Messages from a queue.
final class Subscription implements Finalizable {
  Subscription._(this._handle) {
    _finalizer.attach(this, _handle.cast(), detach: this);
  }

  static final _finalizer = NativeFinalizer(n.freeSubscription);

  Pointer<n.FiremqSubscription> _handle;

  Pointer<n.FiremqSubscription> get _h {
    if (_handle == nullptr) throw ClosedException('the subscription was cancelled', 0);
    return _handle;
  }

  /// Waits up to [timeout] for a message and returns null on timeout. Throws
  /// [ClosedException] once the subscription has ended.
  Message? next({Duration timeout = const Duration(seconds: 1)}) => using((arena) {
        final out = arena<Pointer<n.FiremqMessage>>();
        _check(n.next(_h, timeout.inMilliseconds, out));
        return out.value == nullptr ? null : Message._(out.value);
      });

  /// Messages until the subscription ends.
  Iterable<Message> get messages sync* {
    while (true) {
      final Message? msg;
      try {
        msg = next(timeout: const Duration(milliseconds: 500));
      } on ClosedException {
        return;
      }
      if (msg != null) yield msg;
    }
  }

  /// Stops the subscription and releases it. Messages already received can still be
  /// acknowledged.
  void cancel() {
    if (_handle == nullptr) return;
    try {
      _check(n.cancel(_handle));
    } finally {
      _finalizer.detach(this);
      n.subscriptionFree(_handle);
      _handle = nullptr;
    }
  }
}

/// A delivered message. Its fields are copied into Dart when it arrives.
final class Message implements Finalizable {
  Message._(this._handle)
      : exchange = n.messageExchange(_handle).toDartString(),
        routingKey = n.messageRoutingKey(_handle).toDartString(),
        redelivered = n.messageRedelivered(_handle),
        deliveryTag = n.messageDeliveryTag(_handle),
        headers = (jsonDecode(n.messageHeadersJson(_handle).toDartString()) as Map)
            .cast<String, Object?>(),
        properties = (jsonDecode(n.messagePropertiesJson(_handle).toDartString()) as Map)
            .cast<String, Object?>(),
        body = _copyBody(_handle) {
    _finalizer.attach(this, _handle.cast(), detach: this);
  }

  static final _finalizer = NativeFinalizer(n.freeMessage);

  static Uint8List _copyBody(Pointer<n.FiremqMessage> handle) => using((arena) {
        final len = arena<Size>();
        final data = n.messageBody(handle, len);
        return Uint8List.fromList(data.asTypedList(len.value));
      });

  Pointer<n.FiremqMessage> _handle;

  final Uint8List body;
  final String exchange;
  final String routingKey;

  /// True when the message was delivered before and not acknowledged.
  final bool redelivered;
  final int deliveryTag;

  /// Application headers. Binary values arrive base64-encoded.
  final Map<String, Object?> headers;

  /// The properties that are set, keyed by their protocol names.
  final Map<String, Object?> properties;

  /// The body decoded as UTF-8.
  String get text => utf8.decode(body);

  /// Acknowledges the message. Does nothing when it is already settled.
  void ack() => _settle((h) => n.ack(h));

  /// Rejects the message. With [requeue] it is delivered again; otherwise it is
  /// dead-lettered when the queue has a dead-letter exchange, and dropped if not.
  void nack({bool requeue = true}) => _settle((h) => n.nack(h, requeue));

  void _settle(int Function(Pointer<n.FiremqMessage>) f) {
    if (_handle == nullptr) return;
    try {
      _check(f(_handle));
    } finally {
      _finalizer.detach(this);
      n.messageFree(_handle);
      _handle = nullptr;
    }
  }
}

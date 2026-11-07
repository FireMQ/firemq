// Conformance driver for the Dart binding. See spec/conformance.md.
//
// Reads one JSON command per line on stdin and writes one JSON reply per line on stdout.
//
//   firemq-conformance run --driver "dart run tool/conformance_driver.dart"
import 'dart:convert';
import 'dart:io';

import 'package:firemq/firemq.dart';

class _Failure implements Exception {
  _Failure(this.kind, this.message);
  final String kind;
  final String message;
}

Map<String, Object?> _error(String kind, int code, String message) =>
    {'kind': kind, 'code': code, 'message': message};

Map<String, Object?> _errorOf(FireMQException e) => _error(
    switch (e) {
      ServerException() => 'server',
      UnroutableException() => 'unroutable',
      NackedException() => 'nacked',
      ClosedException() => 'closed',
      OtherException() => 'other',
    },
    e.code,
    e.message);

class Driver {
  Client? client;
  final subs = <String, Subscription>{};
  final pending = <String, Message>{};
  var nextId = 0;

  Client get conn => client ?? (throw _Failure('closed', 'not connected'));

  Map<String, Object?> keep(Message m) {
    final tag = '${++nextId}';
    pending[tag] = m;
    String? text;
    try {
      text = utf8.decode(m.body);
    } on FormatException {
      text = null;
    }
    return {
      'body': text,
      'body_base64': base64.encode(m.body),
      'exchange': m.exchange,
      'routing_key': m.routingKey,
      'redelivered': m.redelivered,
      'tag': tag,
      'properties': m.properties,
      'headers': m.headers,
    };
  }

  Message take(Map c) => pending.remove(c['tag']) ?? (throw _Failure('other', 'unknown tag'));

  Map<String, Object?>? map(Object? v) => (v as Map?)?.cast<String, Object?>();

  Map<String, Object?> handle(String op, Map c) {
    switch (op) {
      case 'hello':
        return {'sdk': 'firemq-dart', 'version': nativeVersion, 'capabilities': []};
      case 'connect':
        client = Client.connect(c['url'] as String);
        return {};
      case 'close':
        for (final s in subs.values) {
          s.cancel();
        }
        subs.clear();
        pending.clear();
        final c = client;
        client = null;
        c?.close();
        return {};
      case 'declare_queue':
        final q = conn.declareQueue(c['name'] as String,
            durable: c['durable'] as bool,
            exclusive: c['exclusive'] as bool,
            autoDelete: c['auto_delete'] as bool,
            arguments: map(c['args']));
        return {'name': q.name, 'messages': q.messages, 'consumers': q.consumers};
      case 'delete_queue':
        return {'messages': conn.deleteQueue(c['name'] as String)};
      case 'purge_queue':
        return {'messages': conn.purgeQueue(c['name'] as String)};
      case 'declare_exchange':
        conn.declareExchange(c['name'] as String, c['type'] as String,
            durable: c['durable'] as bool);
        return {};
      case 'delete_exchange':
        conn.deleteExchange(c['name'] as String);
        return {};
      case 'bind_queue':
        conn.bindQueue(
            c['queue'] as String, c['exchange'] as String, c['key'] as String, map(c['args']));
        return {};
      case 'unbind_queue':
        conn.unbindQueue(c['queue'] as String, c['exchange'] as String, c['key'] as String);
        return {};
      case 'bind_exchange':
        conn.bindExchange(c['destination'] as String, c['source'] as String, c['key'] as String);
        return {};
      case 'publish':
        final b64 = c['body_base64'] as String?;
        final body = b64 != null ? base64.decode(b64) : utf8.encode(c['body'] as String);
        conn.publish(c['exchange'] as String, c['key'] as String, body,
            persistent: c['persistent'] as bool,
            mandatory: c['mandatory'] as bool,
            properties: map(c['properties']),
            headers: map(c['headers']));
        return {};
      case 'subscribe':
        final sub = conn.subscribe(c['queue'] as String,
            prefetch: c['prefetch'] as int, noAck: c['no_ack'] as bool);
        final id = 'sub-${++nextId}';
        subs[id] = sub;
        return {'sub': id};
      case 'next':
        final sub = subs[c['sub']] ?? (throw _Failure('other', 'unknown sub'));
        Message? m;
        try {
          m = sub.next(timeout: Duration(milliseconds: c['timeout_ms'] as int));
        } on ClosedException {
          m = null;
        }
        return {'message': m == null ? null : keep(m)};
      case 'ack':
        take(c).ack();
        return {};
      case 'nack':
        take(c).nack(requeue: c['requeue'] as bool);
        return {};
      case 'unsubscribe':
        subs.remove(c['sub'])?.cancel();
        return {};
      case 'get':
        final m = conn.get(c['queue'] as String, noAck: c['no_ack'] as bool);
        return {'message': m == null ? null : keep(m)};
      default:
        throw _Failure('unsupported', 'unknown op $op');
    }
  }
}

Future<void> main() async {
  final driver = Driver();
  final lines = stdin.transform(utf8.decoder).transform(const LineSplitter());
  await for (final line in lines) {
    if (line.trim().isEmpty) continue;
    final Map cmd;
    try {
      cmd = jsonDecode(line) as Map;
    } on FormatException catch (e) {
      stderr.writeln('driver: bad command: $e');
      continue;
    }
    Map<String, Object?> reply;
    try {
      reply = {'id': cmd['id'], 'ok': true, 'result': driver.handle(cmd['op'] as String, cmd)};
    } on _Failure catch (f) {
      reply = {'id': cmd['id'], 'ok': false, 'error': _error(f.kind, 0, f.message)};
    } on FireMQException catch (e) {
      reply = {'id': cmd['id'], 'ok': false, 'error': _errorOf(e)};
    } catch (e) {
      reply = {'id': cmd['id'], 'ok': false, 'error': _error('other', 0, '$e')};
    }
    stdout.writeln(jsonEncode(reply));
    await stdout.flush();
  }
}

// Publishes a message to a temporary queue, consumes it and acknowledges it.
//
//   FIREMQ_LIB=../../target/release/libfiremq_client_ffi.so dart run example/hello.dart
import 'dart:io';

import 'package:firemq/firemq.dart';

void main() {
  final client = Client.connect(Platform.environment['FIREMQ_URL'] ?? defaultUrl);
  final queue = client.declareQueue('', durable: false, exclusive: true).name;
  client.publishString('', queue, 'hello from Dart', headers: {'lang': 'dart'});

  final sub = client.subscribe(queue, prefetch: 10);
  final msg = sub.next(timeout: const Duration(seconds: 5));
  if (msg == null) throw StateError('no message within 5 s');
  print('received "${msg.text}" on ${msg.routingKey}, headers ${msg.headers}');
  msg.ack();
  sub.cancel();
  client.close();
}

import 'dart:io';
import 'dart:math';

import 'package:firemq/firemq.dart';
import 'package:test/test.dart';

final url = Platform.environment['FIREMQ_URL'] ?? defaultUrl;

String queueName() => 'dart.${Random().nextInt(1 << 32).toRadixString(16)}';

void main() {
  late Client client;
  late String queue;

  setUp(() {
    client = Client.connect(url);
    queue = client.declareQueue(queueName(), durable: false, exclusive: true).name;
  });

  tearDown(() => client.close());

  test('publish, consume and ack', () {
    client.publishString('', queue, 'hello from Dart',
        headers: {'lang': 'dart', 'n': 5, 'f': 1.5},
        properties: {'content_type': 'text/plain', 'priority': 3});
    final sub = client.subscribe(queue, prefetch: 10);
    final msg = sub.next(timeout: const Duration(seconds: 5));
    expect(msg, isNotNull);
    expect(msg!.text, 'hello from Dart');
    expect(msg.routingKey, queue);
    expect(msg.exchange, '');
    expect(msg.redelivered, isFalse);
    expect(msg.headers, {'lang': 'dart', 'n': 5, 'f': 1.5});
    expect(msg.properties, {'content_type': 'text/plain', 'priority': 3});
    msg.ack();
    msg.ack(); // settling twice is harmless
    expect(sub.next(timeout: const Duration(milliseconds: 50)), isNull);
    sub.cancel();
    expect(() => sub.next(), throwsA(isA<ClosedException>()));
    expect(nativeVersion, isNotEmpty);
  });

  test('iterate, requeue and binary bodies', () {
    for (var i = 0; i < 5; i++) {
      client.publishString('', queue, 'm$i');
    }
    final sub = client.subscribe(queue, prefetch: 2);
    final got = sub.messages.take(5).map((m) {
      m.ack();
      return m.text;
    }).toList();
    expect(got, List.generate(5, (i) => 'm$i'));
    sub.cancel();

    client.publish('', queue, [0, 255, 1]);
    client.get(queue)!.nack(requeue: true);
    final again = client.get(queue, noAck: true)!;
    expect(again.redelivered, isTrue);
    expect(again.body, [0, 255, 1]);
    expect(client.get(queue), isNull);
  });

  test('errors carry reply codes', () {
    expect(() => Client.connect(url.replaceFirst('guest:guest', 'guest:wrong')),
        throwsA(isA<ServerException>().having((e) => e.code, 'code', 403)));
    expect(() => client.publishString('', '$queue.nowhere', 'x', mandatory: true),
        throwsA(isA<UnroutableException>().having((e) => e.code, 'code', 312)));
    expect(() => client.bindQueue(queue, '$queue.missing'),
        throwsA(isA<ServerException>().having((e) => e.code, 'code', 404)));

    final full = '$queue.full';
    client.declareQueue(full,
        durable: false,
        exclusive: true,
        arguments: {'x-max-length': 1, 'x-overflow': 'reject-publish'});
    client.publishString('', full, 'fits');
    expect(() => client.publishString('', full, 'rejected'), throwsA(isA<NackedException>()));
    expect(client.purgeQueue(full), 1);
    expect(client.deleteQueue(full), 0);

    final other = Client.connect(url);
    other.close();
    expect(other.isClosed, isTrue);
    expect(() => other.purgeQueue(queue), throwsA(isA<ClosedException>()));
  });
}

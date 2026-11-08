# firemq for Dart

A Dart client for [FireMQ](https://github.com/FireMQ/firemq), over the C API of its Rust
client.

```bash
dart pub add firemq
```

The package calls `libfiremq_client_ffi` through `dart:ffi`, so it also needs that library:

1. Download the archive for your platform, `firemq-ffi-<target>.tar.gz` (`.zip` on Windows),
   from the [FireMQ releases](https://github.com/FireMQ/firemq/releases).
2. Either set `FIREMQ_LIB` to the library's path, for example
   `FIREMQ_LIB=$PWD/firemq-ffi/lib/libfiremq_client_ffi.so`, or put the library on the
   system's library search path.

```dart
import 'package:firemq/firemq.dart';

void main() {
  final client = Client.connect('fire://guest:guest@localhost:5673');
  client.declareQueue('jobs');
  client.publishString('', 'jobs', 'hello', headers: {'attempt': 1});

  final sub = client.subscribe('jobs', prefetch: 10);
  final msg = sub.next(timeout: const Duration(seconds: 5));
  print(msg?.text);
  msg?.ack();
  client.close();
}
```

Calls are synchronous: they block the calling isolate until the broker answers, so run a
consumer in its own isolate to keep an event loop responsive. Errors are subclasses of
`FireMQException`, with the AMQP reply code in `code` when the broker refused a request.

## Develop

Build the library, start a broker, then run the tests and the example:

```bash
cargo build -p firemq-client-ffi
export FIREMQ_LIB=$PWD/../../target/debug/libfiremq_client_ffi.so  # .dylib on macOS
dart pub get
dart test
dart run example/hello.dart
```

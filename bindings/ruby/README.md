# firemq for Ruby

A Ruby client for [FireMQ](https://github.com/FireMQ/firemq), built on the Rust client with
[UniFFI](https://mozilla.github.io/uniffi-rs/).

```bash
gem install firemq
```

The gem includes the native library for Linux (x86-64 and arm64, glibc 2.17 or later), macOS
(Apple silicon and Intel, 11 or later) and Windows x64, so it needs no compiler. It needs
Ruby 3.1 or later.

```ruby
require "firemq"

FireMQ.connect("fire://guest:guest@localhost:5673") do |client|
  client.declare_queue("jobs")
  client.publish("", "jobs", "hello", headers: { "attempt" => 1 }, content_type: "text/plain")

  client.subscribe("jobs", prefetch: 10).each do |msg|
    puts msg.text
    msg.ack
    break
  end
end
```

Calls block until the broker answers. Errors are subclasses of `FireMQ::Error`:
`ServerError` (with the AMQP reply `code`), `UnroutableError`, `NackedError`, `ClosedError`
and `OtherError`.

To use a library you built yourself, set `FIREMQ_LIB` to its path.

## Develop

Build the library, start a broker, then run the tests:

```bash
cargo build -p firemq-client-ffi
FIREMQ_LIB=../../target/debug/libfiremq_client_ffi.so \
  FIREMQ_URL=fire://guest:guest@localhost:5673 \
  bundle exec ruby -Ilib -Itest test/test_firemq.rb
```

On macOS, the library is `libfiremq_client_ffi.dylib`.

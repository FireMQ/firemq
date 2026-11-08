# FireMQ for C and C++

A C API for [FireMQ](https://github.com/FireMQ/firemq), over the Rust client. One header,
`firemq.h`, and one library, `libfiremq_client_ffi`. The header also compiles as C++.

## Get the library

Each [release](https://github.com/FireMQ/firemq/releases) has an archive per platform,
`firemq-ffi-<target>.tar.gz` (`.zip` on Windows), with a `.sha256` checksum:

| Target | Platform |
|---|---|
| `x86_64-unknown-linux-gnu` | Linux x86-64, glibc 2.17 or later |
| `aarch64-unknown-linux-gnu` | Linux arm64, glibc 2.17 or later |
| `aarch64-apple-darwin` | macOS on Apple silicon, 11 or later |
| `x86_64-apple-darwin` | macOS on Intel, 11 or later |
| `x86_64-pc-windows-msvc` | Windows x64 |

Each archive contains:

- `include/firemq.h`
- `lib/`: the shared library (`libfiremq_client_ffi.so`, `.dylib`, or `firemq_client_ffi.dll`
  with its import library `firemq_client_ffi.dll.lib`) and the static library
  (`libfiremq_client_ffi.a`, or `firemq_client_ffi.lib` on Windows)

To build the library from source instead, run `cargo build -p firemq-client-ffi --release` in
the repository. It is written to `target/release`.

## Build a program

Link against the shared library. On Linux and macOS, with the archive unpacked in
`firemq-ffi`:

```bash
cc -std=c11 -Ifiremq-ffi/include -o example example.c \
  -Lfiremq-ffi/lib -lfiremq_client_ffi -Wl,-rpath,"$PWD/firemq-ffi/lib"
FIREMQ_URL=fire://guest:guest@localhost:5673 ./example
```

The static library also works, but it needs the system libraries the Rust code uses. List them
with:

```bash
cargo rustc -p firemq-client-ffi --release --lib --crate-type staticlib -- --print native-static-libs
```

In this directory, `make run` builds the library and [`example.c`](example.c) from source and
runs the example against `$FIREMQ_URL` (default `fire://guest:guest@localhost:5673`).

## Use the API

The comment at the top of [`firemq.h`](firemq.h) describes the rules:

- Fallible functions return `FIREMQ_OK` or a `FIREMQ_ERR_*` status. After a failure,
  `firemq_last_error()` describes it and `firemq_last_error_code()` gives the AMQP reply code.
- The caller owns every handle and frees it once: `firemq_free`, `firemq_subscription_free`,
  `firemq_message_free`. Strings and buffers returned to the caller are freed with
  `firemq_string_free` and `firemq_buffer_free`.
- Calls block until the broker answers, and every handle can be used from any thread.

[`example.c`](example.c) connects, declares a queue, publishes, consumes and acknowledges a
message, and handles a refused request.

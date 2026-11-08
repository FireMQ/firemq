# Install

FireMQ is a single binary. Choose one of the following methods.

## Install script (Linux and macOS)

The script downloads the latest release for your platform, checks its SHA-256 checksum, and installs `firemq` to `/usr/local/bin` when that directory is writable, or to `~/.local/bin` otherwise.

```bash
curl -fsSL https://raw.githubusercontent.com/FireMQ/firemq/main/install.sh | sh
```

To pin a version or choose the directory, set `FIREMQ_VERSION` and `FIREMQ_INSTALL_DIR`:

```bash
curl -fsSL https://raw.githubusercontent.com/FireMQ/firemq/main/install.sh | FIREMQ_VERSION=v0.1.0 FIREMQ_INSTALL_DIR=$HOME/bin sh
```

## Docker

```bash
docker run -d --name firemq -p 5672:5672 -p 5673:5673 -p 15672:15672 -v firemq-data:/data ghcr.io/firemq/firemq
```

The image is built `FROM scratch` and contains only the static binary. Data is stored in the `/data` volume. Inside the container, the `guest` user may connect from any host, because connections arrive through the container network rather than from localhost. Set `FIREMQ_DEFAULT_USER` and `FIREMQ_DEFAULT_PASS` to create a different first user.

A Compose file is in [`deploy/docker-compose.yml`](https://github.com/FireMQ/firemq/blob/main/deploy/docker-compose.yml).

## Homebrew (macOS and Linux)

```bash
brew install firemq/tap/firemq
```

To start it at login, with data in Homebrew's `var/firemq` directory, run `brew services start firemq`.

## GitHub Actions

[`FireMQ/setup-firemq`](https://github.com/FireMQ/setup-firemq) starts a broker for the rest of a job, on Linux, macOS and Windows runners:

```yaml
steps:
  - uses: FireMQ/setup-firemq@v1
  - run: npm test   # connect to amqp://guest:guest@localhost:5672
```

## Release archives

Download an archive for Linux (static, x86_64 or arm64), macOS (Apple silicon or Intel) or Windows from the [releases page](https://github.com/FireMQ/firemq/releases), unpack it, and put `firemq` on your `PATH`.

## With Cargo

You need Rust 1.89 or later.

```bash
cargo install firemq
```

To build the latest code on `main` instead, run `cargo install --git https://github.com/FireMQ/firemq firemq`.

## Run as a service on Linux

[`deploy/firemq.service`](https://github.com/FireMQ/firemq/blob/main/deploy/firemq.service) is a hardened systemd unit that stores data in `/var/lib/firemq`. Its comments list the commands to install it.

## Check the installation

```bash
firemq --version
firemq doctor
```

`firemq doctor` reports the open-files limit, whether the default ports are free, and whether a broker is running.

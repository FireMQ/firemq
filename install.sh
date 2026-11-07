#!/bin/sh
# Installs the latest FireMQ release.
#   curl -fsSL https://raw.githubusercontent.com/FireMQ/firemq/main/install.sh | sh
# Set FIREMQ_VERSION=v0.1.0 for a specific version and FIREMQ_INSTALL_DIR to choose where.
set -eu

repo="FireMQ/firemq"
version="${FIREMQ_VERSION:-latest}"

os=$(uname -s)
arch=$(uname -m)
case "$os" in
  Linux) os_part="unknown-linux-musl" ;;
  Darwin) os_part="apple-darwin" ;;
  *) echo "FireMQ: unsupported OS '$os'. Download a release from https://github.com/$repo/releases" >&2; exit 1 ;;
esac
case "$arch" in
  x86_64 | amd64) arch_part="x86_64" ;;
  arm64 | aarch64) arch_part="aarch64" ;;
  *) echo "FireMQ: unsupported CPU '$arch'." >&2; exit 1 ;;
esac
target="${arch_part}-${os_part}"
asset="firemq-${target}.tar.gz"
if [ "$version" = "latest" ]; then
  base="https://github.com/$repo/releases/latest/download"
else
  base="https://github.com/$repo/releases/download/$version"
fi

if [ -n "${FIREMQ_INSTALL_DIR:-}" ]; then
  dir="$FIREMQ_INSTALL_DIR"
elif [ -w /usr/local/bin ]; then
  dir=/usr/local/bin
else
  dir="$HOME/.local/bin"
fi
mkdir -p "$dir"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
echo "Downloading FireMQ ($target)..."
curl -fsSL "$base/$asset" -o "$tmp/$asset"
curl -fsSL "$base/$asset.sha256" -o "$tmp/$asset.sha256"
expected=$(cut -d' ' -f1 "$tmp/$asset.sha256")
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$tmp/$asset" | cut -d' ' -f1)
else
  actual=$(shasum -a 256 "$tmp/$asset" | cut -d' ' -f1)
fi
if [ "$expected" != "$actual" ]; then
  echo "FireMQ: checksum mismatch for $asset" >&2
  exit 1
fi
tar -xzf "$tmp/$asset" -C "$tmp"
install -m 0755 "$tmp/firemq-${target}/firemq" "$dir/firemq"
echo "Installed $("$dir/firemq" --version) to $dir/firemq"
case ":$PATH:" in
  *":$dir:"*) ;;
  *) echo "Add $dir to your PATH, for example: export PATH=\"$dir:\$PATH\"" ;;
esac
echo "Start a broker with: firemq"

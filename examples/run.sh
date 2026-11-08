#!/usr/bin/env bash
# Runs every documentation example against a broker on localhost. Used by CI.
set -euo pipefail
cd "$(dirname "$0")/.."
export AMQP_URL=${AMQP_URL:-amqp://guest:guest@localhost:5672/%2F}
export FIREMQ_URL=${FIREMQ_URL:-fire://guest:guest@localhost:5673}
for f in examples/python/*.py; do
  echo "== $f"; uv run --quiet --with pika python "$f"
done
(cd examples/node && npm install --silent --no-audit --no-fund)
for f in examples/node/*.mjs; do
  echo "== $f"; node "$f"
done
for e in hello work_queues topics rpc; do
  echo "== crates/firemq-client/examples/$e.rs"; cargo run --quiet -p firemq-client --example "$e"
done
echo "All examples passed."

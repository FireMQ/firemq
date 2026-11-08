#!/usr/bin/env bash
# Benchmarks FireMQ and RabbitMQ under the same conditions: each broker runs in Docker with the
# same CPU and memory limits, and the load generators run in another container on the same
# Docker network. See docs/src/benchmarks.md for the method and results.
#
#   bench/run.sh [seconds-per-run] [runs]
#
# Environment: CPUS (default 4), MEMORY (default 2g), RABBITMQ_IMAGE (default rabbitmq:4-management),
# ONLY (run one scenario by name, or "recovery"), BROKERS (default "firemq rabbitmq"),
# OUT (default bench/results/<date>), BACKLOG (messages in the recovery test, default 2000000).
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION=${1:-20}
RUNS=${2:-3}
CPUS=${CPUS:-4}
MEMORY=${MEMORY:-2g}
RABBITMQ_IMAGE=${RABBITMQ_IMAGE:-rabbitmq:4-management}
BACKLOG=${BACKLOG:-2000000}
BROKERS=${BROKERS:-firemq rabbitmq}
NET=firemq-bench
CLIENT_IMAGE=rust:1.95
OUT=${OUT:-bench/results/$(date +%Y-%m-%d)}
CSV=$OUT/results.csv
mkdir -p "$OUT"

# name | load generator flags
SCENARIOS=(
  "transient-1x1|-p 1 -c 1 -s 256"
  "persistent-1x1|-p 1 -c 1 -s 1024 --persistent"
  "transient-4x4|-p 4 -c 4 -s 256"
  "persistent-4x4|-p 4 -c 4 -s 1024 --persistent"
  "latency-transient|-p 1 -c 1 -s 256 --rate 1000"
  "latency-persistent|-p 1 -c 1 -s 1024 --persistent --rate 1000"
)

docker network inspect $NET >/dev/null 2>&1 || docker network create $NET >/dev/null

echo "== Building the FireMQ image and the Linux load generators"
docker build -q -t firemq:bench . >/dev/null
build() {
  docker run --rm -v "$PWD":/src -v firemq-bench-cargo:/usr/local/cargo/registry \
    -v firemq-bench-target:/target -v firemq-bench-rustup:/usr/local/rustup -w /src $CLIENT_IMAGE cargo build -q --release --locked --target-dir /target "$@"
}
build -p firemq
build -p firemq-amqp --example amqp_bench

client() {
  docker run --rm --network $NET --cpus "$CPUS" -v firemq-bench-target:/target $CLIENT_IMAGE "$@"
}

stop_broker() { docker rm -f bench-broker >/dev/null 2>&1 || true; }

# Starts a broker and prints how long it took until it accepted AMQP connections.
start_broker() {
  stop_broker
  local started; started=$(python3 -c 'import time; print(time.time())')
  case $1 in
    firemq) docker run -d --name bench-broker --network $NET --cpus "$CPUS" --memory "$MEMORY" firemq:bench >/dev/null ;;
    rabbitmq) docker run -d --name bench-broker --network $NET --cpus "$CPUS" --memory "$MEMORY" \
        -v "$PWD/bench/rabbitmq.conf:/etc/rabbitmq/conf.d/90-bench.conf:ro" "$RABBITMQ_IMAGE" >/dev/null ;;
  esac
  client bash -c 'until (echo > /dev/tcp/bench-broker/5672) 2>/dev/null; do sleep 0.05; done; until curl -sf -u guest:guest http://bench-broker:15672/api/overview >/dev/null; do sleep 0.05; done'
  python3 -c "import time; print(f'{time.time() - $started:.2f}')"
}

mem_mib() {
  docker stats --no-stream --format '{{.MemUsage}}' bench-broker | awk '{print $1}' | python3 -c '
import re, sys
v = sys.stdin.read().strip()
n, unit = re.match(r"([\d.]+)\s*([A-Za-z]+)", v).groups()
print(round(float(n) * {"B": 1 / 2**20, "KiB": 1 / 1024, "kB": 1 / 1024, "MiB": 1, "MB": 1, "GiB": 1024, "GB": 1024}[unit]))'
}

# Runs one load generator while sampling broker memory; appends a CSV row.
measure() {
  local broker=$1 protocol=$2 scenario=$3 run=$4 flags=$5 log
  log="$OUT/$broker-$protocol-$scenario-$run.log"
  local cmd
  case $protocol in
    amqp) cmd="/target/release/examples/amqp_bench --url amqp://guest:guest@bench-broker:5672/%2F" ;;
    fire) cmd="/target/release/firemq bench --url fire://guest:guest@bench-broker:5673" ;;
  esac
  (while sleep 2; do mem_mib >> "$log.mem" 2>/dev/null || true; done) &
  local sampler=$!
  # shellcheck disable=SC2086
  client $cmd -d "$DURATION" $flags > "$log" 2>&1 || echo "  (load generator failed, see $log)"
  { kill $sampler && wait $sampler; } 2>/dev/null || true
  local peak; peak=$(sort -n "$log.mem" 2>/dev/null | tail -1)
  python3 - "$log" "$broker" "$protocol" "$scenario" "$run" "${peak:-}" >> "$CSV" <<'PY'
import re, sys
log, broker, protocol, scenario, run, peak = sys.argv[1:]
text = open(log).read()
rate = re.search(r"published (\d+) msg/s .*consumed (\d+) msg/s", text)
lat = re.search(r"p50 ([\d.]+) ms\s+p95 ([\d.]+) ms\s+p99 ([\d.]+) ms\s+max ([\d.]+) ms", text)
row = [broker, protocol, scenario, run]
row += list(rate.groups()) if rate else ["", ""]
row += list(lat.groups()) if lat else ["", "", "", ""]
row.append(peak)
print(",".join(row))
PY
  tail -1 "$CSV" | awk -F, '{printf "  %-9s %-5s %-19s run %s  %8s msg/s  p50 %6s ms  p99 %7s ms  %5s MiB\n", $1, $2, $3, $4, $5, $7, $9, $11}'
}

[ -f "$CSV" ] || echo "broker,protocol,scenario,run,published_per_s,consumed_per_s,p50_ms,p95_ms,p99_ms,max_ms,peak_mib" > "$CSV"
[ -f "$OUT/startup.csv" ] || echo "broker,run,ready_seconds,idle_mib" > "$OUT/startup.csv"
{
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "host: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(sysctl -n hw.ncpu 2>/dev/null || nproc) cores"
  echo "docker: $(docker info --format '{{.OperatingSystem}} {{.ServerVersion}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes')"
  echo "limits: --cpus $CPUS --memory $MEMORY for the broker, --cpus $CPUS for the load generator"
  echo "firemq: $(git rev-parse --short HEAD), image $(docker images --format '{{.Size}}' firemq:bench)"
  echo "rabbitmq: $RABBITMQ_IMAGE, image $(docker images --format '{{.Size}}' "$RABBITMQ_IMAGE"), $(docker run --rm "$RABBITMQ_IMAGE" sh -c 'echo RabbitMQ $RABBITMQ_VERSION, Erlang $(erl -noshell -eval "io:put_chars(erlang:system_info(otp_release)), halt().")' 2>/dev/null)"
  echo "duration: ${DURATION}s per run, $RUNS runs; recovery backlog $BACKLOG messages"
  # Startup and recovery times include starting the container that measures them.
  echo "measuring container start: $(python3 -c 'import subprocess, sys, time; t = time.time(); subprocess.run(sys.argv[1:]); print(f"{time.time() - t:.2f}")' docker run --rm $CLIENT_IMAGE true)s"
} | tee "$OUT/environment.txt"

for broker in $BROKERS; do
  for entry in "${SCENARIOS[@]}"; do
    scenario=${entry%%|*}; flags=${entry#*|}
    [ -n "${ONLY:-}" ] && [ "$ONLY" != "$scenario" ] && continue
    protocols=amqp
    [ "$broker" = firemq ] && protocols="amqp fire"
    for protocol in $protocols; do
      for run in $(seq 1 "$RUNS"); do
        ready=$(start_broker "$broker")
        sleep 3
        echo "$broker,$run,$ready,$(mem_mib)" >> "$OUT/startup.csv"
        measure "$broker" "$protocol" "$scenario" "$run" "$flags"
      done
    done
  done
done

# Recovery: fill a durable queue with BACKLOG persistent messages, restart the broker, and time
# until the queue is back with every message. Counts come from a passive queue.declare, which is
# exact; the management API's counts can lag by several seconds.
if [ -z "${ONLY:-}" ] || [ "$ONLY" = recovery ]; then
  echo "== Recovery after a restart"
  [ -f "$OUT/recovery.csv" ] || echo "broker,run,messages,seconds,mib_before,mib_after" > "$OUT/recovery.csv"
  for broker in $BROKERS; do
   for run in $(seq 1 "$RUNS"); do
    start_broker "$broker" >/dev/null
    client /target/release/examples/amqp_bench --url amqp://guest:guest@bench-broker:5672/%2F \
      -p 4 -c 0 -s 256 --persistent --keep --queue backlog --messages "$BACKLOG" -d 300 > "$OUT/$broker-fill-$run.log" 2>&1
    sleep 2
    count_cmd="/target/release/examples/amqp_bench --url amqp://guest:guest@bench-broker:5672/%2F --queue backlog --count"
    # shellcheck disable=SC2086
    count=$(client $count_cmd)
    before=$(mem_mib)
    docker stop -t 60 bench-broker >/dev/null
    started=$(python3 -c 'import time; print(time.time())')
    docker start bench-broker >/dev/null
    # shellcheck disable=SC2086
    client timeout 600 $count_cmd --wait-for "$count" >/dev/null
    took=$(python3 -c "import time; print(f'{time.time() - $started:.2f}')")
    echo "$broker,$run,$count,$took,$before,$(mem_mib)" | tee -a "$OUT/recovery.csv"
   done
  done
fi
stop_broker
echo "Results in $OUT"

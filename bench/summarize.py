#!/usr/bin/env python3
"""Turns a bench/run.sh results directory into the Markdown tables in docs/src/benchmarks.md.

    bench/summarize.py bench/results/<date>

Every table cell comes from the median run of a scenario: the run with the median delivered
rate, or the median value for startup and recovery times.
"""

import csv
import re
import statistics
import sys
from pathlib import Path

COLUMNS = [
    ("rabbitmq", "amqp", "RabbitMQ"),
    ("firemq", "amqp", "FireMQ, AMQP"),
    ("firemq", "fire", "FireMQ, FireMQ protocol"),
]
THROUGHPUT = [
    ("transient-1x1", "Transient, 1 publisher and 1 consumer, 256 B"),
    ("persistent-1x1", "Persistent, 1 publisher and 1 consumer, 1 KiB"),
    ("transient-4x4", "Transient, 4 publishers and 4 consumers, 256 B"),
    ("persistent-4x4", "Persistent, 4 publishers and 4 consumers, 1 KiB"),
]
LATENCY = [
    ("latency-transient", "Transient, 256 B"),
    ("latency-persistent", "Persistent, 1 KiB"),
]


def rows(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def median_run(runs):
    """The run with the median delivered rate."""
    runs = sorted(runs, key=lambda r: float(r["consumed_per_s"] or 0))
    return runs[(len(runs) - 1) // 2]


def thousands(n):
    return f"{round(float(n), -3):,.0f}"


def ms(v):
    v = float(v)
    if v >= 1000:
        return f"{v / 1000:.1f} s"
    return f"{v:.2f} ms" if v < 10 else f"{v:.0f} ms"


def table(header, body):
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    out += ["| " + " | ".join(r) + " |" for r in body]
    return "\n".join(out)


def main(out_dir):
    out = Path(out_dir)
    results = rows(out / "results.csv")
    runs = {}
    for r in results:
        runs.setdefault((r["broker"], r["protocol"], r["scenario"]), []).append(r)
    med = {k: median_run(v) for k, v in runs.items()}

    def cell(scenario, fmt):
        return [fmt(med[(b, p, scenario)]) if (b, p, scenario) in med else "–" for b, p, _ in COLUMNS]

    header = ["Scenario"] + [c[2] for c in COLUMNS]
    print("Delivered messages per second:\n")
    print(table(header, [[name] + cell(s, lambda r: thousands(r["consumed_per_s"])) for s, name in THROUGHPUT]))

    lagging = []
    for s, name in THROUGHPUT:
        for b, p, label in COLUMNS:
            r = med.get((b, p, s))
            if r and float(r["published_per_s"]) > 1.02 * float(r["consumed_per_s"]):
                lagging.append(f"{label}, {name[0].lower() + name[1:]}: {thousands(r['published_per_s'])} published, "
                               f"{thousands(r['consumed_per_s'])} delivered")
    if lagging:
        print("\nRuns where publishers outpaced consumers:\n")
        print("\n".join(f"- {line}" for line in lagging))

    print("\nEnd-to-end latency at full throughput, p50 / p99:\n")
    print(table(header, [[name] + cell(s, lambda r: f"{ms(r['p50_ms'])} / {ms(r['p99_ms'])}") for s, name in THROUGHPUT]))

    print("\nEnd-to-end latency at 1,000 messages per second, p50 / p99:\n")
    print(table(header, [[name] + cell(s, lambda r: f"{ms(r['p50_ms'])} / {ms(r['p99_ms'])}") for s, name in LATENCY]))

    print("\nPeak broker memory during the run, MiB:\n")
    print(table(header, [[name] + cell(s, lambda r: r["peak_mib"] or "–") for s, name in THROUGHPUT]))

    brokers = [("rabbitmq", "RabbitMQ"), ("firemq", "FireMQ")]
    startup = rows(out / "startup.csv") if (out / "startup.csv").exists() else []
    recovery = rows(out / "recovery.csv") if (out / "recovery.csv").exists() else []

    def med_of(data, broker, key):
        vals = [float(r[key]) for r in data if r["broker"] == broker and r[key]]
        return statistics.median(vals) if vals else None

    env = (out / "environment.txt").read_text() if (out / "environment.txt").exists() else ""
    image = {}
    for b in ("firemq", "rabbitmq"):
        m = re.search(rf"^{b}: .*?image ([\d.]+\s*[KMG]B)", env, re.M)
        image[b] = m.group(1).replace("MB", " MB").replace("  ", " ") if m else "–"

    body = []
    for label, data, key, fmt in [
        ("Ready to accept connections after start", startup, "ready_seconds", lambda v: f"{v:.2f} s"),
        ("Memory when idle", startup, "idle_mib", lambda v: f"{v:.0f} MiB"),
        ("Restart with the backlog until every message is back", recovery, "seconds", lambda v: f"{v:.2f} s"),
        ("Memory holding the backlog, before the restart", recovery, "mib_before", lambda v: f"{v:.0f} MiB"),
        ("Memory holding the backlog, after the restart", recovery, "mib_after", lambda v: f"{v:.0f} MiB"),
    ]:
        vals = [med_of(data, b, key) for b, _ in brokers]
        body.append([label] + [fmt(v) if v is not None else "–" for v in vals])
    body.append(["Container image"] + [image[b] for b, _ in brokers])
    backlog = sorted({r["messages"] for r in recovery})
    print(f"\nStartup and recovery (backlog: {', '.join(backlog) or '–'} messages):\n")
    print(table([""] + [n for _, n in brokers], body))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])

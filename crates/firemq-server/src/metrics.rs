//! Prometheus metrics, using RabbitMQ's metric names where they exist so existing
//! dashboards work.

use std::fmt::Write;
use std::sync::atomic::Ordering;

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;

use crate::mgmt::AppState;

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

struct Out(String);

impl Out {
    fn metric(&mut self, name: &str, kind: &str, help: &str, value: impl std::fmt::Display) {
        let _ = writeln!(
            self.0,
            "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}"
        );
    }

    fn header(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.0, "# HELP {name} {help}\n# TYPE {name} {kind}");
    }

    fn sample(&mut self, name: &str, labels: &str, value: impl std::fmt::Display) {
        let _ = writeln!(self.0, "{name}{{{labels}}} {value}");
    }
}

const TEXT: &str = "text/plain; version=0.0.4; charset=utf-8";

/// `/metrics`: broker totals plus per-queue series labelled by vhost and queue.
pub async fn metrics(State(s): State<AppState>) -> impl IntoResponse {
    let b = &s.broker;
    let mut o = Out(String::with_capacity(8192));
    let node = esc(&b.config().node_name);
    o.header(
        "rabbitmq_build_info",
        "untyped",
        "RabbitMQ-compatible build info",
    );
    o.sample(
        "rabbitmq_build_info",
        &format!(
            "rabbitmq_version=\"3.13.7\",product=\"FireMQ\",product_version=\"{}\"",
            env!("CARGO_PKG_VERSION")
        ),
        1,
    );
    o.header("rabbitmq_identity_info", "untyped", "Node identity");
    o.sample(
        "rabbitmq_identity_info",
        &format!("rabbitmq_node=\"{node}\",rabbitmq_cluster=\"{node}\""),
        1,
    );
    let conns = b.connections();
    let channels: u64 = conns
        .iter()
        .map(|c| c.channels.load(Ordering::Relaxed) as u64)
        .sum();
    let (mut queues, mut consumers, mut ready, mut unacked) = (0u64, 0u64, 0u64, 0u64);
    let mut per_queue = Vec::new();
    for v in b.vhosts() {
        for q in v.topology().queues.values() {
            let st = &q.stats;
            queues += 1;
            let c = st.consumers.load(Ordering::Relaxed) as u64;
            let r = st.ready.load(Ordering::Relaxed);
            let u = st.unacked.load(Ordering::Relaxed);
            consumers += c;
            ready += r;
            unacked += u;
            per_queue.push((
                format!("vhost=\"{}\",queue=\"{}\"", esc(&v.name), esc(&q.name)),
                r,
                u,
                c,
                st.published.load(Ordering::Relaxed),
                st.delivered.load(Ordering::Relaxed),
                st.acked.load(Ordering::Relaxed),
            ));
        }
    }
    let c = b.counters();
    let n = b.node_stats();
    o.metric(
        "rabbitmq_connections",
        "gauge",
        "Open connections",
        conns.len(),
    );
    o.metric(
        "rabbitmq_connections_opened_total",
        "counter",
        "Connections opened since start",
        c.connections_opened.load(Ordering::Relaxed),
    );
    o.metric("rabbitmq_channels", "gauge", "Open AMQP channels", channels);
    o.metric("rabbitmq_consumers", "gauge", "Consumers", consumers);
    o.metric("rabbitmq_queues", "gauge", "Queues", queues);
    o.metric(
        "rabbitmq_queue_messages_ready",
        "gauge",
        "Messages ready for delivery",
        ready,
    );
    o.metric(
        "rabbitmq_queue_messages_unacked",
        "gauge",
        "Messages delivered but not yet acknowledged",
        unacked,
    );
    o.metric(
        "rabbitmq_queue_messages",
        "gauge",
        "Ready and unacknowledged messages",
        ready + unacked,
    );
    o.metric(
        "rabbitmq_global_messages_received_total",
        "counter",
        "Messages received from publishers",
        c.published.load(Ordering::Relaxed),
    );
    o.metric(
        "rabbitmq_global_messages_unroutable_dropped_total",
        "counter",
        "Messages that matched no queue",
        c.unroutable.load(Ordering::Relaxed),
    );
    o.metric(
        "rabbitmq_global_messages_delivered_total",
        "counter",
        "Messages delivered to consumers or fetched with get",
        c.delivered.load(Ordering::Relaxed),
    );
    o.metric(
        "rabbitmq_global_messages_acknowledged_total",
        "counter",
        "Messages acknowledged by consumers",
        c.acked.load(Ordering::Relaxed),
    );
    o.metric(
        "rabbitmq_global_messages_confirmed_total",
        "counter",
        "Publisher confirms sent",
        c.confirmed.load(Ordering::Relaxed),
    );
    o.metric(
        "rabbitmq_process_resident_memory_bytes",
        "gauge",
        "Resident memory of the broker process",
        n.mem_used.load(Ordering::Relaxed),
    );
    o.metric(
        "rabbitmq_resident_memory_limit_bytes",
        "gauge",
        "Memory high watermark",
        n.mem_limit.load(Ordering::Relaxed),
    );
    o.metric(
        "rabbitmq_disk_space_available_bytes",
        "gauge",
        "Free disk space on the data directory's disk",
        n.disk_free.load(Ordering::Relaxed),
    );
    o.metric(
        "rabbitmq_disk_space_available_limit_bytes",
        "gauge",
        "Free disk space limit",
        n.disk_limit.load(Ordering::Relaxed),
    );
    o.metric(
        "rabbitmq_alarms_memory_used_watermark",
        "gauge",
        "1 while the memory alarm is active",
        n.mem_alarm.load(Ordering::Relaxed) as u8,
    );
    o.metric(
        "rabbitmq_alarms_free_disk_space_watermark",
        "gauge",
        "1 while the disk alarm is active",
        n.disk_alarm.load(Ordering::Relaxed) as u8,
    );
    o.metric(
        "firemq_uptime_seconds",
        "gauge",
        "Seconds since the broker started",
        firemq_core::now_ms().saturating_sub(b.started_at()) / 1000,
    );
    let series: [(&str, &str, &str); 6] = [
        (
            "firemq_queue_messages_ready",
            "gauge",
            "Ready messages per queue",
        ),
        (
            "firemq_queue_messages_unacked",
            "gauge",
            "Unacknowledged messages per queue",
        ),
        ("firemq_queue_consumers", "gauge", "Consumers per queue"),
        (
            "firemq_queue_messages_published_total",
            "counter",
            "Messages enqueued per queue",
        ),
        (
            "firemq_queue_messages_delivered_total",
            "counter",
            "Messages delivered per queue",
        ),
        (
            "firemq_queue_messages_acked_total",
            "counter",
            "Messages acknowledged per queue",
        ),
    ];
    for (i, (name, kind, help)) in series.iter().enumerate() {
        o.header(name, kind, help);
        for (labels, r, u, c, p, d, a) in &per_queue {
            let v = [*r, *u, *c, *p, *d, *a][i];
            o.sample(name, labels, v);
        }
    }
    ([(header::CONTENT_TYPE, TEXT)], o.0)
}

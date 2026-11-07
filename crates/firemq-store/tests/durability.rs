//! Restart and crash-recovery tests for the disk store.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use firemq_core::*;
use firemq_store::{DiskStore, StoreConfig};
use tokio::sync::mpsc;
use tokio::time::timeout;

fn cfg() -> StoreConfig {
    StoreConfig {
        segment_size: 64 * 1024,
        ..Default::default()
    }
}

async fn open(dir: &Path, config: StoreConfig) -> Broker {
    let store = DiskStore::open(dir, config).unwrap();
    let broker = Broker::new(
        BrokerConfig {
            queue_cache_bytes: 16 * 1024,
            ..Default::default()
        },
        Arc::new(store),
    );
    broker.recover().await.unwrap();
    broker.ensure_default_user("guest", "guest");
    broker
}

async fn close(broker: Broker) {
    broker.save_definitions().unwrap();
    broker.shutdown().await;
    drop(broker);
    // Let queue actors drop their files.
    tokio::time::sleep(Duration::from_millis(50)).await;
}

fn persistent(body: &str) -> Arc<Message> {
    Arc::new(Message::new(
        "",
        "",
        BasicProperties {
            delivery_mode: Some(2),
            ..Default::default()
        },
        Bytes::copy_from_slice(body.as_bytes()),
    ))
}

fn transient(body: &str) -> Arc<Message> {
    Arc::new(Message::new(
        "",
        "",
        BasicProperties::default(),
        Bytes::copy_from_slice(body.as_bytes()),
    ))
}

fn durable_queue(v: &VHost, name: &str) -> QueueHandle {
    v.declare_queue(
        QueueOptions {
            name: name.into(),
            durable: true,
            ..Default::default()
        },
        None,
    )
    .unwrap()
    .queue
}

/// Publishes with a confirm ticket and waits for the confirm.
async fn publish_confirmed(q: &QueueHandle, msg: Arc<Message>) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    q.publish(msg, Some(Ticket::new(tx, 1, 1))).await;
    match timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap()
    {
        ConnEvent::Confirm { ack, .. } => assert!(ack, "publish was nacked"),
        other => panic!("unexpected {other:?}"),
    }
}

async fn drain(q: &QueueHandle) -> Vec<(String, bool)> {
    let owner = Owner {
        conn: 1,
        channel: 1,
    };
    let mut out = Vec::new();
    while let Some(g) = q.get(owner, true).await.unwrap() {
        out.push((
            String::from_utf8(g.message.body.to_vec()).unwrap(),
            g.redelivered,
        ));
    }
    out
}

#[tokio::test]
async fn persistent_messages_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path(), cfg()).await;
    let q = durable_queue(&b.vhost("/").unwrap(), "orders");
    for i in 0..5 {
        publish_confirmed(&q, persistent(&format!("p{i}"))).await;
    }
    q.publish(transient("t"), None).await;
    // Take and ack the first message.
    let owner = Owner {
        conn: 9,
        channel: 1,
    };
    let g = q.get(owner, false).await.unwrap().unwrap();
    q.ack(owner, vec![g.seq]).await;
    // Take the second one without acking: it must come back as redelivered.
    let _unacked = q.get(owner, false).await.unwrap().unwrap();
    close(b).await;

    let b = open(dir.path(), cfg()).await;
    let q = b.vhost("/").unwrap().queue("orders", None).unwrap();
    let got = drain(&q).await;
    let bodies: Vec<&str> = got.iter().map(|(b, _)| b.as_str()).collect();
    assert_eq!(bodies, ["p1", "p2", "p3", "p4"]);
    assert!(got.iter().all(|(_, r)| *r));
    close(b).await;
}

#[tokio::test]
async fn topology_users_and_policies_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path(), cfg()).await;
    let v = b.vhost("/").unwrap();
    v.declare_exchange(ExchangeOptions {
        name: "events".into(),
        kind: "topic".into(),
        durable: true,
        ..Default::default()
    })
    .unwrap();
    let mut args = FieldTable::new();
    args.insert("x-max-length", Value::I32(10));
    v.declare_queue(
        QueueOptions {
            name: "audit".into(),
            durable: true,
            arguments: args,
            ..Default::default()
        },
        None,
    )
    .unwrap();
    v.bind(
        "events",
        Destination::Queue("audit".into()),
        "#",
        FieldTable::new(),
        None,
    )
    .unwrap();
    v.set_policy(
        Policy::new(
            "short",
            "^audit$",
            "queues",
            serde_json::json!({"message-ttl": 60000}),
            1,
        )
        .unwrap(),
    )
    .await;
    b.users().put("app", Some("pw"), vec!["monitoring".into()]);
    b.add_vhost("staging");
    drop(v);
    close(b).await;

    let b = open(dir.path(), cfg()).await;
    assert!(b.vhost("staging").is_some());
    let v = b.vhost("/").unwrap();
    let routed = v.route("events", "any.thing", None).unwrap();
    assert_eq!(routed.len(), 1);
    assert_eq!(routed[0].name(), "audit");
    assert_eq!(routed[0].arguments.get_i64("x-max-length"), Some(10));
    assert_eq!(v.policies().len(), 1);
    assert!(b.users().authenticate("app", "pw", "PLAIN").is_ok());
    assert!(b.users().authenticate("guest", "guest", "PLAIN").is_ok());
    drop(v);
    close(b).await;
}

#[tokio::test]
async fn a_torn_tail_is_truncated_and_earlier_messages_kept() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path(), cfg()).await;
    let q = durable_queue(&b.vhost("/").unwrap(), "torn");
    for i in 0..3 {
        publish_confirmed(&q, persistent(&format!("m{i}"))).await;
    }
    close(b).await;
    // Simulate a crash in the middle of a write: append half a record.
    let seg = walk(dir.path())
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "seg"))
        .max_by_key(|p| std::fs::metadata(p).unwrap().len())
        .unwrap();
    let mut data = std::fs::read(&seg).unwrap();
    let valid = data.len();
    data.extend_from_slice(&[0, 0, 1, 0, 0xde, 0xad, 0xbe, 0xef, 1, 2, 3]);
    std::fs::write(&seg, &data).unwrap();

    let b = open(dir.path(), cfg()).await;
    assert_eq!(std::fs::metadata(&seg).unwrap().len() as usize, valid);
    let q = b.vhost("/").unwrap().queue("torn", None).unwrap();
    let got: Vec<String> = drain(&q).await.into_iter().map(|(b, _)| b).collect();
    assert_eq!(got, ["m0", "m1", "m2"]);
    close(b).await;
}

#[tokio::test]
async fn acknowledged_segments_are_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path(), cfg()).await;
    let q = durable_queue(&b.vhost("/").unwrap(), "churn");
    let body = "x".repeat(4096);
    for _ in 0..100 {
        q.publish(persistent(&body), None).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let segments = |p: &Path| {
        walk(p)
            .into_iter()
            .filter(|p| p.extension().is_some_and(|e| e == "seg"))
            .count()
    };
    assert!(segments(dir.path()) > 3, "expected several 64 KiB segments");
    assert_eq!(drain(&q).await.len(), 100);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        segments(dir.path()) <= 1,
        "fully acked segments should be gone"
    );
    close(b).await;
}

#[tokio::test]
async fn large_backlogs_page_to_disk() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path(), cfg()).await;
    let q = durable_queue(&b.vhost("/").unwrap(), "backlog");
    for i in 0..500 {
        q.publish(transient(&format!("{i:05}{}", "y".repeat(1000))), None)
            .await;
    }
    let got = drain(&q).await;
    assert_eq!(got.len(), 500);
    for (i, (body, _)) in got.iter().enumerate() {
        assert!(body.starts_with(&format!("{i:05}")));
    }
    close(b).await;
}

#[tokio::test]
async fn queues_missing_from_definitions_are_restored_from_disk() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path(), cfg()).await;
    let q = durable_queue(&b.vhost("/").unwrap(), "fresh");
    publish_confirmed(&q, persistent("kept")).await;
    b.shutdown().await;
    drop(b);
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Crash before the definitions writer ran.
    let _ = std::fs::remove_file(dir.path().join("definitions.json"));

    let b = open(dir.path(), cfg()).await;
    let q = b.vhost("/").unwrap().queue("fresh", None).unwrap();
    assert_eq!(drain(&q).await.len(), 1);
    close(b).await;
}

#[tokio::test]
async fn a_data_directory_has_one_owner() {
    let dir = tempfile::tempdir().unwrap();
    let _first = DiskStore::open(dir.path(), cfg()).unwrap();
    let err = DiskStore::open(dir.path(), cfg()).err().unwrap();
    assert!(err.to_string().contains("in use"), "{err}");
}

#[tokio::test]
async fn deleting_a_queue_deletes_its_files() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(dir.path(), cfg()).await;
    let v = b.vhost("/").unwrap();
    let q = durable_queue(&v, "gone");
    publish_confirmed(&q, persistent("x")).await;
    assert!(walk(dir.path()).iter().any(|p| p.ends_with("meta.json")));
    v.delete_queue("gone", false, false, None).await.unwrap();
    assert!(!walk(dir.path()).iter().any(|p| p.ends_with("meta.json")));
    drop(v);
    close(b).await;
}

fn walk(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                out.push(p);
            }
        }
    }
    out
}

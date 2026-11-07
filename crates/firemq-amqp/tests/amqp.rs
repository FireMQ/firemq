//! End-to-end tests: a real AMQP client (amqprs) against an in-process FireMQ.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use amqprs::BasicProperties;
use amqprs::channel::{
    BasicAckArguments, BasicConsumeArguments, BasicGetArguments, BasicNackArguments,
    BasicPublishArguments, BasicQosArguments, Channel, ExchangeDeclareArguments,
    QueueBindArguments, QueueDeclareArguments,
};
use amqprs::connection::{Connection, OpenConnectionArguments};
use firemq_amqp::{AmqpConfig, serve_connection};
use firemq_core::Broker;
use tokio::net::TcpListener;
use tokio::time::timeout;

async fn start() -> (Broker, SocketAddr) {
    let broker = Broker::in_memory();
    broker.ensure_default_user("guest", "guest");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = broker.clone();
    let cfg = Arc::new(AmqpConfig::default());
    tokio::spawn(async move {
        loop {
            let Ok((stream, peer)) = listener.accept().await else {
                return;
            };
            let local = stream.local_addr().unwrap();
            tokio::spawn(serve_connection(
                b.clone(),
                cfg.clone(),
                stream,
                peer,
                local,
                false,
            ));
        }
    });
    (broker, addr)
}

async fn connect(addr: SocketAddr) -> (Connection, Channel) {
    let conn = Connection::open(&OpenConnectionArguments::new(
        &addr.ip().to_string(),
        addr.port(),
        "guest",
        "guest",
    ))
    .await
    .unwrap();
    let ch = conn.open_channel(None).await.unwrap();
    (conn, ch)
}

async fn publish(ch: &Channel, exchange: &str, key: &str, body: &str) {
    ch.basic_publish(
        BasicProperties::default(),
        body.as_bytes().to_vec(),
        BasicPublishArguments::new(exchange, key),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn hello_world() {
    let (_broker, addr) = start().await;
    let (conn, ch) = connect(addr).await;
    let (name, _, _) = ch
        .queue_declare(QueueDeclareArguments::new("hello"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(name, "hello");
    publish(&ch, "", "hello", "Hello World!").await;
    let (_tag, mut rx) = ch
        .basic_consume_rx(
            BasicConsumeArguments::new("hello", "c1")
                .manual_ack(false)
                .finish(),
        )
        .await
        .unwrap();
    let msg = timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.content.unwrap(), b"Hello World!");
    let deliver = msg.deliver.unwrap();
    assert_eq!(deliver.routing_key(), "hello");
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn work_queue_with_prefetch_and_acks() {
    let (_broker, addr) = start().await;
    let (_conn, ch) = connect(addr).await;
    ch.queue_declare(QueueDeclareArguments::durable_client_named("tasks"))
        .await
        .unwrap();
    for i in 0..10 {
        publish(&ch, "", "tasks", &format!("task {i}")).await;
    }
    ch.basic_qos(BasicQosArguments::new(0, 1, false))
        .await
        .unwrap();
    let (_tag, mut rx) = ch
        .basic_consume_rx(
            BasicConsumeArguments::new("tasks", "worker")
                .manual_ack(true)
                .finish(),
        )
        .await
        .unwrap();
    for i in 0..10 {
        let msg = timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.content.unwrap(), format!("task {i}").as_bytes());
        // With prefetch 1, nothing else arrives until this ack.
        assert!(timeout(Duration::from_millis(30), rx.recv()).await.is_err() || i == 9);
        ch.basic_ack(BasicAckArguments::new(
            msg.deliver.unwrap().delivery_tag(),
            false,
        ))
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn nack_requeues_and_marks_redelivered() {
    let (_broker, addr) = start().await;
    let (_conn, ch) = connect(addr).await;
    ch.queue_declare(QueueDeclareArguments::new("retry"))
        .await
        .unwrap();
    publish(&ch, "", "retry", "once more").await;
    let (_tag, mut rx) = ch
        .basic_consume_rx(
            BasicConsumeArguments::new("retry", "c")
                .manual_ack(true)
                .finish(),
        )
        .await
        .unwrap();
    let first = timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let d = first.deliver.unwrap();
    assert!(!d.redelivered());
    ch.basic_nack(BasicNackArguments::new(d.delivery_tag(), false, true))
        .await
        .unwrap();
    let second = timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(second.deliver.unwrap().redelivered());
}

#[tokio::test]
async fn topic_routing() {
    let (_broker, addr) = start().await;
    let (_conn, ch) = connect(addr).await;
    ch.exchange_declare(ExchangeDeclareArguments::new("topic_logs", "topic"))
        .await
        .unwrap();
    for (q, key) in [("critical", "*.critical"), ("kern", "kern.*")] {
        ch.queue_declare(QueueDeclareArguments::new(q))
            .await
            .unwrap();
        ch.queue_bind(QueueBindArguments::new(q, "topic_logs", key))
            .await
            .unwrap();
    }
    publish(
        &ch,
        "topic_logs",
        "kern.critical",
        "A critical kernel error",
    )
    .await;
    publish(&ch, "topic_logs", "app.critical", "app down").await;
    publish(&ch, "topic_logs", "kern.info", "boot").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let count = |q: &str| {
        let ch = &ch;
        let q = q.to_string();
        async move {
            let mut n = 0;
            while ch
                .basic_get(BasicGetArguments::new(&q).no_ack(true).finish())
                .await
                .unwrap()
                .is_some()
            {
                n += 1;
            }
            n
        }
    };
    assert_eq!(count("critical").await, 2);
    assert_eq!(count("kern").await, 2);
}

#[tokio::test]
async fn basic_get_reports_remaining_messages() {
    let (_broker, addr) = start().await;
    let (_conn, ch) = connect(addr).await;
    ch.queue_declare(QueueDeclareArguments::new("g"))
        .await
        .unwrap();
    assert!(
        ch.basic_get(BasicGetArguments::new("g"))
            .await
            .unwrap()
            .is_none()
    );
    publish(&ch, "", "g", "a").await;
    publish(&ch, "", "g", "b").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (ok, _props, body) = ch
        .basic_get(BasicGetArguments::new("g").no_ack(true).finish())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(body, b"a");
    assert_eq!(ok.message_count(), 1);
}

#[tokio::test]
async fn closed_connections_requeue_unacked_messages() {
    let (broker, addr) = start().await;
    let (conn, ch) = connect(addr).await;
    ch.queue_declare(QueueDeclareArguments::new("hold"))
        .await
        .unwrap();
    publish(&ch, "", "hold", "m").await;
    let (_tag, mut rx) = ch
        .basic_consume_rx(
            BasicConsumeArguments::new("hold", "c")
                .manual_ack(true)
                .finish(),
        )
        .await
        .unwrap();
    let _ = timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    conn.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let q = broker.vhost("/").unwrap().queue("hold", None).unwrap();
    assert_eq!(q.message_count(), 1);
}

#[tokio::test]
async fn wrong_password_is_refused() {
    let (_broker, addr) = start().await;
    let r = Connection::open(&OpenConnectionArguments::new(
        &addr.ip().to_string(),
        addr.port(),
        "guest",
        "wrong",
    ))
    .await;
    assert!(r.is_err());
}

#[tokio::test]
async fn unknown_vhost_is_refused() {
    let (_broker, addr) = start().await;
    let r = Connection::open(
        OpenConnectionArguments::new(&addr.ip().to_string(), addr.port(), "guest", "guest")
            .virtual_host("nope"),
    )
    .await;
    assert!(r.is_err());
}

#[tokio::test]
async fn publishing_to_a_missing_exchange_closes_the_channel_only() {
    let (_broker, addr) = start().await;
    let (conn, ch) = connect(addr).await;
    publish(&ch, "no-such-exchange", "k", "x").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!ch.is_open());
    assert!(conn.is_open());
    let ch2 = conn.open_channel(None).await.unwrap();
    ch2.queue_declare(QueueDeclareArguments::new("still-works"))
        .await
        .unwrap();
}

#[tokio::test]
async fn server_named_exclusive_queue() {
    let (broker, addr) = start().await;
    let (conn, ch) = connect(addr).await;
    let (name, _, _) = ch
        .queue_declare(QueueDeclareArguments::exclusive_server_named())
        .await
        .unwrap()
        .unwrap();
    assert!(name.starts_with("amq.gen-"), "{name}");
    let v = broker.vhost("/").unwrap();
    assert!(v.queue(&name, None).is_err());
    conn.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(v.queue(&name, None).unwrap_err().code, 404);
}

#[tokio::test]
async fn large_messages_span_frames() {
    let (_broker, addr) = start().await;
    let (_conn, ch) = connect(addr).await;
    ch.queue_declare(QueueDeclareArguments::new("big"))
        .await
        .unwrap();
    let body: String = "x".repeat(1_000_000);
    publish(&ch, "", "big", &body).await;
    let (_tag, mut rx) = ch
        .basic_consume_rx(
            BasicConsumeArguments::new("big", "c")
                .manual_ack(false)
                .finish(),
        )
        .await
        .unwrap();
    let msg = timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.content.unwrap().len(), 1_000_000);
}

#[tokio::test]
async fn crossing_channel_close_keeps_the_connection() {
    let (_broker, addr) = start().await;
    let (conn, ch1) = connect(addr).await;
    let ch2 = conn.open_channel(None).await.unwrap();
    // Provoke a channel error and close the channel at the same time.
    publish(&ch1, "nonexisting-exchange", "", "x").await;
    let _ = ch1.close().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(conn.is_open());
    ch2.exchange_declare(ExchangeDeclareArguments::new("test-channel-still-exists", "direct"))
        .await
        .unwrap();
}

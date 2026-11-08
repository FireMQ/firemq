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

/// A minimal raw AMQP client, for exact frame sequences.
struct Raw {
    stream: tokio::net::TcpStream,
    buf: bytes::BytesMut,
}

impl Raw {
    async fn connect(addr: SocketAddr) -> Raw {
        use firemq_amqp_codec::methods::*;
        use tokio::io::AsyncWriteExt;
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(firemq_amqp_codec::PROTOCOL_HEADER)
            .await
            .unwrap();
        let mut raw = Raw {
            stream,
            buf: bytes::BytesMut::new(),
        };
        raw.expect(0, "connection.start").await;
        let mut caps = firemq_amqp_codec::FieldTable::new();
        caps.insert("connection.blocked", true);
        let mut client_properties = firemq_amqp_codec::FieldTable::new();
        client_properties.insert("capabilities", caps);
        raw.send(
            0,
            ConnectionStartOk {
                client_properties,
                mechanism: "PLAIN".into(),
                response: b"\0guest\0guest".to_vec().into(),
                locale: "en_US".into(),
            },
        )
        .await;
        raw.expect(0, "connection.tune").await;
        raw.send(
            0,
            ConnectionTuneOk {
                channel_max: 10,
                frame_max: 131072,
                heartbeat: 0,
            },
        )
        .await;
        raw.send(
            0,
            ConnectionOpen {
                virtual_host: "/".into(),
                ..Default::default()
            },
        )
        .await;
        raw.expect(0, "connection.open-ok").await;
        raw
    }

    async fn send_bytes(&mut self, out: bytes::BytesMut) {
        use tokio::io::AsyncWriteExt;
        self.stream.write_all(&out).await.unwrap();
    }

    async fn send(&mut self, ch: u16, m: impl Into<firemq_amqp_codec::Method>) {
        let mut out = bytes::BytesMut::new();
        firemq_amqp_codec::encode_method(&mut out, ch, &m.into());
        self.send_bytes(out).await;
    }

    async fn next_method(&mut self) -> (u16, firemq_amqp_codec::Method) {
        use tokio::io::AsyncReadExt;
        loop {
            if let Some(f) = firemq_amqp_codec::decode_frame(&mut self.buf, 0).unwrap() {
                if let firemq_amqp_codec::Frame::Method { channel, method } = f {
                    return (channel, method);
                }
                continue;
            }
            let n = timeout(Duration::from_secs(2), self.stream.read_buf(&mut self.buf))
                .await
                .expect("timed out waiting for a frame")
                .unwrap();
            assert!(n > 0, "broker closed the socket");
        }
    }

    async fn expect(&mut self, ch: u16, name: &str) -> firemq_amqp_codec::Method {
        let (c, m) = self.next_method().await;
        assert_eq!((c, m.name()), (ch, name), "unexpected {m:?}");
        m
    }
}

#[tokio::test]
async fn crossing_channel_close_keeps_the_connection() {
    use firemq_amqp_codec::methods::*;
    let (_broker, addr) = start().await;
    let mut raw = Raw::connect(addr).await;
    for ch in [1, 2] {
        raw.send(ch, ChannelOpen::default()).await;
        raw.expect(ch, "channel.open-ok").await;
    }
    // Publish to a missing exchange and close the channel in the same write, so the
    // broker's channel.close and the client's channel.close cross.
    let mut out = bytes::BytesMut::new();
    firemq_amqp_codec::encode_content(
        &mut out,
        1,
        &BasicPublish {
            exchange: "nonexisting-exchange".into(),
            ..Default::default()
        }
        .into(),
        &Default::default(),
        b"x",
        131072,
    );
    firemq_amqp_codec::encode_method(
        &mut out,
        1,
        &ChannelClose {
            reply_code: 200,
            ..Default::default()
        }
        .into(),
    );
    raw.send_bytes(out).await;
    match raw.expect(1, "channel.close").await {
        firemq_amqp_codec::Method::ChannelClose(c) => assert_eq!(c.reply_code, 404),
        _ => unreachable!(),
    }
    raw.expect(1, "channel.close-ok").await;
    raw.send(1, ChannelCloseOk {}).await;
    // A late, duplicate close-ok must not hurt either.
    raw.send(1, ChannelCloseOk {}).await;
    raw.send(
        2,
        ExchangeDeclare {
            exchange: "test-channel-still-exists".into(),
            kind: "direct".into(),
            auto_delete: true,
            ..Default::default()
        },
    )
    .await;
    raw.expect(2, "exchange.declare-ok").await;
    // The broker-closed channel can be reopened.
    raw.send(1, ChannelOpen::default()).await;
    raw.expect(1, "channel.open-ok").await;
}

#[tokio::test]
async fn alarms_send_connection_blocked_and_unblocked() {
    let (broker, addr) = start().await;
    let mut raw = Raw::connect(addr).await;
    broker.set_blocked(true);
    raw.expect(0, "connection.blocked").await;
    broker.set_blocked(false);
    raw.expect(0, "connection.unblocked").await;
    // A connection opened during an alarm learns about it right away.
    broker.set_blocked(true);
    let mut late = Raw::connect(addr).await;
    late.expect(0, "connection.blocked").await;
    broker.set_blocked(false);
}

/// The broker batches the acks it decodes from one read. A nack and a channel close in the
/// same read must still see those acks first, or acked messages would come back.
#[tokio::test]
async fn acks_in_one_read_apply_before_a_following_nack_and_close() {
    use firemq_amqp_codec::methods::*;
    let (_broker, addr) = start().await;
    let (_conn, ch) = connect(addr).await;
    ch.queue_declare(QueueDeclareArguments::new("batched"))
        .await
        .unwrap();
    for i in 1..=10 {
        publish(&ch, "", "batched", &format!("m{i}")).await;
    }

    let mut raw = Raw::connect(addr).await;
    raw.send(1, ChannelOpen::default()).await;
    raw.expect(1, "channel.open-ok").await;
    raw.send(
        1,
        BasicConsume {
            queue: "batched".into(),
            consumer_tag: "c".into(),
            ..Default::default()
        },
    )
    .await;
    raw.expect(1, "basic.consume-ok").await;
    for _ in 1..=10 {
        raw.expect(1, "basic.deliver").await;
    }

    // Acks for 1-8, a requeueing nack for 10, then a close that requeues 9: one write.
    let mut out = bytes::BytesMut::new();
    for tag in 1..=8 {
        let ack = BasicAck {
            delivery_tag: tag,
            multiple: false,
        };
        firemq_amqp_codec::encode_method(&mut out, 1, &ack.into());
    }
    let nack = BasicNack {
        delivery_tag: 10,
        multiple: false,
        requeue: true,
    };
    firemq_amqp_codec::encode_method(&mut out, 1, &nack.into());
    let close = ChannelClose {
        reply_code: 200,
        reply_text: "bye".into(),
        ..Default::default()
    };
    firemq_amqp_codec::encode_method(&mut out, 1, &close.into());
    raw.send_bytes(out).await;
    raw.expect(1, "channel.close-ok").await;

    let mut ready = 0;
    for _ in 0..50 {
        let (_, n, _) = ch
            .queue_declare(QueueDeclareArguments::new("batched").passive(true).finish())
            .await
            .unwrap()
            .unwrap();
        ready = n;
        if ready == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        ready, 2,
        "only messages 9 and 10 should be back in the queue"
    );
}

//! End-to-end tests of the FireMQ protocol, using the Rust client and raw WebSocket frames.

use std::net::SocketAddr;
use std::time::Duration;

use firemq_client::{Client, Error, PublishOptions, QueueOptions, SubscribeOptions};
use firemq_proto::{Auth, Command, Hello, RawFrame};
use firemq_server::{Server, ServerConfig};
use futures::{SinkExt, StreamExt};
use tokio::time::timeout;

fn local() -> Option<SocketAddr> {
    Some("127.0.0.1:0".parse().unwrap())
}

async fn start() -> Server {
    Server::start(ServerConfig {
        amqp: local(),
        fire: local(),
        http: local(),
        ..Default::default()
    })
    .await
    .unwrap()
}

async fn client(s: &Server) -> Client {
    Client::connect(&format!("fire://guest:guest@{}", s.fire_addr().unwrap()))
        .await
        .unwrap()
}

#[tokio::test]
async fn publish_with_confirms_then_consume_and_ack() {
    let s = start().await;
    let c = client(&s).await;
    let q = c
        .declare_queue("jobs", QueueOptions::default())
        .await
        .unwrap();
    assert_eq!(q.name, "jobs");
    for i in 0..10 {
        c.publish("", "jobs", format!("job {i}")).await.unwrap();
    }
    let mut sub = c
        .subscribe(
            "jobs",
            SubscribeOptions {
                prefetch: 4,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    for i in 0..10 {
        let m = timeout(Duration::from_secs(2), sub.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.body_str(), Some(format!("job {i}").as_str()));
        m.ack().unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let q = s.broker().vhost("/").unwrap().queue("jobs", None).unwrap();
    assert_eq!(q.message_count(), 0);
    c.close().await.unwrap();
}

#[tokio::test]
async fn credit_limits_unacked_messages() {
    let s = start().await;
    let c = client(&s).await;
    c.declare_queue("slow", QueueOptions::default())
        .await
        .unwrap();
    for i in 0..6 {
        c.publish("", "slow", format!("m{i}")).await.unwrap();
    }
    let mut sub = c
        .subscribe(
            "slow",
            SubscribeOptions {
                prefetch: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let a = sub.next().await.unwrap();
    let b = sub.next().await.unwrap();
    assert!(
        timeout(Duration::from_millis(150), sub.next())
            .await
            .is_err()
    );
    a.ack().unwrap();
    let third = timeout(Duration::from_secs(2), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(third.body_str(), Some("m2"));
    b.ack().unwrap();
    third.ack().unwrap();
}

#[tokio::test]
async fn mandatory_publish_to_nowhere_is_unroutable() {
    let s = start().await;
    let c = client(&s).await;
    let err = c
        .publish_with(
            "",
            "no-such-queue",
            "lost",
            PublishOptions {
                mandatory: true,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Unroutable { .. }), "{err:?}");
    // Without mandatory, the broker confirms and drops it, like RabbitMQ.
    c.publish("", "no-such-queue", "dropped").await.unwrap();
}

#[tokio::test]
async fn full_queue_with_reject_publish_nacks() {
    let s = start().await;
    let c = client(&s).await;
    c.declare_queue(
        "bounded",
        QueueOptions::default()
            .arg("x-max-length", 1)
            .arg("x-overflow", "reject-publish"),
    )
    .await
    .unwrap();
    c.publish("", "bounded", "first").await.unwrap();
    let err = c.publish("", "bounded", "second").await.unwrap_err();
    assert!(matches!(err, Error::Nacked { .. }), "{err:?}");
}

#[tokio::test]
async fn get_returns_messages_then_none() {
    let s = start().await;
    let c = client(&s).await;
    c.declare_queue("g", QueueOptions::default()).await.unwrap();
    assert!(c.get("g", true).await.unwrap().is_none());
    c.publish("", "g", "only").await.unwrap();
    let m = c.get("g", false).await.unwrap().unwrap();
    assert_eq!(m.body_str(), Some("only"));
    m.ack().unwrap();
    assert!(c.get("g", true).await.unwrap().is_none());
}

#[tokio::test]
async fn broker_errors_carry_amqp_codes() {
    let s = start().await;
    let c = client(&s).await;
    c.declare_queue("q", QueueOptions::default()).await.unwrap();
    let err = c
        .declare_queue(
            "q",
            QueueOptions {
                durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(406));
    let err = c
        .bind_queue("q", "missing-exchange", "k")
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(404));
    // The connection stays usable after errors.
    c.publish("", "q", "still fine").await.unwrap();
}

#[tokio::test]
async fn wrong_password_is_refused() {
    let s = start().await;
    let err = Client::connect(&format!("fire://guest:nope@{}", s.fire_addr().unwrap()))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), Some(403));
}

#[tokio::test]
async fn topic_exchange_routing() {
    let s = start().await;
    let c = client(&s).await;
    c.declare_exchange("orders", "topic", Default::default())
        .await
        .unwrap();
    let q = c
        .declare_queue("", QueueOptions::temporary())
        .await
        .unwrap();
    assert!(q.name.starts_with("amq.gen-"));
    c.bind_queue(&q.name, "orders", "order.*.eu").await.unwrap();
    c.publish("orders", "order.created.eu", "yes")
        .await
        .unwrap();
    c.publish("orders", "order.created.us", "no").await.unwrap();
    let m = c.get(&q.name, true).await.unwrap().unwrap();
    assert_eq!(m.body_str(), Some("yes"));
    assert!(c.get(&q.name, true).await.unwrap().is_none());
}

#[tokio::test]
async fn unacked_messages_return_when_a_client_disconnects() {
    let s = start().await;
    let c1 = client(&s).await;
    c1.declare_queue("handoff", QueueOptions::default())
        .await
        .unwrap();
    c1.publish("", "handoff", "work").await.unwrap();
    let mut sub = c1
        .subscribe("handoff", SubscribeOptions::default())
        .await
        .unwrap();
    let m = sub.next().await.unwrap();
    assert!(!m.redelivered);
    c1.close().await.unwrap();
    let c2 = client(&s).await;
    let mut sub = c2
        .subscribe("handoff", SubscribeOptions::default())
        .await
        .unwrap();
    let m = timeout(Duration::from_secs(2), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert!(m.redelivered);
}

#[tokio::test]
async fn json_messages() {
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Order {
        id: u32,
        item: String,
    }
    let s = start().await;
    let c = client(&s).await;
    c.declare_queue("orders", QueueOptions::default())
        .await
        .unwrap();
    let order = Order {
        id: 7,
        item: "book".into(),
    };
    c.publish_json("", "orders", &order).await.unwrap();
    let m = c.get("orders", true).await.unwrap().unwrap();
    assert_eq!(
        m.properties.content_type.as_deref(),
        Some("application/json")
    );
    assert_eq!(m.json::<Order>().unwrap(), order);
}

#[tokio::test]
async fn websocket_transport_speaks_the_same_frames() {
    let s = start().await;
    let url = format!("ws://{}/fire", s.http_addr().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    use tokio_tungstenite::tungstenite::Message as Ws;
    let hello = Command::Hello(Hello {
        ver: 1,
        vhost: "/".into(),
        auth: Auth::plain("guest", "guest"),
        ..Default::default()
    });
    ws.send(Ws::Binary(hello.to_ws())).await.unwrap();
    let reply = |msg: Ws| match msg {
        Ws::Binary(b) => Command::decode(RawFrame::from_ws(b).unwrap()).unwrap(),
        other => panic!("unexpected {other:?}"),
    };
    let welcome = reply(ws.next().await.unwrap().unwrap());
    assert!(matches!(welcome, Command::Welcome(_)), "{welcome:?}");
    let declare = Command::DeclareQueue(firemq_proto::DeclareQueue {
        rid: 1,
        name: "browser".into(),
        ..Default::default()
    });
    ws.send(Ws::Binary(declare.to_ws())).await.unwrap();
    match reply(ws.next().await.unwrap().unwrap()) {
        Command::Ok(ok) => assert_eq!((ok.rid, ok.name.as_deref()), (1, Some("browser"))),
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn native_and_amqp_share_one_broker() {
    use amqprs::BasicProperties;
    use amqprs::channel::{BasicConsumeArguments, BasicPublishArguments, QueueDeclareArguments};
    use amqprs::connection::{Connection, OpenConnectionArguments};

    let s = start().await;
    let c = client(&s).await;
    c.declare_queue(
        "bridge",
        QueueOptions {
            durable: false,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let addr = s.amqp_addr().unwrap();
    let conn = Connection::open(&OpenConnectionArguments::new(
        &addr.ip().to_string(),
        addr.port(),
        "guest",
        "guest",
    ))
    .await
    .unwrap();
    let ch = conn.open_channel(None).await.unwrap();
    ch.queue_declare(QueueDeclareArguments::new("bridge"))
        .await
        .unwrap();

    // FireMQ protocol -> AMQP consumer, with headers preserved.
    let mut opts = PublishOptions::default();
    opts.headers.insert("source".into(), "native".into());
    c.publish_with("", "bridge", "from native", opts)
        .await
        .unwrap();
    let (_tag, mut rx) = ch
        .basic_consume_rx(
            BasicConsumeArguments::new("bridge", "amqp")
                .manual_ack(false)
                .finish(),
        )
        .await
        .unwrap();
    let got = timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.content.unwrap(), b"from native");
    let headers = got.basic_properties.unwrap().headers().cloned().unwrap();
    assert!(format!("{headers:?}").contains("native"));

    // AMQP publisher -> FireMQ consumer.
    ch.close().await.unwrap();
    let ch = conn.open_channel(None).await.unwrap();
    ch.queue_declare(QueueDeclareArguments::new("bridge2"))
        .await
        .unwrap();
    ch.basic_publish(
        BasicProperties::default()
            .with_correlation_id("c-1")
            .finish(),
        b"from amqp".to_vec(),
        BasicPublishArguments::new("", "bridge2"),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let m = c.get("bridge2", true).await.unwrap().unwrap();
    assert_eq!(m.body_str(), Some("from amqp"));
    assert_eq!(m.properties.correlation_id.as_deref(), Some("c-1"));
}

//! The Rust client reconnects and restores the topology and subscriptions it declared.

use std::net::SocketAddr;
use std::time::Duration;

use firemq::Server;
use firemq_client::{
    Client, ConnectOptions, ExchangeOptions, Message, QueueOptions, Reconnect, SubscribeOptions,
    Subscription,
};

async fn broker(fire: Option<SocketAddr>) -> Server {
    let mut b = firemq::builder().ephemeral().random_ports();
    if fire.is_some() {
        b = b.fire(fire);
    }
    b.start().await.unwrap()
}

fn options(url: &str) -> ConnectOptions {
    let mut o = ConnectOptions::from_url(url).unwrap();
    o.reconnect = Some(Reconnect {
        initial_delay: Duration::from_millis(20),
        max_delay: Duration::from_millis(200),
        ..Default::default()
    });
    o
}

async fn next(sub: &mut Subscription) -> Message {
    tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("no message within 5 s")
        .expect("the subscription ended")
}

async fn until(what: &str, f: impl Fn() -> bool) {
    for _ in 0..250 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting until {what}");
}

/// Asks the broker to close every client connection, like `firemq ctl close-connection`.
fn force_close(server: &Server) {
    for c in server.broker().connections() {
        let _ = c.events.send(firemq::core::ConnEvent::Close {
            code: 320,
            reason: "CONNECTION_FORCED - test".into(),
        });
    }
}

fn transient() -> QueueOptions {
    QueueOptions {
        durable: false,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topology_and_subscriptions_survive_a_broker_restart() {
    let server = broker(None).await;
    let addr = server.fire_addr().unwrap();
    let url = format!("fire://{addr}");
    let client = Client::connect_with(options(&url)).await.unwrap();
    let transient_exchange = ExchangeOptions {
        durable: false,
        ..Default::default()
    };
    client
        .declare_exchange("events", "topic", transient_exchange)
        .await
        .unwrap();
    client
        .declare_queue("audit", QueueOptions::temporary())
        .await
        .unwrap();
    client
        .bind_queue("audit", "events", "order.*")
        .await
        .unwrap();
    let anon = client
        .declare_queue("", QueueOptions::temporary())
        .await
        .unwrap();
    client.bind_queue(&anon.name, "events", "#").await.unwrap();
    let mut audit = client
        .subscribe("audit", SubscribeOptions::default())
        .await
        .unwrap();
    let no_ack = SubscribeOptions {
        no_ack: true,
        ..Default::default()
    };
    let mut all = client.subscribe(&anon.name, no_ack).await.unwrap();

    client
        .publish("events", "order.created", "before")
        .await
        .unwrap();
    let m = next(&mut audit).await;
    assert_eq!(m.body_str(), Some("before"));
    m.ack().unwrap();
    assert_eq!(next(&mut all).await.body_str(), Some("before"));

    // An ephemeral broker forgets everything, so the client has to declare it all again.
    server.shutdown().await;
    until("the client notices", || !client.is_connected()).await;
    let server = broker(Some(addr)).await;
    until("the client reconnects", || client.is_connected()).await;
    assert!(!client.is_closed());

    let publisher = Client::connect(&url).await.unwrap();
    publisher
        .publish("events", "order.paid", "after")
        .await
        .unwrap();
    let m = next(&mut audit).await;
    assert_eq!(m.body_str(), Some("after"));
    m.ack().unwrap();
    assert_eq!(next(&mut all).await.body_str(), Some("after"));
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_waits_for_the_broker_to_come_back() {
    let server = broker(None).await;
    let addr = server.fire_addr().unwrap();
    let client = Client::connect_with(options(&format!("fire://{addr}")))
        .await
        .unwrap();
    client.declare_queue("jobs", transient()).await.unwrap();
    server.shutdown().await;
    until("the client notices", || !client.is_connected()).await;

    let pending = tokio::spawn({
        let client = client.clone();
        async move { client.publish("", "jobs", "sent while down").await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !pending.is_finished(),
        "publish returned while disconnected"
    );

    let server = broker(Some(addr)).await;
    tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .expect("publish still waiting")
        .unwrap()
        .unwrap();
    let m = client.get("jobs", true).await.unwrap().unwrap();
    assert_eq!(m.body_str(), Some("sent while down"));
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unacked_messages_are_redelivered_after_a_forced_close() {
    let server = broker(None).await;
    let client = Client::connect_with(options(&format!("fire://{}", server.fire_addr().unwrap())))
        .await
        .unwrap();
    client.declare_queue("work", transient()).await.unwrap();
    for body in ["one", "two"] {
        client.publish("", "work", body).await.unwrap();
    }
    let mut sub = client
        .subscribe("work", SubscribeOptions::default())
        .await
        .unwrap();
    let first = next(&mut sub).await;
    assert!(!first.redelivered);
    // Wait until the second delivery is buffered in the subscription too.
    tokio::time::sleep(Duration::from_millis(100)).await;

    force_close(&server);
    until("the client notices", || !client.is_connected()).await;
    until("the client reconnects", || client.is_connected()).await;

    // The old copy's ack goes nowhere: the broker put the message back.
    first.ack().unwrap();
    let mut seen = Vec::new();
    for _ in 0..2 {
        let m = next(&mut sub).await;
        assert!(
            m.redelivered,
            "{:?} came back without the redelivered flag",
            m
        );
        seen.push(m.body_str().unwrap().to_string());
        m.ack().unwrap();
    }
    seen.sort();
    assert_eq!(seen, ["one", "two"]);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(client.get("work", true).await.unwrap().is_none());
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_stops_reconnecting_and_ends_subscriptions() {
    let server = broker(None).await;
    let client = Client::connect_with(options(&format!("fire://{}", server.fire_addr().unwrap())))
        .await
        .unwrap();
    client.declare_queue("q", transient()).await.unwrap();
    let mut sub = client.subscribe("q", Default::default()).await.unwrap();
    client.close().await.unwrap();
    assert!(client.is_closed());
    assert!(sub.next().await.is_none());
    let err = client.publish("", "q", "x").await.unwrap_err();
    assert!(matches!(err, firemq_client::Error::Closed(_)), "{err}");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(server.broker().connections().is_empty());
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_reconnect_a_lost_connection_stays_closed() {
    let server = broker(None).await;
    let mut opts = options(&format!("fire://{}", server.fire_addr().unwrap()));
    opts.reconnect = None;
    let client = Client::connect_with(opts).await.unwrap();
    client.declare_queue("q", transient()).await.unwrap();
    force_close(&server);
    until("the client notices", || client.is_closed()).await;
    let err = client.publish("", "q", "x").await.unwrap_err();
    assert!(err.to_string().contains("CONNECTION_FORCED"), "{err}");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn calls_fail_after_the_wait_while_the_broker_is_down() {
    let server = broker(None).await;
    let mut opts = options(&format!("fire://{}", server.fire_addr().unwrap()));
    opts.reconnect = Some(Reconnect {
        wait: Duration::from_millis(200),
        max_attempts: Some(3),
        initial_delay: Duration::from_millis(20),
        max_delay: Duration::from_millis(50),
    });
    let client = Client::connect_with(opts).await.unwrap();
    server.shutdown().await;
    let err = client.declare_queue("q", transient()).await.unwrap_err();
    assert!(matches!(err, firemq_client::Error::Closed(_)), "{err}");
    until("reconnecting gives up", || client.is_closed()).await;
    let err = client.publish("", "q", "x").await.unwrap_err();
    assert!(err.to_string().contains("gave up reconnecting"), "{err}");
}

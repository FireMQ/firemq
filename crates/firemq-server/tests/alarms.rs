//! Resource alarms pause publishers.

use std::net::SocketAddr;
use std::time::Duration;

use firemq_client::{Client, QueueOptions};
use firemq_server::{Server, ServerConfig};
use tokio::time::timeout;

#[tokio::test]
async fn blocked_publishers_wait_until_the_alarm_clears() {
    let local: Option<SocketAddr> = Some("127.0.0.1:0".parse().unwrap());
    let s = Server::start(ServerConfig {
        amqp: None,
        amqps: None,
        fire: local,
        fires: None,
        http: None,
        alarms: None,
        ..Default::default()
    })
    .await
    .unwrap();
    let c = Client::connect(&format!("fire://{}", s.fire_addr().unwrap()))
        .await
        .unwrap();
    c.declare_queue("q", QueueOptions::default()).await.unwrap();
    c.publish("", "q", "before").await.unwrap();

    s.broker().set_blocked(true);
    let pending = {
        let c = c.clone();
        tokio::spawn(async move { c.publish("", "q", "during").await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !pending.is_finished(),
        "publish was confirmed while blocked"
    );

    s.broker().set_blocked(false);
    timeout(Duration::from_secs(2), pending)
        .await
        .expect("publish resumes after the alarm clears")
        .unwrap()
        .unwrap();
}

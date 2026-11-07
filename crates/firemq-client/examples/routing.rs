//! Routing: a direct exchange delivers a message to the queues bound with its exact key.

use firemq_client::{Client, ExchangeOptions, QueueOptions, SubscribeOptions};

#[tokio::main]
async fn main() -> Result<(), firemq_client::Error> {
    let url = std::env::var("FIREMQ_URL").unwrap_or_else(|_| "fire://localhost".into());
    let client = Client::connect(&url).await?;
    client
        .declare_exchange("direct_logs.rust", "direct", ExchangeOptions::default())
        .await?;
    let queue = client.declare_queue("", QueueOptions::temporary()).await?;
    for severity in ["warning", "error"] {
        client
            .bind_queue(&queue.name, "direct_logs.rust", severity)
            .await?;
    }

    for (severity, text) in [
        ("info", "disk 40% full"),
        ("warning", "disk 85% full"),
        ("error", "disk full"),
    ] {
        client
            .publish("direct_logs.rust", severity, format!("{severity}: {text}"))
            .await?;
    }

    // The info message matched no binding, so only two arrive.
    let mut sub = client
        .subscribe(&queue.name, SubscribeOptions::default())
        .await?;
    for _ in 0..2 {
        let Some(msg) = sub.next().await else { break };
        println!(" [x] {}", msg.body_str().unwrap_or_default());
        msg.ack()?;
    }
    client.close().await
}

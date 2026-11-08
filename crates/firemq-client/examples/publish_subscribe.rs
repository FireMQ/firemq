//! Publish/subscribe: a fanout exchange copies every message to every bound queue.

use firemq_client::{Client, ExchangeOptions, QueueOptions, SubscribeOptions};

#[tokio::main]
async fn main() -> Result<(), firemq_client::Error> {
    let url = std::env::var("FIREMQ_URL").unwrap_or_else(|_| "fire://localhost".into());
    let client = Client::connect(&url).await?;
    client
        .declare_exchange("logs.rust", "fanout", ExchangeOptions::default())
        .await?;

    // Two subscribers, each with its own temporary queue.
    let mut subs = Vec::new();
    for _ in 0..2 {
        let queue = client.declare_queue("", QueueOptions::temporary()).await?;
        client.bind_queue(&queue.name, "logs.rust", "").await?;
        subs.push(
            client
                .subscribe(&queue.name, SubscribeOptions::default())
                .await?,
        );
    }

    // The routing key is ignored by fanout exchanges.
    client.publish("logs.rust", "", "info: hello").await?;

    for (n, sub) in subs.iter_mut().enumerate() {
        let Some(msg) = sub.next().await else { break };
        println!(" [subscriber {n}] {}", msg.body_str().unwrap_or_default());
        msg.ack()?;
    }
    client.close().await
}

//! Topics: route with word patterns; `*` matches one word, `#` matches zero or more.

use firemq_client::{Client, QueueOptions, SubscribeOptions};

#[tokio::main]
async fn main() -> Result<(), firemq_client::Error> {
    let url = std::env::var("FIREMQ_URL").unwrap_or_else(|_| "fire://localhost".into());
    let client = Client::connect(&url).await?;
    client
        .declare_exchange("topic_logs.rust", "topic", Default::default())
        .await?;
    let queue = client.declare_queue("", QueueOptions::temporary()).await?;
    client
        .bind_queue(&queue.name, "topic_logs.rust", "*.critical")
        .await?;

    for key in ["kern.critical", "kern.info", "app.critical"] {
        client.publish("topic_logs.rust", key, key).await?;
    }

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

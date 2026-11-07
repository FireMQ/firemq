//! Publish and consume one message.
//!
//! Start a broker with `firemq`, then run `cargo run -p firemq-client --example hello`.

use firemq_client::{Client, QueueOptions, SubscribeOptions};

#[tokio::main]
async fn main() -> Result<(), firemq_client::Error> {
    let url = std::env::var("FIREMQ_URL").unwrap_or_else(|_| "fire://localhost".into());
    let client = Client::connect(&url).await?;
    client
        .declare_queue("hello.rust", QueueOptions::default())
        .await?;
    client.publish("", "hello.rust", "Hello World!").await?;
    println!(" [x] Sent 'Hello World!'");

    let mut sub = client
        .subscribe("hello.rust", SubscribeOptions::default())
        .await?;
    if let Some(msg) = sub.next().await {
        println!(" [x] Received '{}'", msg.body_str().unwrap_or_default());
        msg.ack()?;
    }
    client.close().await
}

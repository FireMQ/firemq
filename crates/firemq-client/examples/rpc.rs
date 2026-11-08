//! RPC: a request carries `reply_to` and `correlation_id`; the server answers there.

use firemq_client::{Client, PublishOptions, QueueOptions, SubscribeOptions};

#[tokio::main]
async fn main() -> Result<(), firemq_client::Error> {
    let url = std::env::var("FIREMQ_URL").unwrap_or_else(|_| "fire://localhost".into());
    let client = Client::connect(&url).await?;
    client
        .declare_queue("rpc_queue.rust", QueueOptions::default())
        .await?;

    // The server squares numbers.
    let server = client.clone();
    let mut requests = server
        .subscribe("rpc_queue.rust", SubscribeOptions::default())
        .await?;
    tokio::spawn(async move {
        if let Some(req) = requests.next().await {
            let n: u64 = req.body_str().unwrap_or("0").parse().unwrap_or(0);
            let mut reply = PublishOptions::default();
            reply.properties.correlation_id = req.properties.correlation_id.clone();
            let to = req.properties.reply_to.clone().unwrap_or_default();
            let _ = server
                .publish_with("", &to, (n * n).to_string(), reply)
                .await;
            let _ = req.ack();
        }
    });

    // The client sends a request and waits for the matching reply.
    let replies = client.declare_queue("", QueueOptions::temporary()).await?;
    let mut answers = client
        .subscribe(&replies.name, SubscribeOptions::default())
        .await?;
    let mut request = PublishOptions::default();
    request.properties.reply_to = Some(replies.name.clone());
    request.properties.correlation_id = Some("req-1".into());
    client
        .publish_with("", "rpc_queue.rust", "7", request)
        .await?;
    while let Some(answer) = answers.next().await {
        if answer.properties.correlation_id.as_deref() == Some("req-1") {
            println!(" [.] square(7) = {}", answer.body_str().unwrap_or_default());
            answer.ack()?;
            break;
        }
    }
    client.close().await
}

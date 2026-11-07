//! Work queues: share tasks between workers, a few unacknowledged tasks each.

use firemq_client::{Client, QueueOptions, SubscribeOptions};

#[tokio::main]
async fn main() -> Result<(), firemq_client::Error> {
    let url = std::env::var("FIREMQ_URL").unwrap_or_else(|_| "fire://localhost".into());
    let client = Client::connect(&url).await?;
    client
        .declare_queue("task_queue.rust", QueueOptions::default())
        .await?;
    for n in 1..=3 {
        // Resolves once the broker has written the task to disk.
        client
            .publish("", "task_queue.rust", format!("task {n}"))
            .await?;
    }

    let opts = SubscribeOptions {
        prefetch: 1,
        ..Default::default()
    };
    let mut tasks = client.subscribe("task_queue.rust", opts).await?;
    for _ in 0..3 {
        let Some(task) = tasks.next().await else {
            break;
        };
        println!(" [x] Done {}", task.body_str().unwrap_or_default());
        task.ack()?;
    }
    client.close().await
}

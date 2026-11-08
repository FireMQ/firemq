//! Crash test: kill -9 the broker while it is confirming persistent publishes, restart it,
//! and check that every confirmed message survived.

#![cfg(unix)]

use std::collections::HashSet;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use firemq_client::{Client, ConnectOptions, PublishOptions, QueueOptions};
use futures::StreamExt;
use futures::stream::FuturesUnordered;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Ports {
    amqp: u16,
    fire: u16,
    http: u16,
}

fn start(dir: &std::path::Path, p: &Ports) -> Child {
    Command::new(env!("CARGO_BIN_EXE_firemq"))
        .args([
            "--data-dir",
            dir.to_str().unwrap(),
            "--amqp",
            &format!("127.0.0.1:{}", p.amqp),
            "--fire",
            &format!("127.0.0.1:{}", p.fire),
            "--http",
            &format!("127.0.0.1:{}", p.http),
        ])
        .env("FIREMQ_LOG", "warn")
        .stdout(Stdio::null())
        .spawn()
        .expect("start firemq")
}

async fn connect(p: &Ports) -> Client {
    for _ in 0..200 {
        // Each round uses a fresh client, so the client must not reconnect on its own.
        let mut opts = ConnectOptions::from_url(&format!("fire://127.0.0.1:{}", p.fire)).unwrap();
        opts.reconnect = None;
        if let Ok(c) = Client::connect_with(opts).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("broker did not come up");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confirmed_messages_survive_kill_9() {
    let dir = tempfile::tempdir().unwrap();
    let ports = Ports {
        amqp: free_port(),
        fire: free_port(),
        http: free_port(),
    };
    let confirmed = Arc::new(Mutex::new(HashSet::<String>::new()));
    let rounds = 4;
    for round in 0..rounds {
        let mut broker = start(dir.path(), &ports);
        let client = connect(&ports).await;
        client
            .declare_queue("chaos", QueueOptions::default())
            .await
            .unwrap();
        let publisher = {
            let client = client.clone();
            let confirmed = confirmed.clone();
            tokio::spawn(async move {
                let mut in_flight = FuturesUnordered::new();
                for n in 0.. {
                    let id = format!("{round}-{n}");
                    let c = client.clone();
                    in_flight.push(async move {
                        let r = c
                            .publish_with("", "chaos", id.clone(), PublishOptions::default())
                            .await;
                        (id, r)
                    });
                    while in_flight.len() >= 256 {
                        match in_flight.next().await {
                            Some((id, Ok(()))) => {
                                confirmed.lock().unwrap().insert(id);
                            }
                            Some((_, Err(_))) => return,
                            None => break,
                        }
                    }
                }
            })
        };
        let wait = 300 + rand::random::<u64>() % 700;
        tokio::time::sleep(Duration::from_millis(wait)).await;
        broker.kill().unwrap();
        broker.wait().unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), publisher).await;
    }

    let mut broker = start(dir.path(), &ports);
    let client = connect(&ports).await;
    let mut found = HashSet::new();
    while let Some(m) = client.get("chaos", true).await.unwrap() {
        found.insert(m.body_str().unwrap().to_string());
    }
    broker.kill().unwrap();
    let _ = broker.wait();

    let confirmed = confirmed.lock().unwrap();
    assert!(
        confirmed.len() > 100,
        "the test published too little: {}",
        confirmed.len()
    );
    let missing: Vec<_> = confirmed.difference(&found).take(10).collect();
    assert!(
        missing.is_empty(),
        "{} of {} confirmed messages were lost, for example {missing:?}",
        confirmed.difference(&found).count(),
        confirmed.len()
    );
    println!(
        "{} confirmed messages across {rounds} crashes, all {} recovered ({} including unconfirmed)",
        confirmed.len(),
        confirmed.len(),
        found.len()
    );
}

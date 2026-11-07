//! Behaviour tests for the broker core, driven through the same API the protocol adapters
//! use.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use firemq_core::*;
use tokio::sync::mpsc;
use tokio::time::timeout;

struct Conn {
    id: u64,
    deliveries_tx: mpsc::Sender<Delivery>,
    deliveries: mpsc::Receiver<Delivery>,
    events_tx: mpsc::UnboundedSender<ConnEvent>,
    events: mpsc::UnboundedReceiver<ConnEvent>,
}

impl Conn {
    fn new(broker: &Broker, buffer: usize) -> Conn {
        let (deliveries_tx, deliveries) = mpsc::channel(buffer);
        let (events_tx, events) = mpsc::unbounded_channel();
        Conn {
            id: broker.next_id(),
            deliveries_tx,
            deliveries,
            events_tx,
            events,
        }
    }

    fn spec(&self, broker: &Broker, limit: Limit, no_ack: bool) -> ConsumerSpec {
        ConsumerSpec {
            id: broker.next_id(),
            tag: "ctag".into(),
            owner: Owner {
                conn: self.id,
                channel: 1,
            },
            no_ack,
            exclusive: false,
            limit,
            shared_limit: None,
            arguments: FieldTable::new(),
            deliveries: self.deliveries_tx.clone(),
            events: self.events_tx.clone(),
        }
    }

    async fn next(&mut self) -> Delivery {
        timeout(Duration::from_secs(2), self.deliveries.recv())
            .await
            .expect("timed out waiting for a delivery")
            .expect("delivery channel closed")
    }

    async fn nothing(&mut self) {
        assert!(
            timeout(Duration::from_millis(100), self.deliveries.recv())
                .await
                .is_err(),
            "expected no delivery"
        );
    }

    async fn event(&mut self) -> ConnEvent {
        timeout(Duration::from_secs(2), self.events.recv())
            .await
            .expect("timed out waiting for an event")
            .expect("event channel closed")
    }
}

fn msg(body: &str) -> Arc<Message> {
    Arc::new(Message::new(
        "",
        "",
        BasicProperties::default(),
        Bytes::copy_from_slice(body.as_bytes()),
    ))
}

fn body(d: &Delivery) -> String {
    String::from_utf8(d.message.body.to_vec()).unwrap()
}

fn queue(v: &VHost, name: &str, args: FieldTable) -> QueueHandle {
    v.declare_queue(
        QueueOptions {
            name: name.into(),
            arguments: args,
            ..Default::default()
        },
        None,
    )
    .unwrap()
    .queue
}

async fn publish_to(v: &VHost, exchange: &str, key: &str, text: &str) -> usize {
    let qs = v.route(exchange, key, None).unwrap();
    for q in &qs {
        q.publish(msg(text), None).await;
    }
    qs.len()
}

#[tokio::test]
async fn publish_consume_ack() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    let q = queue(&v, "jobs", FieldTable::new());
    for i in 0..3 {
        assert_eq!(publish_to(&v, "", "jobs", &format!("m{i}")).await, 1);
    }
    let mut c = Conn::new(&broker, 16);
    q.consume(c.spec(&broker, Limit::Prefetch(0), false))
        .await
        .unwrap();
    let mut seqs = Vec::new();
    for i in 0..3 {
        let d = c.next().await;
        assert_eq!(body(&d), format!("m{i}"));
        assert!(!d.redelivered);
        seqs.push(d.seq);
    }
    q.ack(
        Owner {
            conn: c.id,
            channel: 1,
        },
        seqs,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(q.message_count(), 0);
    assert_eq!(
        q.stats.unacked.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn requeued_messages_keep_their_order_and_come_first() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    let q = queue(&v, "q", FieldTable::new());
    for i in 1..=4 {
        publish_to(&v, "", "q", &format!("m{i}")).await;
    }
    let mut c = Conn::new(&broker, 16);
    q.consume(c.spec(&broker, Limit::Prefetch(3), false))
        .await
        .unwrap();
    let d1 = c.next().await;
    let d2 = c.next().await;
    let d3 = c.next().await;
    c.nothing().await;
    q.reject(d1.owner, vec![d2.seq, d1.seq], true).await;
    let r1 = c.next().await;
    let r2 = c.next().await;
    assert_eq!((body(&r1), r1.redelivered), ("m1".into(), true));
    assert_eq!((body(&r2), r2.redelivered), ("m2".into(), true));
    q.ack(d3.owner, vec![d3.seq, r1.seq, r2.seq]).await;
    assert_eq!(body(&c.next().await), "m4");
}

#[tokio::test]
async fn prefetch_limits_unacked_deliveries() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    let q = queue(&v, "q", FieldTable::new());
    for i in 0..5 {
        publish_to(&v, "", "q", &format!("m{i}")).await;
    }
    let mut c = Conn::new(&broker, 16);
    q.consume(c.spec(&broker, Limit::Prefetch(2), false))
        .await
        .unwrap();
    let a = c.next().await;
    let _b = c.next().await;
    c.nothing().await;
    q.ack(a.owner, vec![a.seq]).await;
    assert_eq!(body(&c.next().await), "m2");
    c.nothing().await;
}

#[tokio::test]
async fn credit_mode_needs_explicit_credit() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    let q = queue(&v, "q", FieldTable::new());
    for i in 0..3 {
        publish_to(&v, "", "q", &format!("m{i}")).await;
    }
    let mut c = Conn::new(&broker, 16);
    let spec = c.spec(&broker, Limit::Credit(1), true);
    let id = spec.id;
    q.consume(spec).await.unwrap();
    assert_eq!(body(&c.next().await), "m0");
    c.nothing().await;
    q.credit(id, 2).await;
    assert_eq!(body(&c.next().await), "m1");
    assert_eq!(body(&c.next().await), "m2");
}

#[tokio::test]
async fn full_delivery_buffer_parks_the_consumer_without_losing_messages() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    let q = queue(&v, "q", FieldTable::new());
    let mut c = Conn::new(&broker, 1);
    q.consume(c.spec(&broker, Limit::Prefetch(0), true))
        .await
        .unwrap();
    for i in 0..50 {
        publish_to(&v, "", "q", &format!("m{i}")).await;
    }
    for i in 0..50 {
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(body(&c.next().await), format!("m{i}"));
    }
}

#[tokio::test]
async fn consumers_share_work_round_robin() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    let q = queue(&v, "q", FieldTable::new());
    let mut a = Conn::new(&broker, 16);
    let mut b = Conn::new(&broker, 16);
    q.consume(a.spec(&broker, Limit::Prefetch(0), true))
        .await
        .unwrap();
    q.consume(b.spec(&broker, Limit::Prefetch(0), true))
        .await
        .unwrap();
    for i in 0..4 {
        publish_to(&v, "", "q", &format!("m{i}")).await;
    }
    let got_a = [body(&a.next().await), body(&a.next().await)];
    let got_b = [body(&b.next().await), body(&b.next().await)];
    assert_eq!(got_a, ["m0", "m2"]);
    assert_eq!(got_b, ["m1", "m3"]);
}

#[tokio::test]
async fn exchanges_route_like_rabbitmq() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    for (name, kind) in [
        ("logs", "fanout"),
        ("direct", "direct"),
        ("topics", "topic"),
        ("hdr", "headers"),
    ] {
        v.declare_exchange(ExchangeOptions {
            name: name.into(),
            kind: kind.into(),
            ..Default::default()
        })
        .unwrap();
    }
    let a = queue(&v, "a", FieldTable::new());
    let b = queue(&v, "b", FieldTable::new());
    let qa = Destination::Queue("a".into());
    let qb = Destination::Queue("b".into());
    v.bind("logs", qa.clone(), "", FieldTable::new(), None)
        .unwrap();
    v.bind("logs", qb.clone(), "", FieldTable::new(), None)
        .unwrap();
    v.bind("direct", qa.clone(), "error", FieldTable::new(), None)
        .unwrap();
    v.bind("topics", qb.clone(), "*.critical", FieldTable::new(), None)
        .unwrap();
    let mut h = FieldTable::new();
    h.insert("x-match", "any");
    h.insert("format", "pdf");
    v.bind("hdr", qa.clone(), "", h, None).unwrap();

    assert_eq!(v.route("logs", "", None).unwrap().len(), 2);
    assert_eq!(v.route("direct", "error", None).unwrap(), vec![a.clone()]);
    assert!(v.route("direct", "info", None).unwrap().is_empty());
    assert_eq!(
        v.route("topics", "kern.critical", None).unwrap(),
        vec![b.clone()]
    );
    let mut headers = FieldTable::new();
    headers.insert("format", "pdf");
    assert_eq!(v.route("hdr", "", Some(&headers)).unwrap(), vec![a.clone()]);
    assert_eq!(v.route("missing", "", None).unwrap_err().code, 404);

    // Exchange-to-exchange and alternate exchanges.
    v.bind(
        "direct",
        Destination::Exchange("logs".into()),
        "all",
        FieldTable::new(),
        None,
    )
    .unwrap();
    assert_eq!(v.route("direct", "all", None).unwrap().len(), 2);
    let mut args = FieldTable::new();
    args.insert("alternate-exchange", "logs");
    v.declare_exchange(ExchangeOptions {
        name: "with-ae".into(),
        kind: "direct".into(),
        arguments: args,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(v.route("with-ae", "nothing-bound", None).unwrap().len(), 2);

    // Deleting a queue removes its bindings.
    v.delete_queue("a", false, false, None).await.unwrap();
    assert!(v.route("direct", "error", None).unwrap().is_empty());
    assert_eq!(v.route("logs", "", None).unwrap(), vec![b]);
}

#[tokio::test]
async fn get_and_purge() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    let q = queue(&v, "q", FieldTable::new());
    let owner = Owner {
        conn: 1,
        channel: 1,
    };
    assert!(q.get(owner, true).await.unwrap().is_none());
    for i in 0..3 {
        publish_to(&v, "", "q", &format!("m{i}")).await;
    }
    let g = q.get(owner, false).await.unwrap().unwrap();
    assert_eq!((g.message.body.as_ref(), g.remaining), (&b"m0"[..], 2));
    assert_eq!(q.purge().await.unwrap(), 2);
    // The unacked message survives the purge and comes back when its owner goes away.
    q.owner_closed(1, None).await;
    let g = q.get(owner, true).await.unwrap().unwrap();
    assert!(g.redelivered);
}

#[tokio::test]
async fn expired_messages_are_dead_lettered() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    v.declare_exchange(ExchangeOptions {
        name: "dlx".into(),
        kind: "fanout".into(),
        ..Default::default()
    })
    .unwrap();
    let dlq = queue(&v, "dead", FieldTable::new());
    v.bind(
        "dlx",
        Destination::Queue("dead".into()),
        "",
        FieldTable::new(),
        None,
    )
    .unwrap();
    let mut args = FieldTable::new();
    args.insert("x-message-ttl", Value::I32(30));
    args.insert("x-dead-letter-exchange", "dlx");
    queue(&v, "work", args);
    publish_to(&v, "", "work", "late").await;
    let mut c = Conn::new(&broker, 4);
    dlq.consume(c.spec(&broker, Limit::Prefetch(0), true))
        .await
        .unwrap();
    let d = c.next().await;
    assert_eq!(body(&d), "late");
    let headers = d.message.properties.headers.as_ref().unwrap();
    let death = headers.get("x-death").unwrap().as_array().unwrap()[0]
        .as_table()
        .unwrap()
        .clone();
    assert_eq!(death.get_str("reason"), Some("expired"));
    assert_eq!(death.get_str("queue"), Some("work"));
    assert_eq!(d.message.exchange, "dlx");
}

#[tokio::test]
async fn rejected_messages_are_dead_lettered_with_routing_key() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    let dlq = queue(&v, "parking", FieldTable::new());
    let mut args = FieldTable::new();
    args.insert("x-dead-letter-exchange", "");
    args.insert("x-dead-letter-routing-key", "parking");
    let q = queue(&v, "work", args);
    publish_to(&v, "", "work", "bad").await;
    let mut c = Conn::new(&broker, 4);
    q.consume(c.spec(&broker, Limit::Prefetch(1), false))
        .await
        .unwrap();
    let d = c.next().await;
    q.reject(d.owner, vec![d.seq], false).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(dlq.message_count(), 1);
}

#[tokio::test]
async fn max_length_drops_head_or_rejects() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    let mut args = FieldTable::new();
    args.insert("x-max-length", Value::I32(2));
    let q = queue(&v, "drop", args);
    for i in 0..5 {
        publish_to(&v, "", "drop", &format!("m{i}")).await;
    }
    let owner = Owner {
        conn: 1,
        channel: 1,
    };
    let first = q.get(owner, true).await.unwrap().unwrap();
    assert_eq!(first.message.body.as_ref(), b"m3");

    let mut args = FieldTable::new();
    args.insert("x-max-length", Value::I32(1));
    args.insert("x-overflow", "reject-publish");
    let q = queue(&v, "reject", args);
    let (tx, mut rx) = mpsc::unbounded_channel();
    q.publish(msg("a"), Some(Ticket::new(tx.clone(), 1, 1)))
        .await;
    q.publish(msg("b"), Some(Ticket::new(tx, 1, 2))).await;
    let mut acks = Vec::new();
    for _ in 0..2 {
        match timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap()
        {
            ConnEvent::Confirm { seq, ack, .. } => acks.push((seq, ack)),
            other => panic!("unexpected {other:?}"),
        }
    }
    acks.sort();
    assert_eq!(acks, [(1, true), (2, false)]);
}

#[tokio::test]
async fn exclusive_queues_belong_to_one_connection() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    let opts = QueueOptions {
        name: "mine".into(),
        exclusive: true,
        ..Default::default()
    };
    v.declare_queue(opts.clone(), Some(1)).unwrap();
    let err = v.declare_queue(opts, Some(2)).unwrap_err();
    assert_eq!(err.code, 405);
    assert_eq!(v.queue("mine", Some(2)).unwrap_err().code, 405);
    v.connection_closed(1).await;
    assert_eq!(v.queue("mine", Some(1)).unwrap_err().code, 404);
}

#[tokio::test]
async fn auto_delete_queue_goes_away_with_its_last_consumer() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    let q = v
        .declare_queue(
            QueueOptions {
                name: "temp".into(),
                auto_delete: true,
                ..Default::default()
            },
            None,
        )
        .unwrap()
        .queue;
    let c = Conn::new(&broker, 4);
    let spec = c.spec(&broker, Limit::Prefetch(0), true);
    let id = spec.id;
    q.consume(spec).await.unwrap();
    q.cancel(id).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(v.queue("temp", None).is_err());
}

#[tokio::test]
async fn deleting_a_queue_cancels_its_consumers() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    let q = queue(&v, "q", FieldTable::new());
    let mut c = Conn::new(&broker, 4);
    let spec = c.spec(&broker, Limit::Prefetch(0), true);
    let id = spec.id;
    q.consume(spec).await.unwrap();
    v.delete_queue("q", false, false, None).await.unwrap();
    match c.event().await {
        ConnEvent::ConsumerCancelled { consumer_id, .. } => assert_eq!(consumer_id, id),
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn transient_publishes_confirm_once_enqueued() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    queue(&v, "a", FieldTable::new());
    queue(&v, "b", FieldTable::new());
    v.declare_exchange(ExchangeOptions {
        name: "fan".into(),
        kind: "fanout".into(),
        ..Default::default()
    })
    .unwrap();
    v.bind(
        "fan",
        Destination::Queue("a".into()),
        "",
        FieldTable::new(),
        None,
    )
    .unwrap();
    v.bind(
        "fan",
        Destination::Queue("b".into()),
        "",
        FieldTable::new(),
        None,
    )
    .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let ticket = Ticket::new(tx, 7, 1);
    for q in v.route("fan", "", None).unwrap() {
        q.publish(msg("x"), Some(ticket.clone())).await;
    }
    drop(ticket);
    match timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap()
    {
        ConnEvent::Confirm { channel, seq, ack } => assert_eq!((channel, seq, ack), (7, 1, true)),
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn redeclaring_with_different_properties_fails() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    queue(&v, "q", FieldTable::new());
    let err = v
        .declare_queue(
            QueueOptions {
                name: "q".into(),
                durable: true,
                ..Default::default()
            },
            None,
        )
        .unwrap_err();
    assert_eq!(err.code, 406);
    assert!(
        err.text.contains("inequivalent arg 'durable'"),
        "{}",
        err.text
    );
    let mut args = FieldTable::new();
    args.insert("x-message-ttl", Value::I32(5));
    let err = v
        .declare_queue(
            QueueOptions {
                name: "q".into(),
                arguments: args,
                ..Default::default()
            },
            None,
        )
        .unwrap_err();
    assert!(err.text.contains("x-message-ttl"));
    v.declare_exchange(ExchangeOptions {
        name: "e".into(),
        kind: "direct".into(),
        ..Default::default()
    })
    .unwrap();
    let err = v
        .declare_exchange(ExchangeOptions {
            name: "e".into(),
            kind: "fanout".into(),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, 406);
    assert_eq!(
        v.declare_exchange(ExchangeOptions {
            name: "amq.custom".into(),
            kind: "direct".into(),
            ..Default::default()
        })
        .unwrap_err()
        .code,
        403
    );
}

#[tokio::test]
async fn closing_an_owner_requeues_its_unacked_messages() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    let q = queue(&v, "q", FieldTable::new());
    publish_to(&v, "", "q", "m").await;
    let mut c1 = Conn::new(&broker, 4);
    q.consume(c1.spec(&broker, Limit::Prefetch(1), false))
        .await
        .unwrap();
    let d = c1.next().await;
    assert!(!d.redelivered);
    q.owner_closed(c1.id, None).await;
    let mut c2 = Conn::new(&broker, 4);
    q.consume(c2.spec(&broker, Limit::Prefetch(1), false))
        .await
        .unwrap();
    let d = c2.next().await;
    assert!(d.redelivered);
}

#[tokio::test]
async fn single_active_consumer_gets_everything() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    let mut args = FieldTable::new();
    args.insert("x-single-active-consumer", true);
    let q = queue(&v, "q", args);
    let mut a = Conn::new(&broker, 16);
    let mut b = Conn::new(&broker, 16);
    let spec_a = a.spec(&broker, Limit::Prefetch(0), true);
    let id_a = spec_a.id;
    q.consume(spec_a).await.unwrap();
    q.consume(b.spec(&broker, Limit::Prefetch(0), true))
        .await
        .unwrap();
    for i in 0..3 {
        publish_to(&v, "", "q", &format!("m{i}")).await;
    }
    for i in 0..3 {
        assert_eq!(body(&a.next().await), format!("m{i}"));
    }
    b.nothing().await;
    q.cancel(id_a).await;
    publish_to(&v, "", "q", "m3").await;
    assert_eq!(body(&b.next().await), "m3");
}

#[tokio::test]
async fn priority_queues_deliver_high_priority_first() {
    let broker = Broker::in_memory();
    let v = broker.vhost("/").unwrap();
    let mut args = FieldTable::new();
    args.insert("x-max-priority", Value::I32(5));
    let q = queue(&v, "q", args);
    for (text, prio) in [("low", 1u8), ("high", 5), ("mid", 3)] {
        let m = Message::new(
            "",
            "q",
            BasicProperties {
                priority: Some(prio),
                ..Default::default()
            },
            Bytes::copy_from_slice(text.as_bytes()),
        );
        q.publish(Arc::new(m), None).await;
    }
    let owner = Owner {
        conn: 1,
        channel: 1,
    };
    let order: Vec<Vec<u8>> = futures_order(&q, owner).await;
    assert_eq!(order, [b"high".to_vec(), b"mid".to_vec(), b"low".to_vec()]);
}

async fn futures_order(q: &QueueHandle, owner: Owner) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(g) = q.get(owner, true).await.unwrap() {
        out.push(g.message.body.to_vec());
    }
    out
}

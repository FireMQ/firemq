//! Runs the conformance scenarios against a driver process.

use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::time::{Duration, Instant};

use base64::Engine;
use firemq::Server;
use firemq_core::{BasicProperties, FieldTable, Message, Owner, Value};
use serde_json::{Value as J, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

pub struct Driver {
    _child: Child,
    stdin: ChildStdin,
    lines: Lines<BufReader<ChildStdout>>,
    next: u64,
}

impl Driver {
    pub async fn spawn(cmd: &str) -> Result<Driver, String> {
        let mut c = if cfg!(windows) {
            let mut c = Command::new("cmd");
            c.args(["/C", cmd]);
            c
        } else {
            let mut c = Command::new("sh");
            c.args(["-c", cmd]);
            c
        };
        let mut child = c
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("cannot start driver `{cmd}`: {e}"))?;
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        Ok(Driver {
            _child: child,
            stdin,
            lines: BufReader::new(stdout).lines(),
            next: 0,
        })
    }

    /// Sends one command and returns `Ok(result)` or `Err(error)` as the driver reported.
    pub async fn call(&mut self, op: &str, fields: J) -> Result<Result<J, J>, String> {
        self.next += 1;
        let id = self.next;
        let mut cmd = fields;
        if !cmd.is_object() {
            cmd = json!({});
        }
        cmd["id"] = json!(id);
        cmd["op"] = json!(op);
        let mut line = cmd.to_string();
        line.push('\n');
        self.stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| format!("driver stdin closed: {e}"))?;
        self.stdin.flush().await.map_err(|e| e.to_string())?;
        let reply = tokio::time::timeout(Duration::from_secs(30), self.lines.next_line())
            .await
            .map_err(|_| format!("driver did not answer `{op}` within 30 s"))?
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("driver exited while handling `{op}`"))?;
        let v: J = serde_json::from_str(&reply)
            .map_err(|e| format!("driver wrote a non-JSON line ({e}): {reply}"))?;
        if v["id"] != json!(id) {
            return Err(format!("driver replied to id {} instead of {id}", v["id"]));
        }
        if v["ok"].as_bool() == Some(true) {
            Ok(Ok(v["result"].clone()))
        } else {
            Ok(Err(v["error"].clone()))
        }
    }
}

pub struct Ctx {
    pub d: Driver,
    pub server: Server,
    pub url: String,
    pub scenario: &'static str,
}

type R<T = ()> = Result<T, String>;

macro_rules! check {
    ($cond:expr, $($fmt:tt)+) => {
        if !$cond {
            return Err(format!($($fmt)+));
        }
    };
}

impl Ctx {
    fn q(&self, name: &str) -> String {
        format!("conf.{}.{name}", self.scenario)
    }

    async fn ok(&mut self, op: &str, fields: J) -> R<J> {
        match self.d.call(op, fields).await? {
            Ok(v) => Ok(v),
            Err(e) => Err(format!("`{op}` failed: {e}")),
        }
    }

    async fn fail(&mut self, op: &str, fields: J) -> R<J> {
        match self.d.call(op, fields).await? {
            Ok(v) => Err(format!("`{op}` succeeded but should have failed: {v}")),
            Err(e) => Ok(e),
        }
    }

    async fn connect(&mut self) -> R {
        let url = self.url.clone();
        self.ok("connect", json!({"url": url})).await.map(|_| ())
    }

    async fn close(&mut self) -> R {
        self.ok("close", json!({})).await.map(|_| ())
    }

    async fn declare(&mut self, name: &str, args: J) -> R<String> {
        let r = self
            .ok(
                "declare_queue",
                json!({"name": name, "durable": true, "exclusive": false, "auto_delete": false, "args": args}),
            )
            .await?;
        Ok(r["name"].as_str().unwrap_or_default().to_string())
    }

    async fn exchange(&mut self, name: &str, kind: &str) -> R {
        self.ok(
            "declare_exchange",
            json!({"name": name, "type": kind, "durable": true}),
        )
        .await
        .map(|_| ())
    }

    async fn bind(&mut self, queue: &str, exchange: &str, key: &str) -> R {
        self.ok(
            "bind_queue",
            json!({"queue": queue, "exchange": exchange, "key": key, "args": {}}),
        )
        .await
        .map(|_| ())
    }

    fn publish_cmd(exchange: &str, key: &str, body: &str) -> J {
        json!({"exchange": exchange, "key": key, "body": body, "persistent": true, "mandatory": false, "properties": {}, "headers": {}})
    }

    async fn publish(&mut self, exchange: &str, key: &str, body: &str) -> R {
        self.ok("publish", Self::publish_cmd(exchange, key, body))
            .await
            .map(|_| ())
    }

    async fn get(&mut self, queue: &str, no_ack: bool) -> R<Option<J>> {
        let r = self
            .ok("get", json!({"queue": queue, "no_ack": no_ack}))
            .await?;
        Ok((!r["message"].is_null()).then(|| r["message"].clone()))
    }

    async fn subscribe(&mut self, queue: &str, prefetch: u32, no_ack: bool) -> R<String> {
        let r = self
            .ok(
                "subscribe",
                json!({"queue": queue, "prefetch": prefetch, "no_ack": no_ack}),
            )
            .await?;
        Ok(r["sub"].as_str().unwrap_or_default().to_string())
    }

    async fn next(&mut self, sub: &str, timeout_ms: u64) -> R<Option<J>> {
        let r = self
            .ok("next", json!({"sub": sub, "timeout_ms": timeout_ms}))
            .await?;
        Ok((!r["message"].is_null()).then(|| r["message"].clone()))
    }

    async fn ack(&mut self, m: &J) -> R {
        self.ok("ack", json!({"tag": m["tag"]})).await.map(|_| ())
    }

    async fn nack(&mut self, m: &J, requeue: bool) -> R {
        self.ok("nack", json!({"tag": m["tag"], "requeue": requeue}))
            .await
            .map(|_| ())
    }

    fn broker_queue(&self, name: &str) -> Option<firemq_core::QueueHandle> {
        self.server.broker().vhost("/")?.queue(name, None).ok()
    }

    async fn settle(&self) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn body(m: &J) -> &str {
    m["body"].as_str().unwrap_or_default()
}

type Fut<'a> = Pin<Box<dyn Future<Output = R> + Send + 'a>>;
type Scenario = for<'a> fn(&'a mut Ctx) -> Fut<'a>;

macro_rules! scenario {
    ($name:ident, |$c:ident| $body:block) => {
        fn $name<'a>($c: &'a mut Ctx) -> Fut<'a> {
            Box::pin(async move { $body })
        }
    };
}

scenario!(connect_and_close, |c| {
    let hello = c.ok("hello", json!({})).await?;
    check!(hello["sdk"].is_string(), "hello must name the SDK: {hello}");
    c.connect().await?;
    c.close().await
});

scenario!(wrong_password, |c| {
    let bad = c.url.replace("guest:guest", "guest:wrong");
    let e = c.fail("connect", json!({"url": bad})).await?;
    check!(e["code"] == 403, "expected code 403, got {e}");
    Ok(())
});

scenario!(publish_and_get, |c| {
    c.connect().await?;
    let q = c.q("q");
    c.declare(&q, json!({})).await?;
    check!(
        c.get(&q, true).await?.is_none(),
        "a new queue must be empty"
    );
    c.publish("", &q, "hello").await?;
    let m = c.get(&q, true).await?.ok_or("expected a message")?;
    check!(body(&m) == "hello", "wrong body: {m}");
    check!(m["routing_key"] == q.as_str(), "wrong routing key: {m}");
    check!(m["exchange"] == "", "wrong exchange: {m}");
    check!(m["redelivered"] == false, "should not be redelivered: {m}");
    c.close().await
});

scenario!(consume_and_ack, |c| {
    c.connect().await?;
    let q = c.q("q");
    c.declare(&q, json!({})).await?;
    for i in 0..100 {
        c.publish("", &q, &format!("m{i}")).await?;
    }
    let sub = c.subscribe(&q, 10, false).await?;
    for i in 0..100 {
        let m = c
            .next(&sub, 3000)
            .await?
            .ok_or(format!("message {i} missing"))?;
        check!(body(&m) == format!("m{i}"), "out of order at {i}: {m}");
        c.ack(&m).await?;
    }
    c.settle().await;
    let left = c.broker_queue(&q).map(|q| q.message_count()).unwrap_or(1);
    let unacked = c
        .broker_queue(&q)
        .map(|q| q.stats.unacked.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or(1);
    check!(
        left == 0 && unacked == 0,
        "queue should be empty, has {left} ready and {unacked} unacked"
    );
    c.close().await
});

scenario!(prefetch_limits_deliveries, |c| {
    c.connect().await?;
    let q = c.q("q");
    c.declare(&q, json!({})).await?;
    for i in 0..4 {
        c.publish("", &q, &format!("m{i}")).await?;
    }
    let sub = c.subscribe(&q, 2, false).await?;
    let a = c.next(&sub, 3000).await?.ok_or("first message missing")?;
    let _b = c.next(&sub, 3000).await?.ok_or("second message missing")?;
    check!(
        c.next(&sub, 300).await?.is_none(),
        "a third message arrived with prefetch 2 and nothing acked"
    );
    c.ack(&a).await?;
    let third = c.next(&sub, 3000).await?.ok_or("no message after ack")?;
    check!(body(&third) == "m2", "wrong third message: {third}");
    c.close().await
});

scenario!(nack_requeue, |c| {
    c.connect().await?;
    let q = c.q("q");
    c.declare(&q, json!({})).await?;
    c.publish("", &q, "again").await?;
    let sub = c.subscribe(&q, 1, false).await?;
    let m = c.next(&sub, 3000).await?.ok_or("message missing")?;
    check!(
        m["redelivered"] == false,
        "first delivery must not be redelivered"
    );
    c.nack(&m, true).await?;
    let m = c
        .next(&sub, 3000)
        .await?
        .ok_or("requeued message missing")?;
    check!(
        m["redelivered"] == true,
        "requeued message must be redelivered: {m}"
    );
    c.ack(&m).await?;
    c.close().await
});

scenario!(nack_dead_letters, |c| {
    c.connect().await?;
    let dlx = c.q("dlx");
    let dead = c.q("dead");
    let work = c.q("work");
    c.exchange(&dlx, "fanout").await?;
    c.declare(&dead, json!({})).await?;
    c.bind(&dead, &dlx, "").await?;
    c.declare(&work, json!({"x-dead-letter-exchange": dlx}))
        .await?;
    c.publish("", &work, "bad").await?;
    let m = c.get(&work, false).await?.ok_or("message missing")?;
    c.nack(&m, false).await?;
    c.settle().await;
    let d = c
        .get(&dead, true)
        .await?
        .ok_or("message was not dead-lettered")?;
    check!(body(&d) == "bad", "wrong dead-lettered body: {d}");
    let reason = &d["headers"]["x-death"][0]["reason"];
    check!(
        reason == "rejected",
        "x-death reason should be rejected: {d}"
    );
    c.close().await
});

scenario!(mandatory_unroutable, |c| {
    c.connect().await?;
    let mut cmd = Ctx::publish_cmd("", &c.q("nowhere"), "lost");
    cmd["mandatory"] = json!(true);
    let e = c.fail("publish", cmd).await?;
    check!(
        e["kind"] == "unroutable",
        "expected kind unroutable, got {e}"
    );
    // Without mandatory the broker confirms and drops it.
    let q = c.q("nowhere");
    c.publish("", &q, "dropped").await?;
    c.close().await
});

scenario!(overflow_nack, |c| {
    c.connect().await?;
    let q = c.q("q");
    c.declare(
        &q,
        json!({"x-max-length": 1, "x-overflow": "reject-publish"}),
    )
    .await?;
    c.publish("", &q, "fits").await?;
    let e = c
        .fail("publish", Ctx::publish_cmd("", &q, "rejected"))
        .await?;
    check!(e["kind"] == "nacked", "expected kind nacked, got {e}");
    c.close().await
});

scenario!(topic_routing, |c| {
    c.connect().await?;
    let ex = c.q("ex");
    let q = c.q("q");
    c.exchange(&ex, "topic").await?;
    c.declare(&q, json!({})).await?;
    c.bind(&q, &ex, "*.critical.#").await?;
    for key in [
        "kern.critical",
        "kern.critical.disk",
        "kern.info",
        "critical",
    ] {
        c.publish(&ex, key, key).await?;
    }
    let mut got = Vec::new();
    while let Some(m) = c.get(&q, true).await? {
        got.push(body(&m).to_string());
    }
    check!(
        got == ["kern.critical", "kern.critical.disk"],
        "wrong routing: {got:?}"
    );
    c.close().await
});

scenario!(exchange_to_exchange, |c| {
    c.connect().await?;
    let up = c.q("up");
    let down = c.q("down");
    let q = c.q("q");
    c.exchange(&up, "direct").await?;
    c.exchange(&down, "fanout").await?;
    c.declare(&q, json!({})).await?;
    c.bind(&q, &down, "").await?;
    c.ok(
        "bind_exchange",
        json!({"destination": down, "source": up, "key": "audit"}),
    )
    .await?;
    c.publish(&up, "audit", "yes").await?;
    c.publish(&up, "other", "no").await?;
    let m = c.get(&q, true).await?.ok_or("nothing routed")?;
    check!(body(&m) == "yes", "wrong message: {m}");
    check!(
        c.get(&q, true).await?.is_none(),
        "an unbound key was routed"
    );
    c.close().await
});

scenario!(properties_and_headers, |c| {
    c.connect().await?;
    let q = c.q("q");
    c.declare(&q, json!({})).await?;
    let props = json!({
        "content_type": "application/json", "content_encoding": "gzip", "correlation_id": "c-1",
        "reply_to": "replies", "message_id": "m-1", "timestamp": 1767225600u64, "type": "order.created",
        "app_id": "shop", "priority": 3, "expiration": 600000
    });
    let headers = json!({"string": "s", "int": 42, "negative": -7, "float": 1.5, "bool": true, "list": [1, "two"], "map": {"k": "v"}, "nil": null});
    let mut cmd = Ctx::publish_cmd("", &q, "{}");
    cmd["properties"] = props.clone();
    cmd["headers"] = headers.clone();
    c.ok("publish", cmd).await?;
    let m = c.get(&q, true).await?.ok_or("message missing")?;
    for (k, v) in props.as_object().unwrap() {
        check!(
            m["properties"][k] == *v,
            "property {k}: sent {v}, got {}",
            m["properties"][k]
        );
    }
    for (k, v) in headers.as_object().unwrap() {
        check!(
            m["headers"][k] == *v,
            "header {k}: sent {v}, got {}",
            m["headers"][k]
        );
    }
    c.close().await
});

scenario!(binary_and_large_bodies, |c| {
    c.connect().await?;
    let q = c.q("q");
    c.declare(&q, json!({})).await?;
    let binary: Vec<u8> = (0..=255u8).collect();
    let b64 = base64::engine::general_purpose::STANDARD.encode(&binary);
    let mut cmd = Ctx::publish_cmd("", &q, "");
    cmd.as_object_mut().unwrap().remove("body");
    cmd["body_base64"] = json!(b64);
    c.ok("publish", cmd).await?;
    let large = "x".repeat(1 << 20);
    c.publish("", &q, &large).await?;
    let m = c.get(&q, true).await?.ok_or("binary message missing")?;
    check!(m["body_base64"] == json!(b64), "binary body changed");
    let m = c.get(&q, true).await?.ok_or("large message missing")?;
    check!(
        body(&m).len() == 1 << 20,
        "large body has {} bytes",
        body(&m).len()
    );
    c.close().await
});

scenario!(error_codes, |c| {
    c.connect().await?;
    let q = c.q("q");
    c.declare(&q, json!({})).await?;
    let e = c
        .fail("declare_queue", json!({"name": q, "durable": false, "exclusive": false, "auto_delete": false, "args": {}}))
        .await?;
    check!(
        e["kind"] == "server" && e["code"] == 406,
        "expected server 406, got {e}"
    );
    let e = c
        .fail(
            "bind_queue",
            json!({"queue": q, "exchange": c.q("missing"), "key": "", "args": {}}),
        )
        .await?;
    check!(e["code"] == 404, "expected 404, got {e}");
    c.publish("", &q, "still works").await?;
    c.close().await
});

scenario!(purge_and_delete_counts, |c| {
    c.connect().await?;
    let q = c.q("q");
    c.declare(&q, json!({})).await?;
    for _ in 0..3 {
        c.publish("", &q, "x").await?;
    }
    let r = c.ok("purge_queue", json!({"name": q})).await?;
    check!(r["messages"] == 3, "purge should report 3, got {r}");
    c.publish("", &q, "y").await?;
    let r = c.ok("delete_queue", json!({"name": q})).await?;
    check!(r["messages"] == 1, "delete should report 1, got {r}");
    check!(c.broker_queue(&q).is_none(), "queue still exists");
    c.close().await
});

scenario!(unacked_requeued_on_close, |c| {
    c.connect().await?;
    let q = c.q("q");
    c.declare(&q, json!({})).await?;
    c.publish("", &q, "work").await?;
    let sub = c.subscribe(&q, 1, false).await?;
    c.next(&sub, 3000).await?.ok_or("message missing")?;
    c.close().await?;
    c.settle().await;
    c.connect().await?;
    let m = c.get(&q, true).await?.ok_or("message was not requeued")?;
    check!(
        m["redelivered"] == true,
        "requeued message must be redelivered"
    );
    c.close().await
});

scenario!(exclusive_queue_deleted_on_close, |c| {
    c.connect().await?;
    let r = c
        .ok("declare_queue", json!({"name": "", "durable": false, "exclusive": true, "auto_delete": true, "args": {}}))
        .await?;
    let name = r["name"].as_str().unwrap_or_default().to_string();
    check!(
        name.starts_with("amq.gen-"),
        "server-named queue expected, got {name}"
    );
    check!(
        c.broker_queue(&name).is_none(),
        "an exclusive queue must not be visible to other connections"
    );
    c.close().await?;
    c.settle().await;
    let exists = c
        .server
        .broker()
        .vhost("/")
        .map(|v| v.topology().queues.contains_key(&name))
        .unwrap_or(false);
    check!(!exists, "exclusive queue survived its connection");
    Ok(())
});

scenario!(unsubscribe_stops_delivery, |c| {
    c.connect().await?;
    let q = c.q("q");
    c.declare(&q, json!({})).await?;
    let sub = c.subscribe(&q, 10, true).await?;
    c.publish("", &q, "before").await?;
    c.next(&sub, 3000).await?.ok_or("message missing")?;
    c.ok("unsubscribe", json!({"sub": sub})).await?;
    c.publish("", &q, "after").await?;
    c.settle().await;
    let m = c
        .get(&q, true)
        .await?
        .ok_or("message published after unsubscribe should stay queued")?;
    check!(body(&m) == "after", "wrong message: {m}");
    c.close().await
});

scenario!(amqp_interop, |c| {
    c.connect().await?;
    let q = c.q("q");
    c.declare(&q, json!({})).await?;
    let mut cmd = Ctx::publish_cmd("", &q, "from sdk");
    cmd["properties"] = json!({"content_type": "text/plain", "correlation_id": "x-1"});
    cmd["headers"] = json!({"trace": "abc", "n": 5});
    c.ok("publish", cmd).await?;
    // Read it as an AMQP consumer would see it.
    let handle = c.broker_queue(&q).ok_or("queue missing")?;
    let owner = Owner {
        conn: 0,
        channel: 0,
    };
    let g = handle
        .get(owner, true)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("message missing on the AMQP side")?;
    let p = &g.message.properties;
    check!(
        p.delivery_mode == Some(2),
        "persistent should map to delivery_mode 2"
    );
    check!(
        p.content_type.as_deref() == Some("text/plain"),
        "content_type lost"
    );
    check!(
        p.correlation_id.as_deref() == Some("x-1"),
        "correlation_id lost"
    );
    let h = p.headers.clone().unwrap_or_default();
    check!(
        h.get_str("trace") == Some("abc"),
        "header trace lost: {h:?}"
    );
    check!(h.get_i64("n") == Some(5), "header n lost: {h:?}");
    // And the other way: an AMQP message with a table header reaches the SDK.
    let mut headers = FieldTable::new();
    headers.insert("source", "amqp");
    headers.insert("count", Value::I32(2));
    let msg = Message::new(
        "",
        q.clone(),
        BasicProperties {
            headers: Some(headers),
            message_id: Some("amqp-1".into()),
            ..Default::default()
        },
        bytes_body("from amqp"),
    );
    handle.publish(std::sync::Arc::new(msg), None).await;
    let m = c.get(&q, true).await?.ok_or("AMQP message missing")?;
    check!(body(&m) == "from amqp", "wrong body: {m}");
    check!(
        m["properties"]["message_id"] == "amqp-1",
        "message_id lost: {m}"
    );
    check!(
        m["headers"]["source"] == "amqp" && m["headers"]["count"] == 2,
        "headers lost: {m}"
    );
    c.close().await
});

fn bytes_body(s: &str) -> bytes::Bytes {
    bytes::Bytes::copy_from_slice(s.as_bytes())
}

pub const SCENARIOS: &[(&str, Scenario)] = &[
    ("connect_and_close", connect_and_close),
    ("wrong_password", wrong_password),
    ("publish_and_get", publish_and_get),
    ("consume_and_ack", consume_and_ack),
    ("prefetch_limits_deliveries", prefetch_limits_deliveries),
    ("nack_requeue", nack_requeue),
    ("nack_dead_letters", nack_dead_letters),
    ("mandatory_unroutable", mandatory_unroutable),
    ("overflow_nack", overflow_nack),
    ("topic_routing", topic_routing),
    ("exchange_to_exchange", exchange_to_exchange),
    ("properties_and_headers", properties_and_headers),
    ("binary_and_large_bodies", binary_and_large_bodies),
    ("error_codes", error_codes),
    ("purge_and_delete_counts", purge_and_delete_counts),
    ("unacked_requeued_on_close", unacked_requeued_on_close),
    (
        "exclusive_queue_deleted_on_close",
        exclusive_queue_deleted_on_close,
    ),
    ("unsubscribe_stops_delivery", unsubscribe_stops_delivery),
    ("amqp_interop", amqp_interop),
];

/// Runs the selected scenarios. Returns the number of failures.
///
/// `listen` is the address the broker binds (use `0.0.0.0` when the driver runs in a
/// container) and `url_host` the host name the driver is told to connect to, for example
/// `host.docker.internal`. A host other than localhost lets `guest` connect remotely.
pub async fn run(
    driver_cmd: &str,
    only: Option<&str>,
    listen: std::net::IpAddr,
    url_host: &str,
) -> Result<usize, String> {
    let mut builder = firemq::builder().ephemeral().random_ports();
    let addr = Some(std::net::SocketAddr::new(listen, 0));
    builder = builder.amqp(addr).fire(addr).http(addr);
    let local = matches!(url_host, "localhost" | "127.0.0.1" | "::1");
    if !local {
        builder.config_mut().broker.loopback_users.clear();
    }
    let server = builder
        .start()
        .await
        .map_err(|e| format!("cannot start the broker: {e}"))?;
    let port = server.fire_addr().expect("fire listener").port();
    let url = format!("fire://guest:guest@{url_host}:{port}");
    let mut ctx = Ctx {
        d: Driver::spawn(driver_cmd).await?,
        server,
        url,
        scenario: "",
    };
    let hello = ctx.d.call("hello", json!({})).await?;
    let sdk = hello
        .as_ref()
        .map(|h| {
            format!(
                "{} {}",
                h["sdk"].as_str().unwrap_or("?"),
                h["version"].as_str().unwrap_or("?")
            )
        })
        .unwrap_or_else(|_| "unknown SDK".into());
    println!("FireMQ conformance: {sdk}");
    let mut failures = 0;
    let mut ran = 0;
    for (name, f) in SCENARIOS {
        if only.is_some_and(|o| o != *name) {
            continue;
        }
        ran += 1;
        ctx.scenario = name;
        let started = Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(60), f(&mut ctx))
            .await
            .unwrap_or_else(|_| Err("timed out after 60 s".into()));
        let ms = started.elapsed().as_millis();
        match result {
            Ok(()) => println!("  ok    {name} ({ms} ms)"),
            Err(e) => {
                failures += 1;
                println!("  FAIL  {name}: {e}");
                // Leave a clean slate for the next scenario.
                let _ = ctx.d.call("close", json!({})).await;
            }
        }
    }
    println!("\n{} of {ran} scenarios passed", ran - failures);
    Ok(failures)
}

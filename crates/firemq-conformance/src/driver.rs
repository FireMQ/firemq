//! The conformance driver for the Rust SDK, `firemq-client`.

use std::collections::HashMap;

use base64::Engine;
use firemq_client::{
    Client, ConnectOptions, Error, ExchangeOptions, Map, Message, Properties, PublishOptions,
    QueueOptions, SubscribeOptions, Subscription, Value,
};
use serde_json::{Value as J, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn to_mp(v: &J) -> Value {
    match v {
        J::Null => Value::Nil,
        J::Bool(b) => Value::Boolean(*b),
        J::Number(n) => match n.as_i64() {
            Some(i) => Value::from(i),
            None => Value::F64(n.as_f64().unwrap_or(0.0)),
        },
        J::String(s) => Value::from(s.as_str()),
        J::Array(a) => Value::Array(a.iter().map(to_mp).collect()),
        J::Object(o) => Value::Map(
            o.iter()
                .map(|(k, v)| (Value::from(k.as_str()), to_mp(v)))
                .collect(),
        ),
    }
}

fn from_mp(v: &Value) -> J {
    match v {
        Value::Nil => J::Null,
        Value::Boolean(b) => J::Bool(*b),
        Value::Integer(i) => i
            .as_i64()
            .map(J::from)
            .or_else(|| i.as_u64().map(J::from))
            .unwrap_or(J::Null),
        Value::F32(f) => json!(f),
        Value::F64(f) => json!(f),
        Value::String(s) => J::String(s.as_str().unwrap_or_default().to_string()),
        Value::Binary(b) => J::String(base64::engine::general_purpose::STANDARD.encode(b)),
        Value::Array(a) => J::Array(a.iter().map(from_mp).collect()),
        Value::Map(m) => J::Object(
            m.iter()
                .map(|(k, v)| (k.as_str().unwrap_or_default().to_string(), from_mp(v)))
                .collect(),
        ),
        Value::Ext(..) => J::Null,
    }
}

fn map(v: &J) -> Map {
    v.as_object()
        .map(|o| o.iter().map(|(k, v)| (k.clone(), to_mp(v))).collect())
        .unwrap_or_default()
}

fn s(v: &J, k: &str) -> String {
    v[k].as_str().unwrap_or_default().to_string()
}

fn b(v: &J, k: &str) -> bool {
    v[k].as_bool().unwrap_or(false)
}

fn err(e: &Error) -> J {
    let (kind, code) = match e {
        Error::Server { code, .. } => ("server", *code),
        Error::Unroutable { .. } => ("unroutable", 312),
        Error::Nacked { code, .. } => ("nacked", *code),
        Error::Closed(_) => ("closed", 0),
        _ => ("other", 0),
    };
    json!({"kind": kind, "code": code, "message": e.to_string()})
}

fn properties(p: &J) -> Properties {
    let st = |k: &str| p[k].as_str().map(String::from);
    let n = |k: &str| p[k].as_u64();
    Properties {
        content_type: st("content_type"),
        content_encoding: st("content_encoding"),
        correlation_id: st("correlation_id"),
        reply_to: st("reply_to"),
        expiration: n("expiration"),
        message_id: st("message_id"),
        timestamp: n("timestamp"),
        kind: st("type"),
        user_id: st("user_id"),
        app_id: st("app_id"),
        priority: n("priority").map(|x| x as u8),
    }
}

fn message_json(m: &Message, tag: String) -> J {
    json!({
        "body": m.body_str(),
        "body_base64": base64::engine::general_purpose::STANDARD.encode(&m.body),
        "exchange": m.exchange,
        "routing_key": m.routing_key,
        "redelivered": m.redelivered,
        "tag": tag,
        "properties": serde_json::to_value(&m.properties).unwrap_or(J::Null),
        "headers": J::Object(m.headers.iter().map(|(k, v)| (k.clone(), from_mp(v))).collect()),
    })
}

#[derive(Default)]
struct State {
    client: Option<Client>,
    subs: HashMap<String, Subscription>,
    pending: HashMap<String, Message>,
    next_tag: u64,
}

impl State {
    fn client(&self) -> Result<&Client, J> {
        self.client
            .as_ref()
            .ok_or_else(|| json!({"kind": "closed", "code": 0, "message": "not connected"}))
    }

    fn keep(&mut self, m: Message) -> J {
        self.next_tag += 1;
        let tag = self.next_tag.to_string();
        let j = message_json(&m, tag.clone());
        self.pending.insert(tag, m);
        j
    }

    async fn handle(&mut self, op: &str, c: &J) -> Result<J, J> {
        let e = |x: Error| err(&x);
        match op {
            "hello" => Ok(
                json!({"sdk": "firemq-rust", "version": env!("CARGO_PKG_VERSION"), "capabilities": []}),
            ),
            "connect" => {
                let opts = ConnectOptions::from_url(&s(c, "url")).map_err(e)?;
                self.client = Some(Client::connect_with(opts).await.map_err(e)?);
                Ok(json!({}))
            }
            "close" => {
                self.subs.clear();
                self.pending.clear();
                if let Some(client) = self.client.take() {
                    client.close().await.map_err(e)?;
                }
                Ok(json!({}))
            }
            "declare_queue" => {
                let q = self
                    .client()?
                    .declare_queue(
                        &s(c, "name"),
                        QueueOptions {
                            durable: b(c, "durable"),
                            exclusive: b(c, "exclusive"),
                            auto_delete: b(c, "auto_delete"),
                            passive: false,
                            args: map(&c["args"]),
                        },
                    )
                    .await
                    .map_err(e)?;
                Ok(json!({"name": q.name, "messages": q.messages, "consumers": q.consumers}))
            }
            "delete_queue" => {
                let n = self
                    .client()?
                    .delete_queue(&s(c, "name"))
                    .await
                    .map_err(e)?;
                Ok(json!({"messages": n}))
            }
            "purge_queue" => {
                let n = self.client()?.purge_queue(&s(c, "name")).await.map_err(e)?;
                Ok(json!({"messages": n}))
            }
            "declare_exchange" => {
                self.client()?
                    .declare_exchange(
                        &s(c, "name"),
                        &s(c, "type"),
                        ExchangeOptions {
                            durable: b(c, "durable"),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(e)?;
                Ok(json!({}))
            }
            "delete_exchange" => {
                self.client()?
                    .delete_exchange(&s(c, "name"), false)
                    .await
                    .map_err(e)?;
                Ok(json!({}))
            }
            "bind_queue" => {
                self.client()?
                    .bind_queue_with(
                        &s(c, "queue"),
                        &s(c, "exchange"),
                        &s(c, "key"),
                        map(&c["args"]),
                    )
                    .await
                    .map_err(e)?;
                Ok(json!({}))
            }
            "unbind_queue" => {
                self.client()?
                    .unbind_queue(&s(c, "queue"), &s(c, "exchange"), &s(c, "key"))
                    .await
                    .map_err(e)?;
                Ok(json!({}))
            }
            "bind_exchange" => {
                self.client()?
                    .bind_exchange(&s(c, "destination"), &s(c, "source"), &s(c, "key"))
                    .await
                    .map_err(e)?;
                Ok(json!({}))
            }
            "publish" => {
                let body = match c["body_base64"].as_str() {
                    Some(b64) => base64::engine::general_purpose::STANDARD
                        .decode(b64)
                        .map_err(
                            |x| json!({"kind": "other", "code": 0, "message": x.to_string()}),
                        )?,
                    None => s(c, "body").into_bytes(),
                };
                let opts = PublishOptions {
                    persistent: b(c, "persistent"),
                    mandatory: b(c, "mandatory"),
                    confirm: true,
                    properties: properties(&c["properties"]),
                    headers: map(&c["headers"]),
                };
                self.client()?
                    .publish_with(&s(c, "exchange"), &s(c, "key"), body, opts)
                    .await
                    .map_err(e)?;
                Ok(json!({}))
            }
            "subscribe" => {
                let sub = self
                    .client()?
                    .subscribe(
                        &s(c, "queue"),
                        SubscribeOptions {
                            prefetch: c["prefetch"].as_u64().unwrap_or(100) as u32,
                            no_ack: b(c, "no_ack"),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(e)?;
                let id = format!("sub-{}", self.subs.len() + self.next_tag as usize + 1);
                self.subs.insert(id.clone(), sub);
                Ok(json!({"sub": id}))
            }
            "next" => {
                let id = s(c, "sub");
                let timeout =
                    std::time::Duration::from_millis(c["timeout_ms"].as_u64().unwrap_or(1000));
                let sub = self
                    .subs
                    .get_mut(&id)
                    .ok_or_else(|| json!({"kind": "other", "code": 0, "message": "unknown sub"}))?;
                match tokio::time::timeout(timeout, sub.next()).await {
                    Ok(Some(m)) => Ok(json!({"message": self.keep(m)})),
                    _ => Ok(json!({"message": null})),
                }
            }
            "ack" | "nack" => {
                let m = self
                    .pending
                    .remove(&s(c, "tag"))
                    .ok_or_else(|| json!({"kind": "other", "code": 0, "message": "unknown tag"}))?;
                if op == "ack" {
                    m.ack().map_err(e)?;
                } else {
                    m.nack(b(c, "requeue")).map_err(e)?;
                }
                Ok(json!({}))
            }
            "unsubscribe" => {
                if let Some(sub) = self.subs.remove(&s(c, "sub")) {
                    sub.cancel().await.map_err(e)?;
                }
                Ok(json!({}))
            }
            "get" => {
                let m = self
                    .client()?
                    .get(&s(c, "queue"), b(c, "no_ack"))
                    .await
                    .map_err(e)?;
                Ok(json!({"message": m.map(|m| self.keep(m))}))
            }
            other => Err(
                json!({"kind": "unsupported", "code": 0, "message": format!("unknown op {other}")}),
            ),
        }
    }
}

/// Runs the driver on stdin and stdout until stdin closes.
pub async fn run() {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut out = tokio::io::stdout();
    let mut state = State::default();
    while let Ok(Some(line)) = lines.next_line().await {
        let cmd: J = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("driver: bad command: {e}");
                continue;
            }
        };
        let id = cmd["id"].clone();
        let op = cmd["op"].as_str().unwrap_or_default().to_string();
        let reply = match state.handle(&op, &cmd).await {
            Ok(result) => json!({"id": id, "ok": true, "result": result}),
            Err(error) => json!({"id": id, "ok": false, "error": error}),
        };
        let mut text = reply.to_string();
        text.push('\n');
        if out.write_all(text.as_bytes()).await.is_err() || out.flush().await.is_err() {
            break;
        }
    }
}

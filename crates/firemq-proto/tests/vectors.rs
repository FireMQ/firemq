//! Keeps `spec/test-vectors/frames.json` in sync with the codec.
//!
//! Run with `FIREMQ_BLESS=1` to regenerate the file after an intentional protocol change.

use std::path::PathBuf;

use bytes::{Bytes, BytesMut};
use firemq_proto::*;
use serde_json::{Value as Json, json};

fn hex(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn samples() -> Vec<(&'static str, Command)> {
    let mut headers = Map::new();
    headers.insert("x-attempt".into(), Value::from(2));
    headers.insert("trace".into(), Value::from("abc"));
    let props = Properties {
        content_type: Some("application/json".into()),
        correlation_id: Some("req-1".into()),
        reply_to: Some("amq.gen-reply".into()),
        expiration: Some(30_000),
        timestamp: Some(1_767_225_600),
        ..Default::default()
    };
    let mut queue_args = Map::new();
    queue_args.insert("x-message-ttl".into(), Value::from(60_000));
    queue_args.insert("x-dead-letter-exchange".into(), Value::from("dlx"));
    let mut client = Map::new();
    client.insert("name".into(), Value::from("firemq-rust"));
    client.insert("version".into(), Value::from("0.1.0"));
    vec![
        (
            "hello",
            Command::Hello(Hello {
                ver: 1,
                vhost: "/".into(),
                auth: Auth::plain("guest", "guest"),
                heartbeat: 30,
                client,
                caps: vec!["confirms".into(), "credit".into()],
            }),
        ),
        (
            "welcome",
            Command::Welcome(Welcome {
                ver: 1,
                server: ServerInfo {
                    name: "firemq".into(),
                    version: "0.1.0".into(),
                },
                conn_id: "127.0.0.1:50000 -> 127.0.0.1:5673".into(),
                heartbeat: 30,
                max_frame: DEFAULT_MAX_FRAME,
                caps: vec!["confirms".into(), "credit".into(), "mandatory".into()],
            }),
        ),
        ("ping", Command::Ping),
        ("pong", Command::Pong),
        (
            "close",
            Command::Close(Close {
                code: 200,
                msg: "bye".into(),
            }),
        ),
        (
            "ok",
            Command::Ok(OkReply {
                rid: 2,
                name: Some("jobs".into()),
                messages: Some(0),
                consumers: Some(0),
                empty: false,
            }),
        ),
        (
            "error",
            Command::Error(ErrorReply {
                rid: Some(3),
                code: 404,
                msg: "no queue 'missing' in vhost '/'".into(),
                fatal: false,
            }),
        ),
        ("flow", Command::Flow(Flow { pause: true })),
        (
            "declare_exchange",
            Command::DeclareExchange(DeclareExchange {
                rid: 1,
                name: "orders".into(),
                kind: "topic".into(),
                durable: true,
                ..Default::default()
            }),
        ),
        (
            "delete_exchange",
            Command::DeleteExchange(DeleteExchange {
                rid: 1,
                name: "orders".into(),
                if_unused: true,
            }),
        ),
        (
            "declare_queue",
            Command::DeclareQueue(DeclareQueue {
                rid: 2,
                name: "jobs".into(),
                durable: true,
                args: queue_args,
                ..Default::default()
            }),
        ),
        (
            "delete_queue",
            Command::DeleteQueue(DeleteQueue {
                rid: 2,
                name: "jobs".into(),
                if_empty: true,
                ..Default::default()
            }),
        ),
        (
            "purge_queue",
            Command::PurgeQueue(PurgeQueue {
                rid: 2,
                name: "jobs".into(),
            }),
        ),
        (
            "bind",
            Command::Bind(Bind {
                rid: 4,
                exchange: "orders".into(),
                queue: "jobs".into(),
                key: "order.*".into(),
                ..Default::default()
            }),
        ),
        (
            "unbind",
            Command::Unbind(Bind {
                rid: 5,
                exchange: "orders".into(),
                to_exchange: "audit".into(),
                key: "#".into(),
                ..Default::default()
            }),
        ),
        (
            "publish",
            Command::Publish(
                Publish {
                    seq: 1,
                    exchange: "orders".into(),
                    key: "order.created".into(),
                    mandatory: true,
                    persistent: true,
                    props: props.clone(),
                    headers: headers.clone(),
                },
                Bytes::from_static(b"{\"id\":42}"),
            ),
        ),
        (
            "confirm",
            Command::Confirm(Confirm {
                upto: Some(1),
                ..Default::default()
            }),
        ),
        (
            "confirm_nack",
            Command::Confirm(Confirm {
                seq: Some(2),
                nack: true,
                code: 406,
                msg: "queue 'jobs' is full".into(),
                ..Default::default()
            }),
        ),
        (
            "return",
            Command::Return(
                Return {
                    seq: 3,
                    code: 312,
                    msg: "NO_ROUTE".into(),
                    exchange: "orders".into(),
                    key: "order.unknown".into(),
                    ..Default::default()
                },
                Bytes::from_static(b"hi"),
            ),
        ),
        (
            "subscribe",
            Command::Subscribe(Subscribe {
                rid: 6,
                sub: 1,
                queue: "jobs".into(),
                credit: 100,
                ..Default::default()
            }),
        ),
        (
            "unsubscribe",
            Command::Unsubscribe(Unsubscribe { rid: 7, sub: 1 }),
        ),
        ("credit", Command::Credit(Credit { sub: 1, credit: 50 })),
        (
            "deliver",
            Command::Deliver(
                Deliver {
                    sub: 1,
                    tag: 1,
                    redelivered: false,
                    exchange: "orders".into(),
                    key: "order.created".into(),
                    props,
                    headers,
                },
                Bytes::from_static(b"{\"id\":42}"),
            ),
        ),
        (
            "cancelled",
            Command::Cancelled(Cancelled {
                sub: 1,
                code: 404,
                msg: "queue 'jobs' was deleted".into(),
            }),
        ),
        (
            "ack",
            Command::Ack(Ack {
                tag: 5,
                multiple: true,
            }),
        ),
        (
            "nack",
            Command::Nack(Nack {
                tag: 6,
                multiple: false,
                requeue: true,
            }),
        ),
        (
            "get",
            Command::Get(Get {
                rid: 8,
                queue: "jobs".into(),
                no_ack: false,
            }),
        ),
        (
            "get_ok",
            Command::GetOk(
                GetOk {
                    rid: 8,
                    tag: 9,
                    key: "jobs".into(),
                    remaining: 3,
                    ..Default::default()
                },
                Bytes::from_static(b"work"),
            ),
        ),
    ]
}

fn vectors_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spec/test-vectors/frames.json")
}

fn build() -> Json {
    let vectors: Vec<Json> = samples()
        .into_iter()
        .map(|(name, cmd)| {
            let frame = cmd.to_bytes();
            let mut buf = BytesMut::from(&frame[..]);
            let raw = decode_frame(&mut buf, u32::MAX).unwrap().unwrap();
            let header: Json = if raw.header.is_empty() {
                json!({})
            } else {
                rmp_serde::from_slice(&raw.header).unwrap()
            };
            json!({
                "name": name,
                "op": raw.op,
                "op_name": op_name(raw.op),
                "header": header,
                "payload_hex": hex(&raw.payload),
                "frame_hex": hex(&frame),
            })
        })
        .collect();
    json!({
        "description": "FireMQ protocol v1 frames. Each frame_hex is a length-prefixed frame as sent on TCP. Decoders must produce the listed header and payload.",
        "vectors": vectors,
    })
}

#[test]
fn test_vectors_match_the_codec() {
    let expected = serde_json::to_string_pretty(&build()).unwrap() + "\n";
    let path = vectors_path();
    if std::env::var_os("FIREMQ_BLESS").is_some() {
        std::fs::write(&path, &expected).unwrap();
        return;
    }
    let actual = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        actual == expected,
        "spec/test-vectors/frames.json is out of date; rerun with FIREMQ_BLESS=1"
    );
}

#[test]
fn every_vector_decodes() {
    let file: Json =
        serde_json::from_str(&std::fs::read_to_string(vectors_path()).unwrap()).unwrap();
    for v in file["vectors"].as_array().unwrap() {
        let mut buf = BytesMut::from(&unhex(v["frame_hex"].as_str().unwrap())[..]);
        let raw = decode_frame(&mut buf, u32::MAX).unwrap().unwrap();
        assert_eq!(raw.op as u64, v["op"].as_u64().unwrap());
        assert_eq!(hex(&raw.payload), v["payload_hex"].as_str().unwrap());
        Command::decode(raw).unwrap_or_else(|e| panic!("{}: {e}", v["name"]));
    }
}

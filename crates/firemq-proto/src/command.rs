use std::collections::BTreeMap;

use bytes::{BufMut, Bytes, BytesMut};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::ProtoError;
use crate::frame::RawFrame;

/// A MessagePack map with string keys: arguments and application headers.
pub type Map = BTreeMap<String, rmpv::Value>;

/// Op codes.
pub mod op {
    pub const HELLO: u8 = 0x01;
    pub const WELCOME: u8 = 0x02;
    pub const PING: u8 = 0x03;
    pub const PONG: u8 = 0x04;
    pub const CLOSE: u8 = 0x05;
    pub const OK: u8 = 0x06;
    pub const ERROR: u8 = 0x07;
    pub const FLOW: u8 = 0x08;
    pub const DECLARE_EXCHANGE: u8 = 0x10;
    pub const DELETE_EXCHANGE: u8 = 0x11;
    pub const DECLARE_QUEUE: u8 = 0x12;
    pub const DELETE_QUEUE: u8 = 0x13;
    pub const PURGE_QUEUE: u8 = 0x14;
    pub const BIND: u8 = 0x15;
    pub const UNBIND: u8 = 0x16;
    pub const PUBLISH: u8 = 0x20;
    pub const CONFIRM: u8 = 0x21;
    pub const RETURN: u8 = 0x22;
    pub const SUBSCRIBE: u8 = 0x30;
    pub const UNSUBSCRIBE: u8 = 0x31;
    pub const CREDIT: u8 = 0x32;
    pub const DELIVER: u8 = 0x33;
    pub const CANCELLED: u8 = 0x34;
    pub const ACK: u8 = 0x35;
    pub const NACK: u8 = 0x36;
    pub const GET: u8 = 0x37;
    pub const GET_OK: u8 = 0x38;
}

fn is_false(v: &bool) -> bool {
    !*v
}

fn is_default<T: Default + PartialEq>(v: &T) -> bool {
    *v == T::default()
}

macro_rules! header {
    ($(#[$meta:meta])* $name:ident { $($(#[$fmeta:meta])* $field:ident : $ty:ty),* $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
        #[serde(default)]
        pub struct $name {
            $($(#[$fmeta])* pub $field: $ty,)*
        }
    };
}

header!(
    /// Credentials in `hello`.
    Auth {
        mech: String,
        #[serde(skip_serializing_if = "is_default")]
        user: String,
        #[serde(skip_serializing_if = "is_default")]
        pass: String,
    }
);

impl Auth {
    pub fn plain(user: impl Into<String>, pass: impl Into<String>) -> Auth {
        Auth {
            mech: "plain".into(),
            user: user.into(),
            pass: pass.into(),
        }
    }
}

header!(
    /// First frame from the client.
    Hello {
        ver: u32,
        #[serde(skip_serializing_if = "is_default")]
        vhost: String,
        auth: Auth,
        heartbeat: u32,
        #[serde(skip_serializing_if = "is_default")]
        client: Map,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        caps: Vec<String>,
    }
);

header!(
    /// Server name and version in `welcome`.
    ServerInfo {
        name: String,
        version: String,
    }
);

header!(
    /// Reply to a successful `hello`.
    Welcome {
        ver: u32,
        server: ServerInfo,
        conn_id: String,
        heartbeat: u32,
        max_frame: u32,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        caps: Vec<String>,
    }
);

header!(
    /// Closes the connection.
    Close {
        code: u16,
        #[serde(skip_serializing_if = "is_default")]
        msg: String,
    }
);

header!(
    /// Generic success reply.
    OkReply {
        rid: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        messages: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        consumers: Option<u32>,
        #[serde(skip_serializing_if = "is_false")]
        empty: bool,
    }
);

header!(
    /// Error reply, tied to a request when `rid` is set.
    ErrorReply {
        #[serde(skip_serializing_if = "Option::is_none")]
        rid: Option<u32>,
        code: u16,
        msg: String,
        #[serde(skip_serializing_if = "is_false")]
        fatal: bool,
    }
);

header!(
    /// Publisher flow control.
    Flow { pause: bool }
);

header!(DeclareExchange {
    rid: u32,
    name: String,
    #[serde(rename = "type")]
    kind: String,
    #[serde(skip_serializing_if = "is_false")]
    durable: bool,
    #[serde(skip_serializing_if = "is_false")]
    auto_delete: bool,
    #[serde(skip_serializing_if = "is_false")]
    internal: bool,
    #[serde(skip_serializing_if = "is_false")]
    passive: bool,
    #[serde(skip_serializing_if = "is_default")]
    args: Map,
});

header!(DeleteExchange {
    rid: u32,
    name: String,
    #[serde(skip_serializing_if = "is_false")]
    if_unused: bool,
});

header!(DeclareQueue {
    rid: u32,
    #[serde(skip_serializing_if = "is_default")]
    name: String,
    #[serde(skip_serializing_if = "is_false")]
    durable: bool,
    #[serde(skip_serializing_if = "is_false")]
    exclusive: bool,
    #[serde(skip_serializing_if = "is_false")]
    auto_delete: bool,
    #[serde(skip_serializing_if = "is_false")]
    passive: bool,
    #[serde(skip_serializing_if = "is_default")]
    args: Map,
});

header!(DeleteQueue {
    rid: u32,
    name: String,
    #[serde(skip_serializing_if = "is_false")]
    if_unused: bool,
    #[serde(skip_serializing_if = "is_false")]
    if_empty: bool,
});

header!(PurgeQueue {
    rid: u32,
    name: String,
});

header!(
    /// Body of `bind` and `unbind`.
    Bind {
        rid: u32,
        exchange: String,
        #[serde(skip_serializing_if = "is_default")]
        queue: String,
        #[serde(skip_serializing_if = "is_default")]
        to_exchange: String,
        #[serde(skip_serializing_if = "is_default")]
        key: String,
        #[serde(skip_serializing_if = "is_default")]
        args: Map,
    }
);

header!(
    /// Message properties, mirroring AMQP's basic properties.
    Properties {
        #[serde(skip_serializing_if = "Option::is_none")]
        content_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        content_encoding: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        correlation_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reply_to: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        expiration: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        timestamp: Option<u64>,
        #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
        kind: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        user_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        app_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        priority: Option<u8>,
    }
);

impl Properties {
    pub fn is_empty(&self) -> bool {
        *self == Properties::default()
    }
}

header!(Publish {
    #[serde(skip_serializing_if = "is_default")]
    seq: u64,
    #[serde(skip_serializing_if = "is_default")]
    exchange: String,
    #[serde(skip_serializing_if = "is_default")]
    key: String,
    #[serde(skip_serializing_if = "is_false")]
    mandatory: bool,
    #[serde(skip_serializing_if = "is_false")]
    persistent: bool,
    #[serde(skip_serializing_if = "Properties::is_empty")]
    props: Properties,
    #[serde(skip_serializing_if = "is_default")]
    headers: Map,
});

header!(
    /// Publisher confirm: `upto` for positive confirms, `seq` with `nack` for rejections.
    Confirm {
        #[serde(skip_serializing_if = "Option::is_none")]
        upto: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        seq: Option<u64>,
        #[serde(skip_serializing_if = "is_false")]
        nack: bool,
        #[serde(skip_serializing_if = "is_default")]
        code: u16,
        #[serde(skip_serializing_if = "is_default")]
        msg: String,
    }
);

header!(Return {
    seq: u64,
    code: u16,
    msg: String,
    #[serde(skip_serializing_if = "is_default")]
    exchange: String,
    #[serde(skip_serializing_if = "is_default")]
    key: String,
    #[serde(skip_serializing_if = "Properties::is_empty")]
    props: Properties,
    #[serde(skip_serializing_if = "is_default")]
    headers: Map,
});

header!(Subscribe {
    rid: u32,
    sub: u32,
    queue: String,
    credit: u32,
    #[serde(skip_serializing_if = "is_false")]
    no_ack: bool,
    #[serde(skip_serializing_if = "is_false")]
    exclusive: bool,
    #[serde(skip_serializing_if = "is_default")]
    tag: String,
    #[serde(skip_serializing_if = "is_default")]
    args: Map,
});

header!(Unsubscribe { rid: u32, sub: u32 });

header!(Credit {
    sub: u32,
    credit: u32
});

header!(Deliver {
    sub: u32,
    tag: u64,
    #[serde(skip_serializing_if = "is_false")]
    redelivered: bool,
    #[serde(skip_serializing_if = "is_default")]
    exchange: String,
    #[serde(skip_serializing_if = "is_default")]
    key: String,
    #[serde(skip_serializing_if = "Properties::is_empty")]
    props: Properties,
    #[serde(skip_serializing_if = "is_default")]
    headers: Map,
});

header!(Cancelled {
    sub: u32,
    code: u16,
    #[serde(skip_serializing_if = "is_default")]
    msg: String,
});

header!(Ack {
    tag: u64,
    #[serde(skip_serializing_if = "is_false")]
    multiple: bool,
});

header!(Nack {
    tag: u64,
    #[serde(skip_serializing_if = "is_false")]
    multiple: bool,
    #[serde(skip_serializing_if = "is_false")]
    requeue: bool,
});

header!(Get {
    rid: u32,
    queue: String,
    #[serde(skip_serializing_if = "is_false")]
    no_ack: bool,
});

header!(GetOk {
    rid: u32,
    tag: u64,
    #[serde(skip_serializing_if = "is_false")]
    redelivered: bool,
    #[serde(skip_serializing_if = "is_default")]
    exchange: String,
    #[serde(skip_serializing_if = "is_default")]
    key: String,
    remaining: u32,
    #[serde(skip_serializing_if = "Properties::is_empty")]
    props: Properties,
    #[serde(skip_serializing_if = "is_default")]
    headers: Map,
});

/// A decoded frame.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Hello(Hello),
    Welcome(Welcome),
    Ping,
    Pong,
    Close(Close),
    Ok(OkReply),
    Error(ErrorReply),
    Flow(Flow),
    DeclareExchange(DeclareExchange),
    DeleteExchange(DeleteExchange),
    DeclareQueue(DeclareQueue),
    DeleteQueue(DeleteQueue),
    PurgeQueue(PurgeQueue),
    Bind(Bind),
    Unbind(Bind),
    Publish(Publish, Bytes),
    Confirm(Confirm),
    Return(Return, Bytes),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Credit(Credit),
    Deliver(Deliver, Bytes),
    Cancelled(Cancelled),
    Ack(Ack),
    Nack(Nack),
    Get(Get),
    GetOk(GetOk, Bytes),
}

fn header<T: DeserializeOwned + Default>(name: &'static str, h: &Bytes) -> Result<T, ProtoError> {
    if h.is_empty() {
        return Ok(T::default());
    }
    rmp_serde::from_slice(h).map_err(|source| ProtoError::Header { op: name, source })
}

/// Op code and name for an op byte.
pub fn op_name(op: u8) -> &'static str {
    match op {
        op::HELLO => "hello",
        op::WELCOME => "welcome",
        op::PING => "ping",
        op::PONG => "pong",
        op::CLOSE => "close",
        op::OK => "ok",
        op::ERROR => "error",
        op::FLOW => "flow",
        op::DECLARE_EXCHANGE => "declare_exchange",
        op::DELETE_EXCHANGE => "delete_exchange",
        op::DECLARE_QUEUE => "declare_queue",
        op::DELETE_QUEUE => "delete_queue",
        op::PURGE_QUEUE => "purge_queue",
        op::BIND => "bind",
        op::UNBIND => "unbind",
        op::PUBLISH => "publish",
        op::CONFIRM => "confirm",
        op::RETURN => "return",
        op::SUBSCRIBE => "subscribe",
        op::UNSUBSCRIBE => "unsubscribe",
        op::CREDIT => "credit",
        op::DELIVER => "deliver",
        op::CANCELLED => "cancelled",
        op::ACK => "ack",
        op::NACK => "nack",
        op::GET => "get",
        op::GET_OK => "get_ok",
        _ => "unknown",
    }
}

impl Command {
    /// The op code of this command.
    pub fn op(&self) -> u8 {
        match self {
            Command::Hello(_) => op::HELLO,
            Command::Welcome(_) => op::WELCOME,
            Command::Ping => op::PING,
            Command::Pong => op::PONG,
            Command::Close(_) => op::CLOSE,
            Command::Ok(_) => op::OK,
            Command::Error(_) => op::ERROR,
            Command::Flow(_) => op::FLOW,
            Command::DeclareExchange(_) => op::DECLARE_EXCHANGE,
            Command::DeleteExchange(_) => op::DELETE_EXCHANGE,
            Command::DeclareQueue(_) => op::DECLARE_QUEUE,
            Command::DeleteQueue(_) => op::DELETE_QUEUE,
            Command::PurgeQueue(_) => op::PURGE_QUEUE,
            Command::Bind(_) => op::BIND,
            Command::Unbind(_) => op::UNBIND,
            Command::Publish(..) => op::PUBLISH,
            Command::Confirm(_) => op::CONFIRM,
            Command::Return(..) => op::RETURN,
            Command::Subscribe(_) => op::SUBSCRIBE,
            Command::Unsubscribe(_) => op::UNSUBSCRIBE,
            Command::Credit(_) => op::CREDIT,
            Command::Deliver(..) => op::DELIVER,
            Command::Cancelled(_) => op::CANCELLED,
            Command::Ack(_) => op::ACK,
            Command::Nack(_) => op::NACK,
            Command::Get(_) => op::GET,
            Command::GetOk(..) => op::GET_OK,
        }
    }

    /// The op's name as written in the spec, for example `declare_queue`.
    pub fn name(&self) -> &'static str {
        op_name(self.op())
    }

    /// The request id, for commands that carry one.
    pub fn rid(&self) -> Option<u32> {
        Some(match self {
            Command::DeclareExchange(c) => c.rid,
            Command::DeleteExchange(c) => c.rid,
            Command::DeclareQueue(c) => c.rid,
            Command::DeleteQueue(c) => c.rid,
            Command::PurgeQueue(c) => c.rid,
            Command::Bind(c) | Command::Unbind(c) => c.rid,
            Command::Subscribe(c) => c.rid,
            Command::Unsubscribe(c) => c.rid,
            Command::Get(c) => c.rid,
            Command::Ok(c) => c.rid,
            Command::GetOk(c, _) => c.rid,
            Command::Error(c) => return c.rid,
            _ => return None,
        })
    }

    /// Decodes a raw frame into a command.
    pub fn decode(frame: RawFrame) -> Result<Command, ProtoError> {
        let h = &frame.header;
        let name = op_name(frame.op);
        Ok(match frame.op {
            op::HELLO => Command::Hello(header(name, h)?),
            op::WELCOME => Command::Welcome(header(name, h)?),
            op::PING => Command::Ping,
            op::PONG => Command::Pong,
            op::CLOSE => Command::Close(header(name, h)?),
            op::OK => Command::Ok(header(name, h)?),
            op::ERROR => Command::Error(header(name, h)?),
            op::FLOW => Command::Flow(header(name, h)?),
            op::DECLARE_EXCHANGE => Command::DeclareExchange(header(name, h)?),
            op::DELETE_EXCHANGE => Command::DeleteExchange(header(name, h)?),
            op::DECLARE_QUEUE => Command::DeclareQueue(header(name, h)?),
            op::DELETE_QUEUE => Command::DeleteQueue(header(name, h)?),
            op::PURGE_QUEUE => Command::PurgeQueue(header(name, h)?),
            op::BIND => Command::Bind(header(name, h)?),
            op::UNBIND => Command::Unbind(header(name, h)?),
            op::PUBLISH => Command::Publish(header(name, h)?, frame.payload),
            op::CONFIRM => Command::Confirm(header(name, h)?),
            op::RETURN => Command::Return(header(name, h)?, frame.payload),
            op::SUBSCRIBE => Command::Subscribe(header(name, h)?),
            op::UNSUBSCRIBE => Command::Unsubscribe(header(name, h)?),
            op::CREDIT => Command::Credit(header(name, h)?),
            op::DELIVER => Command::Deliver(header(name, h)?, frame.payload),
            op::CANCELLED => Command::Cancelled(header(name, h)?),
            op::ACK => Command::Ack(header(name, h)?),
            op::NACK => Command::Nack(header(name, h)?),
            op::GET => Command::Get(header(name, h)?),
            op::GET_OK => Command::GetOk(header(name, h)?, frame.payload),
            other => return Err(ProtoError::UnknownOp(other)),
        })
    }

    /// Appends the command as a length-prefixed frame.
    pub fn encode(&self, out: &mut BytesMut) {
        let op = self.op();
        match self {
            Command::Ping | Command::Pong => write(out, op, None::<&()>, &[]),
            Command::Hello(h) => write(out, op, Some(h), &[]),
            Command::Welcome(h) => write(out, op, Some(h), &[]),
            Command::Close(h) => write(out, op, Some(h), &[]),
            Command::Ok(h) => write(out, op, Some(h), &[]),
            Command::Error(h) => write(out, op, Some(h), &[]),
            Command::Flow(h) => write(out, op, Some(h), &[]),
            Command::DeclareExchange(h) => write(out, op, Some(h), &[]),
            Command::DeleteExchange(h) => write(out, op, Some(h), &[]),
            Command::DeclareQueue(h) => write(out, op, Some(h), &[]),
            Command::DeleteQueue(h) => write(out, op, Some(h), &[]),
            Command::PurgeQueue(h) => write(out, op, Some(h), &[]),
            Command::Bind(h) | Command::Unbind(h) => write(out, op, Some(h), &[]),
            Command::Publish(h, p) => write(out, op, Some(h), p),
            Command::Confirm(h) => write(out, op, Some(h), &[]),
            Command::Return(h, p) => write(out, op, Some(h), p),
            Command::Subscribe(h) => write(out, op, Some(h), &[]),
            Command::Unsubscribe(h) => write(out, op, Some(h), &[]),
            Command::Credit(h) => write(out, op, Some(h), &[]),
            Command::Deliver(h, p) => write(out, op, Some(h), p),
            Command::Cancelled(h) => write(out, op, Some(h), &[]),
            Command::Ack(h) => write(out, op, Some(h), &[]),
            Command::Nack(h) => write(out, op, Some(h), &[]),
            Command::Get(h) => write(out, op, Some(h), &[]),
            Command::GetOk(h, p) => write(out, op, Some(h), p),
        }
    }

    /// Encodes the command as a WebSocket message (a frame without the length prefix).
    pub fn to_ws(&self) -> Bytes {
        let mut out = BytesMut::new();
        self.encode(&mut out);
        out.freeze().slice(4..)
    }

    /// Encodes the command into a new buffer, with the length prefix.
    pub fn to_bytes(&self) -> Bytes {
        let mut out = BytesMut::new();
        self.encode(&mut out);
        out.freeze()
    }
}

fn write<T: Serialize>(out: &mut BytesMut, op: u8, header: Option<&T>, payload: &[u8]) {
    let start = out.len();
    out.reserve(9 + 64 + payload.len());
    out.put_u32(0);
    out.put_u8(op);
    out.put_u32(0);
    let header_start = out.len();
    if let Some(h) = header {
        rmp_serde::encode::write_named(&mut (&mut *out).writer(), h)
            .expect("protocol headers always serialize");
    }
    let hlen = (out.len() - header_start) as u32;
    out[start + 5..start + 9].copy_from_slice(&hlen.to_be_bytes());
    out.put_slice(payload);
    let len = (out.len() - start - 4) as u32;
    out[start..start + 4].copy_from_slice(&len.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode_frame;

    fn round_trip(cmd: Command) {
        let mut buf = BytesMut::new();
        cmd.encode(&mut buf);
        let raw = decode_frame(&mut buf, u32::MAX).unwrap().unwrap();
        assert!(buf.is_empty());
        assert_eq!(Command::decode(raw).unwrap(), cmd);
        let ws = RawFrame::from_ws(cmd.to_ws()).unwrap();
        assert_eq!(Command::decode(ws).unwrap(), cmd);
    }

    #[test]
    fn commands_round_trip() {
        let mut headers = Map::new();
        headers.insert("x-retries".into(), rmpv::Value::from(3));
        headers.insert(
            "tags".into(),
            rmpv::Value::Array(vec!["a".into(), "b".into()]),
        );
        round_trip(Command::Hello(Hello {
            ver: 1,
            vhost: "/".into(),
            auth: Auth::plain("guest", "guest"),
            heartbeat: 30,
            client: Map::new(),
            caps: vec!["confirms".into()],
        }));
        round_trip(Command::Ping);
        round_trip(Command::Publish(
            Publish {
                seq: 42,
                exchange: "orders".into(),
                key: "order.created".into(),
                mandatory: true,
                persistent: true,
                props: Properties {
                    content_type: Some("application/json".into()),
                    expiration: Some(60_000),
                    ..Default::default()
                },
                headers: headers.clone(),
            },
            Bytes::from_static(b"{\"id\":1}"),
        ));
        round_trip(Command::Deliver(
            Deliver {
                sub: 1,
                tag: 7,
                redelivered: true,
                exchange: "".into(),
                key: "q".into(),
                props: Properties::default(),
                headers,
            },
            Bytes::from_static(b"body"),
        ));
        round_trip(Command::Confirm(Confirm {
            upto: Some(10),
            ..Default::default()
        }));
        round_trip(Command::Ok(OkReply {
            rid: 3,
            name: Some("amq.gen-x".into()),
            messages: Some(0),
            consumers: Some(0),
            empty: false,
        }));
        round_trip(Command::Error(ErrorReply {
            rid: None,
            code: 406,
            msg: "unknown delivery tag 9".into(),
            fatal: false,
        }));
    }

    #[test]
    fn omitted_fields_take_zero_values() {
        // {"queue": "jobs"} only.
        let header = rmp_serde::to_vec_named(&serde_json::json!({"queue": "jobs"})).unwrap();
        let mut buf = BytesMut::new();
        crate::encode_raw(&mut buf, op::SUBSCRIBE, &header, &[]);
        let raw = decode_frame(&mut buf, u32::MAX).unwrap().unwrap();
        let Command::Subscribe(s) = Command::decode(raw).unwrap() else {
            panic!("expected subscribe")
        };
        assert_eq!(s.queue, "jobs");
        assert_eq!(s.credit, 0);
        assert!(!s.no_ack);
    }

    #[test]
    fn unknown_keys_are_ignored() {
        let header =
            rmp_serde::to_vec_named(&serde_json::json!({"tag": 5, "future_field": [1, 2]}))
                .unwrap();
        let mut buf = BytesMut::new();
        crate::encode_raw(&mut buf, op::ACK, &header, &[]);
        let raw = decode_frame(&mut buf, u32::MAX).unwrap().unwrap();
        assert_eq!(
            Command::decode(raw).unwrap(),
            Command::Ack(Ack {
                tag: 5,
                multiple: false
            })
        );
    }

    #[test]
    fn unknown_ops_are_reported() {
        let raw = RawFrame {
            op: 0x7f,
            header: Bytes::new(),
            payload: Bytes::new(),
        };
        assert!(matches!(
            Command::decode(raw),
            Err(ProtoError::UnknownOp(0x7f))
        ));
    }
}

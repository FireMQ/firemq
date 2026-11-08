//! The FireMQ protocol server, over TCP, TLS or WebSocket.
//!
//! A reader task turns bytes or WebSocket messages into frames; the connection task owns all
//! state; a writer task writes encoded frames. The connection task always encodes frames
//! with their TCP length prefix, and the WebSocket writer strips it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use firemq_core::auth::{Access, User};
use firemq_core::{
    Broker, BrokerError, ConnEvent, ConnectionInfo, ConnectionRegistration, ConsumerSpec, Delivery,
    Destination, ExchangeOptions, Limit, Message, Owner, QueueHandle, QueueOptions, Ticket, VHost,
    now_ms,
};
use firemq_proto::{
    self as proto, Command, ErrorReply, MAGIC, MAX_HELLO_FRAME, OkReply, ProtoError, RawFrame, code,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout};

use crate::convert;

/// Output bytes buffered before the connection stops pulling deliveries.
const OUT_HIGH: usize = 256 * 1024;

/// Settings for FireMQ protocol connections.
#[derive(Debug, Clone)]
pub struct NativeConfig {
    /// Heartbeat interval the server offers, in seconds.
    pub heartbeat: u32,
    /// Largest frame accepted after `welcome`.
    pub max_frame: u32,
    /// Deliveries buffered per connection before queues park its subscriptions.
    pub delivery_buffer: usize,
    /// Time allowed between accept and `welcome`.
    pub handshake_timeout: Duration,
}

impl Default for NativeConfig {
    fn default() -> Self {
        Self {
            heartbeat: 60,
            max_frame: proto::DEFAULT_MAX_FRAME,
            delivery_buffer: 256,
            handshake_timeout: Duration::from_secs(10),
        }
    }
}

type Inbound = mpsc::Receiver<Result<RawFrame, ProtoError>>;

async fn write_stream<W: AsyncWrite + Unpin>(mut w: W, mut rx: mpsc::Receiver<Bytes>) {
    while let Some(first) = rx.recv().await {
        if w.write_all(&first).await.is_err() {
            return;
        }
        while let Ok(more) = rx.try_recv() {
            if w.write_all(&more).await.is_err() {
                return;
            }
        }
        if w.flush().await.is_err() {
            return;
        }
    }
    let _ = w.shutdown().await;
}

/// Serves one connection over a byte stream (plain TCP or TLS).
pub async fn serve_stream<S>(
    broker: Broker,
    cfg: Arc<NativeConfig>,
    stream: S,
    peer: SocketAddr,
    local: SocketAddr,
    tls: bool,
) where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (mut reader, writer) = tokio::io::split(stream);
    let (frames_tx, frames_rx) = mpsc::channel(64);
    let (wtx, wrx) = mpsc::channel::<Bytes>(64);
    let max_frame = Arc::new(AtomicU32::new(MAX_HELLO_FRAME));
    let limit = max_frame.clone();
    let reader_task = tokio::spawn(async move {
        let mut buf = BytesMut::with_capacity(16 * 1024);
        while buf.len() < 4 {
            match reader.read_buf(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
        if &buf[..4] != MAGIC {
            let _ = frames_tx.send(Err(ProtoError::BadMagic)).await;
            return;
        }
        buf.advance(4);
        loop {
            loop {
                match proto::decode_frame(&mut buf, limit.load(Ordering::Relaxed)) {
                    Ok(Some(f)) => {
                        if frames_tx.send(Ok(f)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = frames_tx.send(Err(e)).await;
                        return;
                    }
                }
            }
            match reader.read_buf(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    });
    let writer_task = tokio::spawn(write_stream(writer, wrx));
    run(
        broker, cfg, frames_rx, wtx, max_frame, peer, local, tls, "tcp",
    )
    .await;
    reader_task.abort();
    let _ = timeout(Duration::from_secs(5), writer_task).await;
}

/// Serves one connection over an axum WebSocket.
pub async fn serve_ws(
    broker: Broker,
    cfg: Arc<NativeConfig>,
    socket: axum::extract::ws::WebSocket,
    peer: SocketAddr,
    local: SocketAddr,
    tls: bool,
) {
    use axum::extract::ws::Message as Ws;
    use futures::{SinkExt, StreamExt};
    let (mut sink, mut stream) = socket.split();
    let (frames_tx, frames_rx) = mpsc::channel(64);
    let (wtx, mut wrx) = mpsc::channel::<Bytes>(64);
    let max_frame = Arc::new(AtomicU32::new(MAX_HELLO_FRAME));
    let limit = max_frame.clone();
    let reader_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = stream.next().await {
            let frame = match msg {
                Ws::Binary(b) => {
                    if b.len() as u64 > limit.load(Ordering::Relaxed) as u64 {
                        Err(ProtoError::FrameTooLarge {
                            size: b.len() as u32,
                            max: limit.load(Ordering::Relaxed),
                        })
                    } else {
                        RawFrame::from_ws(b)
                    }
                }
                Ws::Close(_) => return,
                Ws::Text(_) => Err(ProtoError::Malformed(
                    "text WebSocket messages are not frames",
                )),
                _ => continue,
            };
            let failed = frame.is_err();
            if frames_tx.send(frame).await.is_err() || failed {
                return;
            }
        }
    });
    let writer_task = tokio::spawn(async move {
        while let Some(mut chunk) = wrx.recv().await {
            while chunk.len() >= 4 {
                let len = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) as usize;
                if chunk.len() < 4 + len {
                    break;
                }
                chunk.advance(4);
                let frame = chunk.split_to(len);
                if sink.send(Ws::Binary(frame)).await.is_err() {
                    return;
                }
            }
        }
        let _ = sink.close().await;
    });
    run(
        broker,
        cfg,
        frames_rx,
        wtx,
        max_frame,
        peer,
        local,
        tls,
        "websocket",
    )
    .await;
    reader_task.abort();
    let _ = timeout(Duration::from_secs(5), writer_task).await;
}

fn err(rid: Option<u32>, e: &BrokerError) -> Command {
    Command::Error(ErrorReply {
        rid,
        code: e.code,
        msg: e.text.clone(),
        fatal: false,
    })
}

fn fatal(code: u16, msg: impl Into<String>) -> Command {
    Command::Error(ErrorReply {
        rid: None,
        code,
        msg: msg.into(),
        fatal: true,
    })
}

async fn send_now(wtx: &mpsc::Sender<Bytes>, cmd: Command) {
    let _ = wtx.send(cmd.to_bytes()).await;
}

#[allow(clippy::too_many_arguments)]
async fn run(
    broker: Broker,
    cfg: Arc<NativeConfig>,
    mut frames: Inbound,
    wtx: mpsc::Sender<Bytes>,
    max_frame: Arc<AtomicU32>,
    peer: SocketAddr,
    local: SocketAddr,
    tls: bool,
    transport: &'static str,
) {
    let first = match timeout(cfg.handshake_timeout, frames.recv()).await {
        Ok(Some(Ok(f))) => f,
        Ok(Some(Err(e))) => {
            send_now(&wtx, fatal(code::FRAME_ERROR, format!("FRAME_ERROR - {e}"))).await;
            return;
        }
        _ => return,
    };
    let hello = match Command::decode(first) {
        Ok(Command::Hello(h)) => h,
        Ok(other) => {
            let msg = format!("COMMAND_INVALID - expected hello, got {}", other.name());
            send_now(&wtx, fatal(code::COMMAND_INVALID, msg)).await;
            return;
        }
        Err(e) => {
            send_now(
                &wtx,
                fatal(code::SYNTAX_ERROR, format!("SYNTAX_ERROR - {e}")),
            )
            .await;
            return;
        }
    };
    if hello.ver != proto::PROTOCOL_VERSION {
        let msg = format!(
            "NOT_IMPLEMENTED - protocol version {} is not supported; this server speaks version {}",
            hello.ver,
            proto::PROTOCOL_VERSION
        );
        send_now(&wtx, fatal(code::NOT_IMPLEMENTED, msg)).await;
        return;
    }
    if hello.auth.mech != "plain" {
        let msg = format!(
            "ACCESS_REFUSED - unsupported auth mechanism '{}'; use plain",
            hello.auth.mech
        );
        send_now(&wtx, fatal(code::ACCESS_REFUSED, msg)).await;
        return;
    }
    let user = match broker
        .users()
        .authenticate(&hello.auth.user, &hello.auth.pass, "plain")
    {
        Ok(u) => u,
        Err(e) => {
            tracing::info!(%peer, user = %hello.auth.user, "authentication failed");
            send_now(&wtx, fatal(e.code, e.text)).await;
            return;
        }
    };
    if broker.is_loopback_only(&user.name) && !peer.ip().is_loopback() {
        let msg = format!(
            "ACCESS_REFUSED - user '{}' can only connect via localhost. Create another user, or set FIREMQ_LOOPBACK_USERS to an empty value",
            user.name
        );
        send_now(&wtx, fatal(code::ACCESS_REFUSED, msg)).await;
        return;
    }
    let vhost_name = if hello.vhost.is_empty() {
        "/".to_string()
    } else {
        hello.vhost.clone()
    };
    let Some(vhost) = broker.vhost(&vhost_name) else {
        let msg = format!("NOT_ALLOWED - vhost '{vhost_name}' not found");
        send_now(&wtx, fatal(code::NOT_ALLOWED, msg)).await;
        return;
    };
    if !user.can_access_vhost(&vhost.name) {
        let msg = format!(
            "NOT_ALLOWED - access to vhost '{}' refused for user '{}'",
            vhost.name, user.name
        );
        send_now(&wtx, fatal(code::NOT_ALLOWED, msg)).await;
        return;
    }
    let heartbeat = match hello.heartbeat {
        0 => 0,
        h => h.min(cfg.heartbeat.max(1)),
    };
    let id = broker.next_id();
    let name = format!("{peer} -> {local}");
    max_frame.store(cfg.max_frame, Ordering::Relaxed);
    send_now(
        &wtx,
        Command::Welcome(proto::Welcome {
            ver: proto::PROTOCOL_VERSION,
            server: proto::ServerInfo {
                name: "firemq".into(),
                version: env!("CARGO_PKG_VERSION").into(),
            },
            conn_id: name.clone(),
            heartbeat,
            max_frame: cfg.max_frame,
            caps: [
                "confirms",
                "credit",
                "mandatory",
                "exchange-bindings",
                "dead-letter",
                "ttl",
            ]
            .map(String::from)
            .to_vec(),
        }),
    )
    .await;
    let (deliveries_tx, deliveries_rx) = mpsc::channel(cfg.delivery_buffer.max(1));
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let client_props = serde_json::to_value(
        hello
            .client
            .iter()
            .map(|(k, v)| (k.clone(), v.to_string().trim_matches('"').to_string()))
            .collect::<BTreeMap<_, _>>(),
    )
    .unwrap_or_default();
    let (info, registration) = broker.register_connection(ConnectionInfo {
        id,
        name,
        protocol: format!("FireMQ 1 ({transport})"),
        peer: peer.to_string(),
        local: local.to_string(),
        user: user.name.clone(),
        vhost: vhost.name.clone(),
        tls,
        connected_at: now_ms(),
        client_properties: client_props,
        events: events_tx.clone(),
        channels: Default::default(),
        recv_bytes: Default::default(),
        send_bytes: Default::default(),
        published: Default::default(),
        delivered: Default::default(),
    });
    tracing::debug!(conn = id, %peer, user = %user.name, vhost = %vhost.name, transport, "FireMQ connection opened");
    let conn = Conn {
        broker,
        id,
        user,
        vhost,
        info,
        _registration: registration,
        frames,
        writer: wtx,
        out: BytesMut::with_capacity(16 * 1024),
        deliveries_tx,
        deliveries_rx,
        events_tx,
        events_rx,
        subs: HashMap::new(),
        consumers: HashMap::new(),
        unacked: BTreeMap::new(),
        next_tag: 1,
        pending: BTreeSet::new(),
        acked: BTreeSet::new(),
        returned: BTreeSet::new(),
        max_seq: 0,
        emitted: 0,
        heartbeat,
        last_recv: Instant::now(),
        last_send: Instant::now(),
        blocked: false,
        done: false,
    };
    conn.run().await;
}

struct Sub {
    consumer_id: u64,
    queue: QueueHandle,
}

struct Conn {
    broker: Broker,
    id: u64,
    user: Arc<User>,
    vhost: Arc<VHost>,
    info: Arc<ConnectionInfo>,
    _registration: ConnectionRegistration,
    frames: Inbound,
    writer: mpsc::Sender<Bytes>,
    out: BytesMut,
    deliveries_tx: mpsc::Sender<Delivery>,
    deliveries_rx: mpsc::Receiver<Delivery>,
    events_tx: mpsc::UnboundedSender<ConnEvent>,
    events_rx: mpsc::UnboundedReceiver<ConnEvent>,
    subs: HashMap<u32, Sub>,
    consumers: HashMap<u64, u32>,
    unacked: BTreeMap<u64, (QueueHandle, u64)>,
    next_tag: u64,
    pending: BTreeSet<u64>,
    acked: BTreeSet<u64>,
    returned: BTreeSet<u64>,
    max_seq: u64,
    emitted: u64,
    heartbeat: u32,
    last_recv: Instant,
    last_send: Instant,
    blocked: bool,
    done: bool,
}

type Res<T = ()> = Result<T, BrokerError>;

impl Conn {
    fn owner(&self) -> Owner {
        Owner {
            conn: self.id,
            channel: 0,
        }
    }

    fn send(&mut self, cmd: Command) {
        cmd.encode(&mut self.out);
    }

    fn check(&self, access: Access, kind: &str, name: &str) -> Res {
        self.user.check(&self.vhost.name, access, kind, name)
    }

    async fn run(mut self) {
        let tick = Duration::from_secs((self.heartbeat as u64 / 2).max(1));
        let mut heartbeat = tokio::time::interval(tick);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        while !self.done {
            tokio::select! {
                permit = self.writer.clone().reserve_owned(), if !self.out.is_empty() => {
                    match permit {
                        Ok(p) => {
                            let chunk = self.out.split().freeze();
                            self.info.send_bytes.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                            p.send(chunk);
                            self.last_send = Instant::now();
                        }
                        Err(_) => break,
                    }
                }
                frame = self.frames.recv(), if !self.blocked => {
                    match frame {
                        None => break,
                        Some(Err(e)) => {
                            let cmd = fatal(code::FRAME_ERROR, format!("FRAME_ERROR - {e}"));
                            self.send(cmd);
                            break;
                        }
                        Some(Ok(f)) => {
                            self.last_recv = Instant::now();
                            self.info.recv_bytes.fetch_add(
                                (9 + f.header.len() + f.payload.len()) as u64,
                                Ordering::Relaxed,
                            );
                            self.on_frame(f).await;
                        }
                    }
                }
                Some(ev) = self.events_rx.recv() => self.on_event(ev),
                Some(d) = self.deliveries_rx.recv(), if self.out.len() < OUT_HIGH => {
                    self.on_delivery(d).await;
                    while self.out.len() < OUT_HIGH {
                        match self.deliveries_rx.try_recv() {
                            Ok(d) => self.on_delivery(d).await,
                            Err(_) => break,
                        }
                    }
                }
                _ = heartbeat.tick(), if self.heartbeat > 0 => {
                    if self.last_send.elapsed() >= tick {
                        self.send(Command::Ping);
                    }
                    if self.last_recv.elapsed() > Duration::from_secs(self.heartbeat as u64 * 2) {
                        tracing::info!(conn = self.id, "missed heartbeats from client, timeout: {}s", self.heartbeat);
                        break;
                    }
                }
            }
        }
        if !self.out.is_empty() {
            let chunk = self.out.split().freeze();
            let _ = timeout(Duration::from_secs(1), self.writer.send(chunk)).await;
        }
        self.cleanup().await;
        tracing::debug!(conn = self.id, "FireMQ connection closed");
    }

    async fn on_frame(&mut self, frame: RawFrame) {
        let op = frame.op;
        let cmd = match Command::decode(frame) {
            Ok(c) => c,
            Err(ProtoError::UnknownOp(op)) => {
                let e = BrokerError::not_implemented(format!("unknown op {op:#04x}"));
                self.send(err(None, &e));
                return;
            }
            Err(e) => {
                let e = BrokerError::new(
                    code::SYNTAX_ERROR,
                    format!("SYNTAX_ERROR - {} ({e})", proto::op_name(op)),
                );
                self.send(err(None, &e));
                return;
            }
        };
        let rid = cmd.rid();
        if let Err(e) = self.dispatch(cmd).await {
            self.send(err(rid, &e));
        }
    }

    async fn dispatch(&mut self, cmd: Command) -> Res {
        let conn = Some(self.id);
        match cmd {
            Command::Ping => self.send(Command::Pong),
            Command::Pong => {}
            Command::Close(_) => {
                self.send(Command::Close(proto::Close {
                    code: 200,
                    msg: "bye".into(),
                }));
                self.done = true;
            }
            Command::DeclareExchange(c) => {
                if !c.passive {
                    self.check(Access::Configure, "exchange", &c.name)?;
                }
                self.vhost.declare_exchange(ExchangeOptions {
                    name: c.name,
                    kind: if c.kind.is_empty() {
                        "direct".into()
                    } else {
                        c.kind
                    },
                    durable: c.durable,
                    auto_delete: c.auto_delete,
                    internal: c.internal,
                    passive: c.passive,
                    arguments: convert::map_to_table(&c.args),
                })?;
                self.ok(c.rid);
            }
            Command::DeleteExchange(c) => {
                self.check(Access::Configure, "exchange", &c.name)?;
                self.vhost.delete_exchange(&c.name, c.if_unused)?;
                self.ok(c.rid);
            }
            Command::DeclareQueue(c) => {
                if !c.passive && !c.name.is_empty() {
                    self.check(Access::Configure, "queue", &c.name)?;
                }
                let d = self.vhost.declare_queue(
                    QueueOptions {
                        name: c.name,
                        durable: c.durable,
                        exclusive: c.exclusive,
                        auto_delete: c.auto_delete,
                        passive: c.passive,
                        arguments: convert::map_to_table(&c.args),
                    },
                    conn,
                )?;
                self.send(Command::Ok(OkReply {
                    rid: c.rid,
                    name: Some(d.queue.name.clone()),
                    messages: Some(d.message_count),
                    consumers: Some(d.consumer_count),
                    empty: false,
                }));
            }
            Command::DeleteQueue(c) => {
                self.check(Access::Configure, "queue", &c.name)?;
                let n = self
                    .vhost
                    .delete_queue(&c.name, c.if_unused, c.if_empty, conn)
                    .await?;
                self.ok_messages(c.rid, n);
            }
            Command::PurgeQueue(c) => {
                self.check(Access::Read, "queue", &c.name)?;
                let n = self.vhost.purge_queue(&c.name, conn).await?;
                self.ok_messages(c.rid, n);
            }
            Command::Bind(c) | Command::Unbind(c)
                if c.queue.is_empty() == c.to_exchange.is_empty() =>
            {
                let _ = c;
                return Err(BrokerError::precondition_failed(
                    "set exactly one of 'queue' and 'to_exchange'",
                ));
            }
            Command::Bind(c) => {
                let dest = self.destination(&c.queue, &c.to_exchange)?;
                self.check(Access::Read, "exchange", &c.exchange)?;
                self.vhost.bind(
                    &c.exchange,
                    dest,
                    &c.key,
                    convert::map_to_table(&c.args),
                    conn,
                )?;
                self.ok(c.rid);
            }
            Command::Unbind(c) => {
                let dest = self.destination(&c.queue, &c.to_exchange)?;
                self.check(Access::Read, "exchange", &c.exchange)?;
                self.vhost.unbind(
                    &c.exchange,
                    dest,
                    &c.key,
                    &convert::map_to_table(&c.args),
                    conn,
                )?;
                self.ok(c.rid);
            }
            Command::Publish(p, body) => self.publish(p, body).await?,
            Command::Subscribe(s) => {
                self.check(Access::Read, "queue", &s.queue)?;
                if self.subs.contains_key(&s.sub) {
                    return Err(BrokerError::precondition_failed(format!(
                        "subscription id {} is already in use",
                        s.sub
                    )));
                }
                let q = self.vhost.queue(&s.queue, conn)?;
                let consumer_id = self.broker.next_id();
                let tag = if s.tag.is_empty() {
                    format!("fire-{}", s.sub)
                } else {
                    s.tag
                };
                q.consume(ConsumerSpec {
                    id: consumer_id,
                    tag,
                    owner: self.owner(),
                    no_ack: s.no_ack,
                    exclusive: s.exclusive,
                    limit: Limit::Credit(s.credit),
                    shared_limit: None,
                    arguments: convert::map_to_table(&s.args),
                    deliveries: self.deliveries_tx.clone(),
                    events: self.events_tx.clone(),
                })
                .await?;
                self.consumers.insert(consumer_id, s.sub);
                self.subs.insert(
                    s.sub,
                    Sub {
                        consumer_id,
                        queue: q,
                    },
                );
                self.ok(s.rid);
            }
            Command::Unsubscribe(u) => {
                if let Some(sub) = self.subs.remove(&u.sub) {
                    self.consumers.remove(&sub.consumer_id);
                    sub.queue.cancel(sub.consumer_id).await;
                }
                self.ok(u.rid);
            }
            Command::Credit(c) => {
                if let Some(sub) = self.subs.get(&c.sub) {
                    sub.queue.credit(sub.consumer_id, c.credit).await;
                }
            }
            Command::Ack(a) => {
                let items = self.take(a.tag, a.multiple)?;
                let owner = self.owner();
                for (q, seqs) in group(items) {
                    q.ack(owner, seqs).await;
                }
            }
            Command::Nack(n) => {
                let items = self.take(n.tag, n.multiple)?;
                let owner = self.owner();
                for (q, seqs) in group(items) {
                    q.reject(owner, seqs, n.requeue).await;
                }
            }
            Command::Get(g) => {
                self.check(Access::Read, "queue", &g.queue)?;
                let q = self.vhost.queue(&g.queue, conn)?;
                match q.get(self.owner(), g.no_ack).await? {
                    Some(r) => {
                        let tag = self.next_tag;
                        self.next_tag += 1;
                        if !g.no_ack {
                            self.unacked.insert(tag, (q, r.seq));
                        }
                        let (props, headers) = convert::from_basic(&r.message.properties);
                        self.send(Command::GetOk(
                            proto::GetOk {
                                rid: g.rid,
                                tag,
                                redelivered: r.redelivered,
                                exchange: r.message.exchange.clone(),
                                key: r.message.routing_key.clone(),
                                remaining: r.remaining,
                                props,
                                headers,
                            },
                            r.message.body.clone(),
                        ));
                    }
                    None => self.send(Command::Ok(OkReply {
                        rid: g.rid,
                        empty: true,
                        ..Default::default()
                    })),
                }
            }
            Command::Hello(_) => {
                return Err(BrokerError::command_invalid("hello was already received"));
            }
            other => {
                return Err(BrokerError::command_invalid(format!(
                    "{} is sent by servers, not clients",
                    other.name()
                )));
            }
        }
        Ok(())
    }

    fn ok(&mut self, rid: u32) {
        self.send(Command::Ok(OkReply {
            rid,
            ..Default::default()
        }));
    }

    fn ok_messages(&mut self, rid: u32, n: u64) {
        self.send(Command::Ok(OkReply {
            rid,
            messages: Some(n),
            ..Default::default()
        }));
    }

    fn destination(&self, queue: &str, to_exchange: &str) -> Res<Destination> {
        if !queue.is_empty() {
            self.check(Access::Write, "queue", queue)?;
            Ok(Destination::Queue(queue.to_string()))
        } else {
            self.check(Access::Write, "exchange", to_exchange)?;
            Ok(Destination::Exchange(to_exchange.to_string()))
        }
    }

    fn take(&mut self, tag: u64, multiple: bool) -> Res<Vec<(QueueHandle, u64)>> {
        if multiple {
            let rest = self.unacked.split_off(&(tag + 1));
            let taken = std::mem::replace(&mut self.unacked, rest);
            Ok(taken.into_values().collect())
        } else {
            self.unacked.remove(&tag).map(|x| vec![x]).ok_or_else(|| {
                BrokerError::precondition_failed(format!("unknown delivery tag {tag}"))
            })
        }
    }

    async fn publish(&mut self, p: proto::Publish, body: Bytes) -> Res {
        let result = self.route_and_publish(&p, body).await;
        if let Err(e) = &result
            && p.seq > 0
        {
            // A publish that failed before reaching a queue is rejected.
            self.complete(p.seq, false);
            self.send(Command::Confirm(proto::Confirm {
                seq: Some(p.seq),
                nack: true,
                code: e.code,
                msg: e.text.clone(),
                ..Default::default()
            }));
        }
        result
    }

    async fn route_and_publish(&mut self, p: &proto::Publish, body: Bytes) -> Res {
        if p.seq > 0 {
            self.pending.insert(p.seq);
            self.max_seq = self.max_seq.max(p.seq);
        }
        self.check(Access::Write, "exchange", &p.exchange)?;
        if let Some(uid) = &p.props.user_id
            && *uid != self.user.name
        {
            return Err(BrokerError::precondition_failed(format!(
                "user_id property set to '{uid}' but authenticated user was '{}'",
                self.user.name
            )));
        }
        let props = convert::to_basic(&p.props, &p.headers, p.persistent);
        let queues = self
            .vhost
            .route(&p.exchange, &p.key, props.headers.as_ref())?;
        self.info.published.fetch_add(1, Ordering::Relaxed);
        let ticket = (p.seq > 0).then(|| Ticket::new(self.events_tx.clone(), 0, p.seq));
        if queues.is_empty() {
            if p.mandatory {
                if p.seq > 0 {
                    self.returned.insert(p.seq);
                }
                self.send(Command::Return(
                    proto::Return {
                        seq: p.seq,
                        code: code::NO_ROUTE,
                        msg: "NO_ROUTE".into(),
                        exchange: p.exchange.clone(),
                        key: p.key.clone(),
                        props: p.props.clone(),
                        headers: p.headers.clone(),
                    },
                    body,
                ));
            }
            return Ok(());
        }
        let msg = Arc::new(Message::new(p.exchange.clone(), p.key.clone(), props, body));
        for q in &queues {
            q.publish(msg.clone(), ticket.clone()).await;
        }
        Ok(())
    }

    /// Records a finished publish and returns the new cumulative confirm, if any.
    fn complete(&mut self, seq: u64, ack: bool) -> Option<u64> {
        if !self.pending.remove(&seq) {
            return None;
        }
        self.returned.remove(&seq);
        if ack {
            self.acked.insert(seq);
        }
        let watermark = match self.pending.first() {
            Some(first) => first - 1,
            None => self.max_seq,
        };
        if watermark <= self.emitted {
            return None;
        }
        let has_ack = self.acked.range(..=watermark).next().is_some();
        self.acked = self.acked.split_off(&(watermark + 1));
        self.emitted = watermark;
        has_ack.then_some(watermark)
    }

    async fn on_delivery(&mut self, d: Delivery) {
        let Some(&sub) = self.consumers.get(&d.consumer_id) else {
            if !d.no_ack {
                d.queue.reject(d.owner, vec![d.seq], true).await;
            }
            return;
        };
        let tag = self.next_tag;
        self.next_tag += 1;
        if !d.no_ack {
            self.unacked.insert(tag, (d.queue.clone(), d.seq));
        }
        let (props, headers) = convert::from_basic(&d.message.properties);
        self.send(Command::Deliver(
            proto::Deliver {
                sub,
                tag,
                redelivered: d.redelivered,
                exchange: d.message.exchange.clone(),
                key: d.message.routing_key.clone(),
                props,
                headers,
            },
            d.message.body.clone(),
        ));
        self.info.delivered.fetch_add(1, Ordering::Relaxed);
    }

    fn on_event(&mut self, ev: ConnEvent) {
        match ev {
            ConnEvent::Confirm { seq, ack, .. } => {
                if !ack {
                    self.send(Command::Confirm(proto::Confirm {
                        seq: Some(seq),
                        nack: true,
                        code: code::PRECONDITION_FAILED,
                        msg: "PRECONDITION_FAILED - a queue rejected the message".into(),
                        ..Default::default()
                    }));
                }
                if let Some(upto) = self.complete(seq, ack) {
                    self.send(Command::Confirm(proto::Confirm {
                        upto: Some(upto),
                        ..Default::default()
                    }));
                }
            }
            ConnEvent::ConsumerCancelled {
                consumer_id,
                reason,
            } => {
                if let Some(sub) = self.consumers.remove(&consumer_id) {
                    self.subs.remove(&sub);
                    self.send(Command::Cancelled(proto::Cancelled {
                        sub,
                        code: code::NOT_FOUND,
                        msg: reason,
                    }));
                }
            }
            ConnEvent::Blocked(b) => {
                self.send(Command::Flow(proto::Flow { pause: b }));
            }
            ConnEvent::Close { code, reason } => {
                self.send(fatal(code, reason));
                self.send(Command::Close(proto::Close {
                    code,
                    msg: String::new(),
                }));
                self.done = true;
            }
            ConnEvent::Wake => {}
        }
    }

    async fn cleanup(&mut self) {
        let mut queues: Vec<QueueHandle> = Vec::new();
        for (q, _) in std::mem::take(&mut self.unacked).into_values() {
            if !queues.contains(&q) {
                queues.push(q);
            }
        }
        for (_, s) in self.subs.drain() {
            if !queues.contains(&s.queue) {
                queues.push(s.queue);
            }
        }
        self.deliveries_rx.close();
        while let Ok(d) = self.deliveries_rx.try_recv() {
            if !queues.contains(&d.queue) {
                queues.push(d.queue);
            }
        }
        for q in queues {
            q.owner_closed(self.id, None).await;
        }
        self.vhost.connection_closed(self.id).await;
    }
}

fn group(items: Vec<(QueueHandle, u64)>) -> Vec<(QueueHandle, Vec<u64>)> {
    let mut out: Vec<(QueueHandle, Vec<u64>)> = Vec::new();
    for (q, seq) in items {
        match out.iter_mut().find(|(x, _)| *x == q) {
            Some((_, v)) => v.push(seq),
            None => out.push((q, vec![seq])),
        }
    }
    out
}

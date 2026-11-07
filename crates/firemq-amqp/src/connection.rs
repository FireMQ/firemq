use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use base64::Engine;
use bytes::{Bytes, BytesMut};
use firemq_amqp_codec::methods::*;
use firemq_amqp_codec::{
    self as codec, BasicProperties, CodecError, FieldTable, Frame, LongString, Method, Value,
    reply_code,
};
use firemq_core::auth::{Access, User};
use firemq_core::{
    Broker, BrokerError, ConnEvent, ConnectionInfo, ConnectionRegistration, ConsumerSpec, Delivery,
    Destination, ExchangeOptions, Limit, Message, Owner, QueueHandle, QueueOptions, SharedLimit,
    Ticket, VHost, json, now_ms,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf};
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout};

use crate::AmqpConfig;
use crate::channel::{Channel, PendingContent, Taken, TxOp, group_by_queue};

/// Output bytes buffered before the connection stops pulling deliveries.
const OUT_HIGH: usize = 256 * 1024;

type Res<T = ()> = Result<T, BrokerError>;

fn random_tag(prefix: &str) -> String {
    let bytes: [u8; 16] = rand::random();
    format!(
        "{prefix}{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    )
}

async fn write_loop<W: AsyncWrite + Unpin>(mut w: W, mut rx: mpsc::Receiver<Bytes>) {
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

#[derive(Debug)]
enum HandshakeError {
    Io(std::io::Error),
    Codec(CodecError),
    Refused(String),
    Unexpected(&'static str),
}

impl std::fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandshakeError::Io(e) => write!(f, "I/O error: {e}"),
            HandshakeError::Codec(e) => write!(f, "malformed frame: {e}"),
            HandshakeError::Refused(r) => write!(f, "refused: {r}"),
            HandshakeError::Unexpected(r) => f.write_str(r),
        }
    }
}

impl From<std::io::Error> for HandshakeError {
    fn from(e: std::io::Error) -> Self {
        HandshakeError::Io(e)
    }
}

impl From<CodecError> for HandshakeError {
    fn from(e: CodecError) -> Self {
        HandshakeError::Codec(e)
    }
}

struct Session {
    user: Arc<User>,
    vhost: Arc<firemq_core::VHost>,
    frame_max: u32,
    channel_max: u16,
    heartbeat: u16,
    client_properties: FieldTable,
}

async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    inbuf: &mut BytesMut,
    max: u32,
) -> Result<Frame, HandshakeError> {
    loop {
        if let Some(f) = codec::decode_frame(inbuf, max)? {
            return Ok(f);
        }
        if reader.read_buf(inbuf).await? == 0 {
            return Err(HandshakeError::Io(std::io::ErrorKind::UnexpectedEof.into()));
        }
    }
}

async fn expect_method<R: AsyncRead + Unpin>(
    reader: &mut R,
    inbuf: &mut BytesMut,
) -> Result<Method, HandshakeError> {
    loop {
        match read_frame(reader, inbuf, 131_072).await? {
            Frame::Heartbeat => continue,
            Frame::Method { channel: 0, method } => return Ok(method),
            _ => {
                return Err(HandshakeError::Unexpected(
                    "non-method frame during handshake",
                ));
            }
        }
    }
}

async fn send(wtx: &mpsc::Sender<Bytes>, channel: u16, m: impl Into<Method>) {
    let mut out = BytesMut::new();
    codec::encode_method(&mut out, channel, &m.into());
    let _ = wtx.send(out.freeze()).await;
}

async fn refuse(
    wtx: &mpsc::Sender<Bytes>,
    reader: &mut ReadHalf<impl AsyncRead>,
    inbuf: &mut BytesMut,
    code: u16,
    text: String,
    ids: (u16, u16),
) -> HandshakeError {
    send(
        wtx,
        0,
        ConnectionClose {
            reply_code: code,
            reply_text: text.clone(),
            class_id: ids.0,
            method_id: ids.1,
        },
    )
    .await;
    // Give the client a moment to read the reason and reply close-ok.
    let _ = timeout(Duration::from_secs(1), expect_method(reader, inbuf)).await;
    HandshakeError::Refused(text)
}

fn parse_credentials(mechanism: &str, response: &[u8]) -> Option<(String, String)> {
    match mechanism {
        "PLAIN" => {
            let mut parts = response.split(|b| *b == 0);
            let _authzid = parts.next()?;
            let user = parts.next()?;
            let pass = parts.next()?;
            Some((
                String::from_utf8(user.to_vec()).ok()?,
                String::from_utf8(pass.to_vec()).ok()?,
            ))
        }
        "AMQPLAIN" => {
            // A field table body without its length prefix.
            let mut buf = BytesMut::with_capacity(response.len() + 4);
            buf.extend_from_slice(&(response.len() as u32).to_be_bytes());
            buf.extend_from_slice(response);
            let t = FieldTable::decode(buf.freeze()).ok()?;
            Some((
                t.get_str("LOGIN")?.to_string(),
                t.get_str("PASSWORD")?.to_string(),
            ))
        }
        _ => None,
    }
}

fn server_properties(cfg: &AmqpConfig) -> FieldTable {
    let mut caps = FieldTable::new();
    for cap in [
        "publisher_confirms",
        "exchange_exchange_bindings",
        "basic.nack",
        "consumer_cancel_notify",
        "connection.blocked",
        "authentication_failure_close",
        "per_consumer_qos",
    ] {
        caps.insert(cap, true);
    }
    let mut p = FieldTable::new();
    p.insert("capabilities", caps);
    p.insert("product", "FireMQ");
    p.insert("version", env!("CARGO_PKG_VERSION"));
    p.insert("platform", "Rust");
    p.insert("cluster_name", cfg.cluster_name.as_str());
    p.insert("copyright", "Copyright The FireMQ Authors");
    p.insert(
        "information",
        "Licensed under the Apache License 2.0. Website: https://github.com/FireMQ/firemq",
    );
    p
}

async fn handshake<R: AsyncRead>(
    broker: &Broker,
    cfg: &AmqpConfig,
    reader: &mut ReadHalf<R>,
    inbuf: &mut BytesMut,
    wtx: &mpsc::Sender<Bytes>,
    peer: SocketAddr,
) -> Result<Session, HandshakeError> {
    while inbuf.len() < 8 {
        if reader.read_buf(inbuf).await? == 0 {
            return Err(HandshakeError::Io(std::io::ErrorKind::UnexpectedEof.into()));
        }
    }
    let header = inbuf.split_to(8);
    if &header[..] != codec::PROTOCOL_HEADER {
        let _ = wtx.send(Bytes::from_static(codec::PROTOCOL_HEADER)).await;
        return Err(HandshakeError::Unexpected("unsupported protocol header"));
    }
    send(
        wtx,
        0,
        ConnectionStart {
            version_major: 0,
            version_minor: 9,
            server_properties: server_properties(cfg),
            mechanisms: LongString::from("PLAIN AMQPLAIN"),
            locales: LongString::from("en_US"),
        },
    )
    .await;
    let Method::ConnectionStartOk(start_ok) = expect_method(reader, inbuf).await? else {
        return Err(HandshakeError::Unexpected("expected connection.start-ok"));
    };
    let Some((username, password)) =
        parse_credentials(&start_ok.mechanism, start_ok.response.as_bytes())
    else {
        let text = format!(
            "ACCESS_REFUSED - unsupported authentication mechanism '{}'; use PLAIN or AMQPLAIN",
            start_ok.mechanism
        );
        return Err(refuse(wtx, reader, inbuf, 403, text, (10, 11)).await);
    };
    let user = match broker
        .users()
        .authenticate(&username, &password, &start_ok.mechanism)
    {
        Ok(u) => u,
        Err(e) => {
            tracing::info!(%peer, user = %username, "authentication failed");
            return Err(refuse(wtx, reader, inbuf, e.code, e.text, (10, 11)).await);
        }
    };
    if broker.is_loopback_only(&user.name) && !peer.ip().is_loopback() {
        let text = format!(
            "ACCESS_REFUSED - user '{}' can only connect via localhost. Create another user, or set FIREMQ_LOOPBACK_USERS to an empty value",
            user.name
        );
        return Err(refuse(wtx, reader, inbuf, 403, text, (10, 11)).await);
    }
    send(
        wtx,
        0,
        ConnectionTune {
            channel_max: cfg.channel_max,
            frame_max: cfg.frame_max,
            heartbeat: cfg.heartbeat,
        },
    )
    .await;
    let Method::ConnectionTuneOk(tune_ok) = expect_method(reader, inbuf).await? else {
        return Err(HandshakeError::Unexpected("expected connection.tune-ok"));
    };
    let frame_max = match tune_ok.frame_max {
        0 => cfg.frame_max,
        n => n.min(cfg.frame_max),
    };
    if frame_max < codec::FRAME_MIN_SIZE {
        let text = format!("NOT_ALLOWED - frame_max {frame_max} is below the minimum of 4096");
        return Err(refuse(wtx, reader, inbuf, 530, text, (10, 31)).await);
    }
    let channel_max = match tune_ok.channel_max {
        0 => cfg.channel_max,
        n => n.min(cfg.channel_max),
    };
    let Method::ConnectionOpen(open) = expect_method(reader, inbuf).await? else {
        return Err(HandshakeError::Unexpected("expected connection.open"));
    };
    let Some(vhost) = broker.vhost(&open.virtual_host) else {
        let text = format!("NOT_ALLOWED - vhost '{}' not found", open.virtual_host);
        return Err(refuse(wtx, reader, inbuf, 530, text, (10, 40)).await);
    };
    if !user.can_access_vhost(&vhost.name) {
        let text = format!(
            "NOT_ALLOWED - access to vhost '{}' refused for user '{}'",
            vhost.name, user.name
        );
        return Err(refuse(wtx, reader, inbuf, 530, text, (10, 40)).await);
    }
    send(
        wtx,
        0,
        ConnectionOpenOk {
            known_hosts: String::new(),
        },
    )
    .await;
    Ok(Session {
        user,
        vhost,
        frame_max,
        channel_max,
        heartbeat: tune_ok.heartbeat,
        client_properties: start_ok.client_properties,
    })
}

pub(crate) async fn serve<S>(
    broker: Broker,
    cfg: Arc<AmqpConfig>,
    stream: S,
    peer: SocketAddr,
    local: SocketAddr,
    tls: bool,
) where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (mut reader, writer) = tokio::io::split(stream);
    let (wtx, wrx) = mpsc::channel::<Bytes>(64);
    let writer_task = tokio::spawn(write_loop(writer, wrx));
    let mut inbuf = BytesMut::with_capacity(16 * 1024);
    let hs = timeout(
        cfg.handshake_timeout,
        handshake(&broker, &cfg, &mut reader, &mut inbuf, &wtx, peer),
    )
    .await;
    match hs {
        Ok(Ok(session)) => {
            let conn = Connection::new(broker, cfg, session, reader, inbuf, wtx, peer, local, tls);
            conn.run().await;
        }
        Ok(Err(e)) => {
            tracing::debug!(%peer, error = %e, "AMQP handshake failed");
            drop(wtx);
        }
        Err(_) => {
            tracing::debug!(%peer, "AMQP handshake timed out");
            drop(wtx);
        }
    }
    if timeout(Duration::from_secs(5), writer_task).await.is_err() {
        tracing::debug!(%peer, "writer did not finish in time");
    }
}

struct ConsumerRef {
    channel: u16,
    tag: String,
    queue: QueueHandle,
}

#[derive(Default)]
struct ClientCaps {
    consumer_cancel_notify: bool,
    connection_blocked: bool,
}

struct Connection<R> {
    broker: Broker,
    cfg: Arc<AmqpConfig>,
    id: u64,
    user: Arc<User>,
    vhost: Arc<VHost>,
    info: Arc<ConnectionInfo>,
    _registration: ConnectionRegistration,
    reader: ReadHalf<R>,
    inbuf: BytesMut,
    out: BytesMut,
    writer: mpsc::Sender<Bytes>,
    frame_max: u32,
    channel_max: u16,
    heartbeat: u16,
    channels: HashMap<u16, Channel>,
    consumers: HashMap<u64, ConsumerRef>,
    deliveries_tx: mpsc::Sender<Delivery>,
    deliveries_rx: mpsc::Receiver<Delivery>,
    events_tx: mpsc::UnboundedSender<ConnEvent>,
    events_rx: mpsc::UnboundedReceiver<ConnEvent>,
    caps: ClientCaps,
    last_recv: Instant,
    last_send: Instant,
    closing_until: Option<Instant>,
    client_closed: bool,
    blocked: bool,
    has_published: bool,
    done: bool,
}

impl<R: AsyncRead + Send + 'static> Connection<R> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        broker: Broker,
        cfg: Arc<AmqpConfig>,
        session: Session,
        reader: ReadHalf<R>,
        inbuf: BytesMut,
        writer: mpsc::Sender<Bytes>,
        peer: SocketAddr,
        local: SocketAddr,
        tls: bool,
    ) -> Self {
        let id = broker.next_id();
        let (deliveries_tx, deliveries_rx) = mpsc::channel(cfg.delivery_buffer.max(1));
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let caps_table = session
            .client_properties
            .get("capabilities")
            .and_then(Value::as_table)
            .cloned()
            .unwrap_or_default();
        let caps = ClientCaps {
            consumer_cancel_notify: caps_table.get_bool("consumer_cancel_notify") == Some(true),
            connection_blocked: caps_table.get_bool("connection.blocked") == Some(true),
        };
        let (info, registration) = broker.register_connection(ConnectionInfo {
            id,
            name: format!("{peer} -> {local}"),
            protocol: "AMQP 0-9-1".into(),
            peer: peer.to_string(),
            local: local.to_string(),
            user: session.user.name.clone(),
            vhost: session.vhost.name.clone(),
            tls,
            connected_at: now_ms(),
            client_properties: json::table_to_json(&session.client_properties),
            events: events_tx.clone(),
            channels: Default::default(),
            recv_bytes: Default::default(),
            send_bytes: Default::default(),
            published: Default::default(),
            delivered: Default::default(),
        });
        tracing::debug!(conn = id, %peer, user = %session.user.name, vhost = %session.vhost.name, "AMQP connection opened");
        Connection {
            broker,
            cfg,
            id,
            user: session.user,
            vhost: session.vhost,
            info,
            _registration: registration,
            reader,
            inbuf,
            out: BytesMut::with_capacity(16 * 1024),
            writer,
            frame_max: session.frame_max,
            channel_max: session.channel_max,
            heartbeat: session.heartbeat,
            channels: HashMap::new(),
            consumers: HashMap::new(),
            deliveries_tx,
            deliveries_rx,
            events_tx,
            events_rx,
            caps,
            last_recv: Instant::now(),
            last_send: Instant::now(),
            closing_until: None,
            client_closed: false,
            blocked: false,
            has_published: false,
            done: false,
        }
    }

    async fn run(mut self) {
        let tick = Duration::from_secs((self.heartbeat as u64 / 2).max(1));
        let mut heartbeat = tokio::time::interval(tick);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        while !self.done {
            let read_paused = self.blocked && self.has_published;
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
                n = self.reader.read_buf(&mut self.inbuf), if !read_paused => {
                    match n {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            self.info.recv_bytes.fetch_add(n as u64, Ordering::Relaxed);
                            self.last_recv = Instant::now();
                            self.process_input().await;
                        }
                    }
                }
                Some(ev) = self.events_rx.recv() => self.on_event(ev).await,
                Some(d) = self.deliveries_rx.recv(), if self.out.len() < OUT_HIGH && self.closing_until.is_none() => {
                    self.on_delivery(d).await;
                    while self.out.len() < OUT_HIGH {
                        match self.deliveries_rx.try_recv() {
                            Ok(d) => self.on_delivery(d).await,
                            Err(_) => break,
                        }
                    }
                }
                _ = heartbeat.tick() => self.on_tick(tick),
                _ = tokio::time::sleep_until(self.closing_until.unwrap_or_else(Instant::now)), if self.closing_until.is_some() => {
                    self.done = true;
                }
            }
        }
        self.flush_out().await;
        self.cleanup().await;
        tracing::debug!(conn = self.id, "AMQP connection closed");
    }

    async fn flush_out(&mut self) {
        if !self.out.is_empty() {
            let chunk = self.out.split().freeze();
            let _ = timeout(Duration::from_secs(1), self.writer.send(chunk)).await;
        }
    }

    fn on_tick(&mut self, tick: Duration) {
        if let Some(until) = self.closing_until {
            if Instant::now() >= until {
                self.done = true;
            }
            return;
        }
        if self.heartbeat == 0 {
            return;
        }
        if self.last_send.elapsed() >= tick {
            codec::encode_heartbeat(&mut self.out);
        }
        let limit = Duration::from_secs(self.heartbeat as u64 * 2);
        if self.last_recv.elapsed() > limit {
            tracing::info!(
                conn = self.id,
                "missed heartbeats from client, timeout: {}s",
                self.heartbeat
            );
            self.done = true;
        }
    }

    async fn process_input(&mut self) {
        let max = if self.frame_max == 0 {
            u32::MAX
        } else {
            self.frame_max
        };
        loop {
            if self.done {
                return;
            }
            match codec::decode_frame(&mut self.inbuf, max) {
                Ok(Some(frame)) => self.on_frame(frame).await,
                Ok(None) => return,
                Err(e) => {
                    let code = match e {
                        CodecError::UnknownMethod(..) => reply_code::COMMAND_INVALID,
                        CodecError::InvalidUtf8 | CodecError::UnknownFieldType(_) => {
                            reply_code::SYNTAX_ERROR
                        }
                        _ => reply_code::FRAME_ERROR,
                    };
                    let name = match code {
                        reply_code::COMMAND_INVALID => "COMMAND_INVALID",
                        reply_code::SYNTAX_ERROR => "SYNTAX_ERROR",
                        _ => "FRAME_ERROR",
                    };
                    self.close_connection(BrokerError::new(code, format!("{name} - {e}")), 0, 0);
                    self.inbuf.clear();
                    return;
                }
            }
        }
    }

    fn send(&mut self, channel: u16, m: impl Into<Method>) {
        codec::encode_method(&mut self.out, channel, &m.into());
    }

    fn close_connection(&mut self, e: BrokerError, class_id: u16, method_id: u16) {
        if self.closing_until.is_some() {
            return;
        }
        tracing::info!(conn = self.id, code = e.code, reason = %e.text, "closing AMQP connection");
        self.send(
            0,
            ConnectionClose {
                reply_code: e.code,
                reply_text: e.text,
                class_id,
                method_id,
            },
        );
        self.closing_until = Some(Instant::now() + Duration::from_secs(3));
    }

    async fn close_channel(&mut self, ch: u16, e: BrokerError, class_id: u16, method_id: u16) {
        tracing::debug!(conn = self.id, channel = ch, code = e.code, reason = %e.text, "closing channel");
        self.send(
            ch,
            ChannelClose {
                reply_code: e.code,
                reply_text: e.text,
                class_id,
                method_id,
            },
        );
        self.release_channel(ch).await;
        if let Some(c) = self.channels.get_mut(&ch) {
            c.closing = true;
        }
    }

    async fn fail(&mut self, ch: u16, e: BrokerError, class_id: u16, method_id: u16) {
        if e.is_hard() || ch == 0 {
            self.close_connection(e, class_id, method_id);
        } else {
            self.close_channel(ch, e, class_id, method_id).await;
        }
    }

    async fn on_frame(&mut self, frame: Frame) {
        if self.closing_until.is_some() {
            // After connection.close only close-ok (or a crossing close) matters.
            if !self.client_closed {
                match frame {
                    Frame::Method {
                        method: Method::ConnectionCloseOk(_),
                        ..
                    } => self.done = true,
                    Frame::Method {
                        method: Method::ConnectionClose(_),
                        ..
                    } => {
                        self.send(0, ConnectionCloseOk {});
                        self.done = true;
                    }
                    _ => {}
                }
            }
            return;
        }
        match frame {
            Frame::Heartbeat => {}
            Frame::Method { channel: 0, method } => self.on_connection_method(method).await,
            Frame::Method { channel, method } => self.on_channel_method(channel, method).await,
            Frame::Header {
                channel,
                body_size,
                properties,
                ..
            } => self.on_header(channel, body_size, properties).await,
            Frame::Body { channel, payload } => self.on_body(channel, payload).await,
        }
    }

    async fn on_connection_method(&mut self, method: Method) {
        match method {
            Method::ConnectionClose(_) => {
                // The peer that receives close-ok closes the socket. Wait for the client to
                // do so, with a deadline, instead of racing it.
                self.send(0, ConnectionCloseOk {});
                self.client_closed = true;
                self.closing_until = Some(Instant::now() + Duration::from_secs(3));
            }
            Method::ConnectionCloseOk(_) => self.done = true,
            Method::ConnectionUpdateSecret(_) => self.send(0, ConnectionUpdateSecretOk {}),
            Method::ConnectionBlocked(_) | Method::ConnectionUnblocked(_) => {}
            other => {
                let (c, m) = other.id();
                self.close_connection(
                    BrokerError::new(
                        reply_code::COMMAND_INVALID,
                        format!(
                            "COMMAND_INVALID - unexpected method {} on channel 0",
                            other.name()
                        ),
                    ),
                    c,
                    m,
                );
            }
        }
    }

    async fn on_channel_method(&mut self, ch: u16, method: Method) {
        let (class_id, method_id) = method.id();
        if let Method::ChannelOpen(_) = method {
            if ch > self.channel_max {
                self.close_connection(
                    BrokerError::not_allowed(format!(
                        "channel {ch} exceeds channel_max {}",
                        self.channel_max
                    )),
                    class_id,
                    method_id,
                );
                return;
            }
            if self.channels.get(&ch).is_some_and(|c| !c.closing) {
                self.close_connection(
                    BrokerError::new(
                        reply_code::CHANNEL_ERROR,
                        format!("CHANNEL_ERROR - second 'channel.open' seen on channel {ch}"),
                    ),
                    class_id,
                    method_id,
                );
                return;
            }
            // Reopening a channel the broker closed is fine even before its close-ok arrives.
            if self.channels.insert(ch, Channel::new()).is_none() {
                self.info.channels.fetch_add(1, Ordering::Relaxed);
            }
            self.send(
                ch,
                ChannelOpenOk {
                    channel_id: LongString::default(),
                },
            );
            return;
        }
        let Some(chan) = self.channels.get(&ch) else {
            if let Method::ChannelCloseOk(_) = method {
                // A late close-ok for a channel that is already gone.
                return;
            }
            self.close_connection(
                BrokerError::new(
                    reply_code::CHANNEL_ERROR,
                    format!("CHANNEL_ERROR - expected 'channel.open' on channel {ch}"),
                ),
                class_id,
                method_id,
            );
            return;
        };
        if chan.closing {
            match method {
                Method::ChannelCloseOk(_) => self.remove_channel(ch),
                // Both sides closed at once: answer, and keep waiting for our close-ok.
                Method::ChannelClose(_) => self.send(ch, ChannelCloseOk {}),
                _ => {}
            }
            return;
        }
        if chan.pending.is_some() {
            self.close_connection(
                BrokerError::new(
                    reply_code::UNEXPECTED_FRAME,
                    format!(
                        "UNEXPECTED_FRAME - expected content header for class 60, got non content header frame instead ({})",
                        method.name()
                    ),
                ),
                class_id,
                method_id,
            );
            return;
        }
        if let Err(e) = self.dispatch(ch, method).await {
            self.fail(ch, e, class_id, method_id).await;
        }
    }

    fn remove_channel(&mut self, ch: u16) {
        if self.channels.remove(&ch).is_some() {
            self.info.channels.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn chan(&mut self, ch: u16) -> &mut Channel {
        self.channels
            .get_mut(&ch)
            .expect("channel checked by caller")
    }

    fn check(&self, access: Access, kind: &str, name: &str) -> Res {
        self.user.check(&self.vhost.name, access, kind, name)
    }

    fn queue_name(&mut self, ch: u16, name: String) -> Res<String> {
        if !name.is_empty() {
            return Ok(name);
        }
        let last = self.chan(ch).last_queue.clone();
        if last.is_empty() {
            return Err(BrokerError::not_found("no previously declared queue"));
        }
        Ok(last)
    }

    async fn dispatch(&mut self, ch: u16, method: Method) -> Res {
        let conn = Some(self.id);
        match method {
            Method::ChannelClose(_) => {
                self.release_channel(ch).await;
                self.send(ch, ChannelCloseOk {});
                self.remove_channel(ch);
            }
            Method::ChannelCloseOk(_) => {}
            Method::ChannelFlow(m) => {
                if !m.active {
                    // RabbitMQ does not support pausing deliveries with channel.flow either.
                    return Err(BrokerError::not_implemented("active=false"));
                }
                self.send(ch, ChannelFlowOk { active: true });
            }
            Method::ExchangeDeclare(m) => {
                if !m.passive {
                    self.check(Access::Configure, "exchange", &m.exchange)?;
                }
                self.vhost.declare_exchange(ExchangeOptions {
                    name: m.exchange,
                    kind: m.kind,
                    durable: m.durable,
                    auto_delete: m.auto_delete,
                    internal: m.internal,
                    passive: m.passive,
                    arguments: m.arguments,
                })?;
                if !m.nowait {
                    self.send(ch, ExchangeDeclareOk {});
                }
            }
            Method::ExchangeDelete(m) => {
                self.check(Access::Configure, "exchange", &m.exchange)?;
                self.vhost.delete_exchange(&m.exchange, m.if_unused)?;
                if !m.nowait {
                    self.send(ch, ExchangeDeleteOk {});
                }
            }
            Method::ExchangeBind(m) => {
                self.check(Access::Write, "exchange", &m.destination)?;
                self.check(Access::Read, "exchange", &m.source)?;
                self.vhost.bind(
                    &m.source,
                    Destination::Exchange(m.destination),
                    &m.routing_key,
                    m.arguments,
                    conn,
                )?;
                if !m.nowait {
                    self.send(ch, ExchangeBindOk {});
                }
            }
            Method::ExchangeUnbind(m) => {
                self.check(Access::Write, "exchange", &m.destination)?;
                self.check(Access::Read, "exchange", &m.source)?;
                self.vhost.unbind(
                    &m.source,
                    Destination::Exchange(m.destination),
                    &m.routing_key,
                    &m.arguments,
                    conn,
                )?;
                if !m.nowait {
                    self.send(ch, ExchangeUnbindOk {});
                }
            }
            Method::QueueDeclare(m) => {
                if !m.passive && !m.queue.is_empty() {
                    self.check(Access::Configure, "queue", &m.queue)?;
                }
                let d = self.vhost.declare_queue(
                    QueueOptions {
                        name: m.queue,
                        durable: m.durable,
                        exclusive: m.exclusive,
                        auto_delete: m.auto_delete,
                        passive: m.passive,
                        arguments: m.arguments,
                    },
                    conn,
                )?;
                let name = d.queue.name.clone();
                self.chan(ch).last_queue = name.clone();
                if !m.nowait {
                    self.send(
                        ch,
                        QueueDeclareOk {
                            queue: name,
                            message_count: d.message_count as u32,
                            consumer_count: d.consumer_count,
                        },
                    );
                }
            }
            Method::QueueBind(m) => {
                let queue = self.queue_name(ch, m.queue)?;
                self.check(Access::Write, "queue", &queue)?;
                self.check(Access::Read, "exchange", &m.exchange)?;
                self.vhost.bind(
                    &m.exchange,
                    Destination::Queue(queue),
                    &m.routing_key,
                    m.arguments,
                    conn,
                )?;
                if !m.nowait {
                    self.send(ch, QueueBindOk {});
                }
            }
            Method::QueueUnbind(m) => {
                let queue = self.queue_name(ch, m.queue)?;
                self.check(Access::Write, "queue", &queue)?;
                self.check(Access::Read, "exchange", &m.exchange)?;
                self.vhost.unbind(
                    &m.exchange,
                    Destination::Queue(queue),
                    &m.routing_key,
                    &m.arguments,
                    conn,
                )?;
                self.send(ch, QueueUnbindOk {});
            }
            Method::QueuePurge(m) => {
                let queue = self.queue_name(ch, m.queue)?;
                self.check(Access::Read, "queue", &queue)?;
                let n = self.vhost.purge_queue(&queue, conn).await?;
                if !m.nowait {
                    self.send(
                        ch,
                        QueuePurgeOk {
                            message_count: n as u32,
                        },
                    );
                }
            }
            Method::QueueDelete(m) => {
                let queue = self.queue_name(ch, m.queue)?;
                self.check(Access::Configure, "queue", &queue)?;
                let n = self
                    .vhost
                    .delete_queue(&queue, m.if_unused, m.if_empty, conn)
                    .await?;
                if !m.nowait {
                    self.send(
                        ch,
                        QueueDeleteOk {
                            message_count: n as u32,
                        },
                    );
                }
            }
            Method::BasicQos(m) => {
                if m.prefetch_size != 0 {
                    return Err(BrokerError::not_implemented(format!(
                        "prefetch_size!=0 ({})",
                        m.prefetch_size
                    )));
                }
                let c = self.chan(ch);
                if m.global {
                    c.global_limit =
                        (m.prefetch_count > 0).then(|| SharedLimit::new(m.prefetch_count as u32));
                } else {
                    c.prefetch = m.prefetch_count;
                }
                self.send(ch, BasicQosOk {});
            }
            Method::BasicConsume(m) => self.basic_consume(ch, m).await?,
            Method::BasicCancel(m) => {
                if let Some(id) = self.chan(ch).consumers.remove(&m.consumer_tag)
                    && let Some(c) = self.consumers.remove(&id)
                {
                    c.queue.cancel(id).await;
                }
                if !m.nowait {
                    self.send(
                        ch,
                        BasicCancelOk {
                            consumer_tag: m.consumer_tag,
                        },
                    );
                }
            }
            Method::BasicPublish(m) => {
                if m.immediate {
                    return Err(BrokerError::not_implemented("immediate=true"));
                }
                self.chan(ch).pending = Some(PendingContent::new(m));
            }
            Method::BasicGet(m) => self.basic_get(ch, m).await?,
            Method::BasicAck(m) => {
                let items = self.take(ch, m.delivery_tag, m.multiple)?;
                let owner = self.owner(ch);
                if let Some(tx) = &mut self.chan(ch).tx {
                    tx.push(TxOp::Ack(items));
                } else {
                    for (q, seqs) in group_by_queue(items) {
                        q.ack(owner, seqs).await;
                    }
                }
            }
            Method::BasicReject(m) => {
                let items = self.take(ch, m.delivery_tag, false)?;
                self.reject(ch, items, m.requeue).await;
            }
            Method::BasicNack(m) => {
                let items = self.take(ch, m.delivery_tag, m.multiple)?;
                self.reject(ch, items, m.requeue).await;
            }
            Method::BasicRecover(_) | Method::BasicRecoverAsync(_) => {
                let is_sync = matches!(method, Method::BasicRecover(_));
                let items = self.take(ch, 0, true)?;
                let owner = self.owner(ch);
                for (q, seqs) in group_by_queue(items) {
                    q.reject(owner, seqs, true).await;
                }
                if is_sync {
                    self.send(ch, BasicRecoverOk {});
                }
            }
            Method::ConfirmSelect(m) => {
                let c = self.chan(ch);
                if c.tx.is_some() {
                    return Err(BrokerError::precondition_failed(
                        "cannot switch from tx to confirm mode",
                    ));
                }
                c.confirm.get_or_insert_with(Default::default);
                if !m.nowait {
                    self.send(ch, ConfirmSelectOk {});
                }
            }
            Method::TxSelect(_) => {
                let c = self.chan(ch);
                if c.confirm.is_some() {
                    return Err(BrokerError::precondition_failed(
                        "cannot switch from confirm to tx mode",
                    ));
                }
                c.tx.get_or_insert_with(Vec::new);
                self.send(ch, TxSelectOk {});
            }
            Method::TxCommit(_) => {
                let ops = self
                    .chan(ch)
                    .tx
                    .as_mut()
                    .map(std::mem::take)
                    .ok_or_else(|| {
                        BrokerError::precondition_failed("channel is not transactional")
                    })?;
                let owner = self.owner(ch);
                for op in ops {
                    match op {
                        TxOp::Publish {
                            method,
                            properties,
                            body,
                        } => self.do_publish(ch, method, properties, body).await?,
                        TxOp::Ack(items) => {
                            for (q, seqs) in group_by_queue(items) {
                                q.ack(owner, seqs).await;
                            }
                        }
                        TxOp::Reject(items, requeue) => {
                            for (q, seqs) in group_by_queue(items) {
                                q.reject(owner, seqs, requeue).await;
                            }
                        }
                    }
                }
                self.send(ch, TxCommitOk {});
            }
            Method::TxRollback(_) => {
                let ops = self
                    .chan(ch)
                    .tx
                    .as_mut()
                    .map(std::mem::take)
                    .ok_or_else(|| {
                        BrokerError::precondition_failed("channel is not transactional")
                    })?;
                let c = self.chan(ch);
                for op in ops {
                    match op {
                        TxOp::Ack(items) | TxOp::Reject(items, _) => c.restore_unacked(items),
                        TxOp::Publish { .. } => {}
                    }
                }
                self.send(ch, TxRollbackOk {});
            }
            other => {
                return Err(BrokerError::command_invalid(format!(
                    "unexpected method {} from client",
                    other.name()
                )));
            }
        }
        Ok(())
    }

    fn owner(&self, ch: u16) -> Owner {
        Owner {
            conn: self.id,
            channel: ch,
        }
    }

    fn take(&mut self, ch: u16, tag: u64, multiple: bool) -> Res<Vec<Taken>> {
        self.chan(ch)
            .take_unacked(tag, multiple)
            .ok_or_else(|| BrokerError::precondition_failed(format!("unknown delivery tag {tag}")))
    }

    async fn reject(&mut self, ch: u16, items: Vec<Taken>, requeue: bool) {
        let owner = self.owner(ch);
        if let Some(tx) = &mut self.chan(ch).tx {
            tx.push(TxOp::Reject(items, requeue));
            return;
        }
        for (q, seqs) in group_by_queue(items) {
            q.reject(owner, seqs, requeue).await;
        }
    }

    async fn basic_consume(&mut self, ch: u16, m: BasicConsume) -> Res {
        let queue_name = self.queue_name(ch, m.queue)?;
        self.check(Access::Read, "queue", &queue_name)?;
        let q = self.vhost.queue(&queue_name, Some(self.id))?;
        let tag = if m.consumer_tag.is_empty() {
            random_tag("amq.ctag-")
        } else {
            m.consumer_tag
        };
        let (prefetch, shared_limit) = {
            let c = self.chan(ch);
            if c.consumers.contains_key(&tag) {
                return Err(BrokerError::not_allowed(format!(
                    "attempt to reuse consumer tag '{tag}'"
                )));
            }
            (c.prefetch, c.global_limit.clone())
        };
        let id = self.broker.next_id();
        q.consume(ConsumerSpec {
            id,
            tag: tag.clone(),
            owner: self.owner(ch),
            no_ack: m.no_ack,
            exclusive: m.exclusive,
            limit: Limit::Prefetch(prefetch as u32),
            shared_limit,
            arguments: m.arguments,
            deliveries: self.deliveries_tx.clone(),
            events: self.events_tx.clone(),
        })
        .await?;
        self.chan(ch).consumers.insert(tag.clone(), id);
        self.consumers.insert(
            id,
            ConsumerRef {
                channel: ch,
                tag: tag.clone(),
                queue: q,
            },
        );
        if !m.nowait {
            self.send(ch, BasicConsumeOk { consumer_tag: tag });
        }
        Ok(())
    }

    async fn basic_get(&mut self, ch: u16, m: BasicGet) -> Res {
        let queue_name = self.queue_name(ch, m.queue)?;
        self.check(Access::Read, "queue", &queue_name)?;
        let q = self.vhost.queue(&queue_name, Some(self.id))?;
        match q.get(self.owner(ch), m.no_ack).await? {
            Some(g) => {
                let c = self.chan(ch);
                let tag = c.take_tag();
                if !m.no_ack {
                    c.unacked.insert(tag, (q, g.seq));
                }
                let method: Method = BasicGetOk {
                    delivery_tag: tag,
                    redelivered: g.redelivered,
                    exchange: g.message.exchange.clone(),
                    routing_key: g.message.routing_key.clone(),
                    message_count: g.remaining,
                }
                .into();
                codec::encode_content(
                    &mut self.out,
                    ch,
                    &method,
                    &g.message.properties,
                    &g.message.body,
                    self.frame_max,
                );
                self.info.delivered.fetch_add(1, Ordering::Relaxed);
            }
            None => self.send(
                ch,
                BasicGetEmpty {
                    cluster_id: String::new(),
                },
            ),
        }
        Ok(())
    }

    async fn on_header(&mut self, ch: u16, body_size: u64, properties: BasicProperties) {
        let Some(c) = self.channels.get_mut(&ch) else {
            self.close_connection(
                BrokerError::new(
                    reply_code::CHANNEL_ERROR,
                    format!("CHANNEL_ERROR - expected 'channel.open' on channel {ch}"),
                ),
                0,
                0,
            );
            return;
        };
        if c.closing {
            return;
        }
        let max = self.cfg.max_message_size;
        let Some(p) = c.pending.as_mut().filter(|p| p.properties.is_none()) else {
            self.close_connection(
                BrokerError::new(
                    reply_code::UNEXPECTED_FRAME,
                    "UNEXPECTED_FRAME - content header without a preceding basic.publish",
                ),
                60,
                40,
            );
            return;
        };
        if body_size > max {
            c.pending = None;
            self.close_channel(
                ch,
                BrokerError::precondition_failed(format!(
                    "message size {body_size} is larger than configured max size {max}"
                )),
                60,
                40,
            )
            .await;
            return;
        }
        p.properties = Some(properties);
        p.body_size = body_size;
        if body_size == 0 {
            self.finish_publish(ch).await;
        }
    }

    async fn on_body(&mut self, ch: u16, payload: Bytes) {
        let Some(c) = self.channels.get_mut(&ch) else {
            return;
        };
        if c.closing {
            return;
        }
        let Some(p) = c.pending.as_mut().filter(|p| p.properties.is_some()) else {
            self.close_connection(
                BrokerError::new(
                    reply_code::UNEXPECTED_FRAME,
                    "UNEXPECTED_FRAME - body frame without a content header",
                ),
                60,
                40,
            );
            return;
        };
        p.received += payload.len() as u64;
        p.chunks.push(payload);
        if p.received > p.body_size {
            self.close_connection(
                BrokerError::new(
                    reply_code::FRAME_ERROR,
                    "FRAME_ERROR - body is larger than the size in the content header",
                ),
                60,
                40,
            );
        } else if p.received == p.body_size {
            self.finish_publish(ch).await;
        }
    }

    async fn finish_publish(&mut self, ch: u16) {
        let Some(mut p) = self.chan(ch).pending.take() else {
            return;
        };
        let body = p.body();
        let properties = p.properties.take().unwrap_or_default();
        if let Err(e) = self.publish(ch, p.method, properties, body).await {
            self.fail(ch, e, 60, 40).await;
        }
    }

    async fn publish(
        &mut self,
        ch: u16,
        method: BasicPublish,
        properties: BasicProperties,
        body: Bytes,
    ) -> Res {
        self.check(Access::Write, "exchange", &method.exchange)?;
        if let Some(uid) = &properties.user_id
            && *uid != self.user.name
        {
            return Err(BrokerError::precondition_failed(format!(
                "user_id property set to '{uid}' but authenticated user was '{}'",
                self.user.name
            )));
        }
        if self.chan(ch).tx.is_some() {
            // Fail early on a missing exchange, as RabbitMQ does.
            self.vhost
                .route(&method.exchange, &method.routing_key, None)?;
            if let Some(tx) = &mut self.chan(ch).tx {
                tx.push(TxOp::Publish {
                    method,
                    properties,
                    body,
                });
            }
            return Ok(());
        }
        self.do_publish(ch, method, properties, body).await
    }

    async fn do_publish(
        &mut self,
        ch: u16,
        method: BasicPublish,
        properties: BasicProperties,
        body: Bytes,
    ) -> Res {
        let queues = self.vhost.route(
            &method.exchange,
            &method.routing_key,
            properties.headers.as_ref(),
        )?;
        self.has_published = true;
        self.info.published.fetch_add(1, Ordering::Relaxed);
        let events = self.events_tx.clone();
        let ticket = self
            .chan(ch)
            .confirm
            .as_mut()
            .map(|t| Ticket::new(events, ch, t.issue()));
        if queues.is_empty() {
            if method.mandatory {
                let ret: Method = BasicReturn {
                    reply_code: reply_code::NO_ROUTE,
                    reply_text: "NO_ROUTE".into(),
                    exchange: method.exchange,
                    routing_key: method.routing_key,
                }
                .into();
                codec::encode_content(&mut self.out, ch, &ret, &properties, &body, self.frame_max);
            }
            return Ok(());
        }
        let msg = Arc::new(Message::new(
            method.exchange,
            method.routing_key,
            properties,
            body,
        ));
        for q in &queues {
            q.publish(msg.clone(), ticket.clone()).await;
        }
        Ok(())
    }

    async fn on_delivery(&mut self, d: Delivery) {
        let Some(c) = self.consumers.get(&d.consumer_id) else {
            // The consumer was cancelled while this delivery was in flight.
            if !d.no_ack {
                d.queue.reject(d.owner, vec![d.seq], true).await;
            }
            return;
        };
        let (ch, tag) = (c.channel, c.tag.clone());
        let Some(chan) = self.channels.get_mut(&ch).filter(|c| !c.closing) else {
            return;
        };
        let delivery_tag = chan.take_tag();
        if !d.no_ack {
            chan.unacked.insert(delivery_tag, (d.queue.clone(), d.seq));
        }
        let method: Method = BasicDeliver {
            consumer_tag: tag,
            delivery_tag,
            redelivered: d.redelivered,
            exchange: d.message.exchange.clone(),
            routing_key: d.message.routing_key.clone(),
        }
        .into();
        codec::encode_content(
            &mut self.out,
            ch,
            &method,
            &d.message.properties,
            &d.message.body,
            self.frame_max,
        );
        self.info.delivered.fetch_add(1, Ordering::Relaxed);
    }

    async fn on_event(&mut self, ev: ConnEvent) {
        match ev {
            ConnEvent::Confirm { channel, seq, ack } => {
                let Some(c) = self.channels.get_mut(&channel) else {
                    return;
                };
                let Some(tracker) = c.confirm.as_mut() else {
                    return;
                };
                let result = tracker.complete(seq, ack);
                if c.closing {
                    return;
                }
                if !ack {
                    self.send(
                        channel,
                        BasicNack {
                            delivery_tag: seq,
                            multiple: false,
                            requeue: false,
                        },
                    );
                }
                if let Some((tag, multiple)) = result {
                    self.send(
                        channel,
                        BasicAck {
                            delivery_tag: tag,
                            multiple,
                        },
                    );
                }
            }
            ConnEvent::ConsumerCancelled { consumer_id, .. } => {
                let Some(c) = self.consumers.remove(&consumer_id) else {
                    return;
                };
                let notify = self.caps.consumer_cancel_notify;
                if let Some(chan) = self.channels.get_mut(&c.channel) {
                    chan.consumers.remove(&c.tag);
                    if notify && !chan.closing {
                        self.send(
                            c.channel,
                            BasicCancel {
                                consumer_tag: c.tag,
                                nowait: true,
                            },
                        );
                    }
                }
            }
            ConnEvent::Blocked(blocked) => {
                if blocked != self.blocked && self.caps.connection_blocked {
                    if blocked {
                        self.send(
                            0,
                            ConnectionBlocked {
                                reason: "low on resources".into(),
                            },
                        );
                    } else {
                        self.send(0, ConnectionUnblocked {});
                    }
                }
                self.blocked = blocked;
            }
            ConnEvent::Close { code, reason } => {
                self.close_connection(BrokerError::new(code, reason), 0, 0)
            }
            ConnEvent::Wake => {
                for c in self.consumers.values() {
                    c.queue.wake();
                }
            }
        }
    }

    /// Releases a channel's consumers and unacknowledged messages back to their queues.
    async fn release_channel(&mut self, ch: u16) {
        let Some(c) = self.channels.get_mut(&ch) else {
            return;
        };
        let unacked = std::mem::take(&mut c.unacked);
        let consumer_ids: Vec<u64> = c.consumers.drain().map(|(_, id)| id).collect();
        c.pending = None;
        c.confirm = None;
        c.tx = None;
        let mut queues: Vec<QueueHandle> = Vec::new();
        for (q, _) in unacked.into_values() {
            if !queues.contains(&q) {
                queues.push(q);
            }
        }
        for id in consumer_ids {
            if let Some(cr) = self.consumers.remove(&id)
                && !queues.contains(&cr.queue)
            {
                queues.push(cr.queue);
            }
        }
        for q in queues {
            q.owner_closed(self.id, Some(ch)).await;
        }
    }

    async fn cleanup(&mut self) {
        let mut queues: Vec<QueueHandle> = Vec::new();
        for c in self.channels.values_mut() {
            for (q, _) in std::mem::take(&mut c.unacked).into_values() {
                if !queues.contains(&q) {
                    queues.push(q);
                }
            }
        }
        for (_, c) in self.consumers.drain() {
            if !queues.contains(&c.queue) {
                queues.push(c.queue);
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
        self.channels.clear();
    }
}

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use firemq_proto::{
    self as proto, Auth, Bind, Command, DeclareExchange, DeclareQueue, DeleteExchange, DeleteQueue,
    Get, Hello, Map, OkReply, PurgeQueue, Subscribe, Transport, Value, Welcome,
};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, timeout};

use crate::message::{Acker, Credit, Message, Subscription};
use crate::tls::IoStream;

type Reader = ReadHalf<Box<dyn IoStream>>;
type Writer = WriteHalf<Box<dyn IoStream>>;
use crate::options::*;
use crate::{Error, Result};

pub(crate) enum Reply {
    Ok(OkReply),
    Get(Box<proto::GetOk>, Bytes),
}

/// Requests and confirms waiting on one connection.
#[derive(Default)]
struct Pending {
    requests: HashMap<u32, oneshot::Sender<Result<Reply>>>,
    confirms: BTreeMap<u64, oneshot::Sender<Result<()>>>,
    returned: HashMap<u64, (String, String)>,
    closed: Option<Error>,
}

impl Pending {
    /// Fails everything waiting with [`Error::Closed`], so callers can tell a lost
    /// connection from a refused request.
    fn fail_all(&mut self, reason: &Error) {
        if self.closed.is_some() {
            return;
        }
        let e = match reason {
            Error::Closed(_) => reason.clone(),
            other => Error::Closed(other.to_string()),
        };
        for (_, tx) in self.requests.drain() {
            let _ = tx.send(Err(e.clone()));
        }
        for (_, tx) in std::mem::take(&mut self.confirms) {
            let _ = tx.send(Err(e.clone()));
        }
        self.returned.clear();
        self.closed = Some(e);
    }
}

/// One TCP or TLS connection. A client replaces it when it reconnects.
pub(crate) struct Conn {
    pub(crate) generation: u64,
    out: mpsc::UnboundedSender<Bytes>,
    pending: Arc<Mutex<Pending>>,
    rid: AtomicU32,
    seq: AtomicU64,
}

impl Conn {
    pub(crate) fn is_open(&self) -> bool {
        self.pending.lock().closed.is_none()
    }

    pub(crate) fn send(&self, cmd: Command) -> Result<()> {
        if let Some(e) = &self.pending.lock().closed {
            return Err(e.clone());
        }
        self.out
            .send(cmd.to_bytes())
            .map_err(|_| Error::Closed("connection is closed".into()))
    }

    async fn call(&self, build: impl FnOnce(u32) -> Command) -> Result<Reply> {
        let rid = self.rid.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut p = self.pending.lock();
            if let Some(e) = &p.closed {
                return Err(e.clone());
            }
            p.requests.insert(rid, tx);
        }
        self.send(build(rid))?;
        rx.await
            .map_err(|_| Error::Closed("connection closed before the reply".into()))?
    }

    async fn call_ok(&self, build: impl FnOnce(u32) -> Command) -> Result<OkReply> {
        match self.call(build).await? {
            Reply::Ok(ok) => Ok(ok),
            Reply::Get(..) => Err(Error::Protocol("unexpected get_ok".into())),
        }
    }

    /// Sends a publish and returns the receiver for its confirm, if it asked for one.
    fn publish(
        &self,
        exchange: &str,
        key: &str,
        body: Bytes,
        opts: &PublishOptions,
    ) -> Result<Option<oneshot::Receiver<Result<()>>>> {
        let mut rx = None;
        let seq = if opts.confirm {
            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            let (tx, r) = oneshot::channel();
            let mut p = self.pending.lock();
            if let Some(e) = &p.closed {
                return Err(e.clone());
            }
            p.confirms.insert(seq, tx);
            rx = Some(r);
            seq
        } else {
            0
        };
        self.send(Command::Publish(
            proto::Publish {
                seq,
                exchange: exchange.into(),
                key: key.into(),
                mandatory: opts.mandatory,
                persistent: opts.persistent,
                props: opts.properties.clone(),
                headers: opts.headers.clone(),
            },
            body,
        ))?;
        Ok(rx)
    }
}

/// The client's view of its connection.
#[derive(Clone)]
enum Link {
    Up(Arc<Conn>),
    /// Reconnecting.
    Down,
    /// Closed for good, by [`Client::close`] or because reconnecting gave up.
    Closed(Error),
}

#[derive(Debug, Clone)]
struct QueueRecord {
    name: String,
    opts: QueueOptions,
    /// Declared with an empty name. The broker picks a new name when it is declared again.
    server_named: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum Destination {
    Queue(String),
    Exchange(String),
}

#[derive(Debug, Clone)]
struct BindingRecord {
    source: String,
    destination: Destination,
    key: String,
    args: Map,
}

/// The exchanges, queues and bindings this client declared, to declare again after a
/// reconnect.
#[derive(Debug, Clone, Default)]
struct Topology {
    exchanges: Vec<(String, String, ExchangeOptions)>,
    queues: Vec<QueueRecord>,
    bindings: Vec<BindingRecord>,
}

impl Topology {
    fn add_exchange(&mut self, name: &str, kind: &str, opts: &ExchangeOptions) {
        self.exchanges.retain(|(n, ..)| n != name);
        self.exchanges
            .push((name.to_string(), kind.to_string(), opts.clone()));
    }

    fn remove_exchange(&mut self, name: &str) {
        self.exchanges.retain(|(n, ..)| n != name);
        self.bindings.retain(|b| {
            b.source != name && b.destination != Destination::Exchange(name.to_string())
        });
    }

    fn add_queue(&mut self, record: QueueRecord) {
        self.queues.retain(|q| q.name != record.name);
        self.queues.push(record);
    }

    fn remove_queue(&mut self, name: &str) {
        self.queues.retain(|q| q.name != name);
        self.bindings
            .retain(|b| b.destination != Destination::Queue(name.to_string()));
    }

    fn add_binding(&mut self, b: BindingRecord) {
        self.remove_binding(&b.source, &b.destination, &b.key);
        self.bindings.push(b);
    }

    fn remove_binding(&mut self, source: &str, destination: &Destination, key: &str) {
        self.bindings
            .retain(|b| !(b.source == source && &b.destination == destination && b.key == key));
    }

    fn rename_queue(&mut self, old: &str, new: &str) {
        for q in &mut self.queues {
            if q.name == old {
                q.name = new.to_string();
            }
        }
        for b in &mut self.bindings {
            if b.destination == Destination::Queue(old.to_string()) {
                b.destination = Destination::Queue(new.to_string());
            }
        }
    }
}

/// A subscription the client resumes after reconnecting.
pub(crate) struct SubRecord {
    pub(crate) queue: String,
    pub(crate) opts: SubscribeOptions,
    pub(crate) tx: mpsc::UnboundedSender<Message>,
    /// Credit on the connection the subscription is currently on.
    pub(crate) credit: Arc<Credit>,
}

pub(crate) struct Inner {
    opts: ConnectOptions,
    welcome: Welcome,
    link: watch::Sender<Link>,
    topology: Mutex<Topology>,
    pub(crate) subs: Mutex<HashMap<u32, SubRecord>>,
    next_sub: AtomicU32,
    generation: AtomicU64,
}

/// A connection to FireMQ. Cheap to clone; clones share the connection.
///
/// The client reconnects on its own when the connection drops, unless
/// [`ConnectOptions::reconnect`] is `None`.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<Inner>,
}

/// What `declare_queue` reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueInfo {
    pub name: String,
    pub messages: u64,
    pub consumers: u32,
}

/// A connection that completed the handshake, before its tasks start.
struct Handshake {
    r: Reader,
    w: Writer,
    buf: BytesMut,
    welcome: Welcome,
}

async fn read_command(r: &mut Reader, buf: &mut BytesMut, max: u32) -> Result<Command> {
    loop {
        if let Some(f) =
            proto::decode_frame(buf, max).map_err(|e| Error::Protocol(e.to_string()))?
        {
            return Command::decode(f).map_err(|e| Error::Protocol(e.to_string()));
        }
        if r.read_buf(buf).await? == 0 {
            return Err(Error::Closed(
                "server closed the connection during handshake".into(),
            ));
        }
    }
}

async fn handshake(opts: &ConnectOptions) -> Result<Handshake> {
    let tcp = timeout(
        opts.connect_timeout,
        TcpStream::connect(opts.url.authority()),
    )
    .await
    .map_err(|_| Error::Io(format!("connecting to {} timed out", opts.url.authority())))??;
    tcp.set_nodelay(true)?;
    let stream: Box<dyn IoStream> = match opts.url.transport {
        Transport::Tls => crate::tls::wrap(tcp, opts).await?,
        _ => Box::new(tcp),
    };
    let (mut r, mut w) = tokio::io::split(stream);
    let mut client = Map::new();
    client.insert("product".into(), Value::from("firemq-rust"));
    client.insert("version".into(), Value::from(env!("CARGO_PKG_VERSION")));
    client.insert("lang".into(), Value::from("rust"));
    if !opts.connection_name.is_empty() {
        client.insert(
            "connection_name".into(),
            Value::from(opts.connection_name.as_str()),
        );
    }
    let mut out = BytesMut::new();
    out.put_slice(proto::MAGIC);
    Command::Hello(Hello {
        ver: proto::PROTOCOL_VERSION,
        vhost: opts.url.vhost.clone(),
        auth: Auth::plain(&opts.url.user, &opts.url.pass),
        heartbeat: opts.heartbeat,
        client,
        caps: vec!["confirms".into(), "credit".into()],
    })
    .encode(&mut out);
    w.write_all(&out).await?;
    let mut buf = BytesMut::with_capacity(16 * 1024);
    let welcome = match timeout(
        opts.connect_timeout,
        read_command(&mut r, &mut buf, proto::MAX_HELLO_FRAME),
    )
    .await
    .map_err(|_| Error::Io("handshake timed out".into()))??
    {
        Command::Welcome(w) => w,
        Command::Error(e) => {
            return Err(Error::Server {
                code: e.code,
                msg: e.msg,
            });
        }
        other => {
            return Err(Error::Protocol(format!(
                "expected welcome, got {}",
                other.name()
            )));
        }
    };
    Ok(Handshake { r, w, buf, welcome })
}

/// Creates the connection state for a handshake. `spawn` starts its reader and writer.
fn open(hs: Handshake, generation: u64) -> (Arc<Conn>, impl FnOnce(Weak<Inner>)) {
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    let pending = Arc::new(Mutex::new(Pending::default()));
    let conn = Arc::new(Conn {
        generation,
        out: out_tx,
        pending: pending.clone(),
        rid: AtomicU32::new(1),
        seq: AtomicU64::new(1),
    });
    let weak_conn = Arc::downgrade(&conn);
    let spawn = move |inner: Weak<Inner>| {
        let Handshake { r, w, buf, welcome } = hs;
        let last_recv = Arc::new(Mutex::new(Instant::now()));
        let dead = Arc::new(watch::Sender::new(false));
        tokio::spawn(writer(
            w,
            out_rx,
            welcome.heartbeat,
            last_recv.clone(),
            dead.clone(),
        ));
        tokio::spawn(reader(ReaderState {
            r,
            buf,
            max_frame: welcome.max_frame.max(proto::MAX_HELLO_FRAME),
            inner,
            conn: weak_conn,
            pending,
            generation,
            last_recv,
            dead,
        }));
    };
    (conn, spawn)
}

impl Client {
    /// Connects with a URL such as `fire://user:pass@host:5673/vhost`.
    pub async fn connect(url: &str) -> Result<Client> {
        Client::connect_with(ConnectOptions::from_url(url)?).await
    }

    pub async fn connect_with(opts: ConnectOptions) -> Result<Client> {
        if matches!(opts.url.transport, Transport::Ws | Transport::Wss) {
            return Err(Error::Protocol(
                "the Rust client connects with fire:// or fires://; WebSocket is for browsers"
                    .into(),
            ));
        }
        let hs = handshake(&opts).await?;
        let welcome = hs.welcome.clone();
        let (conn, spawn) = open(hs, 1);
        let inner = Arc::new(Inner {
            opts,
            welcome,
            link: watch::Sender::new(Link::Up(conn)),
            topology: Mutex::default(),
            subs: Mutex::default(),
            next_sub: AtomicU32::new(1),
            generation: AtomicU64::new(1),
        });
        spawn(Arc::downgrade(&inner));
        Ok(Client { inner })
    }

    /// Server name, version and limits from the first handshake.
    pub fn server(&self) -> &Welcome {
        &self.inner.welcome
    }

    /// The current connection, if it is up.
    pub(crate) fn live(&self) -> Option<Arc<Conn>> {
        match &*self.inner.link.borrow() {
            Link::Up(c) if c.is_open() => Some(c.clone()),
            _ => None,
        }
    }

    /// Waits for a usable connection while the client reconnects.
    async fn conn(&self) -> Result<Arc<Conn>> {
        let mut rx = self.inner.link.subscribe();
        let wait = self.inner.opts.reconnect.as_ref().map(|r| r.wait);
        let deadline = Instant::now() + wait.unwrap_or_default();
        loop {
            let ready = match &*rx.borrow_and_update() {
                Link::Up(c) if c.is_open() => Some(Ok(c.clone())),
                Link::Up(c) if wait.is_none() => {
                    Some(Err(c.pending.lock().closed.clone().unwrap_or_else(|| {
                        Error::Closed("connection is closed".into())
                    })))
                }
                Link::Closed(e) => Some(Err(e.clone())),
                _ => None,
            };
            if let Some(r) = ready {
                return r;
            }
            match tokio::time::timeout_at(deadline, rx.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(Error::Closed("client dropped".into())),
                Err(_) => {
                    return Err(Error::Closed(format!(
                        "not connected; still reconnecting after {:?}",
                        wait.unwrap_or_default()
                    )));
                }
            }
        }
    }

    /// True when `e` came from losing `conn` and the client will reconnect, so the caller
    /// should try again.
    fn retry(&self, conn: &Conn, e: &Error) -> bool {
        matches!(e, Error::Closed(_))
            && self.inner.opts.reconnect.is_some()
            && !conn.is_open()
            && !matches!(*self.inner.link.borrow(), Link::Closed(_))
    }

    pub(crate) fn recovers(&self) -> bool {
        self.inner.opts.reconnect.is_some()
    }

    /// Sends on the current connection, if there is one.
    pub(crate) fn send(&self, cmd: Command) -> Result<()> {
        match self.live() {
            Some(c) => c.send(cmd),
            None => Err(Error::Closed("not connected".into())),
        }
    }

    async fn call(&self, build: impl Fn(u32) -> Command) -> Result<Reply> {
        loop {
            let conn = self.conn().await?;
            match conn.call(&build).await {
                Err(e) if self.retry(&conn, &e) => continue,
                r => return r,
            }
        }
    }

    async fn call_ok(&self, build: impl Fn(u32) -> Command) -> Result<OkReply> {
        match self.call(build).await? {
            Reply::Ok(ok) => Ok(ok),
            Reply::Get(..) => Err(Error::Protocol("unexpected get_ok".into())),
        }
    }

    /// Declares an exchange of type `direct`, `fanout`, `topic` or `headers`.
    pub async fn declare_exchange(
        &self,
        name: &str,
        kind: &str,
        opts: ExchangeOptions,
    ) -> Result<()> {
        self.call_ok(|rid| declare_exchange(rid, name, kind, &opts))
            .await?;
        if !opts.passive {
            self.inner.topology.lock().add_exchange(name, kind, &opts);
        }
        Ok(())
    }

    pub async fn delete_exchange(&self, name: &str, if_unused: bool) -> Result<()> {
        self.call_ok(|rid| {
            Command::DeleteExchange(DeleteExchange {
                rid,
                name: name.into(),
                if_unused,
            })
        })
        .await?;
        self.inner.topology.lock().remove_exchange(name);
        Ok(())
    }

    /// Declares a queue. An empty name asks the broker to generate one.
    pub async fn declare_queue(&self, name: &str, opts: QueueOptions) -> Result<QueueInfo> {
        let ok = self.call_ok(|rid| declare_queue(rid, name, &opts)).await?;
        let info = QueueInfo {
            name: ok.name.unwrap_or_else(|| name.to_string()),
            messages: ok.messages.unwrap_or(0),
            consumers: ok.consumers.unwrap_or(0),
        };
        if !opts.passive {
            self.inner.topology.lock().add_queue(QueueRecord {
                name: info.name.clone(),
                opts,
                server_named: name.is_empty(),
            });
        }
        Ok(info)
    }

    /// Deletes a queue and returns how many messages it held.
    pub async fn delete_queue(&self, name: &str) -> Result<u64> {
        let ok = self
            .call_ok(|rid| {
                Command::DeleteQueue(DeleteQueue {
                    rid,
                    name: name.into(),
                    ..Default::default()
                })
            })
            .await?;
        self.inner.topology.lock().remove_queue(name);
        Ok(ok.messages.unwrap_or(0))
    }

    /// Removes every ready message from a queue and returns how many were removed.
    pub async fn purge_queue(&self, name: &str) -> Result<u64> {
        let ok = self
            .call_ok(|rid| {
                Command::PurgeQueue(PurgeQueue {
                    rid,
                    name: name.into(),
                })
            })
            .await?;
        Ok(ok.messages.unwrap_or(0))
    }

    /// Binds `queue` to `exchange` with a routing key or pattern.
    pub async fn bind_queue(&self, queue: &str, exchange: &str, key: &str) -> Result<()> {
        self.bind_queue_with(queue, exchange, key, Map::new()).await
    }

    /// Binds with arguments, for example `x-match` for headers exchanges.
    pub async fn bind_queue_with(
        &self,
        queue: &str,
        exchange: &str,
        key: &str,
        args: Map,
    ) -> Result<()> {
        let b = BindingRecord {
            source: exchange.into(),
            destination: Destination::Queue(queue.into()),
            key: key.into(),
            args,
        };
        self.call_ok(|rid| bind(rid, &b)).await?;
        self.inner.topology.lock().add_binding(b);
        Ok(())
    }

    pub async fn unbind_queue(&self, queue: &str, exchange: &str, key: &str) -> Result<()> {
        self.call_ok(|rid| {
            Command::Unbind(Bind {
                rid,
                exchange: exchange.into(),
                queue: queue.into(),
                key: key.into(),
                ..Default::default()
            })
        })
        .await?;
        self.inner
            .topology
            .lock()
            .remove_binding(exchange, &Destination::Queue(queue.into()), key);
        Ok(())
    }

    /// Binds exchange `destination` to exchange `source`.
    pub async fn bind_exchange(&self, destination: &str, source: &str, key: &str) -> Result<()> {
        let b = BindingRecord {
            source: source.into(),
            destination: Destination::Exchange(destination.into()),
            key: key.into(),
            args: Map::new(),
        };
        self.call_ok(|rid| bind(rid, &b)).await?;
        self.inner.topology.lock().add_binding(b);
        Ok(())
    }

    /// Publishes a persistent message and waits for the broker's confirm. Use exchange `""`
    /// to send straight to the queue named by `key`.
    pub async fn publish(&self, exchange: &str, key: &str, body: impl Into<Bytes>) -> Result<()> {
        self.publish_with(exchange, key, body, PublishOptions::default())
            .await
    }

    /// Publishes a JSON-encoded value.
    pub async fn publish_json<T: serde::Serialize>(
        &self,
        exchange: &str,
        key: &str,
        value: &T,
    ) -> Result<()> {
        let body = serde_json::to_vec(value).map_err(|e| Error::Protocol(e.to_string()))?;
        let mut opts = PublishOptions::default();
        opts.properties.content_type = Some("application/json".into());
        self.publish_with(exchange, key, body, opts).await
    }

    /// Publishes with options. When the connection drops before the confirm arrives, the
    /// client publishes the message again after reconnecting.
    pub async fn publish_with(
        &self,
        exchange: &str,
        key: &str,
        body: impl Into<Bytes>,
        opts: PublishOptions,
    ) -> Result<()> {
        let body = body.into();
        loop {
            let conn = self.conn().await?;
            let result = match conn.publish(exchange, key, body.clone(), &opts) {
                Ok(None) => return Ok(()),
                Ok(Some(rx)) => rx.await.unwrap_or_else(|_| {
                    Err(Error::Closed("connection closed before the confirm".into()))
                }),
                Err(e) => Err(e),
            };
            match result {
                Err(e) if self.retry(&conn, &e) => continue,
                r => return r,
            }
        }
    }

    /// Starts consuming from a queue. The subscription survives reconnects.
    pub async fn subscribe(&self, queue: &str, opts: SubscribeOptions) -> Result<Subscription> {
        let sub = self.inner.next_sub.fetch_add(1, Ordering::Relaxed);
        let window = opts.prefetch.max(1);
        let (tx, rx) = mpsc::unbounded_channel();
        let mut conn = self.conn().await?;
        self.inner.subs.lock().insert(
            sub,
            SubRecord {
                queue: queue.into(),
                opts: opts.clone(),
                tx,
                credit: Arc::new(Credit::new(sub, window, &conn)),
            },
        );
        loop {
            let queue = self
                .inner
                .subs
                .lock()
                .get(&sub)
                .map(|r| r.queue.clone())
                .unwrap_or_else(|| queue.to_string());
            match conn.call_ok(|rid| subscribe(rid, sub, &queue, &opts)).await {
                Ok(_) => break,
                Err(e) if self.retry(&conn, &e) => {
                    conn = match self.conn().await {
                        Ok(c) => c,
                        Err(e) => {
                            self.inner.subs.lock().remove(&sub);
                            return Err(e);
                        }
                    };
                    let mut subs = self.inner.subs.lock();
                    match subs.get_mut(&sub) {
                        None => {
                            return Err(Error::Closed(
                                "subscription ended while reconnecting".into(),
                            ));
                        }
                        // Recovery already resumed it on the new connection.
                        Some(r) if r.credit.generation == conn.generation => break,
                        Some(r) => r.credit = Arc::new(Credit::new(sub, window, &conn)),
                    }
                }
                Err(e) => {
                    self.inner.subs.lock().remove(&sub);
                    return Err(e);
                }
            }
        }
        Ok(Subscription::new(self.clone(), sub, rx, opts.no_ack))
    }

    /// Takes one message from a queue, or `None` when it is empty.
    pub async fn get(&self, queue: &str, no_ack: bool) -> Result<Option<Message>> {
        loop {
            let conn = self.conn().await?;
            let reply = conn
                .call(|rid| {
                    Command::Get(Get {
                        rid,
                        queue: queue.into(),
                        no_ack,
                    })
                })
                .await;
            return match reply {
                Err(e) if self.retry(&conn, &e) => continue,
                Err(e) => Err(e),
                Ok(Reply::Ok(_)) => Ok(None),
                Ok(Reply::Get(g, body)) => Ok(Some(Message {
                    body,
                    exchange: g.exchange,
                    routing_key: g.key,
                    properties: g.props,
                    headers: g.headers,
                    redelivered: g.redelivered,
                    delivery_tag: g.tag,
                    acker: (!no_ack).then(|| Acker::new(conn.clone(), self.recovers())),
                    credit: None,
                })),
            };
        }
    }

    /// Closes the connection gracefully and stops reconnecting.
    pub async fn close(&self) -> Result<()> {
        let closed = Error::Closed("client closed the connection".into());
        let prev = self.inner.link.send_replace(Link::Closed(closed.clone()));
        self.inner.subs.lock().clear();
        if let Link::Up(conn) = prev {
            let _ = conn.send(Command::Close(proto::Close {
                code: 200,
                msg: "client closed".into(),
            }));
            let deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < deadline && conn.is_open() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            conn.pending.lock().fail_all(&closed);
        }
        Ok(())
    }

    /// True once the client is closed for good: [`Client::close`] was called, the
    /// connection dropped with reconnection off, or reconnecting gave up.
    pub fn is_closed(&self) -> bool {
        match &*self.inner.link.borrow() {
            Link::Closed(_) => true,
            Link::Up(c) => !self.recovers() && !c.is_open(),
            Link::Down => false,
        }
    }

    /// True while a connection is up. False while reconnecting.
    pub fn is_connected(&self) -> bool {
        self.live().is_some()
    }
}

fn declare_exchange(rid: u32, name: &str, kind: &str, opts: &ExchangeOptions) -> Command {
    Command::DeclareExchange(DeclareExchange {
        rid,
        name: name.into(),
        kind: kind.into(),
        durable: opts.durable,
        auto_delete: opts.auto_delete,
        internal: opts.internal,
        passive: opts.passive,
        args: opts.args.clone(),
    })
}

fn declare_queue(rid: u32, name: &str, opts: &QueueOptions) -> Command {
    Command::DeclareQueue(DeclareQueue {
        rid,
        name: name.into(),
        durable: opts.durable,
        exclusive: opts.exclusive,
        auto_delete: opts.auto_delete,
        passive: opts.passive,
        args: opts.args.clone(),
    })
}

fn bind(rid: u32, b: &BindingRecord) -> Command {
    let mut cmd = Bind {
        rid,
        exchange: b.source.clone(),
        key: b.key.clone(),
        args: b.args.clone(),
        ..Default::default()
    };
    match &b.destination {
        Destination::Queue(q) => cmd.queue = q.clone(),
        Destination::Exchange(e) => cmd.to_exchange = e.clone(),
    }
    Command::Bind(cmd)
}

fn subscribe(rid: u32, sub: u32, queue: &str, opts: &SubscribeOptions) -> Command {
    Command::Subscribe(Subscribe {
        rid,
        sub,
        queue: queue.into(),
        credit: opts.prefetch.max(1),
        no_ack: opts.no_ack,
        exclusive: opts.exclusive,
        tag: opts.tag.clone(),
        args: Map::new(),
    })
}

impl Inner {
    /// Called by a connection's reader when the connection ends.
    fn connection_lost(self: &Arc<Inner>, generation: u64, reason: Error) {
        let reconnect = self.opts.reconnect.clone();
        let mut lost = false;
        self.link.send_if_modified(|link| match link {
            Link::Up(c) if c.generation == generation => {
                lost = true;
                *link = match reconnect {
                    Some(_) => Link::Down,
                    None => Link::Closed(match &reason {
                        Error::Closed(_) => reason.clone(),
                        other => Error::Closed(other.to_string()),
                    }),
                };
                true
            }
            _ => false,
        });
        if !lost {
            return;
        }
        match reconnect {
            Some(policy) => {
                tracing::warn!(error = %reason, "lost the connection to FireMQ; reconnecting");
                tokio::spawn(reconnect_loop(Arc::downgrade(self), policy));
            }
            None => self.subs.lock().clear(),
        }
    }

    /// Opens a new connection and declares the topology and subscriptions on it.
    async fn reopen(self: &Arc<Inner>) -> Result<Arc<Conn>> {
        let hs = handshake(&self.opts).await?;
        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let (conn, spawn) = open(hs, generation);
        spawn(Arc::downgrade(self));
        self.recover(&conn).await?;
        Ok(conn)
    }

    async fn recover(&self, conn: &Arc<Conn>) -> Result<()> {
        // Errors on a connection that is still open mean the broker refused one object,
        // for example because someone redeclared it with other arguments. Log them and
        // carry on; a lost connection fails the whole attempt.
        let check = |what: String, r: Result<OkReply>| -> Result<Option<OkReply>> {
            match r {
                Ok(ok) => Ok(Some(ok)),
                Err(e) if !conn.is_open() => Err(e),
                Err(e) => {
                    tracing::warn!(error = %e, "could not restore {what} after reconnecting");
                    Ok(None)
                }
            }
        };
        let topology = self.topology.lock().clone();
        for (name, kind, opts) in &topology.exchanges {
            let r = conn
                .call_ok(|rid| declare_exchange(rid, name, kind, opts))
                .await;
            check(format!("exchange '{name}'"), r)?;
        }
        for q in &topology.queues {
            let name = if q.server_named { "" } else { q.name.as_str() };
            let r = conn.call_ok(|rid| declare_queue(rid, name, &q.opts)).await;
            let Some(ok) = check(format!("queue '{}'", q.name), r)? else {
                continue;
            };
            if let Some(new) = ok.name.filter(|n| q.server_named && *n != q.name) {
                tracing::info!(old = %q.name, new = %new, "server-named queue has a new name");
                self.topology.lock().rename_queue(&q.name, &new);
                for s in self.subs.lock().values_mut() {
                    if s.queue == q.name {
                        s.queue = new.clone();
                    }
                }
            }
        }
        let bindings = self.topology.lock().bindings.clone();
        for b in &bindings {
            let r = conn.call_ok(|rid| bind(rid, b)).await;
            check(format!("binding from '{}'", b.source), r)?;
        }
        let subs: Vec<(u32, String, SubscribeOptions)> = self
            .subs
            .lock()
            .iter()
            .map(|(id, s)| (*id, s.queue.clone(), s.opts.clone()))
            .collect();
        for (sub, queue, opts) in subs {
            // Point the subscription at the new connection before the broker starts
            // delivering on it.
            match self.subs.lock().get_mut(&sub) {
                Some(r) => r.credit = Arc::new(Credit::new(sub, opts.prefetch.max(1), conn)),
                None => continue,
            }
            let r = conn.call_ok(|rid| subscribe(rid, sub, &queue, &opts)).await;
            if check(format!("subscription to '{queue}'"), r)?.is_none() {
                // Ends the subscription: its stream returns None.
                self.subs.lock().remove(&sub);
            }
        }
        Ok(())
    }
}

async fn reconnect_loop(weak: Weak<Inner>, policy: Reconnect) {
    let mut delay = policy.initial_delay;
    let mut attempts = 0u32;
    loop {
        tokio::time::sleep(delay).await;
        let Some(inner) = weak.upgrade() else { return };
        if !matches!(*inner.link.borrow(), Link::Down) {
            return;
        }
        attempts += 1;
        let last = match inner.reopen().await {
            Ok(conn) => {
                let installed = inner.link.send_if_modified(|link| {
                    if matches!(link, Link::Down) {
                        *link = Link::Up(conn.clone());
                        true
                    } else {
                        false
                    }
                });
                if installed {
                    tracing::info!(attempts, "reconnected to FireMQ");
                } else {
                    let _ = conn.send(Command::Close(proto::Close {
                        code: 200,
                        msg: "client closed".into(),
                    }));
                }
                return;
            }
            Err(e) => e,
        };
        tracing::warn!(attempt = attempts, error = %last, "reconnecting to FireMQ failed");
        if policy.max_attempts.is_some_and(|max| attempts >= max) {
            let gave_up = Error::Closed(format!(
                "gave up reconnecting after {attempts} attempts: {last}"
            ));
            inner.link.send_if_modified(|link| {
                if matches!(link, Link::Down) {
                    *link = Link::Closed(gave_up);
                    true
                } else {
                    false
                }
            });
            inner.subs.lock().clear();
            return;
        }
        delay = (delay * 2).min(policy.max_delay);
    }
}

async fn writer(
    mut w: Writer,
    mut rx: mpsc::UnboundedReceiver<Bytes>,
    heartbeat: u32,
    last_recv: Arc<Mutex<Instant>>,
    dead: Arc<watch::Sender<bool>>,
) {
    let tick = Duration::from_secs((heartbeat as u64 / 2).max(1));
    let mut interval = tokio::time::interval(tick);
    let mut last_send = Instant::now();
    let mut dead_rx = dead.subscribe();
    loop {
        tokio::select! {
            _ = dead_rx.changed() => break,
            chunk = rx.recv() => {
                let Some(chunk) = chunk else { break };
                let mut ok = w.write_all(&chunk).await.is_ok();
                while ok && let Ok(more) = rx.try_recv() {
                    ok = w.write_all(&more).await.is_ok();
                }
                if !ok {
                    dead.send_replace(true);
                    return;
                }
                last_send = Instant::now();
            }
            _ = interval.tick(), if heartbeat > 0 => {
                if last_send.elapsed() >= tick {
                    if w.write_all(&Command::Ping.to_bytes()).await.is_err() {
                        dead.send_replace(true);
                        return;
                    }
                    last_send = Instant::now();
                }
                if last_recv.lock().elapsed() > Duration::from_secs(heartbeat as u64 * 2) {
                    tracing::warn!("FireMQ server missed heartbeats; closing");
                    dead.send_replace(true);
                    break;
                }
            }
        }
    }
    let _ = w.shutdown().await;
}

/// What a connection's reader task needs.
struct ReaderState {
    r: Reader,
    buf: BytesMut,
    max_frame: u32,
    inner: Weak<Inner>,
    conn: Weak<Conn>,
    pending: Arc<Mutex<Pending>>,
    generation: u64,
    last_recv: Arc<Mutex<Instant>>,
    dead: Arc<watch::Sender<bool>>,
}

async fn reader(mut l: ReaderState) {
    let mut dead_rx = l.dead.subscribe();
    let reason = 'outer: loop {
        loop {
            match proto::decode_frame(&mut l.buf, l.max_frame) {
                Ok(Some(f)) => match Command::decode(f) {
                    Ok(cmd) => {
                        if let Some(reason) = dispatch(cmd, &l) {
                            break 'outer reason;
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "ignoring undecodable frame"),
                },
                Ok(None) => break,
                Err(e) => break 'outer Error::Protocol(e.to_string()),
            }
        }
        tokio::select! {
            r = l.r.read_buf(&mut l.buf) => match r {
                Ok(0) => break Error::Closed("server closed the connection".into()),
                Err(e) => break Error::Io(e.to_string()),
                Ok(_) => *l.last_recv.lock() = Instant::now(),
            },
            _ = dead_rx.changed() => {
                break Error::Io("the server stopped responding".into());
            }
        }
    };
    l.dead.send_replace(true);
    l.pending.lock().fail_all(&reason);
    if let Some(inner) = l.inner.upgrade() {
        inner.connection_lost(l.generation, reason);
    }
}

/// Handles one server frame. Returns a reason when the connection is over.
fn dispatch(cmd: Command, l: &ReaderState) -> Option<Error> {
    match cmd {
        Command::Ok(ok) => {
            if let Some(tx) = l.pending.lock().requests.remove(&ok.rid) {
                let _ = tx.send(Ok(Reply::Ok(ok)));
            }
        }
        Command::GetOk(g, body) => {
            if let Some(tx) = l.pending.lock().requests.remove(&g.rid) {
                let _ = tx.send(Ok(Reply::Get(Box::new(g), body)));
            }
        }
        Command::Error(e) => {
            let err = Error::Server {
                code: e.code,
                msg: e.msg.clone(),
            };
            if let Some(rid) = e.rid {
                if let Some(tx) = l.pending.lock().requests.remove(&rid) {
                    let _ = tx.send(Err(err.clone()));
                }
            } else if !e.fatal {
                tracing::warn!(code = e.code, msg = %e.msg, "FireMQ server reported an error");
            }
            if e.fatal {
                return Some(err);
            }
        }
        Command::Confirm(c) => {
            let mut p = l.pending.lock();
            if c.nack {
                if let Some(seq) = c.seq {
                    p.returned.remove(&seq);
                    if let Some(tx) = p.confirms.remove(&seq) {
                        let _ = tx.send(Err(Error::Nacked {
                            code: c.code,
                            msg: c.msg,
                        }));
                    }
                }
            } else if let Some(upto) = c.upto {
                let rest = p.confirms.split_off(&(upto + 1));
                let done = std::mem::replace(&mut p.confirms, rest);
                for (seq, tx) in done {
                    let result = match p.returned.remove(&seq) {
                        Some((exchange, key)) => Err(Error::Unroutable { exchange, key }),
                        None => Ok(()),
                    };
                    let _ = tx.send(result);
                }
            }
        }
        Command::Return(r, _) => {
            if r.seq > 0 {
                l.pending.lock().returned.insert(r.seq, (r.exchange, r.key));
            }
        }
        Command::Deliver(d, body) => {
            let (Some(inner), Some(conn)) = (l.inner.upgrade(), l.conn.upgrade()) else {
                return None;
            };
            let subs = inner.subs.lock();
            if let Some(s) = subs.get(&d.sub) {
                let msg = Message {
                    body,
                    exchange: d.exchange,
                    routing_key: d.key,
                    properties: d.props,
                    headers: d.headers,
                    redelivered: d.redelivered,
                    delivery_tag: d.tag,
                    acker: (!s.opts.no_ack)
                        .then(|| Acker::new(conn.clone(), inner.opts.reconnect.is_some())),
                    credit: (s.credit.generation == conn.generation).then(|| s.credit.clone()),
                };
                let _ = s.tx.send(msg);
            }
        }
        Command::Cancelled(c) => {
            if let Some(inner) = l.inner.upgrade() {
                inner.subs.lock().remove(&c.sub);
            }
        }
        Command::Ping => {
            if let Some(conn) = l.conn.upgrade() {
                let _ = conn.out.send(Command::Pong.to_bytes());
            }
        }
        Command::Close(c) => {
            if let Some(conn) = l.conn.upgrade() {
                let _ = conn.out.send(
                    Command::Close(proto::Close {
                        code: 200,
                        msg: String::new(),
                    })
                    .to_bytes(),
                );
            }
            return Some(Error::Closed(if c.msg.is_empty() {
                format!("closed by server (code {})", c.code)
            } else {
                c.msg
            }));
        }
        Command::Pong | Command::Flow(_) => {}
        other => tracing::debug!(op = other.name(), "ignoring unexpected frame"),
    }
    None
}

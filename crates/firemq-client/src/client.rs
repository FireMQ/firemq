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
use tokio::sync::{mpsc, oneshot};
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

#[derive(Default)]
pub(crate) struct State {
    requests: HashMap<u32, oneshot::Sender<Result<Reply>>>,
    confirms: BTreeMap<u64, oneshot::Sender<Result<()>>>,
    returned: HashMap<u64, (String, String)>,
    pub(crate) subs: HashMap<u32, (mpsc::UnboundedSender<Message>, Option<Arc<Credit>>)>,
    closed: Option<Error>,
}

impl State {
    fn fail_all(&mut self, e: Error) {
        for (_, tx) in self.requests.drain() {
            let _ = tx.send(Err(e.clone()));
        }
        for (_, tx) in std::mem::take(&mut self.confirms) {
            let _ = tx.send(Err(e.clone()));
        }
        self.subs.clear();
        self.closed.get_or_insert(e);
    }
}

pub(crate) struct Inner {
    pub(crate) out: mpsc::UnboundedSender<Bytes>,
    pub(crate) state: Arc<Mutex<State>>,
    rid: AtomicU32,
    seq: AtomicU64,
    sub: AtomicU32,
    welcome: Welcome,
}

/// A connection to FireMQ. Cheap to clone; clones share the connection.
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
        let tcp = timeout(
            opts.connect_timeout,
            TcpStream::connect(opts.url.authority()),
        )
        .await
        .map_err(|_| Error::Io(format!("connecting to {} timed out", opts.url.authority())))??;
        tcp.set_nodelay(true)?;
        let stream: Box<dyn IoStream> = match opts.url.transport {
            Transport::Tls => crate::tls::wrap(tcp, &opts).await?,
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
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let state = Arc::new(Mutex::new(State::default()));
        let inner = Arc::new(Inner {
            out: out_tx,
            state: state.clone(),
            rid: AtomicU32::new(1),
            seq: AtomicU64::new(1),
            sub: AtomicU32::new(1),
            welcome: welcome.clone(),
        });
        let last_recv = Arc::new(Mutex::new(Instant::now()));
        tokio::spawn(writer(w, out_rx, welcome.heartbeat, last_recv.clone()));
        tokio::spawn(reader(
            r,
            buf,
            welcome.max_frame.max(proto::MAX_HELLO_FRAME),
            Arc::downgrade(&inner),
            state,
            last_recv,
        ));
        Ok(Client { inner })
    }

    /// Server name, version and limits from the handshake.
    pub fn server(&self) -> &Welcome {
        &self.inner.welcome
    }

    pub(crate) fn send(&self, cmd: Command) -> Result<()> {
        if let Some(e) = &self.inner.state.lock().closed {
            return Err(e.clone());
        }
        self.inner
            .out
            .send(cmd.to_bytes())
            .map_err(|_| Error::Closed("connection is closed".into()))
    }

    async fn call(&self, build: impl FnOnce(u32) -> Command) -> Result<Reply> {
        let rid = self.inner.rid.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut st = self.inner.state.lock();
            if let Some(e) = &st.closed {
                return Err(e.clone());
            }
            st.requests.insert(rid, tx);
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

    /// Declares an exchange of type `direct`, `fanout`, `topic` or `headers`.
    pub async fn declare_exchange(
        &self,
        name: &str,
        kind: &str,
        opts: ExchangeOptions,
    ) -> Result<()> {
        self.call_ok(|rid| {
            Command::DeclareExchange(DeclareExchange {
                rid,
                name: name.into(),
                kind: kind.into(),
                durable: opts.durable,
                auto_delete: opts.auto_delete,
                internal: opts.internal,
                passive: opts.passive,
                args: opts.args,
            })
        })
        .await
        .map(|_| ())
    }

    pub async fn delete_exchange(&self, name: &str, if_unused: bool) -> Result<()> {
        self.call_ok(|rid| {
            Command::DeleteExchange(DeleteExchange {
                rid,
                name: name.into(),
                if_unused,
            })
        })
        .await
        .map(|_| ())
    }

    /// Declares a queue. An empty name asks the broker to generate one.
    pub async fn declare_queue(&self, name: &str, opts: QueueOptions) -> Result<QueueInfo> {
        let ok = self
            .call_ok(|rid| {
                Command::DeclareQueue(DeclareQueue {
                    rid,
                    name: name.into(),
                    durable: opts.durable,
                    exclusive: opts.exclusive,
                    auto_delete: opts.auto_delete,
                    passive: opts.passive,
                    args: opts.args,
                })
            })
            .await?;
        Ok(QueueInfo {
            name: ok.name.unwrap_or_else(|| name.to_string()),
            messages: ok.messages.unwrap_or(0),
            consumers: ok.consumers.unwrap_or(0),
        })
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
        self.call_ok(|rid| {
            Command::Bind(Bind {
                rid,
                exchange: exchange.into(),
                queue: queue.into(),
                key: key.into(),
                args,
                ..Default::default()
            })
        })
        .await
        .map(|_| ())
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
        .await
        .map(|_| ())
    }

    /// Binds exchange `destination` to exchange `source`.
    pub async fn bind_exchange(&self, destination: &str, source: &str, key: &str) -> Result<()> {
        self.call_ok(|rid| {
            Command::Bind(Bind {
                rid,
                exchange: source.into(),
                to_exchange: destination.into(),
                key: key.into(),
                ..Default::default()
            })
        })
        .await
        .map(|_| ())
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

    pub async fn publish_with(
        &self,
        exchange: &str,
        key: &str,
        body: impl Into<Bytes>,
        opts: PublishOptions,
    ) -> Result<()> {
        let seq = if opts.confirm {
            self.inner.seq.fetch_add(1, Ordering::Relaxed)
        } else {
            0
        };
        let rx = if seq > 0 {
            let (tx, rx) = oneshot::channel();
            let mut st = self.inner.state.lock();
            if let Some(e) = &st.closed {
                return Err(e.clone());
            }
            st.confirms.insert(seq, tx);
            Some(rx)
        } else {
            None
        };
        self.send(Command::Publish(
            proto::Publish {
                seq,
                exchange: exchange.into(),
                key: key.into(),
                mandatory: opts.mandatory,
                persistent: opts.persistent,
                props: opts.properties,
                headers: opts.headers,
            },
            body.into(),
        ))?;
        match rx {
            Some(rx) => rx
                .await
                .map_err(|_| Error::Closed("connection closed before the confirm".into()))?,
            None => Ok(()),
        }
    }

    /// Starts consuming from a queue.
    pub async fn subscribe(&self, queue: &str, opts: SubscribeOptions) -> Result<Subscription> {
        let sub = self.inner.sub.fetch_add(1, Ordering::Relaxed);
        let window = opts.prefetch.max(1);
        let credit = Arc::new(Credit::new(sub, window));
        let (tx, rx) = mpsc::unbounded_channel();
        self.inner
            .state
            .lock()
            .subs
            .insert(sub, (tx, Some(credit.clone())));
        let result = self
            .call_ok(|rid| {
                Command::Subscribe(Subscribe {
                    rid,
                    sub,
                    queue: queue.into(),
                    credit: window,
                    no_ack: opts.no_ack,
                    exclusive: opts.exclusive,
                    tag: opts.tag,
                    args: Map::new(),
                })
            })
            .await;
        if let Err(e) = result {
            self.inner.state.lock().subs.remove(&sub);
            return Err(e);
        }
        Ok(Subscription::new(
            self.clone(),
            sub,
            rx,
            credit,
            opts.no_ack,
        ))
    }

    /// Takes one message from a queue, or `None` when it is empty.
    pub async fn get(&self, queue: &str, no_ack: bool) -> Result<Option<Message>> {
        match self
            .call(|rid| {
                Command::Get(Get {
                    rid,
                    queue: queue.into(),
                    no_ack,
                })
            })
            .await?
        {
            Reply::Ok(_) => Ok(None),
            Reply::Get(g, body) => Ok(Some(Message {
                body,
                exchange: g.exchange,
                routing_key: g.key,
                properties: g.props,
                headers: g.headers,
                redelivered: g.redelivered,
                delivery_tag: g.tag,
                acker: (!no_ack).then(|| Acker::new(self.clone(), None)),
            })),
        }
    }

    /// Closes the connection gracefully.
    pub async fn close(&self) -> Result<()> {
        let _ = self.send(Command::Close(proto::Close {
            code: 200,
            msg: "client closed".into(),
        }));
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if self.inner.state.lock().closed.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        self.inner
            .state
            .lock()
            .fail_all(Error::Closed("client closed the connection".into()));
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.inner.state.lock().closed.is_some()
    }
}

async fn writer(
    mut w: Writer,
    mut rx: mpsc::UnboundedReceiver<Bytes>,
    heartbeat: u32,
    last_recv: Arc<Mutex<Instant>>,
) {
    let tick = Duration::from_secs((heartbeat as u64 / 2).max(1));
    let mut interval = tokio::time::interval(tick);
    let mut last_send = Instant::now();
    loop {
        tokio::select! {
            chunk = rx.recv() => {
                let Some(chunk) = chunk else { break };
                if w.write_all(&chunk).await.is_err() {
                    return;
                }
                while let Ok(more) = rx.try_recv() {
                    if w.write_all(&more).await.is_err() {
                        return;
                    }
                }
                last_send = Instant::now();
            }
            _ = interval.tick(), if heartbeat > 0 => {
                if last_send.elapsed() >= tick {
                    if w.write_all(&Command::Ping.to_bytes()).await.is_err() {
                        return;
                    }
                    last_send = Instant::now();
                }
                if last_recv.lock().elapsed() > Duration::from_secs(heartbeat as u64 * 2) {
                    tracing::warn!("FireMQ server missed heartbeats; closing");
                    break;
                }
            }
        }
    }
    let _ = w.shutdown().await;
}

async fn reader(
    mut r: Reader,
    mut buf: BytesMut,
    max_frame: u32,
    inner: Weak<Inner>,
    state: Arc<Mutex<State>>,
    last_recv: Arc<Mutex<Instant>>,
) {
    let reason = loop {
        loop {
            match proto::decode_frame(&mut buf, max_frame) {
                Ok(Some(f)) => match Command::decode(f) {
                    Ok(cmd) => {
                        if let Some(reason) = dispatch(cmd, &inner, &state) {
                            let mut st = state.lock();
                            st.fail_all(reason);
                            return;
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "ignoring undecodable frame"),
                },
                Ok(None) => break,
                Err(e) => {
                    let mut st = state.lock();
                    st.fail_all(Error::Protocol(e.to_string()));
                    return;
                }
            }
        }
        match r.read_buf(&mut buf).await {
            Ok(0) => break Error::Closed("server closed the connection".into()),
            Err(e) => break Error::Io(e.to_string()),
            Ok(_) => *last_recv.lock() = Instant::now(),
        }
    };
    state.lock().fail_all(reason);
}

/// Handles one server frame. Returns a reason when the connection is over.
fn dispatch(cmd: Command, inner: &Weak<Inner>, state: &Arc<Mutex<State>>) -> Option<Error> {
    match cmd {
        Command::Ok(ok) => {
            if let Some(tx) = state.lock().requests.remove(&ok.rid) {
                let _ = tx.send(Ok(Reply::Ok(ok)));
            }
        }
        Command::GetOk(g, body) => {
            if let Some(tx) = state.lock().requests.remove(&g.rid) {
                let _ = tx.send(Ok(Reply::Get(Box::new(g), body)));
            }
        }
        Command::Error(e) => {
            let err = Error::Server {
                code: e.code,
                msg: e.msg.clone(),
            };
            if let Some(rid) = e.rid {
                if let Some(tx) = state.lock().requests.remove(&rid) {
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
            let mut st = state.lock();
            if c.nack {
                if let Some(seq) = c.seq {
                    st.returned.remove(&seq);
                    if let Some(tx) = st.confirms.remove(&seq) {
                        let _ = tx.send(Err(Error::Nacked {
                            code: c.code,
                            msg: c.msg,
                        }));
                    }
                }
            } else if let Some(upto) = c.upto {
                let rest = st.confirms.split_off(&(upto + 1));
                let done = std::mem::replace(&mut st.confirms, rest);
                for (seq, tx) in done {
                    let result = match st.returned.remove(&seq) {
                        Some((exchange, key)) => Err(Error::Unroutable { exchange, key }),
                        None => Ok(()),
                    };
                    let _ = tx.send(result);
                }
            }
        }
        Command::Return(r, _) => {
            if r.seq > 0 {
                state.lock().returned.insert(r.seq, (r.exchange, r.key));
            }
        }
        Command::Deliver(d, body) => {
            let st = state.lock();
            if let Some((tx, credit)) = st.subs.get(&d.sub) {
                let client = inner.upgrade().map(|inner| Client { inner });
                let msg = Message {
                    body,
                    exchange: d.exchange,
                    routing_key: d.key,
                    properties: d.props,
                    headers: d.headers,
                    redelivered: d.redelivered,
                    delivery_tag: d.tag,
                    acker: client.map(|c| Acker::new(c, credit.clone())),
                };
                let _ = tx.send(msg);
            }
        }
        Command::Cancelled(c) => {
            state.lock().subs.remove(&c.sub);
        }
        Command::Ping => {
            if let Some(inner) = inner.upgrade() {
                let _ = inner.out.send(Command::Pong.to_bytes());
            }
        }
        Command::Close(c) => {
            if let Some(inner) = inner.upgrade() {
                let _ = inner.out.send(
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

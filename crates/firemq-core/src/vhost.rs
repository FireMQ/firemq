use std::collections::HashMap;
use std::sync::{Arc, Weak};

use arc_swap::ArcSwap;
use base64::Engine;
use firemq_amqp_codec::FieldTable;
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;

use crate::broker::Ctx;
use crate::error::{BrokerError, Result};
use crate::exchange::{Binding, Destination, Exchange, ExchangeKind, ExchangeOptions};
use crate::queue::{Actor, QueueConfig, QueueHandle, QueueOptions, QueueShared};
use crate::storage::Recovered;

/// A routing snapshot: every exchange and queue of a vhost. Readers get an `Arc` and never
/// block writers; writers build a new snapshot and swap it in.
#[derive(Debug, Clone, Default)]
pub struct Topology {
    pub exchanges: HashMap<String, Arc<Exchange>>,
    pub queues: HashMap<String, QueueHandle>,
}

/// The result of declaring a queue.
#[derive(Debug, Clone)]
pub struct QueueDeclared {
    pub queue: QueueHandle,
    pub message_count: u64,
    pub consumer_count: u32,
    pub created: bool,
}

/// A policy: settings applied to queues whose names match a pattern.
#[derive(Debug, Clone)]
pub struct Policy {
    pub name: String,
    pub pattern: String,
    pub regex: regex::Regex,
    pub apply_to: String,
    pub definition: serde_json::Value,
    pub priority: i64,
}

impl Policy {
    pub fn new(
        name: impl Into<String>,
        pattern: impl Into<String>,
        apply_to: impl Into<String>,
        definition: serde_json::Value,
        priority: i64,
    ) -> Result<Policy> {
        let pattern = pattern.into();
        let regex = regex::Regex::new(&pattern).map_err(|e| {
            BrokerError::precondition_failed(format!("invalid policy pattern '{pattern}': {e}"))
        })?;
        Ok(Policy {
            name: name.into(),
            pattern,
            regex,
            apply_to: apply_to.into(),
            definition,
            priority,
        })
    }

    fn applies_to_queue(&self, name: &str) -> bool {
        matches!(self.apply_to.as_str(), "queues" | "classic_queues" | "all")
            && self.regex.is_match(name)
    }
}

/// A virtual host: an isolated namespace of exchanges, queues and bindings.
pub struct VHost {
    pub name: String,
    pub(crate) ctx: Arc<Ctx>,
    topology: ArcSwap<Topology>,
    write: Mutex<()>,
    policies: RwLock<Vec<Policy>>,
    me: Weak<VHost>,
}

impl std::fmt::Debug for VHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VHost").field("name", &self.name).finish()
    }
}

fn not_found_queue(name: &str, vhost: &str) -> BrokerError {
    BrokerError::not_found(format!("no queue '{name}' in vhost '{vhost}'"))
}

fn not_found_exchange(name: &str, vhost: &str) -> BrokerError {
    BrokerError::not_found(format!("no exchange '{name}' in vhost '{vhost}'"))
}

fn inequivalent(
    what: &str,
    arg: &str,
    name: &str,
    vhost: &str,
    got: impl std::fmt::Display,
    current: impl std::fmt::Display,
) -> BrokerError {
    BrokerError::precondition_failed(format!(
        "inequivalent arg '{arg}' for {what} '{name}' in vhost '{vhost}': received '{got}' but current is '{current}'"
    ))
}

fn generated_name() -> String {
    let bytes: [u8; 16] = rand::random();
    format!(
        "amq.gen-{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    )
}

impl VHost {
    pub(crate) fn new(name: impl Into<String>, ctx: Arc<Ctx>) -> Arc<VHost> {
        let name = name.into();
        let mut topology = Topology::default();
        let builtin = [
            ("", ExchangeKind::Direct),
            ("amq.direct", ExchangeKind::Direct),
            ("amq.fanout", ExchangeKind::Fanout),
            ("amq.topic", ExchangeKind::Topic),
            ("amq.headers", ExchangeKind::Headers),
            ("amq.match", ExchangeKind::Headers),
        ];
        for (n, kind) in builtin {
            topology.exchanges.insert(
                n.to_string(),
                Arc::new(Exchange::new(
                    n,
                    kind,
                    true,
                    false,
                    false,
                    FieldTable::new(),
                )),
            );
        }
        Arc::new_cyclic(|me| VHost {
            name,
            ctx,
            topology: ArcSwap::from_pointee(topology),
            write: Mutex::new(()),
            policies: RwLock::new(Vec::new()),
            me: me.clone(),
        })
    }

    /// The current routing snapshot.
    pub fn topology(&self) -> Arc<Topology> {
        self.topology.load_full()
    }

    fn modify<R>(&self, f: impl FnOnce(&mut Topology) -> Result<R>) -> Result<R> {
        let _guard = self.write.lock();
        let mut t = (**self.topology.load()).clone();
        let r = f(&mut t)?;
        self.topology.store(Arc::new(t));
        Ok(r)
    }

    fn durable_changed(&self) {
        self.ctx.definitions_changed();
    }

    pub fn exchange(&self, name: &str) -> Option<Arc<Exchange>> {
        self.topology.load().exchanges.get(name).cloned()
    }

    /// Looks up a queue, enforcing exclusivity for the requesting connection.
    pub fn queue(&self, name: &str, conn: Option<u64>) -> Result<QueueHandle> {
        let t = self.topology.load();
        let q = t
            .queues
            .get(name)
            .ok_or_else(|| not_found_queue(name, &self.name))?;
        self.check_exclusive(q, conn)?;
        Ok(q.clone())
    }

    fn check_exclusive(&self, q: &QueueHandle, conn: Option<u64>) -> Result<()> {
        match q.exclusive_owner {
            Some(owner) if Some(owner) != conn => Err(BrokerError::resource_locked(format!(
                "cannot obtain exclusive access to locked queue '{}' in vhost '{}'. It could be originally declared on another connection or the exclusive property value does not match that of the original declaration.",
                q.name, self.name
            ))),
            _ => Ok(()),
        }
    }

    // ---------------------------------------------------------------- exchanges

    pub fn declare_exchange(&self, opts: ExchangeOptions) -> Result<()> {
        let name = opts.name.as_str();
        if let Some(existing) = self.exchange(name) {
            if opts.passive {
                return Ok(());
            }
            if name.is_empty() {
                return Err(BrokerError::access_refused(
                    "operation not permitted on the default exchange",
                ));
            }
            let kind = ExchangeKind::parse(&opts.kind)?;
            if kind != existing.kind {
                return Err(inequivalent(
                    "exchange",
                    "type",
                    name,
                    &self.name,
                    kind,
                    existing.kind,
                ));
            }
            if opts.durable != existing.durable {
                return Err(inequivalent(
                    "exchange",
                    "durable",
                    name,
                    &self.name,
                    opts.durable,
                    existing.durable,
                ));
            }
            if opts.auto_delete != existing.auto_delete {
                return Err(inequivalent(
                    "exchange",
                    "auto_delete",
                    name,
                    &self.name,
                    opts.auto_delete,
                    existing.auto_delete,
                ));
            }
            if opts.internal != existing.internal {
                return Err(inequivalent(
                    "exchange",
                    "internal",
                    name,
                    &self.name,
                    opts.internal,
                    existing.internal,
                ));
            }
            return Ok(());
        }
        if opts.passive {
            return Err(not_found_exchange(name, &self.name));
        }
        if name.starts_with("amq.") {
            return Err(BrokerError::access_refused(format!(
                "exchange name '{name}' contains reserved prefix 'amq.*'"
            )));
        }
        let kind = ExchangeKind::parse(&opts.kind)?;
        let exchange = Exchange::new(
            name,
            kind,
            opts.durable,
            opts.auto_delete,
            opts.internal,
            opts.arguments,
        );
        self.modify(|t| {
            t.exchanges
                .entry(exchange.name.clone())
                .or_insert_with(|| Arc::new(exchange));
            Ok(())
        })?;
        if opts.durable {
            self.durable_changed();
        }
        Ok(())
    }

    pub fn delete_exchange(&self, name: &str, if_unused: bool) -> Result<()> {
        if name.is_empty() || name.starts_with("amq.") {
            return Err(BrokerError::access_refused(format!(
                "operation not permitted on exchange '{name}'"
            )));
        }
        let durable = self.modify(|t| {
            let Some(ex) = t.exchanges.get(name) else {
                return Ok(false);
            };
            if if_unused && !ex.bindings.is_empty() {
                return Err(BrokerError::precondition_failed(format!(
                    "exchange '{name}' in vhost '{}' in use",
                    self.name
                )));
            }
            let durable = ex.durable;
            remove_exchange(t, name);
            Ok(durable)
        })?;
        if durable {
            self.durable_changed();
        }
        Ok(())
    }

    // ---------------------------------------------------------------- queues

    /// Declares a queue. `conn` is the declaring connection, which owns exclusive queues.
    pub fn declare_queue(&self, opts: QueueOptions, conn: Option<u64>) -> Result<QueueDeclared> {
        let generated = opts.name.is_empty();
        let name = if generated {
            generated_name()
        } else {
            opts.name.clone()
        };
        if let Some(q) = self.topology.load().queues.get(&name).cloned() {
            self.check_exclusive(&q, conn)?;
            if !opts.passive {
                self.check_equivalent(&q, &opts)?;
            }
            q.touch();
            return Ok(QueueDeclared {
                message_count: q.message_count(),
                consumer_count: q.consumer_count(),
                queue: q,
                created: false,
            });
        }
        if opts.passive {
            return Err(not_found_queue(&name, &self.name));
        }
        if !generated && name.starts_with("amq.") {
            return Err(BrokerError::access_refused(format!(
                "queue name '{name}' contains reserved prefix 'amq.*'"
            )));
        }
        if opts.exclusive && conn.is_none() {
            return Err(BrokerError::precondition_failed(
                "exclusive queues need an owning connection",
            ));
        }
        let config = QueueConfig::from_args(&opts.arguments).map_err(|e| {
            BrokerError::precondition_failed(format!(
                "{e} for queue '{name}' in vhost '{}'",
                self.name
            ))
        })?;
        let durable = opts.durable;
        let store = self
            .ctx
            .store
            .open_queue(&self.name, &name, durable && !opts.exclusive)
            .map_err(|e| {
                BrokerError::internal(format!("cannot open storage for queue '{name}': {e}"))
            })?;
        let (q, created) = self.modify(|t| {
            if let Some(q) = t.queues.get(&name) {
                return Ok((q.clone(), false));
            }
            let q = self.spawn_queue(
                name.clone(),
                durable,
                opts.auto_delete,
                opts.exclusive.then_some(conn).flatten(),
                opts.arguments.clone(),
                config,
                store,
                Vec::new(),
            );
            t.queues.insert(name.clone(), q.clone());
            Ok((q, true))
        })?;
        if created && durable && !opts.exclusive {
            self.durable_changed();
        }
        Ok(QueueDeclared {
            message_count: q.message_count(),
            consumer_count: q.consumer_count(),
            queue: q,
            created,
        })
    }

    /// Re-creates a durable queue found in the definitions at startup.
    pub fn restore_queue(
        &self,
        opts: QueueOptions,
        recovered: Vec<Recovered>,
    ) -> Result<QueueHandle> {
        let config =
            QueueConfig::from_args(&opts.arguments).map_err(BrokerError::precondition_failed)?;
        let store = self
            .ctx
            .store
            .open_queue(&self.name, &opts.name, true)
            .map_err(|e| {
                BrokerError::internal(format!(
                    "cannot open storage for queue '{}': {e}",
                    opts.name
                ))
            })?;
        self.restore_queue_with(opts, config, store, recovered)
    }

    /// Re-creates a queue with an already-opened store, used by recovery.
    pub fn restore_queue_with(
        &self,
        opts: QueueOptions,
        config: QueueConfig,
        store: Box<dyn crate::storage::QueueStore>,
        recovered: Vec<Recovered>,
    ) -> Result<QueueHandle> {
        self.modify(|t| {
            let q = self.spawn_queue(
                opts.name.clone(),
                true,
                opts.auto_delete,
                None,
                opts.arguments.clone(),
                config,
                store,
                recovered,
            );
            t.queues.insert(opts.name.clone(), q.clone());
            Ok(q)
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_queue(
        &self,
        name: String,
        durable: bool,
        auto_delete: bool,
        exclusive_owner: Option<u64>,
        arguments: FieldTable,
        config: QueueConfig,
        store: Box<dyn crate::storage::QueueStore>,
        recovered: Vec<Recovered>,
    ) -> QueueHandle {
        let (tx, rx) = mpsc::channel(self.ctx.config.queue_inbox.max(1));
        let effective = self.effective_config(&name, &config);
        let shared = Arc::new(QueueShared {
            name,
            vhost: self.name.clone(),
            durable,
            auto_delete,
            exclusive_owner,
            arguments,
            declared_config: config,
            stats: Default::default(),
            tx,
        });
        let actor = Actor::new(
            shared.clone(),
            rx,
            store,
            effective,
            self.me.clone(),
            self.ctx.config.queue_cache_bytes,
            recovered,
        );
        tokio::spawn(actor.run());
        QueueHandle(shared)
    }

    fn check_equivalent(&self, q: &QueueHandle, opts: &QueueOptions) -> Result<()> {
        let name = &q.name;
        if opts.durable != q.durable {
            return Err(inequivalent(
                "queue",
                "durable",
                name,
                &self.name,
                opts.durable,
                q.durable,
            ));
        }
        if opts.auto_delete != q.auto_delete {
            return Err(inequivalent(
                "queue",
                "auto_delete",
                name,
                &self.name,
                opts.auto_delete,
                q.auto_delete,
            ));
        }
        if opts.exclusive != q.exclusive_owner.is_some() {
            return Err(inequivalent(
                "queue",
                "exclusive",
                name,
                &self.name,
                opts.exclusive,
                q.exclusive_owner.is_some(),
            ));
        }
        let config = QueueConfig::from_args(&opts.arguments).map_err(|e| {
            BrokerError::precondition_failed(format!(
                "{e} for queue '{name}' in vhost '{}'",
                self.name
            ))
        })?;
        if let Some(arg) = config.first_difference(&q.declared_config) {
            let got = opts
                .arguments
                .get(arg)
                .map(|v| format!("{v:?}"))
                .unwrap_or_else(|| "none".into());
            let current = q
                .arguments
                .get(arg)
                .map(|v| format!("{v:?}"))
                .unwrap_or_else(|| "none".into());
            return Err(inequivalent("queue", arg, name, &self.name, got, current));
        }
        Ok(())
    }

    /// Deletes a queue and returns the number of messages it held. Deleting a queue that
    /// does not exist succeeds, as in RabbitMQ.
    pub async fn delete_queue(
        &self,
        name: &str,
        if_unused: bool,
        if_empty: bool,
        conn: Option<u64>,
    ) -> Result<u64> {
        let q = match self.queue(name, conn) {
            Ok(q) => q,
            Err(e) if e.code == 404 => return Ok(0),
            Err(e) => return Err(e),
        };
        if if_unused && q.consumer_count() > 0 {
            return Err(BrokerError::precondition_failed(format!(
                "queue '{name}' in vhost '{}' in use",
                self.name
            )));
        }
        if if_empty && q.message_count() > 0 {
            return Err(BrokerError::precondition_failed(format!(
                "queue '{name}' in vhost '{}' is not empty",
                self.name
            )));
        }
        self.unlink_queue(&q);
        Ok(q.shutdown().await)
    }

    /// Removes a queue and its bindings from the topology without stopping its actor.
    /// Returns false if the queue was already gone.
    pub(crate) fn unlink_queue(&self, q: &QueueHandle) -> bool {
        let removed = self
            .modify(|t| {
                match t.queues.get(&q.name) {
                    Some(current) if current == q => {}
                    _ => return Ok(false),
                }
                t.queues.remove(&q.name);
                remove_bindings_to(t, &Destination::Queue(q.name.clone()));
                Ok(true)
            })
            .unwrap_or(false);
        if removed && q.durable && q.exclusive_owner.is_none() {
            self.durable_changed();
        }
        removed
    }

    pub async fn purge_queue(&self, name: &str, conn: Option<u64>) -> Result<u64> {
        self.queue(name, conn)?.purge().await
    }

    /// Deletes the exclusive queues of a closed connection.
    pub async fn connection_closed(&self, conn: u64) {
        let owned: Vec<QueueHandle> = self
            .topology
            .load()
            .queues
            .values()
            .filter(|q| q.exclusive_owner == Some(conn))
            .cloned()
            .collect();
        for q in owned {
            self.unlink_queue(&q);
            q.shutdown().await;
        }
    }

    // ---------------------------------------------------------------- bindings

    pub fn bind(
        &self,
        source: &str,
        destination: Destination,
        routing_key: &str,
        arguments: FieldTable,
        conn: Option<u64>,
    ) -> Result<()> {
        if source.is_empty() || destination.name().is_empty() {
            return Err(BrokerError::access_refused(
                "operation not permitted on the default exchange",
            ));
        }
        let durable = self.modify(|t| {
            let src = t
                .exchanges
                .get(source)
                .ok_or_else(|| not_found_exchange(source, &self.name))?
                .clone();
            let dest_durable = match &destination {
                Destination::Queue(n) => {
                    let q = t
                        .queues
                        .get(n)
                        .ok_or_else(|| not_found_queue(n, &self.name))?;
                    self.check_exclusive(q, conn)?;
                    q.durable && q.exclusive_owner.is_none()
                }
                Destination::Exchange(n) => {
                    t.exchanges
                        .get(n)
                        .ok_or_else(|| not_found_exchange(n, &self.name))?
                        .durable
                }
            };
            let binding = Binding {
                source: source.to_string(),
                destination: destination.clone(),
                routing_key: routing_key.to_string(),
                arguments,
            };
            if src.bindings.contains(&binding) {
                return Ok(false);
            }
            let mut bindings = src.bindings.clone();
            bindings.push(binding);
            t.exchanges
                .insert(source.to_string(), Arc::new(src.with_bindings(bindings)));
            Ok(src.durable && dest_durable)
        })?;
        if durable {
            self.durable_changed();
        }
        Ok(())
    }

    pub fn unbind(
        &self,
        source: &str,
        destination: Destination,
        routing_key: &str,
        arguments: &FieldTable,
        conn: Option<u64>,
    ) -> Result<()> {
        if let Destination::Queue(n) = &destination {
            if let Some(q) = self.topology.load().queues.get(n) {
                self.check_exclusive(q, conn)?;
            }
        }
        let changed = self.modify(|t| {
            let Some(src) = t.exchanges.get(source).cloned() else {
                return Ok(false);
            };
            let before = src.bindings.len();
            let bindings: Vec<Binding> = src
                .bindings
                .iter()
                .filter(|b| {
                    !(b.destination == destination
                        && b.routing_key == routing_key
                        && (b.arguments == *arguments || src.kind != ExchangeKind::Headers))
                })
                .cloned()
                .collect();
            if bindings.len() == before {
                return Ok(false);
            }
            let empty = bindings.is_empty();
            t.exchanges
                .insert(source.to_string(), Arc::new(src.with_bindings(bindings)));
            if empty && src.auto_delete {
                remove_exchange(t, source);
            }
            Ok(src.durable)
        })?;
        if changed {
            self.durable_changed();
        }
        Ok(())
    }

    // ---------------------------------------------------------------- routing

    /// Finds the queues a message goes to. Fails when the exchange does not exist or is
    /// internal.
    pub fn route(
        &self,
        exchange: &str,
        routing_key: &str,
        headers: Option<&FieldTable>,
    ) -> Result<Vec<QueueHandle>> {
        let t = self.topology.load();
        let mut out: Vec<QueueHandle> = Vec::new();
        if exchange.is_empty() {
            if let Some(q) = t.queues.get(routing_key) {
                out.push(q.clone());
            }
            return Ok(out);
        }
        let ex = t
            .exchanges
            .get(exchange)
            .ok_or_else(|| not_found_exchange(exchange, &self.name))?;
        if ex.internal {
            return Err(BrokerError::access_refused(format!(
                "cannot publish to internal exchange '{exchange}' in vhost '{}'",
                self.name
            )));
        }
        let mut visited: Vec<&str> = Vec::new();
        route_into(&t, ex, routing_key, headers, &mut out, &mut visited);
        // Alternate exchanges catch messages nothing else routed.
        let mut current = ex;
        let mut hops = 0;
        while out.is_empty() && hops < 8 {
            let Some(ae) = current
                .alternate_exchange()
                .and_then(|n| t.exchanges.get(n))
            else {
                break;
            };
            route_into(&t, ae, routing_key, headers, &mut out, &mut visited);
            current = ae;
            hops += 1;
        }
        Ok(out)
    }

    // ---------------------------------------------------------------- policies

    pub fn policies(&self) -> Vec<Policy> {
        self.policies.read().clone()
    }

    /// Adds or replaces a policy and applies it to existing queues.
    pub async fn set_policy(&self, policy: Policy) {
        {
            let mut p = self.policies.write();
            p.retain(|x| x.name != policy.name);
            p.push(policy);
        }
        self.reapply_policies().await;
        self.durable_changed();
    }

    pub async fn clear_policy(&self, name: &str) -> bool {
        let removed = {
            let mut p = self.policies.write();
            let before = p.len();
            p.retain(|x| x.name != name);
            p.len() != before
        };
        if removed {
            self.reapply_policies().await;
            self.durable_changed();
        }
        removed
    }

    async fn reapply_policies(&self) {
        let queues: Vec<QueueHandle> = self.topology.load().queues.values().cloned().collect();
        for q in queues {
            let cfg = self.effective_config(&q.name, &q.declared_config);
            q.set_config(cfg).await;
        }
    }

    /// The highest-priority policy matching a queue name.
    pub fn policy_for_queue(&self, name: &str) -> Option<Policy> {
        self.policies
            .read()
            .iter()
            .filter(|p| p.applies_to_queue(name))
            .max_by_key(|p| p.priority)
            .cloned()
    }

    fn effective_config(&self, name: &str, declared: &QueueConfig) -> QueueConfig {
        let mut cfg = declared.clone();
        if let Some(p) = self.policy_for_queue(name) {
            cfg.apply_policy(&p.definition);
        }
        cfg
    }

    /// Stops every queue actor. Used when the vhost or the broker shuts down.
    pub async fn shutdown(&self, delete_data: bool) {
        let queues: Vec<QueueHandle> = self.topology.load().queues.values().cloned().collect();
        for q in queues {
            if delete_data {
                q.shutdown().await;
            } else {
                q.stop().await;
            }
        }
    }
}

fn route_into<'a>(
    t: &'a Topology,
    ex: &'a Exchange,
    routing_key: &str,
    headers: Option<&FieldTable>,
    out: &mut Vec<QueueHandle>,
    visited: &mut Vec<&'a str>,
) {
    if visited.contains(&ex.name.as_str()) {
        return;
    }
    visited.push(&ex.name);
    let mut dests = Vec::new();
    ex.route(routing_key, headers, &mut dests);
    for d in dests {
        match d {
            Destination::Queue(n) => {
                if let Some(q) = t.queues.get(n) {
                    if !out.contains(q) {
                        out.push(q.clone());
                    }
                }
            }
            Destination::Exchange(n) => {
                if let Some(next) = t.exchanges.get(n) {
                    route_into(t, next, routing_key, headers, out, visited);
                }
            }
        }
    }
}

/// Removes bindings to `dest` everywhere, deleting auto-delete exchanges left without
/// bindings.
fn remove_bindings_to(t: &mut Topology, dest: &Destination) {
    let mut emptied: Vec<String> = Vec::new();
    let names: Vec<String> = t
        .exchanges
        .iter()
        .filter(|(_, e)| e.bindings.iter().any(|b| &b.destination == dest))
        .map(|(n, _)| n.clone())
        .collect();
    for n in names {
        let ex = t.exchanges[&n].clone();
        let bindings: Vec<Binding> = ex
            .bindings
            .iter()
            .filter(|b| &b.destination != dest)
            .cloned()
            .collect();
        if bindings.is_empty() && ex.auto_delete {
            emptied.push(n.clone());
        }
        t.exchanges.insert(n, Arc::new(ex.with_bindings(bindings)));
    }
    for n in emptied {
        remove_exchange(t, &n);
    }
}

fn remove_exchange(t: &mut Topology, name: &str) {
    if t.exchanges.remove(name).is_some() {
        remove_bindings_to(t, &Destination::Exchange(name.to_string()));
    }
}

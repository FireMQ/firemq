//! A RabbitMQ-compatible subset of the management HTTP API.
//!
//! Paths, JSON shapes and status codes follow RabbitMQ 3.13, so `rabbitmqadmin`, the
//! Terraform provider, monitoring scripts and the FireMQ UI all speak to it.

pub mod auth;
pub mod rates;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use axum::extract::{Extension, Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use bytes::Bytes;
use firemq_core::auth::Permission;
use firemq_core::json::{json_to_table, table_to_json};
use firemq_core::{
    BasicProperties, Binding, Broker, BrokerError, ConnectionInfo, Destination, Exchange,
    ExchangeOptions, Limit, Message, Owner, Policy, QueueHandle, QueueOptions, VHost,
};
use serde::Deserialize;
use serde_json::{Value as V, json};

use crate::native::NativeConfig;
use auth::Caller;
use rates::Rates;

/// Shared state of the HTTP listener.
#[derive(Clone)]
pub struct AppState {
    pub broker: Broker,
    pub native: Arc<NativeConfig>,
    pub local: SocketAddr,
    pub tls: bool,
    pub rates: Arc<Rates>,
    /// (protocol, address) of every listener, for the overview.
    pub listeners: Arc<Vec<(String, SocketAddr)>>,
}

/// An API error with RabbitMQ's `{"error", "reason"}` body.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    error: String,
    reason: String,
}

impl ApiError {
    pub fn new(status: StatusCode, error: &str, reason: impl Into<String>) -> ApiError {
        ApiError {
            status,
            error: error.into(),
            reason: reason.into(),
        }
    }

    fn not_found() -> ApiError {
        ApiError::new(StatusCode::NOT_FOUND, "Object Not Found", "Not Found")
    }

    fn bad_request(reason: impl Into<String>) -> ApiError {
        ApiError::new(StatusCode::BAD_REQUEST, "bad_request", reason)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({"error": self.error, "reason": self.reason})),
        )
            .into_response()
    }
}

impl From<BrokerError> for ApiError {
    fn from(e: BrokerError) -> ApiError {
        let status = match e.code {
            404 => StatusCode::NOT_FOUND,
            403 => StatusCode::FORBIDDEN,
            405 | 406 | 503 | 530 => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let error = match status {
            StatusCode::NOT_FOUND => "Object Not Found",
            StatusCode::BAD_REQUEST => "bad_request",
            StatusCode::FORBIDDEN => "access_refused",
            _ => "internal_error",
        };
        ApiError::new(status, error, e.text)
    }
}

type ApiResult<T = Response> = Result<T, ApiError>;

/// Product version reported where RabbitMQ tools expect a RabbitMQ version.
const RABBITMQ_API_VERSION: &str = "3.13.7";

pub fn router(state: AppState) -> Router {
    let api = Router::new()
        .route("/overview", get(overview))
        .route("/cluster-name", get(cluster_name))
        .route("/nodes", get(nodes))
        .route("/nodes/{node}", get(node))
        .route("/extensions", get(|| async { Json(json!([])) }))
        .route("/whoami", get(whoami))
        .route("/vhosts", get(vhosts))
        .route(
            "/vhosts/{vhost}",
            get(vhost_get).put(vhost_put).delete(vhost_delete),
        )
        .route("/vhosts/{vhost}/connections", get(vhost_connections))
        .route("/vhosts/{vhost}/permissions", get(vhost_permissions))
        .route("/users", get(users))
        .route(
            "/users/{user}",
            get(user_get).put(user_put).delete(user_delete),
        )
        .route("/users/{user}/permissions", get(user_permissions))
        .route("/permissions", get(permissions))
        .route(
            "/permissions/{vhost}/{user}",
            get(permission_get)
                .put(permission_put)
                .delete(permission_delete),
        )
        .route("/policies", get(policies))
        .route("/policies/{vhost}", get(policies_vhost))
        .route(
            "/policies/{vhost}/{name}",
            get(policy_get).put(policy_put).delete(policy_delete),
        )
        .route("/exchanges", get(exchanges))
        .route("/exchanges/{vhost}", get(exchanges_vhost))
        .route(
            "/exchanges/{vhost}/{name}",
            get(exchange_get).put(exchange_put).delete(exchange_delete),
        )
        .route("/exchanges/{vhost}/{name}/publish", post(exchange_publish))
        .route(
            "/exchanges/{vhost}/{name}/bindings/source",
            get(exchange_bindings_source),
        )
        .route(
            "/exchanges/{vhost}/{name}/bindings/destination",
            get(exchange_bindings_destination),
        )
        .route("/queues", get(queues))
        .route("/queues/{vhost}", get(queues_vhost))
        .route(
            "/queues/{vhost}/{name}",
            get(queue_get).put(queue_put).delete(queue_delete),
        )
        .route(
            "/queues/{vhost}/{name}/contents",
            axum::routing::delete(queue_purge),
        )
        .route("/queues/{vhost}/{name}/get", post(queue_get_messages))
        .route("/queues/{vhost}/{name}/bindings", get(queue_bindings))
        .route("/bindings", get(bindings))
        .route("/bindings/{vhost}", get(bindings_vhost))
        .route(
            "/bindings/{vhost}/e/{source}/{kind}/{dest}",
            get(binding_list).post(binding_create),
        )
        .route(
            "/bindings/{vhost}/e/{source}/{kind}/{dest}/{props}",
            get(binding_get).delete(binding_delete),
        )
        .route("/connections", get(connections))
        .route(
            "/connections/{name}",
            get(connection_get).delete(connection_delete),
        )
        .route("/channels", get(channels))
        .route("/consumers", get(consumers))
        .route("/consumers/{vhost}", get(consumers_vhost))
        .route("/definitions", get(definitions_get).post(definitions_post))
        .route(
            "/definitions/{vhost}",
            get(definitions_vhost_get).post(definitions_vhost_post),
        )
        .route("/aliveness-test/{vhost}", get(aliveness))
        .route("/health/checks/alarms", get(health_alarms))
        .route("/health/checks/local-alarms", get(health_alarms))
        .route("/health/checks/virtual-hosts", get(health_ok))
        .route("/health/checks/node-is-quorum-critical", get(health_ok))
        .route(
            "/health/checks/node-is-mirror-sync-critical",
            get(health_ok),
        )
        .route("/health/checks/port-listener/{port}", get(health_port))
        .route(
            "/health/checks/protocol-listener/{protocol}",
            get(health_protocol),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_user,
        ))
        .with_state(state);
    Router::new().nest("/api", api)
}

// ------------------------------------------------------------------ helpers

#[derive(Debug, Default, Deserialize)]
struct ListParams {
    columns: Option<String>,
    name: Option<String>,
}

fn pick(v: &V, path: &str) -> Option<V> {
    let mut cur = v;
    for part in path.split('.') {
        cur = cur.get(part)?;
    }
    Some(cur.clone())
}

/// Applies `?columns=a,b.c` and `?name=` filters to a list response.
fn list(items: Vec<V>, p: &ListParams) -> Json<V> {
    let items = items.into_iter().filter(|i| match &p.name {
        Some(n) if !n.is_empty() => i
            .get("name")
            .and_then(V::as_str)
            .is_some_and(|x| x.contains(n.as_str())),
        _ => true,
    });
    let out: Vec<V> = match &p.columns {
        Some(cols) if !cols.is_empty() => items
            .map(|i| {
                let mut o = serde_json::Map::new();
                for c in cols.split(',').map(str::trim).filter(|c| !c.is_empty()) {
                    if let Some(v) = pick(&i, c) {
                        insert_path(&mut o, c, v);
                    }
                }
                V::Object(o)
            })
            .collect(),
        _ => items.collect(),
    };
    Json(V::Array(out))
}

fn insert_path(o: &mut serde_json::Map<String, V>, path: &str, v: V) {
    match path.split_once('.') {
        None => {
            o.insert(path.to_string(), v);
        }
        Some((head, rest)) => {
            let entry = o
                .entry(head.to_string())
                .or_insert_with(|| V::Object(Default::default()));
            if let V::Object(inner) = entry {
                insert_path(inner, rest, v);
            }
        }
    }
}

fn flag(v: &V, key: &str, default: bool) -> bool {
    match v.get(key) {
        Some(V::Bool(b)) => *b,
        Some(V::String(s)) => s == "true",
        _ => default,
    }
}

fn get_vhost(state: &AppState, name: &str) -> ApiResult<Arc<VHost>> {
    state.broker.vhost(name).ok_or_else(ApiError::not_found)
}

fn no_content() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

fn created() -> Response {
    StatusCode::CREATED.into_response()
}

fn node_name(state: &AppState) -> String {
    state.broker.config().node_name.clone()
}

/// The default exchange is called `amq.default` in URLs and `""` in bodies.
fn exchange_url_name(name: &str) -> &str {
    if name == "amq.default" { "" } else { name }
}

fn rate(v: f64) -> V {
    json!({ "rate": (v * 10.0).round() / 10.0 })
}

// ------------------------------------------------------------------ JSON views

fn queue_json(state: &AppState, v: &VHost, q: &QueueHandle) -> V {
    let s = &q.stats;
    let ready = s.ready.load(Ordering::Relaxed);
    let unacked = s.unacked.load(Ordering::Relaxed);
    let r = state.rates.get(&format!("q:{}:{}", v.name, q.name));
    let policy = v.policy_for_queue(&q.name);
    let exclusive_tag = q
        .consumers
        .lock()
        .iter()
        .find(|c| c.exclusive)
        .map(|c| c.tag.clone());
    json!({
        "name": q.name,
        "vhost": v.name,
        "durable": q.durable,
        "auto_delete": q.auto_delete,
        "exclusive": q.exclusive_owner.is_some(),
        "arguments": table_to_json(&q.arguments),
        "node": node_name(state),
        "state": "running",
        "type": "classic",
        "messages": ready + unacked,
        "messages_ready": ready,
        "messages_unacknowledged": unacked,
        "message_bytes_ready": s.ready_bytes.load(Ordering::Relaxed),
        "consumers": s.consumers.load(Ordering::Relaxed),
        "exclusive_consumer_tag": exclusive_tag,
        "policy": policy.as_ref().map(|p| p.name.clone()),
        "effective_policy_definition": policy.map(|p| p.definition).unwrap_or(json!({})),
        "message_stats": {
            "publish": s.published.load(Ordering::Relaxed),
            "publish_details": rate(r[0]),
            "deliver_get": s.delivered.load(Ordering::Relaxed),
            "deliver_get_details": rate(r[1]),
            "ack": s.acked.load(Ordering::Relaxed),
            "ack_details": rate(r[2]),
            "redeliver": s.redelivered.load(Ordering::Relaxed),
            "redeliver_details": rate(r[3]),
        },
    })
}

fn exchange_json(v: &VHost, e: &Exchange) -> V {
    json!({
        "name": e.name,
        "vhost": v.name,
        "type": e.kind.as_str(),
        "durable": e.durable,
        "auto_delete": e.auto_delete,
        "internal": e.internal,
        "arguments": table_to_json(&e.arguments),
        "user_who_performed_action": "",
    })
}

fn properties_key(b: &Binding) -> String {
    let key = if b.routing_key.is_empty() {
        "~".to_string()
    } else {
        b.routing_key.replace('%', "%25").replace('~', "%7E")
    };
    if b.arguments.is_empty() {
        key
    } else {
        use sha2::Digest;
        let digest = sha2::Sha256::digest(table_to_json(&b.arguments).to_string().as_bytes());
        let short: String = digest.iter().take(5).map(|x| format!("{x:02x}")).collect();
        format!("{key}~{short}")
    }
}

fn binding_json(v: &VHost, b: &Binding) -> V {
    json!({
        "source": b.source,
        "vhost": v.name,
        "destination": b.destination.name(),
        "destination_type": b.destination.kind(),
        "routing_key": b.routing_key,
        "arguments": table_to_json(&b.arguments),
        "properties_key": properties_key(b),
    })
}

/// Every binding in a vhost, including the implicit ones from the default exchange.
fn vhost_bindings(v: &VHost) -> Vec<Binding> {
    let t = v.topology();
    let mut queues: Vec<&String> = t.queues.keys().collect();
    queues.sort();
    let mut out: Vec<Binding> = queues
        .into_iter()
        .map(|q| Binding {
            source: String::new(),
            destination: Destination::Queue(q.clone()),
            routing_key: q.clone(),
            arguments: Default::default(),
        })
        .collect();
    let mut exchanges: Vec<_> = t.exchanges.values().collect();
    exchanges.sort_by(|a, b| a.name.cmp(&b.name));
    for e in exchanges {
        out.extend(e.bindings.iter().cloned());
    }
    out
}

fn connection_json(state: &AppState, c: &ConnectionInfo) -> V {
    let r = state.rates.get(&format!("c:{}", c.id));
    let (peer_host, peer_port) = split_addr(&c.peer);
    let (host, port) = split_addr(&c.local);
    json!({
        "name": c.name,
        "node": node_name(state),
        "user": c.user,
        "vhost": c.vhost,
        "protocol": c.protocol,
        "peer_host": peer_host,
        "peer_port": peer_port,
        "host": host,
        "port": port,
        "ssl": c.tls,
        "type": "network",
        "state": "running",
        "connected_at": c.connected_at,
        "channels": c.channels.load(Ordering::Relaxed),
        "recv_oct": c.recv_bytes.load(Ordering::Relaxed),
        "recv_oct_details": rate(r[0]),
        "send_oct": c.send_bytes.load(Ordering::Relaxed),
        "send_oct_details": rate(r[1]),
        "client_properties": c.client_properties,
    })
}

fn split_addr(addr: &str) -> (String, u16) {
    match addr.rsplit_once(':') {
        Some((h, p)) => (
            h.trim_start_matches('[').trim_end_matches(']').to_string(),
            p.parse().unwrap_or(0),
        ),
        None => (addr.to_string(), 0),
    }
}

fn user_json(u: &firemq_core::User) -> V {
    json!({
        "name": u.name,
        "password_hash": u.password_hash,
        "hashing_algorithm": u.hashing_algorithm,
        "tags": u.tags,
        "limits": {},
    })
}

fn permission_json(user: &str, vhost: &str, p: &Permission) -> V {
    json!({"user": user, "vhost": vhost, "configure": p.configure, "write": p.write, "read": p.read})
}

fn policy_json(vhost: &str, p: &Policy) -> V {
    json!({
        "vhost": vhost, "name": p.name, "pattern": p.pattern,
        "apply-to": p.apply_to, "definition": p.definition, "priority": p.priority,
    })
}

// ------------------------------------------------------------------ overview & nodes

async fn overview(State(s): State<AppState>) -> Json<V> {
    let b = &s.broker;
    let (mut queues, mut exchanges, mut consumers) = (0u64, 0u64, 0u64);
    let (mut ready, mut unacked) = (0u64, 0u64);
    for v in b.vhosts() {
        let t = v.topology();
        exchanges += t.exchanges.len() as u64;
        for q in t.queues.values() {
            queues += 1;
            consumers += q.stats.consumers.load(Ordering::Relaxed) as u64;
            ready += q.stats.ready.load(Ordering::Relaxed);
            unacked += q.stats.unacked.load(Ordering::Relaxed);
        }
    }
    let conns = b.connections();
    let channels: u64 = conns
        .iter()
        .map(|c| c.channels.load(Ordering::Relaxed) as u64)
        .sum();
    let c = b.counters();
    let r = s.rates.get("global");
    let listeners: Vec<V> = s
        .listeners
        .iter()
        .map(|(proto, addr)| {
            json!({"node": node_name(&s), "protocol": proto, "ip_address": addr.ip().to_string(), "port": addr.port()})
        })
        .collect();
    let exchange_types: Vec<V> = ["direct", "fanout", "topic", "headers"]
        .iter()
        .map(|t| json!({"name": t, "description": "", "enabled": true}))
        .collect();
    Json(json!({
        "management_version": RABBITMQ_API_VERSION,
        "rates_mode": "basic",
        "product_name": "FireMQ",
        "product_version": env!("CARGO_PKG_VERSION"),
        "rabbitmq_version": RABBITMQ_API_VERSION,
        "cluster_name": node_name(&s),
        "node": node_name(&s),
        "exchange_types": exchange_types,
        "object_totals": {
            "connections": conns.len(),
            "channels": channels,
            "exchanges": exchanges,
            "queues": queues,
            "consumers": consumers,
        },
        "queue_totals": {
            "messages": ready + unacked,
            "messages_ready": ready,
            "messages_unacknowledged": unacked,
        },
        "message_stats": {
            "publish": c.published.load(Ordering::Relaxed),
            "publish_details": rate(r[0]),
            "deliver_get": c.delivered.load(Ordering::Relaxed),
            "deliver_get_details": rate(r[1]),
            "ack": c.acked.load(Ordering::Relaxed),
            "ack_details": rate(r[2]),
            "confirm": c.confirmed.load(Ordering::Relaxed),
            "confirm_details": rate(r[3]),
            "drop_unroutable": c.unroutable.load(Ordering::Relaxed),
        },
        "listeners": listeners,
        "contexts": [{"node": node_name(&s), "description": "FireMQ Management", "path": "/", "port": s.local.port()}],
    }))
}

async fn cluster_name(State(s): State<AppState>) -> Json<V> {
    Json(json!({"name": node_name(&s)}))
}

fn node_json(s: &AppState) -> V {
    let n = s.broker.node_stats();
    let uptime = firemq_core::now_ms().saturating_sub(s.broker.started_at());
    json!({
        "name": node_name(s),
        "type": "disc",
        "running": true,
        "os_pid": std::process::id().to_string(),
        "uptime": uptime,
        "mem_used": n.mem_used.load(Ordering::Relaxed),
        "mem_limit": n.mem_limit.load(Ordering::Relaxed),
        "mem_alarm": n.mem_alarm.load(Ordering::Relaxed),
        "disk_free": n.disk_free.load(Ordering::Relaxed),
        "disk_free_limit": n.disk_limit.load(Ordering::Relaxed),
        "disk_free_alarm": n.disk_alarm.load(Ordering::Relaxed),
        "sockets_used": s.broker.connections().len(),
        "processors": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1),
        "rates_mode": "basic",
        "applications": [{"name": "firemq", "description": "FireMQ", "version": env!("CARGO_PKG_VERSION")}],
    })
}

async fn nodes(State(s): State<AppState>) -> Json<V> {
    Json(json!([node_json(&s)]))
}

async fn node(State(s): State<AppState>, Path(name): Path<String>) -> ApiResult<Json<V>> {
    if name != node_name(&s) {
        return Err(ApiError::not_found());
    }
    Ok(Json(node_json(&s)))
}

async fn whoami(Extension(Caller(u)): Extension<Caller>) -> Json<V> {
    Json(json!({"name": u.name, "tags": u.tags}))
}

// ------------------------------------------------------------------ vhosts

fn vhost_json(v: &VHost) -> V {
    let (mut ready, mut unacked) = (0u64, 0u64);
    for q in v.topology().queues.values() {
        ready += q.stats.ready.load(Ordering::Relaxed);
        unacked += q.stats.unacked.load(Ordering::Relaxed);
    }
    json!({
        "name": v.name, "description": "", "tags": [], "tracing": false,
        "messages": ready + unacked, "messages_ready": ready, "messages_unacknowledged": unacked,
    })
}

async fn vhosts(State(s): State<AppState>, Query(p): Query<ListParams>) -> Json<V> {
    list(
        s.broker.vhosts().iter().map(|v| vhost_json(v)).collect(),
        &p,
    )
}

async fn vhost_get(State(s): State<AppState>, Path(name): Path<String>) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &name)?;
    Ok(Json(vhost_json(&v)))
}

async fn vhost_put(
    State(s): State<AppState>,
    Extension(Caller(caller)): Extension<Caller>,
    Path(name): Path<String>,
) -> Response {
    let existed = s.broker.vhost(&name).is_some();
    s.broker.add_vhost(&name);
    if !existed {
        // RabbitMQ grants the creating user full access to a new vhost.
        let _ = s
            .broker
            .users()
            .set_permission(&caller.name, &name, Permission::all());
        s.broker.mark_definitions_changed();
        created()
    } else {
        no_content()
    }
}

async fn vhost_delete(State(s): State<AppState>, Path(name): Path<String>) -> ApiResult {
    s.broker.delete_vhost(&name).await?;
    Ok(no_content())
}

async fn vhost_connections(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Query(p): Query<ListParams>,
) -> Json<V> {
    let items = s
        .broker
        .connections()
        .iter()
        .filter(|c| c.vhost == name)
        .map(|c| connection_json(&s, c))
        .collect();
    list(items, &p)
}

async fn vhost_permissions(State(s): State<AppState>, Path(name): Path<String>) -> Json<V> {
    let items: Vec<V> = s
        .broker
        .users()
        .list()
        .iter()
        .filter_map(|u| {
            u.permissions
                .get(&name)
                .map(|p| permission_json(&u.name, &name, p))
        })
        .collect();
    Json(V::Array(items))
}

// ------------------------------------------------------------------ users & permissions

async fn users(State(s): State<AppState>, Query(p): Query<ListParams>) -> Json<V> {
    list(
        s.broker
            .users()
            .list()
            .iter()
            .map(|u| user_json(u))
            .collect(),
        &p,
    )
}

async fn user_get(State(s): State<AppState>, Path(name): Path<String>) -> ApiResult<Json<V>> {
    let u = s
        .broker
        .users()
        .get(&name)
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(user_json(&u)))
}

async fn user_put(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<V>,
) -> ApiResult {
    let existed = s.broker.users().get(&name).is_some();
    let tags: Vec<String> = match body.get("tags") {
        Some(V::Array(a)) => a
            .iter()
            .filter_map(|t| t.as_str().map(String::from))
            .collect(),
        Some(V::String(t)) => t
            .split(',')
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .map(String::from)
            .collect(),
        _ => Vec::new(),
    };
    if let Some(pw) = body.get("password").and_then(V::as_str) {
        s.broker.users().put(&name, Some(pw), tags);
    } else if let Some(hash) = body.get("password_hash").and_then(V::as_str) {
        let algo = body
            .get("hashing_algorithm")
            .and_then(V::as_str)
            .unwrap_or(firemq_core::auth::SHA256);
        s.broker
            .users()
            .put_hashed(&name, Some((hash.to_string(), algo.to_string())), tags);
    } else if existed {
        s.broker.users().put(&name, None, tags);
    } else {
        return Err(ApiError::bad_request(
            "a new user needs 'password' or 'password_hash'",
        ));
    }
    s.broker.mark_definitions_changed();
    Ok(if existed { no_content() } else { created() })
}

async fn user_delete(State(s): State<AppState>, Path(name): Path<String>) -> ApiResult {
    if !s.broker.users().delete(&name) {
        return Err(ApiError::not_found());
    }
    s.broker.mark_definitions_changed();
    Ok(no_content())
}

async fn user_permissions(
    State(s): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<V>> {
    let u = s
        .broker
        .users()
        .get(&name)
        .ok_or_else(ApiError::not_found)?;
    let mut items: Vec<V> = u
        .permissions
        .iter()
        .map(|(vh, p)| permission_json(&u.name, vh, p))
        .collect();
    items.sort_by(|a, b| a["vhost"].as_str().cmp(&b["vhost"].as_str()));
    Ok(Json(V::Array(items)))
}

async fn permissions(State(s): State<AppState>) -> Json<V> {
    let mut items = Vec::new();
    for u in s.broker.users().list() {
        let mut vhosts: Vec<_> = u.permissions.iter().collect();
        vhosts.sort_by(|a, b| a.0.cmp(b.0));
        for (vh, p) in vhosts {
            items.push(permission_json(&u.name, vh, p));
        }
    }
    Json(V::Array(items))
}

async fn permission_get(
    State(s): State<AppState>,
    Path((vhost, user)): Path<(String, String)>,
) -> ApiResult<Json<V>> {
    let u = s
        .broker
        .users()
        .get(&user)
        .ok_or_else(ApiError::not_found)?;
    let p = u.permissions.get(&vhost).ok_or_else(ApiError::not_found)?;
    Ok(Json(permission_json(&user, &vhost, p)))
}

async fn permission_put(
    State(s): State<AppState>,
    Path((vhost, user)): Path<(String, String)>,
    Json(body): Json<V>,
) -> ApiResult {
    get_vhost(&s, &vhost)?;
    let field = |k: &str| body.get(k).and_then(V::as_str).unwrap_or("").to_string();
    let p = Permission::new(&field("configure"), &field("write"), &field("read"))?;
    let existed = s
        .broker
        .users()
        .get(&user)
        .ok_or_else(ApiError::not_found)?
        .permissions
        .contains_key(&vhost);
    s.broker.users().set_permission(&user, &vhost, p)?;
    s.broker.mark_definitions_changed();
    Ok(if existed { no_content() } else { created() })
}

async fn permission_delete(
    State(s): State<AppState>,
    Path((vhost, user)): Path<(String, String)>,
) -> ApiResult {
    if !s.broker.users().clear_permission(&user, &vhost) {
        return Err(ApiError::not_found());
    }
    s.broker.mark_definitions_changed();
    Ok(no_content())
}

// ------------------------------------------------------------------ policies

async fn policies(State(s): State<AppState>) -> Json<V> {
    let mut items = Vec::new();
    for v in s.broker.vhosts() {
        items.extend(v.policies().iter().map(|p| policy_json(&v.name, p)));
    }
    Json(V::Array(items))
}

async fn policies_vhost(
    State(s): State<AppState>,
    Path(vhost): Path<String>,
) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    Ok(Json(V::Array(
        v.policies()
            .iter()
            .map(|p| policy_json(&v.name, p))
            .collect(),
    )))
}

async fn policy_get(
    State(s): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    let p = v
        .policies()
        .into_iter()
        .find(|p| p.name == name)
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(policy_json(&vhost, &p)))
}

async fn policy_put(
    State(s): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
    Json(body): Json<V>,
) -> ApiResult {
    let v = get_vhost(&s, &vhost)?;
    let existed = v.policies().iter().any(|p| p.name == name);
    let pattern = body
        .get("pattern")
        .and_then(V::as_str)
        .ok_or_else(|| ApiError::bad_request("'pattern' is required"))?;
    let definition = body
        .get("definition")
        .cloned()
        .ok_or_else(|| ApiError::bad_request("'definition' is required"))?;
    let apply = body.get("apply-to").and_then(V::as_str).unwrap_or("all");
    let priority = body.get("priority").and_then(V::as_i64).unwrap_or(0);
    v.set_policy(Policy::new(&name, pattern, apply, definition, priority)?)
        .await;
    Ok(if existed { no_content() } else { created() })
}

async fn policy_delete(
    State(s): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
) -> ApiResult {
    let v = get_vhost(&s, &vhost)?;
    if !v.clear_policy(&name).await {
        return Err(ApiError::not_found());
    }
    Ok(no_content())
}

// ------------------------------------------------------------------ exchanges

fn all_exchanges(v: &VHost) -> Vec<V> {
    let t = v.topology();
    let mut es: Vec<_> = t.exchanges.values().collect();
    es.sort_by(|a, b| a.name.cmp(&b.name));
    es.into_iter().map(|e| exchange_json(v, e)).collect()
}

async fn exchanges(State(s): State<AppState>, Query(p): Query<ListParams>) -> Json<V> {
    let items = s
        .broker
        .vhosts()
        .iter()
        .flat_map(|v| all_exchanges(v))
        .collect();
    list(items, &p)
}

async fn exchanges_vhost(
    State(s): State<AppState>,
    Path(vhost): Path<String>,
    Query(p): Query<ListParams>,
) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    Ok(list(all_exchanges(&v), &p))
}

async fn exchange_get(
    State(s): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    let e = v
        .exchange(exchange_url_name(&name))
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(exchange_json(&v, &e)))
}

async fn exchange_put(
    State(s): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
    Json(body): Json<V>,
) -> ApiResult {
    let v = get_vhost(&s, &vhost)?;
    let existed = v.exchange(&name).is_some();
    let kind = body
        .get("type")
        .and_then(V::as_str)
        .ok_or_else(|| ApiError::bad_request("'type' is required"))?;
    v.declare_exchange(ExchangeOptions {
        name,
        kind: kind.to_string(),
        durable: flag(&body, "durable", true),
        auto_delete: flag(&body, "auto_delete", false),
        internal: flag(&body, "internal", false),
        passive: false,
        arguments: json_to_table(body.get("arguments").unwrap_or(&V::Null)),
    })?;
    Ok(if existed { no_content() } else { created() })
}

#[derive(Deserialize, Default)]
struct DeleteParams {
    #[serde(rename = "if-unused", default)]
    if_unused: Option<String>,
    #[serde(rename = "if-empty", default)]
    if_empty: Option<String>,
}

async fn exchange_delete(
    State(s): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
    Query(p): Query<DeleteParams>,
) -> ApiResult {
    let v = get_vhost(&s, &vhost)?;
    if v.exchange(&name).is_none() {
        return Err(ApiError::not_found());
    }
    v.delete_exchange(&name, p.if_unused.as_deref() == Some("true"))?;
    Ok(no_content())
}

fn properties_from_json(p: &V) -> BasicProperties {
    let s = |k: &str| p.get(k).and_then(V::as_str).map(String::from);
    let n = |k: &str| p.get(k).and_then(V::as_u64);
    BasicProperties {
        content_type: s("content_type"),
        content_encoding: s("content_encoding"),
        headers: p
            .get("headers")
            .filter(|h| h.as_object().is_some_and(|o| !o.is_empty()))
            .map(json_to_table),
        delivery_mode: n("delivery_mode").map(|d| d as u8),
        priority: n("priority").map(|d| d as u8),
        correlation_id: s("correlation_id"),
        reply_to: s("reply_to"),
        expiration: s("expiration"),
        message_id: s("message_id"),
        timestamp: n("timestamp"),
        kind: s("type"),
        user_id: s("user_id"),
        app_id: s("app_id"),
        cluster_id: None,
    }
}

fn properties_to_json(p: &BasicProperties) -> V {
    let mut o = serde_json::Map::new();
    let mut put = |k: &str, v: Option<V>| {
        if let Some(v) = v {
            o.insert(k.to_string(), v);
        }
    };
    put("content_type", p.content_type.clone().map(V::from));
    put("content_encoding", p.content_encoding.clone().map(V::from));
    put("headers", p.headers.as_ref().map(table_to_json));
    put("delivery_mode", p.delivery_mode.map(V::from));
    put("priority", p.priority.map(V::from));
    put("correlation_id", p.correlation_id.clone().map(V::from));
    put("reply_to", p.reply_to.clone().map(V::from));
    put("expiration", p.expiration.clone().map(V::from));
    put("message_id", p.message_id.clone().map(V::from));
    put("timestamp", p.timestamp.map(V::from));
    put("type", p.kind.clone().map(V::from));
    put("user_id", p.user_id.clone().map(V::from));
    put("app_id", p.app_id.clone().map(V::from));
    V::Object(o)
}

async fn exchange_publish(
    State(s): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
    Json(body): Json<V>,
) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    let exchange = exchange_url_name(&name).to_string();
    let key = body.get("routing_key").and_then(V::as_str).unwrap_or("");
    let payload = body.get("payload").and_then(V::as_str).unwrap_or("");
    let bytes = match body.get("payload_encoding").and_then(V::as_str) {
        Some("base64") => base64::engine::general_purpose::STANDARD
            .decode(payload)
            .map_err(|e| ApiError::bad_request(format!("payload is not base64: {e}")))?,
        _ => payload.as_bytes().to_vec(),
    };
    let props = properties_from_json(body.get("properties").unwrap_or(&V::Null));
    let queues = v.route(&exchange, key, props.headers.as_ref())?;
    let routed = !queues.is_empty();
    let msg = Arc::new(Message::new(exchange, key, props, Bytes::from(bytes)));
    for q in queues {
        q.publish(msg.clone(), None).await;
    }
    s.broker
        .counters()
        .published
        .fetch_add(1, Ordering::Relaxed);
    Ok(Json(json!({"routed": routed})))
}

async fn exchange_bindings_source(
    State(s): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    let name = exchange_url_name(&name).to_string();
    let items = vhost_bindings(&v)
        .iter()
        .filter(|b| b.source == name)
        .map(|b| binding_json(&v, b))
        .collect();
    Ok(Json(V::Array(items)))
}

async fn exchange_bindings_destination(
    State(s): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    let items = vhost_bindings(&v)
        .iter()
        .filter(|b| b.destination == Destination::Exchange(name.clone()))
        .map(|b| binding_json(&v, b))
        .collect();
    Ok(Json(V::Array(items)))
}

// ------------------------------------------------------------------ queues

fn all_queues(s: &AppState, v: &VHost) -> Vec<V> {
    let t = v.topology();
    let mut qs: Vec<_> = t.queues.values().collect();
    qs.sort_by(|a, b| a.name.cmp(&b.name));
    qs.into_iter().map(|q| queue_json(s, v, q)).collect()
}

async fn queues(State(s): State<AppState>, Query(p): Query<ListParams>) -> Json<V> {
    let items = s
        .broker
        .vhosts()
        .iter()
        .flat_map(|v| all_queues(&s, v))
        .collect();
    list(items, &p)
}

async fn queues_vhost(
    State(s): State<AppState>,
    Path(vhost): Path<String>,
    Query(p): Query<ListParams>,
) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    Ok(list(all_queues(&s, &v), &p))
}

fn find_queue(v: &VHost, name: &str) -> ApiResult<QueueHandle> {
    v.topology()
        .queues
        .get(name)
        .cloned()
        .ok_or_else(ApiError::not_found)
}

async fn queue_get(
    State(s): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    let q = find_queue(&v, &name)?;
    let mut j = queue_json(&s, &v, &q);
    j["consumer_details"] = V::Array(consumer_list(&s, &v, Some(&q)));
    Ok(Json(j))
}

async fn queue_put(
    State(s): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
    Json(body): Json<V>,
) -> ApiResult {
    let v = get_vhost(&s, &vhost)?;
    let d = v.declare_queue(
        QueueOptions {
            name,
            durable: flag(&body, "durable", true),
            exclusive: false,
            auto_delete: flag(&body, "auto_delete", false),
            passive: false,
            arguments: json_to_table(body.get("arguments").unwrap_or(&V::Null)),
        },
        None,
    )?;
    Ok(if d.created { created() } else { no_content() })
}

async fn queue_delete(
    State(s): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
    Query(p): Query<DeleteParams>,
) -> ApiResult {
    let v = get_vhost(&s, &vhost)?;
    let q = find_queue(&v, &name)?;
    if q.exclusive_owner.is_some() {
        return Err(ApiError::bad_request(
            "exclusive queues are deleted by their connection",
        ));
    }
    v.delete_queue(
        &name,
        p.if_unused.as_deref() == Some("true"),
        p.if_empty.as_deref() == Some("true"),
        None,
    )
    .await?;
    Ok(no_content())
}

async fn queue_purge(
    State(s): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
) -> ApiResult {
    let v = get_vhost(&s, &vhost)?;
    find_queue(&v, &name)?.purge().await?;
    Ok(no_content())
}

async fn queue_get_messages(
    State(s): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
    Json(body): Json<V>,
) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    let q = find_queue(&v, &name)?;
    let count = body
        .get("count")
        .and_then(V::as_u64)
        .unwrap_or(1)
        .min(10_000);
    let ackmode = body
        .get("ackmode")
        .and_then(V::as_str)
        .unwrap_or("ack_requeue_true");
    let base64_only = body.get("encoding").and_then(V::as_str) == Some("base64");
    let truncate = body.get("truncate").and_then(V::as_u64).map(|t| t as usize);
    let owner = Owner {
        conn: s.broker.next_id(),
        channel: 0,
    };
    let mut seqs = Vec::new();
    let mut out = Vec::new();
    for _ in 0..count {
        let Some(g) = q.get(owner, false).await? else {
            break;
        };
        seqs.push(g.seq);
        let body = &g.message.body;
        let shown = &body[..truncate.unwrap_or(body.len()).min(body.len())];
        let (payload, encoding) = match (base64_only, std::str::from_utf8(shown)) {
            (false, Ok(text)) => (text.to_string(), "string"),
            _ => (
                base64::engine::general_purpose::STANDARD.encode(shown),
                "base64",
            ),
        };
        out.push(json!({
            "payload_bytes": body.len(),
            "redelivered": g.redelivered,
            "exchange": g.message.exchange,
            "routing_key": g.message.routing_key,
            "message_count": g.remaining,
            "properties": properties_to_json(&g.message.properties),
            "payload": payload,
            "payload_encoding": encoding,
        }));
    }
    match ackmode {
        "ack_requeue_false" => q.ack(owner, seqs).await,
        "reject_requeue_false" => q.reject(owner, seqs, false).await,
        _ => q.reject(owner, seqs, true).await,
    }
    Ok(Json(V::Array(out)))
}

async fn queue_bindings(
    State(s): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    find_queue(&v, &name)?;
    let dest = Destination::Queue(name);
    let items = vhost_bindings(&v)
        .iter()
        .filter(|b| b.destination == dest)
        .map(|b| binding_json(&v, b))
        .collect();
    Ok(Json(V::Array(items)))
}

// ------------------------------------------------------------------ bindings

async fn bindings(State(s): State<AppState>) -> Json<V> {
    let mut items = Vec::new();
    for v in s.broker.vhosts() {
        items.extend(vhost_bindings(&v).iter().map(|b| binding_json(&v, b)));
    }
    Json(V::Array(items))
}

async fn bindings_vhost(
    State(s): State<AppState>,
    Path(vhost): Path<String>,
) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    Ok(Json(V::Array(
        vhost_bindings(&v)
            .iter()
            .map(|b| binding_json(&v, b))
            .collect(),
    )))
}

fn destination(kind: &str, dest: String) -> ApiResult<Destination> {
    match kind {
        "q" => Ok(Destination::Queue(dest)),
        "e" => Ok(Destination::Exchange(dest)),
        _ => Err(ApiError::not_found()),
    }
}

async fn binding_list(
    State(s): State<AppState>,
    Path((vhost, source, kind, dest)): Path<(String, String, String, String)>,
) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    let source = exchange_url_name(&source).to_string();
    let dest = destination(&kind, dest)?;
    let items = vhost_bindings(&v)
        .iter()
        .filter(|b| b.source == source && b.destination == dest)
        .map(|b| binding_json(&v, b))
        .collect();
    Ok(Json(V::Array(items)))
}

async fn binding_create(
    State(s): State<AppState>,
    Path((vhost, source, kind, dest)): Path<(String, String, String, String)>,
    Json(body): Json<V>,
) -> ApiResult {
    let v = get_vhost(&s, &vhost)?;
    let dest = destination(&kind, dest)?;
    let key = body.get("routing_key").and_then(V::as_str).unwrap_or("");
    let args = json_to_table(body.get("arguments").unwrap_or(&V::Null));
    let b = Binding {
        source: source.clone(),
        destination: dest.clone(),
        routing_key: key.to_string(),
        arguments: args.clone(),
    };
    v.bind(&source, dest, key, args, None)?;
    let mut r = created();
    if let Ok(loc) = properties_key(&b).parse() {
        r.headers_mut().insert(header::LOCATION, loc);
    }
    Ok(r)
}

fn find_binding(v: &VHost, source: &str, dest: &Destination, props: &str) -> ApiResult<Binding> {
    vhost_bindings(v)
        .into_iter()
        .find(|b| b.source == source && b.destination == *dest && properties_key(b) == props)
        .ok_or_else(ApiError::not_found)
}

async fn binding_get(
    State(s): State<AppState>,
    Path((vhost, source, kind, dest, props)): Path<(String, String, String, String, String)>,
) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    let source = exchange_url_name(&source).to_string();
    let b = find_binding(&v, &source, &destination(&kind, dest)?, &props)?;
    Ok(Json(binding_json(&v, &b)))
}

async fn binding_delete(
    State(s): State<AppState>,
    Path((vhost, source, kind, dest, props)): Path<(String, String, String, String, String)>,
) -> ApiResult {
    let v = get_vhost(&s, &vhost)?;
    let dest = destination(&kind, dest)?;
    let b = find_binding(&v, &source, &dest, &props)?;
    v.unbind(&b.source, b.destination, &b.routing_key, &b.arguments, None)?;
    Ok(no_content())
}

// ------------------------------------------------------------------ connections, channels, consumers

async fn connections(State(s): State<AppState>, Query(p): Query<ListParams>) -> Json<V> {
    list(
        s.broker
            .connections()
            .iter()
            .map(|c| connection_json(&s, c))
            .collect(),
        &p,
    )
}

fn find_connection(s: &AppState, name: &str) -> ApiResult<Arc<ConnectionInfo>> {
    s.broker
        .connections()
        .into_iter()
        .find(|c| c.name == name)
        .ok_or_else(ApiError::not_found)
}

async fn connection_get(State(s): State<AppState>, Path(name): Path<String>) -> ApiResult<Json<V>> {
    let c = find_connection(&s, &name)?;
    Ok(Json(connection_json(&s, &c)))
}

async fn connection_delete(
    State(s): State<AppState>,
    Path(name): Path<String>,
    headers: axum::http::HeaderMap,
) -> ApiResult {
    let c = find_connection(&s, &name)?;
    let reason = headers
        .get("x-reason")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("Closed via management plugin");
    let _ = c.events.send(firemq_core::ConnEvent::Close {
        code: 320,
        reason: format!("CONNECTION_FORCED - {reason}"),
    });
    Ok(no_content())
}

async fn channels(State(s): State<AppState>) -> Json<V> {
    let mut items = Vec::new();
    for c in s.broker.connections() {
        let (peer_host, peer_port) = split_addr(&c.peer);
        for n in 1..=c.channels.load(Ordering::Relaxed) {
            items.push(json!({
                "name": format!("{} ({n})", c.name),
                "number": n,
                "user": c.user,
                "vhost": c.vhost,
                "node": node_name(&s),
                "state": "running",
                "connection_details": {"name": c.name, "peer_host": peer_host, "peer_port": peer_port},
            }));
        }
    }
    Json(V::Array(items))
}

fn consumer_list(s: &AppState, v: &VHost, only: Option<&QueueHandle>) -> Vec<V> {
    let conns = s.broker.connections();
    let mut out = Vec::new();
    let t = v.topology();
    let mut qs: Vec<_> = t.queues.values().collect();
    qs.sort_by(|a, b| a.name.cmp(&b.name));
    for q in qs {
        if only.is_some_and(|o| o != q) {
            continue;
        }
        for c in q.consumers.lock().iter() {
            let conn = conns.iter().find(|x| x.id == c.owner.conn);
            let (peer_host, peer_port) = conn.map(|x| split_addr(&x.peer)).unwrap_or_default();
            let prefetch = match c.limit {
                Limit::Prefetch(n) | Limit::Credit(n) => n,
            };
            out.push(json!({
                "consumer_tag": c.tag,
                "queue": {"name": q.name, "vhost": v.name},
                "channel_details": {
                    "name": conn.map(|x| format!("{} ({})", x.name, c.owner.channel)).unwrap_or_default(),
                    "connection_name": conn.map(|x| x.name.clone()).unwrap_or_default(),
                    "peer_host": peer_host,
                    "peer_port": peer_port,
                    "number": c.owner.channel,
                    "user": conn.map(|x| x.user.clone()).unwrap_or_default(),
                    "node": node_name(s),
                },
                "ack_required": !c.no_ack,
                "exclusive": c.exclusive,
                "prefetch_count": prefetch,
                "active": c.active,
                "activity_status": if c.active { "up" } else { "waiting" },
                "arguments": table_to_json(&c.arguments),
            }));
        }
    }
    out
}

async fn consumers(State(s): State<AppState>) -> Json<V> {
    let mut items = Vec::new();
    for v in s.broker.vhosts() {
        items.extend(consumer_list(&s, &v, None));
    }
    Json(V::Array(items))
}

async fn consumers_vhost(
    State(s): State<AppState>,
    Path(vhost): Path<String>,
) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    Ok(Json(V::Array(consumer_list(&s, &v, None))))
}

// ------------------------------------------------------------------ definitions

async fn definitions_get(State(s): State<AppState>) -> Json<V> {
    Json(s.broker.export_definitions())
}

async fn definitions_post(State(s): State<AppState>, Json(body): Json<V>) -> ApiResult {
    s.broker.import_definitions(&body).await?;
    Ok(no_content())
}

async fn definitions_vhost_get(
    State(s): State<AppState>,
    Path(vhost): Path<String>,
) -> ApiResult<Json<V>> {
    get_vhost(&s, &vhost)?;
    let all = s.broker.export_definitions();
    let mut out = serde_json::Map::new();
    for k in [
        "rabbit_version",
        "rabbitmq_version",
        "product_name",
        "product_version",
    ] {
        if let Some(v) = all.get(k) {
            out.insert(k.into(), v.clone());
        }
    }
    for k in ["policies", "queues", "exchanges", "bindings"] {
        let items: Vec<V> = all[k]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter(|i| i["vhost"] == vhost)
                    .map(|i| {
                        let mut i = i.clone();
                        if let Some(o) = i.as_object_mut() {
                            o.remove("vhost");
                        }
                        i
                    })
                    .collect()
            })
            .unwrap_or_default();
        out.insert(k.into(), V::Array(items));
    }
    out.insert("parameters".into(), json!([]));
    Ok(Json(V::Object(out)))
}

async fn definitions_vhost_post(
    State(s): State<AppState>,
    Path(vhost): Path<String>,
    Json(mut body): Json<V>,
) -> ApiResult {
    get_vhost(&s, &vhost)?;
    if let Some(o) = body.as_object_mut() {
        o.remove("users");
        o.remove("vhosts");
        o.remove("permissions");
        for k in ["policies", "queues", "exchanges", "bindings"] {
            if let Some(V::Array(items)) = o.get_mut(k) {
                for i in items {
                    i["vhost"] = V::from(vhost.clone());
                }
            }
        }
    }
    s.broker.import_definitions(&body).await?;
    Ok(no_content())
}

// ------------------------------------------------------------------ health

async fn aliveness(State(s): State<AppState>, Path(vhost): Path<String>) -> ApiResult<Json<V>> {
    let v = get_vhost(&s, &vhost)?;
    let q = v
        .declare_queue(
            QueueOptions {
                name: "aliveness-test".into(),
                ..Default::default()
            },
            None,
        )?
        .queue;
    let msg = Arc::new(Message::new(
        "",
        "aliveness-test",
        BasicProperties::default(),
        Bytes::from_static(b"test_message"),
    ));
    q.publish(msg, None).await;
    let owner = Owner {
        conn: s.broker.next_id(),
        channel: 0,
    };
    match q.get(owner, true).await? {
        Some(_) => Ok(Json(json!({"status": "ok"}))),
        None => Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "failed",
            "aliveness test message was not delivered",
        )),
    }
}

async fn health_ok() -> Json<V> {
    Json(json!({"status": "ok"}))
}

async fn health_alarms(State(s): State<AppState>) -> Response {
    let n = s.broker.node_stats();
    let mut alarms = Vec::new();
    if n.mem_alarm.load(Ordering::Relaxed) {
        alarms.push("memory");
    }
    if n.disk_alarm.load(Ordering::Relaxed) {
        alarms.push("disk");
    }
    if alarms.is_empty() {
        Json(json!({"status": "ok"})).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "failed", "reason": format!("resource alarm(s) in effect: {}", alarms.join(", ")), "alarms": alarms})),
        )
            .into_response()
    }
}

async fn health_port(State(s): State<AppState>, Path(port): Path<u16>) -> Response {
    if s.listeners.iter().any(|(_, a)| a.port() == port) {
        Json(json!({"status": "ok", "port": port})).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "failed", "reason": format!("no listener on port {port}")})),
        )
            .into_response()
    }
}

async fn health_protocol(State(s): State<AppState>, Path(protocol): Path<String>) -> Response {
    if s.listeners.iter().any(|(p, _)| *p == protocol) {
        Json(json!({"status": "ok", "protocol": protocol})).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "failed", "reason": format!("no {protocol} listener")})),
        )
            .into_response()
    }
}

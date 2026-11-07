//! RabbitMQ-compatible definitions: export, import and recovery at startup.

use std::time::Duration;

use serde_json::{Value as Json, json};

use crate::auth::{Permission, SHA256};
use crate::broker::Broker;
use crate::error::{BrokerError, Result};
use crate::exchange::{Destination, ExchangeOptions};
use crate::json::{json_to_table, table_to_json};
use crate::queue::QueueOptions;
use crate::vhost::Policy;

/// What a recovery found.
#[derive(Debug, Default, Clone)]
pub struct RecoveryReport {
    pub vhosts: usize,
    pub users: usize,
    pub queues: usize,
    pub messages: usize,
}

fn builtin_exchange(name: &str) -> bool {
    name.is_empty() || name.starts_with("amq.")
}

fn s<'a>(v: &'a Json, key: &str) -> &'a str {
    v.get(key).and_then(Json::as_str).unwrap_or_default()
}

fn b(v: &Json, key: &str, default: bool) -> bool {
    v.get(key).and_then(Json::as_bool).unwrap_or(default)
}

fn ctx(what: &str, name: &str, e: BrokerError) -> BrokerError {
    BrokerError::new(e.code, format!("{} (importing {what} '{name}')", e.text))
}

impl Broker {
    /// Exports users, vhosts, permissions, policies and durable topology in RabbitMQ's
    /// definitions format.
    pub fn export_definitions(&self) -> Json {
        let users = self.users().list();
        let mut permissions = Vec::new();
        for u in &users {
            let mut vhosts: Vec<_> = u.permissions.iter().collect();
            vhosts.sort_by(|a, b| a.0.cmp(b.0));
            for (vhost, p) in vhosts {
                permissions.push(json!({
                    "user": u.name, "vhost": vhost,
                    "configure": p.configure, "write": p.write, "read": p.read,
                }));
            }
        }
        let mut vhosts = Vec::new();
        let mut policies = Vec::new();
        let mut queues = Vec::new();
        let mut exchanges = Vec::new();
        let mut bindings = Vec::new();
        for v in self.vhosts() {
            vhosts.push(json!({
                "name": v.name, "description": "", "tags": [],
                "metadata": {"description": "", "tags": []},
            }));
            for p in v.policies() {
                policies.push(json!({
                    "vhost": v.name, "name": p.name, "pattern": p.pattern,
                    "apply-to": p.apply_to, "definition": p.definition, "priority": p.priority,
                }));
            }
            let t = v.topology();
            let mut qs: Vec<_> = t
                .queues
                .values()
                .filter(|q| q.durable && q.exclusive_owner.is_none())
                .collect();
            qs.sort_by(|a, b| a.name.cmp(&b.name));
            for q in &qs {
                queues.push(json!({
                    "name": q.name, "vhost": v.name, "durable": true,
                    "auto_delete": q.auto_delete, "arguments": table_to_json(&q.arguments),
                }));
            }
            let durable_queue = |n: &str| qs.iter().any(|q| q.name == n);
            let mut es: Vec<_> = t.exchanges.values().collect();
            es.sort_by(|a, b| a.name.cmp(&b.name));
            for e in &es {
                if e.durable && !builtin_exchange(&e.name) {
                    exchanges.push(json!({
                        "name": e.name, "vhost": v.name, "type": e.kind.as_str(),
                        "durable": true, "auto_delete": e.auto_delete, "internal": e.internal,
                        "arguments": table_to_json(&e.arguments),
                    }));
                }
            }
            for e in &es {
                if !e.durable || e.name.is_empty() {
                    continue;
                }
                for bd in &e.bindings {
                    let keep = match &bd.destination {
                        Destination::Queue(n) => durable_queue(n),
                        Destination::Exchange(n) => t.exchanges.get(n).is_some_and(|x| x.durable),
                    };
                    if keep {
                        bindings.push(json!({
                            "source": e.name, "vhost": v.name,
                            "destination": bd.destination.name(),
                            "destination_type": bd.destination.kind(),
                            "routing_key": bd.routing_key,
                            "arguments": table_to_json(&bd.arguments),
                        }));
                    }
                }
            }
        }
        let users: Vec<Json> = users
            .iter()
            .map(|u| {
                json!({
                    "name": u.name, "password_hash": u.password_hash,
                    "hashing_algorithm": u.hashing_algorithm, "tags": u.tags, "limits": {},
                })
            })
            .collect();
        json!({
            "rabbit_version": "3.13.7",
            "rabbitmq_version": "3.13.7",
            "product_name": "FireMQ",
            "product_version": env!("CARGO_PKG_VERSION"),
            "users": users,
            "vhosts": vhosts,
            "permissions": permissions,
            "topic_permissions": [],
            "parameters": [],
            "global_parameters": [],
            "policies": policies,
            "queues": queues,
            "exchanges": exchanges,
            "bindings": bindings,
        })
    }

    /// Imports definitions (for example RabbitMQ's `definitions.json`), declaring what is
    /// missing. Existing objects with different properties make the import fail.
    pub async fn import_definitions(&self, defs: &Json) -> Result<()> {
        self.import(defs, false).await.map(|_| ())
    }

    async fn import(&self, defs: &Json, restore: bool) -> Result<RecoveryReport> {
        let mut report = RecoveryReport::default();
        let list = |k: &str| {
            defs.get(k)
                .and_then(Json::as_array)
                .cloned()
                .unwrap_or_default()
        };
        for v in list("vhosts") {
            self.add_vhost(s(&v, "name"));
            report.vhosts += 1;
        }
        for u in list("users") {
            let name = s(&u, "name");
            let tags: Vec<String> = match u.get("tags") {
                Some(Json::Array(a)) => a
                    .iter()
                    .filter_map(|t| t.as_str().map(String::from))
                    .collect(),
                Some(Json::String(t)) => t
                    .split(',')
                    .map(str::trim)
                    .filter(|t| !t.is_empty())
                    .map(String::from)
                    .collect(),
                _ => Vec::new(),
            };
            if let Some(pw) = u.get("password").and_then(Json::as_str) {
                self.users().put(name, Some(pw), tags);
            } else {
                let hash = s(&u, "password_hash").to_string();
                let algo = match s(&u, "hashing_algorithm") {
                    "" => SHA256.to_string(),
                    a => a.to_string(),
                };
                self.users().put_hashed(name, Some((hash, algo)), tags);
            }
            report.users += 1;
        }
        for p in list("permissions") {
            let user = s(&p, "user");
            let vhost = s(&p, "vhost");
            if self.vhost(vhost).is_none() {
                continue;
            }
            let perm = Permission::new(s(&p, "configure"), s(&p, "write"), s(&p, "read"))
                .map_err(|e| ctx("permission for", user, e))?;
            self.users()
                .set_permission(user, vhost, perm)
                .map_err(|e| ctx("permission for", user, e))?;
        }
        for p in list("policies") {
            let Some(v) = self.vhost(s(&p, "vhost")) else {
                continue;
            };
            let apply = match s(&p, "apply-to") {
                "" => "all",
                a => a,
            };
            let policy = Policy::new(
                s(&p, "name"),
                s(&p, "pattern"),
                apply,
                p.get("definition").cloned().unwrap_or(json!({})),
                p.get("priority").and_then(Json::as_i64).unwrap_or(0),
            )
            .map_err(|e| ctx("policy", s(&p, "name"), e))?;
            v.set_policy(policy).await;
        }
        for e in list("exchanges") {
            let name = s(&e, "name");
            if builtin_exchange(name) {
                continue;
            }
            let Some(v) = self.vhost(s(&e, "vhost")) else {
                continue;
            };
            v.declare_exchange(ExchangeOptions {
                name: name.to_string(),
                kind: s(&e, "type").to_string(),
                durable: b(&e, "durable", true),
                auto_delete: b(&e, "auto_delete", false),
                internal: b(&e, "internal", false),
                passive: false,
                arguments: json_to_table(e.get("arguments").unwrap_or(&Json::Null)),
            })
            .map_err(|err| ctx("exchange", name, err))?;
        }
        let mut queues = list("queues");
        if restore {
            // Queues with data on disk but missing from the definitions (a crash right after
            // a declare) are restored too.
            if let Ok(stored) = self.store().stored_queues() {
                for m in stored {
                    let known = queues
                        .iter()
                        .any(|q| s(q, "name") == s(&m, "name") && s(q, "vhost") == s(&m, "vhost"));
                    if !known {
                        queues.push(m);
                    }
                }
            }
        }
        for q in queues {
            let name = s(&q, "name").to_string();
            let vhost_name = s(&q, "vhost");
            let v = if restore {
                self.add_vhost(vhost_name)
            } else {
                match self.vhost(vhost_name) {
                    Some(v) => v,
                    None => continue,
                }
            };
            let opts = QueueOptions {
                name: name.clone(),
                durable: b(&q, "durable", true),
                exclusive: false,
                auto_delete: b(&q, "auto_delete", false),
                passive: false,
                arguments: json_to_table(q.get("arguments").unwrap_or(&Json::Null)),
            };
            if restore && opts.durable {
                let (_, n) = v.restore_queue(opts).map_err(|e| ctx("queue", &name, e))?;
                report.messages += n;
            } else {
                v.declare_queue(opts, None)
                    .map_err(|e| ctx("queue", &name, e))?;
            }
            report.queues += 1;
        }
        for bd in list("bindings") {
            let Some(v) = self.vhost(s(&bd, "vhost")) else {
                continue;
            };
            let dest = match s(&bd, "destination_type") {
                "exchange" => Destination::Exchange(s(&bd, "destination").to_string()),
                _ => Destination::Queue(s(&bd, "destination").to_string()),
            };
            let source = s(&bd, "source");
            if source.is_empty() {
                continue;
            }
            v.bind(
                source,
                dest,
                s(&bd, "routing_key"),
                json_to_table(bd.get("arguments").unwrap_or(&Json::Null)),
                None,
            )
            .map_err(|e| ctx("binding from", source, e))?;
        }
        Ok(report)
    }

    /// Restores users, topology and durable messages from the store. Call once at startup,
    /// before accepting connections.
    pub async fn recover(&self) -> Result<RecoveryReport> {
        let defs = self
            .store()
            .load_definitions()
            .map_err(|e| BrokerError::internal(format!("cannot read definitions: {e}")))?
            .unwrap_or_else(|| json!({}));
        self.import(&defs, true).await
    }

    /// Saves definitions now.
    pub fn save_definitions(&self) -> std::io::Result<()> {
        self.store().save_definitions(&self.export_definitions())
    }

    /// Spawns a task that saves definitions whenever durable state changes.
    pub fn spawn_definitions_writer(&self) -> tokio::task::JoinHandle<()> {
        let broker = self.clone();
        tokio::spawn(async move {
            loop {
                broker.definitions_changed().await;
                // Coalesce bursts of declarations into one write.
                tokio::time::sleep(Duration::from_millis(50)).await;
                let b = broker.clone();
                match tokio::task::spawn_blocking(move || b.save_definitions()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => tracing::error!(error = %e, "failed to save definitions"),
                    Err(e) => tracing::error!(error = %e, "definitions writer panicked"),
                }
            }
        })
    }
}

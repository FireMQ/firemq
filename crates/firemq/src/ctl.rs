//! `firemq ctl`: manage a running broker through its HTTP API.

use std::io::Read;

use base64::Engine;
use clap::{Args, Subcommand, ValueEnum};
use serde_json::{Value, json};

#[derive(Args, Debug)]
pub struct CtlArgs {
    /// Management API URL.
    #[arg(
        long,
        global = true,
        env = "FIREMQ_HTTP_URL",
        default_value = "http://localhost:15672"
    )]
    pub url: String,
    #[arg(
        long,
        short,
        global = true,
        env = "FIREMQ_USER",
        default_value = "guest"
    )]
    pub user: String,
    #[arg(
        long,
        short,
        global = true,
        env = "FIREMQ_PASS",
        default_value = "guest"
    )]
    pub pass: String,
    /// Virtual host to act on.
    #[arg(long, global = true, env = "FIREMQ_VHOST", default_value = "/")]
    pub vhost: String,
    /// Print raw JSON instead of tables.
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub cmd: CtlCommand,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum Kind {
    Queues,
    Exchanges,
    Bindings,
    Connections,
    Consumers,
    Users,
    Vhosts,
    Policies,
    Permissions,
}

#[derive(Subcommand, Debug)]
pub enum CtlCommand {
    /// Broker totals and rates.
    Overview,
    /// List objects.
    List {
        kind: Kind,
    },
    /// Declare a queue.
    DeclareQueue {
        name: String,
        /// Create a transient queue instead of a durable one.
        #[arg(long)]
        transient: bool,
        /// Queue argument such as x-message-ttl=60000 (repeatable).
        #[arg(long = "arg", value_name = "KEY=VALUE")]
        args: Vec<String>,
    },
    /// Declare an exchange.
    DeclareExchange {
        name: String,
        #[arg(value_parser = ["direct", "fanout", "topic", "headers"])]
        kind: String,
        #[arg(long)]
        transient: bool,
    },
    /// Bind a queue (or, with --to-exchange, an exchange) to an exchange.
    Bind {
        exchange: String,
        destination: String,
        #[arg(long, short, default_value = "")]
        key: String,
        /// The destination is an exchange.
        #[arg(long)]
        to_exchange: bool,
    },
    /// Delete a queue.
    DeleteQueue {
        name: String,
    },
    /// Delete an exchange.
    DeleteExchange {
        name: String,
    },
    /// Remove every ready message from a queue.
    Purge {
        queue: String,
    },
    /// Show messages from a queue without removing them (use --take to remove).
    Get {
        queue: String,
        #[arg(long, short, default_value_t = 1)]
        count: u32,
        /// Acknowledge (remove) the messages.
        #[arg(long)]
        take: bool,
    },
    /// Create or update a user.
    AddUser {
        name: String,
        password: String,
        /// Comma-separated tags: administrator, management, monitoring, policymaker.
        #[arg(long, default_value = "")]
        tags: String,
    },
    DeleteUser {
        name: String,
    },
    /// Grant a user configure, write and read access to the vhost.
    SetPermissions {
        #[arg(value_name = "USER")]
        target: String,
        #[arg(long, default_value = ".*")]
        configure: String,
        #[arg(long, default_value = ".*")]
        write: String,
        #[arg(long, default_value = ".*")]
        read: String,
    },
    AddVhost {
        name: String,
    },
    DeleteVhost {
        name: String,
    },
    /// Set a policy, for example: set-policy ttl '^tmp\.' '{"message-ttl":60000}'
    SetPolicy {
        name: String,
        pattern: String,
        definition: String,
        #[arg(long, default_value = "queues")]
        apply_to: String,
        #[arg(long, default_value_t = 0)]
        priority: i64,
    },
    ClearPolicy {
        name: String,
    },
    /// Close a connection by name.
    CloseConnection {
        name: String,
    },
    /// Write definitions (users, vhosts, topology, policies) to a file or stdout.
    Export {
        file: Option<String>,
    },
    /// Import definitions, for example a RabbitMQ export. Reads stdin when no file is given.
    Import {
        file: Option<String>,
    },
}

struct Api<'a> {
    a: &'a CtlArgs,
    agent: ureq::Agent,
}

fn enc(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

impl Api<'_> {
    fn call(&self, method: &str, path: &str, body: Option<Value>) -> Result<Value, String> {
        let url = format!("{}/api{}", self.a.url.trim_end_matches('/'), path);
        let auth = base64::engine::general_purpose::STANDARD
            .encode(format!("{}:{}", self.a.user, self.a.pass));
        let req = ureq::http::Request::builder()
            .method(method)
            .uri(&url)
            .header("Authorization", format!("Basic {auth}"));
        let result = match body {
            Some(b) => self.agent.run(
                req.header("Content-Type", "application/json")
                    .body(b.to_string())
                    .map_err(|e| e.to_string())?,
            ),
            None => self.agent.run(req.body(()).map_err(|e| e.to_string())?),
        };
        let mut resp = result.map_err(|e| {
            format!(
                "cannot reach {}: {e}. Is FireMQ running? Start it with `firemq`.",
                self.a.url
            )
        })?;
        let status = resp.status().as_u16();
        let text = resp.body_mut().read_to_string().unwrap_or_default();
        let value: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        if status >= 400 {
            let reason = value["reason"].as_str().unwrap_or(&text).to_string();
            return Err(match status {
                401 => format!("login failed for user '{}': {reason}", self.a.user),
                _ => format!("{status}: {reason}"),
            });
        }
        Ok(value)
    }
}

fn cell(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Array(a) => a.iter().map(cell).collect::<Vec<_>>().join(","),
        other => other.to_string(),
    }
}

fn get_path(v: &Value, path: &str) -> Value {
    let mut cur = v;
    for p in path.split('.') {
        cur = &cur[p];
    }
    cur.clone()
}

/// Prints rows as an aligned table.
pub fn print_table(rows: &[Value], cols: &[(&str, &str)]) {
    let mut grid: Vec<Vec<String>> = vec![cols.iter().map(|(h, _)| h.to_string()).collect()];
    for r in rows {
        grid.push(cols.iter().map(|(_, p)| cell(&get_path(r, p))).collect());
    }
    let widths: Vec<usize> = (0..cols.len())
        .map(|i| {
            grid.iter()
                .map(|row| row[i].chars().count())
                .max()
                .unwrap_or(0)
        })
        .collect();
    for row in grid {
        let line: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{c:<w$}", w = widths[i]))
            .collect();
        println!("{}", line.join("  ").trim_end());
    }
}

fn columns(kind: Kind) -> (&'static str, Vec<(&'static str, &'static str)>, bool) {
    match kind {
        Kind::Queues => (
            "/queues",
            vec![
                ("NAME", "name"),
                ("DURABLE", "durable"),
                ("READY", "messages_ready"),
                ("UNACKED", "messages_unacknowledged"),
                ("CONSUMERS", "consumers"),
                ("POLICY", "policy"),
            ],
            true,
        ),
        Kind::Exchanges => (
            "/exchanges",
            vec![("NAME", "name"), ("TYPE", "type"), ("DURABLE", "durable")],
            true,
        ),
        Kind::Bindings => (
            "/bindings",
            vec![
                ("SOURCE", "source"),
                ("DESTINATION", "destination"),
                ("TYPE", "destination_type"),
                ("ROUTING KEY", "routing_key"),
            ],
            true,
        ),
        Kind::Connections => (
            "/vhosts/{vhost}/connections",
            vec![
                ("NAME", "name"),
                ("USER", "user"),
                ("PROTOCOL", "protocol"),
                ("CHANNELS", "channels"),
            ],
            false,
        ),
        Kind::Consumers => (
            "/consumers",
            vec![
                ("QUEUE", "queue.name"),
                ("TAG", "consumer_tag"),
                ("PREFETCH", "prefetch_count"),
                ("ACK", "ack_required"),
            ],
            true,
        ),
        Kind::Users => ("/users", vec![("NAME", "name"), ("TAGS", "tags")], false),
        Kind::Vhosts => (
            "/vhosts",
            vec![("NAME", "name"), ("MESSAGES", "messages")],
            false,
        ),
        Kind::Policies => (
            "/policies",
            vec![
                ("NAME", "name"),
                ("PATTERN", "pattern"),
                ("APPLY TO", "apply-to"),
                ("DEFINITION", "definition"),
                ("PRIORITY", "priority"),
            ],
            true,
        ),
        Kind::Permissions => (
            "/permissions",
            vec![
                ("USER", "user"),
                ("VHOST", "vhost"),
                ("CONFIGURE", "configure"),
                ("WRITE", "write"),
                ("READ", "read"),
            ],
            false,
        ),
    }
}

fn parse_arg(kv: &str) -> Result<(String, Value), String> {
    let (k, v) = kv
        .split_once('=')
        .ok_or_else(|| format!("expected KEY=VALUE, got '{kv}'"))?;
    let value = serde_json::from_str(v).unwrap_or_else(|_| Value::String(v.to_string()));
    Ok((k.to_string(), value))
}

pub fn run(a: CtlArgs) -> Result<(), String> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    let api = Api { a: &a, agent };
    let vh = enc(&a.vhost);
    let done = |msg: String| {
        println!("{msg}");
        Ok(())
    };
    match &a.cmd {
        CtlCommand::Overview => {
            let o = api.call("GET", "/overview", None)?;
            if a.json {
                println!("{}", serde_json::to_string_pretty(&o).unwrap_or_default());
                return Ok(());
            }
            let t = &o["object_totals"];
            let q = &o["queue_totals"];
            let m = &o["message_stats"];
            println!(
                "{} ({} {})",
                cell(&o["node"]),
                cell(&o["product_name"]),
                cell(&o["product_version"])
            );
            println!(
                "connections {}  channels {}  queues {}  consumers {}  exchanges {}",
                t["connections"], t["channels"], t["queues"], t["consumers"], t["exchanges"]
            );
            println!(
                "messages {}  ready {}  unacked {}",
                q["messages"], q["messages_ready"], q["messages_unacknowledged"]
            );
            println!(
                "publish {}/s  deliver {}/s  ack {}/s",
                m["publish_details"]["rate"],
                m["deliver_get_details"]["rate"],
                m["ack_details"]["rate"]
            );
            Ok(())
        }
        CtlCommand::List { kind } => {
            let (path, cols, per_vhost) = columns(*kind);
            let path = if per_vhost {
                format!("{path}/{vh}")
            } else {
                path.replace("{vhost}", &vh)
            };
            let v = api.call("GET", &path, None)?;
            if a.json {
                println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
            } else {
                let rows = v.as_array().cloned().unwrap_or_default();
                print_table(&rows, &cols);
            }
            Ok(())
        }
        CtlCommand::DeclareQueue {
            name,
            transient,
            args,
        } => {
            let mut arguments = serde_json::Map::new();
            for kv in args {
                let (k, v) = parse_arg(kv)?;
                arguments.insert(k, v);
            }
            api.call(
                "PUT",
                &format!("/queues/{vh}/{}", enc(name)),
                Some(json!({"durable": !transient, "arguments": arguments})),
            )?;
            done(format!("queue '{name}' declared"))
        }
        CtlCommand::DeclareExchange {
            name,
            kind,
            transient,
        } => {
            api.call(
                "PUT",
                &format!("/exchanges/{vh}/{}", enc(name)),
                Some(json!({"type": kind, "durable": !transient})),
            )?;
            done(format!("exchange '{name}' ({kind}) declared"))
        }
        CtlCommand::Bind {
            exchange,
            destination,
            key,
            to_exchange,
        } => {
            let kind = if *to_exchange { "e" } else { "q" };
            api.call(
                "POST",
                &format!(
                    "/bindings/{vh}/e/{}/{kind}/{}",
                    enc(exchange),
                    enc(destination)
                ),
                Some(json!({"routing_key": key, "arguments": {}})),
            )?;
            done(format!(
                "bound '{destination}' to '{exchange}' with key '{key}'"
            ))
        }
        CtlCommand::DeleteQueue { name } => {
            api.call("DELETE", &format!("/queues/{vh}/{}", enc(name)), None)?;
            done(format!("queue '{name}' deleted"))
        }
        CtlCommand::DeleteExchange { name } => {
            api.call("DELETE", &format!("/exchanges/{vh}/{}", enc(name)), None)?;
            done(format!("exchange '{name}' deleted"))
        }
        CtlCommand::Purge { queue } => {
            api.call(
                "DELETE",
                &format!("/queues/{vh}/{}/contents", enc(queue)),
                None,
            )?;
            done(format!("queue '{queue}' purged"))
        }
        CtlCommand::Get { queue, count, take } => {
            let mode = if *take {
                "ack_requeue_false"
            } else {
                "ack_requeue_true"
            };
            let msgs = api.call(
                "POST",
                &format!("/queues/{vh}/{}/get", enc(queue)),
                Some(json!({"count": count, "ackmode": mode, "encoding": "auto"})),
            )?;
            if a.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&msgs).unwrap_or_default()
                );
                return Ok(());
            }
            let list = msgs.as_array().cloned().unwrap_or_default();
            if list.is_empty() {
                println!("queue '{queue}' is empty");
            }
            for m in list {
                let exchange = match cell(&m["exchange"]) {
                    e if e.is_empty() => "(default)".to_string(),
                    e => e,
                };
                println!(
                    "[{exchange} {}] {}",
                    cell(&m["routing_key"]),
                    cell(&m["payload"])
                );
            }
            Ok(())
        }
        CtlCommand::AddUser {
            name,
            password,
            tags,
        } => {
            api.call(
                "PUT",
                &format!("/users/{}", enc(name)),
                Some(json!({"password": password, "tags": tags})),
            )?;
            done(format!(
                "user '{name}' saved; grant access with `firemq ctl set-permissions {name}`"
            ))
        }
        CtlCommand::DeleteUser { name } => {
            api.call("DELETE", &format!("/users/{}", enc(name)), None)?;
            done(format!("user '{name}' deleted"))
        }
        CtlCommand::SetPermissions {
            target: user,
            configure,
            write,
            read,
        } => {
            api.call(
                "PUT",
                &format!("/permissions/{vh}/{}", enc(user)),
                Some(json!({"configure": configure, "write": write, "read": read})),
            )?;
            done(format!(
                "permissions for '{user}' on vhost '{}' set",
                a.vhost
            ))
        }
        CtlCommand::AddVhost { name } => {
            api.call("PUT", &format!("/vhosts/{}", enc(name)), None)?;
            done(format!("vhost '{name}' added"))
        }
        CtlCommand::DeleteVhost { name } => {
            api.call("DELETE", &format!("/vhosts/{}", enc(name)), None)?;
            done(format!("vhost '{name}' deleted"))
        }
        CtlCommand::SetPolicy {
            name,
            pattern,
            definition,
            apply_to,
            priority,
        } => {
            let def: Value = serde_json::from_str(definition)
                .map_err(|e| format!("definition is not JSON: {e}"))?;
            api.call(
                "PUT",
                &format!("/policies/{vh}/{}", enc(name)),
                Some(json!({"pattern": pattern, "definition": def, "apply-to": apply_to, "priority": priority})),
            )?;
            done(format!("policy '{name}' set"))
        }
        CtlCommand::ClearPolicy { name } => {
            api.call("DELETE", &format!("/policies/{vh}/{}", enc(name)), None)?;
            done(format!("policy '{name}' cleared"))
        }
        CtlCommand::CloseConnection { name } => {
            api.call("DELETE", &format!("/connections/{}", enc(name)), None)?;
            done(format!("connection '{name}' closed"))
        }
        CtlCommand::Export { file } => {
            let d = api.call("GET", "/definitions", None)?;
            let text = serde_json::to_string_pretty(&d).unwrap_or_default();
            match file {
                Some(f) => {
                    std::fs::write(f, text + "\n").map_err(|e| format!("cannot write {f}: {e}"))?;
                    eprintln!("definitions written to {f}");
                }
                None => println!("{text}"),
            }
            Ok(())
        }
        CtlCommand::Import { file } => {
            let text = match file {
                Some(f) => {
                    std::fs::read_to_string(f).map_err(|e| format!("cannot read {f}: {e}"))?
                }
                None => {
                    let mut s = String::new();
                    std::io::stdin()
                        .read_to_string(&mut s)
                        .map_err(|e| e.to_string())?;
                    s
                }
            };
            let d: Value = serde_json::from_str(&text).map_err(|e| format!("not JSON: {e}"))?;
            api.call("POST", "/definitions", Some(d))?;
            done("definitions imported".into())
        }
    }
}

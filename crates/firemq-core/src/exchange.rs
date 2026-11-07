use std::collections::HashMap;
use std::fmt;

use firemq_amqp_codec::FieldTable;

use crate::error::{BrokerError, Result};
use crate::routing::{TopicTrie, headers_match};

/// The four exchange types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExchangeKind {
    Direct,
    Fanout,
    Topic,
    Headers,
}

impl ExchangeKind {
    pub fn parse(s: &str) -> Result<ExchangeKind> {
        Ok(match s {
            "direct" => ExchangeKind::Direct,
            "fanout" => ExchangeKind::Fanout,
            "topic" => ExchangeKind::Topic,
            "headers" => ExchangeKind::Headers,
            other => {
                return Err(BrokerError::command_invalid(format!(
                    "invalid exchange type '{other}'; use direct, fanout, topic or headers"
                )));
            }
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ExchangeKind::Direct => "direct",
            ExchangeKind::Fanout => "fanout",
            ExchangeKind::Topic => "topic",
            ExchangeKind::Headers => "headers",
        }
    }
}

impl fmt::Display for ExchangeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Where a binding sends messages.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Destination {
    Queue(String),
    Exchange(String),
}

impl Destination {
    pub fn name(&self) -> &str {
        match self {
            Destination::Queue(n) | Destination::Exchange(n) => n,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Destination::Queue(_) => "queue",
            Destination::Exchange(_) => "exchange",
        }
    }
}

/// A binding from an exchange to a queue or another exchange.
#[derive(Debug, Clone, PartialEq)]
pub struct Binding {
    pub source: String,
    pub destination: Destination,
    pub routing_key: String,
    pub arguments: FieldTable,
}

/// Options for declaring an exchange.
#[derive(Debug, Clone, Default)]
pub struct ExchangeOptions {
    pub name: String,
    pub kind: String,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub passive: bool,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone)]
enum Router {
    Direct(HashMap<String, Vec<Destination>>),
    Fanout(Vec<Destination>),
    Topic(TopicTrie),
    Headers(Vec<(FieldTable, Destination)>),
}

/// An exchange and its outgoing bindings. Immutable: changes build a new value.
#[derive(Debug, Clone)]
pub struct Exchange {
    pub name: String,
    pub kind: ExchangeKind,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub arguments: FieldTable,
    pub bindings: Vec<Binding>,
    router: Router,
}

impl Exchange {
    pub fn new(
        name: impl Into<String>,
        kind: ExchangeKind,
        durable: bool,
        auto_delete: bool,
        internal: bool,
        arguments: FieldTable,
    ) -> Exchange {
        let mut e = Exchange {
            name: name.into(),
            kind,
            durable,
            auto_delete,
            internal,
            arguments,
            bindings: Vec::new(),
            router: Router::Fanout(Vec::new()),
        };
        e.rebuild();
        e
    }

    /// The `alternate-exchange` argument, if any.
    pub fn alternate_exchange(&self) -> Option<&str> {
        self.arguments
            .get_str("alternate-exchange")
            .filter(|s| !s.is_empty())
    }

    /// Returns a copy with the bindings replaced and the router rebuilt.
    pub fn with_bindings(&self, bindings: Vec<Binding>) -> Exchange {
        let mut e = Exchange {
            name: self.name.clone(),
            kind: self.kind,
            durable: self.durable,
            auto_delete: self.auto_delete,
            internal: self.internal,
            arguments: self.arguments.clone(),
            bindings,
            router: Router::Fanout(Vec::new()),
        };
        e.rebuild();
        e
    }

    fn rebuild(&mut self) {
        self.router = match self.kind {
            ExchangeKind::Direct => {
                let mut map: HashMap<String, Vec<Destination>> = HashMap::new();
                for b in &self.bindings {
                    let v = map.entry(b.routing_key.clone()).or_default();
                    if !v.contains(&b.destination) {
                        v.push(b.destination.clone());
                    }
                }
                Router::Direct(map)
            }
            ExchangeKind::Fanout => {
                let mut v: Vec<Destination> = Vec::new();
                for b in &self.bindings {
                    if !v.contains(&b.destination) {
                        v.push(b.destination.clone());
                    }
                }
                Router::Fanout(v)
            }
            ExchangeKind::Topic => {
                let mut t = TopicTrie::default();
                for b in &self.bindings {
                    t.insert(&b.routing_key, b.destination.clone());
                }
                Router::Topic(t)
            }
            ExchangeKind::Headers => Router::Headers(
                self.bindings
                    .iter()
                    .map(|b| (b.arguments.clone(), b.destination.clone()))
                    .collect(),
            ),
        };
    }

    /// Appends the destinations matching a message. May contain duplicates.
    pub fn route<'a>(
        &'a self,
        routing_key: &str,
        headers: Option<&FieldTable>,
        out: &mut Vec<&'a Destination>,
    ) {
        match &self.router {
            Router::Direct(map) => {
                if let Some(v) = map.get(routing_key) {
                    out.extend(v.iter());
                }
            }
            Router::Fanout(v) => out.extend(v.iter()),
            Router::Topic(t) => t.matches(routing_key, out),
            Router::Headers(v) => {
                for (args, d) in v {
                    if headers_match(args, headers) {
                        out.push(d);
                    }
                }
            }
        }
    }
}

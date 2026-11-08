use std::collections::HashMap;

use firemq_amqp_codec::{FieldTable, Value};

use crate::exchange::Destination;

/// A trie of topic binding patterns. `*` matches exactly one word and `#` matches zero or
/// more words; words are separated by `.`.
#[derive(Debug, Clone, Default)]
pub struct TopicTrie {
    root: Node,
}

#[derive(Debug, Clone, Default)]
struct Node {
    words: HashMap<String, Node>,
    star: Option<Box<Node>>,
    hash: Option<Box<Node>>,
    destinations: Vec<Destination>,
}

impl TopicTrie {
    pub fn insert(&mut self, pattern: &str, destination: Destination) {
        let mut node = &mut self.root;
        for word in pattern.split('.') {
            node = match word {
                "*" => node.star.get_or_insert_with(Default::default),
                "#" => node.hash.get_or_insert_with(Default::default),
                w => node.words.entry(w.to_string()).or_default(),
            };
        }
        if !node.destinations.contains(&destination) {
            node.destinations.push(destination);
        }
    }

    /// Appends every destination whose pattern matches `key`. May contain duplicates.
    pub fn matches<'a>(&'a self, key: &str, out: &mut Vec<&'a Destination>) {
        let words: Vec<&str> = key.split('.').collect();
        walk(&self.root, &words, out);
    }
}

fn walk<'a>(node: &'a Node, words: &[&str], out: &mut Vec<&'a Destination>) {
    if let Some(hash) = &node.hash {
        for i in 0..=words.len() {
            walk(hash, &words[i..], out);
        }
    }
    let Some((first, rest)) = words.split_first() else {
        out.extend(node.destinations.iter());
        return;
    };
    if let Some(child) = node.words.get(*first) {
        walk(child, rest, out);
    }
    if let Some(star) = &node.star {
        walk(star, rest, out);
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a.as_i64(), b.as_i64()) {
        (Some(x), Some(y)) => return x == y,
        (Some(_), None) | (None, Some(_)) => return false,
        _ => {}
    }
    match (a.as_str(), b.as_str()) {
        (Some(x), Some(y)) => x == y,
        _ => a == b,
    }
}

/// Matches message headers against a headers-exchange binding, following RabbitMQ:
/// `x-match` is `all` (default), `any`, `all-with-x` or `any-with-x`. Binding keys that start
/// with `x-` are ignored unless the mode ends in `-with-x`. A void binding value matches any
/// value of the same key.
pub fn headers_match(binding: &FieldTable, headers: Option<&FieldTable>) -> bool {
    let mode = binding.get_str("x-match").unwrap_or("all");
    let (any, with_x) = match mode {
        "any" => (true, false),
        "all-with-x" => (false, true),
        "any-with-x" => (true, true),
        _ => (false, false),
    };
    let empty = FieldTable::new();
    let headers = headers.unwrap_or(&empty);
    for (key, expected) in binding.iter() {
        if key == "x-match" || (!with_x && key.starts_with("x-")) {
            continue;
        }
        let ok = match headers.get(key) {
            Some(actual) => matches!(expected, Value::Void) || values_equal(expected, actual),
            None => false,
        };
        if any && ok {
            return true;
        }
        if !any && !ok {
            return false;
        }
    }
    // `all` matched every key; `any` matched none.
    !any
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(name: &str) -> Destination {
        Destination::Queue(name.to_string())
    }

    fn matched(trie: &TopicTrie, key: &str) -> Vec<String> {
        let mut out = Vec::new();
        trie.matches(key, &mut out);
        let mut names: Vec<String> = out.into_iter().map(|d| d.name().to_string()).collect();
        names.sort();
        names.dedup();
        names
    }

    #[test]
    fn topic_patterns_follow_rabbitmq_semantics() {
        let mut t = TopicTrie::default();
        t.insert("*.orange.*", q("q1"));
        t.insert("*.*.rabbit", q("q2"));
        t.insert("lazy.#", q("q2"));
        t.insert("#", q("all"));
        t.insert("a.#.z", q("az"));
        t.insert("", q("empty"));
        assert_eq!(matched(&t, "quick.orange.rabbit"), ["all", "q1", "q2"]);
        assert_eq!(matched(&t, "lazy.orange.elephant"), ["all", "q1", "q2"]);
        assert_eq!(matched(&t, "quick.orange.fox"), ["all", "q1"]);
        assert_eq!(matched(&t, "lazy.brown.fox"), ["all", "q2"]);
        assert_eq!(matched(&t, "lazy"), ["all", "q2"]);
        assert_eq!(matched(&t, "quick.orange.male.rabbit"), ["all"]);
        assert_eq!(matched(&t, "a.z"), ["all", "az"]);
        assert_eq!(matched(&t, "a.b.c.z"), ["all", "az"]);
        assert_eq!(matched(&t, ""), ["all", "empty"]);
    }

    #[test]
    fn headers_all_and_any() {
        let mut headers = FieldTable::new();
        headers.insert("format", "pdf");
        headers.insert("type", "report");
        headers.insert("size", Value::I64(10));

        let mut all: FieldTable = FieldTable::new();
        all.insert("x-match", "all");
        all.insert("format", "pdf");
        all.insert("size", Value::I32(10));
        assert!(headers_match(&all, Some(&headers)));
        all.insert("type", "log");
        assert!(!headers_match(&all, Some(&headers)));

        let mut any = FieldTable::new();
        any.insert("x-match", "any");
        any.insert("format", "zip");
        any.insert("type", "report");
        assert!(headers_match(&any, Some(&headers)));
        any.insert("type", "log");
        assert!(!headers_match(&any, Some(&headers)));

        let mut presence = FieldTable::new();
        presence.insert("format", Value::Void);
        assert!(headers_match(&presence, Some(&headers)));
        assert!(!headers_match(&presence, None));
    }

    #[test]
    fn empty_all_binding_matches_everything() {
        assert!(headers_match(&FieldTable::new(), None));
    }
}

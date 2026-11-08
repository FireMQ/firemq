use firemq_amqp_codec::FieldTable;

/// What a full queue does with a new message.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Overflow {
    /// Drop (or dead-letter) the oldest ready message.
    #[default]
    DropHead,
    /// Refuse the new message; publishers with confirms get a nack.
    RejectPublish,
    /// Refuse the new message and dead-letter it.
    RejectPublishDlx,
}

impl Overflow {
    pub fn as_str(&self) -> &'static str {
        match self {
            Overflow::DropHead => "drop-head",
            Overflow::RejectPublish => "reject-publish",
            Overflow::RejectPublishDlx => "reject-publish-dlx",
        }
    }
}

/// A queue's effective behaviour, from its `x-` arguments and any matching policy.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueueConfig {
    pub message_ttl: Option<u64>,
    pub expires: Option<u64>,
    pub max_length: Option<u64>,
    pub max_length_bytes: Option<u64>,
    pub overflow: Overflow,
    pub dead_letter_exchange: Option<String>,
    pub dead_letter_routing_key: Option<String>,
    pub single_active_consumer: bool,
    pub max_priority: Option<u8>,
}

fn non_negative(args: &FieldTable, key: &str) -> Result<Option<u64>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(v) => match v.as_i64() {
            Some(n) if n >= 0 => Ok(Some(n as u64)),
            _ => Err(format!(
                "invalid arg '{key}': expected a non-negative integer, got {v:?}"
            )),
        },
    }
}

impl QueueConfig {
    /// Parses and validates queue arguments the way RabbitMQ does.
    pub fn from_args(args: &FieldTable) -> Result<QueueConfig, String> {
        let mut c = QueueConfig {
            message_ttl: non_negative(args, "x-message-ttl")?,
            expires: non_negative(args, "x-expires")?,
            max_length: non_negative(args, "x-max-length")?,
            max_length_bytes: non_negative(args, "x-max-length-bytes")?,
            ..Default::default()
        };
        if c.expires == Some(0) {
            return Err("invalid arg 'x-expires': must be greater than 0".into());
        }
        if let Some(v) = args.get("x-overflow") {
            c.overflow = match v.as_str() {
                Some("drop-head") => Overflow::DropHead,
                Some("reject-publish") => Overflow::RejectPublish,
                Some("reject-publish-dlx") => Overflow::RejectPublishDlx,
                _ => {
                    return Err(format!(
                        "invalid arg 'x-overflow': expected drop-head, reject-publish or reject-publish-dlx, got {v:?}"
                    ));
                }
            };
        }
        if let Some(v) = args.get("x-dead-letter-exchange") {
            let s = v
                .as_str()
                .ok_or("invalid arg 'x-dead-letter-exchange': expected a string")?;
            c.dead_letter_exchange = Some(s.to_string());
        }
        if let Some(v) = args.get("x-dead-letter-routing-key") {
            let s = v
                .as_str()
                .ok_or("invalid arg 'x-dead-letter-routing-key': expected a string")?;
            if c.dead_letter_exchange.is_none() {
                return Err(
                    "invalid arg 'x-dead-letter-routing-key': requires x-dead-letter-exchange"
                        .into(),
                );
            }
            c.dead_letter_routing_key = Some(s.to_string());
        }
        if let Some(v) = args.get("x-single-active-consumer") {
            c.single_active_consumer = v
                .as_bool()
                .ok_or("invalid arg 'x-single-active-consumer': expected a boolean")?;
        }
        if let Some(v) = args.get("x-max-priority") {
            match v.as_i64() {
                Some(n) if (0..=255).contains(&n) => c.max_priority = Some(n as u8),
                _ => return Err("invalid arg 'x-max-priority': expected 0 to 255".into()),
            }
        }
        if let Some(v) = args.get("x-queue-type") {
            match v.as_str() {
                Some("classic") | Some("quorum") => {}
                Some("stream") => {
                    return Err("invalid arg 'x-queue-type': streams are not supported yet".into());
                }
                _ => return Err(format!("invalid arg 'x-queue-type': unknown type {v:?}")),
            }
        }
        Ok(c)
    }

    /// Fills unset fields from a policy definition (RabbitMQ policy keys). Queue arguments
    /// win over policies, except for limits, where the lower value wins.
    pub fn apply_policy(&mut self, def: &serde_json::Value) {
        let num = |k: &str| def.get(k).and_then(|v| v.as_u64());
        let min = |a: Option<u64>, b: Option<u64>| match (a, b) {
            (Some(x), Some(y)) => Some(x.min(y)),
            (x, y) => x.or(y),
        };
        self.message_ttl = min(self.message_ttl, num("message-ttl"));
        self.expires = min(self.expires, num("expires"));
        self.max_length = min(self.max_length, num("max-length"));
        self.max_length_bytes = min(self.max_length_bytes, num("max-length-bytes"));
        if self.dead_letter_exchange.is_none()
            && let Some(dlx) = def.get("dead-letter-exchange").and_then(|v| v.as_str())
        {
            self.dead_letter_exchange = Some(dlx.to_string());
            if let Some(rk) = def.get("dead-letter-routing-key").and_then(|v| v.as_str()) {
                self.dead_letter_routing_key = Some(rk.to_string());
            }
        }
        if let Some(o) = def.get("overflow").and_then(|v| v.as_str())
            && self.overflow == Overflow::DropHead
        {
            self.overflow = match o {
                "reject-publish" => Overflow::RejectPublish,
                "reject-publish-dlx" => Overflow::RejectPublishDlx,
                _ => Overflow::DropHead,
            };
        }
    }

    /// Names the first argument that differs from `other`, for redeclaration errors.
    pub fn first_difference(&self, other: &QueueConfig) -> Option<&'static str> {
        if self.message_ttl != other.message_ttl {
            return Some("x-message-ttl");
        }
        if self.expires != other.expires {
            return Some("x-expires");
        }
        if self.max_length != other.max_length {
            return Some("x-max-length");
        }
        if self.max_length_bytes != other.max_length_bytes {
            return Some("x-max-length-bytes");
        }
        if self.overflow != other.overflow {
            return Some("x-overflow");
        }
        if self.dead_letter_exchange != other.dead_letter_exchange {
            return Some("x-dead-letter-exchange");
        }
        if self.dead_letter_routing_key != other.dead_letter_routing_key {
            return Some("x-dead-letter-routing-key");
        }
        if self.single_active_consumer != other.single_active_consumer {
            return Some("x-single-active-consumer");
        }
        if self.max_priority != other.max_priority {
            return Some("x-max-priority");
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use firemq_amqp_codec::Value;

    #[test]
    fn parses_rabbitmq_arguments() {
        let mut a = FieldTable::new();
        a.insert("x-message-ttl", Value::I32(1000));
        a.insert("x-max-length", Value::I64(10));
        a.insert("x-overflow", "reject-publish");
        a.insert("x-dead-letter-exchange", "dlx");
        a.insert("x-dead-letter-routing-key", "dead");
        a.insert("x-single-active-consumer", true);
        let c = QueueConfig::from_args(&a).unwrap();
        assert_eq!(c.message_ttl, Some(1000));
        assert_eq!(c.max_length, Some(10));
        assert_eq!(c.overflow, Overflow::RejectPublish);
        assert_eq!(c.dead_letter_routing_key.as_deref(), Some("dead"));
        assert!(c.single_active_consumer);
    }

    #[test]
    fn rejects_invalid_arguments() {
        let mut a = FieldTable::new();
        a.insert("x-message-ttl", Value::I32(-1));
        assert!(QueueConfig::from_args(&a).is_err());
        let mut a = FieldTable::new();
        a.insert("x-overflow", "explode");
        assert!(QueueConfig::from_args(&a).is_err());
        let mut a = FieldTable::new();
        a.insert("x-queue-type", "stream");
        assert!(QueueConfig::from_args(&a).is_err());
    }

    #[test]
    fn policy_fills_gaps_and_lowers_limits() {
        let mut a = FieldTable::new();
        a.insert("x-max-length", Value::I32(100));
        let mut c = QueueConfig::from_args(&a).unwrap();
        c.apply_policy(&serde_json::json!({"max-length": 10, "message-ttl": 500, "dead-letter-exchange": "dlx"}));
        assert_eq!(c.max_length, Some(10));
        assert_eq!(c.message_ttl, Some(500));
        assert_eq!(c.dead_letter_exchange.as_deref(), Some("dlx"));
    }
}

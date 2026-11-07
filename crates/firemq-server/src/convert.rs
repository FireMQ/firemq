//! Conversions between FireMQ protocol values (MessagePack) and AMQP field tables.

use bytes::Bytes;
use firemq_core::{BasicProperties, FieldTable, LongString, Value};
use firemq_proto::{Map, Properties, Value as Mp};

/// MessagePack value to AMQP field value.
pub fn to_amqp(v: &Mp) -> Value {
    match v {
        Mp::Nil => Value::Void,
        Mp::Boolean(b) => Value::Bool(*b),
        Mp::Integer(i) => match i.as_i64() {
            Some(n) => Value::I64(n),
            None => Value::I64(i.as_u64().unwrap_or(0) as i64),
        },
        Mp::F32(f) => Value::F32(*f),
        Mp::F64(f) => Value::F64(*f),
        Mp::String(s) => Value::LongString(LongString(Bytes::copy_from_slice(s.as_bytes()))),
        Mp::Binary(b) => Value::ByteArray(Bytes::copy_from_slice(b)),
        Mp::Array(a) => Value::Array(a.iter().map(to_amqp).collect()),
        Mp::Map(m) => Value::Table(FieldTable(
            m.iter().map(|(k, v)| (key_string(k), to_amqp(v))).collect(),
        )),
        Mp::Ext(_, b) => Value::ByteArray(Bytes::copy_from_slice(b)),
    }
}

fn key_string(k: &Mp) -> String {
    match k {
        Mp::String(s) => s.as_str().unwrap_or_default().to_string(),
        other => other.to_string(),
    }
}

/// AMQP field value to MessagePack value.
pub fn to_msgpack(v: &Value) -> Mp {
    match v {
        Value::Bool(b) => Mp::Boolean(*b),
        Value::I8(n) => Mp::from(*n),
        Value::U8(n) => Mp::from(*n),
        Value::I16(n) => Mp::from(*n),
        Value::U16(n) => Mp::from(*n),
        Value::I32(n) => Mp::from(*n),
        Value::U32(n) => Mp::from(*n),
        Value::I64(n) => Mp::from(*n),
        Value::Timestamp(n) => Mp::from(*n),
        Value::F32(f) => Mp::F32(*f),
        Value::F64(f) => Mp::F64(*f),
        Value::Decimal { scale, value } => Mp::F64(*value as f64 / 10f64.powi(*scale as i32)),
        Value::LongString(s) => match std::str::from_utf8(&s.0) {
            Ok(text) => Mp::from(text),
            Err(_) => Mp::Binary(s.0.to_vec()),
        },
        Value::ByteArray(b) => Mp::Binary(b.to_vec()),
        Value::Array(a) => Mp::Array(a.iter().map(to_msgpack).collect()),
        Value::Table(t) => Mp::Map(
            t.iter()
                .map(|(k, v)| (Mp::from(k), to_msgpack(v)))
                .collect(),
        ),
        Value::Void => Mp::Nil,
    }
}

/// Protocol map (arguments, headers) to a field table.
pub fn map_to_table(m: &Map) -> FieldTable {
    FieldTable(m.iter().map(|(k, v)| (k.clone(), to_amqp(v))).collect())
}

/// Field table to a protocol map.
pub fn table_to_map(t: &FieldTable) -> Map {
    t.iter()
        .map(|(k, v)| (k.to_string(), to_msgpack(v)))
        .collect()
}

/// Builds AMQP basic properties from FireMQ publish fields.
pub fn to_basic(props: &Properties, headers: &Map, persistent: bool) -> BasicProperties {
    BasicProperties {
        content_type: props.content_type.clone(),
        content_encoding: props.content_encoding.clone(),
        headers: (!headers.is_empty()).then(|| map_to_table(headers)),
        delivery_mode: persistent.then_some(2),
        priority: props.priority,
        correlation_id: props.correlation_id.clone(),
        reply_to: props.reply_to.clone(),
        expiration: props.expiration.map(|e| e.to_string()),
        message_id: props.message_id.clone(),
        timestamp: props.timestamp,
        kind: props.kind.clone(),
        user_id: props.user_id.clone(),
        app_id: props.app_id.clone(),
        cluster_id: None,
    }
}

/// Splits AMQP basic properties into FireMQ properties and headers.
pub fn from_basic(p: &BasicProperties) -> (Properties, Map) {
    let props = Properties {
        content_type: p.content_type.clone(),
        content_encoding: p.content_encoding.clone(),
        correlation_id: p.correlation_id.clone(),
        reply_to: p.reply_to.clone(),
        expiration: p.expiration.as_deref().and_then(|e| e.parse().ok()),
        message_id: p.message_id.clone(),
        timestamp: p.timestamp,
        kind: p.kind.clone(),
        user_id: p.user_id.clone(),
        app_id: p.app_id.clone(),
        priority: p.priority,
    };
    let headers = p.headers.as_ref().map(table_to_map).unwrap_or_default();
    (props, headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_round_trip() {
        let mut m = Map::new();
        m.insert("n".into(), Mp::from(42));
        m.insert("s".into(), Mp::from("text"));
        m.insert("b".into(), Mp::Boolean(true));
        m.insert("nil".into(), Mp::Nil);
        m.insert("arr".into(), Mp::Array(vec![Mp::from(1), Mp::from("x")]));
        m.insert(
            "map".into(),
            Mp::Map(vec![(Mp::from("k"), Mp::from(1.5f64))]),
        );
        assert_eq!(table_to_map(&map_to_table(&m)), m);
    }

    #[test]
    fn properties_round_trip() {
        let props = Properties {
            content_type: Some("application/json".into()),
            expiration: Some(1500),
            priority: Some(3),
            ..Default::default()
        };
        let mut headers = Map::new();
        headers.insert("trace".into(), Mp::from("abc"));
        let basic = to_basic(&props, &headers, true);
        assert_eq!(basic.expiration.as_deref(), Some("1500"));
        assert!(basic.is_persistent());
        let (p2, h2) = from_basic(&basic);
        assert_eq!(p2, props);
        assert_eq!(h2, headers);
    }
}

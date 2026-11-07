//! Conversions between AMQP field tables and JSON, as used by RabbitMQ's definitions files
//! and management API.

use firemq_amqp_codec::{FieldTable, LongString, Value};
use serde_json::{Map, Number, Value as Json};

/// Converts a field table to a JSON object.
pub fn table_to_json(t: &FieldTable) -> Json {
    let mut map = Map::new();
    for (k, v) in t.iter() {
        map.insert(k.to_string(), value_to_json(v));
    }
    Json::Object(map)
}

/// Converts a field table value to JSON.
pub fn value_to_json(v: &Value) -> Json {
    match v {
        Value::Bool(b) => Json::Bool(*b),
        Value::I8(n) => Json::from(*n),
        Value::U8(n) => Json::from(*n),
        Value::I16(n) => Json::from(*n),
        Value::U16(n) => Json::from(*n),
        Value::I32(n) => Json::from(*n),
        Value::U32(n) => Json::from(*n),
        Value::I64(n) => Json::from(*n),
        Value::Timestamp(n) => Json::from(*n),
        Value::F32(f) => Number::from_f64(*f as f64)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        Value::F64(f) => Number::from_f64(*f).map(Json::Number).unwrap_or(Json::Null),
        Value::Decimal { scale, value } => {
            Number::from_f64(*value as f64 / 10f64.powi(*scale as i32))
                .map(Json::Number)
                .unwrap_or(Json::Null)
        }
        Value::LongString(s) => Json::String(s.to_string_lossy()),
        Value::ByteArray(b) => Json::String(String::from_utf8_lossy(b).into_owned()),
        Value::Array(items) => Json::Array(items.iter().map(value_to_json).collect()),
        Value::Table(t) => table_to_json(t),
        Value::Void => Json::Null,
    }
}

/// Converts a JSON value to a field table value. Integers become 64-bit longs, like
/// RabbitMQ's definitions import.
pub fn json_to_value(j: &Json) -> Value {
    match j {
        Json::Null => Value::Void,
        Json::Bool(b) => Value::Bool(*b),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::I64(i)
            } else if let Some(u) = n.as_u64() {
                Value::I64(u as i64)
            } else {
                Value::F64(n.as_f64().unwrap_or(0.0))
            }
        }
        Json::String(s) => Value::LongString(LongString::from(s.as_str())),
        Json::Array(items) => Value::Array(items.iter().map(json_to_value).collect()),
        Json::Object(_) => Value::Table(json_to_table(j)),
    }
}

/// Converts a JSON object to a field table. Non-objects give an empty table.
pub fn json_to_table(j: &Json) -> FieldTable {
    match j {
        Json::Object(map) => FieldTable(
            map.iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
        ),
        _ => FieldTable::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tables_round_trip_through_json() {
        let j = json!({"x-message-ttl": 1000, "x-dead-letter-exchange": "dlx", "flag": true, "list": [1, "a"], "nested": {"k": null}});
        assert_eq!(table_to_json(&json_to_table(&j)), j);
    }
}

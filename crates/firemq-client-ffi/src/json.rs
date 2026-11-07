//! Converts between JSON text and the MessagePack values the protocol carries in headers and
//! arguments. Foreign code passes headers and arguments as JSON objects.

use base64::Engine;
use firemq_client::{Map, Properties, Value};
use serde_json::{Value as J, json};

use crate::FireMqError;

fn to_value(v: &J) -> Value {
    match v {
        J::Null => Value::Nil,
        J::Bool(b) => Value::Boolean(*b),
        J::Number(n) => match (n.as_i64(), n.as_u64()) {
            (Some(i), _) => Value::from(i),
            (None, Some(u)) => Value::from(u),
            _ => Value::F64(n.as_f64().unwrap_or(0.0)),
        },
        J::String(s) => Value::from(s.as_str()),
        J::Array(a) => Value::Array(a.iter().map(to_value).collect()),
        J::Object(o) => Value::Map(
            o.iter()
                .map(|(k, v)| (Value::from(k.as_str()), to_value(v)))
                .collect(),
        ),
    }
}

fn from_value(v: &Value) -> J {
    match v {
        Value::Nil => J::Null,
        Value::Boolean(b) => J::Bool(*b),
        Value::Integer(i) => i
            .as_i64()
            .map(J::from)
            .or_else(|| i.as_u64().map(J::from))
            .unwrap_or(J::Null),
        Value::F32(f) => json!(f),
        Value::F64(f) => json!(f),
        Value::String(s) => J::String(s.as_str().unwrap_or_default().to_string()),
        // JSON has no byte strings; binary header values (for example from AMQP publishers)
        // arrive base64-encoded.
        Value::Binary(b) => J::String(base64::engine::general_purpose::STANDARD.encode(b)),
        Value::Array(a) => J::Array(a.iter().map(from_value).collect()),
        Value::Map(m) => J::Object(
            m.iter()
                .map(|(k, v)| (k.as_str().unwrap_or_default().to_string(), from_value(v)))
                .collect(),
        ),
        Value::Ext(..) => J::Null,
    }
}

/// Parses a JSON object into a header or argument map. `None`, an empty string and `null`
/// mean an empty map.
pub(crate) fn map_from_json(text: Option<&str>) -> Result<Map, FireMqError> {
    let text = match text.map(str::trim) {
        None | Some("") | Some("null") => return Ok(Map::new()),
        Some(t) => t,
    };
    let parsed: J = serde_json::from_str(text).map_err(|e| FireMqError::Other {
        message: format!("invalid JSON: {e}"),
    })?;
    match parsed {
        J::Object(o) => Ok(o.iter().map(|(k, v)| (k.clone(), to_value(v))).collect()),
        _ => Err(FireMqError::Other {
            message: "headers and arguments must be a JSON object".into(),
        }),
    }
}

/// Renders a header map as a JSON object.
pub(crate) fn map_to_json(map: &Map) -> String {
    J::Object(
        map.iter()
            .map(|(k, v)| (k.clone(), from_value(v)))
            .collect(),
    )
    .to_string()
}

/// Renders message properties as a JSON object with the protocol's key names.
pub(crate) fn properties_to_json(p: &Properties) -> String {
    serde_json::to_string(p).unwrap_or_else(|_| "{}".into())
}

/// Parses message properties from a JSON object with the protocol's key names.
pub(crate) fn properties_from_json(text: Option<&str>) -> Result<Properties, FireMqError> {
    match text.map(str::trim) {
        None | Some("") | Some("null") => Ok(Properties::default()),
        Some(t) => serde_json::from_str(t).map_err(|e| FireMqError::Other {
            message: format!("invalid properties JSON: {e}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headers_round_trip() {
        let text = r#"{"b":true,"f":1.5,"i":-7,"l":[1,"two"],"m":{"k":"v"},"n":null,"s":"x","u":18446744073709551615}"#;
        let map = map_from_json(Some(text)).unwrap();
        assert_eq!(map_to_json(&map), text);
    }

    #[test]
    fn empty_and_invalid_maps() {
        assert!(map_from_json(None).unwrap().is_empty());
        assert!(map_from_json(Some(" ")).unwrap().is_empty());
        assert!(map_from_json(Some("null")).unwrap().is_empty());
        assert!(matches!(
            map_from_json(Some("[1]")),
            Err(FireMqError::Other { .. })
        ));
        assert!(matches!(
            map_from_json(Some("{")),
            Err(FireMqError::Other { .. })
        ));
    }

    #[test]
    fn properties_use_protocol_keys() {
        let p =
            properties_from_json(Some(r#"{"type":"t","priority":3,"expiration":60000}"#)).unwrap();
        assert_eq!(p.kind.as_deref(), Some("t"));
        assert_eq!(p.priority, Some(3));
        assert_eq!(
            properties_to_json(&p),
            r#"{"expiration":60000,"type":"t","priority":3}"#
        );
    }
}

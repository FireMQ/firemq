use std::fmt;

use bytes::{BufMut, Bytes, BytesMut};

use crate::CodecError;
use crate::rw::Reader;

/// An AMQP long string: up to 4 GiB of opaque bytes, usually UTF-8.
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct LongString(pub Bytes);

impl LongString {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the value as UTF-8 text, replacing invalid sequences.
    pub fn to_string_lossy(&self) -> String {
        String::from_utf8_lossy(&self.0).into_owned()
    }
}

impl fmt::Debug for LongString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(&self.0) {
            Ok(s) => write!(f, "{s:?}"),
            Err(_) => write!(f, "{:?}", self.0),
        }
    }
}

impl From<&str> for LongString {
    fn from(s: &str) -> Self {
        LongString(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl From<String> for LongString {
    fn from(s: String) -> Self {
        LongString(Bytes::from(s))
    }
}

impl From<Vec<u8>> for LongString {
    fn from(v: Vec<u8>) -> Self {
        LongString(Bytes::from(v))
    }
}

/// A field table value, using RabbitMQ's type tags (which differ from the 0-9-1 spec for
/// `s`, `u` and `l`; RabbitMQ's choice is what every client implements).
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Bool(bool),
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    F32(f32),
    F64(f64),
    Decimal { scale: u8, value: u32 },
    LongString(LongString),
    Array(Vec<Value>),
    Timestamp(u64),
    Table(FieldTable),
    Void,
    ByteArray(Bytes),
}

impl Value {
    /// Returns the value as an `i64` when it is any integer type.
    pub fn as_i64(&self) -> Option<i64> {
        Some(match *self {
            Value::I8(v) => v as i64,
            Value::U8(v) => v as i64,
            Value::I16(v) => v as i64,
            Value::U16(v) => v as i64,
            Value::I32(v) => v as i64,
            Value::U32(v) => v as i64,
            Value::I64(v) => v,
            Value::Timestamp(v) => v as i64,
            Value::F64(v) if v.fract() == 0.0 => v as i64,
            Value::F32(v) if v.fract() == 0.0 => v as i64,
            _ => return None,
        })
    }

    /// Returns the value as text when it is a long string or byte array.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::LongString(s) => std::str::from_utf8(&s.0).ok(),
            Value::ByteArray(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_table(&self) -> Option<&FieldTable> {
        match self {
            Value::Table(t) => Some(t),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn string(s: impl Into<String>) -> Value {
        Value::LongString(LongString::from(s.into()))
    }

    fn read(r: &mut Reader) -> Result<Value, CodecError> {
        let tag = r.u8()?;
        Ok(match tag {
            b't' => Value::Bool(r.u8()? != 0),
            b'b' => Value::I8(r.u8()? as i8),
            b'B' => Value::U8(r.u8()?),
            b's' => Value::I16(r.u16()? as i16),
            b'u' => Value::U16(r.u16()?),
            b'I' => Value::I32(r.u32()? as i32),
            b'i' => Value::U32(r.u32()?),
            b'l' | b'L' => Value::I64(r.u64()? as i64),
            b'f' => Value::F32(f32::from_bits(r.u32()?)),
            b'd' => Value::F64(f64::from_bits(r.u64()?)),
            b'D' => Value::Decimal {
                scale: r.u8()?,
                value: r.u32()?,
            },
            b'S' => Value::LongString(LongString(r.long_bytes()?)),
            b'A' => {
                let len = r.u32()? as usize;
                let mut inner = Reader::new(r.bytes(len, "array")?);
                let mut items = Vec::new();
                while inner.remaining() > 0 {
                    items.push(Value::read(&mut inner)?);
                }
                Value::Array(items)
            }
            b'T' => Value::Timestamp(r.u64()?),
            b'F' => Value::Table(FieldTable::read(r)?),
            b'V' => Value::Void,
            b'x' => Value::ByteArray(r.long_bytes()?),
            other => return Err(CodecError::UnknownFieldType(other)),
        })
    }

    fn write(&self, out: &mut BytesMut) {
        match self {
            Value::Bool(v) => {
                out.put_u8(b't');
                out.put_u8(*v as u8);
            }
            Value::I8(v) => {
                out.put_u8(b'b');
                out.put_i8(*v);
            }
            Value::U8(v) => {
                out.put_u8(b'B');
                out.put_u8(*v);
            }
            Value::I16(v) => {
                out.put_u8(b's');
                out.put_i16(*v);
            }
            Value::U16(v) => {
                out.put_u8(b'u');
                out.put_u16(*v);
            }
            Value::I32(v) => {
                out.put_u8(b'I');
                out.put_i32(*v);
            }
            Value::U32(v) => {
                out.put_u8(b'i');
                out.put_u32(*v);
            }
            Value::I64(v) => {
                out.put_u8(b'l');
                out.put_i64(*v);
            }
            Value::F32(v) => {
                out.put_u8(b'f');
                out.put_f32(*v);
            }
            Value::F64(v) => {
                out.put_u8(b'd');
                out.put_f64(*v);
            }
            Value::Decimal { scale, value } => {
                out.put_u8(b'D');
                out.put_u8(*scale);
                out.put_u32(*value);
            }
            Value::LongString(s) => {
                out.put_u8(b'S');
                out.put_u32(s.0.len() as u32);
                out.put_slice(&s.0);
            }
            Value::Array(items) => {
                out.put_u8(b'A');
                let at = out.len();
                out.put_u32(0);
                for item in items {
                    item.write(out);
                }
                let len = (out.len() - at - 4) as u32;
                out[at..at + 4].copy_from_slice(&len.to_be_bytes());
            }
            Value::Timestamp(v) => {
                out.put_u8(b'T');
                out.put_u64(*v);
            }
            Value::Table(t) => {
                out.put_u8(b'F');
                t.write(out);
            }
            Value::Void => out.put_u8(b'V'),
            Value::ByteArray(b) => {
                out.put_u8(b'x');
                out.put_u32(b.len() as u32);
                out.put_slice(b);
            }
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::I32(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::I64(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::string(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::string(v)
    }
}

impl From<FieldTable> for Value {
    fn from(v: FieldTable) -> Self {
        Value::Table(v)
    }
}

/// An ordered AMQP field table. Lookups are linear, which is fastest for the small tables
/// AMQP uses (queue arguments, client properties, message headers).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FieldTable(pub Vec<(String, Value)>);

impl FieldTable {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.get(key).and_then(Value::as_i64)
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(Value::as_str)
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.get(key).and_then(Value::as_bool)
    }

    /// Inserts or replaces a key, keeping the original position when it exists.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        let key = key.into();
        let value = value.into();
        match self.0.iter_mut().find(|(k, _)| *k == key) {
            Some(slot) => slot.1 = value,
            None => self.0.push((key, value)),
        }
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        let pos = self.0.iter().position(|(k, _)| k == key)?;
        Some(self.0.remove(pos).1)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub(crate) fn read(r: &mut Reader) -> Result<FieldTable, CodecError> {
        let len = r.u32()? as usize;
        let mut inner = Reader::new(r.bytes(len, "table")?);
        let mut entries = Vec::new();
        while inner.remaining() > 0 {
            let key = inner.short_str()?;
            let value = Value::read(&mut inner)?;
            entries.push((key, value));
        }
        Ok(FieldTable(entries))
    }

    /// Appends the table, including its 32-bit length prefix.
    pub fn write(&self, out: &mut BytesMut) {
        let at = out.len();
        out.put_u32(0);
        for (key, value) in &self.0 {
            let mut end = key.len().min(255);
            while !key.is_char_boundary(end) {
                end -= 1;
            }
            out.put_u8(end as u8);
            out.put_slice(&key.as_bytes()[..end]);
            value.write(out);
        }
        let len = (out.len() - at - 4) as u32;
        out[at..at + 4].copy_from_slice(&len.to_be_bytes());
    }

    /// Decodes a table from its encoded form, including the length prefix.
    pub fn decode(data: Bytes) -> Result<FieldTable, CodecError> {
        FieldTable::read(&mut Reader::new(data))
    }
}

impl<K: Into<String>, V: Into<Value>> FromIterator<(K, V)> for FieldTable {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        FieldTable(
            iter.into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_value_type_round_trips() {
        let mut nested = FieldTable::new();
        nested.insert("inner", Value::I32(-5));
        let table = FieldTable(vec![
            ("bool".into(), Value::Bool(true)),
            ("i8".into(), Value::I8(-8)),
            ("u8".into(), Value::U8(8)),
            ("i16".into(), Value::I16(-16)),
            ("u16".into(), Value::U16(16)),
            ("i32".into(), Value::I32(-32)),
            ("u32".into(), Value::U32(32)),
            ("i64".into(), Value::I64(-64)),
            ("f32".into(), Value::F32(1.5)),
            ("f64".into(), Value::F64(2.25)),
            (
                "dec".into(),
                Value::Decimal {
                    scale: 2,
                    value: 12345,
                },
            ),
            ("str".into(), Value::string("héllo")),
            (
                "arr".into(),
                Value::Array(vec![Value::I32(1), Value::string("two"), Value::Void]),
            ),
            ("ts".into(), Value::Timestamp(1_700_000_000)),
            ("table".into(), Value::Table(nested)),
            ("void".into(), Value::Void),
            (
                "bytes".into(),
                Value::ByteArray(Bytes::from_static(b"\x00\x01")),
            ),
        ]);
        let mut out = BytesMut::new();
        table.write(&mut out);
        let decoded = FieldTable::decode(out.freeze()).unwrap();
        assert_eq!(decoded, table);
        assert_eq!(decoded.get_i64("u16"), Some(16));
        assert_eq!(decoded.get_str("str"), Some("héllo"));
    }

    #[test]
    fn unknown_type_is_an_error() {
        let data = Bytes::from_static(&[0, 0, 0, 3, 1, b'k', b'?']);
        assert_eq!(
            FieldTable::decode(data),
            Err(CodecError::UnknownFieldType(b'?'))
        );
    }

    #[test]
    fn insert_replaces_in_place() {
        let mut t = FieldTable::new();
        t.insert("a", 1);
        t.insert("b", 2);
        t.insert("a", 3);
        assert_eq!(t.0[0], ("a".into(), Value::I32(3)));
        assert_eq!(t.len(), 2);
    }
}

use bytes::{BufMut, Bytes, BytesMut};

use crate::CodecError;
use crate::rw::Reader;
use crate::table::FieldTable;

/// The 14 properties of the `basic` content class, in wire order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BasicProperties {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub headers: Option<FieldTable>,
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub expiration: Option<String>,
    pub message_id: Option<String>,
    pub timestamp: Option<u64>,
    pub kind: Option<String>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
    pub cluster_id: Option<String>,
}

const CONTENT_TYPE: u16 = 1 << 15;
const CONTENT_ENCODING: u16 = 1 << 14;
const HEADERS: u16 = 1 << 13;
const DELIVERY_MODE: u16 = 1 << 12;
const PRIORITY: u16 = 1 << 11;
const CORRELATION_ID: u16 = 1 << 10;
const REPLY_TO: u16 = 1 << 9;
const EXPIRATION: u16 = 1 << 8;
const MESSAGE_ID: u16 = 1 << 7;
const TIMESTAMP: u16 = 1 << 6;
const TYPE: u16 = 1 << 5;
const USER_ID: u16 = 1 << 4;
const APP_ID: u16 = 1 << 3;
const CLUSTER_ID: u16 = 1 << 2;
const CONTINUATION: u16 = 1;

impl BasicProperties {
    /// True when `delivery_mode` is 2 (persistent).
    pub fn is_persistent(&self) -> bool {
        self.delivery_mode == Some(2)
    }

    /// Decodes the property flags and list (the part of a content header after body size).
    pub fn read(r: &mut Reader) -> Result<BasicProperties, CodecError> {
        let mut flags = r.u16()?;
        let first = flags;
        // Extra flag words are allowed by the spec but unused by the basic class.
        while flags & CONTINUATION != 0 {
            flags = r.u16()?;
        }
        let flags = first;
        let mut p = BasicProperties::default();
        let has = |bit: u16| flags & bit != 0;
        if has(CONTENT_TYPE) {
            p.content_type = Some(r.short_str()?);
        }
        if has(CONTENT_ENCODING) {
            p.content_encoding = Some(r.short_str()?);
        }
        if has(HEADERS) {
            p.headers = Some(r.table()?);
        }
        if has(DELIVERY_MODE) {
            p.delivery_mode = Some(r.u8()?);
        }
        if has(PRIORITY) {
            p.priority = Some(r.u8()?);
        }
        if has(CORRELATION_ID) {
            p.correlation_id = Some(r.short_str()?);
        }
        if has(REPLY_TO) {
            p.reply_to = Some(r.short_str()?);
        }
        if has(EXPIRATION) {
            p.expiration = Some(r.short_str()?);
        }
        if has(MESSAGE_ID) {
            p.message_id = Some(r.short_str()?);
        }
        if has(TIMESTAMP) {
            p.timestamp = Some(r.u64()?);
        }
        if has(TYPE) {
            p.kind = Some(r.short_str()?);
        }
        if has(USER_ID) {
            p.user_id = Some(r.short_str()?);
        }
        if has(APP_ID) {
            p.app_id = Some(r.short_str()?);
        }
        if has(CLUSTER_ID) {
            p.cluster_id = Some(r.short_str()?);
        }
        Ok(p)
    }

    /// Encodes the property flags and list.
    pub fn write(&self, out: &mut BytesMut) {
        let mut flags = 0u16;
        let mut set = |cond: bool, bit: u16| {
            if cond {
                flags |= bit;
            }
        };
        set(self.content_type.is_some(), CONTENT_TYPE);
        set(self.content_encoding.is_some(), CONTENT_ENCODING);
        set(self.headers.is_some(), HEADERS);
        set(self.delivery_mode.is_some(), DELIVERY_MODE);
        set(self.priority.is_some(), PRIORITY);
        set(self.correlation_id.is_some(), CORRELATION_ID);
        set(self.reply_to.is_some(), REPLY_TO);
        set(self.expiration.is_some(), EXPIRATION);
        set(self.message_id.is_some(), MESSAGE_ID);
        set(self.timestamp.is_some(), TIMESTAMP);
        set(self.kind.is_some(), TYPE);
        set(self.user_id.is_some(), USER_ID);
        set(self.app_id.is_some(), APP_ID);
        set(self.cluster_id.is_some(), CLUSTER_ID);
        out.put_u16(flags);
        let short = |out: &mut BytesMut, s: &Option<String>| {
            if let Some(s) = s {
                let mut end = s.len().min(255);
                while !s.is_char_boundary(end) {
                    end -= 1;
                }
                out.put_u8(end as u8);
                out.put_slice(&s.as_bytes()[..end]);
            }
        };
        short(out, &self.content_type);
        short(out, &self.content_encoding);
        if let Some(h) = &self.headers {
            h.write(out);
        }
        if let Some(v) = self.delivery_mode {
            out.put_u8(v);
        }
        if let Some(v) = self.priority {
            out.put_u8(v);
        }
        short(out, &self.correlation_id);
        short(out, &self.reply_to);
        short(out, &self.expiration);
        short(out, &self.message_id);
        if let Some(v) = self.timestamp {
            out.put_u64(v);
        }
        short(out, &self.kind);
        short(out, &self.user_id);
        short(out, &self.app_id);
        short(out, &self.cluster_id);
    }

    /// Encodes the properties into a standalone buffer.
    pub fn to_bytes(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(32);
        self.write(&mut out);
        out.freeze()
    }

    /// Decodes properties from a standalone buffer produced by [`BasicProperties::to_bytes`].
    pub fn from_bytes(data: Bytes) -> Result<BasicProperties, CodecError> {
        BasicProperties::read(&mut Reader::new(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::Value;

    #[test]
    fn all_properties_round_trip() {
        let mut headers = FieldTable::new();
        headers.insert("x-retries", Value::I32(3));
        let props = BasicProperties {
            content_type: Some("application/json".into()),
            content_encoding: Some("gzip".into()),
            headers: Some(headers),
            delivery_mode: Some(2),
            priority: Some(5),
            correlation_id: Some("c-1".into()),
            reply_to: Some("amq.rabbitmq.reply-to".into()),
            expiration: Some("60000".into()),
            message_id: Some("m-1".into()),
            timestamp: Some(1_700_000_000),
            kind: Some("order.created".into()),
            user_id: Some("guest".into()),
            app_id: Some("shop".into()),
            cluster_id: Some("".into()),
        };
        let decoded = BasicProperties::from_bytes(props.to_bytes()).unwrap();
        assert_eq!(decoded, props);
        assert!(decoded.is_persistent());
    }

    #[test]
    fn empty_properties_are_two_bytes() {
        assert_eq!(BasicProperties::default().to_bytes().len(), 2);
    }
}

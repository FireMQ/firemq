//! The on-disk record format.
//!
//! ```text
//! len: u32 | crc32c: u32 | seq: u64 | ts: u64 | flags: u8
//!          | ex_len: u8 | exchange | key_len: u8 | routing key
//!          | props_len: u32 | properties (AMQP encoding) | body
//! ```
//!
//! `len` counts the bytes after the CRC field; the CRC covers those same bytes. All integers
//! are big-endian. A record that is cut short or fails its CRC marks the end of valid data.

use bytes::{BufMut, Bytes};
use firemq_amqp_codec::BasicProperties;
use firemq_core::Message;

pub const HEADER: usize = 8;
pub const FLAG_PERSISTENT: u8 = 1;
const FIXED: usize = 8 + 8 + 1 + 1 + 1 + 4;

/// Encoded size of a record, including its header.
pub fn size(msg: &Message, props_len: usize) -> usize {
    HEADER
        + FIXED
        + msg.exchange.len().min(255)
        + msg.routing_key.len().min(255)
        + props_len
        + msg.body.len()
}

fn short(s: &str) -> &[u8] {
    let mut end = s.len().min(255);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s.as_bytes()[..end]
}

/// Appends an encoded record to `out`. Returns the record length.
pub fn encode(out: &mut Vec<u8>, seq: u64, msg: &Message, persistent: bool) -> usize {
    let props = msg.properties.to_bytes();
    let start = out.len();
    out.put_u32(0);
    out.put_u32(0);
    out.put_u64(seq);
    out.put_u64(msg.published_at);
    out.put_u8(if persistent { FLAG_PERSISTENT } else { 0 });
    let ex = short(&msg.exchange);
    out.put_u8(ex.len() as u8);
    out.put_slice(ex);
    let key = short(&msg.routing_key);
    out.put_u8(key.len() as u8);
    out.put_slice(key);
    out.put_u32(props.len() as u32);
    out.put_slice(&props);
    out.put_slice(&msg.body);
    let len = (out.len() - start - HEADER) as u32;
    let crc = crc32c::crc32c(&out[start + HEADER..]);
    out[start..start + 4].copy_from_slice(&len.to_be_bytes());
    out[start + 4..start + 8].copy_from_slice(&crc.to_be_bytes());
    out.len() - start
}

/// Fields read back from a record.
pub struct Decoded<'a> {
    pub seq: u64,
    pub ts: u64,
    pub flags: u8,
    pub exchange: &'a str,
    pub routing_key: &'a str,
    pub props: &'a [u8],
    pub body: &'a [u8],
    /// Total length including the header.
    pub len: usize,
}

/// Decodes the record at the start of `data`. Returns `None` when the data is truncated or
/// corrupt.
pub fn decode(data: &[u8]) -> Option<Decoded<'_>> {
    if data.len() < HEADER {
        return None;
    }
    let len = u32::from_be_bytes(data[0..4].try_into().ok()?) as usize;
    let crc = u32::from_be_bytes(data[4..8].try_into().ok()?);
    if len < FIXED || data.len() < HEADER + len {
        return None;
    }
    let payload = &data[HEADER..HEADER + len];
    if crc32c::crc32c(payload) != crc {
        return None;
    }
    let seq = u64::from_be_bytes(payload[0..8].try_into().ok()?);
    let ts = u64::from_be_bytes(payload[8..16].try_into().ok()?);
    let flags = payload[16];
    let mut pos = 17;
    let ex_len = *payload.get(pos)? as usize;
    pos += 1;
    let exchange = std::str::from_utf8(payload.get(pos..pos + ex_len)?).ok()?;
    pos += ex_len;
    let key_len = *payload.get(pos)? as usize;
    pos += 1;
    let routing_key = std::str::from_utf8(payload.get(pos..pos + key_len)?).ok()?;
    pos += key_len;
    let props_len = u32::from_be_bytes(payload.get(pos..pos + 4)?.try_into().ok()?) as usize;
    pos += 4;
    let props = payload.get(pos..pos + props_len)?;
    pos += props_len;
    let body = payload.get(pos..)?;
    Some(Decoded {
        seq,
        ts,
        flags,
        exchange,
        routing_key,
        props,
        body,
        len: HEADER + len,
    })
}

/// Rebuilds a message from a decoded record.
pub fn to_message(d: &Decoded<'_>) -> std::io::Result<Message> {
    let properties = BasicProperties::from_bytes(Bytes::copy_from_slice(d.props))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Message {
        exchange: d.exchange.to_string(),
        routing_key: d.routing_key.to_string(),
        properties,
        body: Bytes::copy_from_slice(d.body),
        published_at: d.ts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg() -> Message {
        let mut m = Message::new(
            "orders",
            "order.created",
            BasicProperties {
                delivery_mode: Some(2),
                expiration: Some("5000".into()),
                ..Default::default()
            },
            Bytes::from_static(b"{\"id\":1}"),
        );
        m.published_at = 1_700_000_000_000;
        m
    }

    #[test]
    fn records_round_trip() {
        let m = msg();
        let mut out = Vec::new();
        let n = encode(&mut out, 42, &m, true);
        assert_eq!(n, out.len());
        assert_eq!(n, size(&m, m.properties.to_bytes().len()));
        let d = decode(&out).unwrap();
        assert_eq!((d.seq, d.flags, d.len), (42, FLAG_PERSISTENT, n));
        assert_eq!(to_message(&d).unwrap(), m);
    }

    #[test]
    fn corruption_and_truncation_are_detected() {
        let mut out = Vec::new();
        encode(&mut out, 1, &msg(), true);
        assert!(decode(&out[..out.len() - 1]).is_none());
        let last = out.len() - 1;
        out[last] ^= 0xff;
        assert!(decode(&out).is_none());
    }
}

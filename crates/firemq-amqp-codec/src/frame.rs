use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::CodecError;
use crate::methods::Method;
use crate::properties::BasicProperties;
use crate::rw::Reader;

pub const FRAME_METHOD: u8 = 1;
pub const FRAME_HEADER: u8 = 2;
pub const FRAME_BODY: u8 = 3;
pub const FRAME_HEARTBEAT: u8 = 8;
pub const FRAME_END: u8 = 0xCE;

/// The smallest `frame_max` a peer may negotiate.
pub const FRAME_MIN_SIZE: u32 = 4096;

/// What a client sends first: `AMQP` followed by protocol id 0 and version 0-9-1.
pub const PROTOCOL_HEADER: &[u8; 8] = b"AMQP\x00\x00\x09\x01";

/// Frame overhead: type (1), channel (2), size (4) and frame end (1).
const OVERHEAD: usize = 8;

/// A decoded AMQP frame.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Method {
        channel: u16,
        method: Method,
    },
    Header {
        channel: u16,
        class_id: u16,
        body_size: u64,
        properties: BasicProperties,
    },
    Body {
        channel: u16,
        payload: Bytes,
    },
    Heartbeat,
}

impl Frame {
    pub fn channel(&self) -> u16 {
        match self {
            Frame::Method { channel, .. }
            | Frame::Header { channel, .. }
            | Frame::Body { channel, .. } => *channel,
            Frame::Heartbeat => 0,
        }
    }
}

/// Decodes one frame from the front of `buf`.
///
/// Returns `Ok(None)` when `buf` does not yet hold a whole frame. `max_frame` is the
/// negotiated maximum frame size including overhead; 0 means unlimited.
pub fn decode_frame(buf: &mut BytesMut, max_frame: u32) -> Result<Option<Frame>, CodecError> {
    if buf.len() < 7 {
        return Ok(None);
    }
    let size = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]);
    if max_frame != 0 && size as u64 + OVERHEAD as u64 > max_frame as u64 {
        return Err(CodecError::FrameTooLarge {
            size,
            max: max_frame,
        });
    }
    let total = size as usize + OVERHEAD;
    if buf.len() < total {
        buf.reserve(total - buf.len());
        return Ok(None);
    }
    let mut frame = buf.split_to(total).freeze();
    let kind = frame.get_u8();
    let channel = frame.get_u16();
    let _size = frame.get_u32();
    if frame[size as usize] != FRAME_END {
        return Err(CodecError::BadFrameEnd);
    }
    let payload = frame.slice(..size as usize);
    let frame = match kind {
        FRAME_METHOD => Frame::Method {
            channel,
            method: Method::decode(payload)?,
        },
        FRAME_HEADER => {
            let mut r = Reader::new(payload);
            let class_id = r.u16()?;
            let _weight = r.u16()?;
            let body_size = r.u64()?;
            let properties = BasicProperties::read(&mut r)?;
            Frame::Header {
                channel,
                class_id,
                body_size,
                properties,
            }
        }
        FRAME_BODY => Frame::Body { channel, payload },
        FRAME_HEARTBEAT => Frame::Heartbeat,
        other => return Err(CodecError::UnknownFrameType(other)),
    };
    Ok(Some(frame))
}

fn begin(out: &mut BytesMut, kind: u8, channel: u16) -> usize {
    out.put_u8(kind);
    out.put_u16(channel);
    let at = out.len();
    out.put_u32(0);
    at
}

fn finish(out: &mut BytesMut, at: usize) {
    let size = (out.len() - at - 4) as u32;
    out[at..at + 4].copy_from_slice(&size.to_be_bytes());
    out.put_u8(FRAME_END);
}

/// Appends a method frame.
pub fn encode_method(out: &mut BytesMut, channel: u16, method: &Method) {
    let at = begin(out, FRAME_METHOD, channel);
    method.encode(out);
    finish(out, at);
}

/// Appends a content header frame for the `basic` class.
pub fn encode_content_header(
    out: &mut BytesMut,
    channel: u16,
    body_size: u64,
    properties: &BasicProperties,
) {
    let at = begin(out, FRAME_HEADER, channel);
    out.put_u16(60);
    out.put_u16(0);
    out.put_u64(body_size);
    properties.write(out);
    finish(out, at);
}

/// Appends a content header frame whose property bytes are already encoded.
pub fn encode_content_header_raw(
    out: &mut BytesMut,
    channel: u16,
    body_size: u64,
    properties: &[u8],
) {
    let at = begin(out, FRAME_HEADER, channel);
    out.put_u16(60);
    out.put_u16(0);
    out.put_u64(body_size);
    out.put_slice(properties);
    finish(out, at);
}

/// Appends body frames, splitting `body` so no frame exceeds `frame_max`.
pub fn encode_body(out: &mut BytesMut, channel: u16, body: &[u8], frame_max: u32) {
    let chunk = if frame_max == 0 {
        usize::MAX
    } else {
        (frame_max as usize).saturating_sub(OVERHEAD).max(1)
    };
    for part in body.chunks(chunk) {
        let at = begin(out, FRAME_BODY, channel);
        out.put_slice(part);
        finish(out, at);
    }
}

/// Appends a method frame followed by its content header and body frames.
pub fn encode_content(
    out: &mut BytesMut,
    channel: u16,
    method: &Method,
    properties: &BasicProperties,
    body: &[u8],
    frame_max: u32,
) {
    encode_method(out, channel, method);
    encode_content_header(out, channel, body.len() as u64, properties);
    encode_body(out, channel, body, frame_max);
}

/// Appends a heartbeat frame.
pub fn encode_heartbeat(out: &mut BytesMut) {
    out.put_slice(&[FRAME_HEARTBEAT, 0, 0, 0, 0, 0, 0, FRAME_END]);
}

/// Appends any frame.
pub fn encode_frame(out: &mut BytesMut, frame: &Frame) {
    match frame {
        Frame::Method { channel, method } => encode_method(out, *channel, method),
        Frame::Header {
            channel,
            body_size,
            properties,
            ..
        } => encode_content_header(out, *channel, *body_size, properties),
        Frame::Body { channel, payload } => encode_body(out, *channel, payload, 0),
        Frame::Heartbeat => encode_heartbeat(out),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::methods::{BasicPublish, ChannelOpen};

    #[test]
    fn partial_frames_wait_for_more_data() {
        let mut out = BytesMut::new();
        encode_method(&mut out, 1, &ChannelOpen::default().into());
        let full = out.clone();
        let mut buf = BytesMut::from(&full[..full.len() - 1]);
        assert_eq!(decode_frame(&mut buf, 0).unwrap(), None);
        buf.extend_from_slice(&full[full.len() - 1..]);
        let frame = decode_frame(&mut buf, 0).unwrap().unwrap();
        assert_eq!(frame.channel(), 1);
        assert!(buf.is_empty());
    }

    #[test]
    fn content_splits_by_frame_max_and_reassembles() {
        let body = vec![7u8; 10_000];
        let props = BasicProperties {
            delivery_mode: Some(2),
            ..Default::default()
        };
        let method: Method = BasicPublish {
            exchange: "".into(),
            routing_key: "q".into(),
            ..Default::default()
        }
        .into();
        let mut out = BytesMut::new();
        encode_content(&mut out, 3, &method, &props, &body, 4096);
        let mut frames = Vec::new();
        while let Some(f) = decode_frame(&mut out, 4096).unwrap() {
            frames.push(f);
        }
        assert_eq!(frames.len(), 2 + 3);
        assert!(matches!(frames[0], Frame::Method { channel: 3, .. }));
        match &frames[1] {
            Frame::Header {
                body_size,
                properties,
                ..
            } => {
                assert_eq!(*body_size, 10_000);
                assert!(properties.is_persistent());
            }
            other => panic!("expected header, got {other:?}"),
        }
        let total: usize = frames[2..]
            .iter()
            .map(|f| match f {
                Frame::Body { payload, .. } => payload.len(),
                _ => 0,
            })
            .sum();
        assert_eq!(total, 10_000);
    }

    #[test]
    fn oversized_frames_are_rejected() {
        let mut out = BytesMut::new();
        encode_body(&mut out, 1, &[0u8; 5000], 0);
        assert!(matches!(
            decode_frame(&mut out, 4096),
            Err(CodecError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn bad_frame_end_is_rejected() {
        let mut out = BytesMut::new();
        encode_heartbeat(&mut out);
        let last = out.len() - 1;
        out[last] = 0;
        assert_eq!(decode_frame(&mut out, 0), Err(CodecError::BadFrameEnd));
    }
}

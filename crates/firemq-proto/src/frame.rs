use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::ProtoError;

/// Bytes a TCP client sends before its first frame.
pub const MAGIC: &[u8; 4] = b"FMQ1";

/// Largest frame accepted before `welcome`.
pub const MAX_HELLO_FRAME: u32 = 65_536;

/// A frame split into op, encoded header and payload, before header decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFrame {
    pub op: u8,
    pub header: Bytes,
    pub payload: Bytes,
}

impl RawFrame {
    /// Parses a WebSocket message: a frame without the length prefix.
    pub fn from_ws(mut msg: Bytes) -> Result<RawFrame, ProtoError> {
        if msg.len() < 5 {
            return Err(ProtoError::Malformed("frame shorter than 5 bytes"));
        }
        let op = msg.get_u8();
        let hlen = msg.get_u32() as usize;
        if hlen > msg.len() {
            return Err(ProtoError::Malformed("header length exceeds frame"));
        }
        let header = msg.split_to(hlen);
        Ok(RawFrame {
            op,
            header,
            payload: msg,
        })
    }

    /// Encodes the frame with its length prefix.
    pub fn encode(&self, out: &mut BytesMut) {
        encode_raw(out, self.op, &self.header, &self.payload);
    }
}

/// Splits one frame off the front of a TCP byte stream.
///
/// Returns `Ok(None)` until a whole frame is buffered. `max_frame` bounds the frame size
/// counted from the op byte (the value of the length prefix).
pub fn decode_frame(buf: &mut BytesMut, max_frame: u32) -> Result<Option<RawFrame>, ProtoError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if len > max_frame {
        return Err(ProtoError::FrameTooLarge {
            size: len,
            max: max_frame,
        });
    }
    if len < 5 {
        return Err(ProtoError::Malformed("frame shorter than 5 bytes"));
    }
    let total = 4 + len as usize;
    if buf.len() < total {
        buf.reserve(total - buf.len());
        return Ok(None);
    }
    let mut frame = buf.split_to(total).freeze();
    frame.advance(4);
    RawFrame::from_ws(frame).map(Some)
}

/// Appends a length-prefixed frame.
pub fn encode_raw(out: &mut BytesMut, op: u8, header: &[u8], payload: &[u8]) {
    let len = 1 + 4 + header.len() + payload.len();
    out.reserve(4 + len);
    out.put_u32(len as u32);
    out.put_u8(op);
    out.put_u32(header.len() as u32);
    out.put_slice(header);
    out.put_slice(payload);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_split_across_reads() {
        let mut out = BytesMut::new();
        encode_raw(&mut out, 0x20, b"\x80", b"hello");
        encode_raw(&mut out, 0x03, b"", b"");
        let mut buf = BytesMut::new();
        let mut frames = Vec::new();
        for byte in out.iter() {
            buf.put_u8(*byte);
            while let Some(f) = decode_frame(&mut buf, 1024).unwrap() {
                frames.push(f);
            }
        }
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].op, 0x20);
        assert_eq!(&frames[0].header[..], b"\x80");
        assert_eq!(&frames[0].payload[..], b"hello");
        assert_eq!(frames[1].op, 0x03);
        assert!(frames[1].header.is_empty());
    }

    #[test]
    fn oversized_frames_are_rejected() {
        let mut out = BytesMut::new();
        encode_raw(&mut out, 0x20, b"", &[0u8; 100]);
        assert!(matches!(
            decode_frame(&mut out, 50),
            Err(ProtoError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn header_longer_than_frame_is_malformed() {
        let msg = Bytes::from_static(&[0x20, 0, 0, 0, 9, 1]);
        assert!(RawFrame::from_ws(msg).is_err());
    }
}

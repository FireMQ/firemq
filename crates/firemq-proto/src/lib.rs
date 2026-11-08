//! The FireMQ native protocol: frames, commands and connection URLs.
//!
//! The full specification lives in `spec/fire-protocol.md` in the FireMQ repository. A frame
//! is a length prefix, an op code, a MessagePack header and a raw payload:
//!
//! ```text
//! len: u32 | op: u8 | hlen: u32 | header (MessagePack map) | payload
//! ```
//!
//! [`Command`] is the typed view of a frame. [`decode_frame`] splits frames off a TCP byte
//! stream; [`RawFrame::from_ws`] parses a WebSocket message, which has no length prefix.

mod command;
mod frame;
mod url;

pub use command::*;
pub use frame::{MAGIC, MAX_HELLO_FRAME, RawFrame, decode_frame, encode_raw};
pub use rmpv::Value;
pub use url::{ConnectUrl, Transport, UrlError};

/// Protocol major version spoken by this crate.
pub const PROTOCOL_VERSION: u32 = 1;

/// Default TCP port of the native protocol.
pub const DEFAULT_PORT: u16 = 5673;

/// Default TLS port of the native protocol.
pub const DEFAULT_TLS_PORT: u16 = 5674;

/// Default maximum frame size the server accepts after `welcome`.
pub const DEFAULT_MAX_FRAME: u32 = 16 * 1024 * 1024;

/// Errors raised while decoding frames or commands.
#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("frame of {size} bytes exceeds the maximum of {max} bytes")]
    FrameTooLarge { size: u32, max: u32 },
    #[error("malformed frame: {0}")]
    Malformed(&'static str),
    #[error("unknown op {0:#04x}")]
    UnknownOp(u8),
    #[error("invalid header for {op}: {source}")]
    Header {
        op: &'static str,
        #[source]
        source: rmp_serde::decode::Error,
    },
    #[error("bad magic bytes; expected FMQ1")]
    BadMagic,
}

/// AMQP reply codes, shared by the FireMQ protocol.
pub mod code {
    pub const OK: u16 = 200;
    pub const NO_ROUTE: u16 = 312;
    pub const CONNECTION_FORCED: u16 = 320;
    pub const ACCESS_REFUSED: u16 = 403;
    pub const NOT_FOUND: u16 = 404;
    pub const RESOURCE_LOCKED: u16 = 405;
    pub const PRECONDITION_FAILED: u16 = 406;
    pub const FRAME_ERROR: u16 = 501;
    pub const SYNTAX_ERROR: u16 = 502;
    pub const COMMAND_INVALID: u16 = 503;
    pub const RESOURCE_ERROR: u16 = 506;
    pub const NOT_ALLOWED: u16 = 530;
    pub const NOT_IMPLEMENTED: u16 = 540;
    pub const INTERNAL_ERROR: u16 = 541;
}

//! AMQP 0-9-1 codec for the FireMQ broker.
//!
//! The codec covers the wire format RabbitMQ clients speak: frames, every method of the
//! `connection`, `channel`, `exchange`, `queue`, `basic`, `confirm` and `tx` classes,
//! content headers with the 14 basic properties, and field tables with RabbitMQ's type set.
//!
//! Methods are defined by one declarative table in [`methods`], so adding a method is a
//! one-line change. Strings are validated as UTF-8 on decode. Message bodies are
//! [`bytes::Bytes`] slices of the read buffer, so they are never copied.

mod error;
mod frame;
pub mod methods;
mod properties;
mod rw;
mod table;

pub use error::CodecError;
pub use frame::{
    FRAME_BODY, FRAME_END, FRAME_HEADER, FRAME_HEARTBEAT, FRAME_METHOD, FRAME_MIN_SIZE, Frame,
    PROTOCOL_HEADER, decode_frame, encode_body, encode_content, encode_content_header,
    encode_content_header_raw, encode_frame, encode_heartbeat, encode_method,
};
pub use methods::Method;
pub use properties::BasicProperties;
pub use rw::{Reader, Writer};
pub use table::{FieldTable, LongString, Value};

/// AMQP reply codes used by RabbitMQ in `connection.close` and `channel.close`.
pub mod reply_code {
    pub const REPLY_SUCCESS: u16 = 200;
    pub const CONTENT_TOO_LARGE: u16 = 311;
    pub const NO_ROUTE: u16 = 312;
    pub const NO_CONSUMERS: u16 = 313;
    pub const CONNECTION_FORCED: u16 = 320;
    pub const INVALID_PATH: u16 = 402;
    pub const ACCESS_REFUSED: u16 = 403;
    pub const NOT_FOUND: u16 = 404;
    pub const RESOURCE_LOCKED: u16 = 405;
    pub const PRECONDITION_FAILED: u16 = 406;
    pub const FRAME_ERROR: u16 = 501;
    pub const SYNTAX_ERROR: u16 = 502;
    pub const COMMAND_INVALID: u16 = 503;
    pub const CHANNEL_ERROR: u16 = 504;
    pub const UNEXPECTED_FRAME: u16 = 505;
    pub const RESOURCE_ERROR: u16 = 506;
    pub const NOT_ALLOWED: u16 = 530;
    pub const NOT_IMPLEMENTED: u16 = 540;
    pub const INTERNAL_ERROR: u16 = 541;
}

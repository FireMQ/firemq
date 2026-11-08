/// Errors raised while decoding AMQP 0-9-1 data.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    #[error("frame of {size} bytes exceeds the negotiated maximum of {max} bytes")]
    FrameTooLarge { size: u32, max: u32 },
    #[error("frame does not end with 0xCE")]
    BadFrameEnd,
    #[error("unknown frame type {0}")]
    UnknownFrameType(u8),
    #[error("unknown method {0}.{1}")]
    UnknownMethod(u16, u16),
    #[error("unexpected end of data while decoding {0}")]
    Truncated(&'static str),
    #[error("string is not valid UTF-8")]
    InvalidUtf8,
    #[error("unknown field table value type {0:#04x}")]
    UnknownFieldType(u8),
    #[error("{0} trailing bytes after method arguments")]
    TrailingBytes(usize),
}

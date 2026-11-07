use std::fmt;

/// A broker error with an AMQP reply code. Both protocols report these codes to clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerError {
    pub code: u16,
    pub text: String,
}

pub type Result<T, E = BrokerError> = std::result::Result<T, E>;

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

impl std::error::Error for BrokerError {}

impl BrokerError {
    pub fn new(code: u16, text: impl Into<String>) -> Self {
        Self {
            code,
            text: text.into(),
        }
    }

    fn named(code: u16, name: &str, text: impl fmt::Display) -> Self {
        Self::new(code, format!("{name} - {text}"))
    }

    pub fn access_refused(text: impl fmt::Display) -> Self {
        Self::named(403, "ACCESS_REFUSED", text)
    }

    pub fn not_found(text: impl fmt::Display) -> Self {
        Self::named(404, "NOT_FOUND", text)
    }

    pub fn resource_locked(text: impl fmt::Display) -> Self {
        Self::named(405, "RESOURCE_LOCKED", text)
    }

    pub fn precondition_failed(text: impl fmt::Display) -> Self {
        Self::named(406, "PRECONDITION_FAILED", text)
    }

    pub fn command_invalid(text: impl fmt::Display) -> Self {
        Self::named(503, "COMMAND_INVALID", text)
    }

    pub fn resource_error(text: impl fmt::Display) -> Self {
        Self::named(506, "RESOURCE_ERROR", text)
    }

    pub fn not_allowed(text: impl fmt::Display) -> Self {
        Self::named(530, "NOT_ALLOWED", text)
    }

    pub fn not_implemented(text: impl fmt::Display) -> Self {
        Self::named(540, "NOT_IMPLEMENTED", text)
    }

    pub fn internal(text: impl fmt::Display) -> Self {
        Self::named(541, "INTERNAL_ERROR", text)
    }

    /// True for errors that close the whole AMQP connection rather than one channel.
    pub fn is_hard(&self) -> bool {
        !matches!(self.code, 311..=313 | 403..=406)
    }
}

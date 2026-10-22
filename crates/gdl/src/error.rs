use std::fmt;

use serde::{Deserialize, Serialize};

/// The error type of every gdl stream and call.
///
/// It is serializable so that a failure raised in one segment reaches the
/// consumer in another segment with its kind intact. An `Err` item in a
/// response stream is terminal: the runtime ends the stream after it.
#[derive(Clone, Serialize, Deserialize, thiserror::Error)]
#[error("{kind}: {message}")]
pub struct Error {
    kind: ErrorKind,
    message: String,
    /// Whether this segment received the error from another one, as a
    /// failure on a response stream, rather than raising it: the runtime
    /// counts an error once, where it was raised. It is not sent: the
    /// receiving segment sets it again.
    #[serde(skip)]
    relayed: bool,
}

/// Two errors are equal when their kind and message are: whether one was
/// relayed is the runtime's bookkeeping, not part of the error.
impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.message == other.message
    }
}

impl Eq for Error {}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.kind)
            .field("message", &self.message)
            .finish()
    }
}

impl Error {
    /// Create an error of the given kind.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            relayed: false,
        }
    }

    /// An error raised by node code.
    pub fn node(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Node, message)
    }

    /// The request, or this attempt, was cancelled.
    pub fn cancelled() -> Self {
        Self::new(ErrorKind::Cancelled, "the request was cancelled")
    }

    /// The request's deadline passed.
    pub fn deadline_exceeded() -> Self {
        Self::new(ErrorKind::DeadlineExceeded, "the request deadline passed")
    }

    /// A routing selector could not choose an instance for the request.
    pub fn routing(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Routing, message)
    }

    /// A node was invoked in a segment that does not host it.
    pub fn not_hosted(node: &str) -> Self {
        Self::new(
            ErrorKind::NotHosted,
            format!("node `{node}` is not hosted by this segment"),
        )
    }

    /// The kind of this error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The human-readable message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The error, marked as received from another segment.
    #[doc(hidden)]
    pub fn relayed(mut self) -> Self {
        self.relayed = true;
        self
    }

    /// Whether the error was received from another segment, not raised in
    /// this one.
    pub(crate) fn is_relayed(&self) -> bool {
        self.relayed
    }
}

/// What failed. Callers branch on the kind; the message is for people.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Node code reported a failure.
    Node,
    /// The request or attempt was cancelled.
    Cancelled,
    /// The request's deadline passed.
    DeadlineExceeded,
    /// A stream ended without its producer finishing it.
    Truncated,
    /// A node was invoked where it is not hosted.
    NotHosted,
    /// An instance refused work because it is at capacity.
    Overloaded,
    /// The network or the messaging layer failed.
    Transport,
    /// A message could not be encoded or decoded.
    Codec,
    /// A placement or configuration is invalid.
    Placement,
    /// A routing selector could not choose an instance for the request.
    Routing,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Node => "node error",
            Self::Cancelled => "cancelled",
            Self::DeadlineExceeded => "deadline exceeded",
            Self::Truncated => "truncated",
            Self::NotHosted => "not hosted",
            Self::Overloaded => "overloaded",
            Self::Transport => "transport error",
            Self::Codec => "codec error",
            Self::Placement => "placement error",
            Self::Routing => "routing error",
        })
    }
}

/// The result type used across gdl.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<gdl_model::Error> for Error {
    fn from(error: gdl_model::Error) -> Self {
        Self::new(ErrorKind::Placement, error.to_string())
    }
}

impl From<gdl_model::PlacementErrors> for Error {
    fn from(errors: gdl_model::PlacementErrors) -> Self {
        Self::new(ErrorKind::Placement, errors.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The flag stays in the segment that set it, and two errors that
    /// differ only in it are equal.
    #[test]
    fn whether_an_error_was_relayed_is_not_sent_or_compared() {
        let error = Error::node("x").relayed();
        assert!(error.is_relayed());
        assert!(error.clone().is_relayed());
        assert_eq!(error, Error::node("x"));
        assert_eq!(format!("{error:?}"), format!("{:?}", Error::node("x")));
        let bytes = rmp_serde::to_vec(&error).expect("encode");
        let received: Error = rmp_serde::from_slice(&bytes).expect("decode");
        assert!(!received.is_relayed());
        assert_eq!(received, error);
    }
}

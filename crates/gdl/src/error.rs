use std::fmt;

use serde::{Deserialize, Serialize};

/// The error type of every gdl stream and call.
///
/// It is serializable so that a failure raised in one segment reaches the
/// consumer in another segment with its kind intact. An `Err` item in a
/// response stream is terminal: the runtime ends the stream after it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{kind}: {message}")]
pub struct Error {
    kind: ErrorKind,
    message: String,
}

impl Error {
    /// Create an error of the given kind.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
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
        })
    }
}

/// The result type used across gdl.
pub type Result<T, E = Error> = std::result::Result<T, E>;

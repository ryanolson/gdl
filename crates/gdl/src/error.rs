use std::fmt;

use serde::{Deserialize, Serialize};

/// The error type of every gdl stream and call.
///
/// It is serializable so that a failure raised in one segment reaches the
/// consumer in another segment with its kind intact. An `Err` item in a
/// response stream is terminal: the runtime ends the stream after it.
#[derive(Clone, Deserialize, thiserror::Error)]
#[serde(from = "Wire<String>")]
#[error("{kind}: {message}")]
pub struct Error {
    kind: ErrorKind,
    message: String,
    /// Whether this segment received the error from another one, as a
    /// failure on a response stream, rather than raising it: the runtime
    /// counts an error once, where it was raised. It is not sent: the
    /// receiving segment sets it again.
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
    /// A hand-off into a cell went unanswered, or the transport failed
    /// after it may have left: the receiver may have taken the request and
    /// run it, so trying it again may run it twice. Raised for a chooser's
    /// hand-off into another class; any other hand-off that goes
    /// unanswered fails with `Transport`.
    OutcomeUnknown,
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
            Self::OutcomeUnknown => "outcome unknown",
        })
    }
}

/// An error as it crosses: its kind, among those every peer can read, and
/// its message. A peer of an earlier build refuses a kind it does not know,
/// and with it the whole item that carries the error, so a kind added
/// since is sent as the earlier kind nearest it, with its own name at the
/// start of the message ([`OUTCOME_UNKNOWN`]), and read back here. An extra
/// field would not do: such a peer refuses an error of three fields too.
#[derive(Serialize, Deserialize)]
struct Wire<M> {
    kind: WireKind,
    message: M,
}

/// The kinds every peer reads. Never add to it: a new kind is sent as one
/// of these.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum WireKind {
    Node,
    Cancelled,
    DeadlineExceeded,
    Truncated,
    NotHosted,
    Overloaded,
    Transport,
    Codec,
    Placement,
    Routing,
}

/// What starts the message of an [`ErrorKind::OutcomeUnknown`] sent as a
/// `Transport` error. A peer of an earlier build shows it as written.
const OUTCOME_UNKNOWN: &str = "[outcome unknown] ";

impl Serialize for Error {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let message = std::borrow::Cow::Borrowed(self.message.as_str());
        let (kind, message) = match self.kind {
            ErrorKind::Node => (WireKind::Node, message),
            ErrorKind::Cancelled => (WireKind::Cancelled, message),
            ErrorKind::DeadlineExceeded => (WireKind::DeadlineExceeded, message),
            ErrorKind::Truncated => (WireKind::Truncated, message),
            ErrorKind::NotHosted => (WireKind::NotHosted, message),
            ErrorKind::Overloaded => (WireKind::Overloaded, message),
            ErrorKind::Transport => (WireKind::Transport, message),
            ErrorKind::Codec => (WireKind::Codec, message),
            ErrorKind::Placement => (WireKind::Placement, message),
            ErrorKind::Routing => (WireKind::Routing, message),
            ErrorKind::OutcomeUnknown => (
                WireKind::Transport,
                std::borrow::Cow::Owned(format!("{OUTCOME_UNKNOWN}{message}")),
            ),
        };
        Wire { kind, message }.serialize(serializer)
    }
}

impl From<Wire<String>> for Error {
    fn from(Wire { kind, message }: Wire<String>) -> Self {
        let kind = match kind {
            WireKind::Node => ErrorKind::Node,
            WireKind::Cancelled => ErrorKind::Cancelled,
            WireKind::DeadlineExceeded => ErrorKind::DeadlineExceeded,
            WireKind::Truncated => ErrorKind::Truncated,
            WireKind::NotHosted => ErrorKind::NotHosted,
            WireKind::Overloaded => ErrorKind::Overloaded,
            WireKind::Transport => {
                if let Some(rest) = message.strip_prefix(OUTCOME_UNKNOWN) {
                    return Self::new(ErrorKind::OutcomeUnknown, rest);
                }
                ErrorKind::Transport
            }
            WireKind::Codec => ErrorKind::Codec,
            WireKind::Placement => ErrorKind::Placement,
            WireKind::Routing => ErrorKind::Routing,
        };
        Self::new(kind, message)
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

    /// The error and its kinds as the previous build declares them.
    mod previous {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Serialize, Deserialize)]
        pub struct Error {
            pub kind: ErrorKind,
            pub message: String,
        }

        #[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
        pub enum ErrorKind {
            Node,
            Cancelled,
            DeadlineExceeded,
            Truncated,
            NotHosted,
            Overloaded,
            Transport,
            Codec,
            Placement,
            Routing,
        }
    }

    const EARLIER: [(ErrorKind, previous::ErrorKind); 10] = [
        (ErrorKind::Node, previous::ErrorKind::Node),
        (ErrorKind::Cancelled, previous::ErrorKind::Cancelled),
        (
            ErrorKind::DeadlineExceeded,
            previous::ErrorKind::DeadlineExceeded,
        ),
        (ErrorKind::Truncated, previous::ErrorKind::Truncated),
        (ErrorKind::NotHosted, previous::ErrorKind::NotHosted),
        (ErrorKind::Overloaded, previous::ErrorKind::Overloaded),
        (ErrorKind::Transport, previous::ErrorKind::Transport),
        (ErrorKind::Codec, previous::ErrorKind::Codec),
        (ErrorKind::Placement, previous::ErrorKind::Placement),
        (ErrorKind::Routing, previous::ErrorKind::Routing),
    ];

    /// Every kind the previous build has crosses in both directions as it
    /// did, byte for byte.
    #[test]
    fn the_earlier_kinds_cross_as_they_did() {
        for (kind, earlier) in EARLIER {
            let error = Error::new(kind, "why");
            let bytes = rmp_serde::to_vec(&error).expect("encode");
            let then = previous::Error {
                kind: earlier,
                message: "why".into(),
            };
            assert_eq!(bytes, rmp_serde::to_vec(&then).expect("encode"), "{kind}");
            let back: Error = rmp_serde::from_slice(&bytes).expect("decode");
            assert_eq!(back, error);
        }
    }

    /// A peer of the previous build reads an unknown outcome as the
    /// transport error it would have raised itself, with the message
    /// saying what it was; this build reads it back whole.
    #[test]
    fn an_unknown_outcome_crosses_to_a_peer_that_does_not_know_the_kind() {
        let error = Error::new(ErrorKind::OutcomeUnknown, "no answer from cell m1");
        let bytes = rmp_serde::to_vec(&error).expect("encode");
        let then: previous::Error =
            rmp_serde::from_slice(&bytes).expect("the previous build reads it");
        assert_eq!(then.kind, previous::ErrorKind::Transport);
        assert_eq!(then.message, "[outcome unknown] no answer from cell m1");
        let back: Error = rmp_serde::from_slice(&bytes).expect("decode");
        assert_eq!(back, error);
        assert_eq!(back.kind(), ErrorKind::OutcomeUnknown);
        assert_eq!(back.message(), "no answer from cell m1");
        let json = serde_json::to_string(&error).expect("encode");
        assert_eq!(serde_json::from_str::<Error>(&json).expect("decode"), error);
    }
}

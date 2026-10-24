use std::fmt;
use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

/// The error type of every gdl stream and call.
///
/// It is serializable so that a failure raised in one segment reaches the
/// consumer in another segment with its kind intact. An `Err` item in a
/// response stream is terminal: the runtime ends the stream after it.
///
/// It crosses as a struct of two fields, `kind` and `message`, with the
/// kind sent by its name, as serde sends a unit variant: in msgpack, as
/// velo and the `msgpack` edges send it, and in JSON alike. A kind a
/// later build added is read as [`ErrorKind::Transport`] and keeps the
/// name it was sent under: [`Error::unknown_kind`] returns it, `Display`
/// shows it, and a segment that sends the error on sends it under that
/// name, so that a peer that knows the kind reads it as itself.
#[derive(Clone, Deserialize)]
#[serde(try_from = "Wire<Received, String>")]
pub struct Error {
    kind: ErrorKind,
    /// The message, after the name of the kind it was sent under when this
    /// build does not know that kind: one buffer, so that keeping the name
    /// adds nothing to the size of every `Result` that holds an error.
    text: String,
    /// The length of that name at the start of `text`, plus one; `None`
    /// for a kind this build knows.
    unknown: Option<NonZeroU32>,
    /// Whether this segment received the error from another one, as a
    /// failure on a response stream, rather than raising it: the runtime
    /// counts an error once, where it was raised. It is not sent: the
    /// receiving segment sets it again.
    relayed: bool,
}

/// Two errors are equal when their kind, the name of a kind this build
/// does not know, and message are: whether one was relayed is the
/// runtime's bookkeeping, not part of the error.
impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.unknown == other.unknown && self.text == other.text
    }
}

impl Eq for Error {}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Error");
        debug.field("kind", &self.kind);
        if let Some(name) = self.unknown_kind() {
            debug.field("unknown_kind", &name);
        }
        debug.field("message", &self.message()).finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.unknown_kind() {
            Some(name) => write!(f, "{} (unknown kind {name}): {}", self.kind, self.message()),
            None => write!(f, "{}: {}", self.kind, self.message()),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    /// Create an error of the given kind.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            text: message.into(),
            unknown: None,
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

    /// The kind of this error: [`ErrorKind::Transport`] for one received
    /// under a kind this build does not know.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The human-readable message.
    pub fn message(&self) -> &str {
        &self.text[self.split()..]
    }

    /// The name of the kind the error was received under, when this build
    /// does not know it: a kind a later build added, as its sender named
    /// it, or, for one sent as its index, the index in decimal. `None` for
    /// every kind this build knows.
    pub fn unknown_kind(&self) -> Option<&str> {
        self.unknown.map(|_| &self.text[..self.split()])
    }

    /// Where the message starts in `text`.
    fn split(&self) -> usize {
        self.unknown.map_or(0, |length| length.get() as usize - 1)
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
///
/// A kind is sent by its name. A peer of a build that does not know a
/// kind reads it as `Transport`, keeping its name ([`Error::unknown_kind`]),
/// so that a kind added later rolls out one cell at a time; a build from
/// before `OutcomeUnknown` refuses an unknown kind, and with it the stream
/// item that carries it.
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
    /// The network or the messaging layer failed, or the error came from a
    /// peer under a kind this build does not know.
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

/// Every kind by the name it is sent under, in the order it is declared,
/// which is its index: what an error's kind is read from.
const KINDS: [(&str, ErrorKind); 11] = [
    ("Node", ErrorKind::Node),
    ("Cancelled", ErrorKind::Cancelled),
    ("DeadlineExceeded", ErrorKind::DeadlineExceeded),
    ("Truncated", ErrorKind::Truncated),
    ("NotHosted", ErrorKind::NotHosted),
    ("Overloaded", ErrorKind::Overloaded),
    ("Transport", ErrorKind::Transport),
    ("Codec", ErrorKind::Codec),
    ("Placement", ErrorKind::Placement),
    ("Routing", ErrorKind::Routing),
    ("OutcomeUnknown", ErrorKind::OutcomeUnknown),
];

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

/// An error as it crosses: its kind and its message.
#[derive(Serialize, Deserialize)]
struct Wire<K, M> {
    kind: K,
    message: M,
}

/// A kind as it is sent: a known one as the enum's variant, so that every
/// kind encodes as it always has, and an unknown one by the name it came
/// under, which a unit variant's encoding is in msgpack and JSON alike.
enum Sent<'a> {
    Known(ErrorKind),
    Unknown(&'a str),
}

impl Serialize for Sent<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Known(kind) => kind.serialize(serializer),
            Self::Unknown(name) => serializer.serialize_str(name),
        }
    }
}

impl Serialize for Error {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let kind = match self.unknown_kind() {
            Some(name) => Sent::Unknown(name),
            None => Sent::Known(self.kind),
        };
        Wire {
            kind,
            message: self.message(),
        }
        .serialize(serializer)
    }
}

/// A kind as it is read: one this build knows, or the name of one it does
/// not.
enum Received {
    Known(ErrorKind),
    Unknown(String),
}

/// Read as an enum, as the derived `ErrorKind` is, so that every form a
/// kind was read from before is read the same way; only a name or an
/// index this build does not know is read where it was refused.
impl<'de> Deserialize<'de> for Received {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Kind;
        impl<'de> serde::de::Visitor<'de> for Kind {
            type Value = Received;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an error kind")
            }
            fn visit_enum<A: serde::de::EnumAccess<'de>>(
                self,
                data: A,
            ) -> Result<Received, A::Error> {
                let (kind, variant) = data.variant_seed(Name)?;
                serde::de::VariantAccess::unit_variant(variant)?;
                Ok(kind)
            }
        }
        // The variant's name, as its identifier.
        struct Name;
        impl serde::de::Visitor<'_> for Name {
            type Value = Received;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an error kind's name or index")
            }
            fn visit_str<E: serde::de::Error>(self, name: &str) -> Result<Received, E> {
                // A known name is matched where it lies: reading it
                // allocates nothing.
                Ok(KINDS.iter().find(|(known, _)| *known == name).map_or_else(
                    || Received::Unknown(name.to_string()),
                    |(_, kind)| Received::Known(*kind),
                ))
            }
            fn visit_bytes<E: serde::de::Error>(self, name: &[u8]) -> Result<Received, E> {
                match std::str::from_utf8(name) {
                    Ok(name) => self.visit_str(name),
                    Err(_) => Ok(Received::Unknown(
                        String::from_utf8_lossy(name).into_owned(),
                    )),
                }
            }
            fn visit_u64<E: serde::de::Error>(self, index: u64) -> Result<Received, E> {
                Ok(usize::try_from(index)
                    .ok()
                    .and_then(|index| KINDS.get(index))
                    .map_or_else(
                        || Received::Unknown(index.to_string()),
                        |(_, kind)| Received::Known(*kind),
                    ))
            }
        }
        // The variant's identifier is read through this.
        impl<'de> serde::de::DeserializeSeed<'de> for Name {
            type Value = Received;
            fn deserialize<D: serde::Deserializer<'de>>(
                self,
                deserializer: D,
            ) -> Result<Received, D::Error> {
                deserializer.deserialize_identifier(self)
            }
        }
        deserializer.deserialize_enum("ErrorKind", &[], Kind)
    }
}

impl TryFrom<Wire<Received, String>> for Error {
    type Error = String;

    fn try_from(Wire { kind, message }: Wire<Received, String>) -> Result<Self, String> {
        match kind {
            Received::Known(kind) => Ok(Self::new(kind, message)),
            Received::Unknown(mut text) => {
                let length = u32::try_from(text.len() + 1)
                    .ok()
                    .and_then(NonZeroU32::new)
                    .ok_or_else(|| "an error kind's name is too long".to_string())?;
                text.push_str(&message);
                Ok(Self {
                    kind: ErrorKind::Transport,
                    text,
                    unknown: Some(length),
                    relayed: false,
                })
            }
        }
    }
}

/// The result type used across gdl.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<gdl_model::Error> for Error {
    fn from(error: gdl_model::Error) -> Self {
        Self::new(ErrorKind::Placement, error.to_string())
    }
}

impl From<gdl_model::Problems> for Error {
    fn from(problems: gdl_model::Problems) -> Self {
        Self::new(ErrorKind::Placement, problems.to_string())
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

    /// The error as velo sends it in a stream item, a struct as an array.
    fn array<T: Serialize>(value: &T) -> Vec<u8> {
        rmp_serde::to_vec(value).expect("encode")
    }

    /// The error as a `msgpack` edge sends it, a struct as a map.
    fn named<T: Serialize>(value: &T) -> Vec<u8> {
        let mut bytes = Vec::new();
        rmp_serde::encode::write_named(&mut bytes, value).expect("encode");
        bytes
    }

    fn json<T: Serialize>(value: &T) -> Vec<u8> {
        serde_json::to_vec(value).expect("encode")
    }

    /// Each encoding an error crosses in, with its decoder.
    type Encode<T> = fn(&T) -> Vec<u8>;
    type Decode<T> = fn(&[u8]) -> T;

    fn encodings<T: Serialize, U: serde::de::DeserializeOwned>()
    -> [(&'static str, Encode<T>, Decode<U>); 3] {
        let msgpack: Decode<U> = |bytes| rmp_serde::from_slice(bytes).expect("decode");
        [
            ("msgpack array", array::<T>, msgpack),
            ("msgpack map", named::<T>, msgpack),
            ("json", json::<T>, |bytes| {
                serde_json::from_slice(bytes).expect("decode")
            }),
        ]
    }

    /// The flag stays in the segment that set it, and two errors that
    /// differ only in it are equal.
    #[test]
    fn whether_an_error_was_relayed_is_not_sent_or_compared() {
        let error = Error::node("x").relayed();
        assert!(error.is_relayed());
        assert!(error.clone().is_relayed());
        assert_eq!(error, Error::node("x"));
        assert_eq!(format!("{error:?}"), format!("{:?}", Error::node("x")));
        let received: Error = rmp_serde::from_slice(&array(&error)).expect("decode");
        assert!(!received.is_relayed());
        assert_eq!(received, error);
    }

    /// An error is in every stream item's result and every hop's future:
    /// keeping an unknown kind's name costs it no size.
    #[test]
    fn an_error_is_as_large_as_it_was() {
        assert_eq!(std::mem::size_of::<Error>(), 32);
    }

    /// The error and its kinds as the build before `OutcomeUnknown`
    /// declares them.
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

    /// The error as a later build declares it, with a kind this build
    /// does not know.
    mod later {
        use serde::Serialize;

        #[derive(Serialize)]
        pub struct Error {
            pub kind: ErrorKind,
            pub message: String,
        }

        #[derive(Serialize)]
        pub enum ErrorKind {
            Throttled,
        }
    }

    /// Each kind the previous build has, with the bytes velo sends for it
    /// with the message `why`, as that build sent them.
    const EARLIER: [(ErrorKind, previous::ErrorKind, &[u8]); 10] = [
        (
            ErrorKind::Node,
            previous::ErrorKind::Node,
            b"\x92\xa4Node\xa3why",
        ),
        (
            ErrorKind::Cancelled,
            previous::ErrorKind::Cancelled,
            b"\x92\xa9Cancelled\xa3why",
        ),
        (
            ErrorKind::DeadlineExceeded,
            previous::ErrorKind::DeadlineExceeded,
            b"\x92\xb0DeadlineExceeded\xa3why",
        ),
        (
            ErrorKind::Truncated,
            previous::ErrorKind::Truncated,
            b"\x92\xa9Truncated\xa3why",
        ),
        (
            ErrorKind::NotHosted,
            previous::ErrorKind::NotHosted,
            b"\x92\xa9NotHosted\xa3why",
        ),
        (
            ErrorKind::Overloaded,
            previous::ErrorKind::Overloaded,
            b"\x92\xaaOverloaded\xa3why",
        ),
        (
            ErrorKind::Transport,
            previous::ErrorKind::Transport,
            b"\x92\xa9Transport\xa3why",
        ),
        (
            ErrorKind::Codec,
            previous::ErrorKind::Codec,
            b"\x92\xa5Codec\xa3why",
        ),
        (
            ErrorKind::Placement,
            previous::ErrorKind::Placement,
            b"\x92\xa9Placement\xa3why",
        ),
        (
            ErrorKind::Routing,
            previous::ErrorKind::Routing,
            b"\x92\xa7Routing\xa3why",
        ),
    ];

    /// Every kind the previous build has is sent byte for byte as that
    /// build sent it, in each encoding, and read back.
    #[test]
    fn the_earlier_kinds_cross_as_they_did() {
        for (kind, earlier, pinned) in EARLIER {
            let error = Error::new(kind, "why");
            assert_eq!(array(&error), pinned, "{kind}");
            // As a map, the kind and the message are each keyed by name.
            let name = &pinned[1..pinned.len() - 4];
            let mut map = b"\x82\xa4kind".to_vec();
            map.extend_from_slice(name);
            map.extend_from_slice(b"\xa7message\xa3why");
            assert_eq!(named(&error), map, "{kind}");
            let then = previous::Error {
                kind: earlier,
                message: "why".into(),
            };
            for (encoding, encode, decode) in encodings::<Error, Error>() {
                assert_eq!(
                    encode(&error),
                    encode_previous(encoding, &then),
                    "{kind} {encoding}"
                );
                assert_eq!(decode(&encode(&error)), error, "{kind} {encoding}");
            }
        }
        assert_eq!(
            json(&Error::node("why")),
            br#"{"kind":"Node","message":"why"}"#
        );
    }

    fn encode_previous(encoding: &str, error: &previous::Error) -> Vec<u8> {
        encodings::<previous::Error, previous::Error>()
            .into_iter()
            .find(|(name, _, _)| *name == encoding)
            .map(|(_, encode, _)| encode(error))
            .expect("an encoding")
    }

    /// An unknown outcome is sent as its own kind, its message as written,
    /// and read back as it was between two peers of this build.
    #[test]
    fn an_unknown_outcome_crosses_as_itself() {
        let error = Error::new(ErrorKind::OutcomeUnknown, "no answer");
        assert_eq!(array(&error), b"\x92\xaeOutcomeUnknown\xa9no answer");
        assert_eq!(
            named(&error),
            b"\x82\xa4kind\xaeOutcomeUnknown\xa7message\xa9no answer"
        );
        assert_eq!(
            json(&error),
            br#"{"kind":"OutcomeUnknown","message":"no answer"}"#
        );
        for (encoding, encode, decode) in encodings::<Error, Error>() {
            let back = decode(&encode(&error));
            assert_eq!(back, error, "{encoding}");
            assert_eq!(back.kind(), ErrorKind::OutcomeUnknown, "{encoding}");
            assert_eq!(back.message(), "no answer", "{encoding}");
            assert_eq!(back.unknown_kind(), None, "{encoding}");
        }
    }

    /// A peer of the previous build refuses the kind: this build cannot
    /// roll cell by cell from it.
    #[test]
    fn the_previous_build_refuses_an_unknown_outcome() {
        let error = Error::new(ErrorKind::OutcomeUnknown, "no answer");
        assert!(rmp_serde::from_slice::<previous::Error>(&array(&error)).is_err());
        assert!(rmp_serde::from_slice::<previous::Error>(&named(&error)).is_err());
        assert!(serde_json::from_slice::<previous::Error>(&json(&error)).is_err());
    }

    /// A transport error's message is sent as written, whatever it begins
    /// with.
    #[test]
    fn a_transport_message_is_sent_as_written() {
        for message in [
            "[outcome unknown] peer refused",
            "[outcome unknown] ",
            "[transport] peer refused",
        ] {
            let error = Error::new(ErrorKind::Transport, message);
            let then = previous::Error {
                kind: previous::ErrorKind::Transport,
                message: message.into(),
            };
            for (encoding, encode, decode) in encodings::<Error, Error>() {
                let bytes = encode(&error);
                assert_eq!(
                    bytes,
                    encode_previous(encoding, &then),
                    "{message:?} {encoding}"
                );
                let back = decode(&bytes);
                assert_eq!(back.kind(), ErrorKind::Transport, "{message:?} {encoding}");
                assert_eq!(back.message(), message, "{encoding}");
            }
        }
    }

    /// A kind a later build added is read as a transport error that keeps
    /// the kind's name, shown and sent on as it came, in each encoding.
    #[test]
    fn a_kind_this_build_does_not_know_is_a_transport_error_with_its_name() {
        let sent = later::Error {
            kind: later::ErrorKind::Throttled,
            message: "slow down".into(),
        };
        for (encoding, encode, decode) in encodings::<later::Error, Error>() {
            let bytes = encode(&sent);
            let error = decode(&bytes);
            assert_eq!(error.kind(), ErrorKind::Transport, "{encoding}");
            assert_eq!(error.unknown_kind(), Some("Throttled"), "{encoding}");
            assert_eq!(error.message(), "slow down", "{encoding}");
            assert_eq!(
                error.to_string(),
                "transport error (unknown kind Throttled): slow down",
                "{encoding}"
            );
            // A segment that relays it sends it on under its own name, for
            // a peer that knows it.
            let relayed = encodings::<Error, Error>()
                .into_iter()
                .find(|(name, _, _)| *name == encoding)
                .map(|(_, encode, _)| encode(&error.clone().relayed()))
                .expect("an encoding");
            assert_eq!(relayed, bytes, "{encoding}");
            assert_ne!(error, Error::new(ErrorKind::Transport, "slow down"));
        }
    }

    /// msgpack also reads a kind sent as its index, as serde's own enums
    /// do: a known index is its kind, and an unknown one a transport error
    /// named by the index.
    #[test]
    fn a_kind_sent_as_its_index_is_read() {
        let known: Error = rmp_serde::from_slice(b"\x92\x0a\xa3why").expect("decode");
        assert_eq!(known, Error::new(ErrorKind::OutcomeUnknown, "why"));
        let unknown: Error = rmp_serde::from_slice(b"\x92\x0b\xa3why").expect("decode");
        assert_eq!(unknown.kind(), ErrorKind::Transport);
        assert_eq!(unknown.unknown_kind(), Some("11"));
        assert_eq!(unknown.message(), "why");
    }

    /// The table an error's kind is read from holds every kind declared,
    /// at its index, under its name: a kind declared and left out of it
    /// would be read back from a peer of this build as unknown. The kinds
    /// are found from the derived encoding, by index, not from the table.
    #[test]
    fn the_kinds_read_are_every_kind_declared() {
        let declared: Vec<ErrorKind> = (0u8..=127)
            .map_while(|index| rmp_serde::from_slice::<ErrorKind>(&[index]).ok())
            .collect();
        assert_eq!(declared.len(), KINDS.len());
        for (index, kind) in declared.iter().enumerate() {
            assert_eq!(KINDS[index].1, *kind);
            assert_eq!(
                serde_json::to_value(kind).expect("encode"),
                serde_json::json!(KINDS[index].0)
            );
        }
        // JSON never read a kind by its index, and does not now.
        assert!(serde_json::from_str::<Error>(r#"{"kind":10,"message":"m"}"#).is_err());
    }

    /// Every kind is sent under its name, which is its declared name, and
    /// read back.
    #[test]
    fn every_kind_crosses_under_its_name() {
        for (index, (name, kind)) in KINDS.iter().enumerate() {
            assert_eq!(
                serde_json::to_value(kind).expect("encode"),
                serde_json::json!(name)
            );
            let error = Error::new(*kind, "m");
            assert_eq!(
                rmp_serde::from_slice::<Error>(&array(&error)).expect("decode"),
                error
            );
            let mut by_index = vec![0x92, u8::try_from(index).expect("a fixint")];
            by_index.extend_from_slice(b"\xa1m");
            assert_eq!(
                rmp_serde::from_slice::<Error>(&by_index).expect("decode"),
                error
            );
        }
    }
}

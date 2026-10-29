//! What crosses a segment boundary besides stream items: the request
//! envelopes and their replies. Every message carries the deployment
//! identity, so an instance built from another contract or placement refuses
//! it instead of misreading it.

use std::collections::BTreeMap;

use bytes::Bytes;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::fabric::ReplyTo;
use gdl::{Cx, Deadline, RequestId};
use gdl::{Error, ErrorKind, Result};

/// The envelope format. A receiver rejects any other version. Version 4
/// carries each stream item as velo's own byte item, which version 3
/// segments cannot read, so the two refuse each other at the envelope that
/// starts the stream.
pub(crate) const VERSION: u8 = 4;

/// The room in front of a request's head for the rest of its envelope: an
/// envelope with no meta in its context needs about 100 bytes.
pub(crate) const HEADER_ROOM: usize = 256;

/// The buffer a request's head is first written into, with its room: a
/// head of up to 768 bytes is serialized once. A larger one is serialized
/// to count it, then into a buffer of exactly its size.
const FIRST_BUFFER: usize = 1024;

/// Refuse an envelope of a version other than [`VERSION`].
pub(crate) fn check_version(version: u8) -> Result<()> {
    if version == VERSION {
        return Ok(());
    }
    Err(Error::new(
        ErrorKind::Placement,
        format!("envelope version {version} is not supported"),
    ))
}

/// Which deployment a message belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Identity {
    pub(crate) contract: u64,
    pub(crate) placement: u64,
}

/// The part of a [`Cx`] that crosses segments. Its default is what an
/// entry leaves in an envelope whose context it took.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct WireCx {
    id: u128,
    meta: BTreeMap<String, String>,
    deadline: Option<u64>,
}

impl WireCx {
    pub(crate) fn of(cx: &Cx) -> Self {
        Self {
            id: cx.id().as_u128(),
            meta: cx.meta().clone(),
            deadline: cx.deadline().map(Deadline::unix_nanos),
        }
    }

    /// Rebuild the context on arrival, or fail if its deadline has passed.
    /// `wall_now` reads this instance's wall clock, in Unix nanoseconds.
    pub(crate) fn arrive(
        self,
        skew: std::time::Duration,
        wall_now: impl FnOnce() -> u64,
    ) -> Result<Cx> {
        let deadline = match self.deadline {
            None => None,
            Some(unix_nanos) => Some(
                Deadline::from_wall(unix_nanos, skew, wall_now)
                    .ok_or_else(Error::deadline_exceeded)?,
            ),
        };
        Ok(Cx::arrived(
            RequestId::from_u128(self.id),
            self.meta,
            deadline,
        ))
    }

    pub(crate) fn root(&self) -> u128 {
        self.id
    }

    /// Add what a routing selector chose to tell the receiver.
    pub(crate) fn add_meta(&mut self, meta: Vec<(String, String)>) {
        self.meta.extend(meta);
    }
}

/// A request handed to the next segment on a hop's forward path.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Forward {
    pub(crate) version: u8,
    pub(crate) identity: Identity,
    pub(crate) hop: u16,
    /// Position on the hop's forward path of the first stage to run.
    pub(crate) pos: u16,
    pub(crate) cx: WireCx,
    pub(crate) reply: ReplyTo,
    pub(crate) payload: Head,
}

/// `rmp_serde`'s encoding of a [`Forward`] starts with this: an array of
/// its seven fields.
const FIELDS: u8 = 0x97;

impl Forward {
    /// The envelope's bytes, as an offer sends them, which [`encode`]
    /// gives too. The first offer writes the envelope's other fields into
    /// the room in front of the head, so the bytes are the head's own
    /// buffer. A later one, as a failover makes, or one whose fields do not
    /// fit the room, copies the head after them.
    pub(crate) fn wire(&mut self) -> Result<Bytes> {
        let mut header = [0u8; HEADER_ROOM];
        let mut free = &mut header[..];
        let fits = self.write_header(&mut free).is_ok();
        let len = HEADER_ROOM - free.len();
        let at = self.payload.at;
        if let (true, Buffer::Room(buffer)) = (fits && len <= at, &mut self.payload.buffer) {
            let start = at - len;
            buffer[start..at].copy_from_slice(&header[..len]);
            let bytes = Bytes::from(std::mem::take(buffer));
            self.payload.buffer = Buffer::Sent(bytes.clone());
            return Ok(bytes.slice(start..));
        }
        let mut bytes = Vec::with_capacity(2 * HEADER_ROOM + self.payload.len());
        self.write_header(&mut bytes)?;
        bytes.extend_from_slice(&self.payload);
        Ok(Bytes::from(bytes))
    }

    /// Everything [`encode`] writes before the head's bytes: the array,
    /// the other fields, and the head's binary length.
    fn write_header(&self, out: &mut impl std::io::Write) -> Result<()> {
        out.write_all(&[FIELDS]).map_err(codec)?;
        write(out, &self.version)?;
        write(out, &self.identity)?;
        write(out, &self.hop)?;
        write(out, &self.pos)?;
        write(out, &self.cx)?;
        write(out, &self.reply)?;
        let len = self.payload.len();
        let marker = if let Ok(len) = u8::try_from(len) {
            out.write_all(&[0xc4, len])
        } else if let Ok(len) = u16::try_from(len) {
            out.write_all(&[0xc5])
                .and_then(|()| out.write_all(&len.to_be_bytes()))
        } else if let Ok(len) = u32::try_from(len) {
            out.write_all(&[0xc6])
                .and_then(|()| out.write_all(&len.to_be_bytes()))
        } else {
            return Err(Error::new(
                ErrorKind::Codec,
                format!("a request head of {len} bytes is over msgpack's 4 GiB"),
            ));
        };
        marker.map_err(codec)
    }
}

/// A request's encoded head, behind room for the rest of the envelope that
/// carries it, so that the head crosses from the buffer it was encoded
/// into (docs/src/concepts/crossings.md).
#[doc(hidden)]
pub struct Head {
    buffer: Buffer,
    /// Where the head starts in `buffer`.
    at: usize,
}

enum Buffer {
    /// Not offered yet: the bytes before the head are free.
    Room(Vec<u8>),
    /// Offered once, and shared with that send.
    Sent(Bytes),
}

impl Head {
    /// Encode `value` behind [`HEADER_ROOM`] with `encode`, which sizes
    /// the buffer once (`gdl::__runtime::encoder_sized`): a head larger
    /// than [`FIRST_BUFFER`] leaves no spare room, and is not moved after
    /// it is written.
    pub(crate) fn encode<T: ?Sized>(
        value: &T,
        encode: fn(&T, &mut Vec<u8>) -> Result<()>,
    ) -> Result<Self> {
        let mut buffer = Vec::with_capacity(FIRST_BUFFER);
        buffer.resize(HEADER_ROOM, 0);
        encode(value, &mut buffer)?;
        Ok(Self {
            buffer: Buffer::Room(buffer),
            at: HEADER_ROOM,
        })
    }
}

impl std::ops::Deref for Head {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match &self.buffer {
            Buffer::Room(buffer) => &buffer[self.at..],
            Buffer::Sent(bytes) => &bytes[self.at..],
        }
    }
}

/// A head with no room in front of it, as an envelope that arrived holds.
impl From<Vec<u8>> for Head {
    fn from(head: Vec<u8>) -> Self {
        Self {
            buffer: Buffer::Room(head),
            at: 0,
        }
    }
}

impl std::fmt::Debug for Head {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Head({} bytes)", self.len())
    }
}

impl Serialize for Head {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self)
    }
}

impl<'de> Deserialize<'de> for Head {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        serde_bytes::ByteBuf::deserialize(deserializer).map(|head| Self::from(head.into_vec()))
    }
}

/// What a stage that accepted a forward hand-off answers: for a streamed
/// request, where to send its items. The stage that reads the items mints
/// their anchor, because a velo anchor is read only in the process that
/// minted it.
pub(crate) type Accepted = Option<ReplyTo>;

/// A request to prepare one backward run of a hop.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Open {
    pub(crate) version: u8,
    pub(crate) identity: Identity,
    pub(crate) hop: u16,
    /// Position on the hop's backward path of the run's first stage.
    pub(crate) pos: u16,
    pub(crate) cx: WireCx,
    /// Where the run delivers its output.
    pub(crate) reply: ReplyTo,
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    rmp_serde::to_vec(value).map_err(codec)
}

/// Append `value` as [`encode`] encodes it.
fn write<T: Serialize>(out: &mut impl std::io::Write, value: &T) -> Result<()> {
    rmp_serde::encode::write(out, value).map_err(codec)
}

fn codec(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Codec, error.to_string())
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    rmp_serde::from_slice(bytes).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dist::counting::{counting, take_sizes};
    use crate::wire::{Codec, RequestCodec};
    use gdl_model::Encoding;

    #[test]
    fn envelopes_round_trip_with_full_width_ids() {
        let cx = Cx::new().with_timeout(std::time::Duration::from_secs(30));
        let envelope = Forward {
            version: VERSION,
            identity: Identity {
                contract: u64::MAX,
                placement: 7,
            },
            hop: 1,
            pos: 2,
            cx: WireCx::of(&cx),
            reply: ReplyTo::test_value(u128::MAX - 5),
            payload: vec![1, 2, 3].into(),
        };
        let back: Forward = decode(&encode(&envelope).expect("encode")).expect("decode");
        assert_eq!(back.identity, envelope.identity);
        assert_eq!(back.cx.root(), cx.id().as_u128());
        assert_eq!(back.reply.handle_u128(), u128::MAX - 5);
        assert_eq!(back.payload[..], [1, 2, 3]);
        let arrived = back
            .cx
            .arrive(
                std::time::Duration::from_millis(50),
                gdl::__runtime::wall_now_nanos,
            )
            .expect("live");
        assert_eq!(arrived.id(), cx.id());
        assert!(arrived.deadline().is_some());
    }

    /// A forward hand-off of `head`, encoded as a request crosses, with
    /// `meta` in its context.
    fn hand_off<T>(codec: &Codec<T>, head: T, meta: &[(&str, &str)]) -> Forward
    where
        T: Send + 'static,
    {
        let mut cx = Cx::new().with_timeout(std::time::Duration::from_secs(30));
        for (key, value) in meta {
            cx.insert_meta(*key, *value);
        }
        Forward {
            version: VERSION,
            identity: Identity {
                contract: u64::MAX,
                placement: 7,
            },
            hop: 1,
            pos: 2,
            cx: WireCx::of(&cx),
            reply: ReplyTo::test_value(u128::MAX - 5),
            payload: codec.split(head).expect("split").head,
        }
    }

    /// An image request's head: base64 text, then a field after it, as
    /// a chat request has its property and its limits after its messages.
    #[derive(Serialize, Deserialize)]
    struct Image {
        url: String,
        detail: String,
    }

    /// The envelope an offer sends holds the request's head where the
    /// head's encoder wrote it: nothing copied the head in between.
    #[test]
    fn a_head_crosses_where_its_encoder_wrote_it() {
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        let image = Image {
            url: "a".repeat(64 * 1024),
            detail: "high".into(),
        };
        let mut envelope = hand_off(&codec, image, &[]);
        let written = envelope.payload.as_ptr() as usize;
        let head = envelope.payload.len();
        let bytes = envelope.wire().expect("wire");
        let sent = &bytes[bytes.len() - head..];
        assert_eq!(sent.as_ptr() as usize, written, "the head was copied");
    }

    /// A 4 MiB head and its envelope take one buffer, sized before the
    /// head is written: no buffer larger than the envelope and the room
    /// for its header, and no more bytes allocated in all.
    #[test]
    fn a_4_mib_head_takes_one_buffer_the_size_of_its_envelope() {
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        let image = Image {
            url: "a".repeat(4 * 1024 * 1024),
            detail: "high".into(),
        };
        take_sizes();
        counting(true);
        let mut envelope = hand_off(&codec, image, &[]);
        let bytes = envelope.wire().expect("wire");
        counting(false);
        let sizes = take_sizes();
        let bound = bytes.len() + HEADER_ROOM;
        assert!(
            sizes.largest <= bound,
            "a buffer of {} B for an envelope of {} B",
            sizes.largest,
            bytes.len()
        );
        // The context, its meta map, and the shared count of the bytes.
        assert!(
            sizes.bytes <= bound + 4096,
            "{} B allocated for an envelope of {} B",
            sizes.bytes,
            bytes.len()
        );
    }

    /// The bytes an offer sends are the envelope's own encoding, for heads
    /// on each side of msgpack's length bounds and of the first buffer,
    /// with and without meta beyond the header's room, and again on a
    /// second offer, which a failover makes.
    #[test]
    fn an_offer_sends_the_envelope_as_it_encodes() {
        let codec = Codec::<serde_bytes::ByteBuf>::new(Encoding::Msgpack);
        let large = "m".repeat(2 * HEADER_ROOM);
        for meta in [
            &[][..],
            &[("model", "x")][..],
            &[("large", large.as_str())][..],
        ] {
            let first = FIRST_BUFFER - HEADER_ROOM;
            for len in [
                0,
                1,
                250,
                256,
                first - 4,
                first - 3,
                first - 2,
                65_530,
                65_536,
            ] {
                let head = serde_bytes::ByteBuf::from(vec![9; len]);
                let mut envelope = hand_off(&codec, head, meta);
                for offer in 0..2 {
                    let bytes = envelope.wire().expect("wire");
                    let expected = encode(&envelope).expect("encode");
                    assert!(
                        bytes[..] == expected[..],
                        "a head of {len} B, meta {meta:?}, offer {offer}"
                    );
                    let back: Forward = decode(&bytes).expect("decode");
                    assert_eq!(back.payload[..], envelope.payload[..]);
                }
            }
        }
    }

    #[test]
    fn an_acknowledgement_carries_its_input_reply() {
        let accepted: Result<Accepted> = Ok(Some(ReplyTo::test_value(u128::MAX - 9)));
        let back: Result<Accepted> = decode(&encode(&accepted).expect("encode")).expect("decode");
        let reply = back.expect("accepted").expect("an input reply");
        assert_eq!(reply.handle_u128(), u128::MAX - 9);
        let plain: Result<Accepted> = Ok(None);
        let back: Result<Accepted> = decode(&encode(&plain).expect("encode")).expect("decode");
        assert!(back.expect("accepted").is_none());
    }

    #[test]
    fn an_envelope_of_version_3_is_refused() {
        let envelope = Open {
            version: 3,
            identity: Identity {
                contract: 1,
                placement: 2,
            },
            hop: 0,
            pos: 1,
            cx: WireCx::default(),
            reply: ReplyTo::test_value(7),
        };
        let back: Open = decode(&encode(&envelope).expect("encode")).expect("decode");
        let refused = check_version(back.version).expect_err("version 3 was accepted");
        assert_eq!(refused.kind(), ErrorKind::Placement);
        assert!(refused.to_string().contains("version 3"), "{refused}");
        assert!(check_version(VERSION).is_ok());
    }
}

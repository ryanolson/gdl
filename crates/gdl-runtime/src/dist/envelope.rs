//! What crosses a segment boundary besides stream items: the request
//! envelopes and their replies. Every message carries the deployment
//! identity, so an instance built from another contract or placement refuses
//! it instead of misreading it.

use std::collections::BTreeMap;

use bytes::Bytes;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::fabric::ReplyTo;
use gdl::{Cx, Deadline, OfferView, RequestId};
use gdl::{Error, ErrorKind, Result};

/// The envelope format. A receiver rejects any other version. Version 4
/// carries each stream item as velo's own byte item, which version 3
/// segments cannot read, so the two refuse each other at the envelope that
/// starts the stream.
pub(crate) const VERSION: u8 = 4;

/// The room in front of a request's head for the rest of its envelope: an
/// envelope with no meta in its context needs about 100 bytes.
pub(crate) const HEADER_ROOM: usize = 256;

/// The buffer a request's head is first written into, with its room in
/// front and [`TAIL_ROOM`] after it: a head of up to 744 bytes is
/// serialized once. A larger one is serialized to count it, then into a
/// buffer of exactly its size and its room.
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
    /// The request's head, and after it the view of the pool the chooser
    /// decided on, when its pool policy asked to put it on the offer
    /// ([`Head::set_view`]).
    pub(crate) payload: Head,
}

/// `rmp_serde`'s encoding of a [`Forward`] starts with this: an array of
/// its seven fields.
const FIELDS: u8 = 0x97;

/// The room an encoder leaves after a request's head, for the view an offer
/// writes after it: `[epoch, sequence]` takes at most 19 bytes.
pub(crate) const TAIL_ROOM: usize = gdl::__runtime::TAIL_ROOM;

/// A [`Forward`] as it arrives, its head borrowed from the bytes, so that
/// where the head ends, and the view after it begins, is known.
#[derive(Deserialize)]
struct Arrived<'a> {
    version: u8,
    identity: Identity,
    hop: u16,
    pos: u16,
    cx: WireCx,
    reply: ReplyTo,
    #[serde(borrow)]
    payload: &'a [u8],
}

impl Forward {
    /// The envelope's bytes, as an offer sends them, which [`encode`]
    /// gives too, then the head's view, if it has one. The first offer
    /// writes the envelope's other fields into the room in front of the
    /// head, so the bytes are the head's own buffer, the view after it
    /// included. A later one, as a failover makes, or one whose fields do
    /// not fit the room, copies the head and its view after them.
    pub(crate) fn wire(&mut self) -> Result<Bytes> {
        let mut room = [0u8; HEADER_ROOM];
        let mut free = &mut room[..];
        // A header past the room on the stack, as large meta makes, is
        // written to the heap: a failover's copy of the head can still have
        // room in front for it (`Forward::set_view`).
        let spilled;
        let header = if self.write_header(&mut free).is_ok() {
            let len = HEADER_ROOM - free.len();
            &room[..len]
        } else {
            let mut written = Vec::new();
            self.write_header(&mut written)?;
            spilled = written;
            &spilled[..]
        };
        let len = header.len();
        let at = self.payload.at();
        if let (true, Buffer::Room(buffer)) = (len <= at, &mut self.payload.buffer) {
            let start = at - len;
            buffer[start..at].copy_from_slice(header);
            let bytes = Bytes::from(std::mem::take(buffer));
            self.payload.buffer = Buffer::Sent(bytes.clone());
            return Ok(bytes.slice(start..));
        }
        let carried = self.payload.carried();
        let mut bytes = Vec::with_capacity(len + carried.len());
        bytes.extend_from_slice(header);
        bytes.extend_from_slice(carried);
        Ok(Bytes::from(bytes))
    }

    /// Carry `view` after the head, in place of any view before
    /// ([`Head::set_view`]). When that takes a copy of a head an offer
    /// already sent, the copy has room in front for the envelope's header
    /// as it is now, so the next offer copies nothing more.
    #[cfg_attr(not(feature = "hub"), allow(dead_code))]
    pub(crate) fn set_view(&mut self, view: Option<OfferView>) {
        if self.payload.view() == view {
            return;
        }
        let room = match self.payload.buffer {
            Buffer::Room(_) => HEADER_ROOM,
            Buffer::Sent(_) => {
                let mut header = Vec::new();
                match self.write_header(&mut header) {
                    Ok(()) => header.len().max(HEADER_ROOM),
                    Err(_) => HEADER_ROOM,
                }
            }
        };
        self.payload.set_view_with_room(view, room);
    }

    /// The envelope an offer sent, `bytes`, with the view after it, if one
    /// is there ([`Head::view`]). The head is borrowed to find where it
    /// ends, then copied once, as [`decode`] copies it, with room for the
    /// view after it. Bytes after the envelope that are not a view are
    /// neither kept nor copied.
    pub(crate) fn read(bytes: &[u8]) -> Result<Self> {
        let arrived: Arrived<'_> = rmp_serde::from_slice(bytes).map_err(codec)?;
        // The head is a slice of `bytes`, so what follows it is the rest.
        let end =
            arrived.payload.as_ptr() as usize - bytes.as_ptr() as usize + arrived.payload.len();
        let mut payload = Vec::with_capacity(arrived.payload.len() + TAIL_ROOM);
        payload.extend_from_slice(arrived.payload);
        let mut payload = Head::from(payload);
        payload.set_view(read_view(&bytes[end..]));
        Ok(Self {
            version: arrived.version,
            identity: arrived.identity,
            hop: arrived.hop,
            pos: arrived.pos,
            cx: arrived.cx,
            reply: arrived.reply,
            payload,
        })
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
    /// Where the head starts in `buffer`, and how long it is, in one word,
    /// so that an envelope, which every hand-off's future holds, is no
    /// larger for them: a head is at most msgpack's 4 GiB. What follows the
    /// head in `buffer` is its view ([`Head::view`]).
    at: u32,
    len: u32,
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
        let len = u32::try_from(buffer.len() - HEADER_ROOM).map_err(|_| too_large(buffer.len()))?;
        Ok(Self {
            buffer: Buffer::Room(buffer),
            at: HEADER_ROOM as u32,
            len,
        })
    }

    fn at(&self) -> usize {
        self.at as usize
    }

    /// The head and what follows it in its buffer.
    fn carried(&self) -> &[u8] {
        match &self.buffer {
            Buffer::Room(buffer) => &buffer[self.at()..],
            Buffer::Sent(bytes) => &bytes[self.at()..],
        }
    }

    /// The view of the pool a chooser decided on, which an offer carries
    /// after the head as `[epoch, sequence]`: an entry of an earlier
    /// build, which decodes the envelope's seven fields and stops, ignores
    /// it. None when no view follows the head, or what follows is not one.
    pub(crate) fn view(&self) -> Option<OfferView> {
        read_view(&self.carried()[self.len as usize..])
    }

    /// Carry `view` after the head, in place of any view before. Before the
    /// first offer it is written into the room the head's encoder left
    /// after it; after, the buffer is shared with that offer's send, so a
    /// different view takes a copy of the head, as the next offer's copy
    /// would.
    pub(crate) fn set_view(&mut self, view: Option<OfferView>) {
        self.set_view_with_room(view, HEADER_ROOM);
    }

    /// [`Head::set_view`], with `room` in front of a copy it takes.
    fn set_view_with_room(&mut self, view: Option<OfferView>, room: usize) {
        if self.view() == view {
            return;
        }
        let end = self.at() + self.len as usize;
        let mut buffer = match std::mem::replace(&mut self.buffer, Buffer::Room(Vec::new())) {
            Buffer::Room(mut buffer) => {
                buffer.truncate(end);
                buffer
            }
            // A copy with room in front, so that the next offer writes the
            // envelope into it rather than copying the head again.
            Buffer::Sent(bytes) => {
                let mut buffer = Vec::with_capacity(room + self.len as usize + TAIL_ROOM);
                buffer.resize(room, 0);
                buffer.extend_from_slice(&bytes[self.at()..end]);
                // A header is far under 4 GiB.
                self.at = u32::try_from(room).unwrap_or(u32::MAX);
                buffer
            }
        };
        if let Some(view) = view {
            // A `Vec`'s writer does not fail.
            let _ = write(&mut buffer, &(view.epoch, view.sequence));
        }
        self.buffer = Buffer::Room(buffer);
    }
}

/// The view in `after`, the bytes after a head, if they begin with one.
fn read_view(after: &[u8]) -> Option<OfferView> {
    if after.is_empty() {
        return None;
    }
    let (epoch, sequence) = rmp_serde::from_slice::<(u64, u64)>(after).ok()?;
    Some(OfferView { epoch, sequence })
}

fn too_large(len: usize) -> Error {
    Error::new(
        ErrorKind::Codec,
        format!("a request head of {len} bytes is over msgpack's 4 GiB"),
    )
}

impl std::ops::Deref for Head {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.carried()[..self.len as usize]
    }
}

/// A head with no room in front of it, as an envelope that arrived holds.
impl From<Vec<u8>> for Head {
    fn from(head: Vec<u8>) -> Self {
        Self {
            // A head that arrived is at most msgpack's 4 GiB.
            len: u32::try_from(head.len()).unwrap_or(u32::MAX),
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

    /// The bytes an offer with a view sends: the envelope's own encoding,
    /// then the view as `[epoch, sequence]`.
    fn with_trailer(envelope: &Forward, view: OfferView) -> Vec<u8> {
        let mut expected = encode(envelope).expect("encode");
        expected.extend(encode(&(view.epoch, view.sequence)).expect("encode"));
        expected
    }

    const VIEW: OfferView = OfferView {
        epoch: 1_793_000_000_123,
        sequence: 7,
    };

    /// An offer carries its view after the envelope, for heads on each side
    /// of msgpack's length bounds and of the first buffer, and in JSON. A
    /// second offer, as a failover makes, carries its own view, or none, and
    /// never the first one's inside its head.
    #[test]
    fn an_offer_carries_its_view_after_the_envelope() {
        let first = FIRST_BUFFER - HEADER_ROOM;
        let other = OfferView {
            epoch: VIEW.epoch + 1,
            sequence: 1,
        };
        for encoding in [Encoding::Msgpack, Encoding::Json] {
            let codec = Codec::<serde_bytes::ByteBuf>::new(encoding);
            for len in [0, 1, 250, first - 30, first - 4, first - 3, first, 65_536] {
                let head = serde_bytes::ByteBuf::from(vec![9; len]);
                let mut envelope = hand_off(&codec, head, &[]);
                let payload = envelope.payload.to_vec();
                for (offer, view) in [(0, Some(VIEW)), (1, Some(other)), (2, None)] {
                    envelope.payload.set_view(view);
                    let bytes = envelope.wire().expect("wire");
                    let expected = match view {
                        Some(view) => with_trailer(&envelope, view),
                        None => encode(&envelope).expect("encode"),
                    };
                    assert!(
                        bytes[..] == expected[..],
                        "{encoding:?}, a head of {len} B, offer {offer}"
                    );
                    let back = Forward::read(&bytes).expect("read");
                    assert_eq!(
                        back.payload.view(),
                        view,
                        "{encoding:?}, {len} B, offer {offer}"
                    );
                    assert_eq!(
                        back.payload[..],
                        payload[..],
                        "{encoding:?}, {len} B, offer {offer}"
                    );
                    let head: serde_bytes::ByteBuf =
                        codec.decode(&back.payload).expect("the head decodes alone");
                    assert_eq!(head.len(), len);
                }
            }
        }
    }

    /// An entry of the build before views decodes an envelope with a view
    /// as the envelope it was, with the decoder it calls, and leaves the
    /// view: builds roll one at a time.
    #[test]
    fn an_earlier_entry_reads_an_offer_with_a_view() {
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        let image = Image {
            url: "a".repeat(100),
            detail: "high".into(),
        };
        let mut envelope = hand_off(&codec, image, &[("model", "x")]);
        envelope.payload.set_view(Some(VIEW));
        let bytes = envelope.wire().expect("wire");
        let earlier: Forward = decode(&bytes).expect("an earlier entry reads it");
        assert_eq!(earlier.payload[..], envelope.payload[..]);
        assert_eq!(earlier.payload.view(), None);
        assert_eq!(earlier.cx.root(), envelope.cx.root());
    }

    /// An offer without a view reads as having none, and bytes after the
    /// envelope that are not a view are ignored, as an earlier entry
    /// ignores them.
    #[test]
    fn an_offer_reads_a_view_only_where_one_is() {
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        let image = Image {
            url: "a".into(),
            detail: "low".into(),
        };
        let mut envelope = hand_off(&codec, image, &[]);
        let bytes = envelope.wire().expect("wire");
        assert_eq!(Forward::read(&bytes).expect("read").payload.view(), None);
        for after in [&[0xc0][..], &[0x92, 0x01][..], &[0x91, 0x01][..], b"junk"] {
            let mut odd = bytes.to_vec();
            odd.extend_from_slice(after);
            let back = Forward::read(&odd).expect("read");
            assert_eq!(back.payload.view(), None, "{after:?}");
            assert_eq!(back.payload[..], envelope.payload[..]);
        }
    }

    /// A head with a view crosses where its encoder wrote it, small or
    /// large: the encoder leaves room after the head for the view.
    #[test]
    fn a_head_with_a_view_crosses_where_its_encoder_wrote_it() {
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        for len in [16, 700, 64 * 1024] {
            let image = Image {
                url: "a".repeat(len),
                detail: "high".into(),
            };
            let mut envelope = hand_off(&codec, image, &[]);
            envelope.payload.set_view(Some(VIEW));
            let written = envelope.payload.as_ptr() as usize;
            let head = envelope.payload.len();
            let bytes = envelope.wire().expect("wire");
            let trailer = encode(&(VIEW.epoch, VIEW.sequence)).expect("encode").len();
            let sent = &bytes[bytes.len() - trailer - head..];
            assert_eq!(
                sent.as_ptr() as usize,
                written,
                "a head of {len} B was copied"
            );
        }
    }

    /// A 4 MiB head with a view still takes one buffer, sized before the
    /// head is written.
    #[test]
    fn a_4_mib_head_with_a_view_takes_one_buffer() {
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        let image = Image {
            url: "a".repeat(4 * 1024 * 1024),
            detail: "high".into(),
        };
        take_sizes();
        counting(true);
        let mut envelope = hand_off(&codec, image, &[]);
        envelope.payload.set_view(Some(VIEW));
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
        assert!(
            sizes.bytes <= bound + 4096,
            "{} B allocated for an envelope of {} B",
            sizes.bytes,
            bytes.len()
        );
    }

    /// Reading an offer at the entry allocates what decoding it did before
    /// views, with a view or without: the head is copied once, out of the
    /// bytes that arrived, into a buffer with room for the view after it.
    #[test]
    fn reading_an_offer_allocates_what_decoding_it_did() {
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        for len in [100, 64 * 1024] {
            for view in [None, Some(VIEW)] {
                let image = Image {
                    url: "a".repeat(len),
                    detail: "high".into(),
                };
                let mut envelope = hand_off(&codec, image, &[("model", "x")]);
                envelope.payload.set_view(view);
                let bytes = envelope.wire().expect("wire");
                take_sizes();
                counting(true);
                let decoded: Forward = decode(&bytes).expect("decode");
                counting(false);
                let before = take_sizes();
                counting(true);
                let read = Forward::read(&bytes).expect("read");
                counting(false);
                let after = take_sizes();
                assert_eq!(read.payload.view(), view);
                // The head's copy holds room for the view, in the same
                // allocation.
                assert_eq!(
                    after.bytes,
                    before.bytes + TAIL_ROOM,
                    "a head of {len} B, view {view:?}"
                );
                assert!(
                    after.largest <= before.largest + TAIL_ROOM,
                    "a head of {len} B, view {view:?}"
                );
                drop((decoded, read));
            }
        }
    }

    /// The entry puts an offer's view in the request's context once it
    /// admits the request: two allocations, the context's map of
    /// extensions and the value. An offer without a view adds none.
    #[test]
    fn a_view_in_the_arrived_context_takes_two_allocations() {
        let mut cx = Cx::new();
        let before = crate::dist::counting::counted();
        counting(true);
        cx.insert_ext(VIEW);
        counting(false);
        assert_eq!(crate::dist::counting::counted() - before, 2);
        assert_eq!(cx.ext::<OfferView>(), Some(&VIEW));
    }

    /// A failover to another pool whose view differs, or that has none,
    /// copies a large head once, as a failover with the same view does,
    /// also when its selector's meta grew the envelope past the room an
    /// encoder leaves: the new view is written into a copy that keeps room
    /// in front for the envelope as it is, so sending it copies nothing
    /// more.
    #[test]
    fn a_failover_with_another_view_copies_the_head_once() {
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        let other = OfferView {
            epoch: VIEW.epoch,
            sequence: VIEW.sequence + 1,
        };
        let large = "m".repeat(2 * HEADER_ROOM);
        for meta in [&[][..], &[("large", large.as_str())][..]] {
            for next in [Some(other), None, Some(VIEW)] {
                let image = Image {
                    url: "a".repeat(4 * 1024 * 1024),
                    detail: "high".into(),
                };
                let mut envelope = hand_off(&codec, image, &[]);
                envelope.set_view(Some(VIEW));
                envelope.wire().expect("the first offer");
                take_sizes();
                counting(true);
                // As a failover's selector adds its meta, then the view.
                envelope.cx.add_meta(
                    meta.iter()
                        .map(|(key, value)| (key.to_string(), value.to_string()))
                        .collect(),
                );
                envelope.set_view(next);
                let bytes = envelope.wire().expect("the failover's offer");
                counting(false);
                let sizes = take_sizes();
                let case = format!("next view {next:?}, meta of {} B", meta.len());
                // One copy of the head, and the meta's own few allocations:
                // far under a second copy.
                assert!(
                    sizes.bytes <= bytes.len() + 64 * 1024,
                    "{} B allocated for an offer of {} B, {case}",
                    sizes.bytes,
                    bytes.len()
                );
                let back = Forward::read(&bytes).expect("read");
                assert_eq!(back.payload.view(), next, "{case}");
                assert_eq!(back.payload[..], envelope.payload[..], "{case}");
            }
        }
    }

    /// Bytes after the envelope that are not a view are not kept, however
    /// many: the entry keeps the head, and a view only when one is there.
    #[test]
    fn an_offer_keeps_only_its_head_and_view() {
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        let image = Image {
            url: "a".into(),
            detail: "low".into(),
        };
        let junk = vec![0xc1; 1024 * 1024];
        for view in [None, Some(VIEW)] {
            let mut envelope = hand_off(&codec, image_like(&image), &[]);
            envelope.payload.set_view(view);
            let mut bytes = envelope.wire().expect("wire").to_vec();
            bytes.extend_from_slice(&junk);
            let back = Forward::read(&bytes).expect("read");
            assert_eq!(back.payload.view(), view);
            let kept = back.payload.carried().len() - back.payload.len();
            assert!(
                kept <= TAIL_ROOM,
                "{kept} B kept after the head, view {view:?}"
            );
        }
    }

    fn image_like(image: &Image) -> Image {
        Image {
            url: image.url.clone(),
            detail: image.detail.clone(),
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

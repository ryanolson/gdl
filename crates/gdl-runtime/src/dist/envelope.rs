//! What crosses a segment boundary besides stream items: the request
//! envelopes and their replies. Every message carries the deployment
//! identity, so an instance built from another contract or placement refuses
//! it instead of misreading it.

use std::collections::BTreeMap;

use bytes::Bytes;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::fabric::ReplyTo;
use crate::zstd::{self, Compression, HeadCompression};
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

    /// The context `cx` crosses with, its deadline `bound`: a call to a
    /// unary service in another segment always carries one.
    pub(crate) fn bounded(cx: &Cx, bound: Deadline) -> Self {
        Self {
            id: cx.id().as_u128(),
            meta: cx.meta().clone(),
            deadline: Some(bound.unix_nanos()),
        }
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
    ///
    /// On an edge that compresses, `compression` says how, and the head
    /// takes its transition for a remote offer first ([`Head::offered_remote`]).
    pub(crate) fn wire(&mut self, compression: Option<HeadCompression>) -> Result<Bytes> {
        if let Some(compression) = compression {
            self.payload.offered_remote(compression)?;
        }
        if matches!(self.payload.held(), Held::Both | Held::Frame) {
            return self.wire_squeezed();
        }
        let mut room = [0u8; HEADER_ROOM];
        let mut free = &mut room[..];
        let on_stack = self.write_header(&mut free).is_ok();
        // A header past the room on the stack, as large meta makes, is
        // counted, then written where it is sent: into the room in front of
        // the head when a failover's copy left enough (`Forward::set_view`),
        // or at the start of the copy.
        let len = if on_stack {
            HEADER_ROOM - free.len()
        } else {
            self.header_len()?
        };
        let at = self.payload.at();
        let head = self.payload.len();
        let Self {
            version,
            identity,
            hop,
            pos,
            cx,
            reply,
            payload,
        } = self;
        if let (true, Buffer::Room(buffer)) = (len <= at, &mut payload.buffer) {
            let start = at - len;
            if on_stack {
                buffer[start..at].copy_from_slice(&room[..len]);
            } else {
                let header = Header {
                    version: *version,
                    identity,
                    hop: *hop,
                    pos: *pos,
                    cx,
                    reply,
                    len: head,
                };
                let mut front = &mut buffer[start..at];
                header.write(&mut front)?;
            }
            let bytes = Bytes::from(std::mem::take(buffer));
            payload.buffer = Buffer::Sent(bytes.clone());
            return Ok(bytes.slice(start..));
        }
        let carried = self.payload.carried();
        let mut bytes = Vec::with_capacity(len + carried.len());
        if on_stack {
            bytes.extend_from_slice(&room[..len]);
        } else {
            self.write_header(&mut bytes)?;
        }
        bytes.extend_from_slice(carried);
        Ok(Bytes::from(bytes))
    }

    /// [`Forward::wire`] for a compressed head: the frame and, after it, the
    /// plain head's view. The first offer writes the header into the room
    /// in front of the frame, and later ones copy the frame.
    fn wire_squeezed(&mut self) -> Result<Bytes> {
        let Self {
            version,
            identity,
            hop,
            pos,
            cx,
            reply,
            payload,
        } = self;
        let Head {
            buffer,
            at,
            len,
            squeezed,
        } = payload;
        let Some(Squeezed::Frame {
            buffer: squeezed,
            len: frame,
            ..
        }) = squeezed.as_deref_mut()
        else {
            return Err(Error::new(ErrorKind::Codec, "internal: no compressed head"));
        };
        let view = &carried(buffer, *at)[*len as usize..];
        let frame = *frame as usize;
        let header = Header {
            version: *version,
            identity,
            hop: *hop,
            pos: *pos,
            cx,
            reply,
            len: frame,
        };
        let mut counter = Counter(0);
        header.write(&mut counter)?;
        let need = counter.0;
        if let (true, Buffer::Room(room)) = (need <= HEADER_ROOM, &mut *squeezed) {
            let start = HEADER_ROOM - need;
            let mut front = &mut room[start..HEADER_ROOM];
            header.write(&mut front)?;
            room.extend_from_slice(view);
            let bytes = Bytes::from(std::mem::take(room));
            *squeezed = Buffer::Sent(bytes.clone());
            return Ok(bytes.slice(start..));
        }
        let compressed = &carried(squeezed, HEADER_ROOM as u32)[..frame];
        let mut bytes = Vec::with_capacity(need + frame + view.len());
        header.write(&mut bytes)?;
        bytes.extend_from_slice(compressed);
        bytes.extend_from_slice(view);
        Ok(Bytes::from(bytes))
    }

    /// How many bytes [`Forward::write_header`] writes, counted without
    /// writing them anywhere.
    fn header_len(&self) -> Result<usize> {
        let mut counter = Counter(0);
        self.write_header(&mut counter)?;
        Ok(counter.0)
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
            Buffer::Sent(_) => self.header_len().unwrap_or(0).max(HEADER_ROOM),
        };
        self.payload.set_view_with_room(view, room);
    }

    /// The envelope an offer sent, `bytes`, with the view after it, if one
    /// is there ([`Head::view`]). The head is borrowed to find where it
    /// ends, then copied once, as [`decode`] copies it, with room for the
    /// view after it. Bytes after the envelope that are not a view are
    /// neither kept nor copied.
    ///
    /// On an edge that `compression` (by hop and position) says compresses,
    /// a head that is a frame is decompressed in place of that copy, under
    /// the receiver's rules ([`zstd::content_size`]).
    pub(crate) fn read(
        bytes: &[u8],
        compression: impl FnOnce(u16, u16) -> Option<Compression>,
    ) -> Result<Self> {
        let arrived: Arrived<'_> = rmp_serde::from_slice(bytes).map_err(codec)?;
        // The head is a slice of `bytes`, so what follows it is the rest.
        let end =
            arrived.payload.as_ptr() as usize - bytes.as_ptr() as usize + arrived.payload.len();
        let payload = match compression(arrived.hop, arrived.pos) {
            Some(compression) if zstd::is_frame(arrived.payload) => {
                decoded(arrived.payload, compression.limit(), &[])?
            }
            _ => {
                let mut payload = Vec::with_capacity(arrived.payload.len() + TAIL_ROOM);
                payload.extend_from_slice(arrived.payload);
                payload
            }
        };
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
        Header {
            version: self.version,
            identity: &self.identity,
            hop: self.hop,
            pos: self.pos,
            cx: &self.cx,
            reply: &self.reply,
            len: self.payload.len(),
        }
        .write(out)
    }
}

/// Everything [`encode`] writes of a [`Forward`] before its head's bytes,
/// borrowed apart from the head's buffer, so that it can be written into
/// that buffer.
struct Header<'a> {
    version: u8,
    identity: &'a Identity,
    hop: u16,
    pos: u16,
    cx: &'a WireCx,
    reply: &'a ReplyTo,
    /// The head's length.
    len: usize,
}

impl Header<'_> {
    fn write(&self, out: &mut impl std::io::Write) -> Result<()> {
        out.write_all(&[FIELDS]).map_err(codec)?;
        write(out, &self.version)?;
        write(out, self.identity)?;
        write(out, &self.hop)?;
        write(out, &self.pos)?;
        write(out, self.cx)?;
        write(out, self.reply)?;
        write_bin_len(out, self.len)
    }
}

/// Write msgpack's marker of a binary of `len` bytes: what comes before the
/// bytes of a head that an envelope carries.
fn write_bin_len(out: &mut impl std::io::Write, len: usize) -> Result<()> {
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
    /// The frame, or the decision to send the head plain, that its first
    /// remote offer made when its edge compresses ([`Held`]). Boxed, so a
    /// head that is not compressed is one pointer larger.
    squeezed: Option<Box<Squeezed>>,
}

/// What a sender holds of a request's head, on an edge that compresses
/// its head. `buffer` holds the plain head, or only its view where the
/// plain head was released, and `squeezed` the frame or the decision. A
/// head changes state only in [`Head::offered_remote`] and [`Head::offered_local`]:
///
/// | From | Remote offer, routing reads the head | Remote offer, it does not | Local offer |
/// |---|---|---|---|
/// | `Plain` | `Both`, or `Declined` | `Frame`, or `Declined` | `Plain` |
/// | `Declined` | `Declined` | `Declined` | `Declined` |
/// | `Both` | `Both` | `Frame` | `Both` |
/// | `Frame` | `Frame` | `Frame` | `Both`, decoded as a remote head is |
///
/// A remote offer of a `Plain` head below its edge's floor leaves it
/// `Plain`. See docs/src/concepts/crossings.md#edge-codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Held {
    /// The plain head alone, not compressed yet.
    Plain,
    /// The plain head alone, for good: its frame was not shorter.
    Declined,
    /// The plain head and its frame: routing reads the plain head on a
    /// later choice, and a local offer reads it.
    Both,
    /// The frame alone, and the view: nothing on this crossing reads the
    /// plain head.
    Frame,
}

/// A head's compression, decided at its first remote offer above its floor.
enum Squeezed {
    /// The frame behind [`HEADER_ROOM`] of room, until an offer sends it,
    /// its length, and whether the plain head was released ([`Held::Frame`]).
    Frame {
        buffer: Buffer,
        len: u32,
        released: bool,
    },
    /// The frame was not shorter: the head crosses plain.
    Declined,
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
            squeezed: None,
        })
    }

    /// What the sender holds of the head ([`Held`]).
    pub(crate) fn held(&self) -> Held {
        match self.squeezed.as_deref() {
            None => Held::Plain,
            Some(Squeezed::Declined) => Held::Declined,
            Some(Squeezed::Frame { released: true, .. }) => Held::Frame,
            Some(Squeezed::Frame {
                released: false, ..
            }) => Held::Both,
        }
    }

    /// The transition of a remote offer on an edge that compresses as
    /// `head` says. A `Plain` head at its floor is compressed, once, into
    /// a frame behind its own room, with room for the view after it, so
    /// that the offer writes the envelope in front and copies nothing; a
    /// frame that is not shorter makes it `Declined`. Then, whether the
    /// frame is new or the one an earlier offer made, the plain head is
    /// released unless routing reads it again.
    pub(crate) fn offered_remote(&mut self, head: HeadCompression) -> Result<()> {
        if self.held() == Held::Plain && head.compression.applies(self.len as usize) {
            self.compress()?;
        }
        if self.held() == Held::Both && !head.keeps_plain {
            self.release();
        }
        Ok(())
    }

    /// The transition of an offer to this instance, a direct call, which
    /// reads the plain head: a `Frame` head takes its plain head back from
    /// its frame through the decode a remote head takes ([`decoded`]),
    /// under the receiver's `limit`, and becomes `Both`.
    pub(crate) fn offered_local(&mut self, limit: usize) -> Result<()> {
        if self.held() != Held::Frame {
            return Ok(());
        }
        let Some(Squeezed::Frame {
            buffer,
            len,
            released,
        }) = self.squeezed.as_deref_mut()
        else {
            return Ok(());
        };
        let frame = &carried(buffer, HEADER_ROOM as u32)[..*len as usize];
        let view = carried(&self.buffer, self.at);
        let plain = decoded(frame, limit, view)?;
        let size = plain.len() - view.len();
        self.len = u32::try_from(size).map_err(|_| too_large(size))?;
        self.at = 0;
        self.buffer = Buffer::Room(plain);
        *released = false;
        Ok(())
    }

    /// `Plain` to `Both`, or to `Declined` when the frame is not shorter.
    fn compress(&mut self) -> Result<()> {
        let plain: &[u8] = self;
        let bound = HEADER_ROOM + zstd::bound(plain.len()) + TAIL_ROOM;
        let mut buffer = Vec::with_capacity(bound);
        buffer.resize(HEADER_ROOM, 0);
        zstd::compress_into(plain, &mut buffer)?;
        let len = buffer.len() - HEADER_ROOM;
        self.squeezed = Some(Box::new(if len < plain.len() {
            Squeezed::Frame {
                buffer: Buffer::Room(buffer),
                // Shorter than the plain head, which is under 4 GiB.
                len: u32::try_from(len).unwrap_or(u32::MAX),
                released: false,
            }
        } else {
            Squeezed::Declined
        }));
        Ok(())
    }

    /// `Both` to `Frame`: the plain head goes, and its view stays, for the
    /// frame's offers to send after it.
    fn release(&mut self) {
        let view = self.carried()[self.len as usize..].to_vec();
        self.buffer = Buffer::Room(view);
        self.at = 0;
        self.len = 0;
        if let Some(Squeezed::Frame { released, .. }) = self.squeezed.as_deref_mut() {
            *released = true;
        }
    }

    /// Whether the head crosses as a frame.
    #[cfg(test)]
    pub(crate) fn is_squeezed(&self) -> bool {
        matches!(self.held(), Held::Both | Held::Frame)
    }

    fn at(&self) -> usize {
        self.at as usize
    }

    /// The head and what follows it in its buffer.
    fn carried(&self) -> &[u8] {
        carried(&self.buffer, self.at)
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
            let _ = write_view(&mut buffer, view);
        }
        self.buffer = Buffer::Room(buffer);
    }
}

/// A head that arrived as `frame`, decoded under the receiver's rules
/// ([`zstd::content_size`]) with at most `limit` bytes, then `view` after
/// it, with room for a view: what a remote head and a local offer's
/// restored head both take.
fn decoded(frame: &[u8], limit: usize, view: &[u8]) -> Result<Vec<u8>> {
    let size = zstd::content_size(frame, limit)?;
    let mut plain = Vec::with_capacity(size + TAIL_ROOM.max(view.len()));
    zstd::decompress_into(frame, limit, &mut plain)?;
    plain.extend_from_slice(view);
    Ok(plain)
}

/// What `buffer` holds from `at` on.
fn carried(buffer: &Buffer, at: u32) -> &[u8] {
    match buffer {
        Buffer::Room(buffer) => &buffer[at as usize..],
        Buffer::Sent(bytes) => &bytes[at as usize..],
    }
}

/// A writer that only counts the bytes written to it.
struct Counter(usize);

impl std::io::Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The view in `after`, the bytes after a head, if they begin with one.
fn read_view(after: &[u8]) -> Option<OfferView> {
    if after.is_empty() {
        return None;
    }
    rmp_serde::from_slice::<WireView>(after)
        .ok()
        .map(|view| view.0)
}

/// A view as it is read: `[epoch, sequence]`, as a chooser of the build
/// before the fleet's fullest fill sends it, or `[epoch, sequence,
/// fill_max_ppm]`, in one pass.
struct WireView(OfferView);

impl<'de> Deserialize<'de> for WireView {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Elements;
        impl<'de> serde::de::Visitor<'de> for Elements {
            type Value = WireView;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("[epoch, sequence] or [epoch, sequence, fill_max_ppm]")
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<WireView, A::Error> {
                let missing = || serde::de::Error::invalid_length(0, &self);
                let epoch = seq.next_element()?.ok_or_else(missing)?;
                let sequence = seq.next_element()?.ok_or_else(missing)?;
                let fill_max_ppm = seq.next_element()?;
                Ok(WireView(OfferView {
                    epoch,
                    sequence,
                    fill_max_ppm,
                }))
            }
        }
        deserializer.deserialize_seq(Elements)
    }
}

/// Write `view` as an offer carries it after the head:
/// `[epoch, sequence, fill_max_ppm]`, or `[epoch, sequence]` without the
/// fleet's fullest fill, which an entry of the build before it reads too.
/// At most 24 bytes ([`TAIL_ROOM`]).
fn write_view(out: &mut Vec<u8>, view: OfferView) -> Result<()> {
    match view.fill_max_ppm {
        Some(ppm) => write(out, &(view.epoch, view.sequence, ppm)),
        None => write(out, &(view.epoch, view.sequence)),
    }
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
            squeezed: None,
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

/// A call to a unary service in another segment, as the `gdl.call`
/// handler receives it: the deployment's identity, the service's index
/// among the unary nodes in name order, the context with the call's bound as its
/// deadline, and the request, encoded behind the room the envelope's other
/// fields are written into (docs/src/concepts/crossings.md).
pub(crate) struct Call {
    pub(crate) identity: Identity,
    pub(crate) service: u16,
    pub(crate) cx: WireCx,
    pub(crate) payload: CallHead,
}

/// A call's request, encoded plain behind room for the rest of its
/// envelope. Not a [`Head`]: a head may be compressed, or shared with a
/// send, and a call's request is neither, so [`Call::wire`] has no such
/// case to meet. A `[codec]` entry for a unary node is read, but not
/// applied to its calls yet.
pub(crate) struct CallHead(Vec<u8>);

impl CallHead {
    /// Encode `value` behind [`HEADER_ROOM`] with `encode`, as
    /// [`Head::encode`] does.
    pub(crate) fn encode<T: ?Sized>(
        value: &T,
        encode: fn(&T, &mut Vec<u8>) -> Result<()>,
    ) -> Result<Self> {
        let mut buffer = Vec::with_capacity(FIRST_BUFFER);
        buffer.resize(HEADER_ROOM, 0);
        encode(value, &mut buffer)?;
        Ok(Self(buffer))
    }
}

/// `rmp_serde`'s encoding of a [`Call`] starts with this: an array of its
/// five fields, the version first.
const CALL_FIELDS: u8 = 0x95;

/// A [`Call`] as it arrives, its request borrowed from the bytes.
#[derive(Deserialize)]
pub(crate) struct CallArrived<'a> {
    pub(crate) version: u8,
    pub(crate) identity: Identity,
    pub(crate) service: u16,
    pub(crate) cx: WireCx,
    #[serde(borrow)]
    pub(crate) payload: &'a [u8],
}

impl Call {
    /// The envelope's bytes. Its other fields are written into the room in
    /// front of the request, so the bytes are the request's own buffer. A
    /// header past the room, as large meta makes, is written with a copy of
    /// the request after it.
    pub(crate) fn wire(self) -> Result<Bytes> {
        let Self {
            identity,
            service,
            cx,
            payload,
        } = self;
        let CallHead(mut buffer) = payload;
        let (at, len) = (HEADER_ROOM, buffer.len() - HEADER_ROOM);
        let mut room = [0u8; HEADER_ROOM];
        let mut free = &mut room[..];
        let fits = write_call_header(&mut free, &identity, service, &cx, len).is_ok();
        let header = HEADER_ROOM - free.len();
        if fits {
            let start = at - header;
            buffer[start..at].copy_from_slice(&room[..header]);
            return Ok(Bytes::from(buffer).slice(start..));
        }
        let mut bytes = Vec::with_capacity(header + len);
        write_call_header(&mut bytes, &identity, service, &cx, len)?;
        bytes.extend_from_slice(&buffer[at..]);
        Ok(Bytes::from(bytes))
    }
}

#[cfg(test)]
mod calls;

impl<'a> CallArrived<'a> {
    /// Read a [`Call`] from `bytes`, its request left where it arrived.
    pub(crate) fn read(bytes: &'a [u8]) -> Result<Self> {
        rmp_serde::from_slice(bytes).map_err(codec)
    }
}

/// Everything a [`Call`]'s bytes hold before its request's.
fn write_call_header(
    out: &mut impl std::io::Write,
    identity: &Identity,
    service: u16,
    cx: &WireCx,
    len: usize,
) -> Result<()> {
    out.write_all(&[CALL_FIELDS]).map_err(codec)?;
    write(out, &VERSION)?;
    write(out, identity)?;
    write(out, &service)?;
    write(out, cx)?;
    write_bin_len(out, len)
}

/// Append `value` as MessagePack, as [`encode`] encodes it.
pub(crate) fn write_msgpack<T: Serialize>(out: &mut Vec<u8>, value: &T) -> Result<()> {
    write(out, value)
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

    /// A head's compression for a crossing whose routing reads the head
    /// again, so the sender keeps the plain head.
    #[cfg(feature = "zstd")]
    fn kept(compression: Compression) -> HeadCompression {
        HeadCompression {
            compression,
            keeps_plain: true,
        }
    }

    /// A head whose crossing reads it again keeps its plain bytes, and one
    /// whose crossing does not releases them, keeping its view. An offer
    /// to this instance gets them back from the frame, with the view.
    #[cfg(feature = "zstd")]
    #[test]
    fn a_head_keeps_its_plain_bytes_only_for_routing_that_reads_them() {
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        let image = || Image {
            url: crate::zstd::words(64 * 1024, 17),
            detail: "high".into(),
        };
        let compression = Compression::new(4096, 1 << 30);
        let plain = hand_off(&codec, image(), &[]).payload.to_vec();
        let view = OfferView {
            epoch: 5,
            sequence: 8,
            fill_max_ppm: None,
        };
        // A selector reads the head on a later choice: the plain head stays.
        let mut read = hand_off(&codec, image(), &[]);
        read.wire(Some(kept(compression))).expect("wire");
        assert!(read.payload.is_squeezed());
        assert_eq!(read.payload[..], plain[..], "routing reads the plain head");
        // Round robin reads nothing: the plain head goes, its view stays.
        let mut released = hand_off(&codec, image(), &[]);
        released.payload.set_view(Some(view));
        let head = HeadCompression {
            compression,
            keeps_plain: false,
        };
        let first = released.wire(Some(head)).expect("wire");
        assert!(released.payload.is_squeezed());
        assert!(released.payload.is_empty(), "the plain head is released");
        let again = released.wire(Some(head)).expect("wire");
        for bytes in [&first, &again] {
            let arrived = Forward::read(bytes, |_, _| Some(compression)).expect("read");
            assert_eq!(arrived.payload[..], plain[..]);
            assert_eq!(arrived.payload.view(), Some(view));
        }
        // An offer to this instance takes the plain head back.
        released
            .payload
            .offered_local(usize::MAX)
            .expect("restored");
        assert_eq!(released.payload[..], plain[..]);
        assert_eq!(released.payload.view(), Some(view));
        let after = released.wire(Some(head)).expect("wire");
        let arrived = Forward::read(&after, |_, _| Some(compression)).expect("read");
        assert_eq!(arrived.payload[..], plain[..]);
    }

    /// A remote re-offer releases plain bytes restored for a local offer.
    #[cfg(feature = "zstd")]
    #[test]
    fn a_remote_reoffer_releases_restored_plain_bytes() {
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        let mut envelope = hand_off(
            &codec,
            Image {
                url: "a".repeat(4 * 1024 * 1024),
                detail: "high".into(),
            },
            &[],
        );
        envelope.set_view(Some(VIEW));
        let plain = envelope.payload.to_vec();
        let head = HeadCompression {
            compression: Compression::new(4096, 8 * 1024 * 1024),
            keeps_plain: false,
        };
        envelope.wire(Some(head)).expect("first remote offer");
        assert!(envelope.payload.is_empty(), "the first offer releases it");
        envelope
            .payload
            .offered_local(8 * 1024 * 1024)
            .expect("local offer");
        assert_eq!(envelope.payload[..], plain[..]);
        assert_eq!(envelope.payload.view(), Some(VIEW));
        let again = envelope.wire(Some(head)).expect("remote re-offer");
        let arrived = Forward::read(&again, |_, _| Some(head.compression)).expect("read");
        assert_eq!(arrived.payload[..], plain[..]);
        assert_eq!(arrived.payload.view(), Some(VIEW));
        assert!(
            envelope.payload.is_empty(),
            "the remote re-offer retains {} plain bytes after local restoration",
            envelope.payload.len()
        );
    }

    /// An envelope whose head is `len` bytes of text, or of noise, which
    /// does not compress.
    #[cfg(feature = "zstd")]
    fn text_head(len: usize, noise: bool) -> (Forward, Vec<u8>) {
        let codec = Codec::<serde_bytes::ByteBuf>::new(Encoding::Msgpack);
        let bytes = if noise {
            crate::zstd::noise(len, 21)
        } else {
            crate::zstd::words(len, 21).into_bytes()
        };
        let mut envelope = hand_off(&codec, serde_bytes::ByteBuf::from(bytes), &[]);
        envelope.payload.set_view(Some(VIEW));
        let plain = envelope.payload.to_vec();
        (envelope, plain)
    }

    /// The compression of an edge with a 4 KiB floor, `keeps` its plain
    /// head where routing reads it.
    #[cfg(feature = "zstd")]
    fn floor_4k(keeps: bool) -> HeadCompression {
        HeadCompression {
            compression: Compression::new(4096, 1 << 30),
            keeps_plain: keeps,
        }
    }

    /// `envelope` in state `held`, from a fresh head: `Plain` as it is,
    /// `Declined` by a remote offer of noise, `Both` by one that keeps the
    /// plain head, `Frame` by one that does not.
    #[cfg(feature = "zstd")]
    fn in_state(held: Held) -> (Forward, Vec<u8>) {
        let (mut envelope, plain) = text_head(64 * 1024, held == Held::Declined);
        match held {
            Held::Plain => {}
            Held::Declined | Held::Both => {
                envelope.wire(Some(floor_4k(true))).expect("wire");
            }
            Held::Frame => {
                envelope.wire(Some(floor_4k(false))).expect("wire");
            }
        }
        assert_eq!(envelope.payload.held(), held);
        (envelope, plain)
    }

    /// Every transition in [`Held`]'s table: from each state, a remote
    /// offer whose routing reads the head, one whose routing does not, and
    /// a local offer. A remote offer sends what reads back as the plain
    /// head with its view, a local offer leaves the plain head readable,
    /// and a released head keeps only its view.
    #[cfg(all(feature = "zstd", feature = "fault-injection"))]
    #[test]
    fn every_head_state_takes_the_transitions_of_its_table() {
        use Held::{Both, Declined, Frame, Plain};
        enum Offer {
            Reads,
            Blind,
            Local,
        }
        let table = [
            (Plain, Offer::Reads, Both),
            (Plain, Offer::Blind, Frame),
            (Plain, Offer::Local, Plain),
            (Declined, Offer::Reads, Declined),
            (Declined, Offer::Blind, Declined),
            (Declined, Offer::Local, Declined),
            (Both, Offer::Reads, Both),
            (Both, Offer::Blind, Frame),
            (Both, Offer::Local, Both),
            (Frame, Offer::Reads, Frame),
            (Frame, Offer::Blind, Frame),
            (Frame, Offer::Local, Both),
        ];
        let read = |bytes: &Bytes, plain: &[u8]| {
            let arrived =
                Forward::read(bytes, |_, _| Some(floor_4k(true).compression)).expect("read");
            assert_eq!(arrived.payload[..], plain[..]);
            assert_eq!(arrived.payload.view(), Some(VIEW));
        };
        for (from, offer, to) in table {
            let (mut envelope, plain) = in_state(from);
            let written = crate::zstd::frames().0;
            match offer {
                Offer::Reads => read(&envelope.wire(Some(floor_4k(true))).expect("wire"), &plain),
                Offer::Blind => read(&envelope.wire(Some(floor_4k(false))).expect("wire"), &plain),
                Offer::Local => {
                    envelope.payload.offered_local(1 << 30).expect("local");
                    assert_eq!(
                        envelope.payload[..],
                        plain[..],
                        "{from:?}: the entry's head"
                    );
                    assert_eq!(envelope.payload.view(), Some(VIEW));
                }
            }
            assert_eq!(envelope.payload.held(), to, "{from:?}");
            let compressed = crate::zstd::frames().0 - written;
            assert_eq!(
                compressed,
                u64::from(from == Plain && to != Plain),
                "{from:?} to {to:?}"
            );
            if to == Frame {
                assert!(
                    envelope.payload.is_empty(),
                    "{from:?}: the plain head is released"
                );
                assert!(
                    envelope.payload.carried().len() <= TAIL_ROOM,
                    "only the view stays"
                );
            }
        }
        // Below its floor a `Plain` head stays `Plain` on a remote offer.
        let (mut small, _) = text_head(1024, false);
        small.wire(Some(floor_4k(false))).expect("wire");
        assert_eq!(small.payload.held(), Plain);
    }

    /// A local offer restores under the receiver's limit, as a remote head
    /// arrives: over it, it refuses with `Codec` and the head stays
    /// `Frame`; at it, it restores.
    #[cfg(feature = "zstd")]
    #[test]
    fn a_local_offer_restores_only_within_the_limit() {
        let (mut envelope, plain) = in_state(Held::Frame);
        let refused = envelope
            .payload
            .offered_local(plain.len() - 1)
            .expect_err("over");
        assert_eq!(refused.kind(), ErrorKind::Codec);
        assert_eq!(envelope.payload.held(), Held::Frame);
        envelope
            .payload
            .offered_local(plain.len())
            .expect("at the limit");
        assert_eq!(envelope.payload[..], plain[..]);
    }

    /// The sequence a failover can make: a remote offer that releases the
    /// plain head, a local offer that takes it back, a remote one again,
    /// a local one again. The head is compressed once, every offer reads
    /// back as the plain head with its view, and each remote offer leaves
    /// only the view.
    #[cfg(all(feature = "zstd", feature = "fault-injection"))]
    #[test]
    fn remote_local_remote_local_releases_and_restores_each_time() {
        let (mut envelope, plain) = text_head(4 * 1024 * 1024, false);
        let written = crate::zstd::frames().0;
        for round in 0..2 {
            let bytes = envelope.wire(Some(floor_4k(false))).expect("remote");
            let arrived =
                Forward::read(&bytes, |_, _| Some(floor_4k(false).compression)).expect("read");
            assert_eq!(arrived.payload[..], plain[..], "round {round}");
            assert_eq!(arrived.payload.view(), Some(VIEW));
            assert_eq!(envelope.payload.held(), Held::Frame, "round {round}");
            assert!(envelope.payload.is_empty(), "round {round}: released");
            envelope.payload.offered_local(8 << 20).expect("local");
            assert_eq!(envelope.payload.held(), Held::Both, "round {round}");
            assert_eq!(envelope.payload[..], plain[..], "round {round}");
            assert_eq!(envelope.payload.view(), Some(VIEW));
        }
        assert_eq!(crate::zstd::frames().0 - written, 1, "compressed once");
    }

    /// A head on an edge that compresses: below its floor it crosses as it
    /// would without a codec, at its floor as one frame behind the room,
    /// which reads back to the plain head. Routing still reads the plain
    /// head, and a later offer, after a refusal, sends the frame again with
    /// the view the chooser set.
    #[cfg(feature = "zstd")]
    #[test]
    fn a_head_crosses_compressed_from_its_floor_and_reads_back() {
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        let image = |len| Image {
            url: crate::zstd::words(len, 7),
            detail: "high".into(),
        };
        let plain = hand_off(&codec, image(20_000), &[]).payload.to_vec();
        let floor = u32::try_from(plain.len()).expect("small");
        let squeeze = Compression::new(floor, 1 << 30);
        // One byte under the floor: the bytes are the bytes without a codec.
        let mut under = hand_off(&codec, image(20_000 - 1), &[]);
        let first = under.wire(Some(kept(squeeze))).expect("wire");
        assert!(!under.payload.is_squeezed());
        assert_eq!(first, under.wire(None).expect("wire"));
        // At the floor: one frame.
        let mut envelope = hand_off(&codec, image(20_000), &[]);
        let bytes = envelope.wire(Some(kept(squeeze))).expect("wire");
        assert!(envelope.payload.is_squeezed());
        assert_eq!(
            envelope.payload[..],
            plain[..],
            "routing reads the plain head"
        );
        let arrived = Forward::read(&bytes, |hop, pos| ((hop, pos) == (1, 2)).then_some(squeeze))
            .expect("read");
        assert_eq!(arrived.payload[..], plain[..]);
        assert!(
            bytes.len() < plain.len() / 2,
            "{} of {}",
            bytes.len(),
            plain.len()
        );
        // A later offer with a view: the frame again, the view after it.
        let view = OfferView {
            epoch: 3,
            sequence: 9,
            fill_max_ppm: None,
        };
        envelope.set_view(Some(view));
        let again = envelope.wire(Some(kept(squeeze))).expect("wire");
        let arrived = Forward::read(&again, |_, _| Some(squeeze)).expect("read");
        assert_eq!(arrived.payload[..], plain[..]);
        assert_eq!(arrived.payload.view(), Some(view));
        // A receiver whose edge does not compress takes the frame as the
        // head, and its decode fails: both ends must agree, which the
        // placement hash makes them.
        let unread = Forward::read(&bytes, |_, _| None).expect("read");
        assert!(crate::zstd::is_frame(&unread.payload));
    }

    /// A compressed head costs two allocations more on its first offer
    /// than a plain one, its frame's buffer and the box that holds it, and
    /// none more where it arrives: the frame is decompressed in place of
    /// the copy a plain head takes.
    #[cfg(feature = "zstd")]
    #[test]
    fn a_compressed_head_allocates_twice_more_to_send_and_no_more_to_read() {
        use crate::dist::counting::{counted, counting};
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        let image = || Image {
            url: crate::zstd::words(64 * 1024, 13),
            detail: "high".into(),
        };
        let squeeze = Compression::new(4096, 1 << 30);
        // libzstd makes this thread's contexts at its first frame.
        hand_off(&codec, image(), &[])
            .wire(Some(kept(squeeze)))
            .expect("wire");
        let count = |compression: Option<Compression>| {
            let mut envelope = hand_off(&codec, image(), &[]);
            counting(true);
            let before = counted();
            let bytes = envelope.wire(compression.map(kept)).expect("wire");
            let sent = counted() - before;
            let before = counted();
            let read = Forward::read(&bytes, |_, _| compression).expect("read");
            let arrived = counted() - before;
            counting(false);
            assert_eq!(read.payload[..], envelope.payload[..]);
            (sent, arrived)
        };
        let (plain_sent, plain_read) = count(None);
        let (sent, read) = count(Some(squeeze));
        assert_eq!(sent, plain_sent + 2);
        assert_eq!(read, plain_read);
    }

    /// A head that does not shrink crosses plain.
    #[cfg(feature = "zstd")]
    #[test]
    fn a_head_that_does_not_shrink_crosses_plain() {
        let codec = Codec::<serde_bytes::ByteBuf>::new(Encoding::Msgpack);
        let noise = serde_bytes::ByteBuf::from(crate::zstd::noise(64 * 1024, 3));
        let mut envelope = hand_off(&codec, noise, &[]);
        let bytes = envelope
            .wire(Some(kept(Compression::new(1024, 1 << 30))))
            .expect("wire");
        assert!(!envelope.payload.is_squeezed());
        assert_eq!(bytes, envelope.wire(None).expect("wire"));
    }

    /// A refused offer must not repeat compression that cannot shrink the head.
    #[cfg(all(feature = "zstd", feature = "fault-injection"))]
    #[test]
    fn an_incompressible_head_is_tried_once_across_offers() {
        let codec = Codec::<serde_bytes::ByteBuf>::new(Encoding::Msgpack);
        let noise = serde_bytes::ByteBuf::from(crate::zstd::noise(64 * 1024, 3));
        let mut envelope = hand_off(&codec, noise, &[]);
        let compression = Some(Compression::new(1024, 1 << 30));
        let before = crate::zstd::frames().0;
        let first = envelope.wire(compression.map(kept)).expect("first offer");
        assert!(!envelope.payload.is_squeezed(), "the frame did not shrink");
        assert_eq!(crate::zstd::frames().0 - before, 1);
        let second = envelope.wire(compression.map(kept)).expect("second offer");
        assert_eq!(first, second, "the re-offer carries the same head");
        assert_eq!(
            crate::zstd::frames().0 - before,
            1,
            "compression runs once for the hand-off, including an unhelpful frame"
        );
    }

    /// Round-robin routing releases a plain head after its first remote offer.
    #[cfg(feature = "zstd")]
    #[tokio::test]
    async fn a_round_robin_offer_releases_the_plain_head() {
        use crate::dist::fabric::{Fabric, bind_node};
        use crate::dist::roster::{Policy, Roster};
        use crate::dist::runtime::{Router, Runtime, RuntimeConfig, Within};
        use gdl_model::{
            CodecEntry, EdgeCodec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo,
        };

        let manifest = Manifest {
            name: "retention".into(),
            schema: 1,
            nodes: vec![
                NodeInfo {
                    name: "S".into(),
                    role: Role::Source,
                    binding: None,
                    calls: Vec::new(),
                    unary: None,
                },
                NodeInfo {
                    name: "D".into(),
                    role: Role::Sink,
                    binding: Some("d".into()),
                    calls: Vec::new(),
                    unary: None,
                },
            ],
            hops: vec![HopInfo {
                outer: "S".into(),
                forward: vec![],
                inner: "D".into(),
                backward: vec![],
                forward_types: vec![TypeInfo::new("Image", true, Encoding::Msgpack)],
                backward_types: vec![TypeInfo::new("()", true, Encoding::Msgpack)],
            }],
            properties: vec![],
        };
        let placement = Placement::new()
            .segment("front", ["S"])
            .segment("engine", ["D"])
            .codec("S -> D", CodecEntry::every(EdgeCodec::MsgpackZstd));
        let sender = bind_node(([127, 0, 0, 1], 0).into()).await.expect("sender");
        let receiver = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("receiver");
        let mut roster = Roster::new();
        roster.add("engine", receiver.peer_info());
        let rt = Runtime::new(
            Fabric::new(sender, None, None, std::time::Duration::from_secs(30)),
            gdl_model::layout(&manifest, &placement).expect("layout"),
            "front".into(),
            &roster,
            RuntimeConfig::default(),
            None,
        )
        .expect("runtime");
        let Router::Roster(within) = rt.router(0, 1, Encoding::Msgpack) else {
            panic!("one class routes through its roster");
        };
        assert!(matches!(within, Within::Policy(Policy::RoundRobin)));
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        let mut envelope = hand_off(
            &codec,
            Image {
                url: "a".repeat(4 * 1024 * 1024),
                detail: "high".into(),
            },
            &[],
        );
        envelope.hop = 0;
        envelope.pos = 1;
        envelope.identity = rt.identity;
        let cx = Cx::new();
        let (instance, _) = rt
            .choose("engine", &within, &cx, &mut envelope, &[])
            .await
            .unwrap_or_else(|_| panic!("round-robin choice"));
        assert_eq!(instance, receiver.instance_id());
        let (offering, _) = rt.deliver(instance, &mut envelope, false).expect("offer");
        drop(offering);
        assert!(
            envelope.payload.is_squeezed(),
            "the head crosses compressed"
        );
        let retained = match &envelope.payload.buffer {
            Buffer::Room(buffer) => buffer.capacity(),
            Buffer::Sent(bytes) => bytes.len(),
        };
        assert!(
            retained <= HEADER_ROOM + TAIL_ROOM,
            "round-robin retains {retained} bytes of plain head after compression"
        );
    }

    /// The split-MessagePack regression: two frames that each obey their
    /// own size, and pass a check of the first one's against the limit,
    /// are refused in a head.
    #[cfg(feature = "zstd")]
    #[test]
    fn a_head_of_two_frames_is_refused() {
        let value = rmp_serde::to_vec_named(&"x".repeat(1024)).expect("encoded");
        assert_eq!(value.len(), 1027);
        let frame = |src: &[u8]| {
            let mut out = Vec::new();
            crate::zstd::compress_into(src, &mut out).expect("compressed");
            out
        };
        let mut two = frame(&value[..1024]);
        two.extend_from_slice(&frame(&value[1024..]));
        let codec = Codec::<Vec<u8>>::new(Encoding::Msgpack);
        let mut envelope = hand_off(&codec, Vec::new(), &[]);
        envelope.payload = Head::from(two);
        let bytes = envelope.wire(None).expect("wire");
        let limit = Compression::new(0, 1024);
        let refused = Forward::read(&bytes, |_, _| Some(limit)).expect_err("two frames");
        assert_eq!(refused.kind(), ErrorKind::Codec);
        // The control: the first frame alone, at the limit, reads.
        envelope.payload = Head::from(frame(&value[..1024]));
        let bytes = envelope.wire(None).expect("wire");
        let read = Forward::read(&bytes, |_, _| Some(limit)).expect("one frame");
        assert_eq!(read.payload[..], value[..1024]);
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
        let bytes = envelope.wire(None).expect("wire");
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
        let bytes = envelope.wire(None).expect("wire");
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
                    let bytes = envelope.wire(None).expect("wire");
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
        write_view(&mut expected, view).expect("encode");
        expected
    }

    const VIEW: OfferView = OfferView {
        epoch: 1_793_000_000_123,
        sequence: 7,
        fill_max_ppm: Some(850_000),
    };

    /// A view carries the fleet's fullest fill as a third element; an
    /// entry of the build before it, which reads two, reads a view of
    /// three as none, and has no gate to miss it. A view of two, from a
    /// chooser of that build, reads here with no fullest fill. The widest
    /// view fits the room after the head.
    #[test]
    fn a_view_carries_the_fleets_fullest_fill_both_ways() {
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        let image = Image {
            url: "a".repeat(700),
            detail: "high".into(),
        };
        let widest = OfferView {
            epoch: u64::MAX,
            sequence: u64::MAX,
            fill_max_ppm: Some(u32::MAX),
        };
        let mut envelope = hand_off(&codec, image_like(&image), &[]);
        envelope.set_view(Some(widest));
        let written = envelope.payload.as_ptr() as usize;
        let bytes = envelope.wire(None).expect("wire");
        assert_eq!(bytes[..], with_trailer(&envelope, widest)[..]);
        let head = envelope.payload.len();
        let after = bytes.len() - head - TAIL_ROOM;
        assert_eq!(
            bytes[after..].as_ptr() as usize,
            written,
            "the widest view did not fit after the head"
        );
        assert_eq!(
            Forward::read(&bytes, |_, _| None)
                .expect("read")
                .payload
                .view(),
            Some(widest)
        );
        assert_eq!(
            Forward::read(&bytes, |_, _| None)
                .expect("read")
                .payload
                .view()
                .and_then(|view| view.fill_max()),
            Some(f64::from(u32::MAX) / 1_000_000.0)
        );

        // The entry of the build before: it reads two elements after the
        // head, or no view.
        let earlier = |after: &[u8]| rmp_serde::from_slice::<(u64, u64)>(after).ok();
        let back: Forward = decode(&bytes).expect("an earlier entry reads it");
        let head_end = bytes.len() - (bytes.len() - encode(&back).expect("encode").len());
        assert_eq!(earlier(&bytes[head_end..]), None);

        // A chooser of the build before sends two elements.
        let mut two = encode(&back).expect("encode");
        two.extend(encode(&(VIEW.epoch, VIEW.sequence)).expect("encode"));
        let read = Forward::read(&two, |_, _| None).expect("read");
        assert_eq!(
            read.payload.view(),
            Some(OfferView {
                epoch: VIEW.epoch,
                sequence: VIEW.sequence,
                fill_max_ppm: None,
            })
        );
    }

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
            ..Default::default()
        };
        for encoding in [Encoding::Msgpack, Encoding::Json] {
            let codec = Codec::<serde_bytes::ByteBuf>::new(encoding);
            for len in [0, 1, 250, first - 30, first - 4, first - 3, first, 65_536] {
                let head = serde_bytes::ByteBuf::from(vec![9; len]);
                let mut envelope = hand_off(&codec, head, &[]);
                let payload = envelope.payload.to_vec();
                for (offer, view) in [(0, Some(VIEW)), (1, Some(other)), (2, None)] {
                    envelope.payload.set_view(view);
                    let bytes = envelope.wire(None).expect("wire");
                    let expected = match view {
                        Some(view) => with_trailer(&envelope, view),
                        None => encode(&envelope).expect("encode"),
                    };
                    assert!(
                        bytes[..] == expected[..],
                        "{encoding:?}, a head of {len} B, offer {offer}"
                    );
                    let back = Forward::read(&bytes, |_, _| None).expect("read");
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
        let bytes = envelope.wire(None).expect("wire");
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
        let bytes = envelope.wire(None).expect("wire");
        assert_eq!(
            Forward::read(&bytes, |_, _| None)
                .expect("read")
                .payload
                .view(),
            None
        );
        for after in [&[0xc0][..], &[0x92, 0x01][..], &[0x91, 0x01][..], b"junk"] {
            let mut odd = bytes.to_vec();
            odd.extend_from_slice(after);
            let back = Forward::read(&odd, |_, _| None).expect("read");
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
            let bytes = envelope.wire(None).expect("wire");
            let trailer =
                with_trailer(&envelope, VIEW).len() - encode(&envelope).expect("encode").len();
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
        let bytes = envelope.wire(None).expect("wire");
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
                let bytes = envelope.wire(None).expect("wire");
                take_sizes();
                counting(true);
                let decoded: Forward = decode(&bytes).expect("decode");
                counting(false);
                let before = take_sizes();
                counting(true);
                let read = Forward::read(&bytes, |_, _| None).expect("read");
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

    /// An offer whose header is past the room in front of its head, as
    /// large meta makes, takes one allocation beyond its header's failed
    /// try on the stack: the buffer it sends. A failover's change of view
    /// takes one too, the copy of the head, which the offer then writes its
    /// header into, also when the header grew past the room after the
    /// first offer sent the head's own buffer. Neither writes its header
    /// anywhere else first.
    #[test]
    fn a_long_header_allocates_only_what_is_sent() {
        let codec = Codec::<Image>::new(Encoding::Msgpack);
        let large = "m".repeat(300);
        let image = Image {
            url: "a".repeat(100),
            detail: "high".into(),
        };
        let mut envelope = hand_off(&codec, image, &[("large", large.as_str())]);
        // What the header's first try on the stack costs as it fails, as
        // it did before views: its error's message.
        let before = crate::dist::counting::counted();
        counting(true);
        let mut room = [0u8; HEADER_ROOM];
        assert!(envelope.write_header(&mut &mut room[..]).is_err());
        counting(false);
        let tried = crate::dist::counting::counted() - before;
        let before = crate::dist::counting::counted();
        counting(true);
        let first = envelope.wire(None).expect("the first offer");
        counting(false);
        let made = crate::dist::counting::counted() - before;
        assert_eq!(made, tried + 1, "the first offer");
        assert_eq!(first[..], encode(&envelope).expect("encode")[..]);
        envelope.wire(None).expect("sent once");
        let before = crate::dist::counting::counted();
        counting(true);
        envelope.set_view(Some(VIEW));
        let again = envelope.wire(None).expect("the failover's offer");
        counting(false);
        let made = crate::dist::counting::counted() - before;
        assert_eq!(
            made,
            tried + 1,
            "the failover: the head's copy, and no more"
        );
        assert_eq!(
            Forward::read(&again, |_, _| None)
                .expect("read")
                .payload
                .view(),
            Some(VIEW)
        );

        // A first offer with a short header sends the head's own buffer;
        // the failover's selector then grows the header, and the view
        // changes: the copy keeps room for the longer header, which the
        // offer writes into it.
        let image = Image {
            url: "a".repeat(100),
            detail: "high".into(),
        };
        let mut envelope = hand_off(&codec, image, &[]);
        envelope.set_view(Some(VIEW));
        let first = envelope.wire(None).expect("the first offer");
        assert!(matches!(envelope.payload.buffer, Buffer::Sent(_)));
        assert_eq!(first[..], with_trailer(&envelope, VIEW)[..]);
        envelope
            .cx
            .add_meta(vec![("large".to_string(), large.clone())]);
        let other = OfferView {
            epoch: VIEW.epoch,
            sequence: VIEW.sequence + 1,
            ..Default::default()
        };
        let before = crate::dist::counting::counted();
        counting(true);
        envelope.set_view(Some(other));
        counting(false);
        assert_eq!(
            crate::dist::counting::counted() - before,
            1,
            "the view's copy of the head"
        );
        let written = envelope.payload.as_ptr() as usize;
        let again = envelope.wire(None).expect("the failover's offer");
        let trailer =
            with_trailer(&envelope, other).len() - encode(&envelope).expect("encode").len();
        let head = envelope.payload.len();
        let sent = &again[again.len() - trailer - head..];
        assert_eq!(
            sent.as_ptr() as usize,
            written,
            "the offer copied the head again, past the view's copy"
        );
        assert_eq!(again[..], with_trailer(&envelope, other)[..]);
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
            ..Default::default()
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
                envelope.wire(None).expect("the first offer");
                take_sizes();
                counting(true);
                // As a failover's selector adds its meta, then the view.
                envelope.cx.add_meta(
                    meta.iter()
                        .map(|(key, value)| (key.to_string(), value.to_string()))
                        .collect(),
                );
                envelope.set_view(next);
                let bytes = envelope.wire(None).expect("the failover's offer");
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
                let back = Forward::read(&bytes, |_, _| None).expect("read");
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
            let mut bytes = envelope.wire(None).expect("wire").to_vec();
            bytes.extend_from_slice(&junk);
            let back = Forward::read(&bytes, |_, _| None).expect("read");
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

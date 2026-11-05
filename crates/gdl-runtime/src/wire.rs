use std::fmt;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use futures::StreamExt;

use gdl::Streamed;
use gdl::{Error, ErrorKind, Result};
use gdl::{Items, items};
use gdl_model::Encoding;

use crate::dist::{CallHead, EncodedItem, Head};
use crate::zstd::{self, Compression};

/// How one edge's message type crosses a segment boundary.
///
/// A codec is a pair of function pointers monomorphized for `T` where `T` is
/// concrete. Runtime code that is generic over `T` cannot call a serde API on
/// `T` without a `Serialize` bound, and a runtime check cannot supply that
/// bound; carrying the already-monomorphized functions can. An edge whose
/// type is not serializable has no codec, and a placement that would cross
/// it is rejected before anything runs.
///
/// The message's `#[gdl(codec = "...")]` chooses named MessagePack
/// (`msgpack`, the default), positional MessagePack (`msgpack_array`), or
/// JSON (`json`). Arrays must not conditionally omit fields.
pub struct Codec<T> {
    encode: fn(&T, &mut Vec<u8>) -> Result<()>,
    /// `encode`, into a buffer it sizes once: for a request's head.
    encode_sized: fn(&T, &mut Vec<u8>) -> Result<()>,
    decode: fn(&[u8]) -> Result<T>,
    encoding: Encoding,
    /// How a stream item on the edge is compressed, when a `[codec]` entry
    /// names the edge. A request's head is compressed by its offer, after
    /// routing has read it (`Forward::wire`).
    compression: Option<Compression>,
}

impl<T> Codec<T> {
    /// Encode one value.
    pub fn encode(&self, value: &T) -> Result<Vec<u8>> {
        // Reserved as serde_json does: token-sized values need no growth.
        let mut bytes = Vec::with_capacity(128);
        (self.encode)(value, &mut bytes)?;
        Ok(bytes)
    }

    /// Encode one value of a stream into the frame that carries it, sized
    /// by the stream's `hint`, and compressed through the stream's
    /// `scratch` when the edge compresses and the value is long enough.
    pub(crate) fn encode_item(
        &self,
        value: &T,
        hint: &mut SizeHint,
        scratch: &mut Vec<u8>,
    ) -> Result<EncodedItem> {
        let (item, plain) = EncodedItem::encode_compressed(
            value,
            self.encode,
            hint.next(),
            self.compression,
            scratch,
        )?;
        // The plain size: a compressed one would reserve too little for the
        // next item, whose frame would then grow.
        hint.record(plain);
        Ok(item)
    }

    /// Decode one item of a stream. A compressed one is decompressed into
    /// the stream's `scratch` first: the decoder needs the plain bytes, so
    /// it cannot read velo's buffer in place.
    pub(crate) fn decode_item(&self, bytes: &[u8], scratch: &mut Vec<u8>) -> Result<T> {
        match self.compression {
            Some(compression) if zstd::is_frame(bytes) => {
                zstd::decompress_into(bytes, compression.limit(), scratch)?;
                (self.decode)(scratch)
            }
            _ => (self.decode)(bytes),
        }
    }

    /// Compress the edge's stream items as `compression` says.
    pub(crate) fn compress(&mut self, compression: Option<Compression>) {
        self.compression = compression;
    }

    /// Encode a request's head into the buffer its envelope is sent from.
    pub(crate) fn encode_head(&self, value: &T) -> Result<Head> {
        Head::encode(value, self.encode_sized)
    }

    /// Encode a call's request into the buffer its envelope is sent from:
    /// plain, as a call's request always is.
    pub(crate) fn encode_call(&self, value: &T) -> Result<CallHead> {
        CallHead::encode(value, self.encode_sized)
    }

    /// Append the encoding of `value` to `out`.
    pub(crate) fn encode_into(&self, value: &T, out: &mut Vec<u8>) -> Result<()> {
        (self.encode)(value, out)
    }

    /// Decode one value.
    pub fn decode(&self, bytes: &[u8]) -> Result<T> {
        (self.decode)(bytes)
    }
}

impl<T> Codec<T> {
    /// A codec from its functions, so that a test can see what the
    /// runtime hands them.
    #[cfg(test)]
    pub(crate) fn from_fns(
        encode: fn(&T, &mut Vec<u8>) -> Result<()>,
        decode: fn(&[u8]) -> Result<T>,
    ) -> Self {
        Self {
            encode,
            encode_sized: encode,
            decode,
            encoding: Encoding::default(),
            compression: None,
        }
    }
}

impl<T: Serialize + DeserializeOwned> Codec<T> {
    /// A serde codec for `T` using the selected encoding.
    pub fn new(encoding: Encoding) -> Self {
        Self {
            encode: gdl::__runtime::encoder_into::<T>(encoding),
            encode_sized: gdl::__runtime::encoder_sized::<T>(encoding),
            decode: gdl::__runtime::decoder::<T>(encoding),
            encoding,
            compression: None,
        }
    }
}

impl<T> Clone for Codec<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Codec<T> {}

impl<T> fmt::Debug for Codec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Codec")
            .field("type", &std::any::type_name::<T>())
            .field("encoding", &self.encoding)
            .field("compression", &self.compression)
            .finish()
    }
}

/// How a forward edge's request crosses a segment boundary: as a value, or
/// as a head value and a stream of items. Implemented by [`Codec`] and
/// [`StreamCodec`]; the graph macro picks one from the edge's syntax.
#[doc(hidden)]
pub trait RequestCodec: sealed::Sealed + Copy + Send + Sync + 'static {
    /// The request type on the edge.
    type Value: Send + 'static;
    /// Whether the edge carries a stream.
    const STREAMED: bool;

    /// The type of what crosses as the head, and its name: the value
    /// itself, or a streamed request's head. A routing selector that reads
    /// the head must read it as this type.
    fn head() -> (std::any::TypeId, &'static str);
    /// The encoding used for the request's head.
    fn head_encoding(&self) -> Encoding;

    /// The request as it crosses.
    fn split(&self, value: Self::Value) -> Result<Split>;

    /// Rebuild a request from its head's bytes and, for a streamed request,
    /// its encoded items as they arrived.
    fn join(&self, head: &[u8], input: Option<Items<Bytes>>) -> Result<Self::Value>;

    /// Compress a streamed request's items as `items` says. A request's
    /// head is compressed by its offer, so a value's codec ignores it.
    fn compress_items(&mut self, items: Option<Compression>);
}

/// A request as it crosses: its head's bytes, and for a streamed request its
/// encoded items.
#[doc(hidden)]
pub struct Split {
    pub head: Head,
    pub input: Option<Items<EncodedItem>>,
}

impl<T: Send + 'static> RequestCodec for Codec<T> {
    type Value = T;
    const STREAMED: bool = false;

    fn head() -> (std::any::TypeId, &'static str) {
        (std::any::TypeId::of::<T>(), std::any::type_name::<T>())
    }

    fn head_encoding(&self) -> Encoding {
        self.encoding
    }

    fn split(&self, value: T) -> Result<Split> {
        Ok(Split {
            head: self.encode_head(&value)?,
            input: None,
        })
    }

    fn join(&self, head: &[u8], input: Option<Items<Bytes>>) -> Result<T> {
        match input {
            None => self.decode(head),
            Some(_) => Err(mismatch()),
        }
    }

    fn compress_items(&mut self, _: Option<Compression>) {}
}

/// The codec of a streamed request: one for its head and one for its items.
pub struct StreamCodec<H, T> {
    head: Codec<H>,
    item: Codec<T>,
}

impl<H, T> StreamCodec<H, T> {
    /// A codec when both parts have one: a request crosses whole or not at
    /// all.
    pub fn pair(head: Option<Codec<H>>, item: Option<Codec<T>>) -> Option<Self> {
        Some(Self {
            head: head?,
            item: item?,
        })
    }
}

impl<H, T> Clone for StreamCodec<H, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<H, T> Copy for StreamCodec<H, T> {}

impl<H, T> fmt::Debug for StreamCodec<H, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamCodec")
            .field("head", &std::any::type_name::<H>())
            .field("item", &std::any::type_name::<T>())
            .finish()
    }
}

impl<H: Send + 'static, T: Send + 'static> RequestCodec for StreamCodec<H, T> {
    type Value = Streamed<H, T>;
    const STREAMED: bool = true;

    fn head() -> (std::any::TypeId, &'static str) {
        (std::any::TypeId::of::<H>(), std::any::type_name::<H>())
    }

    fn head_encoding(&self) -> Encoding {
        self.head.encoding
    }

    fn split(&self, value: Streamed<H, T>) -> Result<Split> {
        let (head, input) = value.into_parts();
        let item = self.item;
        let mut hint = SizeHint::default();
        let mut scratch = Vec::new();
        let encoded =
            items(input.map(move |value| item.encode_item(&value?, &mut hint, &mut scratch)));
        Ok(Split {
            head: self.head.encode_head(&head)?,
            input: Some(encoded),
        })
    }

    fn join(&self, head: &[u8], input: Option<Items<Bytes>>) -> Result<Streamed<H, T>> {
        let input = input.ok_or_else(mismatch)?;
        let item = self.item;
        let mut scratch = Vec::new();
        // `Streamed::new` fuses the decoded items: nothing follows a bad one.
        let decoded = items(input.map(move |bytes| item.decode_item(&bytes?, &mut scratch)));
        Ok(Streamed::new(self.head.decode(head)?, decoded))
    }

    fn compress_items(&mut self, items: Option<Compression>) {
        self.item.compress(items);
    }
}

/// The payload room to reserve for a stream's next item: the smaller of its
/// last two items' sizes. A stream of like items reserves each one's size
/// exactly, so its frame needs no growth and has no spare room. A large item
/// raises the hint for no more than one item after it, so a small item that
/// follows reserves a small frame.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SizeHint {
    last: usize,
    before: usize,
}

impl SizeHint {
    /// The room a stream's first item reserves: a token-sized value fits.
    const FIRST: usize = 128;

    fn next(&self) -> usize {
        self.last.min(self.before)
    }

    fn record(&mut self, len: usize) {
        self.before = self.last;
        self.last = len;
    }
}

impl Default for SizeHint {
    fn default() -> Self {
        Self {
            last: Self::FIRST,
            before: Self::FIRST,
        }
    }
}

fn mismatch() -> Error {
    // The layout and the bindings agree on which edges are streamed
    // (checked when a hop is built), so reaching this is a runtime defect.
    Error::new(
        ErrorKind::Placement,
        "internal: a request and its edge disagree on whether it is streamed",
    )
}

/// Only this module's codecs cross a forward edge.
mod sealed {
    pub trait Sealed {}

    impl<T> Sealed for super::Codec<T> {}
    impl<H, T> Sealed for super::StreamCodec<H, T> {}
}

/// The codec for a concrete type, or `None` if it is not wire-capable.
///
/// This must be expanded where the type is concrete. The selection is
/// autoref specialization: the `Serialize + DeserializeOwned` impl is on
/// `Probe<T>` and the fallback on `&Probe<T>`, so method lookup on a
/// `&Probe<T>` tries the bounded impl first. Inside a generic function the
/// bound is not provable for an arbitrary `T`, so the fallback would always
/// win; that is why the graph macro expands this per edge.
/// A message's annotation chooses the encoding; an unmarked serde type
/// uses named-field MessagePack.
#[macro_export]
macro_rules! wire_codec {
    ($ty:ty) => {{
        #[allow(unused_imports)]
        use $crate::__private::{ViaLocal as _, ViaWire as _};
        (&$crate::__private::Probe::<$ty>::new()).__gdl_codec($crate::message_encoding!($ty))
    }};
}

/// The codec of a streamed request with head `H` and items `T`, or `None`
/// unless both are wire-capable. Like [`wire_codec!`], it must be expanded
/// where both types are concrete.
#[macro_export]
macro_rules! stream_codec {
    ($head:ty, $item:ty) => {
        $crate::StreamCodec::pair($crate::wire_codec!($head), $crate::wire_codec!($item))
    };
}

/// Support for macro expansions. Not a stable interface.
#[doc(hidden)]
pub mod probe {
    use std::marker::PhantomData;

    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use super::Codec;
    use gdl_model::Encoding;

    pub struct Probe<T>(PhantomData<fn() -> T>);

    impl<T> Probe<T> {
        pub const fn new() -> Self {
            Self(PhantomData)
        }
    }

    impl<T> Default for Probe<T> {
        fn default() -> Self {
            Self::new()
        }
    }

    // The method name is in the reserved `__gdl` namespace: the call sits in
    // the caller's scope, where a trait of theirs with a method of the same
    // name would make it ambiguous.
    pub trait ViaWire<T> {
        fn __gdl_codec(&self, encoding: Encoding) -> Option<Codec<T>>;
    }

    impl<T: Serialize + DeserializeOwned> ViaWire<T> for Probe<T> {
        fn __gdl_codec(&self, encoding: Encoding) -> Option<Codec<T>> {
            Some(Codec::new(encoding))
        }
    }

    pub trait ViaLocal<T> {
        fn __gdl_codec(&self, encoding: Encoding) -> Option<Codec<T>>;
    }

    impl<T> ViaLocal<T> for &Probe<T> {
        fn __gdl_codec(&self, _: Encoding) -> Option<Codec<T>> {
            None
        }
    }
}

#[cfg(all(test, feature = "zstd"))]
mod tests {
    use super::*;
    use gdl::ErrorKind;

    fn compressing(floor: u32, limit: usize) -> Codec<String> {
        let mut codec = Codec::<String>::new(Encoding::Msgpack);
        codec.compress(Some(Compression::new(floor, limit)));
        codec
    }

    /// One item under the floor is the plain encoding, byte for byte; one
    /// at the floor is a frame that decodes to the same value. The size
    /// hint keeps the plain size, so the next frame needs no growth.
    #[test]
    fn an_item_crosses_compressed_from_its_floor() {
        let text = crate::zstd::words(20_000, 5);
        let plain = Codec::<String>::new(Encoding::Msgpack)
            .encode(&text)
            .expect("plain");
        let floor = u32::try_from(plain.len()).expect("small");
        let codec = compressing(floor, 1 << 30);
        let (mut hint, mut scratch) = (SizeHint::default(), Vec::new());
        let under = text[..text.len() - 1].to_string();
        let item = codec
            .encode_item(&under, &mut hint, &mut scratch)
            .expect("encoded");
        let plain_under = Codec::<String>::new(Encoding::Msgpack)
            .encode(&under)
            .expect("plain");
        assert_eq!(item.payload(), &plain_under[..]);
        let item = codec
            .encode_item(&text, &mut hint, &mut scratch)
            .expect("encoded");
        assert!(zstd::is_frame(item.payload()));
        assert!(item.payload().len() < plain.len() / 2);
        assert_eq!(hint.next(), plain_under.len().min(plain.len()));
        let mut read = Vec::new();
        assert_eq!(
            codec
                .decode_item(item.payload(), &mut read)
                .expect("decoded"),
            text
        );
        // A plain item on the edge still decodes in place.
        assert_eq!(
            codec.decode_item(&plain_under, &mut read).expect("decoded"),
            under
        );
    }

    /// After a stream's first compressed item, which grows its two scratch
    /// buffers, an item takes the allocations a plain one does: its frame
    /// to send, and its decoded value. libzstd keeps one context per
    /// thread, made at first use.
    #[test]
    fn a_compressed_item_allocates_as_a_plain_one_after_the_first() {
        use crate::dist::counting::{counted, counting};
        let text = crate::zstd::words(32 * 1024, 11);
        let plain = Codec::<String>::new(Encoding::Msgpack);
        let codec = compressing(4096, 1 << 30);
        let round =
            |codec: &Codec<String>, hint: &mut SizeHint, sent: &mut Vec<u8>, read: &mut Vec<u8>| {
                counting(true);
                let before = counted();
                let item = codec.encode_item(&text, hint, sent).expect("encoded");
                let value = codec.decode_item(item.payload(), read).expect("decoded");
                let made = counted() - before;
                counting(false);
                assert_eq!(value, text);
                made
            };
        let (mut hint, mut sent, mut read) = (SizeHint::default(), Vec::new(), Vec::new());
        round(&codec, &mut hint, &mut sent, &mut read);
        let compressed = round(&codec, &mut hint, &mut sent, &mut read);
        let (mut hint, mut sent, mut read) = (SizeHint::default(), Vec::new(), Vec::new());
        round(&plain, &mut hint, &mut sent, &mut read);
        let uncompressed = round(&plain, &mut hint, &mut sent, &mut read);
        assert_eq!(compressed, uncompressed);
    }

    /// An item that does not shrink crosses plain.
    #[test]
    fn an_item_that_does_not_shrink_crosses_plain() {
        let mut codec = Codec::<serde_bytes::ByteBuf>::new(Encoding::Msgpack);
        codec.compress(Some(Compression::new(1024, 1 << 30)));
        let noise = serde_bytes::ByteBuf::from(zstd::noise(64 * 1024, 9));
        let item = codec
            .encode_item(&noise, &mut SizeHint::default(), &mut Vec::new())
            .expect("encoded");
        assert!(!zstd::is_frame(item.payload()));
        assert_eq!(item.payload(), &codec.encode(&noise).expect("plain")[..]);
    }

    /// The split-MessagePack regression for a stream item, read into a
    /// buffer the stream reuses, whose capacity is over the limit: two
    /// frames are refused, and the frame at the limit reads.
    #[test]
    fn an_item_of_two_frames_is_refused_in_a_reused_buffer() {
        let codec = compressing(0, 1024);
        let value = rmp_serde::to_vec_named(&"x".repeat(1024)).expect("encoded");
        let frame = |src: &[u8]| {
            let mut out = Vec::new();
            zstd::compress_into(src, &mut out).expect("compressed");
            out
        };
        let mut two = frame(&value[..1024]);
        two.extend_from_slice(&frame(&value[1024..]));
        let mut scratch = Vec::with_capacity(8192);
        let refused = codec
            .decode_item(&two, &mut scratch)
            .expect_err("two frames");
        assert_eq!(refused.kind(), ErrorKind::Codec);
        let whole = frame(&value);
        assert!(
            codec.decode_item(&whole, &mut scratch).is_err(),
            "over the limit"
        );
        let short = rmp_serde::to_vec_named(&"y".repeat(1000)).expect("encoded");
        assert_eq!(
            codec
                .decode_item(&frame(&short), &mut scratch)
                .expect("read"),
            "y".repeat(1000)
        );
    }
}

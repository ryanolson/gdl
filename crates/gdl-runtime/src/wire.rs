use std::fmt;

use serde::Serialize;
use serde::de::DeserializeOwned;

use futures::StreamExt;

use gdl::Streamed;
use gdl::{Error, ErrorKind, Result};
use gdl::{Items, items};
use gdl_model::Encoding;

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
    encode: fn(&T) -> Result<Vec<u8>>,
    decode: fn(&[u8]) -> Result<T>,
    encoding: Encoding,
}

impl<T> Codec<T> {
    /// Encode one value.
    pub fn encode(&self, value: &T) -> Result<Vec<u8>> {
        (self.encode)(value)
    }

    /// Decode one value.
    pub fn decode(&self, bytes: &[u8]) -> Result<T> {
        (self.decode)(bytes)
    }
}

impl<T: Serialize + DeserializeOwned> Codec<T> {
    /// A serde codec for `T` using the selected encoding.
    pub fn new(encoding: Encoding) -> Self {
        Self {
            encode: gdl::__runtime::encoder::<T>(encoding),
            decode: gdl::__runtime::decoder::<T>(encoding),
            encoding,
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
    /// its encoded items.
    fn join(&self, head: &[u8], input: Option<Items<Vec<u8>>>) -> Result<Self::Value>;
}

/// A request as it crosses: its head's bytes, and for a streamed request its
/// encoded items.
#[doc(hidden)]
pub struct Split {
    pub head: Vec<u8>,
    pub input: Option<Items<Vec<u8>>>,
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
            head: self.encode(&value)?,
            input: None,
        })
    }

    fn join(&self, head: &[u8], input: Option<Items<Vec<u8>>>) -> Result<T> {
        match input {
            None => self.decode(head),
            Some(_) => Err(mismatch()),
        }
    }
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
        let encoded = items(input.map(move |value| item.encode(&value?)));
        Ok(Split {
            head: self.head.encode(&head)?,
            input: Some(encoded),
        })
    }

    fn join(&self, head: &[u8], input: Option<Items<Vec<u8>>>) -> Result<Streamed<H, T>> {
        let input = input.ok_or_else(mismatch)?;
        let item = self.item;
        // `Streamed::new` fuses the decoded items: nothing follows a bad one.
        let decoded = items(input.map(move |bytes| item.decode(&bytes?)));
        Ok(Streamed::new(self.head.decode(head)?, decoded))
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

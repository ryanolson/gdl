use std::fmt;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, ErrorKind, Result};

/// How one edge's message type crosses a segment boundary.
///
/// A codec is a pair of function pointers monomorphized for `T` where `T` is
/// concrete. Runtime code that is generic over `T` cannot call a serde API on
/// `T` without a `Serialize` bound, and a runtime check cannot supply that
/// bound; carrying the already-monomorphized functions can. An edge whose
/// type is not serializable has no codec, and a placement that would cross
/// it is rejected before anything runs.
pub struct Codec<T> {
    encode: fn(&T) -> Result<Vec<u8>>,
    decode: fn(&[u8]) -> Result<T>,
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
    /// The msgpack codec for `T`.
    pub fn msgpack() -> Self {
        Self {
            encode: encode_msgpack::<T>,
            decode: decode_msgpack::<T>,
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
            .finish()
    }
}

// Compact (array-form) msgpack: field names are not repeated per item. A
// change to a message's wire form needs a schema bump anyway, so named
// fields would buy nothing.
fn encode_msgpack<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    rmp_serde::to_vec(value).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}

fn decode_msgpack<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    rmp_serde::from_slice(bytes).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}

/// The codec for a concrete type, or `None` if it is not wire-capable.
///
/// This must be expanded where the type is concrete. The selection is
/// autoref specialization: the `Serialize + DeserializeOwned` impl is on
/// `Probe<T>` and the fallback on `&Probe<T>`, so method lookup on a
/// `&Probe<T>` tries the bounded impl first. Inside a generic function the
/// bound is not provable for an arbitrary `T`, so the fallback would always
/// win; that is why the graph macro expands this per edge.
#[macro_export]
macro_rules! wire_codec {
    ($ty:ty) => {{
        #[allow(unused_imports)]
        use $crate::__private::{ViaLocal as _, ViaWire as _};
        (&$crate::__private::Probe::<$ty>::new()).__gdl_codec()
    }};
}

/// Support for macro expansions. Not a stable interface.
#[doc(hidden)]
pub mod probe {
    use std::marker::PhantomData;

    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use super::Codec;

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
        fn __gdl_codec(&self) -> Option<Codec<T>>;
    }

    impl<T: Serialize + DeserializeOwned> ViaWire<T> for Probe<T> {
        fn __gdl_codec(&self) -> Option<Codec<T>> {
            Some(Codec::msgpack())
        }
    }

    pub trait ViaLocal<T> {
        fn __gdl_codec(&self) -> Option<Codec<T>>;
    }

    impl<T> ViaLocal<T> for &Probe<T> {
        fn __gdl_codec(&self) -> Option<Codec<T>> {
            None
        }
    }
}
